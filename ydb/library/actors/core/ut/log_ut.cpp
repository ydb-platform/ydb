#include "log.h"

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/util/struct_log/create_message.h>
#include <ydb/core/util/struct_log/structured_message.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/testlib/test_runtime.h>

using namespace NMonitoring;
using namespace NActors;
using namespace NActors::NLog;
using namespace NKikimr::NStructLog;

namespace {
    const TString& ServiceToString(int) {
        static const TString FAKE{"FAKE"};
        return FAKE;
    }

    TIntrusivePtr<TSettings> DefaultSettings() {
        auto loggerId = TActorId{0, "Logger"};
        auto s = MakeIntrusive<TSettings>(loggerId, 0, EPriority::PRI_TRACE);
        s->SetAllowDrop(false);
        s->Append(0, 1, ServiceToString);
        return s;
    }

    TIntrusivePtr<TSettings> DroppingSettings(ui64 timeThresholdMs) {
        auto loggerId = TActorId{0, "Logger"};
        auto s = MakeIntrusive<TSettings>(
            loggerId,
            0,
            EPriority::PRI_TRACE,
            EPriority::PRI_DEBUG,
            (ui32)0,
            timeThresholdMs,
            (ui64)0);
        s->Append(0, 1, ServiceToString);
        return s;
    }

    TIntrusivePtr<TSettings> BufferSettings(ui64 bufferSizeLimitBytes) {
        auto loggerId = TActorId{0, "Logger"};
        auto s = MakeIntrusive<TSettings>(
            loggerId,
            0,
            EPriority::PRI_TRACE,
            EPriority::PRI_DEBUG,
            (ui32)0,
            (ui32)0,
            bufferSizeLimitBytes);
        s->Append(0, 1, ServiceToString);
        s->SetAllowDrop(true);
        return s;
    }

    TIntrusivePtr<TSettings> NoBufferSettings() {
        return BufferSettings(0);
    }

    class TMockBackend: public TLogBackend {
    public:
        using TWriteImpl = std::function<void(const TLogRecord&)>;
        using TReopenImpl = std::function<void()>;

        static void REOPEN_NOP() { }

        TMockBackend(TWriteImpl writeImpl, TReopenImpl reopenImpl = REOPEN_NOP)
             : WriteImpl_{writeImpl}
             , ReopenImpl_{reopenImpl}
         {
         }

        void WriteData(const TLogRecord& r) override {
            WriteImpl_(r);
        }

        void ReopenLog() override { }

        void SetWriteImpl(TWriteImpl writeImpl) {
            WriteImpl_ = writeImpl;
        }

    private:
        TWriteImpl WriteImpl_;
        TReopenImpl ReopenImpl_;
    };

    void ThrowAlways(const TLogRecord&) {
        ythrow yexception();
    };

    static bool CheckInclude(const TStructuredMessage& message, const TStructuredMessage& subMessage) {
        struct TMessageInfo {
            TNativeTypeCode TypeCode;
            const void* Data;
            std::size_t Length;
        };

        auto GetNameWithDots = [](const std::vector<TKeyName>& name) {
            TString result;
            for(auto& item: name) {
                if (!result.empty()) {
                    result += ".";
                }
                result += item.ToString();
            }
            return result;
        };

        std::map<std::string, TMessageInfo> messageInfo;
        message.ForEachSerialized(
            [&](const std::vector<TKeyName>& name, TNativeTypeCode typeCode, const void* data, std::size_t length){
                auto nameWithDots = GetNameWithDots(name);
                auto& newItem = messageInfo[nameWithDots];
                newItem.TypeCode = typeCode;
                newItem.Data = data;
                newItem.Length = length;
                return true;
            });

        auto result = subMessage.ForEachSerialized(
            [&](const std::vector<TKeyName>& name, TNativeTypeCode typeCode, const void* data, std::size_t length){
                auto nameWithDots = GetNameWithDots(name);
                auto it = messageInfo.find(nameWithDots);
                if (it == end(messageInfo)) {
                    return false;
                }
                return it->second.TypeCode == typeCode &&
                    it->second.Length == length &&
                    memcmp(it->second.Data, data, length) == 0;
            });

        return result;
    }

    struct TFixture {
        TFixture(
            TIntrusivePtr<TSettings> settings,
            TMockBackend::TWriteImpl writeImpl = ThrowAlways) : Settings(settings)
        {
            Runtime.Initialize();
            LogBackend.reset(new TMockBackend{writeImpl});
            LoggerActor = Runtime.Register(new TLoggerActor{settings, LogBackend, Counters});
            Runtime.SetScheduledEventFilter([] (auto&&, auto&&, auto&&, auto) {
                return false;
            });
        }

        TFixture(TMockBackend::TWriteImpl writeImpl = ThrowAlways)
            : TFixture(DefaultSettings(), writeImpl)
        {}

        // Emulate log context
        NLog::TSettings* LoggerSettings() {
            return Settings.Get();
        }

        void Send(TAutoPtr<IEventHandle> ev, ui32 senderNodeIndex = 0, bool viaActorSystem = false) {
            auto logEvent = ev->StaticCastAsLocal<TEvLog>();

            auto ts  = Runtime.GetCurrentTime() - TDuration::Seconds(10);
            TStructuredMessage structMessage;
            if (logEvent->StructMessage.Defined()) {
                structMessage = logEvent->StructMessage.GetRef();
            }
            Runtime.Send(new IEventHandle{LoggerActor, {},
                new TEvLog(
                    static_cast<EPriority>(logEvent->Level.ToPrio()),
                    logEvent->Component,
                    logEvent->FileName,
                    logEvent->LineNumber,
                    logEvent->Line,
                    std::move(structMessage),
                    ts)},
                senderNodeIndex, viaActorSystem);
        }

        void WriteLog() {
            Runtime.Send(new IEventHandle{LoggerActor, {}, new TEvLog(TInstant::Zero(), TLevel{EPrio::Emerg}, 0, "foo")});
        }

        void WriteLog(TInstant ts, EPrio prio = EPrio::Emerg, TString msg = "foo") {
            Runtime.Send(new IEventHandle{LoggerActor, {}, new TEvLog(ts, TLevel{prio}, 0, msg)});
        }

        void FlushLogBuffer() {
            Runtime.Send(new IEventHandle{LoggerActor, {}, new TFlushLogBuffer()});
        }

        void Wakeup() {
            Runtime.Send(new IEventHandle{LoggerActor, {}, new TEvents::TEvWakeup});
        }

        void StartAccumulateMessages(TSettings::ELogFormat format = TSettings::ELogFormat::PLAIN_FULL_FORMAT) {
            Settings->Format = format;
            Settings->Append(1000, 1002,
            [](EComponent comp) ->TString {
                static std::vector<TString> names{"A","B","C"};
                return names[comp - 1000];
            });

            auto acceptWrites = [&] (const TLogRecord& r) {
                TReceivedMessage received;
                received.Text = TString(r.Data, r.Len);
                received.StructMessage = r.StructMessage;
                ReceivedMessages.push_back(received);

                Cerr << received.Text << Endl;
            };
            LogBackend->SetWriteImpl(acceptWrites);

            Wakeup();
            Runtime.AdvanceCurrentTime(TDuration::Days(1));
        }

        void FetchMessage(const TString& text, const TStructuredMessage& structMessage = {}) {
            UNIT_ASSERT(!ReceivedMessages.empty());
            ReceivedMessages[0].Check(text, structMessage);
            ReceivedMessages.erase(begin(ReceivedMessages), begin(ReceivedMessages) + 1);
        }

        TIntrusivePtr<TDynamicCounters> Counters{MakeIntrusive<TDynamicCounters>()};
        std::shared_ptr<TMockBackend> LogBackend;
        TActorId LoggerActor;
        TTestActorRuntimeBase Runtime;
        TIntrusivePtr<NLog::TSettings> Settings;

        struct TReceivedMessage {
            TString Text;
            TMaybe<TStructuredMessage> StructMessage;

            void Check(const TString& text, const TStructuredMessage& structMessage = {}) const {
                UNIT_ASSERT_VALUES_EQUAL(Text, text);

                if (structMessage.GetValuesCount() != 0) {
                    UNIT_ASSERT(StructMessage.Defined());
                    UNIT_ASSERT(CheckInclude(StructMessage.GetRef(), structMessage));
                }
            }
        };
        std::vector<TReceivedMessage> ReceivedMessages;
    };
}


Y_UNIT_TEST_SUITE(TLoggerActorTest) {
    Y_UNIT_TEST(NoCrashOnWriteFailure) {
        TFixture test;
        test.WriteLog();
        // everything is okay as long as we get here
    }

    Y_UNIT_TEST(SubsequentWritesAreIgnored) {
        size_t count{0};
        auto countWrites = [&count] (auto&& r) {
            count++;
            ThrowAlways(r);
        };

        TFixture test{countWrites};
        test.WriteLog();
        UNIT_ASSERT_VALUES_EQUAL(count, 1);

        // at this point we should have started dropping messages
        for (auto i = 0; i < 5; ++i) {
            test.WriteLog();
        }

        UNIT_ASSERT_VALUES_EQUAL(count, 1);
    }

    Y_UNIT_TEST(LoggerCanRecover) {
        TFixture test;
        test.WriteLog();

        TVector<TString> messages;
        auto acceptWrites = [&] (const TLogRecord& r) {
            messages.emplace_back(r.Data, r.Len);
        };

        auto scheduled = test.Runtime.CaptureScheduledEvents();
        UNIT_ASSERT_VALUES_EQUAL(scheduled.size(), 1);

        test.LogBackend->SetWriteImpl(acceptWrites);
        test.Wakeup();

        const auto COUNT = 10;
        for (auto i = 0; i < COUNT; ++i) {
            test.WriteLog();
        }

        UNIT_ASSERT_VALUES_EQUAL(messages.size(), COUNT);
    }

    Y_UNIT_TEST(ShouldObeyTimeThresholdMsWhenOverloaded) {
        TFixture test{DroppingSettings(5000)};

        TVector<TString> messages;
        auto acceptWrites = [&] (const TLogRecord& r) {
            messages.emplace_back(r.Data, r.Len);
        };

        test.LogBackend->SetWriteImpl(acceptWrites);
        test.Wakeup();

        const auto COUNT = 11;
        for (auto i = 0; i < COUNT; ++i) {
            test.WriteLog();
        }

        UNIT_ASSERT_VALUES_EQUAL(messages.size(), COUNT);

        test.Runtime.AdvanceCurrentTime(TDuration::Seconds(20));
        auto now = test.Runtime.GetCurrentTime();

        test.WriteLog(now - TDuration::Seconds(5));

        UNIT_ASSERT_VALUES_EQUAL(messages.size(), COUNT + 1);

        test.WriteLog(now - TDuration::Seconds(6));

        UNIT_ASSERT_VALUES_EQUAL(messages.size(), COUNT + 1);
    }

    int BufferTest(TFixture &test, const int COUNT) {
        TVector<TString> messages;
        auto acceptWrites = [&] (const TLogRecord& r) {
            messages.emplace_back(r.Data, r.Len);
        };

        test.LogBackend->SetWriteImpl(acceptWrites);
        test.Wakeup();
        test.Runtime.AdvanceCurrentTime(TDuration::Days(1));
        auto now = test.Runtime.GetCurrentTime();

        for (auto i = 0; i < COUNT; ++i) {
            test.WriteLog(now - TDuration::Seconds(10), EPrio::Debug, std::to_string(i));
        }

        for (auto i = 0; i < COUNT; ++i) {
            test.FlushLogBuffer();
        }

        for (ui64 i = 0; i < messages.size(); ++i) {
            Cerr << messages[i] << Endl;
        }

        return messages.size();
    }

    Y_UNIT_TEST(ShouldUseLogBufferWhenOverloaded) {
        TFixture test{BufferSettings(1024 * 1024 * 300)};
        const auto LOG_COUNT = 100;
        auto outputLogSize = BufferTest(test, LOG_COUNT);

        UNIT_ASSERT_VALUES_EQUAL(outputLogSize, LOG_COUNT);
    }

    Y_UNIT_TEST(ShouldLoseLogsIfBufferZeroSize) {
        TFixture test{NoBufferSettings()};
        const auto LOG_COUNT = 100;
        auto outputLogSize = BufferTest(test, LOG_COUNT);
        UNIT_ASSERT(outputLogSize < LOG_COUNT);
    }
}

Y_UNIT_TEST_SUITE(TWriteLogTest) {

    Y_UNIT_TEST(MemLogAdapter) {
        TFixture env{NoBufferSettings()};
        env.StartAccumulateMessages();

        MemStructLogAdapter(env, NLog::EPriority::PRI_DEBUG, 0, nullptr, 0, "My log message");
        MemStructLogAdapter(env, NLog::EPriority::PRI_DEBUG, 0, nullptr, 0, "My log message1", YDBLOG_CREATE_MESSAGE({"value1", 1}));
        MemStructLogAdapter(env, NLog::EPriority::PRI_DEBUG, 0, nullptr, 0, "My log message2", YDBLOG_CREATE_MESSAGE({"value2", 2}));

        env.FlushLogBuffer();

        UNIT_ASSERT_VALUES_EQUAL(env.ReceivedMessages.size(), 3);

        env.FetchMessage("1970-01-01T23:59:50.000000Z :FAKE DEBUG: My log message");
        env.FetchMessage("1970-01-01T23:59:50.000000Z :FAKE DEBUG: My log message1", YDBLOG_CREATE_MESSAGE({"value1", 1}));
        env.FetchMessage("1970-01-01T23:59:50.000000Z :FAKE DEBUG: My log message2", YDBLOG_CREATE_MESSAGE({"value2", 2}));
    }

    Y_UNIT_TEST(WriteSimple) {
        TFixture env{NoBufferSettings()};
        env.StartAccumulateMessages();

        YDBLOG_CTX_COMP(env, PRI_DEBUG, 1, "Test message");
        YDBLOG_CTX_COMP(env, PRI_DEBUG, 1, "Test message with data", {"value", 1});
        env.FetchMessage("1970-01-01T23:59:50.000000Z :FAKE DEBUG: log_ut.cpp:393: Test message");
        env.FetchMessage("1970-01-01T23:59:50.000000Z :FAKE DEBUG: log_ut.cpp:394: Test message with data", YDBLOG_CREATE_MESSAGE({"value", 1}));
    }

    Y_UNIT_TEST(WritePriority) {
        TFixture env{NoBufferSettings()};
        env.StartAccumulateMessages();

        YDBLOG_CTX_COMP_EMERG(env, 1, "Test message");
        YDBLOG_CTX_COMP_ALERT(env, 1, "Test message");
        YDBLOG_CTX_COMP_CRIT(env, 1, "Test message");
        YDBLOG_CTX_COMP_ERROR(env, 1, "Test message");
        YDBLOG_CTX_COMP_WARN(env, 1, "Test message");
        YDBLOG_CTX_COMP_NOTICE(env, 1, "Test message");
        YDBLOG_CTX_COMP_INFO(env, 1, "Test message");
        YDBLOG_CTX_COMP_DEBUG(env, 1, "Test message");
        YDBLOG_CTX_COMP_TRACE(env, 1, "Test message");

        env.FetchMessage("1970-01-01T23:59:50.000000Z :FAKE EMERG: log_ut.cpp:403: Test message");
        env.FetchMessage("1970-01-01T23:59:50.000000Z :FAKE ALERT: log_ut.cpp:404: Test message");
        env.FetchMessage("1970-01-01T23:59:50.000000Z :FAKE CRIT: log_ut.cpp:405: Test message");
        env.FetchMessage("1970-01-01T23:59:50.000000Z :FAKE ERROR: log_ut.cpp:406: Test message");
        env.FetchMessage("1970-01-01T23:59:50.000000Z :FAKE WARN: log_ut.cpp:407: Test message");
        env.FetchMessage("1970-01-01T23:59:50.000000Z :FAKE NOTICE: log_ut.cpp:408: Test message");
        env.FetchMessage("1970-01-01T23:59:50.000000Z :FAKE INFO: log_ut.cpp:409: Test message");
        env.FetchMessage("1970-01-01T23:59:50.000000Z :FAKE DEBUG: log_ut.cpp:410: Test message");
        env.FetchMessage("1970-01-01T23:59:50.000000Z :FAKE TRACE: log_ut.cpp:411: Test message");
    }

    Y_UNIT_TEST(WriteComponent) {
        TFixture env{NoBufferSettings()};
        env.StartAccumulateMessages();

        YDBLOG_CTX_COMP_EMERG(env, 1000, "Test message");
        YDBLOG_CTX_COMP_EMERG(env, 1001, "Test message");
        YDBLOG_CTX_COMP_EMERG(env, 1002, "Test message");

        env.FetchMessage("1970-01-01T23:59:50.000000Z :A EMERG: log_ut.cpp:428: Test message");
        env.FetchMessage("1970-01-01T23:59:50.000000Z :B EMERG: log_ut.cpp:429: Test message");
        env.FetchMessage("1970-01-01T23:59:50.000000Z :C EMERG: log_ut.cpp:430: Test message");
    }

    Y_UNIT_TEST(WriteWithoutComponent) {
        TFixture env{NoBufferSettings()};
        env.StartAccumulateMessages();

#define YDBLOG_THIS_FILE_COMPONENT 1000
        YDBLOG_CTX_EMERG(env, "Test message");
#undef YDBLOG_THIS_FILE_COMPONENT

#define YDBLOG_THIS_FILE_COMPONENT 1001
        YDBLOG_CTX_EMERG(env, "Test message");
#undef YDBLOG_THIS_FILE_COMPONENT

#define YDBLOG_THIS_FILE_COMPONENT 1002
        YDBLOG_CTX_EMERG(env, "Test message");
#undef YDBLOG_THIS_FILE_COMPONENT

        env.FetchMessage("1970-01-01T23:59:50.000000Z :A EMERG: log_ut.cpp:442: Test message");
        env.FetchMessage("1970-01-01T23:59:50.000000Z :B EMERG: log_ut.cpp:446: Test message");
        env.FetchMessage("1970-01-01T23:59:50.000000Z :C EMERG: log_ut.cpp:450: Test message");
    }

    Y_UNIT_TEST(WriteWithContext) {
        using namespace NKikimr::NStructLog;

        TFixture env{NoBufferSettings()};
        env.StartAccumulateMessages();

        {
            TLogStack::TLogGuard g;
            YDBLOG_UPDATE_CONTEXT({"context", 1});
            YDBLOG_CTX_COMP_EMERG(env, 1, "Test message");
            YDBLOG_CTX_COMP_EMERG(env, 1, "Test message", {"value", 100});

            env.FetchMessage("1970-01-01T23:59:50.000000Z :FAKE EMERG: log_ut.cpp:467: Test message",
                YDBLOG_CREATE_MESSAGE({"context", 1}));
            env.FetchMessage("1970-01-01T23:59:50.000000Z :FAKE EMERG: log_ut.cpp:468: Test message",
                YDBLOG_CREATE_MESSAGE({"context", 1}, {"value", 100}));
        }

        {
            TLogStack::TLogGuard g;
            YDBLOG_UPDATE_CONTEXT({"context", 2});
            YDBLOG_CTX_COMP_EMERG(env, 1, "Test message");
            YDBLOG_CTX_COMP_EMERG(env, 1, "Test message", {"value", 100});

            env.FetchMessage("1970-01-01T23:59:50.000000Z :FAKE EMERG: log_ut.cpp:479: Test message",
                YDBLOG_CREATE_MESSAGE({"context", 2}));
            env.FetchMessage("1970-01-01T23:59:50.000000Z :FAKE EMERG: log_ut.cpp:480: Test message",
                YDBLOG_CREATE_MESSAGE({"context", 2}, {"value", 100}));

            {
                TLogStack::TLogGuard g2;
                YDBLOG_UPDATE_CONTEXT({"context", 3}, {"subcontext", 4});
                YDBLOG_CTX_COMP_EMERG(env, 1, "Test message");
                YDBLOG_CTX_COMP_EMERG(env, 1, "Test message", {"value", 100});

                env.FetchMessage("1970-01-01T23:59:50.000000Z :FAKE EMERG: log_ut.cpp:490: Test message",
                    YDBLOG_CREATE_MESSAGE({"context", 3}, {"subcontext", 4}));
                env.FetchMessage("1970-01-01T23:59:50.000000Z :FAKE EMERG: log_ut.cpp:491: Test message",
                    YDBLOG_CREATE_MESSAGE({"context", 3}, {"subcontext", 4}, {"value", 100}));
            }

            YDBLOG_CTX_COMP_EMERG(env, 1, "Test message");
            YDBLOG_CTX_COMP_EMERG(env, 1, "Test message", {"value", 100});

            env.FetchMessage("1970-01-01T23:59:50.000000Z :FAKE EMERG: log_ut.cpp:499: Test message",
                YDBLOG_CREATE_MESSAGE({"context", 2}));
            env.FetchMessage("1970-01-01T23:59:50.000000Z :FAKE EMERG: log_ut.cpp:500: Test message",
                YDBLOG_CREATE_MESSAGE({"context", 2}, {"value", 100}));
        }

        YDBLOG_CTX_COMP_EMERG(env, 1, "Test message");
        YDBLOG_CTX_COMP_EMERG(env, 1, "Test message", {"value", 100});

        env.FetchMessage("1970-01-01T23:59:50.000000Z :FAKE EMERG: log_ut.cpp:508: Test message");
        env.FetchMessage("1970-01-01T23:59:50.000000Z :FAKE EMERG: log_ut.cpp:509: Test message", YDBLOG_CREATE_MESSAGE({"value", 100}));
    }

    Y_UNIT_TEST(WriteJson) {
        TFixture env{NoBufferSettings()};
        env.StartAccumulateMessages(TSettings::ELogFormat::JSON_FORMAT);

        YDBLOG_CTX_COMP_EMERG(env, 1, "Test message with json");
        YDBLOG_CTX_COMP_EMERG(env, 1, "Test message with json", {"value1", 1});
        YDBLOG_CTX_COMP_EMERG(env, 1, "Test message with json", {"value1", 1}, {"value2", 2});
        YDBLOG_CTX_COMP_EMERG(env, 1, "Test message with json", {"value1", 1}, {"value2", 2}, {"value3", 3});
        YDBLOG_CTX_COMP_EMERG(env, 1, "Test message with json", {"component", "MY"});

        env.FetchMessage(R"({"@timestamp":"1970-01-01T23:59:50.000000Z","@log_type":"debug","microseconds":86390000000,"host":"",)"
            R"("cluster":"","database":"static","node_id":0,"priority":"EMERG","npriority":0,"component":"FAKE","tag":"KIKIMR","revision":-1,)"
            R"("location":"log_ut.cpp:519","message":"Test message with json"})");

        env.FetchMessage(R"({"@timestamp":"1970-01-01T23:59:50.000000Z","@log_type":"debug","microseconds":86390000000,"host":"",)"
            R"("cluster":"","database":"static","node_id":0,"priority":"EMERG","npriority":0,"component":"FAKE","tag":"KIKIMR","revision":-1,)"
            R"("location":"log_ut.cpp:520","message":"Test message with json","value1":1})");

        env.FetchMessage(R"({"@timestamp":"1970-01-01T23:59:50.000000Z","@log_type":"debug","microseconds":86390000000,"host":"",)"
            R"("cluster":"","database":"static","node_id":0,"priority":"EMERG","npriority":0,"component":"FAKE","tag":"KIKIMR","revision":-1,)"
            R"("location":"log_ut.cpp:521","message":"Test message with json","value1":1,"value2":2})");

        env.FetchMessage(R"({"@timestamp":"1970-01-01T23:59:50.000000Z","@log_type":"debug","microseconds":86390000000,"host":"",)"
            R"("cluster":"","database":"static","node_id":0,"priority":"EMERG","npriority":0,"component":"FAKE","tag":"KIKIMR","revision":-1,)"
            R"("location":"log_ut.cpp:522","message":"Test message with json","value1":1,"value2":2,"value3":3})");

        env.FetchMessage(R"({"@timestamp":"1970-01-01T23:59:50.000000Z","@log_type":"debug","microseconds":86390000000,"host":"",)"
            R"("cluster":"","database":"static","node_id":0,"priority":"EMERG","npriority":0,"component":"FAKE","tag":"KIKIMR","revision":-1,)"
            R"("location":"log_ut.cpp:523","message":"Test message with json","_component":"MY"})");
    }
}

#include "log.h"

#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/actors/testlib/test_runtime.h>

using namespace NMonitoring;
using namespace NActors;
using namespace NActors::NLog;

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
            timeThresholdMs);
        s->Append(0, 1, ServiceToString);
        return s;
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

    struct TFixture {
        TFixture(
            TIntrusivePtr<TSettings> settings,
            TMockBackend::TWriteImpl writeImpl = ThrowAlways)
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

        void WriteLog() {
            Runtime.Send(new IEventHandle{LoggerActor, {}, new TEvLog(TInstant::Zero(), TLevel{EPrio::Emerg}, 0, "foo")});
        }

        void WriteLog(TInstant ts) {
            Runtime.Send(new IEventHandle{LoggerActor, {}, new TEvLog(ts, TLevel{EPrio::Emerg}, 0, "foo")});
        }

        void Wakeup() {
            Runtime.Send(new IEventHandle{LoggerActor, {}, new TEvents::TEvWakeup});
        }

        TIntrusivePtr<TDynamicCounters> Counters{MakeIntrusive<TDynamicCounters>()};
        std::shared_ptr<TMockBackend> LogBackend;
        TActorId LoggerActor;
        TTestActorRuntimeBase Runtime;
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
}

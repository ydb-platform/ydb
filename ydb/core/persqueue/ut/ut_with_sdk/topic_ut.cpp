#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>

#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/persqueue/partition_key_range/partition_key_range.h>
#include <ydb/core/persqueue/pqrb/partition_scale_manager.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_env.h>

#include <util/stream/output.h>

namespace NKikimr {

using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;
using namespace NSchemeShardUT_Private;
using namespace NKikimr::NPQ::NTest;

#define UNIT_ASSERT_TIME_EQUAL(A, B, D)                                                               \
  do {                                                                                                \
    if (!(((A - B) >= TDuration::Zero()) && ((A - B) <= D))                                           \
            && !(((B - A) >= TDuration::Zero()) && ((B - A) <= D))) {                                 \
        auto&& failMsg = Sprintf("%s and %s diferent more then %s", (::TStringBuilder() << A).data(), \
            (::TStringBuilder() << B).data(), (::TStringBuilder() << D).data());                      \
        UNIT_FAIL_IMPL("assertion failure", failMsg);                                                 \
    }                                                                                                 \
  } while (false)


Y_UNIT_TEST_SUITE(WithSDK) {

    Y_UNIT_TEST(DescribeConsumer) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1);

        auto describe = [&]() {
            return setup.DescribeConsumer(TEST_TOPIC, TEST_CONSUMER);
        };

        auto write = [&](size_t seqNo) {
            TTopicClient client(setup.MakeDriver());

            TWriteSessionSettings settings;
            settings.Path(TEST_TOPIC);
            settings.PartitionId(0);
            settings.DeduplicationEnabled(false);
            auto session = client.CreateSimpleBlockingWriteSession(settings);

            TString msgTxt = TStringBuilder() << "message_" << seqNo;
            TWriteMessage msg(msgTxt);
            msg.CreateTimestamp(TInstant::Now() - TDuration::Seconds(10 - seqNo));
            UNIT_ASSERT(session->Write(std::move(msg)));

            session->Close(TDuration::Seconds(5));
        };

        // Check describe for empty topic
        {
            auto d = describe();
            UNIT_ASSERT_STRINGS_EQUAL(TEST_CONSUMER, d.GetConsumer().GetConsumerName());
            UNIT_ASSERT_VALUES_EQUAL(1, d.GetPartitions().size());
            auto& p = d.GetPartitions()[0];
            UNIT_ASSERT_VALUES_EQUAL(0, p.GetPartitionId());
            UNIT_ASSERT_VALUES_EQUAL(true, p.GetActive());
            UNIT_ASSERT_VALUES_EQUAL(0, p.GetPartitionStats()->GetEndOffset());
            auto& c = p.GetPartitionConsumerStats();
            UNIT_ASSERT_VALUES_EQUAL(true, c.has_value());
            UNIT_ASSERT_VALUES_EQUAL(0, c->GetCommittedOffset());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), c->GetMaxWriteTimeLag());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), c->GetMaxReadTimeLag());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), c->GetMaxCommittedTimeLag());
            UNIT_ASSERT_TIME_EQUAL(TInstant::Now(), c->GetLastReadTime(), TDuration::Seconds(3)); // why not zero?
            UNIT_ASSERT_VALUES_EQUAL(0, c->GetLastReadOffset());
        }

        write(3);
        write(7);

        // Check describe for topic which contains messages, but consumer hasn`t read
        {
            auto d = describe();
            UNIT_ASSERT_STRINGS_EQUAL(TEST_CONSUMER, d.GetConsumer().GetConsumerName());
            UNIT_ASSERT_VALUES_EQUAL(1, d.GetPartitions().size());
            auto& p = d.GetPartitions()[0];
            UNIT_ASSERT_VALUES_EQUAL(0, p.GetPartitionId());
            UNIT_ASSERT_VALUES_EQUAL(true, p.GetActive());
            UNIT_ASSERT_VALUES_EQUAL(2, p.GetPartitionStats()->GetEndOffset());
            auto& c = p.GetPartitionConsumerStats();
            UNIT_ASSERT_VALUES_EQUAL(true, c.has_value());
            UNIT_ASSERT_VALUES_EQUAL(0, c->GetCommittedOffset());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(7), c->GetMaxWriteTimeLag()); //
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), c->GetMaxReadTimeLag());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), c->GetMaxCommittedTimeLag());
            UNIT_ASSERT_TIME_EQUAL(TInstant::Now(), c->GetLastReadTime(), TDuration::Seconds(3)); // why not zero?
            UNIT_ASSERT_VALUES_EQUAL(1, c->GetLastReadOffset());
        }

        UNIT_ASSERT(setup.Commit(TEST_TOPIC, TEST_CONSUMER, 0, 1).IsSuccess());

        // Check describe for topic whis contains messages, has commited offset but hasn`t read (restart tablet for example)
        {
            auto d = describe();
            UNIT_ASSERT_STRINGS_EQUAL(TEST_CONSUMER, d.GetConsumer().GetConsumerName());
            UNIT_ASSERT_VALUES_EQUAL(1, d.GetPartitions().size());
            auto& p = d.GetPartitions()[0];
            UNIT_ASSERT_VALUES_EQUAL(0, p.GetPartitionId());
            UNIT_ASSERT_VALUES_EQUAL(true, p.GetActive());
            UNIT_ASSERT_VALUES_EQUAL(2, p.GetPartitionStats()->GetEndOffset());
            auto& c = p.GetPartitionConsumerStats();
            UNIT_ASSERT_VALUES_EQUAL(true, c.has_value());
            UNIT_ASSERT_VALUES_EQUAL(1, c->GetCommittedOffset());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(7), c->GetMaxWriteTimeLag());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), c->GetMaxReadTimeLag());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), c->GetMaxCommittedTimeLag());
            UNIT_ASSERT_TIME_EQUAL(TInstant::Now(), c->GetLastReadTime(), TDuration::Seconds(3)); // why not zero?
            UNIT_ASSERT_VALUES_EQUAL(1, c->GetLastReadOffset());
        }

        {
            TTopicClient client(setup.MakeDriver());
            TReadSessionSettings settings;
            settings.ConsumerName(TEST_CONSUMER);
            settings.AppendTopics(TTopicReadSettings().Path(TEST_TOPIC));

            auto session = client.CreateReadSession(settings);

            TInstant endTime = TInstant::Now() + TDuration::Seconds(5);
            while (true) {
                auto e = session->GetEvent();
                if (e) {
                    Cerr << ">>>>> Event = " << e->index() << Endl << Flush;
                }
                if (e && std::holds_alternative<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(e.value())) {
                    // we must recive only one date event with second message
                    break;
                } else if (e && std::holds_alternative<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(e.value())) {
                    std::get<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(e.value()).Confirm();
                }
                UNIT_ASSERT_C(endTime > TInstant::Now(), "Unable wait");
            }

            session->Close(TDuration::Seconds(1));
        }

        // Check describe for topic wich contains messages, has commited offset of first message and read second message
        {
            auto d = describe();
            UNIT_ASSERT_STRINGS_EQUAL(TEST_CONSUMER, d.GetConsumer().GetConsumerName());
            UNIT_ASSERT_VALUES_EQUAL(1, d.GetPartitions().size());
            auto& p = d.GetPartitions()[0];
            UNIT_ASSERT_VALUES_EQUAL(0, p.GetPartitionId());
            UNIT_ASSERT_VALUES_EQUAL(true, p.GetActive());
            UNIT_ASSERT_VALUES_EQUAL(2, p.GetPartitionStats()->GetEndOffset());
            auto& c = p.GetPartitionConsumerStats();
            UNIT_ASSERT_VALUES_EQUAL(true, c.has_value());
            UNIT_ASSERT_VALUES_EQUAL(1, c->GetCommittedOffset());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(7), c->GetMaxWriteTimeLag());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), c->GetMaxReadTimeLag());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), c->GetMaxCommittedTimeLag());
            UNIT_ASSERT_TIME_EQUAL(TInstant::Now(), c->GetLastReadTime(), TDuration::Seconds(3));
            UNIT_ASSERT_VALUES_EQUAL(2, c->GetLastReadOffset());
        }
    }

    struct TTimestampReadOptions {
        ui32 MessageSize;
        ui32 MessageCount = 20;
        ui32 TailMessageSize = 40_KB;
        ui32 TailMessageCount = 10;
        TDuration Interval = TDuration::MilliSeconds(200);
    };

    enum class ETimestampFnKind {
        Exact,
        Offset,
        Middle,
    };

    void TimestampReadImpl(const bool topicsAreFirstClassCitizen, const bool enableSkipMessagesWithObsoleteTimestamp, const TTimestampReadOptions options, const std::span<const ETimestampFnKind> timestampKinds) {
        auto createSetup = [=]() {
            NKikimrConfig::TFeatureFlags ff;
            ff.SetEnableTopicSplitMerge(true);
            ff.SetEnablePQConfigTransactionsAtSchemeShard(true);
            ff.SetEnableTopicServiceTx(true);
            ff.SetEnableTopicAutopartitioningForCDC(true);
            ff.SetEnableTopicAutopartitioningForReplication(true);
            ff.SetEnableSkipMessagesWithObsoleteTimestamp(enableSkipMessagesWithObsoleteTimestamp);

            NKikimr::Tests::TServerSettings settings = TTopicSdkTestSetup::MakeServerSettings();
            settings.SetFeatureFlags(ff);
            settings.PQConfig.MutableCompactionConfig()->SetBlobsCount(300);
            settings.PQConfig.MutableCompactionConfig()->SetBlobsSize(8_MB);
            auto setup = TTopicSdkTestSetup("TopicReadTimestamp", settings, false);

            setup.GetRuntime().SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
            setup.GetRuntime().SetLogPriority(NKikimrServices::PERSQUEUE, NActors::NLog::PRI_TRACE);
            setup.GetRuntime().SetLogPriority(NKikimrServices::PQ_PARTITION_CHOOSER, NActors::NLog::PRI_TRACE);
            setup.GetRuntime().SetLogPriority(NKikimrServices::PQ_READ_PROXY, NActors::NLog::PRI_TRACE);

            setup.GetRuntime().GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(topicsAreFirstClassCitizen);
            setup.GetRuntime().GetAppData().PQConfig.SetUseSrcIdMetaMappingInFirstClass(true);
            setup.GetRuntime().GetAppData().PQConfig.SetBalancerWakeupIntervalSec(1);
            setup.GetRuntime().GetAppData().PQConfig.SetACLRetryTimeoutSec(1);
            return setup;
        };

        TTopicSdkTestSetup setup = createSetup();
        const std::string topicName = topicsAreFirstClassCitizen ? TEST_TOPIC : "rt3.dc1--test--" + TEST_TOPIC;
        setup.CreateTopic(topicName, TEST_CONSUMER, 1);

        TVector<TInstant> createTimestamps;
        TString lastMessage;

        auto write = [&](TDuration interval, size_t count, size_t messageSize) {
            TTopicClient client(setup.MakeDriver());

            TWriteSessionSettings settings;
            settings.Path(topicName);
            settings.PartitionId(0);
            settings.DeduplicationEnabled(false);
            settings.Codec(NYdb::NTopic::ECodec::RAW);
            auto session = client.CreateSimpleBlockingWriteSession(settings);

            TInstant cur = TInstant::Now();
            for (size_t i = 0; i < count; ++i) {
                TString msgTxt = TStringBuilder() << LeftPad(i, 16);
                msgTxt *= messageSize / msgTxt.size() + 1;
                msgTxt.resize(messageSize);
                lastMessage = msgTxt;
                TWriteMessage msg(msgTxt);
                msg.CreateTimestamp(cur);
                UNIT_ASSERT(session->Write(std::move(msg)));
                createTimestamps.push_back(cur);
                cur += interval;
                SleepUntil(cur);
            }

            UNIT_ASSERT(session->Close(TDuration::Seconds(20)));
        };

        write(options.Interval, options.MessageCount, options.MessageSize);
        write(options.Interval, options.TailMessageCount, options.TailMessageSize);

        Sleep(TDuration::Seconds(5));
        auto readFromTimestamp = [&setup, &lastMessage, &topicName](std::optional<TInstant> startTimestamp, TStringBuf sessionId) {
            TTopicClient client(setup.MakeDriver());
            TReadSessionSettings settings;
            settings.WithoutConsumer();
            settings.AppendTopics(TTopicReadSettings().Path(topicName).ReadFromTimestamp(startTimestamp).AppendPartitionIds(0));
            auto session = client.CreateReadSession(settings);
            TInstant endTime = TInstant::Now() + TDuration::Seconds(10);

            TVector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage> messages;
            while (true) {
                session->WaitEvent().Wait(TDuration::Seconds(10));

                auto e = session->GetEvent();
                UNIT_ASSERT(e.has_value());

                if (NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent* data = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*e)) {
                    for (auto&& m : data->GetMessages()) {
                        messages.push_back(std::move(m));
                    }
                    if (!messages.empty() && messages.back().GetData() == lastMessage) {
                        break;
                    }
                } else if (NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent* start = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*e)) {
                    start->Confirm();
                }
                UNIT_ASSERT_C(endTime > TInstant::Now(), "Unable wait");
            }

            session->Close(TDuration::Seconds(1));
            TStringStream ssLog;
            ssLog << "SESSION " << sessionId;
            if (startTimestamp.has_value()) {
                ssLog << " " << startTimestamp->MicroSeconds() << " (" << *startTimestamp << ")";
            }

            size_t early = 0;
            for (auto&& m : messages) {
                if (startTimestamp.has_value() && m.GetWriteTime() < *startTimestamp) {
                    early++;
                }
            }
            ssLog << ": " << messages.size() << " messages;";
            ssLog << " " << early << " early messages";
            ssLog << ": [";
            for (auto&& m : messages) {
                ssLog << "{" << m.GetCreateTime().MicroSeconds() << "," << m.GetWriteTime().MicroSeconds() << "}, ";
            }
            ssLog << "]\n";
            Cerr << ssLog.Str() << Endl;
            return std::make_tuple(messages, early);
        };

        const auto [messages, _] = readFromTimestamp(std::nullopt, "all");
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), createTimestamps.size());

        auto getOffsetTimestampFor = [&](TDuration offset ,size_t sessionId) {
            TInstant writeTimestamp = messages.at(sessionId).GetWriteTime();
            TInstant prevWriteTimestamp = sessionId > 0 ?  messages.at(sessionId - 1).GetWriteTime() : writeTimestamp - TDuration::Seconds(1);
            return Max(writeTimestamp - offset, prevWriteTimestamp + TDuration::MilliSeconds(1));
        };
        auto getMiddleTimestampFor = [&](size_t sessionId) {
            TInstant writeTimestamp = messages.at(sessionId).GetWriteTime();
            TInstant createTimestamp = createTimestamps.at(sessionId);
            TInstant prevWriteTimestamp = sessionId > 0 ?  messages.at(sessionId - 1).GetWriteTime() : writeTimestamp - TDuration::Seconds(1);
            TInstant threshold = writeTimestamp - (writeTimestamp - prevWriteTimestamp) / 2;
            TInstant mid = writeTimestamp - (writeTimestamp - createTimestamp) / 2;
            return Max(mid, threshold);
        };
        struct TTimestampFn {
            std::function<TInstant(size_t)> Fn;
            TString Name;
            ETimestampFnKind Kind;
        };
        const TTimestampFn timestampCases[]{
            {std::bind_front(getOffsetTimestampFor, TDuration::MilliSeconds(0)), "exact", ETimestampFnKind::Exact},
            {std::bind_front(getOffsetTimestampFor, TDuration::MilliSeconds(1)), "offset 1ms", ETimestampFnKind::Offset},
            {std::bind_front(getOffsetTimestampFor, TDuration::MilliSeconds(50)), "offset 50ms", ETimestampFnKind::Offset},
            {std::bind_front(getOffsetTimestampFor, TDuration::MilliSeconds(6)), "offset 6ms", ETimestampFnKind::Offset},
            {getMiddleTimestampFor, "middle", ETimestampFnKind::Middle},
        };
        struct TStatistics {
            size_t Size;
            size_t Early;
        };
        struct TCase {
            TString TimestampFn;
            size_t SessionId;
            auto operator<=>(const TCase& v) const {
                const TCase& u = *this;
                return std::tie(u.TimestampFn, u.SessionId) <=> std::tie(v.TimestampFn, v.SessionId);
            }
        };
        TMap<TCase, TStatistics> result;
        for (const auto& [tsFn, tsName, tsKind] : timestampCases) {
            if (!FindPtr(timestampKinds, tsKind)) {
                continue;
            }
            for (size_t sessionId = 0; sessionId < messages.size(); ++sessionId) {
                TInstant startTimestamp = tsFn(sessionId);
                const auto [tail, early] = readFromTimestamp(startTimestamp, TStringBuilder() << LabeledOutput(tsName, sessionId));
                result[TCase{.TimestampFn = tsName, .SessionId = sessionId,}] = TStatistics{.Size = tail.size(), .Early = early,};
            }
        }
        for (const auto& [testCase, stats] : result) {
            Cerr << (TStringBuilder()
                << "CASE " << testCase.TimestampFn << " " << testCase.SessionId << ": " << stats.Size << " messages; " << stats.Early << " early messages" << "\n") << Flush;
        }
        for (const auto& [testCase, stats] : result) {
            const auto [timestampFn, sessionId] = testCase;
            UNIT_ASSERT_GE_C(stats.Size, messages.size() - sessionId, LabeledOutput(timestampFn, sessionId, stats.Size, messages.size() - sessionId));
        }
        for (const auto& [testCase, stats] : result) {
            const auto [timestampFn, sessionId] = testCase;
            UNIT_ASSERT_VALUES_EQUAL_C(stats.Early, 0, LabeledOutput(timestampFn, sessionId));
            UNIT_ASSERT_VALUES_EQUAL_C(stats.Size, messages.size() - sessionId, LabeledOutput(timestampFn, sessionId));
        }
    }

    struct TTestRegistration {
        TTestRegistration() {
            const std::tuple<TString, TTimestampReadOptions> options[]{
                {"1MB", TTimestampReadOptions{.MessageSize = 1_MB,}},
                {"6MB", TTimestampReadOptions{.MessageSize = 6_MB,}},
                {"40MB", TTimestampReadOptions{.MessageSize = 40_MB, .MessageCount = 2,}},
            };

            const std::tuple<TString, bool, bool> flags[]{
                {"LegacyTopic", true, false},
                {"Topic", true, true},
                {"LB", false, true},
            };

            const std::tuple<TString, std::vector<ETimestampFnKind>> readTimestampKinds[]{
                {"exact", {ETimestampFnKind::Exact,}},
                {"offset+middle", {ETimestampFnKind::Offset, ETimestampFnKind::Middle,}},
            };

            for (const auto& [optName, opt]: options) {
                for (const auto& [flagsName, topicsAreFirstClassCitizen, enableSkipMessagesWithObsoleteTimestamp] : flags) {
                    for (const auto& [tsName, tsKinds]: readTimestampKinds) {
                        Names.push_back(TStringBuilder() << "TimestampRead_" << optName << "_" << flagsName << "_" << tsName);
                        TCurrentTest::AddTest(Names.back().c_str(), [=](NUnitTest::TTestContext&) {
                            TimestampReadImpl(topicsAreFirstClassCitizen, enableSkipMessagesWithObsoleteTimestamp, opt, tsKinds);
                        }, false);
                    }
                }
            }
        }

        TDeque<TString> Names;
    };
    static const TTestRegistration TestRegistration;
}

} // namespace NKikimr

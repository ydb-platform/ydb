#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>

#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/persqueue/public/partition_key_range/partition_key_range.h>
#include <ydb/core/persqueue/pqrb/partition_scale_manager.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_env.h>

#include <util/stream/output.h>

namespace NKikimr {

using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;
using namespace NSchemeShardUT_Private;
using namespace NKikimr::NPQ::NTest;

using TDataEvent = NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent;
using TStartPartitionEvent = NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent;
using TStopPartitionEvent = NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent;
using TCommitOffsetAcknowledgementEvent = NYdb::NTopic::TReadSessionEvent::TCommitOffsetAcknowledgementEvent;
using TMessage = TDataEvent::TMessage;

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

        std::deque<TString> messagesTextQueue;
        auto write = [&](size_t seqNo) {
            TTopicClient client(setup.MakeDriver());

            TWriteSessionSettings settings;
            settings.Path(TEST_TOPIC);
            settings.PartitionId(0);
            settings.DeduplicationEnabled(false);
            auto session = client.CreateSimpleBlockingWriteSession(settings);

            TString msgTxt = TStringBuilder() << "message_" << seqNo;
            TWriteMessage msg(msgTxt);
            constexpr size_t maxSeqNo = 10;
            Y_ASSERT(seqNo <= maxSeqNo);
            msg.CreateTimestamp(TInstant::Now() - TDuration::Seconds(maxSeqNo - seqNo));
            UNIT_ASSERT(session->Write(std::move(msg)));
            messagesTextQueue.push_back(msgTxt);
            session->Close(TDuration::Seconds(5));
        };

        Cerr << ">>>>> Check describe for empty topic\n";
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
        Sleep(TDuration::Seconds(2));
        write(7);
        Sleep(TDuration::Seconds(2));
        write(10);

        Cerr << ">>>>> Check describe for topic which contains messages, but consumer hasn`t read\n";
        {
            auto d = describe();
            UNIT_ASSERT_STRINGS_EQUAL(TEST_CONSUMER, d.GetConsumer().GetConsumerName());
            UNIT_ASSERT_VALUES_EQUAL(1, d.GetPartitions().size());
            auto& p = d.GetPartitions()[0];
            UNIT_ASSERT_VALUES_EQUAL(0, p.GetPartitionId());
            UNIT_ASSERT_VALUES_EQUAL(true, p.GetActive());
            UNIT_ASSERT_VALUES_EQUAL(3, p.GetPartitionStats()->GetEndOffset());
            auto& c = p.GetPartitionConsumerStats();
            UNIT_ASSERT_VALUES_EQUAL(true, c.has_value());
            UNIT_ASSERT_VALUES_EQUAL(0, c->GetCommittedOffset());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(7), c->GetMaxWriteTimeLag()); //
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), c->GetMaxReadTimeLag());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(4), c->GetMaxCommittedTimeLag());
            UNIT_ASSERT_TIME_EQUAL(TInstant::Now(), c->GetLastReadTime(), TDuration::Seconds(3)); // why not zero?
            UNIT_ASSERT_VALUES_EQUAL(1, c->GetLastReadOffset());
        }

        UNIT_ASSERT(setup.Commit(TEST_TOPIC, TEST_CONSUMER, 0, 1).IsSuccess());
        messagesTextQueue.pop_front();

        Cerr << ">>>>> Check describe for topic whis contains messages, has commited offset but hasn`t read (restart tablet for example)\n";
        {
            auto d = describe();
            UNIT_ASSERT_STRINGS_EQUAL(TEST_CONSUMER, d.GetConsumer().GetConsumerName());
            UNIT_ASSERT_VALUES_EQUAL(1, d.GetPartitions().size());
            auto& p = d.GetPartitions()[0];
            UNIT_ASSERT_VALUES_EQUAL(0, p.GetPartitionId());
            UNIT_ASSERT_VALUES_EQUAL(true, p.GetActive());
            UNIT_ASSERT_VALUES_EQUAL(3, p.GetPartitionStats()->GetEndOffset());
            auto& c = p.GetPartitionConsumerStats();
            UNIT_ASSERT_VALUES_EQUAL(true, c.has_value());
            UNIT_ASSERT_VALUES_EQUAL(1, c->GetCommittedOffset());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(7), c->GetMaxWriteTimeLag());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), c->GetMaxReadTimeLag());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(2), c->GetMaxCommittedTimeLag());
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
                    for (const auto& message : std::get<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(e.value()).GetMessages()) {
                        UNIT_ASSERT(!messagesTextQueue.empty());
                        UNIT_ASSERT_VALUES_EQUAL(message.GetData(), messagesTextQueue.front());
                        messagesTextQueue.pop_front();
                    }
                    if (messagesTextQueue.empty()) {
                        // we must receive data events for all messages except the first one
                        break;
                    }
                } else if (e && std::holds_alternative<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(e.value())) {
                    std::get<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(e.value()).Confirm();
                }
                UNIT_ASSERT_C(endTime > TInstant::Now(), "Unable wait");
            }

            session->Close(TDuration::Seconds(1));
        }

        Cerr << ">>>>> Check describe for topic wich contains messages, has commited offset of first message and read second message\n";
        {
            auto d = describe();
            UNIT_ASSERT_STRINGS_EQUAL(TEST_CONSUMER, d.GetConsumer().GetConsumerName());
            UNIT_ASSERT_VALUES_EQUAL(1, d.GetPartitions().size());
            auto& p = d.GetPartitions()[0];
            UNIT_ASSERT_VALUES_EQUAL(0, p.GetPartitionId());
            UNIT_ASSERT_VALUES_EQUAL(true, p.GetActive());
            UNIT_ASSERT_VALUES_EQUAL(3, p.GetPartitionStats()->GetEndOffset());
            auto& c = p.GetPartitionConsumerStats();
            UNIT_ASSERT_VALUES_EQUAL(true, c.has_value());
            UNIT_ASSERT_VALUES_EQUAL(1, c->GetCommittedOffset());
            //UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(7), c->GetMaxWriteTimeLag());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), c->GetMaxReadTimeLag());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(2), c->GetMaxCommittedTimeLag());
            UNIT_ASSERT_TIME_EQUAL(TInstant::Now(), c->GetLastReadTime(), TDuration::Seconds(3));
            UNIT_ASSERT_VALUES_EQUAL(3, c->GetLastReadOffset());
        }
    }

    Y_UNIT_TEST(ReadWithBadTopic) {
        TTopicSdkTestSetup setup = CreateSetup();

        TTopicClient client(setup.MakeDriver());

        std::vector<std::string> topics = { "//", "==", "--", "**", "@@", "!!", "##", "$$", "\%\%", "^^", "::", "&&", "??",
             "\\\\", "||", "++", "--", ",,", "..", "``", "~~", ";;", "::", "((", "))", "[[", "]]", "{{", "}}", ""};
        for (auto& topic : topics) {
            Cerr << "Checking topic: '" << topic << "'" << Endl;

            TReadSessionSettings settings;
            settings.AppendTopics(TTopicReadSettings().Path(topic));
            settings.ConsumerName(TEST_CONSUMER);
            auto session = client.CreateReadSession(settings);
            auto event = session->GetEvent(true);
            // check that verify didn`t happened
            UNIT_ASSERT(event.has_value());
        }
    }

    Y_UNIT_TEST(ReadWithBadConsumer) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC);

        TTopicClient client(setup.MakeDriver());

        std::vector<std::string> consumers = { "//", "==", "--", "**", "@@", "!!", "##", "$$", "\%\%", "^^", "::", "&&", "??",
             "\\\\", "||", "++", "--", ",,", "..", "``", "~~", ";;", "::", "((", "))", "[[", "]]", "{{", "}}", ""};
        for (auto& consumer : consumers) {
            Cerr << "Checking consumer: '" << consumer << "'" << Endl;

            TReadSessionSettings settings;
            settings.AppendTopics(TTopicReadSettings().Path(TEST_TOPIC));
            settings.ConsumerName(consumer);
            auto session = client.CreateReadSession(settings);
            auto event = session->GetEvent(true);
            // check that verify didn`t happened
            UNIT_ASSERT(event.has_value());
        }
    }

    Y_UNIT_TEST(Read_WithConsumer_WithBadPartitions) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC);

        TTopicClient client(setup.MakeDriver());

        TReadSessionSettings settings;
        settings.AppendTopics(TTopicReadSettings().Path(TEST_TOPIC)
            .AppendPartitionIds(0)
            .AppendPartitionIds(101)
            .AppendPartitionIds(113));
        settings.ConsumerName(TEST_CONSUMER);
        auto session = client.CreateReadSession(settings);
        auto event = session->GetEvent(true);
        // check that verify didn`t happened
        UNIT_ASSERT(event.has_value());
    }

    Y_UNIT_TEST(Read_WithoutConsumer_WithBadPartitions) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC);

        TTopicClient client(setup.MakeDriver());

        TReadSessionSettings settings;
        settings.AppendTopics(TTopicReadSettings().Path(TEST_TOPIC)
            .AppendPartitionIds(0)
            .AppendPartitionIds(101)
            .AppendPartitionIds(113));
        settings.WithoutConsumer();
        auto session = client.CreateReadSession(settings);
        auto event = session->GetEvent(true);
        // check that verify didn`t happened
        UNIT_ASSERT(event.has_value());
    }

    Y_UNIT_TEST(WriteBigMessage) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.GetRuntime().GetAppData().PQConfig.SetMaxMessageSizeBytes(1_KB);
        setup.CreateTopic(TEST_TOPIC);

        TTopicClient client(setup.MakeDriver());
        TWriteSessionSettings settings;
        settings.Path(TEST_TOPIC)
            .ProducerId("producer-1")
            .MessageGroupId("producer-1")
            .Codec(ECodec::RAW);

        auto session = client.CreateWriteSession(settings);

        auto event = session->GetEvent(true);
        UNIT_ASSERT(event.has_value());
        auto* readyEvent = std::get_if<NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(&*event);
        UNIT_ASSERT(readyEvent);
        session->Write(std::move(readyEvent->ContinuationToken), TString(2_KB, 'a'));

        for (size_t i = 0; i < 10; ++i) {
            event = session->GetEvent(false);
            if (event.has_value()) {
                auto closeEvent = std::get_if<NYdb::NTopic::TSessionClosedEvent>(&*event);
                if (closeEvent) {
                    UNIT_ASSERT_C(closeEvent->GetIssues().ToOneLineString().contains("Too big message"), closeEvent->GetIssues().ToOneLineString());
                    return;
                }
            }

            Sleep(TDuration::Seconds(1));
        }

        UNIT_ASSERT_C(false, "Session closed event not received");
    }

    Y_UNIT_TEST(WithPartitionMaxInFlightBytesSetting) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC);

        TTopicClient client(setup.MakeDriver());

        TWriteSessionSettings writeSettings;
        writeSettings.Path(TEST_TOPIC)
            .ProducerId("producer-1")
            .MessageGroupId("producer-1")
            .Codec(ECodec::RAW);
        auto writeSession = client.CreateSimpleBlockingWriteSession(writeSettings);

        auto writeMessages = [&](size_t count) {
            for (size_t i = 0; i < count; ++i) {
                writeSession->Write(TString(1000, 'a'));
            }
        };

        writeMessages(10);
        Sleep(TDuration::Seconds(5));

        TReadSessionSettings settings;
        settings.AppendTopics(TTopicReadSettings().Path(TEST_TOPIC)
            .AppendPartitionIds(0));
        settings.ConsumerName(TEST_CONSUMER);
        settings.PartitionMaxInFlightBytes(10000);
        auto session = client.CreateReadSession(settings);

        TVector<TMessage> first10;
        first10.reserve(10);

        // 1) Read first 10 messages, without committing them
        while (first10.size() < 10) {
            UNIT_ASSERT_C(session->WaitEvent().Wait(TDuration::Seconds(30)), TStringBuilder() << "No event received at iteration, first10 size: " << first10.size());
            auto eventOpt = session->GetEvent(false);
            auto& event = *eventOpt;

            if (auto* start = std::get_if<TStartPartitionEvent>(&event)) {
                start->Confirm();
                continue;
            }
            if (auto* stop = std::get_if<TStopPartitionEvent>(&event)) {
                stop->Confirm();
                continue;
            }

            if (auto* data = std::get_if<TDataEvent>(&event)) {
                for (auto& msg : data->GetMessages()) {
                    if (first10.size() >= 10) {
                        break;
                    }
                    first10.push_back(msg);
                }
            }
        }

        writeMessages(10);
        Sleep(TDuration::Seconds(5));

        // 2) Check that no new events appear due to the inflight bytes limit
        {
            auto future = session->WaitEvent();
            UNIT_ASSERT(!future.Wait(TDuration::Seconds(3)));
        }

        // 3) Commit these 10 messages
        for (auto& msg : first10) {
            msg.Commit();
        }

        first10.clear();

        // 4) Check that after commit new events start appearing again
        while (first10.size() < 10) {
            UNIT_ASSERT_C(session->WaitEvent().Wait(TDuration::Seconds(30)), TStringBuilder() << "No event received at iteration, first10 size: " << first10.size());
            auto eventOpt = session->GetEvent(false);
            auto& event = *eventOpt;

            if (auto* stop = std::get_if<TStopPartitionEvent>(&event)) {
                stop->Confirm();
                break;
            }

            if (auto* data = std::get_if<TDataEvent>(&event)) {
                for (auto& msg : data->GetMessages()) {
                    first10.push_back(msg);
                }
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(10, first10.size());
        UNIT_ASSERT(session->Close(TDuration::Seconds(5)));
    }

    Y_UNIT_TEST(WithPartitionMaxInFlightBytesSetting_ManyPartitions) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 3, 3);

        TTopicClient client(setup.MakeDriver());

        auto describeResult = client.DescribeTopic(TEST_TOPIC).GetValueSync();
        UNIT_ASSERT(describeResult.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(3, describeResult.GetTopicDescription().GetPartitions().size());

        const auto& partitions = describeResult.GetTopicDescription().GetPartitions();

        TWriteSessionSettings writeSettings1;
        writeSettings1.Path(TEST_TOPIC)
            .ProducerId("producer-1")
            .MessageGroupId("producer-1")
            .Codec(ECodec::RAW)
            .PartitionId(partitions[0].GetPartitionId())
            .DirectWriteToPartition(true);

        TWriteSessionSettings writeSettings2 = writeSettings1;
        writeSettings1.ProducerId("producer-2");
        writeSettings1.MessageGroupId("producer-2");
        writeSettings2.PartitionId(partitions[1].GetPartitionId());

        TWriteSessionSettings writeSettings3 = writeSettings1;
        writeSettings3.ProducerId("producer-3");
        writeSettings3.MessageGroupId("producer-3");
        writeSettings3.PartitionId(partitions[2].GetPartitionId());

        std::unordered_map<ui32, std::shared_ptr<ISimpleBlockingWriteSession>> writeSessions;
        writeSessions[partitions[0].GetPartitionId()] = client.CreateSimpleBlockingWriteSession(writeSettings1);
        writeSessions[partitions[1].GetPartitionId()] = client.CreateSimpleBlockingWriteSession(writeSettings2);
        writeSessions[partitions[2].GetPartitionId()] = client.CreateSimpleBlockingWriteSession(writeSettings3);

        std::vector<TMessage> receivedMessages;
        std::vector<ui64> committedOffsets;

        auto writeMessages = [&](size_t count, ui32 partitionId) {
            for (size_t i = 0; i < count; ++i) {
                writeSessions[partitionId]->Write(TString(1000, 'a'));
            }
        };

        auto handleEvents = [&](std::shared_ptr<IReadSession> session) {
            while (true) {
                auto eventOpt = session->GetEvent(false);
                if (!eventOpt) {
                    break;
                }
                auto& event = *eventOpt;

                if (auto* start = std::get_if<TStartPartitionEvent>(&event)) {
                    start->Confirm();
                    continue;
                }
                if (auto* stop = std::get_if<TStopPartitionEvent>(&event)) {
                    stop->Confirm();
                    continue;
                }
                if (auto* data = std::get_if<TDataEvent>(&event)) {
                    for (auto& msg : data->GetMessages()) {
                        receivedMessages.push_back(msg);
                    }
                    continue;
                }
                if (auto* commit = std::get_if<TCommitOffsetAcknowledgementEvent>(&event)) {
                    committedOffsets.push_back(commit->GetCommittedOffset());
                    continue;
                }
                if (std::get_if<TSessionClosedEvent>(&event)) {
                    return;
                }
            }
        };

        TReadSessionSettings settings;
        settings.AppendTopics(
            TTopicReadSettings()
            .Path(TEST_TOPIC)
            .AppendPartitionIds(0)
            .AppendPartitionIds(1)
            .AppendPartitionIds(2));
        settings.ConsumerName(TEST_CONSUMER);
        settings.PartitionMaxInFlightBytes(2000);
        auto session = client.CreateReadSession(settings);

        auto readMessages = [&](size_t count, TDuration timeout) {
            auto deadline = TInstant::Now() + timeout;
            while (receivedMessages.size() < count && TInstant::Now() < deadline) {
                UNIT_ASSERT_C(session->WaitEvent().Wait(TDuration::Seconds(30)), TStringBuilder() << "No event received in 30 seconds");
                handleEvents(session);
            }
            UNIT_ASSERT_VALUES_EQUAL(count, receivedMessages.size());
        };

        auto waitCommitMessages = [&](size_t count, TDuration timeout) {
            auto deadline = TInstant::Now() + timeout;
            while (committedOffsets.size() < count && TInstant::Now() < deadline) {
                UNIT_ASSERT_C(session->WaitEvent().Wait(TDuration::Seconds(30)), TStringBuilder() << "No event received in 30 seconds");
                Sleep(TDuration::Seconds(1));
                handleEvents(session);
            }
            UNIT_ASSERT_VALUES_EQUAL(count, committedOffsets.size());
        };

        writeMessages(2, partitions[0].GetPartitionId());
        Sleep(TDuration::Seconds(5));
        readMessages(2, TDuration::Seconds(30));

        writeMessages(1, partitions[0].GetPartitionId());
        writeMessages(2, partitions[1].GetPartitionId());
        writeMessages(2, partitions[2].GetPartitionId());
        Sleep(TDuration::Seconds(10));
        readMessages(6, TDuration::Seconds(30));

        std::unordered_set<ui64> expectedPartitions = { partitions[1].GetPartitionId(), partitions[2].GetPartitionId() };
        for (size_t i = 0; i < receivedMessages.size(); ++i) {
            if (i >= 2) {
                UNIT_ASSERT(expectedPartitions.contains(receivedMessages[i].GetPartitionSession()->GetPartitionId()));
            } else {
                UNIT_ASSERT_VALUES_EQUAL(partitions[0].GetPartitionId(), receivedMessages[i].GetPartitionSession()->GetPartitionId());
            }

            receivedMessages[i].Commit();
        }

        receivedMessages.clear();
        waitCommitMessages(6, TDuration::Seconds(30));
        readMessages(1, TDuration::Seconds(30));
        receivedMessages[0].Commit();

        UNIT_ASSERT(session->Close(TDuration::Seconds(5)));
        for (const auto& [_, writeSession] : writeSessions) {
            UNIT_ASSERT(writeSession->Close(TDuration::Seconds(5)));
        }
    }
}
} // namespace NKikimr

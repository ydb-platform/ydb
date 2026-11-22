#include <ydb/public/sdk/cpp/src/client/persqueue_public/include/client.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/testlib/helpers.h>

namespace NKikimr::NPersQueueTests {
namespace {

using namespace NYdb::NTopic;

const TString TOPIC = "/Root/source";
const TString CONSUMER = "reader";
const TString REPLICATION_CONSUMER = "replicator";
const TString RESTRICTION_REASON = "Topic mutations are temporarily disabled";

class TSourceTopicMutateEnv {
public:
    NYdb::NTopic::NTests::TTopicSdkTestSetup Setup;
    NYdb::TDriver Driver;
    TTopicClient Client;

    explicit TSourceTopicMutateEnv(bool replicationConsumer = false)
        : Setup("SourceTopicMutate", MakeSettings(), false)
        , Driver(Setup.MakeDriverConfig())
        , Client(Driver)
    {
        if (replicationConsumer) {
            NYdb::NPersQueue::TPersQueueClient pqClient(Driver);
            const auto result = pqClient.CreateTopic(TOPIC, NYdb::NPersQueue::TCreateTopicSettings()
                .PartitionsCount(1)
                .ReadRules({
                    NYdb::NPersQueue::TReadRuleSettings().ConsumerName(CONSUMER),
                    NYdb::NPersQueue::TReadRuleSettings().ConsumerName(REPLICATION_CONSUMER),
                })
                .ReplicationConsumer(REPLICATION_CONSUMER)).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return;
        }

        const auto result = Client.CreateTopic(TOPIC, TCreateTopicSettings()
            .BeginConfigurePartitioningSettings()
                .MinActivePartitions(1)
            .EndConfigurePartitioningSettings()
            .BeginAddConsumer(CONSUMER)
            .EndAddConsumer()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    void SetRestricted(bool restricted) {
        auto& runtime = Setup.GetRuntime();
        for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
            runtime.GetAppData(node).FeatureFlags.SetEnableSourceTopicMutateRestricted(restricted);
        }
    }

    TWriteSessionSettings WriteSettings() const {
        return TWriteSessionSettings().Path(TOPIC).PartitionId(0)
            .ProducerId("producer").MessageGroupId("producer").Codec(ECodec::RAW)
            .RetryPolicy(NYdb::NTopic::IRetryPolicy::GetNoRetryPolicy());
    }

    void Write(ui64 seqNo, NYdb::TTransactionBase* tx = nullptr) {
        auto writer = Client.CreateSimpleBlockingWriteSession(WriteSettings());
        const auto data = "message-" + ToString(seqNo);
        TWriteMessage message(data);
        message.SeqNo(seqNo);
        UNIT_ASSERT(writer->Write(std::move(message), tx, TDuration::Seconds(10)));
        UNIT_ASSERT(writer->Close(TDuration::Seconds(30)));
    }

    void AssertOffsets(ui64 committedOffset, ui64 endOffset, const TString& consumer = CONSUMER) {
        auto reader = Client.CreateReadSession(TReadSessionSettings()
            .ConsumerName(consumer).AppendTopics(TTopicReadSettings(TOPIC))
            .RetryPolicy(NYdb::NTopic::IRetryPolicy::GetNoRetryPolicy()));
        const auto deadline = TInstant::Now() + TDuration::Seconds(30);
        bool started = false;
        while (!started && TInstant::Now() < deadline) {
            UNIT_ASSERT_C(reader->WaitEvent().Wait(deadline - TInstant::Now()), "Read session did not start");
            auto event = reader->GetEvent(false);
            UNIT_ASSERT(event);
            if (auto* start = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
                // Session offsets are authoritative, unlike cached Describe statistics.
                UNIT_ASSERT_VALUES_EQUAL(start->GetCommittedOffset(), committedOffset);
                UNIT_ASSERT_VALUES_EQUAL(start->GetEndOffset(), endOffset);
                start->Confirm();
                started = true;
            } else if (auto* closed = std::get_if<TSessionClosedEvent>(&*event)) {
                UNIT_FAIL(closed->DebugString());
            }
        }
        UNIT_ASSERT(started);
        UNIT_ASSERT(reader->Close(TDuration::Seconds(10)));
    }

private:
    static Tests::TServerSettings MakeSettings() {
        auto settings = NYdb::NTopic::NTests::TTopicSdkTestSetup::MakeServerSettings();
        settings.FeatureFlags.SetEnableSourceTopicMutateRestricted(false);
        return settings;
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TSourceTopicMutateRestricted) {
    Y_UNIT_TEST(AllowOnlyReplicationOffsetCommitAfterEnablingFlag) {
        TSourceTopicMutateEnv env(true);
        env.Write(1);
        env.Write(2);
        const auto commitSettings = TCommitOffsetSettings().ClientTimeout(TDuration::Seconds(10));
        for (const auto& consumer : {CONSUMER, REPLICATION_CONSUMER}) {
            const auto initial = env.Client.CommitOffset(TOPIC, 0, consumer, 1, commitSettings).GetValueSync();
            UNIT_ASSERT_C(initial.IsSuccess(), initial.GetIssues().ToString());
            env.AssertOffsets(1, 2, consumer);
        }

        env.SetRestricted(true);
        const auto rejected = env.Client.CommitOffset(TOPIC, 0, CONSUMER, 2, commitSettings).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(rejected.GetStatus(), NYdb::EStatus::UNAVAILABLE, rejected.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(rejected.GetIssues().ToString(), RESTRICTION_REASON);
        env.AssertOffsets(1, 2, CONSUMER);

        const auto committed = env.Client.CommitOffset(TOPIC, 0, REPLICATION_CONSUMER, 2, commitSettings).GetValueSync();
        UNIT_ASSERT_C(committed.IsSuccess(), committed.GetIssues().ToString());
        env.AssertOffsets(2, 2, REPLICATION_CONSUMER);

        env.SetRestricted(false);
        const auto resumed = env.Client.CommitOffset(TOPIC, 0, CONSUMER, 2, commitSettings).GetValueSync();
        UNIT_ASSERT_C(resumed.IsSuccess(), resumed.GetIssues().ToString());
        env.AssertOffsets(2, 2, CONSUMER);
    }

    Y_UNIT_TEST_TWIN(ReplicationConsumerCommitsWithClientModificationsDisabled, largeMessage) {
        TSourceTopicMutateEnv env(true);
        auto writer = env.Client.CreateSimpleBlockingWriteSession(env.WriteSettings());
        const TString data(largeMessage ? 600 * 1024 : 16, 'x');
        UNIT_ASSERT(writer->Write(data, 1));
        UNIT_ASSERT(writer->Write(data, 2));
        UNIT_ASSERT(writer->Close(TDuration::Seconds(30)));

        auto& runtime = env.Setup.GetRuntime();
        for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
            runtime.GetAppData(node).FeatureFlags.SetEnableClientModifications(false);
        }

        const auto settings = TCommitOffsetSettings().ClientTimeout(TDuration::Seconds(10));
        const auto rejected = env.Client.CommitOffset(TOPIC, 0, CONSUMER, 1, settings).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(rejected.GetStatus(), NYdb::EStatus::PRECONDITION_FAILED,
            rejected.GetIssues().ToString());
        env.AssertOffsets(0, 2, CONSUMER);

        const auto committed = env.Client.CommitOffset(TOPIC, 0, REPLICATION_CONSUMER, 1, settings).GetValueSync();
        UNIT_ASSERT_C(committed.IsSuccess(), committed.GetIssues().ToString());
        env.AssertOffsets(1, 2, REPLICATION_CONSUMER);

        auto reader = env.Client.CreateReadSession(TReadSessionSettings()
            .ConsumerName(REPLICATION_CONSUMER).AppendTopics(TTopicReadSettings(TOPIC))
            .RetryPolicy(NYdb::NTopic::IRetryPolicy::GetNoRetryPolicy()));
        const auto deadline = TInstant::Now() + TDuration::Seconds(30);
        bool acknowledged = false;
        while (!acknowledged && TInstant::Now() < deadline) {
            UNIT_ASSERT_C(reader->WaitEvent().Wait(deadline - TInstant::Now()), "Commit was not acknowledged");
            auto event = reader->GetEvent(false);
            UNIT_ASSERT(event);
            if (auto* start = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
                UNIT_ASSERT_VALUES_EQUAL(start->GetCommittedOffset(), 1);
                start->Confirm();
            } else if (auto* received = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
                received->Commit();
            } else if (auto* ack = std::get_if<NYdb::NTopic::TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(&*event)) {
                UNIT_ASSERT_VALUES_EQUAL(ack->GetCommittedOffset(), 2);
                acknowledged = true;
            } else if (auto* closed = std::get_if<TSessionClosedEvent>(&*event)) {
                UNIT_FAIL(closed->DebugString());
            }
        }
        UNIT_ASSERT(acknowledged);
        UNIT_ASSERT(reader->Close(TDuration::Seconds(10)));
        env.AssertOffsets(2, 2, REPLICATION_CONSUMER);
    }

}

} // namespace NKikimr::NPersQueueTests

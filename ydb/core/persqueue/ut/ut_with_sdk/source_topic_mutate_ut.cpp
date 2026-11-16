#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPersQueueTests {
namespace {

using namespace NYdb::NTopic;

const TString TOPIC = "/Root/source";
const TString CONSUMER = "reader";
const TString RESTRICTION_REASON = "Topic mutations are temporarily disabled";

class TSourceTopicMutateEnv {
public:
    NYdb::NTopic::NTests::TTopicSdkTestSetup Setup;
    NYdb::TDriver Driver;
    TTopicClient Client;

    TSourceTopicMutateEnv()
        : Setup("SourceTopicMutate", MakeSettings(), false)
        , Driver(Setup.MakeDriverConfig())
        , Client(Driver)
    {
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

    void AssertOffsets(ui64 committedOffset, ui64 endOffset) {
        auto reader = Client.CreateReadSession(TReadSessionSettings()
            .ConsumerName(CONSUMER).AppendTopics(TTopicReadSettings(TOPIC))
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

TWriteSessionEvent::TEvent WaitWriteEvent(const std::shared_ptr<IWriteSession>& writer) {
    UNIT_ASSERT_C(writer->WaitEvent().Wait(TDuration::Seconds(30)), "Timed out waiting for write session event");
    auto event = writer->GetEvent(false);
    UNIT_ASSERT(event);
    return std::move(*event);
}

TContinuationToken WaitReady(const std::shared_ptr<IWriteSession>& writer) {
    for (;;) {
        auto event = WaitWriteEvent(writer);
        if (auto* ready = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&event)) {
            return std::move(ready->ContinuationToken);
        }
        if (auto* closed = std::get_if<TSessionClosedEvent>(&event)) {
            UNIT_FAIL(closed->DebugString());
        }
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TSourceTopicMutateRestricted) {
    Y_UNIT_TEST(RejectOrdinaryWriteAfterEnablingFlag) {
        TSourceTopicMutateEnv env;
        auto writer = env.Client.CreateWriteSession(env.WriteSettings());
        writer->Write(WaitReady(writer), "before-restriction", 1);
        auto flushed = writer->Flush();
        UNIT_ASSERT(flushed.Wait(TDuration::Seconds(30)));
        UNIT_ASSERT(flushed.GetValue());
        env.AssertOffsets(0, 1);

        auto token = WaitReady(writer);
        env.SetRestricted(true);
        writer->Write(std::move(token), "must-not-be-written", 2);

        bool closed = false;
        while (!closed) {
            auto event = WaitWriteEvent(writer);
            if (auto* result = std::get_if<TSessionClosedEvent>(&event)) {
                UNIT_ASSERT_VALUES_EQUAL_C(result->GetStatus(), NYdb::EStatus::UNAVAILABLE, result->DebugString());
                UNIT_ASSERT_STRING_CONTAINS(result->GetIssues().ToString(), RESTRICTION_REASON);
                closed = true;
            } else if (auto* acks = std::get_if<TWriteSessionEvent::TAcksEvent>(&event)) {
                for (const auto& ack : acks->Acks) {
                    UNIT_ASSERT_C(ack.SeqNo != 2 || ack.State == TWriteSessionEvent::TWriteAck::EES_DISCARDED,
                        "Restricted write was acknowledged");
                }
            }
        }
        env.AssertOffsets(0, 1);
    }

    Y_UNIT_TEST(RejectOffsetCommitAfterEnablingFlag) {
        TSourceTopicMutateEnv env;
        env.Write(1);
        env.Write(2);
        const auto initial = env.Client.CommitOffset(TOPIC, 0, CONSUMER, 1).GetValueSync();
        UNIT_ASSERT_C(initial.IsSuccess(), initial.GetIssues().ToString());
        env.AssertOffsets(1, 2);

        env.SetRestricted(true);
        const auto rejected = env.Client.CommitOffset(TOPIC, 0, CONSUMER, 2,
            TCommitOffsetSettings().ClientTimeout(TDuration::Seconds(10))).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(rejected.GetStatus(), NYdb::EStatus::UNAVAILABLE, rejected.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(rejected.GetIssues().ToString(), RESTRICTION_REASON);
        env.AssertOffsets(1, 2);
    }

    Y_UNIT_TEST(AbortTransactionAfterEnablingFlag) {
        TSourceTopicMutateEnv env;
        NYdb::NQuery::TQueryClient queryClient(env.Driver);
        const auto sessionResult = queryClient.GetSession().GetValueSync();
        UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
        auto session = sessionResult.GetSession();
        auto beginTx = [&] {
            const auto result = session.BeginTransaction(NYdb::NQuery::TTxSettings()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return result.GetTransaction();
        };

        auto committedTx = beginTx();
        env.Write(1, &committedTx);
        const auto committed = committedTx.Commit().GetValueSync();
        UNIT_ASSERT_C(committed.IsSuccess(), committed.GetIssues().ToString());
        env.AssertOffsets(0, 1);

        auto abortedTx = beginTx();
        // Wait for the SDK write acknowledgement before restricting mutations:
        // the failure must come from transaction commit, not from staging data.
        env.Write(2, &abortedTx);
        env.SetRestricted(true);
        const auto aborted = abortedTx.Commit().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(aborted.GetStatus(), NYdb::EStatus::ABORTED, aborted.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(aborted.GetIssues().ToString(), RESTRICTION_REASON);
        env.AssertOffsets(0, 1);
    }
}

} // namespace NKikimr::NPersQueueTests

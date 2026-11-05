#include <ydb/core/persqueue/common/proxy/actor_persqueue_client_iface.h>
#include <ydb/core/persqueue/public/mlp/ut/common/common.h>
#include <ydb/public/api/grpc/draft/ydb_persqueue_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>

#include <chrono>

namespace NKikimr::NPersQueueTests {
namespace {

class TTopicReplicationEnv {
public:
    // The factory must outlive the server and its mirrorer actors.
    std::shared_ptr<NPQ::TPersQueueMirrorReaderFactory> Factory;
    NYdb::NTopic::NTests::TTopicSdkTestSetup Setup;
    NYdb::TDriver Driver;
    NYdb::NTopic::TTopicClient Client;
    std::unique_ptr<Ydb::PersQueue::V1::PersQueueService::Stub> Stub;

    explicit TTopicReplicationEnv(const Tests::TServerSettings& settings = NYdb::NTopic::NTests::TTopicSdkTestSetup::MakeServerSettings())
        : Setup("TopicReplication", settings, false)
        , Driver(Setup.MakeDriverConfig())
        , Client(Driver)
        , Stub(Ydb::PersQueue::V1::PersQueueService::NewStub(grpc::CreateChannel(
            Setup.GetEndpoint(), grpc::InsecureChannelCredentials())))
    {
        auto& runtime = Setup.GetRuntime();
        Factory = std::make_shared<NPQ::TPersQueueMirrorReaderFactory>();
        Factory->Initialize(runtime.GetAnyNodeActorSystem(), runtime.GetAppData().PQConfig.GetMirrorConfig().GetPQLibSettings());
        for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
            runtime.GetAppData(node).PersQueueMirrorReaderFactory = Factory.get();
        }
    }

    void Create(const TString& path, bool replicationConsumer) {
        Ydb::PersQueue::V1::CreateTopicRequest request;
        request.set_path(path);
        auto* settings = request.mutable_settings();
        settings->set_partitions_count(1);
        settings->set_retention_period_ms(TDuration::Days(1).MilliSeconds());
        settings->set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
        settings->add_supported_codecs(Ydb::PersQueue::V1::CODEC_RAW);
        for (const auto* name : {"reader", "replicator"}) {
            auto* rule = settings->add_read_rules();
            rule->set_consumer_name(name);
            rule->set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
            rule->add_supported_codecs(Ydb::PersQueue::V1::CODEC_RAW);
        }
        if (replicationConsumer) {
            (*settings->mutable_attributes())["_ydb_replication_consumer"] = "replicator";
        }
        grpc::ClientContext context;
        InitContext(context);
        Ydb::PersQueue::V1::CreateTopicResponse response;
        UNIT_ASSERT(Stub->CreateTopic(&context, request, &response).ok());
        AssertSuccess(response.operation());
    }

    Ydb::PersQueue::V1::TopicSettings Describe(const TString& path) {
        Ydb::PersQueue::V1::DescribeTopicRequest request;
        request.set_path(path);
        grpc::ClientContext context;
        InitContext(context);
        Ydb::PersQueue::V1::DescribeTopicResponse response;
        UNIT_ASSERT(Stub->DescribeTopic(&context, request, &response).ok());
        AssertSuccess(response.operation());
        Ydb::PersQueue::V1::DescribeTopicResult result;
        UNIT_ASSERT(response.operation().result().UnpackTo(&result));
        return result.settings();
    }

    void Alter(const TString& path, const Ydb::PersQueue::V1::TopicSettings& settings,
        Ydb::StatusIds::StatusCode expectedStatus = Ydb::StatusIds::SUCCESS) {
        Ydb::PersQueue::V1::AlterTopicRequest request;
        request.set_path(path);
        *request.mutable_settings() = settings;
        grpc::ClientContext context;
        InitContext(context);
        Ydb::PersQueue::V1::AlterTopicResponse response;
        UNIT_ASSERT(Stub->AlterTopic(&context, request, &response).ok());
        UNIT_ASSERT(response.operation().ready());
        UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), expectedStatus, response.operation().ShortDebugString());
    }

    void EnableMirroring(const TString& source, const TString& destination, TInstant readFrom = TInstant::Zero()) {
        auto settings = Describe(destination);
        auto* mirror = settings.mutable_remote_mirror_rule();
        mirror->set_endpoint(Setup.GetEndpoint());
        mirror->set_database(Setup.GetDatabase());
        mirror->set_topic_path(source);
        mirror->set_consumer_name("replicator");
        mirror->set_starting_message_timestamp_ms(readFrom.MilliSeconds());
        mirror->mutable_credentials()->set_oauth_token("");
        (*settings.mutable_attributes())["_ydb_mirror_consumer_offsets"] = "true";
        Alter(destination, settings);
    }

    void Commit(const TString& path, ui64 offset) {
        const auto result = Client.CommitOffset(path, 0, "reader", offset).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    NYdb::NTopic::TPartitionInfo DescribePartition(const TString& path, const TString& consumer) {
        const auto result = Client.DescribeConsumer(path, consumer,
            NYdb::NTopic::TDescribeConsumerSettings().IncludeStats(true).IncludeLocation(true)).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        const auto& partitions = result.GetConsumerDescription().GetPartitions();
        UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);
        return partitions.front();
    }

    NYdb::NTopic::TPartitionInfo DescribePartition(const TString& path) {
        // DescribeConsumer filters partition status by consumer and omits PartitionSize.
        const auto result = Client.DescribePartition(path, 0,
            NYdb::NTopic::TDescribePartitionSettings().IncludeStats(true)).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        return result.GetPartitionDescription().GetPartition();
    }

    void WaitForOffsets(const TString& path, ui64 expectedCommittedOffset, ui64 expectedEndOffset,
        const TString& consumer = "reader", TMaybe<ui64> expectedStartOffset = Nothing()) {
        const auto deadline = TInstant::Now() + TDuration::Seconds(90);
        ui64 committedOffset = 0;
        ui64 endOffset = 0;
        ui64 startOffset = 0;
        while (TInstant::Now() < deadline) {
            const auto partition = DescribePartition(path, consumer);
            const auto& consumerStats = partition.GetPartitionConsumerStats();
            const auto& partitionStats = partition.GetPartitionStats();
            if (consumerStats && partitionStats) {
                committedOffset = consumerStats->GetCommittedOffset();
                endOffset = partitionStats->GetEndOffset();
                startOffset = partitionStats->GetStartOffset();
                if (committedOffset == expectedCommittedOffset && endOffset == expectedEndOffset
                    && (!expectedStartOffset.Defined() || startOffset == *expectedStartOffset)) {
                    return;
                }
            }
            Sleep(TDuration::MilliSeconds(200));
        }
        UNIT_ASSERT_VALUES_EQUAL_C(committedOffset, expectedCommittedOffset,
            "Timed out waiting for consumer offset: topic=" << path << ", consumer=" << consumer
            << ", endOffset=" << endOffset << ", expectedEndOffset=" << expectedEndOffset);
        UNIT_ASSERT_VALUES_EQUAL_C(endOffset, expectedEndOffset,
            "Timed out waiting for mirrored messages: topic=" << path << ", consumer=" << consumer);
        if (expectedStartOffset.Defined()) {
            UNIT_ASSERT_VALUES_EQUAL_C(startOffset, *expectedStartOffset,
                "Timed out waiting for start offset: topic=" << path);
        }
    }

    void AssertEmpty(const TString& path, ui64 committedOffset, ui64 endOffset) {
        const auto partition = DescribePartition(path);
        UNIT_ASSERT(partition.GetPartitionStats());
        const auto& stats = *partition.GetPartitionStats();
        UNIT_ASSERT_VALUES_EQUAL(stats.GetStartOffset(), endOffset);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetEndOffset(), endOffset);

        using TEvent = NYdb::NTopic::TReadSessionEvent;
        auto reader = Client.CreateReadSession(NYdb::NTopic::TReadSessionSettings()
            .ConsumerName("reader").AppendTopics(NYdb::NTopic::TTopicReadSettings(path)));
        const auto deadline = TInstant::Now() + TDuration::Seconds(30);
        NYdb::NTopic::TPartitionSession::TPtr session;
        bool reachedEnd = false;
        while (!reachedEnd && TInstant::Now() < deadline) {
            if (session) {
                session->RequestStatus();
            }
            if (!reader->WaitEvent().Wait(TDuration::MilliSeconds(200))) {
                continue;
            }
            auto event = reader->GetEvent(false);
            if (!event) {
                continue;
            }
            if (auto* start = std::get_if<TEvent::TStartPartitionSessionEvent>(&*event)) {
                UNIT_ASSERT_VALUES_EQUAL(start->GetCommittedOffset(), committedOffset);
                UNIT_ASSERT_VALUES_EQUAL(start->GetEndOffset(), endOffset);
                session = start->GetPartitionSession();
                // StartOffset == EndOffset was checked above. Reading from zero
                // would be rejected because it is below the replicated commit.
                start->Confirm(stats.GetStartOffset());
            } else if (auto* data = std::get_if<TEvent::TDataReceivedEvent>(&*event)) {
                UNIT_ASSERT_VALUES_EQUAL_C(data->GetMessagesCount(), 0, "Unexpected data in " << path);
            } else if (auto* status = std::get_if<TEvent::TPartitionSessionStatusEvent>(&*event)) {
                UNIT_ASSERT_VALUES_EQUAL(status->GetEndOffset(), endOffset);
                reachedEnd = status->GetReadOffset() == endOffset;
            } else if (auto* stop = std::get_if<TEvent::TStopPartitionSessionEvent>(&*event)) {
                session.Reset();
                stop->Confirm();
            } else if (auto* closed = std::get_if<NYdb::NTopic::TSessionClosedEvent>(&*event)) {
                UNIT_FAIL(closed->DebugString());
            }
        }
        UNIT_ASSERT_C(reachedEnd, "Reader did not reach the end of empty topic " << path);
        UNIT_ASSERT(reader->Close(TDuration::Seconds(10)));
    }

    void Write(const TString& path, ui64 first, ui64 count, const TString& payloadSuffix = {}) {
        auto writer = Client.CreateSimpleBlockingWriteSession(NYdb::NTopic::TWriteSessionSettings()
            .Path(path).ProducerId("producer").MessageGroupId("producer").Codec(NYdb::NTopic::ECodec::RAW));
        for (ui64 offset = first; offset < first + count; ++offset) {
            UNIT_ASSERT_C(writer->Write("message-" + ToString(offset) + payloadSuffix,
                offset + 1, std::nullopt, TDuration::Seconds(10)), offset);
        }
        UNIT_ASSERT_C(writer->Close(TDuration::Seconds(30)), path);
    }

    void AddMLPConsumer(const TString& path) {
        // Use PQv1 so the existing _ydb_replication_consumer attribute is
        // reapplied when rebuilding the consumer list.
        auto settings = Describe(path);
        auto* rule = settings.add_read_rules();
        rule->set_consumer_name("mlp-consumer");
        rule->set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
        rule->add_supported_codecs(Ydb::PersQueue::V1::CODEC_RAW);
        rule->mutable_shared_consumer_type()->set_keep_messages_order(false);
        Alter(path, settings);
    }

    void ReadMLP(const TString& path, ui64 first, ui64 count, const TString& payloadSuffix) {
        auto& runtime = Setup.GetRuntime();
        const auto deadline = TInstant::Now() + TDuration::Seconds(60);
        ui64 received = 0;
        while (received < count && TInstant::Now() < deadline) {
            NPQ::NMLP::CreateReaderActor(runtime, {
                .DatabasePath = Setup.GetDatabase(),
                .TopicName = path,
                .Consumer = "mlp-consumer",
                .WaitTime = TDuration::Seconds(1),
                .ProcessingTimeout = TDuration::Minutes(5),
                .MaxNumberOfMessage = 1,
            });
            auto response = NPQ::NMLP::GetReadResponse(runtime, TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
            if (response->Messages.empty()) {
                continue;
            }
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
            const auto& message = response->Messages.front();
            UNIT_ASSERT_VALUES_EQUAL(message.MessageId.PartitionId, 0);
            UNIT_ASSERT_VALUES_EQUAL(message.MessageId.Offset, first + received);
            UNIT_ASSERT_VALUES_EQUAL(message.Data, "message-" + ToString(first + received) + payloadSuffix);
            ++received;
        }
        UNIT_ASSERT_VALUES_EQUAL_C(received, count, path);
    }

    void CommitMLP(const TString& path, const TVector<ui64>& offsets) {
        NPQ::NMLP::TCommitterSettings settings{
            .DatabasePath = Setup.GetDatabase(),
            .TopicName = path,
            .Consumer = "mlp-consumer",
        };
        for (auto offset : offsets) {
            settings.Messages.push_back({0, offset});
        }
        auto& runtime = Setup.GetRuntime();
        NPQ::NMLP::CreateCommitterActor(runtime, std::move(settings));
        auto response = NPQ::NMLP::GetChangeResponse(runtime, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), offsets.size());
        for (const auto& message : response->Messages) {
            UNIT_ASSERT_C(message.Status == NPQ::NMLP::EOperationResult::Success, message.MessageId.Offset);
        }
    }

    void Read(const TString& path, ui64 first, ui64 count, ui64 endOffset) {
        using TEvent = NYdb::NTopic::TReadSessionEvent;
        auto reader = Client.CreateReadSession(NYdb::NTopic::TReadSessionSettings()
            .ConsumerName("reader").AppendTopics(NYdb::NTopic::TTopicReadSettings(path)));
        const auto deadline = TInstant::Now() + TDuration::Seconds(60);
        ui64 received = 0;
        ui64 committed = first;
        bool started = false;
        while ((received < count || committed < first + count) && TInstant::Now() < deadline) {
            if (!reader->WaitEvent().Wait(TDuration::Seconds(1))) {
                continue;
            }
            // Deliver one message at a time to stop at the requested offset.
            auto event = reader->GetEvent(false, 1);
            if (!event) {
                continue;
            }
            if (auto* start = std::get_if<TEvent::TStartPartitionSessionEvent>(&*event)) {
                UNIT_ASSERT_VALUES_EQUAL(start->GetCommittedOffset(), first);
                UNIT_ASSERT_VALUES_EQUAL(start->GetEndOffset(), endOffset);
                started = true;
                start->Confirm(std::nullopt, std::nullopt, first + count - 1);
            } else if (auto* data = std::get_if<TEvent::TDataReceivedEvent>(&*event)) {
                for (auto& message : data->GetMessages()) {
                    UNIT_ASSERT_C(received < count, "Read more messages than requested");
                    UNIT_ASSERT_VALUES_EQUAL(message.GetOffset(), first + received);
                    UNIT_ASSERT_VALUES_EQUAL(message.GetData(), "message-" + ToString(first + received));
                    message.Commit();
                    ++received;
                }
            } else if (auto* ack = std::get_if<TEvent::TCommitOffsetAcknowledgementEvent>(&*event)) {
                committed = ack->GetCommittedOffset();
            } else if (auto* stop = std::get_if<TEvent::TStopPartitionSessionEvent>(&*event)) {
                stop->Confirm();
            } else if (auto* closed = std::get_if<NYdb::NTopic::TSessionClosedEvent>(&*event)) {
                UNIT_FAIL(closed->DebugString());
            }
        }
        UNIT_ASSERT(started);
        UNIT_ASSERT_VALUES_EQUAL_C(received, count, path);
        UNIT_ASSERT_VALUES_EQUAL(committed, first + count);
        UNIT_ASSERT(reader->Close(TDuration::Seconds(10)));
        // DescribeConsumer statistics may lag behind the commit acknowledgement.
        ui64 describedOffset = first;
        while (TInstant::Now() < deadline) {
            const auto description = Client.DescribeConsumer(path, "reader",
                NYdb::NTopic::TDescribeConsumerSettings().IncludeStats(true)).GetValueSync();
            UNIT_ASSERT_C(description.IsSuccess(), description.GetIssues().ToString());
            const auto& partitions = description.GetConsumerDescription().GetPartitions();
            UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);
            UNIT_ASSERT(partitions.front().GetPartitionConsumerStats());
            describedOffset = partitions.front().GetPartitionConsumerStats()->GetCommittedOffset();
            if (describedOffset == first + count) {
                break;
            }
            Sleep(TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT_VALUES_EQUAL(describedOffset, first + count);
    }

private:
    void InitContext(grpc::ClientContext& context) {
        context.AddMetadata("x-ydb-database", Setup.GetDatabase());
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
    }

    void AssertSuccess(const Ydb::Operations::Operation& operation) {
        UNIT_ASSERT_C(operation.ready(), operation.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(operation.status(), Ydb::StatusIds::SUCCESS, operation.ShortDebugString());
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TTopicReplication) {
    Y_UNIT_TEST(WriteWithDisabledReplicationAndRetainedMirrorRule) {
        TTopicReplicationEnv env;
        auto& runtime = env.Setup.GetRuntime();
        for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
            runtime.GetAppData(node).FeatureFlags.SetEnableDstTopicReplicationDisabled(true);
        }

        const TString source = "/Root/disabled_replication_source";
        const TString destination = "/Root/disabled_replication_destination";
        env.Create(source, true);
        env.Create(destination, false);
        env.Write(source, 0, 3);
        env.Commit(source, 2);
        env.EnableMirroring(source, destination);

        env.Write(destination, 0, 1);
        env.Read(destination, 0, 1, 1);
        const auto settings = env.Describe(destination);
        UNIT_ASSERT(settings.has_remote_mirror_rule());
        UNIT_ASSERT_VALUES_EQUAL(settings.attributes().at("_ydb_mirror_consumer_offsets"), "true");

        env.Setup.GetServer().KillTopicPqTablets(destination);
        // Replaying the producer sequence after restart must still be deduplicated.
        env.Write(destination, 0, 2);
        env.Read(destination, 1, 1, 2);
        env.WaitForOffsets(destination, 2, 2);
    }

    Y_UNIT_TEST(RejectInvalidReplicationAttributes) {
        TTopicReplicationEnv env;
        const TString path = "/Root/replication_attributes";
        env.Create(path, false);
        const auto original = env.Describe(path);
        for (const auto& [name, value] : TVector<std::pair<TString, TString>>{
                 {"_ydb_replication_consumer", "unknown"},
                 {"_ydb_mirror_consumer_offsets", "not-a-bool"},
                 {"_ydb_mirror_consumer_offsets", "true"}}) {
            auto settings = original;
            (*settings.mutable_attributes())[name] = value;
            env.Alter(path, settings, Ydb::StatusIds::BAD_REQUEST);
        }
    }

    Y_UNIT_TEST(ReplicateInitialAndUpdatedConsumerOffsets) {
        TTopicReplicationEnv env;
        const TString source = "/Root/replication_source";
        const TString destination = "/Root/replication_destination";
        env.Create(source, true);
        env.Create(destination, false);
        UNIT_ASSERT_VALUES_EQUAL(env.Describe(source).attributes().at("_ydb_replication_consumer"), "replicator");
        env.Write(source, 0, 1000);
        env.Read(source, 0, 500, 1000);

        env.EnableMirroring(source, destination);
        UNIT_ASSERT_VALUES_EQUAL(env.Describe(destination).attributes().at("_ydb_mirror_consumer_offsets"), "true");

        env.WaitForOffsets(destination, 500, 1000);
        env.Read(source, 500, 100, 1000);
        env.WaitForOffsets(destination, 600, 1000);

        auto settings = env.Describe(destination);
        UNIT_ASSERT(settings.has_remote_mirror_rule());
        settings.clear_remote_mirror_rule();
        settings.mutable_attributes()->erase("_ydb_mirror_consumer_offsets");
        env.Alter(destination, settings);

        env.Write(destination, 1000, 100);
        env.Read(destination, 600, 500, 1100);
    }

    Y_UNIT_TEST(ReplicateOffsetsWithoutMessages) {
        constexpr ui64 sourceStartOffset = 300000000000;
        constexpr ui64 sourceEndOffset = sourceStartOffset + 3;
        constexpr ui64 consumerOffset = sourceStartOffset + 2;
        auto settings = NYdb::NTopic::NTests::TTopicSdkTestSetup::MakeServerSettings();
        settings.PQConfig.SetMinOffset(sourceStartOffset);
        settings.PQConfig.MutableMirrorConfig()->SetRewindCommitDelaySeconds(0);
        NKikimrConfig::TFeatureFlags flags;
        flags.SetEnableSkipMessagesWithObsoleteTimestamp(true);
        settings.SetFeatureFlags(flags);
        TTopicReplicationEnv env(settings);
        const TString source = "/Root/old_replication_source";
        const TString destination = "/Root/empty_replication_destination";
        env.Create(source, true);
        env.Create(destination, false);
        env.Write(source, sourceStartOffset, 3);
        // Verify that the source actually contains readable messages. StoreSizeBytes
        // is a metering value and can be zero for a small nonempty partition.
        env.Commit(source, sourceStartOffset);
        env.Read(source, sourceStartOffset, 2, sourceEndOffset);
        env.WaitForOffsets(source, consumerOffset, sourceEndOffset, "reader", sourceStartOffset);
        env.WaitForOffsets(destination, 0, 0, "reader", 0);

        // All existing writes precede the mirror's read timestamp. This models
        // an idle source without waiting for messages to age in wall-clock time.
        // No messages are written after mirroring starts.
        env.EnableMirroring(source, destination, TInstant::Now() + TDuration::Seconds(1));
        env.WaitForOffsets(destination, consumerOffset, sourceEndOffset, "reader", sourceEndOffset);
        env.AssertEmpty(destination, consumerOffset, sourceEndOffset);

        // Consumer offsets must keep replicating even though no data was mirrored.
        env.Commit(source, sourceEndOffset);
        env.WaitForOffsets(destination, sourceEndOffset, sourceEndOffset, "reader", sourceEndOffset);

        auto destinationSettings = env.Describe(destination);
        destinationSettings.clear_remote_mirror_rule();
        destinationSettings.mutable_attributes()->erase("_ydb_mirror_consumer_offsets");
        env.Alter(destination, destinationSettings);
        env.Setup.GetServer().KillTopicPqTablets(destination);
        env.WaitForOffsets(destination, sourceEndOffset, sourceEndOffset, "reader", sourceEndOffset);
        env.AssertEmpty(destination, sourceEndOffset, sourceEndOffset);
    }

    void TestReplicateInitialAndUpdatedMLPConsumerOffsets(bool largeMessages) {
        TTopicReplicationEnv env;
        const TString source = "/Root/mlp_replication_source";
        const TString destination = "/Root/mlp_replication_destination";
        const TString payloadSuffix = largeMessages ? TString(600 * 1024, 'x') : TString{};
        env.Create(source, true);
        env.Create(destination, false);
        env.AddMLPConsumer(source);
        env.AddMLPConsumer(destination);
        const auto sourceSettings = env.Describe(source);
        const auto replicationConsumer = sourceSettings.attributes().find("_ydb_replication_consumer");
        UNIT_ASSERT_C(replicationConsumer != sourceSettings.attributes().end(),
            "Adding an MLP consumer must preserve the replication consumer type");
        UNIT_ASSERT_VALUES_EQUAL(replicationConsumer->second, "replicator");
        env.Write(source, 0, 10, payloadSuffix);

        // An acknowledgement beyond a gap must not advance the committed prefix.
        env.ReadMLP(source, 0, 5, payloadSuffix);
        env.CommitMLP(source, {0, 1, 2, 4});
        env.WaitForOffsets(source, 3, 10, "mlp-consumer");

        // The first replication session delivers the existing MLP offset.
        env.EnableMirroring(source, destination);
        env.WaitForOffsets(destination, 3, 10, "mlp-consumer");

        // Closing the gap advances the mirror without writing any new messages.
        env.CommitMLP(source, {3});
        env.WaitForOffsets(source, 5, 10, "mlp-consumer");
        env.WaitForOffsets(destination, 5, 10, "mlp-consumer");

        auto settings = env.Describe(destination);
        settings.clear_remote_mirror_rule();
        settings.mutable_attributes()->erase("_ydb_mirror_consumer_offsets");
        env.Alter(destination, settings);

        // Restart without a source that could reapply the offset: the MLP state
        // itself must be durable, and the uncommitted suffix must remain readable.
        env.Setup.GetServer().KillTopicPqTablets(destination);
        env.WaitForOffsets(destination, 5, 10, "mlp-consumer");
        env.ReadMLP(destination, 5, 5, payloadSuffix);
        env.CommitMLP(destination, {5, 6, 7, 8, 9});
        env.WaitForOffsets(destination, 10, 10, "mlp-consumer");
    }

    Y_UNIT_TEST(ReplicateInitialAndUpdatedMLPConsumerOffsets) {
        TestReplicateInitialAndUpdatedMLPConsumerOffsets(false);
    }

    Y_UNIT_TEST(ReplicateInitialAndUpdatedMLPConsumerOffsetsWithLargeMessages) {
        TestReplicateInitialAndUpdatedMLPConsumerOffsets(true);
    }

    Y_UNIT_TEST(ConsumerOffsetsSurviveSourceTabletRestart) {
        TTopicReplicationEnv env;
        const TString source = "/Root/replication_source";
        const TString destination = "/Root/replication_destination";
        env.Create(source, true);
        env.Create(destination, false);
        env.Write(source, 0, 100);
        env.Commit(source, 20);
        env.EnableMirroring(source, destination);
        env.WaitForOffsets(destination, 20, 100);

        const auto sourceBefore = env.DescribePartition(source, "replicator");
        const auto destinationBefore = env.DescribePartition(destination, "reader");
        UNIT_ASSERT(sourceBefore.GetPartitionConsumerStats());
        UNIT_ASSERT(sourceBefore.GetPartitionLocation());
        UNIT_ASSERT(destinationBefore.GetPartitionLocation());
        const auto readSessionId = sourceBefore.GetPartitionConsumerStats()->GetReadSessionId();
        UNIT_ASSERT_C(!readSessionId.empty(), "Mirrorer must have an active source read session before the restart");
        const auto sourceGeneration = sourceBefore.GetPartitionLocation()->GetGeneration();
        const auto destinationGeneration = destinationBefore.GetPartitionLocation()->GetGeneration();

        // Keep the frontend and mirror alive, restarting only the source PQ tablet.
        env.Setup.GetServer().KillTopicPqTablets(source);

        // No new messages: restoring delivery must also work through HasDataInfo.
        env.Commit(source, 40);
        env.WaitForOffsets(destination, 40, 100);

        auto checkSessionsAndGenerations = [&] {
            const auto sourceAfter = env.DescribePartition(source, "replicator");
            const auto destinationAfter = env.DescribePartition(destination, "reader");
            UNIT_ASSERT(sourceAfter.GetPartitionConsumerStats());
            UNIT_ASSERT(sourceAfter.GetPartitionLocation());
            UNIT_ASSERT(destinationAfter.GetPartitionLocation());
            UNIT_ASSERT_GT(sourceAfter.GetPartitionLocation()->GetGeneration(), sourceGeneration);
            UNIT_ASSERT_VALUES_EQUAL_C(sourceAfter.GetPartitionConsumerStats()->GetReadSessionId(), readSessionId,
                "Replication must recover without recreating the source read session");
            UNIT_ASSERT_VALUES_EQUAL_C(destinationAfter.GetPartitionLocation()->GetGeneration(), destinationGeneration,
                "Replication must recover without restarting the mirror tablet");
        };
        checkSessionsAndGenerations();

        // Ordinary messages and subsequent consumer commits must keep progressing.
        env.Write(source, 100, 20);
        env.Commit(source, 60);
        env.WaitForOffsets(destination, 60, 120);
        checkSessionsAndGenerations();
    }
}

} // namespace NKikimr::NPersQueueTests

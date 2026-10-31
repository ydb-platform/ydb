#include <ydb/core/persqueue/common/proxy/actor_persqueue_client_iface.h>
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

    TTopicReplicationEnv()
        : Setup("TopicReplication", NYdb::NTopic::NTests::TTopicSdkTestSetup::MakeServerSettings(), false)
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

    void Write(const TString& path, ui64 first, ui64 count) {
        auto writer = Client.CreateSimpleBlockingWriteSession(NYdb::NTopic::TWriteSessionSettings()
            .Path(path).ProducerId("producer").MessageGroupId("producer").Codec(NYdb::NTopic::ECodec::RAW));
        for (ui64 offset = first; offset < first + count; ++offset) {
            UNIT_ASSERT_C(writer->Write("message-" + ToString(offset), offset + 1, std::nullopt, TDuration::Seconds(10)), offset);
        }
        UNIT_ASSERT_C(writer->Close(TDuration::Seconds(30)), path);
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
            // Deliver one message at a time, so the source stops at exactly 500.
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
    Y_UNIT_TEST(RejectInvalidReplicationAttributes) {
        TTopicReplicationEnv env;
        const TString path = "/Root/replication_attributes";
        env.Create(path, false);
        const auto original = env.Describe(path);
        for (const auto& [name, value] : TVector<std::pair<TString, TString>>{
                 {"_ydb_replication_consumer", "unknown"},
                 {"_ydb_is_for_topic_replication", "not-a-bool"},
                 {"_ydb_is_for_topic_replication", "true"}}) {
            auto settings = original;
            (*settings.mutable_attributes())[name] = value;
            env.Alter(path, settings, Ydb::StatusIds::BAD_REQUEST);
        }
        UNIT_ASSERT_VALUES_EQUAL(env.Describe(path).attributes().at("_ydb_topic_replication_status"), "0");
    }

    Y_UNIT_TEST(ContinueReadingAndWritingAfterReplicationDone) {
        TTopicReplicationEnv env;
        const TString source = "/Root/replication_source";
        const TString destination = "/Root/replication_destination";
        env.Create(source, true);
        env.Create(destination, false);
        UNIT_ASSERT_VALUES_EQUAL(env.Describe(source).attributes().at("_ydb_replication_consumer"), "replicator");
        UNIT_ASSERT_VALUES_EQUAL(env.Describe(destination).attributes().at("_ydb_topic_replication_status"), "0");
        env.Write(source, 0, 1000);

        auto settings = env.Describe(destination);
        auto* mirror = settings.mutable_remote_mirror_rule();
        mirror->set_endpoint(env.Setup.GetEndpoint());
        mirror->set_database(env.Setup.GetDatabase());
        mirror->set_topic_path(source);
        mirror->set_consumer_name("replicator");
        mirror->mutable_credentials()->set_oauth_token("");
        (*settings.mutable_attributes())["_ydb_is_for_topic_replication"] = "true";
        (*settings.mutable_attributes())["_ydb_start_replication_in_dest_topic"] = "true";
        env.Alter(destination, settings);
        UNIT_ASSERT_VALUES_EQUAL(env.Describe(destination).attributes().at("_ydb_is_for_topic_replication"), "true");

        env.Read(source, 0, 500, 1000);
        settings = env.Describe(source);
        (*settings.mutable_attributes())["_ydb_finish_replication_in_src_topic"] = "true";
        env.Alter(source, settings);

        const auto deadline = TInstant::Now() + TDuration::Seconds(90);
        TString status;
        while (TInstant::Now() < deadline) {
            status = env.Describe(destination).attributes().at("_ydb_topic_replication_status");
            if (status == "3") {
                break;
            }
            UNIT_ASSERT_VALUES_EQUAL(status, "1");
            Sleep(TDuration::MilliSeconds(200));
        }
        UNIT_ASSERT_VALUES_EQUAL_C(status, "3", "Timed out waiting for ReplicationDone through PQv1 DescribeTopic");

        settings = env.Describe(destination);
        UNIT_ASSERT(settings.has_remote_mirror_rule());
        settings.clear_remote_mirror_rule();
        settings.mutable_attributes()->erase("_ydb_is_for_topic_replication");
        env.Alter(destination, settings);

        env.Write(destination, 1000, 100);
        env.Read(destination, 500, 600, 1100);
    }
}

} // namespace NKikimr::NPersQueueTests

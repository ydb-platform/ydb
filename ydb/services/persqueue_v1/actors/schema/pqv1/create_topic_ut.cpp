#include "actors.h"

#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/testlib/grpc_request/grpc_request.h>
#include <ydb/public/api/protos/ydb_persqueue_v1.pb.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

namespace NKikimr::NGRpcProxy::V1::NPQv1 {

using namespace NYdb::NTopic::NTests;
using namespace NKikimr::Tests::NGrpc;

std::shared_ptr<TTopicSdkTestSetup> CreateSetup() {
    auto setup = std::make_shared<TTopicSdkTestSetup>("PQv1");
    setup->GetServer().EnableLogs({
            NKikimrServices::PQ_SCHEMA,
            NKikimrServices::PQ_MLP_DESCRIBER,
        },
        NActors::NLog::PRI_DEBUG
    );
    setup->GetServer().EnableLogs({
            NKikimrServices::PERSQUEUE,
            NKikimrServices::PERSQUEUE_READ_BALANCER,
            NKikimrServices::PQ_WRITE_PROXY
        },
        NActors::NLog::PRI_INFO
    );

    return setup;
}

template<typename TRequest, typename TResponse>
std::shared_ptr<TResultHolder<TResponse>> DoRequest(NActors::TTestActorRuntime& runtime, const TRequest& request, TString path = "/Root/test_db/topic1", TString database = "/Root/test_db") {
    auto result = std::make_shared<TResultHolder<TResponse>>();
    auto edgeActor = runtime.AllocateEdgeActor();

    auto ctx = new TRequestCtx<TRequest, TResponse>(
        request,
        path,
        database,
        result,
        edgeActor
    );
    runtime.Register(CreateCreateTopicActor(ctx));

    runtime.GrabEdgeEvent<NActors::TEvents::TEvWakeup>(edgeActor, TDuration::Seconds(10));

    UNIT_ASSERT_C(result->ResultStatus, "The operation is still in progress");
    return result;
}

void CreateDlqTopic(
    NActors::TTestActorRuntime& runtime,
    const TString& dlqTopicPath,
    const TString& database = "/Root/test_db"
) {
    Ydb::PersQueue::V1::CreateTopicRequest request;
    request.set_path(dlqTopicPath);

    auto& settings = *request.mutable_settings();
    settings.set_partitions_count(1);
    settings.set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
    settings.set_retention_period_ms(TDuration::Days(1).MilliSeconds());
    settings.mutable_attributes()->insert({"_federation_account", "account1"});

    auto result = DoRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(
        runtime,
        request,
        dlqTopicPath,
        database
    );
    UNIT_ASSERT(result->ResultStatus);
    UNIT_ASSERT_VALUES_EQUAL_C(*result->ResultStatus, Ydb::StatusIds::SUCCESS, result->Issues.ToString());
}

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(CreateTopic_PQv1API) {

Y_UNIT_TEST(SharedConsumer) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);

    CreateDlqTopic(runtime, "/Root/test_db/test_dead_letter_queue");

    Ydb::PersQueue::V1::CreateTopicRequest request;
    request.set_path("/Root/test_db/topic1");

    auto& settings = *request.mutable_settings();
    settings.set_partitions_count(1);
    settings.set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
    settings.set_retention_period_ms(TDuration::Days(1).MilliSeconds());
    settings.set_max_partition_write_messages_speed(100000);
    settings.set_max_partition_write_messages_burst(50000);

    settings.mutable_attributes()->insert({"_federation_account", "account1"});

    auto& readRule = *settings.add_read_rules();

    readRule.set_consumer_name("test_consumer");
    readRule.set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
    readRule.set_version(1);
    //readRule.set_service_type("test_service_type");
    readRule.set_starting_message_timestamp_ms(1000);

    auto& type = *readRule.mutable_shared_consumer_type();
    type.set_keep_messages_order(true);
    type.mutable_default_processing_timeout()->set_seconds(3);
    type.mutable_receive_message_wait_time()->set_seconds(5);
    type.mutable_receive_message_delay()->set_seconds(7);
    type.mutable_dead_letter_policy()->set_enabled(true);
    type.mutable_dead_letter_policy()->mutable_condition()->set_max_processing_attempts(11);
    type.mutable_dead_letter_policy()->mutable_move_action()->set_dead_letter_queue("test_dead_letter_queue");

    auto result = DoRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(runtime, request);

    auto status = result->ResultStatus;
    UNIT_ASSERT(status);
    UNIT_ASSERT_VALUES_EQUAL_C(*status, Ydb::StatusIds::SUCCESS, result->Issues.ToString());

    runtime.Register(NPQ::NDescriber::CreateDescriberActor(runtime.AllocateEdgeActor(), "/Root/test_db", {"/Root/test_db/topic1"}));
    auto response = runtime.GrabEdgeEvent<NPQ::NDescriber::TEvDescribeTopicsResponse>(TDuration::Seconds(5));

    UNIT_ASSERT_VALUES_EQUAL(response->Topics.size(), 1);
    auto topic = response->Topics.begin()->second;
    UNIT_ASSERT_VALUES_EQUAL(topic.Status, NPQ::NDescriber::EStatus::Success);

    auto config = topic.Info->Description.GetPQTabletConfig();
    const auto* consumer = NPQ::GetConsumer(config, "test_consumer");
    UNIT_ASSERT(consumer);
    UNIT_ASSERT_VALUES_EQUAL(consumer->GetImportant(), false);
    UNIT_ASSERT_VALUES_EQUAL(NKikimrPQ::TPQTabletConfig::EConsumerType_Name(consumer->GetType()),
        ::NKikimrPQ::TPQTabletConfig::EConsumerType_Name(::NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP));
    UNIT_ASSERT_VALUES_EQUAL(consumer->GetKeepMessageOrder(), true);
    UNIT_ASSERT_VALUES_EQUAL(consumer->GetDefaultProcessingTimeoutSeconds(), 3);
    UNIT_ASSERT_VALUES_EQUAL(consumer->GetDefaultReceiveMessageWaitTimeMs(), 5000);
    UNIT_ASSERT_VALUES_EQUAL(consumer->GetDefaultDelayMessageTimeMs(), 7000);
    UNIT_ASSERT_VALUES_EQUAL(NKikimrPQ::TPQTabletConfig::EDeadLetterPolicy_Name(consumer->GetDeadLetterPolicy()),
        NKikimrPQ::TPQTabletConfig::EDeadLetterPolicy_Name(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE));
    UNIT_ASSERT_VALUES_EQUAL(consumer->GetMaxProcessingAttempts(), 11);
    UNIT_ASSERT_VALUES_EQUAL(consumer->GetDeadLetterQueue(), "test_dead_letter_queue");

    UNIT_ASSERT_VALUES_EQUAL(config.GetPartitionConfig().GetWriteSpeedInMessagesPerSecond(), 100000);
    UNIT_ASSERT_VALUES_EQUAL(config.GetPartitionConfig().GetBurstSizeInMessages(), 50000);
}

Y_UNIT_TEST(MessageWriteBurstDefaultsToSpeed) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);

    Ydb::PersQueue::V1::CreateTopicRequest request;
    request.set_path("/Root/test_db/topic1");

    auto& settings = *request.mutable_settings();
    settings.set_partitions_count(1);
    settings.set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
    settings.set_retention_period_ms(TDuration::Days(1).MilliSeconds());
    settings.set_max_partition_write_messages_speed(777);

    settings.mutable_attributes()->insert({"_federation_account", "account1"});

    auto result = DoRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(runtime, request);

    auto status = result->ResultStatus;
    UNIT_ASSERT(status);
    UNIT_ASSERT_VALUES_EQUAL_C(*status, Ydb::StatusIds::SUCCESS, result->Issues.ToString());

    runtime.Register(NPQ::NDescriber::CreateDescriberActor(runtime.AllocateEdgeActor(), "/Root/test_db", {"/Root/test_db/topic1"}));
    auto response = runtime.GrabEdgeEvent<NPQ::NDescriber::TEvDescribeTopicsResponse>(TDuration::Seconds(5));

    UNIT_ASSERT_VALUES_EQUAL(response->Topics.size(), 1);
    auto topic = response->Topics.begin()->second;
    UNIT_ASSERT_VALUES_EQUAL(topic.Status, NPQ::NDescriber::EStatus::Success);

    const auto& partitionConfig = topic.Info->Description.GetPQTabletConfig().GetPartitionConfig();
    UNIT_ASSERT_VALUES_EQUAL(partitionConfig.GetWriteSpeedInMessagesPerSecond(), 777);
    UNIT_ASSERT_VALUES_EQUAL(partitionConfig.GetBurstSizeInMessages(), 777);
}

Y_UNIT_TEST(RejectsTooManyPartitions) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);

    Ydb::PersQueue::V1::CreateTopicRequest request;
    request.set_path("/Root/test_db/topic_too_many_parts");

    auto& settings = *request.mutable_settings();
    settings.set_partitions_count(static_cast<i32>(NPQ::MAX_TOPIC_PARTITIONS + 1));
    settings.set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
    settings.set_retention_period_ms(TDuration::Days(1).MilliSeconds());

    auto result = DoRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(
        runtime,
        request,
        "/Root/test_db/topic_too_many_parts"
    );

    auto status = result->ResultStatus;
    UNIT_ASSERT(status);
    UNIT_ASSERT_VALUES_EQUAL_C(*status, Ydb::StatusIds::BAD_REQUEST, result->Issues.ToString());
    UNIT_ASSERT_STRING_CONTAINS(result->Issues.ToString(), "less than");
}

Y_UNIT_TEST(CreateTopicWithNameEqDB) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);

    Ydb::PersQueue::V1::CreateTopicRequest request;
    request.set_path("/Root");

    auto& settings = *request.mutable_settings();
    settings.set_partitions_count(1);
    settings.set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
    settings.set_retention_period_ms(TDuration::Days(1).MilliSeconds());

    auto result = DoRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(runtime, request, "/Root", "/Root");

    auto status = result->ResultStatus;
    UNIT_ASSERT(status);
    UNIT_ASSERT_VALUES_EQUAL_C(*status, Ydb::StatusIds::SCHEME_ERROR, result->Issues.ToString());
}

void EnableFederation(NActors::TTestActorRuntime& runtime) {
    runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);
    runtime.GetAppData().PQConfig.SetRoot("/Root/PQ");
}

void FillBaseCreateSettings(Ydb::PersQueue::V1::TopicSettings& settings) {
    settings.set_partitions_count(1);
    settings.set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
    settings.set_retention_period_ms(TDuration::Days(1).MilliSeconds());
}

void FillRemoteMirrorRule(Ydb::PersQueue::V1::TopicSettings& settings) {
    auto* rmr = settings.mutable_remote_mirror_rule();
    rmr->set_endpoint("sas.logbroker.yandex.net:2135");
    rmr->set_topic_path("account/topic");
    rmr->set_consumer_name("shared/mirror-from-dc2-to-dc1");
    rmr->mutable_credentials()->set_oauth_token("oauth-token");
}

NKikimrPQ::TPQTabletConfig DescribePqTabletConfig(
    NActors::TTestActorRuntime& runtime,
    const TString& path,
    const TString& database
) {
    runtime.Register(NPQ::NDescriber::CreateDescriberActor(runtime.AllocateEdgeActor(), database, {path}));
    auto response = runtime.GrabEdgeEvent<NPQ::NDescriber::TEvDescribeTopicsResponse>(TDuration::Seconds(5));

    UNIT_ASSERT_VALUES_EQUAL(response->Topics.size(), 1);
    auto topic = response->Topics.begin()->second;
    UNIT_ASSERT_VALUES_EQUAL_C(topic.Status, NPQ::NDescriber::EStatus::Success, NPQ::NDescriber::Description(path, topic.Status));
    UNIT_ASSERT(topic.Info);

    return topic.Info->Description.GetPQTabletConfig();
}

Y_UNIT_TEST(FederationRemoteCopyWithRemoteMirrorRule) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    EnableFederation(runtime);

    const TString path = "/Root/PQ/rt3.dc2--account--remote-copy";
    const TString database = "/Root";

    Ydb::PersQueue::V1::CreateTopicRequest request;
    request.set_path(path);

    auto& settings = *request.mutable_settings();
    FillBaseCreateSettings(settings);
    settings.set_client_write_disabled(true);
    FillRemoteMirrorRule(settings);

    auto result = DoRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(
        runtime,
        request,
        path,
        database
    );

    auto status = result->ResultStatus;
    UNIT_ASSERT(status);
    UNIT_ASSERT_VALUES_EQUAL_C(*status, Ydb::StatusIds::SUCCESS, result->Issues.ToString());
    UNIT_ASSERT(!result->Issues.ToString().Contains("Local cluster is not correct"));

    const auto config = DescribePqTabletConfig(runtime, path, database);
    UNIT_ASSERT_VALUES_EQUAL(config.GetLocalDC(), false);
    UNIT_ASSERT_VALUES_EQUAL(config.GetDC(), "dc2");
    UNIT_ASSERT(config.GetPartitionConfig().HasMirrorFrom());
}

Y_UNIT_TEST(FederationLocalDcMirrorWithRemoteMirrorRule) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    EnableFederation(runtime);

    const TString path = "/Root/PQ/rt3.dc1--account--local-mirror";
    const TString database = "/Root";

    Ydb::PersQueue::V1::CreateTopicRequest request;
    request.set_path(path);

    auto& settings = *request.mutable_settings();
    FillBaseCreateSettings(settings);
    FillRemoteMirrorRule(settings);

    auto result = DoRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(
        runtime,
        request,
        path,
        database
    );

    auto status = result->ResultStatus;
    UNIT_ASSERT(status);
    UNIT_ASSERT_VALUES_EQUAL_C(*status, Ydb::StatusIds::SUCCESS, result->Issues.ToString());

    const auto config = DescribePqTabletConfig(runtime, path, database);
    UNIT_ASSERT_VALUES_EQUAL(config.GetLocalDC(), true);
    UNIT_ASSERT_VALUES_EQUAL(config.GetDC(), "dc1");
    UNIT_ASSERT(config.GetPartitionConfig().HasMirrorFrom());
}

Y_UNIT_TEST(ContentBasedDeduplication) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);

    Ydb::PersQueue::V1::CreateTopicRequest request;
    request.set_path("/Root/test_db/topic1");

    auto& settings = *request.mutable_settings();
    settings.set_partitions_count(1);
    settings.set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
    settings.set_retention_period_ms(TDuration::Days(1).MilliSeconds());
    settings.set_content_based_deduplication(true);

    auto result = DoRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(runtime, request);

    auto status = result->ResultStatus;
    UNIT_ASSERT(status);
    UNIT_ASSERT_VALUES_EQUAL_C(*status, Ydb::StatusIds::SUCCESS, result->Issues.ToString());

    runtime.Register(NPQ::NDescriber::CreateDescriberActor(runtime.AllocateEdgeActor(), "/Root/test_db", {"/Root/test_db/topic1"}));
    auto response = runtime.GrabEdgeEvent<NPQ::NDescriber::TEvDescribeTopicsResponse>(TDuration::Seconds(5));

    UNIT_ASSERT_VALUES_EQUAL(response->Topics.size(), 1);
    auto topic = response->Topics.begin()->second;
    UNIT_ASSERT_VALUES_EQUAL(topic.Status, NPQ::NDescriber::EStatus::Success);

    auto config = topic.Info->Description.GetPQTabletConfig();
    UNIT_ASSERT_VALUES_EQUAL(config.GetContentBasedDeduplication(), true);
}

};

} // namespace NKikimr::NGRpcProxy::V1::NPQv1

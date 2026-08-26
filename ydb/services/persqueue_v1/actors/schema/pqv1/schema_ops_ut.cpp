#include "actors.h"

#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/persqueue/public/utils.h>
#include <ydb/core/testlib/grpc_request/grpc_request.h>
#include <ydb/public/api/protos/ydb_persqueue_v1.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/size_literals.h>
#include <util/generic/vector.h>

namespace NKikimr::NGRpcProxy::V1::NPQv1 {

using namespace NYdb::NTopic::NTests;
using namespace NKikimr::Tests::NGrpc;

namespace {

std::shared_ptr<TTopicSdkTestSetup> CreateSetup(const char* name = "PQv1SchemaOps") {
    auto setup = std::make_shared<TTopicSdkTestSetup>(name, TTopicSdkTestSetup::MakeServerSettings(), false);
    setup->GetServer().EnableLogs({
            NKikimrServices::PQ_SCHEMA,
            NKikimrServices::PQ_MLP_DESCRIBER,
        },
        NActors::NLog::PRI_DEBUG);
    return setup;
}

template <typename TRequest, typename TResponse>
std::shared_ptr<TResultHolder<TResponse>> DoActorRequest(
    NActors::TTestActorRuntime& runtime,
    const TRequest& request,
    NActors::IActor* (*factory)(NGRpcService::IRequestOpCtx*),
    const TString& path,
    const TString& database = "/Root")
{
    auto result = std::make_shared<TResultHolder<TResponse>>();
    auto edgeActor = runtime.AllocateEdgeActor();
    auto* ctx = new TRequestCtx<TRequest, TResponse>(request, path, database, result, edgeActor);
    runtime.Register(factory(ctx));
    runtime.GrabEdgeEvent<NActors::TEvents::TEvWakeup>(edgeActor, TDuration::Seconds(10));
    UNIT_ASSERT_C(result->ResultStatus, "The operation is still in progress");
    return result;
}

Ydb::PersQueue::V1::CreateTopicRequest MakeCreateTopicRequest(const TString& path, ui32 partitions = 1) {
    Ydb::PersQueue::V1::CreateTopicRequest request;
    request.set_path(path);
    auto& settings = *request.mutable_settings();
    settings.set_partitions_count(partitions);
    settings.set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
    settings.set_retention_period_ms(TDuration::Days(1).MilliSeconds());
    return request;
}

Ydb::PersQueue::V1::AlterTopicRequest MakeAlterTopicRequest(const TString& path, ui32 partitions = 1) {
    Ydb::PersQueue::V1::AlterTopicRequest request;
    request.set_path(path);
    *request.mutable_settings() = MakeCreateTopicRequest(path, partitions).settings();
    return request;
}

Ydb::PersQueue::V1::TopicSettings::ReadRule MakeStreamingReadRule(const TString& consumerName, i32 version = 1) {
    Ydb::PersQueue::V1::TopicSettings::ReadRule rule;
    rule.set_consumer_name(consumerName);
    rule.set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
    rule.set_version(version);
    rule.mutable_streaming_consumer_type();
    return rule;
}

Ydb::PersQueue::V1::TopicSettings::ReadRule MakeSharedReadRule(const TString& consumerName) {
    Ydb::PersQueue::V1::TopicSettings::ReadRule rule;
    rule.set_consumer_name(consumerName);
    rule.set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
    rule.set_version(1);
    rule.mutable_shared_consumer_type()->set_keep_messages_order(true);
    return rule;
}

template <typename TResponse>
void AssertStatus(
    const std::shared_ptr<TResultHolder<TResponse>>& result,
    Ydb::StatusIds::StatusCode expected,
    const TString& substring = {})
{
    UNIT_ASSERT(result->ResultStatus);
    UNIT_ASSERT_VALUES_EQUAL_C(*result->ResultStatus, expected, result->Issues.ToString());
    if (!substring.empty()) {
        UNIT_ASSERT_STRING_CONTAINS(result->Issues.ToString(), substring);
    }
}

void ExecuteDDL(TTopicSdkTestSetup& setup, const TString& query) {
    NYdb::TDriver driver(setup.MakeDriverConfig());
    NYdb::NQuery::TQueryClient client(driver);
    auto session = client.GetSession().GetValueSync().GetSession();
    auto res = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
    UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
    driver.Stop(true);
}

void MkDir(TTopicSdkTestSetup& setup, const TString& parent, const TString& name) {
    setup.GetServer().AnnoyingClient->MkDir(parent, name);
}

void AssertDescribeAliases(
    NActors::TTestActorRuntime& runtime,
    const TVector<TString>& names,
    const TString& expectedRealPath,
    const TString& database = "/Root")
{
    for (const auto& name : names) {
        auto edge = runtime.AllocateEdgeActor();
        runtime.Register(NPQ::NDescriber::CreateDescriberActor(edge, database, {name}));
        auto response = runtime.GrabEdgeEvent<NPQ::NDescriber::TEvDescribeTopicsResponse>(TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL_C(response->Topics.size(), 1u, name);
        const auto it = response->Topics.find(name);
        UNIT_ASSERT_C(it != response->Topics.end(), name);
        UNIT_ASSERT_VALUES_EQUAL_C(it->second.Status, NPQ::NDescriber::EStatus::SUCCESS, name);
        UNIT_ASSERT_VALUES_EQUAL_C(it->second.RealPath, expectedRealPath, name);
    }
}

void CreateTopic(NActors::TTestActorRuntime& runtime, const TString& path, const TString& database = "/Root") {
    auto result = DoActorRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(
        runtime, MakeCreateTopicRequest(path), CreateCreateTopicActor, path, database);
    AssertStatus(result, Ydb::StatusIds::SUCCESS);
}

NKikimrPQ::TPQTabletConfig DescribeTabletConfig(
    NActors::TTestActorRuntime& runtime,
    const TString& path,
    const TString& database = "/Root")
{
    auto edge = runtime.AllocateEdgeActor();
    runtime.Register(NPQ::NDescriber::CreateDescriberActor(edge, database, {path}));
    auto response = runtime.GrabEdgeEvent<NPQ::NDescriber::TEvDescribeTopicsResponse>(TDuration::Seconds(5));
    UNIT_ASSERT_VALUES_EQUAL(response->Topics.size(), 1u);
    const auto& topic = response->Topics.begin()->second;
    UNIT_ASSERT_VALUES_EQUAL(topic.Status, NPQ::NDescriber::EStatus::SUCCESS);
    return topic.Info->Description.GetPQTabletConfig();
}

} // namespace

Y_UNIT_TEST_SUITE(SchemaOps_PQv1) {

Y_UNIT_TEST(DropTopicSuccessAndMissing) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_drop";

    CreateTopic(runtime, path);

    {
        Ydb::PersQueue::V1::DropTopicRequest request;
        request.set_path(path);
        auto result = DoActorRequest<Ydb::PersQueue::V1::DropTopicRequest, Ydb::PersQueue::V1::DropTopicResponse>(
            runtime, request, CreateDropTopicActor, path);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
    }

    {
        Ydb::PersQueue::V1::DropTopicRequest request;
        request.set_path(path);
        auto result = DoActorRequest<Ydb::PersQueue::V1::DropTopicRequest, Ydb::PersQueue::V1::DropTopicResponse>(
            runtime, request, CreateDropTopicActor, path);
        AssertStatus(result, Ydb::StatusIds::SCHEME_ERROR);
    }
}

Y_UNIT_TEST(AddAndRemoveReadRule) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_add_remove";
    CreateTopic(runtime, path);

    {
        Ydb::PersQueue::V1::AddReadRuleRequest request;
        request.set_path(path);
        *request.mutable_read_rule() = MakeStreamingReadRule("c1", /*version=*/0);
        auto result = DoActorRequest<Ydb::PersQueue::V1::AddReadRuleRequest, Ydb::PersQueue::V1::AddReadRuleResponse>(
            runtime, request, CreateAddConsumerActor, path);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
    }

    {
        Ydb::PersQueue::V1::AddReadRuleRequest request;
        request.set_path(path);
        *request.mutable_read_rule() = MakeStreamingReadRule("c1");
        auto result = DoActorRequest<Ydb::PersQueue::V1::AddReadRuleRequest, Ydb::PersQueue::V1::AddReadRuleResponse>(
            runtime, request, CreateAddConsumerActor, path);
        UNIT_ASSERT(result->ResultStatus);
        UNIT_ASSERT_C(
            *result->ResultStatus == Ydb::StatusIds::ALREADY_EXISTS
                || *result->ResultStatus == Ydb::StatusIds::BAD_REQUEST
                || *result->ResultStatus == Ydb::StatusIds::SCHEME_ERROR,
            result->Issues.ToString());
    }

    {
        Ydb::PersQueue::V1::RemoveReadRuleRequest request;
        request.set_path(path);
        request.set_consumer_name("c1");
        auto result = DoActorRequest<Ydb::PersQueue::V1::RemoveReadRuleRequest, Ydb::PersQueue::V1::RemoveReadRuleResponse>(
            runtime, request, CreateRemoveConsumerActor, path);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
    }

    {
        Ydb::PersQueue::V1::RemoveReadRuleRequest request;
        request.set_path(path);
        request.set_consumer_name("c1");
        auto result = DoActorRequest<Ydb::PersQueue::V1::RemoveReadRuleRequest, Ydb::PersQueue::V1::RemoveReadRuleResponse>(
            runtime, request, CreateRemoveConsumerActor, path);
        UNIT_ASSERT(result->ResultStatus);
        UNIT_ASSERT_C(
            *result->ResultStatus == Ydb::StatusIds::NOT_FOUND
                || *result->ResultStatus == Ydb::StatusIds::SCHEME_ERROR,
            result->Issues.ToString());
    }
}

Y_UNIT_TEST(AddSharedReadRuleAndRemove) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    runtime.GetAppData().FeatureFlags.SetEnableTopicMessageLevelParallelism(true);
    const TString path = "/Root/topic_shared_rr";
    CreateTopic(runtime, path);

    {
        Ydb::PersQueue::V1::AddReadRuleRequest request;
        request.set_path(path);
        *request.mutable_read_rule() = MakeSharedReadRule("shared_c1");
        auto result = DoActorRequest<Ydb::PersQueue::V1::AddReadRuleRequest, Ydb::PersQueue::V1::AddReadRuleResponse>(
            runtime, request, CreateAddConsumerActor, path);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
    }

    {
        Ydb::PersQueue::V1::RemoveReadRuleRequest request;
        request.set_path(path);
        request.set_consumer_name("shared_c1");
        auto result = DoActorRequest<Ydb::PersQueue::V1::RemoveReadRuleRequest, Ydb::PersQueue::V1::RemoveReadRuleResponse>(
            runtime, request, CreateRemoveConsumerActor, path);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
    }
}

Y_UNIT_TEST(SharedConsumersDisabledRejected) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    runtime.GetAppData().FeatureFlags.SetEnableTopicMessageLevelParallelism(false);

    auto request = MakeCreateTopicRequest("/Root/topic_shared_disabled");
    *request.mutable_settings()->add_read_rules() = MakeSharedReadRule("shared_c1");

    auto result = DoActorRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(
        runtime, request, CreateCreateTopicActor, request.path());
    AssertStatus(result, Ydb::StatusIds::BAD_REQUEST, "shared consumers are disabled");
}

Y_UNIT_TEST(AlterCdcStreamForbidden) {
    auto setup = CreateSetup();
    ExecuteDDL(*setup, "CREATE TABLE table_cdc (id Uint64, PRIMARY KEY (id))");
    ExecuteDDL(*setup, "ALTER TABLE table_cdc ADD CHANGEFEED feed WITH (FORMAT = 'JSON', MODE = 'UPDATES')");

    auto& runtime = setup->GetRuntime();
    Ydb::PersQueue::V1::AlterTopicRequest request;
    request.set_path("/Root/table_cdc/feed");
    auto& settings = *request.mutable_settings();
    settings.set_partitions_count(1);
    settings.set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
    settings.set_retention_period_ms(TDuration::Days(1).MilliSeconds());

    auto result = DoActorRequest<Ydb::PersQueue::V1::AlterTopicRequest, Ydb::PersQueue::V1::AlterTopicResponse>(
        runtime, request, CreateAlterTopicActor, request.path());
    AssertStatus(result, Ydb::StatusIds::SCHEME_ERROR, "Full alter of CDC stream is forbidden");
}

Y_UNIT_TEST(UnauthenticatedRejectedWhenRequired) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    runtime.GetAppData().PQConfig.SetRequireCredentialsInNewProtocol(true);

    auto request = MakeCreateTopicRequest("/Root/topic_auth");
    auto result = DoActorRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(
        runtime, request, CreateCreateTopicActor, request.path());
    AssertStatus(result, Ydb::StatusIds::UNAUTHORIZED, "Unauthenticated access is forbidden");
}

Y_UNIT_TEST(RejectEmptyAndIllegalConsumerName) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_bad_consumer";
    CreateTopic(runtime, path);

    {
        Ydb::PersQueue::V1::AddReadRuleRequest request;
        request.set_path(path);
        *request.mutable_read_rule() = MakeStreamingReadRule("");
        auto result = DoActorRequest<Ydb::PersQueue::V1::AddReadRuleRequest, Ydb::PersQueue::V1::AddReadRuleResponse>(
            runtime, request, CreateAddConsumerActor, path);
        AssertStatus(result, Ydb::StatusIds::BAD_REQUEST, "empty name");
    }

    {
        Ydb::PersQueue::V1::AddReadRuleRequest request;
        request.set_path(path);
        *request.mutable_read_rule() = MakeStreamingReadRule("bad/name");
        auto result = DoActorRequest<Ydb::PersQueue::V1::AddReadRuleRequest, Ydb::PersQueue::V1::AddReadRuleResponse>(
            runtime, request, CreateAddConsumerActor, path);
        AssertStatus(result, Ydb::StatusIds::BAD_REQUEST, "illegal symbols");
    }

    {
        Ydb::PersQueue::V1::AddReadRuleRequest request;
        request.set_path(path);
        *request.mutable_read_rule() = MakeStreamingReadRule("bad|name");
        auto result = DoActorRequest<Ydb::PersQueue::V1::AddReadRuleRequest, Ydb::PersQueue::V1::AddReadRuleResponse>(
            runtime, request, CreateAddConsumerActor, path);
        AssertStatus(result, Ydb::StatusIds::BAD_REQUEST, "illegal symbols");
    }
}

Y_UNIT_TEST(RejectMissingRetentionAndNegativeWriteSpeed) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    {
        Ydb::PersQueue::V1::CreateTopicRequest request;
        request.set_path("/Root/topic_no_retention");
        auto& settings = *request.mutable_settings();
        settings.set_partitions_count(1);
        settings.set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
        auto result = DoActorRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(
            runtime, request, CreateCreateTopicActor, request.path());
        AssertStatus(result, Ydb::StatusIds::BAD_REQUEST, "retention");
    }

    {
        auto request = MakeCreateTopicRequest("/Root/topic_neg_speed");
        request.mutable_settings()->set_max_partition_write_speed(-1);
        auto result = DoActorRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(
            runtime, request, CreateCreateTopicActor, request.path());
        AssertStatus(result, Ydb::StatusIds::BAD_REQUEST);
    }

    {
        Ydb::PersQueue::V1::CreateTopicRequest request;
        request.set_path("/Root/topic_neg_storage");
        auto& settings = *request.mutable_settings();
        settings.set_partitions_count(1);
        settings.set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
        settings.set_retention_storage_bytes(-5);
        auto result = DoActorRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(
            runtime, request, CreateCreateTopicActor, request.path());
        AssertStatus(result, Ydb::StatusIds::BAD_REQUEST, "retention_storage_bytes");
    }
}

Y_UNIT_TEST(CreateWithRetentionStorageBytes) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    Ydb::PersQueue::V1::CreateTopicRequest request;
    request.set_path("/Root/topic_storage_retention");
    auto& settings = *request.mutable_settings();
    settings.set_partitions_count(1);
    settings.set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
    settings.set_retention_storage_bytes(10_MB);

    auto result = DoActorRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(
        runtime, request, CreateCreateTopicActor, request.path());
    AssertStatus(result, Ydb::StatusIds::SUCCESS);

    runtime.Register(NPQ::NDescriber::CreateDescriberActor(
        runtime.AllocateEdgeActor(), "/Root", {"/Root/topic_storage_retention"}));
    auto response = runtime.GrabEdgeEvent<NPQ::NDescriber::TEvDescribeTopicsResponse>(TDuration::Seconds(5));
    UNIT_ASSERT_VALUES_EQUAL(response->Topics.size(), 1u);
    const auto& topic = response->Topics.begin()->second;
    UNIT_ASSERT_VALUES_EQUAL(topic.Status, NPQ::NDescriber::EStatus::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(
        topic.Info->Description.GetPQTabletConfig().GetPartitionConfig().GetStorageLimitBytes(),
        10_MB);
}

Y_UNIT_TEST(ByteBurstDefaultsToWriteSpeed) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    auto request = MakeCreateTopicRequest("/Root/topic_byte_burst");
    request.mutable_settings()->set_max_partition_write_speed(123456);
    request.mutable_settings()->set_max_partition_write_burst(0);

    auto result = DoActorRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(
        runtime, request, CreateCreateTopicActor, request.path());
    AssertStatus(result, Ydb::StatusIds::SUCCESS);

    runtime.Register(NPQ::NDescriber::CreateDescriberActor(
        runtime.AllocateEdgeActor(), "/Root", {"/Root/topic_byte_burst"}));
    auto response = runtime.GrabEdgeEvent<NPQ::NDescriber::TEvDescribeTopicsResponse>(TDuration::Seconds(5));
    const auto& partConfig = response->Topics.begin()->second.Info->Description.GetPQTabletConfig().GetPartitionConfig();
    UNIT_ASSERT_VALUES_EQUAL(partConfig.GetWriteSpeedInBytesPerSecond(), 123456u);
    UNIT_ASSERT_VALUES_EQUAL(partConfig.GetBurstSize(), 123456u);
}

Y_UNIT_TEST(CreateExistingTopicSucceeds) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_dup";
    CreateTopic(runtime, path);

    // gRPC CreateTopic uses IfNotExists=true, so a second create is SUCCESS.
    auto result = DoActorRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(
        runtime, MakeCreateTopicRequest(path), CreateCreateTopicActor, path);
    AssertStatus(result, Ydb::StatusIds::SUCCESS);
}

Y_UNIT_TEST(ZeroPartitionsRejected) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    auto request = MakeCreateTopicRequest("/Root/topic_zero_parts", /*partitions=*/0);
    auto result = DoActorRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(
        runtime, request, CreateCreateTopicActor, request.path());
    AssertStatus(result, Ydb::StatusIds::BAD_REQUEST, "Partitions count must be positive");
}

Y_UNIT_TEST(UnknownFormatRejected) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    auto request = MakeCreateTopicRequest("/Root/topic_bad_format");
    request.mutable_settings()->set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_UNSPECIFIED);
    auto result = DoActorRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(
        runtime, request, CreateCreateTopicActor, request.path());
    AssertStatus(result, Ydb::StatusIds::BAD_REQUEST, "Unknown format version");
}

Y_UNIT_TEST(UnknownCodecRejected) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    auto request = MakeCreateTopicRequest("/Root/topic_bad_codec");
    request.mutable_settings()->add_supported_codecs(Ydb::PersQueue::V1::CODEC_UNSPECIFIED);
    auto result = DoActorRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(
        runtime, request, CreateCreateTopicActor, request.path());
    AssertStatus(result, Ydb::StatusIds::BAD_REQUEST, "Unknown codec");
}

Y_UNIT_TEST(ImportantConsumerAndCodecsStored) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_important_codecs";

    auto request = MakeCreateTopicRequest(path);
    request.mutable_settings()->add_supported_codecs(Ydb::PersQueue::V1::CODEC_RAW);
    request.mutable_settings()->add_supported_codecs(Ydb::PersQueue::V1::CODEC_ZSTD);
    *request.mutable_settings()->add_read_rules() = MakeStreamingReadRule("important_c");
    request.mutable_settings()->mutable_read_rules(0)->set_important(true);
    // Consumer codecs must include every topic codec.
    request.mutable_settings()->mutable_read_rules(0)->add_supported_codecs(Ydb::PersQueue::V1::CODEC_RAW);
    request.mutable_settings()->mutable_read_rules(0)->add_supported_codecs(Ydb::PersQueue::V1::CODEC_ZSTD);
    request.mutable_settings()->mutable_read_rules(0)->add_supported_codecs(Ydb::PersQueue::V1::CODEC_GZIP);

    auto result = DoActorRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(
        runtime, request, CreateCreateTopicActor, path);
    AssertStatus(result, Ydb::StatusIds::SUCCESS);

    const auto config = DescribeTabletConfig(runtime, path);
    const auto& codecs = config.GetCodecs().GetIds();
    UNIT_ASSERT_VALUES_EQUAL(codecs.size(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(codecs.Get(0), 0); // RAW = 1 on wire, stored as 0
    UNIT_ASSERT_VALUES_EQUAL(codecs.Get(1), 3); // ZSTD = 4 on wire, stored as 3

    const auto* consumer = NPQ::GetConsumer(config, "important_c");
    UNIT_ASSERT(consumer);
    UNIT_ASSERT(consumer->GetImportant());
    UNIT_ASSERT_VALUES_EQUAL(consumer->GetCodec().IdsSize(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(consumer->GetCodec().GetIds(0), 0); // RAW
    UNIT_ASSERT_VALUES_EQUAL(consumer->GetCodec().GetIds(1), 3); // ZSTD
    UNIT_ASSERT_VALUES_EQUAL(consumer->GetCodec().GetIds(2), 1); // GZIP
}

Y_UNIT_TEST(ConsumerCodecsMustCoverTopicCodecs) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    auto request = MakeCreateTopicRequest("/Root/topic_codec_mismatch");
    request.mutable_settings()->add_supported_codecs(Ydb::PersQueue::V1::CODEC_RAW);
    request.mutable_settings()->add_supported_codecs(Ydb::PersQueue::V1::CODEC_ZSTD);
    *request.mutable_settings()->add_read_rules() = MakeStreamingReadRule("c1");
    request.mutable_settings()->mutable_read_rules(0)->add_supported_codecs(Ydb::PersQueue::V1::CODEC_GZIP);

    auto result = DoActorRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(
        runtime, request, CreateCreateTopicActor, request.path());
    AssertStatus(result, Ydb::StatusIds::BAD_REQUEST, "got unsupported codec");
}

Y_UNIT_TEST(ReadQuotaStored) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_read_quota";

    auto request = MakeCreateTopicRequest(path);
    request.mutable_settings()->set_partition_total_read_speed_bytes_per_second(1111);
    request.mutable_settings()->set_partition_total_read_speed_messages_per_second(2222);
    request.mutable_settings()->set_partition_read_without_consumer_speed_bytes_per_second(3333);
    request.mutable_settings()->set_partition_read_without_consumer_speed_messages_per_second(4444);
    auto* rule = request.mutable_settings()->add_read_rules();
    *rule = MakeStreamingReadRule("quota_c");
    rule->set_read_speed_bytes_per_second(5555);
    rule->set_read_speed_messages_per_second(6666);

    auto result = DoActorRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(
        runtime, request, CreateCreateTopicActor, path);
    AssertStatus(result, Ydb::StatusIds::SUCCESS);

    const auto config = DescribeTabletConfig(runtime, path);
    const auto& partConfig = config.GetPartitionConfig();
    UNIT_ASSERT_VALUES_EQUAL(partConfig.GetReadSpeedInBytesPerSecond(), 1111u);
    UNIT_ASSERT_VALUES_EQUAL(partConfig.GetReadSpeedInMessagesPerSecond(), 2222u);

    const auto* withoutConsumer = NPQ::GetReadQuota(config, NPQ::CLIENTID_WITHOUT_CONSUMER);
    UNIT_ASSERT(withoutConsumer);
    UNIT_ASSERT_VALUES_EQUAL(withoutConsumer->GetSpeedInBytesPerSecond(), 3333u);
    UNIT_ASSERT_VALUES_EQUAL(withoutConsumer->GetSpeedInMessagesPerSecond(), 4444u);

    const auto* consumerQuota = NPQ::GetReadQuota(config, "quota_c");
    UNIT_ASSERT(consumerQuota);
    UNIT_ASSERT_VALUES_EQUAL(consumerQuota->GetSpeedInBytesPerSecond(), 5555u);
    UNIT_ASSERT_VALUES_EQUAL(consumerQuota->GetSpeedInMessagesPerSecond(), 6666u);
}

Y_UNIT_TEST(AlterSuccessAndMissing) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_alter";
    CreateTopic(runtime, path);

    {
        auto request = MakeAlterTopicRequest(path);
        request.mutable_settings()->set_retention_period_ms(TDuration::Hours(2).MilliSeconds());
        auto result = DoActorRequest<Ydb::PersQueue::V1::AlterTopicRequest, Ydb::PersQueue::V1::AlterTopicResponse>(
            runtime, request, CreateAlterTopicActor, path);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(
            DescribeTabletConfig(runtime, path).GetPartitionConfig().GetLifetimeSeconds(),
            TDuration::Hours(2).Seconds());
    }

    {
        auto request = MakeAlterTopicRequest("/Root/missing_topic");
        auto result = DoActorRequest<Ydb::PersQueue::V1::AlterTopicRequest, Ydb::PersQueue::V1::AlterTopicResponse>(
            runtime, request, CreateAlterTopicActor, request.path());
        AssertStatus(result, Ydb::StatusIds::SCHEME_ERROR);
    }
}

Y_UNIT_TEST(AlterCannotChangeConsumerType) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    runtime.GetAppData().FeatureFlags.SetEnableTopicMessageLevelParallelism(true);
    const TString path = "/Root/topic_type_change";
    CreateTopic(runtime, path);

    {
        Ydb::PersQueue::V1::AddReadRuleRequest request;
        request.set_path(path);
        *request.mutable_read_rule() = MakeStreamingReadRule("c1");
        auto result = DoActorRequest<Ydb::PersQueue::V1::AddReadRuleRequest, Ydb::PersQueue::V1::AddReadRuleResponse>(
            runtime, request, CreateAddConsumerActor, path);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
    }

    auto request = MakeAlterTopicRequest(path);
    *request.mutable_settings()->add_read_rules() = MakeSharedReadRule("c1");
    auto result = DoActorRequest<Ydb::PersQueue::V1::AlterTopicRequest, Ydb::PersQueue::V1::AlterTopicResponse>(
        runtime, request, CreateAlterTopicActor, path);
    AssertStatus(result, Ydb::StatusIds::BAD_REQUEST, "Cannot alter consumer type");
}

Y_UNIT_TEST(NegativeReadSpeedRejected) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    auto request = MakeCreateTopicRequest("/Root/topic_neg_read");
    request.mutable_settings()->set_partition_total_read_speed_bytes_per_second(-1);
    auto result = DoActorRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(
        runtime, request, CreateCreateTopicActor, request.path());
    AssertStatus(result, Ydb::StatusIds::BAD_REQUEST, "partition_total_read_speed_bytes_per_second");
}

Y_UNIT_TEST(FccTopicNameFormatsCreateAndDescribeAliases) {
    auto setup = CreateSetup("PQv1NameFormatsFcc");
    auto& runtime = setup->GetRuntime();

    MkDir(*setup, "/Root", "fcclegacy");
    MkDir(*setup, "/Root", "fccmodern");
    MkDir(*setup, "/Root", "fccshort");

    CreateTopic(runtime, "rt3.dc1--fcclegacy--topic");
    AssertDescribeAliases(
        runtime,
        {
            "rt3.dc1--fcclegacy--topic",
            "fcclegacy--topic",
            "fcclegacy/topic",
            "/Root/fcclegacy/topic",
        },
        "/Root/fcclegacy/topic");

    CreateTopic(runtime, "fccmodern/topic");
    AssertDescribeAliases(
        runtime,
        {
            "rt3.dc1--fccmodern--topic",
            "fccmodern--topic",
            "fccmodern/topic",
            "/Root/fccmodern/topic",
        },
        "/Root/fccmodern/topic");

    CreateTopic(runtime, "fccshort--topic");
    AssertDescribeAliases(
        runtime,
        {
            "rt3.dc1--fccshort--topic",
            "fccshort--topic",
            "fccshort/topic",
            "/Root/fccshort/topic",
        },
        "/Root/fccshort/topic");
}

Y_UNIT_TEST(FederationTopicNameFormats) {
    auto setup = CreateSetup("PQv1NameFormatsFed");
    auto& runtime = setup->GetRuntime();
    runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);

    {
        auto request = MakeCreateTopicRequest("fedshort--topic");
        auto result = DoActorRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(
            runtime, request, CreateCreateTopicActor, request.path());
        AssertStatus(result, Ydb::StatusIds::BAD_REQUEST, "expected legacy-style name");
    }

    CreateTopic(runtime, "rt3.dc1--fedleaf--topic");
    AssertDescribeAliases(
        runtime,
        {"/Root/rt3.dc1--fedleaf--topic"},
        "/Root/rt3.dc1--fedleaf--topic");

    runtime.GetAppData().PQConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot("/Root/LbCommunal");
    MkDir(*setup, "/Root", "LbCommunal");
    MkDir(*setup, "/Root/LbCommunal", "account");

    auto modern = MakeCreateTopicRequest("/Root/LbCommunal/account/fedtopic");
    modern.mutable_settings()->mutable_attributes()->insert({"_federation_account", "account"});
    auto modernResult = DoActorRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(
        runtime, modern, CreateCreateTopicActor, modern.path(), "/Root/LbCommunal/account");
    AssertStatus(modernResult, Ydb::StatusIds::SUCCESS);

    AssertDescribeAliases(
        runtime,
        {
            "rt3.dc1--account--fedtopic",
            "account--fedtopic",
            "account/fedtopic",
            "/Root/LbCommunal/account/fedtopic",
        },
        "/Root/LbCommunal/account/fedtopic");
}

} // Y_UNIT_TEST_SUITE(SchemaOps_PQv1)

} // namespace NKikimr::NGRpcProxy::V1::NPQv1

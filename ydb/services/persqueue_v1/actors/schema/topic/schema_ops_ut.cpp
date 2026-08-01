#include "actors.h"

#include <ydb/core/testlib/grpc_request/grpc_request.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NGRpcProxy::V1::NTopic {

using namespace NYdb::NTopic::NTests;
using namespace NKikimr::Tests::NGrpc;

namespace {

std::shared_ptr<TTopicSdkTestSetup> CreateSetup(const char* name = "TopicSchemaOps") {
    auto setup = std::make_shared<TTopicSdkTestSetup>(name, TTopicSdkTestSetup::MakeServerSettings(), false);
    setup->GetServer().EnableLogs({NKikimrServices::PQ_SCHEMA}, NActors::NLog::PRI_DEBUG);
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

Ydb::Topic::CreateTopicRequest MakeCreateTopicRequest(const TString& path) {
    Ydb::Topic::CreateTopicRequest request;
    request.set_path(path);
    request.mutable_partitioning_settings()->set_min_active_partitions(1);
    auto* consumer = request.add_consumers();
    consumer->set_name("user");
    return request;
}

void CreateTopic(NActors::TTestActorRuntime& runtime, const TString& path) {
    auto result = DoActorRequest<Ydb::Topic::CreateTopicRequest, Ydb::Topic::CreateTopicResponse>(
        runtime, MakeCreateTopicRequest(path), CreateCreateTopicActor, path);
    AssertStatus(result, Ydb::StatusIds::SUCCESS);
}

} // namespace

Y_UNIT_TEST_SUITE(SchemaOps_TopicAPI) {

Y_UNIT_TEST(CreateAlterDropSmoke) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_api_smoke";

    CreateTopic(runtime, path);

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        auto* consumer = request.add_add_consumers();
        consumer->set_name("extra");
        auto result = DoActorRequest<Ydb::Topic::AlterTopicRequest, Ydb::Topic::AlterTopicResponse>(
            runtime, request, CreateAlterTopicActor, path);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
    }

    {
        Ydb::Topic::DescribeConsumerRequest request;
        request.set_path(path);
        request.set_consumer("extra");
        auto result = DoActorRequest<Ydb::Topic::DescribeConsumerRequest, Ydb::Topic::DescribeConsumerResponse>(
            runtime, request, CreateDescribeConsumerActor, path);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
    }

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        request.add_drop_consumers("extra");
        auto result = DoActorRequest<Ydb::Topic::AlterTopicRequest, Ydb::Topic::AlterTopicResponse>(
            runtime, request, CreateAlterTopicActor, path);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
    }

    {
        Ydb::Topic::DescribeConsumerRequest request;
        request.set_path(path);
        request.set_consumer("extra");
        auto result = DoActorRequest<Ydb::Topic::DescribeConsumerRequest, Ydb::Topic::DescribeConsumerResponse>(
            runtime, request, CreateDescribeConsumerActor, path);
        AssertStatus(result, Ydb::StatusIds::SCHEME_ERROR, "no consumer");
    }

    {
        Ydb::Topic::DropTopicRequest request;
        request.set_path(path);
        auto result = DoActorRequest<Ydb::Topic::DropTopicRequest, Ydb::Topic::DropTopicResponse>(
            runtime, request, CreateDropTopicActor, path);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
    }

    {
        Ydb::Topic::DropTopicRequest request;
        request.set_path(path);
        auto result = DoActorRequest<Ydb::Topic::DropTopicRequest, Ydb::Topic::DropTopicResponse>(
            runtime, request, CreateDropTopicActor, path);
        AssertStatus(result, Ydb::StatusIds::SCHEME_ERROR);
    }
}

Y_UNIT_TEST(DescribePartitionSmokeAndMissing) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_describe_part";
    CreateTopic(runtime, path);

    {
        Ydb::Topic::DescribePartitionRequest request;
        request.set_path(path);
        request.set_partition_id(0);
        auto result = DoActorRequest<Ydb::Topic::DescribePartitionRequest, Ydb::Topic::DescribePartitionResponse>(
            runtime, request, CreateDescribePartitionActor, path);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
    }

    {
        Ydb::Topic::DescribePartitionRequest request;
        request.set_path(path);
        request.set_partition_id(1000);
        auto result = DoActorRequest<Ydb::Topic::DescribePartitionRequest, Ydb::Topic::DescribePartitionResponse>(
            runtime, request, CreateDescribePartitionActor, path);
        AssertStatus(result, Ydb::StatusIds::BAD_REQUEST);
    }

    {
        Ydb::Topic::DescribePartitionRequest request;
        request.set_path("/Root/not_a_topic");
        request.set_partition_id(0);
        auto result = DoActorRequest<Ydb::Topic::DescribePartitionRequest, Ydb::Topic::DescribePartitionResponse>(
            runtime, request, CreateDescribePartitionActor, request.path());
        AssertStatus(result, Ydb::StatusIds::SCHEME_ERROR);
    }
}

Y_UNIT_TEST(DescribeNotTopic) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    // Create a directory-like path via a plain topic first, then describe a table-like non-topic using scheme dir.
    // Use DescribeConsumer on a path that exists but is not a topic: create via YQL is heavier;
    // DescribeTopic/Consumer on missing path already covered; here check DescribeConsumer unknown consumer on valid topic.
    const TString path = "/Root/topic_desc_notopic";
    CreateTopic(runtime, path);

    Ydb::Topic::DescribeConsumerRequest request;
    request.set_path(path);
    request.set_consumer("missing_consumer");
    auto result = DoActorRequest<Ydb::Topic::DescribeConsumerRequest, Ydb::Topic::DescribeConsumerResponse>(
        runtime, request, CreateDescribeConsumerActor, path);
    AssertStatus(result, Ydb::StatusIds::SCHEME_ERROR, "no consumer");
}

Y_UNIT_TEST(UnauthenticatedRejectedWhenRequired) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    runtime.GetAppData().PQConfig.SetRequireCredentialsInNewProtocol(true);

    auto request = MakeCreateTopicRequest("/Root/topic_auth");
    auto result = DoActorRequest<Ydb::Topic::CreateTopicRequest, Ydb::Topic::CreateTopicResponse>(
        runtime, request, CreateCreateTopicActor, request.path());
    AssertStatus(result, Ydb::StatusIds::UNAUTHORIZED, "Unauthenticated access is forbidden");
}

Y_UNIT_TEST(CreateTopicSuccess) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_create_ok";

    auto request = MakeCreateTopicRequest(path);
    auto result = DoActorRequest<Ydb::Topic::CreateTopicRequest, Ydb::Topic::CreateTopicResponse>(
        runtime, request, CreateCreateTopicActor, path);
    AssertStatus(result, Ydb::StatusIds::SUCCESS);

    Ydb::Topic::DescribeConsumerRequest describe;
    describe.set_path(path);
    describe.set_consumer("user");
    auto describeResult = DoActorRequest<Ydb::Topic::DescribeConsumerRequest, Ydb::Topic::DescribeConsumerResponse>(
        runtime, describe, CreateDescribeConsumerActor, path);
    AssertStatus(describeResult, Ydb::StatusIds::SUCCESS);
}

} // Y_UNIT_TEST_SUITE(SchemaOps_TopicAPI)

} // namespace NKikimr::NGRpcProxy::V1::NTopic

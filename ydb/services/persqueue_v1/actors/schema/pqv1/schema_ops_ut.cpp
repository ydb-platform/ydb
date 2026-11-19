#include "actors.h"

#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/persqueue/public/utils.h>
#include <ydb/core/testlib/grpc_request/grpc_request.h>
#include <ydb/public/api/protos/ydb_persqueue_v1.pb.h>
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

Y_UNIT_TEST(ReplicateOffsets) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_replicate_offsets";
    auto request = MakeCreateTopicRequest(path);
    for (const auto* name : {"default", "local", "replicated"}) {
        auto* rule = request.mutable_settings()->add_read_rules();
        *rule = MakeStreamingReadRule(name);
        if (rule->consumer_name() != "default") {
            rule->set_replicate_offsets(rule->consumer_name() == "replicated");
        }
    }
    auto created = DoActorRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(
        runtime, request, CreateCreateTopicActor, path);
    AssertStatus(created, Ydb::StatusIds::SUCCESS);

    auto check = [&](bool localReplicateOffsets) {
        const auto config = DescribeTabletConfig(runtime, path);
        UNIT_ASSERT_VALUES_EQUAL(config.ConsumersSize(), 3);
        for (const auto& consumer : config.GetConsumers()) {
            UNIT_ASSERT_VALUES_EQUAL(consumer.HasReplicateOffsets(), consumer.GetName() != "default");
            UNIT_ASSERT_VALUES_EQUAL(consumer.GetReplicateOffsets(),
                consumer.GetName() != "local" || localReplicateOffsets);
        }
    };
    check(false);

    auto alter = MakeAlterTopicRequest(path);
    *alter.mutable_settings() = request.settings();
    alter.mutable_settings()->set_retention_period_ms(TDuration::Hours(12).MilliSeconds());
    for (auto& rule : *alter.mutable_settings()->mutable_read_rules()) {
        rule.clear_replicate_offsets();
    }
    auto altered = DoActorRequest<Ydb::PersQueue::V1::AlterTopicRequest, Ydb::PersQueue::V1::AlterTopicResponse>(
        runtime, alter, CreateAlterTopicActor, path);
    AssertStatus(altered, Ydb::StatusIds::SUCCESS);
    check(false);

    alter.mutable_settings()->mutable_read_rules(1)->set_replicate_offsets(true);
    altered = DoActorRequest<Ydb::PersQueue::V1::AlterTopicRequest, Ydb::PersQueue::V1::AlterTopicResponse>(
        runtime, alter, CreateAlterTopicActor, path);
    AssertStatus(altered, Ydb::StatusIds::SUCCESS);
    check(true);
}

}

} // namespace NKikimr::NGRpcProxy::V1::NPQv1

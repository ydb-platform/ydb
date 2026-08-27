#include "actors.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/grpc_request/grpc_request.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/test_server.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/async.h>

#include <util/thread/pool.h>

namespace NKikimr::NGRpcProxy::V1::NTopic {

using namespace NYdb::NTopic::NTests;
using namespace NKikimr::Tests::NGrpc;

namespace {

std::shared_ptr<TTopicSdkTestSetup> CreateSetup(const char* name = "TopicSchemaOps") {
    auto setup = std::make_shared<TTopicSdkTestSetup>(name, TTopicSdkTestSetup::MakeServerSettings(), false);
    setup->GetServer().EnableLogs({NKikimrServices::PQ_SCHEMA}, NActors::NLog::PRI_DEBUG);
    return setup;
}

// Simulated threads are required for AddObserver on TEvPipeCache::TEvForward.
// Avoid TTopicSdkTestSetup here: its StartServer(true)/FullInit deadlocks without a dispatcher.
struct TSimulatedServer {
    THolder<TThreadPool> Pool;
    THolder<::NPersQueue::TTestServer> Server;

    ~TSimulatedServer() {
        Server.Reset();
        if (Pool) {
            Pool->Stop();
        }
    }

    NActors::TTestActorRuntime& GetRuntime() {
        return *Server->CleverServer->GetRuntime();
    }
};

std::unique_ptr<TSimulatedServer> CreateSimulatedServer() {
    auto settings = TTopicSdkTestSetup::MakeServerSettings();
    settings.SetUseRealThreads(false);

    auto out = std::make_unique<TSimulatedServer>();
    out->Server = MakeHolder<::NPersQueue::TTestServer>(settings, /*start=*/false);
    out->Server->StartServer(/*doClientInit=*/false, TString("/Root"));

    auto& runtime = out->GetRuntime();
    runtime.UpdateCurrentTime(TInstant::Now());
    out->Server->EnableLogs({NKikimrServices::PQ_SCHEMA}, NActors::NLog::PRI_DEBUG);

    out->Server->AnnoyingClient->SetNoConfigMode();
    out->Pool = MakeHolder<TThreadPool>();
    out->Pool->Start(2);
    auto* server = out->Server.Get();
    auto future = NThreading::Async([server] {
        server->AnnoyingClient->FullInit();
        return true;
    }, *out->Pool);
    static_cast<NKikimr::TTestActorRuntime&>(runtime).WaitFuture(std::move(future));
    return out;
}

template <typename TRequest, typename TResponse>
std::shared_ptr<TResultHolder<TResponse>> DoActorRequest(
    NActors::TTestActorRuntime& runtime,
    const TRequest& request,
    NActors::IActor* (*factory)(NGRpcService::IRequestOpCtx*),
    const TString& path,
    const TString& database = "/Root",
    TDuration waitTimeout = TDuration::Seconds(10))
{
    auto result = std::make_shared<TResultHolder<TResponse>>();
    auto edgeActor = runtime.AllocateEdgeActor();
    auto* ctx = new TRequestCtx<TRequest, TResponse>(request, path, database, result, edgeActor);
    runtime.Register(factory(ctx));
    runtime.GrabEdgeEvent<NActors::TEvents::TEvWakeup>(edgeActor, waitTimeout);
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

// Core create/alter/drop/CDC/msg-s coverage lives in ydb/core/persqueue/public/schema.
// Here we keep Topic service describe/auth wrappers only.

Y_UNIT_TEST_SUITE(SchemaOps_TopicAPI) {

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

        auto* describeResult = dynamic_cast<const Ydb::Topic::DescribePartitionResult*>(result->Response.get());
        UNIT_ASSERT(describeResult);
        UNIT_ASSERT_VALUES_EQUAL(describeResult->partition().partition_id(), 0u);
        UNIT_ASSERT(describeResult->partition().active());
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

Y_UNIT_TEST(DescribePartitionRetriesOnLocationDeliveryProblem) {
    auto server = CreateSimulatedServer();
    auto& runtime = server->GetRuntime();
    const TString path = "/Root/topic_describe_part_delivery_problem";
    CreateTopic(runtime, path);

    // Break the first location forward and inject DeliveryProblem with the matching
    // subscribe cookie — regresses silent ignore of OnUndelivered()==true.
    size_t broken = 0;
    auto* rt = &runtime;
    auto breakObserver = runtime.AddObserver<TEvPipeCache::TEvForward>(
        [&broken, rt](TEvPipeCache::TEvForward::TPtr& ev) {
            if (!ev || !ev->Get()->Ev) {
                return;
            }
            if (ev->Get()->Ev->Type() != TEvPersQueue::TEvGetPartitionsLocation::EventType) {
                return;
            }
            if (broken >= 1) {
                return;
            }
            ++broken;
            const ui64 tabletId = ev->Get()->TabletId;
            const ui64 subscribeCookie = ev->Get()->Options.SubscribeCookie;
            rt->Send(new IEventHandle(
                ev->Sender,
                ev->Recipient,
                new TEvPipeCache::TEvDeliveryProblem(tabletId, true /*notDelivered*/),
                0,
                subscribeCookie));
            ev.Reset();
        });

    Ydb::Topic::DescribePartitionRequest request;
    request.set_path(path);
    request.set_partition_id(0);
    request.set_include_location(true);

    auto result = DoActorRequest<Ydb::Topic::DescribePartitionRequest, Ydb::Topic::DescribePartitionResponse>(
        runtime, request, CreateDescribePartitionActor, path);

    UNIT_ASSERT_VALUES_EQUAL(broken, 1u);
    AssertStatus(result, Ydb::StatusIds::SUCCESS);

    auto* describeResult = dynamic_cast<const Ydb::Topic::DescribePartitionResult*>(result->Response.get());
    UNIT_ASSERT(describeResult);
    UNIT_ASSERT(describeResult->partition().has_partition_location());
    UNIT_ASSERT_GT(describeResult->partition().partition_location().node_id(), 0);
}

Y_UNIT_TEST(DescribePartitionTimesOutWhenLocationStuck) {
    auto server = CreateSimulatedServer();
    auto& runtime = server->GetRuntime();
    const TString path = "/Root/topic_describe_part_timeout";
    CreateTopic(runtime, path);

    // Drop location forwards so the actor never gets a response and must hit RequestTimeout.
    auto dropObserver = runtime.AddObserver<TEvPipeCache::TEvForward>(
        [](TEvPipeCache::TEvForward::TPtr& ev) {
            if (ev && ev->Get()->Ev &&
                ev->Get()->Ev->Type() == TEvPersQueue::TEvGetPartitionsLocation::EventType)
            {
                ev.Reset();
            }
        });

    Ydb::Topic::DescribePartitionRequest request;
    request.set_path(path);
    request.set_partition_id(0);
    request.set_include_location(true);

    auto result = std::make_shared<TResultHolder<Ydb::Topic::DescribePartitionResponse>>();
    auto edgeActor = runtime.AllocateEdgeActor();
    auto* ctx = new TRequestCtx<Ydb::Topic::DescribePartitionRequest, Ydb::Topic::DescribePartitionResponse>(
        request, path, "/Root", result, edgeActor);
    auto actorId = runtime.Register(CreateDescribePartitionActor(ctx));
    runtime.EnableScheduleForActor(actorId, true);

    // Reach StateWork (location request stuck), then jump past RequestTimeout.
    runtime.DispatchEvents(TDispatchOptions{}, TDuration::MilliSeconds(100));
    runtime.AdvanceCurrentTime(TDuration::Seconds(11));

    runtime.GrabEdgeEvent<NActors::TEvents::TEvWakeup>(edgeActor, TDuration::Seconds(5));
    UNIT_ASSERT_C(result->ResultStatus, "The operation is still in progress");
    AssertStatus(result, Ydb::StatusIds::TIMEOUT, "Describe request timed out");
}

Y_UNIT_TEST(DescribePartitionFailsAfterStatsRetriesExhausted) {
    auto server = CreateSimulatedServer();
    auto& runtime = server->GetRuntime();
    const TString path = "/Root/topic_describe_part_stats_retries";
    CreateTopic(runtime, path);

    size_t broken = 0;
    auto* rt = &runtime;
    auto breakObserver = runtime.AddObserver<TEvPipeCache::TEvForward>(
        [&broken, rt](TEvPipeCache::TEvForward::TPtr& ev) {
            if (!ev || !ev->Get()->Ev) {
                return;
            }
            if (ev->Get()->Ev->Type() != TEvPersQueue::TEvStatus::EventType) {
                return;
            }
            ++broken;
            const ui64 tabletId = ev->Get()->TabletId;
            const ui64 subscribeCookie = ev->Get()->Options.SubscribeCookie;
            rt->Send(new IEventHandle(
                ev->Sender,
                ev->Recipient,
                new TEvPipeCache::TEvDeliveryProblem(tabletId, true /*notDelivered*/),
                0,
                subscribeCookie));
            ev.Reset();
        });

    Ydb::Topic::DescribePartitionRequest request;
    request.set_path(path);
    request.set_partition_id(0);
    request.set_include_stats(true);

    auto result = DoActorRequest<Ydb::Topic::DescribePartitionRequest, Ydb::Topic::DescribePartitionResponse>(
        runtime, request, CreateDescribePartitionActor, path);

    // Initial attempt + StatsMaxRetries(15) backoff retries, then fail on the next DeliveryProblem.
    UNIT_ASSERT_VALUES_EQUAL(broken, 16u);
    AssertStatus(result, Ydb::StatusIds::UNAVAILABLE, "unresponsive");
}

Y_UNIT_TEST(DescribeUnknownConsumer) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_desc_unknown_consumer";
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

} // Y_UNIT_TEST_SUITE(SchemaOps_TopicAPI)

} // namespace NKikimr::NGRpcProxy::V1::NTopic

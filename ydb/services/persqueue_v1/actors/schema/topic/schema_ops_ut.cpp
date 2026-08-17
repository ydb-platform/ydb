#include "actors.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/grpc_request/grpc_request.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/test_server.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>
#include <ydb/services/persqueue_v1/actors/events.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/async.h>

#include <util/thread/pool.h>

#include <memory>

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
class TInternalRequestCtx
    : public TRequestCtx<TRequest, TResponse>
    , public NGRpcService::IInternalRequestCtx
{
public:
    using TRequestCtx<TRequest, TResponse>::TRequestCtx;
};

template <typename TRequest, typename TResponse>
std::shared_ptr<TResultHolder<TResponse>> DoActorRequest(
    NActors::TTestActorRuntime& runtime,
    const TRequest& request,
    NActors::IActor* (*factory)(NGRpcService::IRequestOpCtx*),
    const TString& path,
    const TString& database = "/Root",
    TDuration waitTimeout = TDuration::Seconds(30))
{
    auto result = std::make_shared<TResultHolder<TResponse>>();
    auto edgeActor = runtime.AllocateEdgeActor();
    auto* ctx = new TRequestCtx<TRequest, TResponse>(request, path, database, result, edgeActor);
    runtime.Register(factory(ctx));
    runtime.GrabEdgeEvent<NActors::TEvents::TEvWakeup>(edgeActor, waitTimeout);
    UNIT_ASSERT_C(result->ResultStatus, "The operation is still in progress");
    return result;
}

class TEnableScheduleForRootGuard {
public:
    explicit TEnableScheduleForRootGuard(NActors::TTestActorRuntime& runtime)
        : Runtime(runtime)
        , RootActorId(std::make_shared<TActorId>())
    {
        PrevObserver = Runtime.SetRegistrationObserverFunc(
            [rootActorId = RootActorId](
                TTestActorRuntimeBase& rt,
                const TActorId& parentId,
                const TActorId& actorId)
            {
                if (actorId == *rootActorId || parentId == *rootActorId) {
                    rt.EnableScheduleForActor(actorId);
                }
            });
    }

    ~TEnableScheduleForRootGuard() {
        Runtime.SetRegistrationObserverFunc(std::move(PrevObserver));
    }

    TEnableScheduleForRootGuard(const TEnableScheduleForRootGuard&) = delete;
    TEnableScheduleForRootGuard& operator=(const TEnableScheduleForRootGuard&) = delete;

    void SetRoot(const TActorId& actorId) {
        *RootActorId = actorId;
        Runtime.EnableScheduleForActor(actorId, true);
    }

    const TActorId& GetRoot() const {
        return *RootActorId;
    }

private:
    NActors::TTestActorRuntime& Runtime;
    std::shared_ptr<TActorId> RootActorId;
    TTestActorRuntimeBase::TRegistrationObserver PrevObserver;
};

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

template <typename TResult, typename TResponse>
const TResult& GetResult(const std::shared_ptr<TResultHolder<TResponse>>& result) {
    auto* typed = dynamic_cast<const TResult*>(result->Response.get());
    UNIT_ASSERT(typed);
    return *typed;
}

Ydb::Topic::CreateTopicRequest MakeCreateTopicRequest(const TString& path, ui32 partitions = 1) {
    Ydb::Topic::CreateTopicRequest request;
    request.set_path(path);
    request.mutable_partitioning_settings()->set_min_active_partitions(partitions);
    auto* consumer = request.add_consumers();
    consumer->set_name("user");
    return request;
}

void CreateTopic(NActors::TTestActorRuntime& runtime, const TString& path, ui32 partitions = 1) {
    auto result = DoActorRequest<Ydb::Topic::CreateTopicRequest, Ydb::Topic::CreateTopicResponse>(
        runtime, MakeCreateTopicRequest(path, partitions), CreateCreateTopicActor, path);
    AssertStatus(result, Ydb::StatusIds::SUCCESS);
}

THolder<TEvPQProxy::TEvPartitionLocationResponse> DoPartitionsLocationRequest(
    NActors::TTestActorRuntime& runtime,
    const TGetPartitionsLocationRequest& request,
    TDuration waitTimeout = TDuration::Seconds(30))
{
    const auto edge = runtime.AllocateEdgeActor();
    TEnableScheduleForRootGuard schedule(runtime);
    schedule.SetRoot(runtime.Register(CreatePartitionsLocationActor(edge, request)));
    runtime.DispatchEvents();
    auto handle = runtime.GrabEdgeEvent<TEvPQProxy::TEvPartitionLocationResponse>(edge, waitTimeout);
    UNIT_ASSERT(handle);
    return THolder(handle->Release());
}

auto BreakFirstLocationForward(NActors::TTestActorRuntime& runtime, size_t& broken) {
    auto* rt = &runtime;
    return runtime.AddObserver<TEvPipeCache::TEvForward>(
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
}

auto InjectFalseLocationStatusOnce(NActors::TTestActorRuntime& runtime, size_t& injected) {
    auto* rt = &runtime;
    return runtime.AddObserver<TEvPipeCache::TEvForward>(
        [&injected, rt](TEvPipeCache::TEvForward::TPtr& ev) {
            if (!ev || !ev->Get()->Ev) {
                return;
            }
            if (ev->Get()->Ev->Type() != TEvPersQueue::TEvGetPartitionsLocation::EventType) {
                return;
            }
            if (injected >= 1) {
                return;
            }
            ++injected;
            auto* response = new TEvPersQueue::TEvGetPartitionsLocationResponse();
            response->Record.SetStatus(false);
            rt->Send(new IEventHandle(ev->Sender, ev->Recipient, response));
            ev.Reset();
        });
}

} // namespace

Y_UNIT_TEST_SUITE(SchemaOps_TopicAPI) {

Y_UNIT_TEST(DescribePartitionSmokeAndMissing) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_describe_partition_smoke";
    CreateTopic(runtime, path);

    {
        Ydb::Topic::DescribePartitionRequest request;
        request.set_path(path);
        request.set_partition_id(0);
        auto result = DoActorRequest<Ydb::Topic::DescribePartitionRequest, Ydb::Topic::DescribePartitionResponse>(
            runtime, request, CreateDescribePartitionActor, path);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
        const auto& describeResult = GetResult<Ydb::Topic::DescribePartitionResult>(result);
        UNIT_ASSERT_VALUES_EQUAL(describeResult.partition().partition_id(), 0u);
        UNIT_ASSERT(describeResult.partition().active());
        UNIT_ASSERT(!describeResult.partition().has_partition_location());
        UNIT_ASSERT(!describeResult.partition().has_partition_stats());
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

Y_UNIT_TEST(DescribePartitionWithLocationAndStats) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_describe_partition_loc_stats";
    CreateTopic(runtime, path);

    {
        Ydb::Topic::DescribePartitionRequest request;
        request.set_path(path);
        request.set_partition_id(0);
        request.set_include_location(true);
        auto result = DoActorRequest<Ydb::Topic::DescribePartitionRequest, Ydb::Topic::DescribePartitionResponse>(
            runtime, request, CreateDescribePartitionActor, path);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
        const auto& describeResult = GetResult<Ydb::Topic::DescribePartitionResult>(result);
        UNIT_ASSERT(describeResult.partition().has_partition_location());
        UNIT_ASSERT_GT(describeResult.partition().partition_location().node_id(), 0);
        UNIT_ASSERT_GT(describeResult.partition().partition_location().generation(), 0);
        UNIT_ASSERT(!describeResult.partition().has_partition_stats());
    }

    {
        Ydb::Topic::DescribePartitionRequest request;
        request.set_path(path);
        request.set_partition_id(0);
        request.set_include_stats(true);
        auto result = DoActorRequest<Ydb::Topic::DescribePartitionRequest, Ydb::Topic::DescribePartitionResponse>(
            runtime, request, CreateDescribePartitionActor, path);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
        const auto& describeResult = GetResult<Ydb::Topic::DescribePartitionResult>(result);
        UNIT_ASSERT(describeResult.partition().has_partition_stats());
        UNIT_ASSERT_GT(describeResult.partition().partition_stats().partition_node_id(), 0);
        UNIT_ASSERT(!describeResult.partition().has_partition_location());
    }

    {
        Ydb::Topic::DescribePartitionRequest request;
        request.set_path(path);
        request.set_partition_id(0);
        request.set_include_location(true);
        request.set_include_stats(true);
        auto result = DoActorRequest<Ydb::Topic::DescribePartitionRequest, Ydb::Topic::DescribePartitionResponse>(
            runtime, request, CreateDescribePartitionActor, path);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
        const auto& describeResult = GetResult<Ydb::Topic::DescribePartitionResult>(result);
        UNIT_ASSERT(describeResult.partition().has_partition_location());
        UNIT_ASSERT(describeResult.partition().has_partition_stats());
        UNIT_ASSERT_VALUES_EQUAL(
            describeResult.partition().partition_stats().partition_node_id(),
            describeResult.partition().partition_location().node_id());
    }
}

Y_UNIT_TEST(DescribePartitionRetriesOnLocationDeliveryProblem) {
    auto server = CreateSimulatedServer();
    auto& runtime = server->GetRuntime();
    const TString path = "/Root/topic_describe_part_delivery_problem";
    CreateTopic(runtime, path);

    size_t broken = 0;
    auto breakObserver = BreakFirstLocationForward(runtime, broken);

    Ydb::Topic::DescribePartitionRequest request;
    request.set_path(path);
    request.set_partition_id(0);
    request.set_include_location(true);
    auto result = DoActorRequest<Ydb::Topic::DescribePartitionRequest, Ydb::Topic::DescribePartitionResponse>(
        runtime, request, CreateDescribePartitionActor, path);

    UNIT_ASSERT_VALUES_EQUAL(broken, 1u);
    AssertStatus(result, Ydb::StatusIds::SUCCESS);
    const auto& describeResult = GetResult<Ydb::Topic::DescribePartitionResult>(result);
    UNIT_ASSERT(describeResult.partition().has_partition_location());
    UNIT_ASSERT_GT(describeResult.partition().partition_location().node_id(), 0);

    // Same server: retry on false Status from balancer.
    {
        const TString path2 = "/Root/topic_describe_part_false_status";
        CreateTopic(runtime, path2);
        size_t injected = 0;
        auto injectObserver = InjectFalseLocationStatusOnce(runtime, injected);

        Ydb::Topic::DescribePartitionRequest request2;
        request2.set_path(path2);
        request2.set_partition_id(0);
        request2.set_include_location(true);
        auto result2 = DoActorRequest<Ydb::Topic::DescribePartitionRequest, Ydb::Topic::DescribePartitionResponse>(
            runtime, request2, CreateDescribePartitionActor, path2);
        UNIT_ASSERT_VALUES_EQUAL(injected, 1u);
        AssertStatus(result2, Ydb::StatusIds::SUCCESS);
        const auto& describeResult2 = GetResult<Ydb::Topic::DescribePartitionResult>(result2);
        UNIT_ASSERT(describeResult2.partition().has_partition_location());
    }
}

Y_UNIT_TEST(DescribePartitionTimesOutWhenLocationStuck) {
    auto server = CreateSimulatedServer();
    auto& runtime = server->GetRuntime();
    const TString path = "/Root/topic_describe_part_timeout";
    CreateTopic(runtime, path);

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

    TEnableScheduleForRootGuard schedule(runtime);
    schedule.SetRoot(runtime.Register(CreateDescribePartitionActor(ctx)));

    runtime.DispatchEvents(TDispatchOptions{}, TDuration::MilliSeconds(100));
    runtime.AdvanceCurrentTime(TDuration::Seconds(31));

    runtime.GrabEdgeEvent<NActors::TEvents::TEvWakeup>(edgeActor, TDuration::Seconds(5));
    UNIT_ASSERT_C(result->ResultStatus, "The operation is still in progress");
    AssertStatus(result, Ydb::StatusIds::TIMEOUT, "Describe request timed out");
}

Y_UNIT_TEST(DescribePartitionUnauthenticatedRejectedWhenRequired) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_describe_part_auth";
    CreateTopic(runtime, path);
    runtime.GetAppData().PQConfig.SetRequireCredentialsInNewProtocol(true);

    Ydb::Topic::DescribePartitionRequest request;
    request.set_path(path);
    request.set_partition_id(0);
    auto result = DoActorRequest<Ydb::Topic::DescribePartitionRequest, Ydb::Topic::DescribePartitionResponse>(
        runtime, request, CreateDescribePartitionActor, path);
    AssertStatus(result, Ydb::StatusIds::UNAUTHORIZED, "Unauthenticated access is forbidden");
}

Y_UNIT_TEST(DescribeConsumerSmokeAndMissingTopic) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_describe_consumer_smoke";
    CreateTopic(runtime, path);

    {
        Ydb::Topic::DescribeConsumerRequest request;
        request.set_path(path);
        request.set_consumer("user");
        auto result = DoActorRequest<Ydb::Topic::DescribeConsumerRequest, Ydb::Topic::DescribeConsumerResponse>(
            runtime, request, CreateDescribeConsumerActor, path);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
        const auto& describeResult = GetResult<Ydb::Topic::DescribeConsumerResult>(result);
        UNIT_ASSERT_VALUES_EQUAL(describeResult.consumer().name(), "user");
        UNIT_ASSERT(describeResult.self().name().EndsWith("/user"));
        UNIT_ASSERT_VALUES_EQUAL(describeResult.partitions_size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(describeResult.partitions(0).partition_id(), 0u);
        UNIT_ASSERT(describeResult.partitions(0).active());
    }

    {
        Ydb::Topic::DescribeConsumerRequest request;
        request.set_path("/Root/not_a_topic");
        request.set_consumer("user");
        auto result = DoActorRequest<Ydb::Topic::DescribeConsumerRequest, Ydb::Topic::DescribeConsumerResponse>(
            runtime, request, CreateDescribeConsumerActor, request.path());
        AssertStatus(result, Ydb::StatusIds::SCHEME_ERROR);
    }
}

Y_UNIT_TEST(DescribeConsumerWithLocationAndStats) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_describe_consumer_loc_stats";
    CreateTopic(runtime, path, /*partitions=*/2);

    {
        Ydb::Topic::DescribeConsumerRequest request;
        request.set_path(path);
        request.set_consumer("user");
        request.set_include_location(true);
        auto result = DoActorRequest<Ydb::Topic::DescribeConsumerRequest, Ydb::Topic::DescribeConsumerResponse>(
            runtime, request, CreateDescribeConsumerActor, path);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
        const auto& describeResult = GetResult<Ydb::Topic::DescribeConsumerResult>(result);
        UNIT_ASSERT_VALUES_EQUAL(describeResult.partitions_size(), 2u);
        for (const auto& p : describeResult.partitions()) {
            UNIT_ASSERT(p.has_partition_location());
            UNIT_ASSERT_GT(p.partition_location().node_id(), 0);
            UNIT_ASSERT(!p.has_partition_stats());
            UNIT_ASSERT(!p.has_partition_consumer_stats());
        }
    }

    {
        Ydb::Topic::DescribeConsumerRequest request;
        request.set_path(path);
        request.set_consumer("user");
        request.set_include_stats(true);
        auto result = DoActorRequest<Ydb::Topic::DescribeConsumerRequest, Ydb::Topic::DescribeConsumerResponse>(
            runtime, request, CreateDescribeConsumerActor, path);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
        const auto& describeResult = GetResult<Ydb::Topic::DescribeConsumerResult>(result);
        for (const auto& p : describeResult.partitions()) {
            UNIT_ASSERT(p.has_partition_stats());
            UNIT_ASSERT(p.has_partition_consumer_stats());
            UNIT_ASSERT_GT(p.partition_stats().partition_node_id(), 0);
        }
    }

    {
        Ydb::Topic::DescribeConsumerRequest request;
        request.set_path(path);
        request.set_consumer("user");
        request.set_include_location(true);
        request.set_include_stats(true);
        auto result = DoActorRequest<Ydb::Topic::DescribeConsumerRequest, Ydb::Topic::DescribeConsumerResponse>(
            runtime, request, CreateDescribeConsumerActor, path);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
        const auto& describeResult = GetResult<Ydb::Topic::DescribeConsumerResult>(result);
        for (const auto& p : describeResult.partitions()) {
            UNIT_ASSERT(p.has_partition_location());
            UNIT_ASSERT(p.has_partition_stats());
            UNIT_ASSERT(p.has_partition_consumer_stats());
        }
    }
}

Y_UNIT_TEST(DescribeUnknownConsumer) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_describe_unknown_consumer";
    CreateTopic(runtime, path);

    Ydb::Topic::DescribeConsumerRequest request;
    request.set_path(path);
    request.set_consumer("missing_user");
    auto result = DoActorRequest<Ydb::Topic::DescribeConsumerRequest, Ydb::Topic::DescribeConsumerResponse>(
        runtime, request, CreateDescribeConsumerActor, path);
    AssertStatus(result, Ydb::StatusIds::SCHEME_ERROR);
}

Y_UNIT_TEST(DescribeConsumerRetriesOnLocationDeliveryProblem) {
    auto server = CreateSimulatedServer();
    auto& runtime = server->GetRuntime();
    const TString path = "/Root/topic_describe_consumer_retry";
    CreateTopic(runtime, path);

    size_t broken = 0;
    auto breakObserver = BreakFirstLocationForward(runtime, broken);

    Ydb::Topic::DescribeConsumerRequest request;
    request.set_path(path);
    request.set_consumer("user");
    request.set_include_location(true);
    auto result = DoActorRequest<Ydb::Topic::DescribeConsumerRequest, Ydb::Topic::DescribeConsumerResponse>(
        runtime, request, CreateDescribeConsumerActor, path);

    UNIT_ASSERT_VALUES_EQUAL(broken, 1u);
    AssertStatus(result, Ydb::StatusIds::SUCCESS);
    const auto& describeResult = GetResult<Ydb::Topic::DescribeConsumerResult>(result);
    UNIT_ASSERT(describeResult.partitions(0).has_partition_location());

    {
        const TString path2 = "/Root/topic_describe_consumer_false_status";
        CreateTopic(runtime, path2);
        size_t injected = 0;
        auto injectObserver = InjectFalseLocationStatusOnce(runtime, injected);

        Ydb::Topic::DescribeConsumerRequest request2;
        request2.set_path(path2);
        request2.set_consumer("user");
        request2.set_include_location(true);
        auto result2 = DoActorRequest<Ydb::Topic::DescribeConsumerRequest, Ydb::Topic::DescribeConsumerResponse>(
            runtime, request2, CreateDescribeConsumerActor, path2);
        UNIT_ASSERT_VALUES_EQUAL(injected, 1u);
        AssertStatus(result2, Ydb::StatusIds::SUCCESS);
        const auto& describeResult2 = GetResult<Ydb::Topic::DescribeConsumerResult>(result2);
        UNIT_ASSERT(describeResult2.partitions(0).has_partition_location());
    }
}

Y_UNIT_TEST(DescribeConsumerTimesOutWhenLocationStuck) {
    auto server = CreateSimulatedServer();
    auto& runtime = server->GetRuntime();
    const TString path = "/Root/topic_describe_consumer_timeout";
    CreateTopic(runtime, path);

    auto dropObserver = runtime.AddObserver<TEvPipeCache::TEvForward>(
        [](TEvPipeCache::TEvForward::TPtr& ev) {
            if (ev && ev->Get()->Ev &&
                ev->Get()->Ev->Type() == TEvPersQueue::TEvGetPartitionsLocation::EventType)
            {
                ev.Reset();
            }
        });

    Ydb::Topic::DescribeConsumerRequest request;
    request.set_path(path);
    request.set_consumer("user");
    request.set_include_location(true);

    auto result = std::make_shared<TResultHolder<Ydb::Topic::DescribeConsumerResponse>>();
    auto edgeActor = runtime.AllocateEdgeActor();
    auto* ctx = new TRequestCtx<Ydb::Topic::DescribeConsumerRequest, Ydb::Topic::DescribeConsumerResponse>(
        request, path, "/Root", result, edgeActor);

    TEnableScheduleForRootGuard schedule(runtime);
    schedule.SetRoot(runtime.Register(CreateDescribeConsumerActor(ctx)));

    runtime.DispatchEvents(TDispatchOptions{}, TDuration::MilliSeconds(100));
    runtime.AdvanceCurrentTime(TDuration::Seconds(31));

    runtime.GrabEdgeEvent<NActors::TEvents::TEvWakeup>(edgeActor, TDuration::Seconds(5));
    UNIT_ASSERT_C(result->ResultStatus, "The operation is still in progress");
    AssertStatus(result, Ydb::StatusIds::TIMEOUT, "Describe request timed out");
}

Y_UNIT_TEST(DescribeConsumerUnauthenticatedRejectedWhenRequired) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_describe_consumer_auth";
    CreateTopic(runtime, path);
    runtime.GetAppData().PQConfig.SetRequireCredentialsInNewProtocol(true);

    Ydb::Topic::DescribeConsumerRequest request;
    request.set_path(path);
    request.set_consumer("user");
    auto result = DoActorRequest<Ydb::Topic::DescribeConsumerRequest, Ydb::Topic::DescribeConsumerResponse>(
        runtime, request, CreateDescribeConsumerActor, path);
    AssertStatus(result, Ydb::StatusIds::UNAUTHORIZED, "Unauthenticated access is forbidden");
}

Y_UNIT_TEST(DescribeTopicSmokeAndMissing) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_describe_topic_smoke";
    CreateTopic(runtime, path);

    {
        Ydb::Topic::DescribeTopicRequest request;
        request.set_path(path);
        auto result = DoActorRequest<Ydb::Topic::DescribeTopicRequest, Ydb::Topic::DescribeTopicResponse>(
            runtime, request, CreateDescribeTopicActor, path);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
        const auto& describeResult = GetResult<Ydb::Topic::DescribeTopicResult>(result);
        UNIT_ASSERT_VALUES_EQUAL(describeResult.partitions_size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(describeResult.partitions(0).partition_id(), 0u);
        UNIT_ASSERT(describeResult.partitions(0).active());
        UNIT_ASSERT(!describeResult.partitions(0).has_partition_location());
        UNIT_ASSERT(!describeResult.partitions(0).has_partition_stats());
        UNIT_ASSERT(!describeResult.has_topic_stats());
        UNIT_ASSERT_VALUES_EQUAL(describeResult.consumers_size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(describeResult.consumers(0).name(), "user");
        UNIT_ASSERT(!describeResult.consumers(0).has_consumer_stats());
    }

    {
        Ydb::Topic::DescribeTopicRequest request;
        request.set_path("/Root/not_a_topic");
        auto result = DoActorRequest<Ydb::Topic::DescribeTopicRequest, Ydb::Topic::DescribeTopicResponse>(
            runtime, request, CreateDescribeTopicActor, request.path());
        AssertStatus(result, Ydb::StatusIds::SCHEME_ERROR);
    }
}

Y_UNIT_TEST(DescribeTopicWithLocationAndStats) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_describe_topic_loc_stats";
    CreateTopic(runtime, path, /*partitions=*/2);

    {
        Ydb::Topic::DescribeTopicRequest request;
        request.set_path(path);
        request.set_include_location(true);
        auto result = DoActorRequest<Ydb::Topic::DescribeTopicRequest, Ydb::Topic::DescribeTopicResponse>(
            runtime, request, CreateDescribeTopicActor, path);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
        const auto& describeResult = GetResult<Ydb::Topic::DescribeTopicResult>(result);
        UNIT_ASSERT_VALUES_EQUAL(describeResult.partitions_size(), 2u);
        UNIT_ASSERT(!describeResult.has_topic_stats());
        for (const auto& p : describeResult.partitions()) {
            UNIT_ASSERT(p.has_partition_location());
            UNIT_ASSERT_GT(p.partition_location().node_id(), 0);
            UNIT_ASSERT_GT(p.partition_location().generation(), 0);
            UNIT_ASSERT(!p.has_partition_stats());
        }
    }

    {
        Ydb::Topic::DescribeTopicRequest request;
        request.set_path(path);
        request.set_include_stats(true);
        auto result = DoActorRequest<Ydb::Topic::DescribeTopicRequest, Ydb::Topic::DescribeTopicResponse>(
            runtime, request, CreateDescribeTopicActor, path);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
        const auto& describeResult = GetResult<Ydb::Topic::DescribeTopicResult>(result);
        UNIT_ASSERT(describeResult.has_topic_stats());
        for (const auto& p : describeResult.partitions()) {
            UNIT_ASSERT(p.has_partition_stats());
            UNIT_ASSERT_GT(p.partition_stats().partition_node_id(), 0);
            UNIT_ASSERT(!p.has_partition_location());
        }
        UNIT_ASSERT_VALUES_EQUAL(describeResult.consumers_size(), 1u);
        UNIT_ASSERT(describeResult.consumers(0).has_consumer_stats());
        ui64 storeSum = 0;
        for (const auto& p : describeResult.partitions()) {
            storeSum += p.partition_stats().store_size_bytes();
        }
        UNIT_ASSERT_VALUES_EQUAL(describeResult.topic_stats().store_size_bytes(), storeSum);
    }

    {
        Ydb::Topic::DescribeTopicRequest request;
        request.set_path(path);
        request.set_include_location(true);
        request.set_include_stats(true);
        auto result = DoActorRequest<Ydb::Topic::DescribeTopicRequest, Ydb::Topic::DescribeTopicResponse>(
            runtime, request, CreateDescribeTopicActor, path);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
        const auto& describeResult = GetResult<Ydb::Topic::DescribeTopicResult>(result);
        UNIT_ASSERT(describeResult.has_topic_stats());
        for (const auto& p : describeResult.partitions()) {
            UNIT_ASSERT(p.has_partition_location());
            UNIT_ASSERT(p.has_partition_stats());
            UNIT_ASSERT_VALUES_EQUAL(
                p.partition_stats().partition_node_id(),
                p.partition_location().node_id());
        }
    }
}

Y_UNIT_TEST(DescribeTopicRetriesOnLocationDeliveryProblem) {
    auto server = CreateSimulatedServer();
    auto& runtime = server->GetRuntime();
    const TString path = "/Root/topic_describe_topic_retry";
    CreateTopic(runtime, path);

    size_t broken = 0;
    auto breakObserver = BreakFirstLocationForward(runtime, broken);

    Ydb::Topic::DescribeTopicRequest request;
    request.set_path(path);
    request.set_include_location(true);
    auto result = DoActorRequest<Ydb::Topic::DescribeTopicRequest, Ydb::Topic::DescribeTopicResponse>(
        runtime, request, CreateDescribeTopicActor, path);

    UNIT_ASSERT_VALUES_EQUAL(broken, 1u);
    AssertStatus(result, Ydb::StatusIds::SUCCESS);
    const auto& describeResult = GetResult<Ydb::Topic::DescribeTopicResult>(result);
    UNIT_ASSERT(describeResult.partitions(0).has_partition_location());

    {
        const TString path2 = "/Root/topic_describe_topic_false_status";
        CreateTopic(runtime, path2);
        size_t injected = 0;
        auto injectObserver = InjectFalseLocationStatusOnce(runtime, injected);

        Ydb::Topic::DescribeTopicRequest request2;
        request2.set_path(path2);
        request2.set_include_location(true);
        auto result2 = DoActorRequest<Ydb::Topic::DescribeTopicRequest, Ydb::Topic::DescribeTopicResponse>(
            runtime, request2, CreateDescribeTopicActor, path2);
        UNIT_ASSERT_VALUES_EQUAL(injected, 1u);
        AssertStatus(result2, Ydb::StatusIds::SUCCESS);
        const auto& describeResult2 = GetResult<Ydb::Topic::DescribeTopicResult>(result2);
        UNIT_ASSERT(describeResult2.partitions(0).has_partition_location());
    }
}

Y_UNIT_TEST(DescribeTopicTimesOutWhenLocationStuck) {
    auto server = CreateSimulatedServer();
    auto& runtime = server->GetRuntime();
    const TString path = "/Root/topic_describe_topic_timeout";
    CreateTopic(runtime, path);

    auto dropObserver = runtime.AddObserver<TEvPipeCache::TEvForward>(
        [](TEvPipeCache::TEvForward::TPtr& ev) {
            if (ev && ev->Get()->Ev &&
                ev->Get()->Ev->Type() == TEvPersQueue::TEvGetPartitionsLocation::EventType)
            {
                ev.Reset();
            }
        });

    Ydb::Topic::DescribeTopicRequest request;
    request.set_path(path);
    request.set_include_location(true);

    auto result = std::make_shared<TResultHolder<Ydb::Topic::DescribeTopicResponse>>();
    auto edgeActor = runtime.AllocateEdgeActor();
    auto* ctx = new TRequestCtx<Ydb::Topic::DescribeTopicRequest, Ydb::Topic::DescribeTopicResponse>(
        request, path, "/Root", result, edgeActor);

    TEnableScheduleForRootGuard schedule(runtime);
    schedule.SetRoot(runtime.Register(CreateDescribeTopicActor(ctx)));

    runtime.DispatchEvents(TDispatchOptions{}, TDuration::MilliSeconds(100));
    runtime.AdvanceCurrentTime(TDuration::Seconds(31));

    runtime.GrabEdgeEvent<NActors::TEvents::TEvWakeup>(edgeActor, TDuration::Seconds(5));
    UNIT_ASSERT_C(result->ResultStatus, "The operation is still in progress");
    AssertStatus(result, Ydb::StatusIds::TIMEOUT, "Describe request timed out");
}

Y_UNIT_TEST(DescribeTopicUnauthenticatedRejectedWhenRequired) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_describe_topic_auth";
    CreateTopic(runtime, path);
    runtime.GetAppData().PQConfig.SetRequireCredentialsInNewProtocol(true);

    Ydb::Topic::DescribeTopicRequest request;
    request.set_path(path);
    auto result = DoActorRequest<Ydb::Topic::DescribeTopicRequest, Ydb::Topic::DescribeTopicResponse>(
        runtime, request, CreateDescribeTopicActor, path);
    AssertStatus(result, Ydb::StatusIds::UNAUTHORIZED, "Unauthenticated access is forbidden");
}

Y_UNIT_TEST(DescribeTopicInternalRequestAllowedWithoutToken) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_describe_topic_internal";
    CreateTopic(runtime, path);
    runtime.GetAppData().PQConfig.SetRequireCredentialsInNewProtocol(true);

    Ydb::Topic::DescribeTopicRequest request;
    request.set_path(path);
    auto result = std::make_shared<TResultHolder<Ydb::Topic::DescribeTopicResponse>>();
    auto edgeActor = runtime.AllocateEdgeActor();
    auto* ctx = new TInternalRequestCtx<Ydb::Topic::DescribeTopicRequest, Ydb::Topic::DescribeTopicResponse>(
        request, path, "/Root", result, edgeActor);
    runtime.Register(CreateDescribeTopicActor(ctx));
    runtime.GrabEdgeEvent<NActors::TEvents::TEvWakeup>(edgeActor, TDuration::Seconds(30));
    UNIT_ASSERT_C(result->ResultStatus, "The operation is still in progress");
    AssertStatus(result, Ydb::StatusIds::SUCCESS);
}

Y_UNIT_TEST(PartitionsLocationSmokeAndErrors) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_partitions_location_smoke";
    CreateTopic(runtime, path, /*partitions=*/3);

    {
        auto ev = DoPartitionsLocationRequest(runtime, {path, "/Root", "", {}});
        UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(ev->Partitions.size(), 3u);
        UNIT_ASSERT_GT(ev->PathId, 0);
        UNIT_ASSERT_GT(ev->SchemeShardId, 0);
        for (const auto& p : ev->Partitions) {
            UNIT_ASSERT_GT(p.NodeId, 0);
            UNIT_ASSERT_GT(p.Generation, 0);
            UNIT_ASSERT_LT(p.PartitionId, 3);
        }
    }

    {
        auto ev = DoPartitionsLocationRequest(runtime, {path, "/Root", "", {0, 2}});
        UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(ev->Partitions.size(), 2u);
    }

    {
        auto ev = DoPartitionsLocationRequest(runtime, {path, "/Root", "", {1, 1, 1}});
        UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(ev->Partitions.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Partitions[0].PartitionId, 1u);
    }

    {
        auto ev = DoPartitionsLocationRequest(runtime, {path, "/Root", "", {1000}});
        UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT(ev->Issues.ToString().Contains("No partition"));
    }

    {
        auto ev = DoPartitionsLocationRequest(runtime, {"/Root/missing_topic", "/Root", "", {}});
        UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::SCHEME_ERROR);
    }
}

Y_UNIT_TEST(PartitionsLocationRetriesOnDeliveryProblem) {
    auto server = CreateSimulatedServer();
    auto& runtime = server->GetRuntime();
    const TString path = "/Root/topic_partitions_location_retry";
    CreateTopic(runtime, path);

    size_t broken = 0;
    auto breakObserver = BreakFirstLocationForward(runtime, broken);
    auto ev = DoPartitionsLocationRequest(runtime, {path, "/Root", "", {0}});
    UNIT_ASSERT_VALUES_EQUAL(broken, 1u);
    UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(ev->Partitions.size(), 1u);
    UNIT_ASSERT_GT(ev->Partitions[0].NodeId, 0);
}

Y_UNIT_TEST(PartitionsLocationTimesOutWhenStuck) {
    auto server = CreateSimulatedServer();
    auto& runtime = server->GetRuntime();
    const TString path = "/Root/topic_partitions_location_timeout";
    CreateTopic(runtime, path);

    auto dropObserver = runtime.AddObserver<TEvPipeCache::TEvForward>(
        [](TEvPipeCache::TEvForward::TPtr& ev) {
            if (ev && ev->Get()->Ev &&
                ev->Get()->Ev->Type() == TEvPersQueue::TEvGetPartitionsLocation::EventType)
            {
                ev.Reset();
            }
        });

    const auto edge = runtime.AllocateEdgeActor();
    TEnableScheduleForRootGuard schedule(runtime);
    schedule.SetRoot(runtime.Register(CreatePartitionsLocationActor(
        edge, TGetPartitionsLocationRequest{path, "/Root", "", {0}})));

    runtime.DispatchEvents(TDispatchOptions{}, TDuration::MilliSeconds(100));
    runtime.AdvanceCurrentTime(TDuration::Seconds(31));

    auto handle = runtime.GrabEdgeEvent<TEvPQProxy::TEvPartitionLocationResponse>(edge, TDuration::Seconds(5));
    UNIT_ASSERT(handle);
    const auto* ev = handle->Get();
    UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::TIMEOUT);
    UNIT_ASSERT(ev->Issues.ToString().Contains("timed out"));
}

Y_UNIT_TEST(PartitionsLocationUnauthenticatedRejectedWhenRequired) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_partitions_location_auth";
    CreateTopic(runtime, path);
    runtime.GetAppData().PQConfig.SetRequireCredentialsInNewProtocol(true);

    auto ev = DoPartitionsLocationRequest(runtime, {path, "/Root", "", {}});
    UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::UNAUTHORIZED);
    UNIT_ASSERT(ev->Issues.ToString().Contains("Unauthenticated access is forbidden"));
}

Y_UNIT_TEST(DropTopicSuccessAndMissing) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_drop";
    CreateTopic(runtime, path);

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

Y_UNIT_TEST(AlterTopicSuccessAndMissing) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_alter";
    CreateTopic(runtime, path);

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        request.mutable_set_retention_period()->set_seconds(3600);
        auto result = DoActorRequest<Ydb::Topic::AlterTopicRequest, Ydb::Topic::AlterTopicResponse>(
            runtime, request, CreateAlterTopicActor, path);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
    }

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path("/Root/missing_topic");
        request.mutable_set_retention_period()->set_seconds(3600);
        auto result = DoActorRequest<Ydb::Topic::AlterTopicRequest, Ydb::Topic::AlterTopicResponse>(
            runtime, request, CreateAlterTopicActor, request.path());
        AssertStatus(result, Ydb::StatusIds::SCHEME_ERROR);
    }
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

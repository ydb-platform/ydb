#include "actors.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/persqueue/public/utils.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/grpc_request/grpc_request.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/test_server.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>
#include <ydb/services/persqueue_v1/actors/events.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/async.h>

#include <util/generic/hash_set.h>
#include <util/generic/size_literals.h>
#include <util/generic/vector.h>
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

void CreateTopic(
    NActors::TTestActorRuntime& runtime,
    const TString& path,
    ui32 partitions = 1,
    const TString& database = "/Root")
{
    auto result = DoActorRequest<Ydb::Topic::CreateTopicRequest, Ydb::Topic::CreateTopicResponse>(
        runtime, MakeCreateTopicRequest(path, partitions), CreateCreateTopicActor, path, database);
    AssertStatus(result, Ydb::StatusIds::SUCCESS);
}

Ydb::Topic::DescribeTopicResult DescribeTopic(
    NActors::TTestActorRuntime& runtime,
    const TString& path)
{
    Ydb::Topic::DescribeTopicRequest request;
    request.set_path(path);
    auto result = DoActorRequest<Ydb::Topic::DescribeTopicRequest, Ydb::Topic::DescribeTopicResponse>(
        runtime, request, CreateDescribeTopicActor, path);
    AssertStatus(result, Ydb::StatusIds::SUCCESS);
    return GetResult<Ydb::Topic::DescribeTopicResult>(result);
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

        const auto describe = DescribeTopic(runtime, name);
        UNIT_ASSERT_VALUES_EQUAL_C(describe.partitioning_settings().min_active_partitions(), 1, name);
    }
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

void StripPartitionFromNavigateResult(
    TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev,
    ui32 partitionId,
    bool onlyWithoutSync)
{
    if (!ev || !ev->Get()->Request) {
        return;
    }
    for (auto& entry : ev->Get()->Request->ResultSet) {
        if (!entry.PQGroupInfo) {
            continue;
        }
        if (onlyWithoutSync && entry.SyncVersion) {
            continue;
        }
        auto copy = MakeIntrusive<NSchemeCache::TSchemeCacheNavigate::TPQGroupInfo>(*entry.PQGroupInfo);
        auto* partitions = copy->Description.MutablePartitions();
        for (int i = partitions->size() - 1; i >= 0; --i) {
            if (partitions->Get(i).GetPartitionId() == partitionId) {
                partitions->DeleteSubrange(i, 1);
            }
        }
        entry.PQGroupInfo = copy;
    }
}

void StripConsumerFromNavigateResult(
    TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev,
    const TString& consumerName,
    bool onlyWithoutSync)
{
    if (!ev || !ev->Get()->Request) {
        return;
    }
    for (auto& entry : ev->Get()->Request->ResultSet) {
        if (!entry.PQGroupInfo) {
            continue;
        }
        if (onlyWithoutSync && entry.SyncVersion) {
            continue;
        }
        auto copy = MakeIntrusive<NSchemeCache::TSchemeCacheNavigate::TPQGroupInfo>(*entry.PQGroupInfo);
        auto* consumers = copy->Description.MutablePQTabletConfig()->MutableConsumers();
        for (int i = consumers->size() - 1; i >= 0; --i) {
            if (consumers->Get(i).GetName() == consumerName) {
                consumers->DeleteSubrange(i, 1);
            }
        }
        entry.PQGroupInfo = copy;
    }
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

Y_UNIT_TEST(DescribePartitionRetriesWithSyncWhenStaleCache) {
    auto server = CreateSimulatedServer();
    auto& runtime = server->GetRuntime();
    const TString path = "/Root/topic_describe_part_stale_cache";
    CreateTopic(runtime, path, /*partitions=*/2);

    size_t staleNavigates = 0;
    size_t syncNavigates = 0;
    auto observer = runtime.AddObserver<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(
        [&](TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
            if (!ev || !ev->Get()->Request) {
                return;
            }
            for (const auto& entry : ev->Get()->Request->ResultSet) {
                if (!entry.PQGroupInfo) {
                    continue;
                }
                if (entry.SyncVersion) {
                    ++syncNavigates;
                } else {
                    ++staleNavigates;
                }
            }
            StripPartitionFromNavigateResult(ev, /*partitionId=*/1, /*onlyWithoutSync=*/true);
        });

    Ydb::Topic::DescribePartitionRequest request;
    request.set_path(path);
    request.set_partition_id(1);
    auto result = DoActorRequest<Ydb::Topic::DescribePartitionRequest, Ydb::Topic::DescribePartitionResponse>(
        runtime, request, CreateDescribePartitionActor, path);

    UNIT_ASSERT_GT(staleNavigates, 0u);
    UNIT_ASSERT_GT(syncNavigates, 0u);
    AssertStatus(result, Ydb::StatusIds::SUCCESS);
    const auto& describeResult = GetResult<Ydb::Topic::DescribePartitionResult>(result);
    UNIT_ASSERT_VALUES_EQUAL(describeResult.partition().partition_id(), 1u);
}

Y_UNIT_TEST(DescribePartitionMissingAfterSync) {
    auto server = CreateSimulatedServer();
    auto& runtime = server->GetRuntime();
    const TString path = "/Root/topic_describe_part_missing_after_sync";
    CreateTopic(runtime, path, /*partitions=*/2);

    size_t syncNavigates = 0;
    auto observer = runtime.AddObserver<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(
        [&](TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
            if (!ev || !ev->Get()->Request) {
                return;
            }
            for (const auto& entry : ev->Get()->Request->ResultSet) {
                if (entry.PQGroupInfo && entry.SyncVersion) {
                    ++syncNavigates;
                }
            }
            StripPartitionFromNavigateResult(ev, /*partitionId=*/1, /*onlyWithoutSync=*/false);
        });

    Ydb::Topic::DescribePartitionRequest request;
    request.set_path(path);
    request.set_partition_id(1);
    auto result = DoActorRequest<Ydb::Topic::DescribePartitionRequest, Ydb::Topic::DescribePartitionResponse>(
        runtime, request, CreateDescribePartitionActor, path);

    UNIT_ASSERT_GT(syncNavigates, 0u);
    AssertStatus(result, Ydb::StatusIds::BAD_REQUEST, "No partition 1 in topic");
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

Y_UNIT_TEST(DescribeConsumerRetriesWithSyncWhenStaleCache) {
    auto server = CreateSimulatedServer();
    auto& runtime = server->GetRuntime();
    const TString path = "/Root/topic_describe_consumer_stale_cache";
    CreateTopic(runtime, path);

    size_t staleNavigates = 0;
    size_t syncNavigates = 0;
    auto observer = runtime.AddObserver<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(
        [&](TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
            if (!ev || !ev->Get()->Request) {
                return;
            }
            for (const auto& entry : ev->Get()->Request->ResultSet) {
                if (!entry.PQGroupInfo) {
                    continue;
                }
                if (entry.SyncVersion) {
                    ++syncNavigates;
                } else {
                    ++staleNavigates;
                }
            }
            StripConsumerFromNavigateResult(ev, /*consumerName=*/"user", /*onlyWithoutSync=*/true);
        });

    Ydb::Topic::DescribeConsumerRequest request;
    request.set_path(path);
    request.set_consumer("user");
    auto result = DoActorRequest<Ydb::Topic::DescribeConsumerRequest, Ydb::Topic::DescribeConsumerResponse>(
        runtime, request, CreateDescribeConsumerActor, path);

    UNIT_ASSERT_GT(staleNavigates, 0u);
    UNIT_ASSERT_GT(syncNavigates, 0u);
    AssertStatus(result, Ydb::StatusIds::SUCCESS);
    const auto& describeResult = GetResult<Ydb::Topic::DescribeConsumerResult>(result);
    UNIT_ASSERT_VALUES_EQUAL(describeResult.consumer().name(), "user");
}

Y_UNIT_TEST(DescribeConsumerMissingAfterSync) {
    auto server = CreateSimulatedServer();
    auto& runtime = server->GetRuntime();
    const TString path = "/Root/topic_describe_consumer_missing_after_sync";
    CreateTopic(runtime, path);

    size_t syncNavigates = 0;
    auto observer = runtime.AddObserver<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(
        [&](TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
            if (!ev || !ev->Get()->Request) {
                return;
            }
            for (const auto& entry : ev->Get()->Request->ResultSet) {
                if (entry.PQGroupInfo && entry.SyncVersion) {
                    ++syncNavigates;
                }
            }
            StripConsumerFromNavigateResult(ev, /*consumerName=*/"user", /*onlyWithoutSync=*/false);
        });

    Ydb::Topic::DescribeConsumerRequest request;
    request.set_path(path);
    request.set_consumer("user");
    auto result = DoActorRequest<Ydb::Topic::DescribeConsumerRequest, Ydb::Topic::DescribeConsumerResponse>(
        runtime, request, CreateDescribeConsumerActor, path);

    UNIT_ASSERT_GT(syncNavigates, 0u);
    AssertStatus(result, Ydb::StatusIds::SCHEME_ERROR, "no consumer 'user' in topic");
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

Y_UNIT_TEST(PartitionsLocationRetriesWithSyncWhenStaleCache) {
    auto server = CreateSimulatedServer();
    auto& runtime = server->GetRuntime();
    const TString path = "/Root/topic_partitions_location_stale_cache";
    CreateTopic(runtime, path, /*partitions=*/2);

    size_t staleNavigates = 0;
    size_t syncNavigates = 0;
    auto observer = runtime.AddObserver<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(
        [&](TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
            if (!ev || !ev->Get()->Request) {
                return;
            }
            for (const auto& entry : ev->Get()->Request->ResultSet) {
                if (!entry.PQGroupInfo) {
                    continue;
                }
                if (entry.SyncVersion) {
                    ++syncNavigates;
                } else {
                    ++staleNavigates;
                }
            }
            StripPartitionFromNavigateResult(ev, /*partitionId=*/1, /*onlyWithoutSync=*/true);
        });

    auto ev = DoPartitionsLocationRequest(runtime, {path, "/Root", "", {1}});
    UNIT_ASSERT_GT(staleNavigates, 0u);
    UNIT_ASSERT_GT(syncNavigates, 0u);
    UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(ev->Partitions.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(ev->Partitions[0].PartitionId, 1u);
}

Y_UNIT_TEST(PartitionsLocationMissingAfterSync) {
    auto server = CreateSimulatedServer();
    auto& runtime = server->GetRuntime();
    const TString path = "/Root/topic_partitions_location_missing_after_sync";
    CreateTopic(runtime, path, /*partitions=*/2);

    size_t syncNavigates = 0;
    auto observer = runtime.AddObserver<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(
        [&](TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
            if (!ev || !ev->Get()->Request) {
                return;
            }
            for (const auto& entry : ev->Get()->Request->ResultSet) {
                if (entry.PQGroupInfo && entry.SyncVersion) {
                    ++syncNavigates;
                }
            }
            StripPartitionFromNavigateResult(ev, /*partitionId=*/1, /*onlyWithoutSync=*/false);
        });

    auto ev = DoPartitionsLocationRequest(runtime, {path, "/Root", "", {1}});
    UNIT_ASSERT_GT(syncNavigates, 0u);
    UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::BAD_REQUEST);
    UNIT_ASSERT(ev->Issues.ToString().Contains("No partition 1 in topic"));
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

Y_UNIT_TEST(CreateTopicDefaultsAndIdempotentCreate) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_create_defaults";

    CreateTopic(runtime, path);

    const auto describe = DescribeTopic(runtime, path);
    UNIT_ASSERT_VALUES_EQUAL(describe.partitioning_settings().min_active_partitions(), 1);
    UNIT_ASSERT_VALUES_EQUAL(describe.retention_period().seconds(), TDuration::Days(1).Seconds());
    UNIT_ASSERT_VALUES_EQUAL(describe.partition_write_speed_bytes_per_second(), NPQ::DEFAULT_PARTITION_SPEED);
    UNIT_ASSERT_VALUES_EQUAL(describe.partition_write_burst_bytes(), NPQ::DEFAULT_PARTITION_SPEED);
    UNIT_ASSERT_VALUES_EQUAL(
        describe.partition_write_speed_messages_per_second(),
        NPQ::DEFAULT_PARTITION_WRITE_SPEED_MESSAGES_PER_SECOND);
    UNIT_ASSERT_VALUES_EQUAL(
        describe.partition_write_burst_messages(),
        NPQ::DEFAULT_PARTITION_WRITE_SPEED_MESSAGES_PER_SECOND);
    UNIT_ASSERT_VALUES_EQUAL(describe.consumers_size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(describe.consumers(0).name(), "user");
    UNIT_ASSERT(!describe.content_based_deduplication());
    UNIT_ASSERT_VALUES_EQUAL(describe.supported_codecs().codecs_size(), 0u);
    UNIT_ASSERT_VALUES_EQUAL(describe.attributes().at("_partitions_per_tablet"), "1");

    // gRPC CreateTopic uses IfNotExists=true, so a second create is SUCCESS.
    auto duplicate = DoActorRequest<Ydb::Topic::CreateTopicRequest, Ydb::Topic::CreateTopicResponse>(
        runtime, MakeCreateTopicRequest(path), CreateCreateTopicActor, path);
    AssertStatus(duplicate, Ydb::StatusIds::SUCCESS);
}

Y_UNIT_TEST(CreateTopicWithCodecsWriteSpeedAndRetention) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_create_explicit";

    auto request = MakeCreateTopicRequest(path, /*partitions=*/3);
    request.mutable_retention_period()->set_seconds(TDuration::Hours(2).Seconds());
    request.set_partition_write_speed_bytes_per_second(9000);
    request.set_partition_write_burst_bytes(18000);
    request.set_partition_write_speed_messages_per_second(111);
    request.set_partition_write_burst_messages(222);
    request.set_content_based_deduplication(true);
    request.mutable_supported_codecs()->add_codecs(Ydb::Topic::CODEC_RAW);
    request.mutable_supported_codecs()->add_codecs(Ydb::Topic::CODEC_GZIP);

    auto result = DoActorRequest<Ydb::Topic::CreateTopicRequest, Ydb::Topic::CreateTopicResponse>(
        runtime, request, CreateCreateTopicActor, path);
    AssertStatus(result, Ydb::StatusIds::SUCCESS);

    const auto describe = DescribeTopic(runtime, path);
    UNIT_ASSERT_VALUES_EQUAL(describe.partitioning_settings().min_active_partitions(), 3);
    UNIT_ASSERT_VALUES_EQUAL(describe.partitions_size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(describe.retention_period().seconds(), TDuration::Hours(2).Seconds());
    UNIT_ASSERT_VALUES_EQUAL(describe.partition_write_speed_bytes_per_second(), 9000u);
    UNIT_ASSERT_VALUES_EQUAL(describe.partition_write_burst_bytes(), 18000u);
    UNIT_ASSERT_VALUES_EQUAL(describe.partition_write_speed_messages_per_second(), 111u);
    UNIT_ASSERT_VALUES_EQUAL(describe.partition_write_burst_messages(), 222u);
    UNIT_ASSERT(describe.content_based_deduplication());
    UNIT_ASSERT_VALUES_EQUAL(describe.supported_codecs().codecs_size(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(describe.supported_codecs().codecs(0), Ydb::Topic::CODEC_RAW);
    UNIT_ASSERT_VALUES_EQUAL(describe.supported_codecs().codecs(1), Ydb::Topic::CODEC_GZIP);
}

Y_UNIT_TEST(CreateTopicUnknownCodecRejected) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    auto request = MakeCreateTopicRequest("/Root/topic_bad_codec");
    request.mutable_supported_codecs()->add_codecs(0);
    auto result = DoActorRequest<Ydb::Topic::CreateTopicRequest, Ydb::Topic::CreateTopicResponse>(
        runtime, request, CreateCreateTopicActor, request.path());
    AssertStatus(result, Ydb::StatusIds::BAD_REQUEST, "Unknown codec");
}

Y_UNIT_TEST(CreateTopicSharedConsumer) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    runtime.GetAppData().FeatureFlags.SetEnableTopicMessageLevelParallelism(true);
    const TString path = "/Root/topic_shared_consumer";

    auto request = MakeCreateTopicRequest(path);
    request.mutable_consumers(0)->set_name("shared_c");
    request.mutable_consumers(0)->mutable_shared_consumer_type()->set_keep_messages_order(true);
    auto result = DoActorRequest<Ydb::Topic::CreateTopicRequest, Ydb::Topic::CreateTopicResponse>(
        runtime, request, CreateCreateTopicActor, path);
    AssertStatus(result, Ydb::StatusIds::SUCCESS);

    const auto config = DescribeTabletConfig(runtime, path);
    const auto* consumer = NPQ::GetConsumer(config, "shared_c");
    UNIT_ASSERT(consumer);
    UNIT_ASSERT_VALUES_EQUAL(
        NKikimrPQ::TPQTabletConfig::EConsumerType_Name(consumer->GetType()),
        NKikimrPQ::TPQTabletConfig::EConsumerType_Name(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP));
    UNIT_ASSERT(consumer->GetKeepMessageOrder());
}

Y_UNIT_TEST(CreateTopicSharedConsumerDisabledRejected) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    runtime.GetAppData().FeatureFlags.SetEnableTopicMessageLevelParallelism(false);

    auto request = MakeCreateTopicRequest("/Root/topic_shared_disabled");
    request.mutable_consumers(0)->mutable_shared_consumer_type();
    auto result = DoActorRequest<Ydb::Topic::CreateTopicRequest, Ydb::Topic::CreateTopicResponse>(
        runtime, request, CreateCreateTopicActor, request.path());
    AssertStatus(result, Ydb::StatusIds::BAD_REQUEST, "shared consumers are disabled");
}

Y_UNIT_TEST(AlterTopicAddAndDropConsumer) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_alter_consumers";
    CreateTopic(runtime, path);

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        auto* consumer = request.add_add_consumers();
        consumer->set_name("extra");
        consumer->mutable_streaming_consumer_type();
        auto result = DoActorRequest<Ydb::Topic::AlterTopicRequest, Ydb::Topic::AlterTopicResponse>(
            runtime, request, CreateAlterTopicActor, path);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
    }

    {
        const auto describe = DescribeTopic(runtime, path);
        UNIT_ASSERT_VALUES_EQUAL(describe.consumers_size(), 2u);
        THashSet<TString> names;
        for (const auto& consumer : describe.consumers()) {
            names.insert(consumer.name());
        }
        UNIT_ASSERT(names.contains("user"));
        UNIT_ASSERT(names.contains("extra"));
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
        const auto describe = DescribeTopic(runtime, path);
        UNIT_ASSERT_VALUES_EQUAL(describe.consumers_size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(describe.consumers(0).name(), "user");
    }
}

Y_UNIT_TEST(AlterTopicWriteLimits) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_alter_limits";
    CreateTopic(runtime, path);

    Ydb::Topic::AlterTopicRequest request;
    request.set_path(path);
    request.set_set_partition_write_speed_bytes_per_second(9000);
    request.set_set_partition_write_burst_bytes(18000);
    request.set_set_partition_write_speed_messages_per_second(1234);
    request.set_set_partition_write_burst_messages(5678);
    auto result = DoActorRequest<Ydb::Topic::AlterTopicRequest, Ydb::Topic::AlterTopicResponse>(
        runtime, request, CreateAlterTopicActor, path);
    AssertStatus(result, Ydb::StatusIds::SUCCESS);

    const auto describe = DescribeTopic(runtime, path);
    UNIT_ASSERT_VALUES_EQUAL(describe.partition_write_speed_bytes_per_second(), 9000u);
    UNIT_ASSERT_VALUES_EQUAL(describe.partition_write_burst_bytes(), 18000u);
    UNIT_ASSERT_VALUES_EQUAL(describe.partition_write_speed_messages_per_second(), 1234u);
    UNIT_ASSERT_VALUES_EQUAL(describe.partition_write_burst_messages(), 5678u);
}

Y_UNIT_TEST(CannotChangeConsumerType) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    runtime.GetAppData().FeatureFlags.SetEnableTopicMessageLevelParallelism(true);
    const TString path = "/Root/topic_type_change";
    CreateTopic(runtime, path);

    Ydb::Topic::AlterTopicRequest request;
    request.set_path(path);
    auto* alter = request.add_alter_consumers();
    alter->set_name("user");
    alter->mutable_alter_shared_consumer_type();
    auto result = DoActorRequest<Ydb::Topic::AlterTopicRequest, Ydb::Topic::AlterTopicResponse>(
        runtime, request, CreateAlterTopicActor, path);
    AssertStatus(result, Ydb::StatusIds::BAD_REQUEST, "Cannot alter consumer type");
}

Y_UNIT_TEST(AlterCdcAllowsRetentionAndWriteLimits) {
    auto setup = CreateSetup();
    ExecuteDDL(*setup, "CREATE TABLE table_cdc (id Uint64, PRIMARY KEY (id))");
    ExecuteDDL(*setup, "ALTER TABLE table_cdc ADD CHANGEFEED feed WITH (FORMAT = 'JSON', MODE = 'UPDATES')");

    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/table_cdc/feed";

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        request.set_set_retention_storage_mb(100);
        request.set_set_partition_write_speed_bytes_per_second(9000);
        request.set_set_partition_write_burst_bytes(100500);
        auto result = DoActorRequest<Ydb::Topic::AlterTopicRequest, Ydb::Topic::AlterTopicResponse>(
            runtime, request, CreateAlterTopicActor, path);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
    }

    const auto config = DescribeTabletConfig(runtime, path);
    const auto& partConfig = config.GetPartitionConfig();
    UNIT_ASSERT_VALUES_EQUAL(partConfig.GetStorageLimitBytes(), 100_MB);
    UNIT_ASSERT_VALUES_EQUAL(partConfig.GetWriteSpeedInBytesPerSecond(), 9000u);
    UNIT_ASSERT_VALUES_EQUAL(partConfig.GetBurstSize(), 100500u);

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        (*request.mutable_alter_attributes())["_allowed_codecs"] = "RAW";
        auto result = DoActorRequest<Ydb::Topic::AlterTopicRequest, Ydb::Topic::AlterTopicResponse>(
            runtime, request, CreateAlterTopicActor, path);
        AssertStatus(result, Ydb::StatusIds::BAD_REQUEST, "Full alter of cdc stream is forbidden");
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

Y_UNIT_TEST(FccTopicNameFormatsCreateAndDescribeAliases) {
    auto setup = CreateSetup("TopicNameFormatsFcc");
    auto& runtime = setup->GetRuntime();

    MkDir(*setup, "/Root", "fcclegacy");
    MkDir(*setup, "/Root", "fccmodern");
    MkDir(*setup, "/Root", "fccshort");

    const TVector<TString> legacyAliases = {
        "rt3.dc1--fcclegacy--topic",
        "fcclegacy--topic",
        "fcclegacy/topic",
        "/Root/fcclegacy/topic",
    };
    CreateTopic(runtime, "rt3.dc1--fcclegacy--topic");
    AssertDescribeAliases(runtime, legacyAliases, "/Root/fcclegacy/topic");

    const TVector<TString> modernAliases = {
        "rt3.dc1--fccmodern--topic",
        "fccmodern--topic",
        "fccmodern/topic",
        "/Root/fccmodern/topic",
    };
    CreateTopic(runtime, "fccmodern/topic");
    AssertDescribeAliases(runtime, modernAliases, "/Root/fccmodern/topic");

    const TVector<TString> shortAliases = {
        "rt3.dc1--fccshort--topic",
        "fccshort--topic",
        "fccshort/topic",
        "/Root/fccshort/topic",
    };
    CreateTopic(runtime, "fccshort--topic");
    AssertDescribeAliases(runtime, shortAliases, "/Root/fccshort/topic");
}

Y_UNIT_TEST(FederationTopicNameFormats) {
    auto setup = CreateSetup("TopicNameFormatsFed");
    auto& runtime = setup->GetRuntime();
    runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);

    {
        auto request = MakeCreateTopicRequest("fedshort--topic");
        auto result = DoActorRequest<Ydb::Topic::CreateTopicRequest, Ydb::Topic::CreateTopicResponse>(
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
    (*modern.mutable_attributes())["_federation_account"] = "account";
    auto modernResult = DoActorRequest<Ydb::Topic::CreateTopicRequest, Ydb::Topic::CreateTopicResponse>(
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

} // Y_UNIT_TEST_SUITE(SchemaOps_TopicAPI)

} // namespace NKikimr::NGRpcProxy::V1::NTopic

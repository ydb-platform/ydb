#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/kafka_proxy/actors/actors.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/public/schema/schema_ut_helpers.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/test_server.h>
#include <ydb/services/persqueue_v1/actors/events.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/async.h>

#include <util/generic/algorithm.h>
#include <util/thread/pool.h>

namespace NKafka::NTests {

using namespace NKikimr;
using namespace NKikimr::NPQ::NSchema::NTests;
using TEvLocationResponse = NKikimr::NGRpcProxy::V1::TEvPQProxy::TEvPartitionLocationResponse;

namespace {

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
    out->Server->EnableLogs(
        {NKikimrServices::KAFKA_PROXY, NKikimrServices::PQ_MLP_DESCRIBER},
        NActors::NLog::PRI_DEBUG);

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

void CreateTopic(NActors::TTestActorRuntime& runtime, const TString& path, ui32 partitions = 1) {
    AssertStatus(DoCreate(runtime, MakeCreateTopicRequest(path, partitions)), Ydb::StatusIds::SUCCESS);
}

THolder<TEvLocationResponse> RunTopicLocation(
    NActors::TTestActorRuntime& runtime,
    const TString& path,
    const TString& token = {},
    TDuration waitTimeout = TDuration::Seconds(30))
{
    const auto edge = runtime.AllocateEdgeActor();
    TEnableScheduleForRootGuard schedule(runtime);
    schedule.SetRoot(runtime.Register(CreateTopicLocationActor(edge, path, "/Root", token)));
    runtime.DispatchEvents();
    auto handle = runtime.GrabEdgeEvent<TEvLocationResponse>(edge, waitTimeout);
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

auto DropLocationForwards(NActors::TTestActorRuntime& runtime) {
    return runtime.AddObserver<TEvPipeCache::TEvForward>(
        [](TEvPipeCache::TEvForward::TPtr& ev) {
            if (ev && ev->Get()->Ev &&
                ev->Get()->Ev->Type() == TEvPersQueue::TEvGetPartitionsLocation::EventType)
            {
                ev.Reset();
            }
        });
}

auto CaptureLocationRequestPartitions(TEvPipeCache::TEvForward::TPtr& ev) {
    TVector<ui64> ids;
    if (!ev || !ev->Get()->Ev) {
        return ids;
    }
    if (ev->Get()->Ev->Type() != TEvPersQueue::TEvGetPartitionsLocation::EventType) {
        return ids;
    }
    auto* req = static_cast<TEvPersQueue::TEvGetPartitionsLocation*>(ev->Get()->Ev.Get());
    ids.assign(req->Record.GetPartitions().begin(), req->Record.GetPartitions().end());
    return ids;
}

} // namespace

Y_UNIT_TEST_SUITE(TTopicLocationActor) {

Y_UNIT_TEST(SuccessReturnsLivePartitions) {
    auto setup = CreateSetup("TopicLocationSmoke");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_location_smoke";
    CreateTopic(runtime, path, /*partitions=*/3);

    auto ev = RunTopicLocation(runtime, path);
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

Y_UNIT_TEST(MissingTopicIsSchemeError) {
    auto setup = CreateSetup("TopicLocationMissing");
    auto& runtime = setup->GetRuntime();

    auto ev = RunTopicLocation(runtime, "/Root/missing_topic_location");
    UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::SCHEME_ERROR);
    UNIT_ASSERT(!ev->Issues.Empty());
}

Y_UNIT_TEST(UnauthorizedStaysUnauthorized) {
    auto setup = CreateSetup("TopicLocationAuth");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_location_auth";
    CreateTopic(runtime, path);

    auto token = MakeIntrusive<NACLib::TUserToken>("bad-user@staff", TVector<TString>{});
    token->SaveSerializationInfo();
    auto ev = RunTopicLocation(runtime, path, token->GetSerializedToken());
    UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::UNAUTHORIZED);
    UNIT_ASSERT(!ev->Issues.Empty());
}

Y_UNIT_TEST(RetriesOnDeliveryProblem) {
    auto server = CreateSimulatedServer();
    auto& runtime = server->GetRuntime();
    const TString path = "/Root/topic_location_retry";
    CreateTopic(runtime, path);

    size_t broken = 0;
    auto breakObserver = BreakFirstLocationForward(runtime, broken);
    auto ev = RunTopicLocation(runtime, path);
    UNIT_ASSERT_VALUES_EQUAL(broken, 1u);
    UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(ev->Partitions.size(), 1u);
    UNIT_ASSERT_GT(ev->Partitions[0].NodeId, 0);
}

Y_UNIT_TEST(RetriesOnFalseLocationStatus) {
    auto server = CreateSimulatedServer();
    auto& runtime = server->GetRuntime();
    const TString path = "/Root/topic_location_false_status";
    CreateTopic(runtime, path);

    size_t injected = 0;
    ui64 firstCookie = 0;
    auto injectObserver = runtime.AddObserver<TEvPipeCache::TEvForward>(
        [&injected, &firstCookie, rt = &runtime](TEvPipeCache::TEvForward::TPtr& ev) {
            if (!ev || !ev->Get()->Ev ||
                ev->Get()->Ev->Type() != TEvPersQueue::TEvGetPartitionsLocation::EventType)
            {
                return;
            }
            if (injected == 0) {
                firstCookie = ev->Cookie;
                ++injected;
                auto* rejected = new TEvPersQueue::TEvGetPartitionsLocationResponse();
                rejected->Record.SetStatus(false);
                rt->Send(new IEventHandle(ev->Sender, ev->Recipient, rejected, 0, ev->Cookie));

                // Stale complete success must not win while the actor is waiting to retry.
                auto* stale = new TEvPersQueue::TEvGetPartitionsLocationResponse();
                stale->Record.SetStatus(true);
                auto* location = stale->Record.AddLocations();
                location->SetPartitionId(0);
                location->SetNodeId(999);
                location->SetGeneration(1);
                rt->Send(new IEventHandle(ev->Sender, ev->Recipient, stale, 0, ev->Cookie));
                ev.Reset();
                return;
            }
            if (injected == 1 && firstCookie != 0) {
                ++injected;
                auto* staleGen = new TEvPersQueue::TEvGetPartitionsLocationResponse();
                staleGen->Record.SetStatus(true);
                auto* location = staleGen->Record.AddLocations();
                location->SetPartitionId(0);
                location->SetNodeId(888);
                location->SetGeneration(1);
                rt->Send(new IEventHandle(ev->Sender, ev->Recipient, staleGen, 0, firstCookie));
            }
        });
    auto ev = RunTopicLocation(runtime, path);
    UNIT_ASSERT(injected >= 1u);
    UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(ev->Partitions.size(), 1u);
    UNIT_ASSERT_VALUES_UNEQUAL(ev->Partitions[0].NodeId, 999u);
    UNIT_ASSERT_VALUES_UNEQUAL(ev->Partitions[0].NodeId, 888u);
}

Y_UNIT_TEST(OldBalancerZeroCookieIsAccepted) {
    auto server = CreateSimulatedServer();
    auto& runtime = server->GetRuntime();
    const TString path = "/Root/topic_location_old_cookie";
    CreateTopic(runtime, path);

    size_t injected = 0;
    auto injectObserver = runtime.AddObserver<TEvPipeCache::TEvForward>(
        [&injected, rt = &runtime](TEvPipeCache::TEvForward::TPtr& ev) {
            if (!ev || !ev->Get()->Ev ||
                ev->Get()->Ev->Type() != TEvPersQueue::TEvGetPartitionsLocation::EventType)
            {
                return;
            }
            if (injected >= 1) {
                return;
            }
            ++injected;
            auto* response = new TEvPersQueue::TEvGetPartitionsLocationResponse();
            response->Record.SetStatus(true);
            auto* location = response->Record.AddLocations();
            location->SetPartitionId(0);
            location->SetNodeId(42);
            location->SetGeneration(7);
            // Cookie 0: old PQRB does not echo the request cookie.
            rt->Send(new IEventHandle(ev->Sender, ev->Recipient, response, 0, 0));
            ev.Reset();
        });
    auto ev = RunTopicLocation(runtime, path);
    UNIT_ASSERT_VALUES_EQUAL(injected, 1u);
    UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(ev->Partitions.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(ev->Partitions[0].NodeId, 42u);
    UNIT_ASSERT_VALUES_EQUAL(ev->Partitions[0].Generation, 7u);
}

Y_UNIT_TEST(RetriesOnIncompleteLocationSet) {
    auto server = CreateSimulatedServer();
    auto& runtime = server->GetRuntime();
    const TString path = "/Root/topic_location_incomplete";
    CreateTopic(runtime, path, /*partitions=*/3);

    size_t injected = 0;
    TVector<ui64> requested;
    auto injectObserver = runtime.AddObserver<TEvPipeCache::TEvForward>(
        [&injected, &requested, rt = &runtime](TEvPipeCache::TEvForward::TPtr& ev) {
            if (!ev || !ev->Get()->Ev ||
                ev->Get()->Ev->Type() != TEvPersQueue::TEvGetPartitionsLocation::EventType)
            {
                return;
            }
            requested = CaptureLocationRequestPartitions(ev);
            if (injected >= 1) {
                return;
            }
            ++injected;
            auto* response = new TEvPersQueue::TEvGetPartitionsLocationResponse();
            response->Record.SetStatus(true);
            auto* location = response->Record.AddLocations();
            location->SetPartitionId(0);
            location->SetNodeId(1);
            location->SetGeneration(1);
            rt->Send(new IEventHandle(ev->Sender, ev->Recipient, response));
            ev.Reset();
        });

    auto ev = RunTopicLocation(runtime, path);
    UNIT_ASSERT_VALUES_EQUAL(injected, 1u);
    UNIT_ASSERT_VALUES_EQUAL(requested.size(), 3u);
    Sort(requested);
    UNIT_ASSERT_VALUES_EQUAL(requested[0], 0u);
    UNIT_ASSERT_VALUES_EQUAL(requested[1], 1u);
    UNIT_ASSERT_VALUES_EQUAL(requested[2], 2u);
    UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(ev->Partitions.size(), 3u);
}

Y_UNIT_TEST(TimesOutWhenLocationStuck) {
    auto server = CreateSimulatedServer();
    auto& runtime = server->GetRuntime();
    const TString path = "/Root/topic_location_timeout";
    CreateTopic(runtime, path);

    auto dropObserver = DropLocationForwards(runtime);
    const auto edge = runtime.AllocateEdgeActor();
    TEnableScheduleForRootGuard schedule(runtime);
    schedule.SetRoot(runtime.Register(CreateTopicLocationActor(edge, path, "/Root", TString{})));

    runtime.DispatchEvents(TDispatchOptions{}, TDuration::MilliSeconds(100));
    runtime.AdvanceCurrentTime(TDuration::Seconds(31));

    auto handle = runtime.GrabEdgeEvent<TEvLocationResponse>(edge, TDuration::Seconds(5));
    UNIT_ASSERT(handle);
    const auto* ev = handle->Get();
    UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::TIMEOUT);
    UNIT_ASSERT(ev->Issues.ToString().Contains("timed out"));
}

Y_UNIT_TEST(PoisonRepliesCancelled) {
    auto server = CreateSimulatedServer();
    auto& runtime = server->GetRuntime();
    const TString path = "/Root/topic_location_poison";
    CreateTopic(runtime, path);

    auto dropObserver = DropLocationForwards(runtime);
    const auto edge = runtime.AllocateEdgeActor();
    TEnableScheduleForRootGuard schedule(runtime);
    schedule.SetRoot(runtime.Register(CreateTopicLocationActor(edge, path, "/Root", TString{})));

    runtime.DispatchEvents(TDispatchOptions{}, TDuration::MilliSeconds(100));
    runtime.Send(new IEventHandle(schedule.GetRoot(), edge, new NActors::TEvents::TEvPoison()));

    auto handle = runtime.GrabEdgeEvent<TEvLocationResponse>(edge, TDuration::Seconds(5));
    UNIT_ASSERT(handle);
    const auto* ev = handle->Get();
    UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::CANCELLED);
}

} // Y_UNIT_TEST_SUITE(TTopicLocationActor)

} // namespace NKafka::NTests

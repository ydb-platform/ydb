#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/kafka_proxy/actors/actors.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/public/schema/schema_ut_helpers.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/services/persqueue_v1/actors/events.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>

#include <functional>

namespace NKafka::NTests {

    using namespace NKikimr;
    using namespace NKikimr::NPQ::NSchema::NTests;
    using TEvLocationResponse = NKikimr::NGRpcProxy::V1::TEvPQProxy::TEvPartitionLocationResponse;
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;

    namespace {

        constexpr ui64 BALANCER_TABLET = 1001;
        constexpr ui64 PARTITION_TABLET = 2001;
        constexpr ui32 LOCATION_NODE_ID = 7;
        constexpr ui32 LOCATION_GENERATION = 3;

        class TFakeSchemeCacheActor: public NActors::TActorBootstrapped<TFakeSchemeCacheActor> {
        public:
            explicit TFakeSchemeCacheActor(ui32 partitions)
                : Partitions(partitions)
            {
            }

            void Bootstrap() {
                Become(&TFakeSchemeCacheActor::StateWork);
            }

            STRICT_STFUNC(StateWork,
                          hFunc(TEvTxProxySchemeCache::TEvNavigateKeySet, Handle);)

        private:
            void Handle(TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr& ev) {
                auto request = std::move(ev->Get()->Request);
                for (auto& entry : request->ResultSet) {
                    entry.Status = TNavigate::EStatus::Ok;
                    entry.Kind = TNavigate::EKind::KindTopic;
                    auto pqInfo = MakeIntrusive<TNavigate::TPQGroupInfo>();
                    pqInfo->Description.SetBalancerTabletID(BALANCER_TABLET);
                    for (ui32 i = 0; i < Partitions; ++i) {
                        auto* partition = pqInfo->Description.AddPartitions();
                        partition->SetPartitionId(i);
                        partition->SetTabletId(PARTITION_TABLET);
                    }
                    entry.PQGroupInfo = pqInfo;

                    auto self = MakeIntrusive<TNavigate::TDirEntryInfo>();
                    self->Info.SetName("topic");
                    self->Info.SetPathType(NKikimrSchemeOp::EPathTypePersQueueGroup);
                    self->Info.SetPathId(42);
                    self->Info.SetSchemeshardId(7);
                    entry.Self = self;
                }
                Send(ev->Sender, new TEvTxProxySchemeCache::TEvNavigateKeySetResult(std::move(request)));
            }

            ui32 Partitions;
        };

        class TFakePipeCacheActor: public NActors::TActorBootstrapped<TFakePipeCacheActor> {
        public:
            explicit TFakePipeCacheActor(ui32 partitions)
                : Partitions(partitions)
            {
            }

            void Bootstrap() {
                Become(&TFakePipeCacheActor::StateWork);
            }

            STRICT_STFUNC(StateWork,
                          hFunc(TEvPipeCache::TEvForward, Handle);
                          IgnoreFunc(TEvPipeCache::TEvUnlink);)

        private:
            void Handle(TEvPipeCache::TEvForward::TPtr& ev) {
                if (!ev->Get()->Ev ||
                    ev->Get()->Ev->Type() != TEvPersQueue::TEvGetPartitionsLocation::EventType)
                {
                    return;
                }
                auto* response = new TEvPersQueue::TEvGetPartitionsLocationResponse();
                response->Record.SetStatus(true);
                for (ui32 i = 0; i < Partitions; ++i) {
                    auto* location = response->Record.AddLocations();
                    location->SetPartitionId(i);
                    location->SetNodeId(LOCATION_NODE_ID);
                    location->SetGeneration(LOCATION_GENERATION);
                }
                Send(ev->Sender, response, 0, ev->Cookie);
            }

            ui32 Partitions;
        };

        struct TIsolatedEnv {
            NActors::TTestBasicRuntime Runtime;

            explicit TIsolatedEnv(ui32 partitions = 1)
                : Runtime(1, false)
            {
                Runtime.Initialize(TAppPrepare().Unwrap());
                Runtime.UpdateCurrentTime(TInstant::Now());
                Runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);
                Runtime.SetLogPriority(NKikimrServices::KAFKA_PROXY, NActors::NLog::PRI_DEBUG);
                Runtime.SetLogPriority(NKikimrServices::PQ_DESCRIBER, NActors::NLog::PRI_DEBUG);

                auto schemeCacheId = Runtime.Register(new TFakeSchemeCacheActor(partitions));
                Runtime.RegisterService(MakeSchemeCacheID(), schemeCacheId);

                auto pipeCacheId = Runtime.Register(new TFakePipeCacheActor(partitions));
                Runtime.RegisterService(MakePipePerNodeCacheID(false), pipeCacheId);
                Runtime.DispatchEvents();
            }
        };

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

        THolder<TEvLocationResponse> GrabLocationResponse(
            NActors::TTestActorRuntime& runtime,
            const TActorId& edge,
            TDuration waitTimeout)
        {
            auto handle = runtime.GrabEdgeEvent<TEvLocationResponse>(edge, waitTimeout);
            UNIT_ASSERT(handle);
            return THolder(handle->Release());
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
            return GrabLocationResponse(runtime, edge, waitTimeout);
        }

        // Isolated runtime has no cluster timers. Wait until the test observer has
        // fired, jump past the actor retry delay, then grab the reply.
        THolder<TEvLocationResponse> RunTopicLocationAfterInjection(
            NActors::TTestActorRuntime& runtime,
            const TString& path,
            std::function<bool()> injected)
        {
            const auto edge = runtime.AllocateEdgeActor();
            TEnableScheduleForRootGuard schedule(runtime);
            schedule.SetRoot(runtime.Register(CreateTopicLocationActor(edge, path, "/Root", TString{})));

            TDispatchOptions options;
            options.CustomFinalCondition = std::move(injected);
            runtime.DispatchEvents(options, TDuration::Seconds(5));
            runtime.AdvanceCurrentTime(TDuration::MilliSeconds(250));
            return GrabLocationResponse(runtime, edge, TDuration::Seconds(5));
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

        auto DropLocationForwards(NActors::TTestActorRuntime& runtime, size_t* dropped = nullptr) {
            return runtime.AddObserver<TEvPipeCache::TEvForward>(
                [dropped](TEvPipeCache::TEvForward::TPtr& ev) {
                    if (ev && ev->Get()->Ev &&
                        ev->Get()->Ev->Type() == TEvPersQueue::TEvGetPartitionsLocation::EventType)
                    {
                        if (dropped) {
                            ++*dropped;
                        }
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
            TIsolatedEnv env;
            auto& runtime = env.Runtime;
            const TString path = "/Root/topic_location_retry";

            size_t broken = 0;
            auto breakObserver = BreakFirstLocationForward(runtime, broken);
            auto ev = RunTopicLocationAfterInjection(runtime, path, [&] {
                return broken >= 1;
            });
            UNIT_ASSERT_VALUES_EQUAL(broken, 1u);
            UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(ev->Partitions.size(), 1u);
            UNIT_ASSERT_GT(ev->Partitions[0].NodeId, 0);
        }

        Y_UNIT_TEST(RetriesOnFalseLocationStatus) {
            TIsolatedEnv env;
            auto& runtime = env.Runtime;
            const TString path = "/Root/topic_location_false_status";

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
            auto ev = RunTopicLocationAfterInjection(runtime, path, [&] {
                return injected >= 1;
            });
            UNIT_ASSERT(injected >= 1u);
            UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(ev->Partitions.size(), 1u);
            UNIT_ASSERT_VALUES_UNEQUAL(ev->Partitions[0].NodeId, 999u);
            UNIT_ASSERT_VALUES_UNEQUAL(ev->Partitions[0].NodeId, 888u);
        }

        Y_UNIT_TEST(OldBalancerZeroCookieIsAccepted) {
            TIsolatedEnv env;
            auto& runtime = env.Runtime;
            const TString path = "/Root/topic_location_old_cookie";

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
            auto ev = RunTopicLocationAfterInjection(runtime, path, [&] {
                return injected >= 1;
            });
            UNIT_ASSERT_VALUES_EQUAL(injected, 1u);
            UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(ev->Partitions.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(ev->Partitions[0].NodeId, 42u);
            UNIT_ASSERT_VALUES_EQUAL(ev->Partitions[0].Generation, 7u);
        }

        Y_UNIT_TEST(RetriesOnIncompleteLocationSet) {
            TIsolatedEnv env(/*partitions=*/3);
            auto& runtime = env.Runtime;
            const TString path = "/Root/topic_location_incomplete";

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

            auto ev = RunTopicLocationAfterInjection(runtime, path, [&] {
                return injected >= 1;
            });
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
            TIsolatedEnv env;
            auto& runtime = env.Runtime;
            const TString path = "/Root/topic_location_timeout";

            size_t dropped = 0;
            auto dropObserver = DropLocationForwards(runtime, &dropped);
            const auto edge = runtime.AllocateEdgeActor();
            TEnableScheduleForRootGuard schedule(runtime);
            schedule.SetRoot(runtime.Register(CreateTopicLocationActor(edge, path, "/Root", TString{})));

            TDispatchOptions options;
            options.CustomFinalCondition = [&] {
                return dropped >= 1;
            };
            runtime.DispatchEvents(options, TDuration::Seconds(5));
            runtime.AdvanceCurrentTime(TDuration::Seconds(31));

            auto handle = runtime.GrabEdgeEvent<TEvLocationResponse>(edge, TDuration::Seconds(5));
            UNIT_ASSERT(handle);
            const auto* ev = handle->Get();
            UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::TIMEOUT);
            UNIT_ASSERT(ev->Issues.ToString().Contains("timed out"));
        }

        Y_UNIT_TEST(PoisonRepliesCancelled) {
            TIsolatedEnv env;
            auto& runtime = env.Runtime;
            const TString path = "/Root/topic_location_poison";

            size_t dropped = 0;
            auto dropObserver = DropLocationForwards(runtime, &dropped);
            const auto edge = runtime.AllocateEdgeActor();
            TEnableScheduleForRootGuard schedule(runtime);
            schedule.SetRoot(runtime.Register(CreateTopicLocationActor(edge, path, "/Root", TString{})));

            TDispatchOptions options;
            options.CustomFinalCondition = [&] {
                return dropped >= 1;
            };
            runtime.DispatchEvents(options, TDuration::Seconds(5));
            runtime.Send(new IEventHandle(schedule.GetRoot(), edge, new NActors::TEvents::TEvPoison()));

            auto handle = runtime.GrabEdgeEvent<TEvLocationResponse>(edge, TDuration::Seconds(5));
            UNIT_ASSERT(handle);
            const auto* ev = handle->Get();
            UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::CANCELLED);
        }

    } // Y_UNIT_TEST_SUITE(TTopicLocationActor)

} // namespace NKafka::NTests

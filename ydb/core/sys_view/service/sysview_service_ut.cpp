#include "sysview_service.h"

#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/protos/table_metrics_settings.pb.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/services/services.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/cast.h>
#include <util/string/join.h>

using namespace NActors;

namespace NKikimr {
    namespace NSysView {

        namespace {

            constexpr ui64 ProcessorTabletId = 100500;
            const TString Database = "/Root/db1";

            const TString TablePath = "/Root/db1/Table";

            class TStubDetailedCounters: public IDbDetailedCounters {
            public:
                ui64 PackCount = 0;
                int Tables = 1;
                int Leaves = 0;
                // out.ClearedCount() on entry to every Pack: the Tables the service kept for reuse
                std::vector<int> ClearedOnPack;

                void Pack(NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters>& out) override {
                    ++PackCount;
                    ClearedOnPack.push_back(out.ClearedCount());

                    for (int i = 0; i < Tables; ++i) {
                        auto* table = out.Add();
                        table->SetTablePath(i ? TablePath + ToString(i) : TablePath);
                        table->SetLevel(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable);
                        auto* metrics = table->MutableTableMetrics();
                        metrics->AddSimple(0);
                        metrics->AddSimple(PackCount);
                        for (int j = 0; j < Leaves; ++j) {
                            table->AddLeaves()->SetTabletId(j);
                        }
                    }
                }

                // Indexes of the packs that found no Tables kept for reuse
                std::vector<size_t> FreshPacks() const {
                    std::vector<size_t> fresh;
                    for (size_t i = 0; i < ClearedOnPack.size(); ++i) {
                        if (ClearedOnPack[i] == 0) {
                            fresh.push_back(i);
                        }
                    }
                    return fresh;
                }
            };

            struct TServiceIds {
                TActorId ServiceId;
                TActorId PipeCacheEdge;
            };

            class TFakeSchemeCache: public TActorBootstrapped<TFakeSchemeCache> {
            public:
                void Bootstrap() {
                    Become(&TThis::StateWork);
                }

                STFUNC(StateWork) {
                    switch (ev->GetTypeRewrite()) {
                        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySet, Handle);
                        IgnoreFunc(TEvTxProxySchemeCache::TEvWatchPathId);
                    }
                }

            private:
                void Handle(TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr& ev) {
                    THolder<NSchemeCache::TSchemeCacheNavigate> request(ev->Get()->Request.Release());

                    if (request->ResultSet.size() == 1) {
                        auto& entry = request->ResultSet.back();
                        entry.Status = NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
                        const TPathId domainKey(72057594046644480ull, 1);
                        auto domainInfo = MakeIntrusive<NSchemeCache::TDomainInfo>(domainKey, domainKey);
                        domainInfo->Params.SetSysViewProcessor(ProcessorTabletId);
                        entry.DomainInfo = domainInfo;
                    }

                    Send(ev->Sender, new TEvTxProxySchemeCache::TEvNavigateKeySetResult(request.Release()));
                }
            };

            TServiceIds SetupService(TTestBasicRuntime& runtime) {
                runtime.Initialize(TAppPrepare().Unwrap());
                runtime.UpdateCurrentTime(TInstant::Now());
                runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);
                runtime.GetAppData().FeatureFlags.SetEnablePersistentQueryStats(false);

                TActorId schemeCacheId = runtime.Register(new TFakeSchemeCache());
                runtime.RegisterService(MakeSchemeCacheID(), schemeCacheId);

                TActorId pipeCacheEdge = runtime.AllocateEdgeActor();
                runtime.RegisterService(MakePipePerNodeCacheID(false), pipeCacheEdge);

                TActorId serviceId = runtime.Register(CreateSysViewServiceForTests().Release());
                runtime.RegisterService(MakeSysViewServiceID(runtime.GetNodeId(0)), serviceId);
                runtime.EnableScheduleForActor(serviceId);
                runtime.SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

                return {serviceId, pipeCacheEdge};
            }

            TIntrusivePtr<TStubDetailedCounters> RegisterRole(TTestBasicRuntime& runtime, const TActorId& serviceId,
                                                            NKikimrSysView::EDbCountersService service)
            {
                auto stub = MakeIntrusive<TStubDetailedCounters>();
                auto ev = MakeHolder<TEvSysView::TEvRegisterDbDetailedCounters>(Database, service, stub);
                runtime.Send(new IEventHandle(serviceId, runtime.AllocateEdgeActor(), ev.Release()), 0, true);
                return stub;
            }

            void UnregisterRole(TTestBasicRuntime& runtime, const TActorId& serviceId,
                                NKikimrSysView::EDbCountersService service)
            {
                auto ev = MakeHolder<TEvSysView::TEvUnregisterDbDetailedCounters>(Database, service);
                runtime.Send(new IEventHandle(serviceId, runtime.AllocateEdgeActor(), ev.Release()), 0, true);
            }

            NKikimrSysView::TEvSendDbCountersRequest GrabRequest(TTestBasicRuntime& runtime, const TActorId& pipeCacheEdge) {
                auto ev = runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(pipeCacheEdge, TDuration::Seconds(30));
                UNIT_ASSERT(ev);
                UNIT_ASSERT_VALUES_EQUAL(ev->Get()->TabletId, ProcessorTabletId);
                auto* req = static_cast<TEvSysView::TEvSendDbCountersRequest*>(ev->Get()->Ev.Get());
                return req->Record;
            }

            void SendAck(TTestBasicRuntime& runtime, const TActorId& serviceId, ui64 generation)
            {
                auto ack = MakeHolder<TEvSysView::TEvSendDbCountersResponse>();
                ack->Record.SetDatabase(Database);
                ack->Record.SetGeneration(generation);
                runtime.Send(new IEventHandle(serviceId, runtime.AllocateEdgeActor(), ack.Release()), 0, true);
            }

            NKikimrSysView::TEvSendDbCountersRequest GrabAndAck(TTestBasicRuntime& runtime, const TActorId& serviceId,
                                                                const TActorId& pipeCacheEdge)
            {
                auto req = GrabRequest(runtime, pipeCacheEdge);
                SendAck(runtime, serviceId, req.GetGeneration());
                return req;
            }

        } // anonymous namespace

        Y_UNIT_TEST_SUITE(SysViewServiceDetailedCounters) {

            Y_UNIT_TEST(BothRolesRideOneMessage) {
                TTestBasicRuntime runtime(1);
                auto [serviceId, pipeCacheEdge] = SetupService(runtime);

                auto leaderStub = RegisterRole(runtime, serviceId, NKikimrSysView::TABLETS);
                auto followerStub = RegisterRole(runtime, serviceId, NKikimrSysView::TABLETS_FOLLOWERS);

                auto req = GrabRequest(runtime, pipeCacheEdge);

                UNIT_ASSERT_VALUES_EQUAL(req.GetNodeId(), runtime.GetNodeId(0));
                UNIT_ASSERT_VALUES_EQUAL(req.DetailedCountersSize(), 2);

                THashSet<int> services;
                for (const auto& detailed : req.GetDetailedCounters()) {
                    services.insert(detailed.GetService());
                    UNIT_ASSERT_VALUES_EQUAL(detailed.TablesSize(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(detailed.GetTables(0).GetTablePath(), TablePath);
                }

                UNIT_ASSERT_VALUES_EQUAL(services.size(), 2u);
                UNIT_ASSERT(services.contains(NKikimrSysView::TABLETS));
                UNIT_ASSERT(services.contains(NKikimrSysView::TABLETS_FOLLOWERS));
            }

            Y_UNIT_TEST(GenerationAdvancesOnlyOnAck) {
                TTestBasicRuntime runtime(1);
                auto [serviceId, pipeCacheEdge] = SetupService(runtime);

                auto leaderStub = RegisterRole(runtime, serviceId, NKikimrSysView::TABLETS);
                auto followerStub = RegisterRole(runtime, serviceId, NKikimrSysView::TABLETS_FOLLOWERS);

                auto req1 = GrabRequest(runtime, pipeCacheEdge);
                auto gen1 = req1.GetGeneration();

                auto req2 = GrabRequest(runtime, pipeCacheEdge);
                UNIT_ASSERT_VALUES_EQUAL(req2.GetGeneration(), gen1);

                SendAck(runtime, serviceId, gen1);

                auto req3 = GrabRequest(runtime, pipeCacheEdge);
                auto gen3 = req3.GetGeneration();
                UNIT_ASSERT_VALUES_EQUAL(gen3, gen1 + 1);

                UNIT_ASSERT_VALUES_EQUAL(leaderStub->PackCount, 2);
                UNIT_ASSERT_VALUES_EQUAL(followerStub->PackCount, 2);
                for (const auto& detailed : req3.GetDetailedCounters()) {
                    UNIT_ASSERT_VALUES_EQUAL(detailed.GetTables(0).GetTableMetrics().GetSimple(1), 2);
                }
            }

            Y_UNIT_TEST(UnackedRetryResendsSamePayload) {
                TTestBasicRuntime runtime(1);
                auto [serviceId, pipeCacheEdge] = SetupService(runtime);

                auto stub = RegisterRole(runtime, serviceId, NKikimrSysView::TABLETS);

                auto req1 = GrabRequest(runtime, pipeCacheEdge);
                auto req2 = GrabRequest(runtime, pipeCacheEdge);

                UNIT_ASSERT_VALUES_EQUAL(req1.GetGeneration(), req2.GetGeneration());
                UNIT_ASSERT_VALUES_EQUAL(req1.SerializeAsString(), req2.SerializeAsString());
                UNIT_ASSERT_VALUES_EQUAL(stub->PackCount, 1);
            }

            Y_UNIT_TEST(RegistrationDuringRetryWaitsForNextGeneration) {
                TTestBasicRuntime runtime(1);
                auto [serviceId, pipeCacheEdge] = SetupService(runtime);

                auto leaderStub = RegisterRole(runtime, serviceId, NKikimrSysView::TABLETS);
                auto req1 = GrabRequest(runtime, pipeCacheEdge);

                auto followerStub = RegisterRole(runtime, serviceId, NKikimrSysView::TABLETS_FOLLOWERS);
                auto req2 = GrabRequest(runtime, pipeCacheEdge);
                UNIT_ASSERT_VALUES_EQUAL(req1.SerializeAsString(), req2.SerializeAsString());
                UNIT_ASSERT_VALUES_EQUAL(leaderStub->PackCount, 1);
                UNIT_ASSERT_VALUES_EQUAL(followerStub->PackCount, 0);

                SendAck(runtime, serviceId, req1.GetGeneration());
                auto req3 = GrabRequest(runtime, pipeCacheEdge);
                UNIT_ASSERT_VALUES_EQUAL(req3.GetGeneration(), req1.GetGeneration() + 1);
                UNIT_ASSERT_VALUES_EQUAL(req3.DetailedCountersSize(), 2);
                UNIT_ASSERT_VALUES_EQUAL(leaderStub->PackCount, 2);
                UNIT_ASSERT_VALUES_EQUAL(followerStub->PackCount, 1);
            }

            Y_UNIT_TEST(StaleAckIgnored) {
                TTestBasicRuntime runtime(1);
                auto [serviceId, pipeCacheEdge] = SetupService(runtime);

                RegisterRole(runtime, serviceId, NKikimrSysView::TABLETS);

                auto req1 = GrabRequest(runtime, pipeCacheEdge);
                auto gen1 = req1.GetGeneration();

                SendAck(runtime, serviceId, gen1 - 1);

                auto req2 = GrabRequest(runtime, pipeCacheEdge);
                UNIT_ASSERT_VALUES_EQUAL(req2.GetGeneration(), gen1);
            }

            Y_UNIT_TEST(DetailedOnlyDatabaseStillSends) {
                TTestBasicRuntime runtime(1);
                auto [serviceId, pipeCacheEdge] = SetupService(runtime);

                RegisterRole(runtime, serviceId, NKikimrSysView::TABLETS);

                auto req = GrabRequest(runtime, pipeCacheEdge);

                UNIT_ASSERT_VALUES_EQUAL(req.ServiceCountersSize(), 0);
                UNIT_ASSERT_VALUES_EQUAL(req.DetailedCountersSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL((int)req.GetDetailedCounters(0).GetService(),
                                         (int)NKikimrSysView::TABLETS);
                UNIT_ASSERT_VALUES_EQUAL(req.GetDetailedCounters(0).TablesSize(), 1);
            }

            Y_UNIT_TEST(ConfirmedSendReusesDetailedPayload) {
                TTestBasicRuntime runtime(1);
                auto [serviceId, pipeCacheEdge] = SetupService(runtime);

                auto stub = RegisterRole(runtime, serviceId, NKikimrSysView::TABLETS);

                GrabAndAck(runtime, serviceId, pipeCacheEdge);
                auto req = GrabRequest(runtime, pipeCacheEdge);

                UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", stub->ClearedOnPack), "0,1");
                UNIT_ASSERT_VALUES_EQUAL(req.DetailedCountersSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(req.GetDetailedCounters(0).TablesSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(
                    req.GetDetailedCounters(0).GetTables(0).GetTableMetrics().GetSimple(1), 2);
            }

            Y_UNIT_TEST(ShrunkDetailedPayloadReleasedLater) {
                TTestBasicRuntime runtime(1);
                auto [serviceId, pipeCacheEdge] = SetupService(runtime);

                auto stub = RegisterRole(runtime, serviceId, NKikimrSysView::TABLETS);
                stub->Tables = 3;
                GrabAndAck(runtime, serviceId, pipeCacheEdge);

                // More sends than the 12 shrunk ones that free the payload
                stub->Tables = 1;
                for (int i = 0; i < 20; ++i) {
                    auto req = GrabAndAck(runtime, serviceId, pipeCacheEdge);
                    UNIT_ASSERT_VALUES_EQUAL(req.GetDetailedCounters(0).TablesSize(), 1);
                }

                const TString packs = JoinSeq(",", stub->ClearedOnPack);
                const auto fresh = stub->FreshPacks();
                UNIT_ASSERT_VALUES_EQUAL_C(fresh.size(), 2, packs);
                UNIT_ASSERT_VALUES_EQUAL_C(fresh[0], 0, packs);

                // The shrunk payload keeps the three Tables for a while, then is freed once
                // and reused from then on
                const size_t released = fresh[1];
                UNIT_ASSERT_C(released >= 3, packs);
                for (size_t i = 1; i < released; ++i) {
                    UNIT_ASSERT_VALUES_EQUAL_C(stub->ClearedOnPack[i], 3, packs);
                }
                for (size_t i = released + 1; i < stub->ClearedOnPack.size(); ++i) {
                    UNIT_ASSERT_VALUES_EQUAL_C(stub->ClearedOnPack[i], 1, packs);
                }
            }

            Y_UNIT_TEST(ShrunkLeavesReleasedLater) {
                TTestBasicRuntime runtime(1);
                auto [serviceId, pipeCacheEdge] = SetupService(runtime);

                auto stub = RegisterRole(runtime, serviceId, NKikimrSysView::TABLETS);
                stub->Leaves = 100;
                GrabAndAck(runtime, serviceId, pipeCacheEdge);

                // The table slot stays in use, only its leaves go spare
                stub->Leaves = 0;
                for (int i = 0; i < 20; ++i) {
                    auto req = GrabAndAck(runtime, serviceId, pipeCacheEdge);
                    UNIT_ASSERT_VALUES_EQUAL(req.GetDetailedCounters(0).GetTables(0).LeavesSize(), 0);
                }

                const TString packs = JoinSeq(",", stub->ClearedOnPack);
                const auto fresh = stub->FreshPacks();
                UNIT_ASSERT_VALUES_EQUAL_C(fresh.size(), 2, packs);
                UNIT_ASSERT_VALUES_EQUAL_C(fresh[0], 0, packs);
                UNIT_ASSERT_C(fresh[1] >= 3, packs);
                for (size_t i = 1; i < stub->ClearedOnPack.size(); ++i) {
                    if (i != fresh[1]) {
                        UNIT_ASSERT_VALUES_EQUAL_C(stub->ClearedOnPack[i], 1, packs);
                    }
                }
            }

            Y_UNIT_TEST(UnregisterReleasesDetailedPayload) {
                TTestBasicRuntime runtime(1);
                auto [serviceId, pipeCacheEdge] = SetupService(runtime);

                auto first = RegisterRole(runtime, serviceId, NKikimrSysView::TABLETS);
                first->Tables = 3;
                GrabAndAck(runtime, serviceId, pipeCacheEdge);

                UnregisterRole(runtime, serviceId, NKikimrSysView::TABLETS);
                auto idle = GrabAndAck(runtime, serviceId, pipeCacheEdge);
                UNIT_ASSERT_VALUES_EQUAL(idle.DetailedCountersSize(), 0);

                auto second = RegisterRole(runtime, serviceId, NKikimrSysView::TABLETS);
                auto req = GrabRequest(runtime, pipeCacheEdge);
                UNIT_ASSERT_VALUES_EQUAL(req.DetailedCountersSize(), 1);

                // Kept elements would hand the three Tables of the first role to the second
                UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", second->ClearedOnPack), "0");
                UNIT_ASSERT_VALUES_EQUAL(first->PackCount, 1);
            }

            Y_UNIT_TEST(RetryNeverReleasesDetailedPayload) {
                TTestBasicRuntime runtime(1);
                auto [serviceId, pipeCacheEdge] = SetupService(runtime);

                auto stub = RegisterRole(runtime, serviceId, NKikimrSysView::TABLETS);
                stub->Tables = 3;
                GrabAndAck(runtime, serviceId, pipeCacheEdge);

                // More retries than the 12 shrunk sends that would free the payload
                stub->Tables = 1;
                auto req1 = GrabRequest(runtime, pipeCacheEdge);
                for (int i = 0; i < 19; ++i) {
                    auto retry = GrabRequest(runtime, pipeCacheEdge);
                    UNIT_ASSERT_VALUES_EQUAL(retry.SerializeAsString(), req1.SerializeAsString());
                }
                UNIT_ASSERT_VALUES_EQUAL(stub->PackCount, 2);

                // The ack of a retried generation sends the next one at once. Retries do not
                // count as shrunk sends, so the payload still keeps the three Tables
                SendAck(runtime, serviceId, req1.GetGeneration());
                auto req2 = GrabRequest(runtime, pipeCacheEdge);
                UNIT_ASSERT_VALUES_EQUAL(req2.GetGeneration(), req1.GetGeneration() + 1);
                UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", stub->ClearedOnPack), "0,3,3");
            }

            Y_UNIT_TEST(SteadyDetailedPayloadReleasedPeriodically) {
                TTestBasicRuntime runtime(1);
                auto [serviceId, pipeCacheEdge] = SetupService(runtime);

                // More sends than the 120 that free even a steady payload
                auto stub = RegisterRole(runtime, serviceId, NKikimrSysView::TABLETS);
                stub->Tables = 3;
                for (int i = 0; i < 130; ++i) {
                    GrabAndAck(runtime, serviceId, pipeCacheEdge);
                }

                const TString packs = JoinSeq(",", stub->ClearedOnPack);
                const auto fresh = stub->FreshPacks();
                UNIT_ASSERT_VALUES_EQUAL_C(fresh.size(), 2, packs);
                UNIT_ASSERT_VALUES_EQUAL_C(fresh[0], 0, packs);
                UNIT_ASSERT_C(fresh[1] > 100, packs);
                for (size_t i = 1; i < stub->ClearedOnPack.size(); ++i) {
                    if (i != fresh[1]) {
                        UNIT_ASSERT_VALUES_EQUAL_C(stub->ClearedOnPack[i], 3, packs);
                    }
                }
            }

        } // Y_UNIT_TEST_SUITE(SysViewServiceDetailedCounters)

    } // namespace NSysView
} // namespace NKikimr

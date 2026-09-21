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

                void Pack(NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters>& out) override {
                    ++PackCount;

                    auto* table = out.Add();
                    table->SetTablePath(TablePath);
                    table->SetLevel(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable);
                    auto* counters = table->MutableTableCounters()->MutableAppCounters();
                    counters->AddSimple(0);
                    counters->AddSimple(PackCount);
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
                    UNIT_ASSERT_VALUES_EQUAL(detailed.GetTables(0).GetTableCounters().GetAppCounters().GetSimple(1), 2);
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

        } // Y_UNIT_TEST_SUITE(SysViewServiceDetailedCounters)

    } // namespace NSysView
} // namespace NKikimr

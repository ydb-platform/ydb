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

            constexpr ui64 TableOwnerId = 1;
            constexpr ui64 TablePathId = 2;
            const TString TablePath = "/Root/db1/Table";

            class TStubDetailedCounters: public IDbDetailedCounters {
            public:
                TVector<ui64> PackedGenerations;

                void Pack(NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters>& out, ui64 generation) override {
                    PackedGenerations.push_back(generation);

                    auto* table = out.Add();
                    table->SetOwnerId(TableOwnerId);
                    table->SetPathId(TablePathId);
                    table->SetTablePath(TablePath);
                    table->SetLevel(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable);
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

            TServiceIds SetupServiceWithLabeledCounters(TTestBasicRuntime& runtime) {
                runtime.Initialize(TAppPrepare().Unwrap());
                runtime.UpdateCurrentTime(TInstant::Now());
                runtime.GetAppData().FeatureFlags.SetEnableDbCounters(true);
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

            TIntrusivePtr<TStubDetailedCounters> RegisterStream(TTestBasicRuntime& runtime, const TActorId& serviceId,
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

                auto leaderStub = RegisterStream(runtime, serviceId, NKikimrSysView::TABLETS);
                auto followerStub = RegisterStream(runtime, serviceId, NKikimrSysView::TABLETS_FOLLOWERS);

                auto req = GrabRequest(runtime, pipeCacheEdge);

                UNIT_ASSERT_VALUES_EQUAL(req.GetNodeId(), runtime.GetNodeId(0));
                UNIT_ASSERT_VALUES_EQUAL(req.DetailedCountersSize(), 2);

                THashSet<int> services;
                for (const auto& detailed : req.GetDetailedCounters()) {
                    services.insert(detailed.GetService());
                    UNIT_ASSERT_VALUES_EQUAL(detailed.TablesSize(), 1);
                    const auto& table = detailed.GetTables(0);
                    UNIT_ASSERT_VALUES_EQUAL(table.GetTablePath(), TablePath);
                    UNIT_ASSERT_VALUES_EQUAL(table.GetOwnerId(), TableOwnerId);
                    UNIT_ASSERT_VALUES_EQUAL(table.GetPathId(), TablePathId);
                }

                UNIT_ASSERT_VALUES_EQUAL(services.size(), 2u);
                UNIT_ASSERT(services.contains(NKikimrSysView::TABLETS));
                UNIT_ASSERT(services.contains(NKikimrSysView::TABLETS_FOLLOWERS));
            }

            Y_UNIT_TEST(GenerationAdvancesOnlyOnAck) {
                TTestBasicRuntime runtime(1);
                auto [serviceId, pipeCacheEdge] = SetupService(runtime);

                auto leaderStub = RegisterStream(runtime, serviceId, NKikimrSysView::TABLETS);
                auto followerStub = RegisterStream(runtime, serviceId, NKikimrSysView::TABLETS_FOLLOWERS);

                auto req1 = GrabRequest(runtime, pipeCacheEdge);
                auto gen1 = req1.GetGeneration();

                auto req2 = GrabRequest(runtime, pipeCacheEdge);
                UNIT_ASSERT_VALUES_EQUAL(req2.GetGeneration(), gen1);

                SendAck(runtime, serviceId, gen1);

                auto req3 = GrabRequest(runtime, pipeCacheEdge);
                auto gen3 = req3.GetGeneration();
                UNIT_ASSERT_VALUES_EQUAL(gen3, gen1 + 1);

                UNIT_ASSERT(leaderStub->PackedGenerations.size() >= 2);
                UNIT_ASSERT(followerStub->PackedGenerations.size() >= 2);
                UNIT_ASSERT_VALUES_EQUAL(leaderStub->PackedGenerations.back(), gen3);
                UNIT_ASSERT_VALUES_EQUAL(followerStub->PackedGenerations.back(), gen3);
            }

            Y_UNIT_TEST(UnackedRetryResendsSameGeneration) {
                TTestBasicRuntime runtime(1);
                auto [serviceId, pipeCacheEdge] = SetupService(runtime);

                auto stub = RegisterStream(runtime, serviceId, NKikimrSysView::TABLETS);

                auto req1 = GrabRequest(runtime, pipeCacheEdge);
                auto req2 = GrabRequest(runtime, pipeCacheEdge);

                UNIT_ASSERT_VALUES_EQUAL(req1.GetGeneration(), req2.GetGeneration());
                UNIT_ASSERT_VALUES_EQUAL(stub->PackedGenerations.size(), 2u);
                UNIT_ASSERT_VALUES_EQUAL(stub->PackedGenerations[0], stub->PackedGenerations[1]);
            }

            Y_UNIT_TEST(StaleAckIgnored) {
                TTestBasicRuntime runtime(1);
                auto [serviceId, pipeCacheEdge] = SetupService(runtime);

                RegisterStream(runtime, serviceId, NKikimrSysView::TABLETS);

                auto req1 = GrabRequest(runtime, pipeCacheEdge);
                auto gen1 = req1.GetGeneration();

                SendAck(runtime, serviceId, gen1 - 1);

                auto req2 = GrabRequest(runtime, pipeCacheEdge);
                UNIT_ASSERT_VALUES_EQUAL(req2.GetGeneration(), gen1);
            }

            Y_UNIT_TEST(DetailedOnlyDatabaseStillSends) {
                TTestBasicRuntime runtime(1);
                auto [serviceId, pipeCacheEdge] = SetupService(runtime);

                RegisterStream(runtime, serviceId, NKikimrSysView::TABLETS);

                auto req = GrabRequest(runtime, pipeCacheEdge);

                UNIT_ASSERT_VALUES_EQUAL(req.ServiceCountersSize(), 0);
                UNIT_ASSERT_VALUES_EQUAL(req.DetailedCountersSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(req.GetDetailedCounters(0).GetService(), NKikimrSysView::TABLETS);
                UNIT_ASSERT_VALUES_EQUAL(req.GetDetailedCounters(0).TablesSize(), 1);
            }

        } // Y_UNIT_TEST_SUITE(SysViewServiceDetailedCounters)

    } // namespace NSysView
} // namespace NKikimr

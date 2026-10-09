#include "helpers.h"
#include "storage.h"
#include "appdata.h"
#include "runtime.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/quoter/public/quoter.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/cms/console/immediate_controls_configurator.h>
#include <ydb/core/control/immediate_control_board_actor.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
#include <ydb/core/blobstorage/subsystem/mock/mock.h>
#include <ydb/core/blobstorage/subsystem/subsystem.h>
#include <ydb/core/quoter/quoter_service.h>
#include <ydb/core/tablet/tablet_monitoring_proxy.h>
#include <ydb/core/tablet/resource_broker.h>
#include <ydb/core/tablet/node_tablet_monitor.h>
#include <ydb/core/tablet/tablet_list_renderer.h>
#include <ydb/core/tablet_flat/shared_sausagecache.h>
#include <ydb/core/tx/columnshard/data_accessor/cache_policy/policy.h>
#include <ydb/core/tx/general_cache/service/service.h>
#include <ydb/core/tx/columnshard/column_fetching/cache_policy.h>
#include <ydb/core/tx/scheme_board/replica.h>
#include <ydb/core/client/server/grpc_proxy_status.h>
#include <ydb/core/scheme/tablet_scheme.h>
#include <ydb/core/util/console.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/scheme_board/cache.h>
#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/sys_view/service/sysview_service.h>
#include <ydb/core/statistics/service/service.h>

#include <util/system/env.h>

#include <ydb/core/protos/key.pb.h>

static constexpr TDuration DISK_DISPATCH_TIMEOUT = NSan::PlainOrUnderSanitizer(TDuration::Seconds(10), TDuration::Seconds(20));

namespace NKikimr {

    void SetupIcb(TTestActorRuntime& runtime, ui32 nodeIndex, const NKikimrConfig::TImmediateControlsConfig& config,
            const TIntrusivePtr<NKikimr::TControlBoard>& icb,
            const TIntrusivePtr<NKikimr::TDynamicControlBoard>& dcb)
    {
        runtime.AddLocalService(MakeIcbId(runtime.GetNodeId(nodeIndex)),
            TActorSetupCmd(CreateImmediateControlActor(icb, dcb, runtime.GetDynamicCounters(nodeIndex)),
                    TMailboxType::ReadAsFilled, 0),
            nodeIndex);

        runtime.AddLocalService(TActorId{},
            TActorSetupCmd(NConsole::CreateImmediateControlsConfigurator(icb, config),
                    TMailboxType::ReadAsFilled, 0),
            nodeIndex);
    }

    void SetupBSNodeWarden(TTestActorRuntime& runtime, ui32 nodeIndex, TIntrusivePtr<TNodeWardenConfig> nodeWardenConfig)
    {
        if (const auto& poolIds = runtime.GetBlobStorageExecutorPoolIds(); !poolIds.empty()) {
            nodeWardenConfig->BlobStorageExecutorPoolIds = poolIds;
        }
        auto previous = std::move(runtime.SetupNodeSubSystems);
        runtime.SetupNodeSubSystems = [nodeIndex, nodeWardenConfig, previous = std::move(previous)](
                ui32 currentNode, TActorSystemSetup* setup) {
            if (previous) {
                previous(currentNode, setup);
            }
            if (currentNode == nodeIndex) {
                InstallBlobStorageSubsystem(*setup, CreateBlobStorageSubsystem(
                    nodeWardenConfig, 0, TMailboxType::Revolving));
            }
        };
    }

    void SetupMockBlobStorage(TTestActorRuntime& runtime, ui32 nodeIndex,
            TVector<TIntrusivePtr<NFake::TProxyDS>> dsProxies)
    {
        auto previous = std::move(runtime.SetupNodeSubSystems);
        runtime.SetupNodeSubSystems = [nodeIndex, dsProxies = std::move(dsProxies), previous = std::move(previous)](
                ui32 currentNode, TActorSystemSetup* setup) {
            if (previous) {
                previous(currentNode, setup);
            }
            if (currentNode == nodeIndex) {
                InstallBlobStorageSubsystem(*setup, CreateMockBlobStorageSubsystem(dsProxies));
            }
        };
    }

    void SetupSchemeCache(TTestActorRuntime& runtime, ui32 nodeIndex, const TString& root)
    {
        auto cacheConfig = MakeIntrusive<NSchemeCache::TSchemeCacheConfig>();
        cacheConfig->Roots.emplace_back(1, TTestTxConfig::SchemeShard, root);
        cacheConfig->Counters = new ::NMonitoring::TDynamicCounters();

        runtime.AddLocalService(MakeSchemeCacheID(),
            TActorSetupCmd(CreateSchemeBoardSchemeCache(cacheConfig.Get()), TMailboxType::Revolving, 0), nodeIndex);
    }

    void SetupGRpcProxyStatus(TTestActorRuntime& runtime, ui32 nodeIndex)
    {
        runtime.AddLocalService(MakeGRpcProxyStatusID(runtime.GetNodeId(nodeIndex)),
            TActorSetupCmd(CreateGRpcProxyStatus(), TMailboxType::Revolving, 0), nodeIndex);
    }

    void SetupCSMetadataCache(TTestActorRuntime& runtime, ui32 nodeIndex) {
        auto* actor = NGeneralCache::CreateService<NOlap::NGeneralCache::TPortionsMetadataCachePolicy>(
			NGeneralCache::NPublic::TConfig::BuildDefault(), runtime.GetDynamicCounters(nodeIndex));
		runtime.AddLocalService(NOlap::NDataAccessorControl::TGeneralCache::MakeServiceId(runtime.GetNodeId(nodeIndex)),
			TActorSetupCmd(actor, TMailboxType::ReadAsFilled, 0), nodeIndex);
    }

    void SetupCSColumnDataCache(TTestActorRuntime& runtime, ui32 nodeIndex) {
        auto* actor = NGeneralCache::CreateService<NOlap::NGeneralCache::TColumnDataCachePolicy>(
            NGeneralCache::NPublic::TConfig::BuildDefault(), runtime.GetDynamicCounters(nodeIndex));
        runtime.AddLocalService(NOlap::NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(nodeIndex)),
            TActorSetupCmd(actor, TMailboxType::ReadAsFilled, 0), nodeIndex);
    }

    void SetupBlobCache(TTestActorRuntime& runtime, ui32 nodeIndex, const NKikimrConfig::TBlobCacheConfig& config)
    {
        runtime.AddLocalService(NBlobCache::MakeBlobCacheServiceId(),
            TActorSetupCmd(
                NBlobCache::CreateBlobCache(NBlobCache::TBlobCacheSettings::FromProto(config),
                    runtime.GetDynamicCounters(nodeIndex)->GetSubgroup("type", "BLOB_CACHE")),
                TMailboxType::ReadAsFilled,
                0),
            nodeIndex);
    }

    void SetupQuoterService(TTestActorRuntime& runtime, ui32 nodeIndex)
    {
        runtime.AddLocalService(MakeQuoterServiceID(),
                TActorSetupCmd(CreateQuoterService(), TMailboxType::HTSwap, 0),
                nodeIndex);
    }

    void SetupSysViewService(TTestActorRuntime& runtime, ui32 nodeIndex)
    {
        runtime.AddLocalService(NSysView::MakeSysViewServiceID(runtime.GetNodeId(nodeIndex)),
                TActorSetupCmd(NSysView::CreateSysViewServiceForTests().Release(), TMailboxType::Revolving, 0),
                nodeIndex);
    }

    void SetupStatService(TTestActorRuntime& runtime, ui32 nodeIndex)
    {
        runtime.AddLocalService(NStat::MakeStatServiceID(runtime.GetNodeId(nodeIndex)),
                TActorSetupCmd(NStat::CreateStatService().Release(), TMailboxType::HTSwap, 0),
                nodeIndex);
    }

    void SetupBasicServices(TTestActorRuntime& runtime, TAppPrepare& app, bool mock,
                            NFake::INode* factory, NFake::TStorage storage, const NSharedCache::TSharedCacheConfig* sharedCacheConfig, bool forceFollowers,
                            TVector<TIntrusivePtr<NFake::TProxyDS>> dsProxies)
    {
        runtime.SetDispatchTimeout(storage.UseDisk ? storage.EventDispatchTimeout.value_or(DISK_DISPATCH_TIMEOUT) : DEFAULT_DISPATCH_TIMEOUT);

        bool addGroups = dsProxies.empty();
        TTestStorageFactory disk(runtime, storage, mock, addGroups);

        {
            NKikimrBlobStorage::TNodeWardenServiceSet bsConfig;
            Y_ABORT_UNLESS(google::protobuf::TextFormat::ParseFromString(disk.MakeTextConf(*app.Domains), &bsConfig));
            app.SetBSConf(std::move(bsConfig));
        }

        if (!app.Domains->Domain) {
            app.AddDomain(TDomainsInfo::TDomain::ConstructEmptyDomain("dc-1").Release());
            app.AddHive(0);
        }

        while (app.Icb.size() < runtime.GetNodeCount()) {
            app.Icb.emplace_back(new TControlBoard);
        }

        while (app.Dcb.size() < runtime.GetNodeCount()) {
            app.Dcb.emplace_back(new TDynamicControlBoard());
        }

        NSharedCache::TSharedCacheConfig defaultSharedCacheConfig;
        defaultSharedCacheConfig.SetMemoryLimit(32_MB);

        for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
            SetupStateStorageGroups(runtime, nodeIndex);
            NKikimrProto::TKeyConfig keyConfig;
            if (const auto it = app.Keys.find(nodeIndex); it != app.Keys.end()) {
                keyConfig = it->second;
            }
            SetupIcb(runtime, nodeIndex, app.ImmediateControlsConfig, app.Icb[nodeIndex], app.Dcb[nodeIndex]);
            auto nodeWardenConfig = disk.MakeWardenConf(*app.Domains, keyConfig);
            if (dsProxies.empty()) {
                SetupBSNodeWarden(runtime, nodeIndex, nodeWardenConfig);
            } else {
                SetupMockBlobStorage(runtime, nodeIndex, dsProxies);
                // Legacy fixtures use NodeWarden even with externally supplied
                // mock proxies. Keep it until those fixtures opt into mock-only setup.
                if (const auto& poolIds = runtime.GetBlobStorageExecutorPoolIds(); !poolIds.empty()) {
                    nodeWardenConfig->BlobStorageExecutorPoolIds = poolIds;
                }
                runtime.AddLocalService(MakeBlobStorageNodeWardenID(runtime.GetNodeId(nodeIndex)),
                    TActorSetupCmd(CreateBSNodeWarden(nodeWardenConfig), TMailboxType::Revolving, 0), nodeIndex);
            }

            SetupTabletResolver(runtime, nodeIndex);
            SetupTabletPipePerNodeCaches(runtime, nodeIndex, forceFollowers);
            SetupResourceBroker(runtime, nodeIndex, app.ResourceBrokerConfig);
            SetupSharedPageCache(runtime, nodeIndex, sharedCacheConfig ? *sharedCacheConfig : defaultSharedCacheConfig);
            SetupBlobCache(runtime, nodeIndex, app.BlobCacheConfig);
            SetupCSMetadataCache(runtime, nodeIndex);
            SetupCSColumnDataCache(runtime, nodeIndex);
            SetupSysViewService(runtime, nodeIndex);
            SetupQuoterService(runtime, nodeIndex);
            SetupStatService(runtime, nodeIndex);

            if (factory)
                factory->Birth(nodeIndex);
        }

        runtime.Initialize(app.Unwrap());

        for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
            // NodeWarden (and its actors) relies on timers to work correctly
            auto blobStorageActorId = runtime.GetLocalServiceId(
                MakeBlobStorageNodeWardenID(runtime.GetNodeId(nodeIndex)),
                nodeIndex);
            Y_ABORT_UNLESS(blobStorageActorId, "Missing node warden on node %" PRIu32, nodeIndex);
            runtime.EnableScheduleForActor(blobStorageActorId);

            // SysView Service uses Scheduler to send counters
            auto sysViewServiceId = runtime.GetLocalServiceId(
                NSysView::MakeSysViewServiceID(runtime.GetNodeId(nodeIndex)), nodeIndex);
            Y_ABORT_UNLESS(sysViewServiceId, "Missing SysView Service on node %" PRIu32, nodeIndex);
            runtime.EnableScheduleForActor(sysViewServiceId);
        }


        if (!mock && !runtime.IsRealThreads()) {
            ui32 evNum = disk.DomainsNum * disk.DisksInDomain;
            TDispatchOptions options;
            options.FinalEvents.push_back(
                TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvLocalRecoveryDone, evNum));
            runtime.DispatchEvents(options);
        }
    }
}

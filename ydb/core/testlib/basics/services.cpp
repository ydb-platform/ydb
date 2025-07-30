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
#include <ydb/core/blobstorage/dsproxy/mock/dsproxy_mock.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
#include <ydb/core/quoter/quoter_service.h>
#include <ydb/core/tablet/tablet_monitoring_proxy.h>
#include <ydb/core/tablet/resource_broker.h>
#include <ydb/core/tablet/node_tablet_monitor.h>
#include <ydb/core/tablet/tablet_list_renderer.h>
#include <ydb/core/tablet_flat/shared_sausagecache.h>
#include <ydb/core/tx/columnshard/data_accessor/cache_policy/policy.h>
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

namespace NPDisk {
    extern const ui64 YdbDefaultPDiskSequence = 0x7e5700007e570000;
}

    void SetupIcb(TTestActorRuntime& runtime, ui32 nodeIndex, const NKikimrConfig::TImmediateControlsConfig& config,
            const TIntrusivePtr<NKikimr::TControlBoard>& icb)
    {
        runtime.AddLocalService(MakeIcbId(runtime.GetNodeId(nodeIndex)),
            TActorSetupCmd(CreateImmediateControlActor(icb, runtime.GetDynamicCounters(nodeIndex)),
                    TMailboxType::ReadAsFilled, 0),
            nodeIndex);

        runtime.AddLocalService(TActorId{},
            TActorSetupCmd(NConsole::CreateImmediateControlsConfigurator(icb, config),
                    TMailboxType::ReadAsFilled, 0),
            nodeIndex);
    }

    void SetupBSNodeWarden(TTestActorRuntime& runtime, ui32 nodeIndex, TIntrusivePtr<TNodeWardenConfig> nodeWardenConfig)
    {
        runtime.AddLocalService(MakeBlobStorageNodeWardenID(runtime.GetNodeId(nodeIndex)),
            TActorSetupCmd(CreateBSNodeWarden(nodeWardenConfig), TMailboxType::Revolving, 0),
            nodeIndex);
    }

    void SetupSchemeCache(TTestActorRuntime& runtime, ui32 nodeIndex, const TString& root)
    {
        auto cacheConfig = MakeIntrusive<NSchemeCache::TSchemeCacheConfig>();
        cacheConfig->Roots.emplace_back(1, TTestTxConfig::SchemeShard, root);
        cacheConfig->Counters = new ::NMonitoring::TDynamicCounters();

        runtime.AddLocalService(MakeSchemeCacheID(),
            TActorSetupCmd(CreateSchemeBoardSchemeCache(cacheConfig.Get()), TMailboxType::Revolving, 0), nodeIndex);
    }

    void SetupTabletResolver(TTestActorRuntime& runtime, ui32 nodeIndex)
    {
        TIntrusivePtr<TTabletResolverConfig> tabletResolverConfig(new TTabletResolverConfig());
        //tabletResolverConfig->TabletCacheLimit = 1;

        IActor* tabletResolver = CreateTabletResolver(tabletResolverConfig);
        runtime.AddLocalService(MakeTabletResolverID(),
            TActorSetupCmd(tabletResolver, TMailboxType::Revolving, 0), nodeIndex);
    }

    void SetupTabletPipePerNodeCaches(TTestActorRuntime& runtime, ui32 nodeIndex, bool forceFollowers)
    {
        TIntrusivePtr<TPipePerNodeCacheConfig> leaderPipeConfig = new TPipePerNodeCacheConfig();
        leaderPipeConfig->PipeRefreshTime = TDuration::Zero();

        TIntrusivePtr<TPipePerNodeCacheConfig> followerPipeConfig = new TPipePerNodeCacheConfig();
        followerPipeConfig->PipeRefreshTime = TDuration::Seconds(30);
        followerPipeConfig->PipeConfig.AllowFollower = true;
        followerPipeConfig->PipeConfig.ForceFollower = forceFollowers;

        TIntrusivePtr<TPipePerNodeCacheConfig> persistentPipeConfig = new TPipePerNodeCacheConfig();
        persistentPipeConfig->PipeRefreshTime = TDuration::Zero();
        persistentPipeConfig->PipeConfig = TPipePerNodeCacheConfig::DefaultPersistentPipeConfig();

        runtime.AddLocalService(MakePipePerNodeCacheID(false),
            TActorSetupCmd(CreatePipePerNodeCache(leaderPipeConfig), TMailboxType::Revolving, 0), nodeIndex);
        runtime.AddLocalService(MakePipePerNodeCacheID(true),
            TActorSetupCmd(CreatePipePerNodeCache(followerPipeConfig), TMailboxType::Revolving, 0), nodeIndex);
        runtime.AddLocalService(MakePipePerNodeCacheID(EPipePerNodeCache::Persistent),
            TActorSetupCmd(CreatePipePerNodeCache(persistentPipeConfig), TMailboxType::Revolving, 0), nodeIndex);
    }

    void SetupResourceBroker(TTestActorRuntime& runtime, ui32 nodeIndex, const NKikimrResourceBroker::TResourceBrokerConfig& resourceBrokerConfig)
    {
        NKikimrResourceBroker::TResourceBrokerConfig config = NResourceBroker::MakeDefaultConfig();
        if (resourceBrokerConfig.IsInitialized()) {
            NResourceBroker::MergeConfigUpdates(config, resourceBrokerConfig);
        }

        runtime.AddLocalService(NResourceBroker::MakeResourceBrokerID(),
            TActorSetupCmd(
                NResourceBroker::CreateResourceBrokerActor(config, runtime.GetDynamicCounters(0)),
                TMailboxType::Revolving, 0),
            nodeIndex);
    }

    void SetupNodeWhiteboard(TTestActorRuntime& runtime, ui32 nodeIndex)
    {
        runtime.AddLocalService(NNodeWhiteboard::MakeNodeWhiteboardServiceId(runtime.GetNodeId(nodeIndex)),
            TActorSetupCmd(NNodeWhiteboard::CreateNodeWhiteboardService(), TMailboxType::Simple, 0), nodeIndex);
    }

    void SetupNodeTabletMonitor(TTestActorRuntime& runtime, ui32 nodeIndex)
    {
        runtime.AddLocalService(
            NNodeTabletMonitor::MakeNodeTabletMonitorID(runtime.GetNodeId(nodeIndex)),
            TActorSetupCmd(
                NNodeTabletMonitor::CreateNodeTabletMonitor(
                    new NNodeTabletMonitor::TTabletStateClassifier(),
                    new NNodeTabletMonitor::TTabletListRenderer()),
                TMailboxType::Simple, 0),
            nodeIndex);
    }

    void SetupMonitoringProxy(TTestActorRuntime& runtime, ui32 nodeIndex)
    {
        NTabletMonitoringProxy::TTabletMonitoringProxyConfig tabletMonitoringProxyConfig;
        tabletMonitoringProxyConfig.SetRetryLimitCount(1u);

        runtime.AddLocalService(NTabletMonitoringProxy::MakeTabletMonitoringProxyID(),
            TActorSetupCmd(NTabletMonitoringProxy::CreateTabletMonitoringProxy(std::move(tabletMonitoringProxyConfig)),
                           TMailboxType::Revolving, 0), nodeIndex);
    }

    void SetupGRpcProxyStatus(TTestActorRuntime& runtime, ui32 nodeIndex)
    {
        runtime.AddLocalService(MakeGRpcProxyStatusID(runtime.GetNodeId(nodeIndex)),
            TActorSetupCmd(CreateGRpcProxyStatus(), TMailboxType::Revolving, 0), nodeIndex);
    }

    void SetupSharedPageCache(TTestActorRuntime& runtime, ui32 nodeIndex, const NSharedCache::TSharedCacheConfig& config)
    {
        runtime.AddLocalService(NSharedCache::MakeSharedPageCacheId(0),
            TActorSetupCmd(
                NSharedCache::CreateSharedPageCache(config, runtime.GetDynamicCounters(nodeIndex)),
                TMailboxType::ReadAsFilled,
                0),
            nodeIndex);
    }

    void SetupCSMetadataCache(TTestActorRuntime& runtime, ui32 nodeIndex) {
        auto* actor = NOlap::NDataAccessorControl::TGeneralCache::CreateService(
			NGeneralCache::NPublic::TConfig::BuildDefault(), runtime.GetDynamicCounters(nodeIndex));
		runtime.AddLocalService(NOlap::NDataAccessorControl::TGeneralCache::MakeServiceId(runtime.GetNodeId(nodeIndex)),
			TActorSetupCmd(actor, TMailboxType::ReadAsFilled, 0), nodeIndex);
    }

    void SetupCSColumnDataCache(TTestActorRuntime& runtime, ui32 nodeIndex) {
        auto* actor = NOlap::NColumnFetching::TGeneralCache::CreateService(
            NGeneralCache::NPublic::TConfig::BuildDefault(), runtime.GetDynamicCounters(nodeIndex));
        runtime.AddLocalService(NOlap::NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(nodeIndex)),
            TActorSetupCmd(actor, TMailboxType::ReadAsFilled, 0), nodeIndex);
    }

    void SetupBlobCache(TTestActorRuntime& runtime, ui32 nodeIndex)
    {
        runtime.AddLocalService(NBlobCache::MakeBlobCacheServiceId(),
            TActorSetupCmd(
                NBlobCache::CreateBlobCache(20<<20, runtime.GetDynamicCounters(nodeIndex)),
                TMailboxType::ReadAsFilled,
                0),
            nodeIndex);
    }

    template<size_t N>
    static TIntrusivePtr<TStateStorageInfo> GenerateStateStorageInfo(const TActorId (&replicas)[N])
    {
        auto info = MakeIntrusive<TStateStorageInfo>();
        info->RingGroups.resize(1);
        auto& group = info->RingGroups.back();
        group.NToSelect = N;
        group.Rings.resize(N);
        for (size_t i = 0; i < N; ++i) {
            group.Rings[i].Replicas.push_back(replicas[i]);
        }

        return info;
    }

    static TIntrusivePtr<TStateStorageInfo> GenerateStateStorageInfo(const TVector<TActorId> &replicas, ui32 NToSelect, ui32 nrings, ui32 ringSize, ui32 ringGroups = 1)
    {
        Y_ABORT_UNLESS(replicas.size() >= ringGroups * nrings * ringSize);
        Y_ABORT_UNLESS(NToSelect <= nrings);

        auto info = MakeIntrusive<TStateStorageInfo>();
        info->RingGroups.resize(ringGroups);
        ui32 inode = 0;
        for (ui32 rg : xrange(ringGroups)) {
            auto& group = info->RingGroups[rg];
            group.NToSelect = NToSelect;
            group.Rings.resize(nrings);

            for (size_t i = 0; i < nrings; ++i) {
                for (size_t j = 0; j < ringSize; ++j) {
                    group.Rings[i].Replicas.push_back(replicas[inode++]);
                }
            }
        }

        return info;
    }

    TActorId MakeBoardReplicaID(ui32 node, ui32 replicaIndex) {
        char x[12] = { 's', 's', 'b' };
        x[3] = (char)1;
        memcpy(x + 5, &replicaIndex, sizeof(ui32));
        return TActorId(node, TStringBuf(x, 12));
    }

    void SetupCustomStateStorage(
        TTestActorRuntime &runtime,
        ui32 NToSelect,
        ui32 nrings,
        ui32 ringSize,
        ui32 ringGroups)
    {
        TVector<TActorId> ssreplicas;
        for (size_t i = 0; i < ringGroups * nrings * ringSize; ++i) {
            ssreplicas.push_back(MakeStateStorageReplicaID(runtime.GetNodeId(i), i));
        }

        TVector<TActorId> breplicas;
        for (size_t i = 0; i < ringGroups * nrings * ringSize; ++i) {
            breplicas.push_back(MakeBoardReplicaID(runtime.GetNodeId(i), i));
        }

        TVector<TActorId> sbreplicas;
        for (size_t i = 0; i < ringGroups * nrings * ringSize; ++i) {
            sbreplicas.push_back(MakeSchemeBoardReplicaID(runtime.GetNodeId(i), i));
        }

        const TActorId ssproxy = MakeStateStorageProxyID();

        auto ssInfo = GenerateStateStorageInfo(ssreplicas, NToSelect, nrings, ringSize, ringGroups);
        auto sbInfo = GenerateStateStorageInfo(sbreplicas, NToSelect, nrings, ringSize, ringGroups);
        auto bInfo = GenerateStateStorageInfo(breplicas, NToSelect, nrings, ringSize, ringGroups);


        for (ui32 ssIndex = 0; ssIndex < ringGroups * nrings * ringSize; ++ssIndex) {
            runtime.AddLocalService(ssreplicas[ssIndex],
                TActorSetupCmd(CreateStateStorageReplica(ssInfo.Get(), ssIndex), TMailboxType::Revolving, 0), ssIndex);
            runtime.AddLocalService(sbreplicas[ssIndex],
                TActorSetupCmd(CreateSchemeBoardReplica(sbInfo.Get(), ssIndex), TMailboxType::Revolving, 0), ssIndex);
            runtime.AddLocalService(breplicas[ssIndex],
                TActorSetupCmd(CreateStateStorageBoardReplica(bInfo.Get(), ssIndex), TMailboxType::Revolving, 0), ssIndex);
        }

        for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
            runtime.AddLocalService(ssproxy,
                    TActorSetupCmd(CreateStateStorageProxy(ssInfo.Get(), bInfo.Get(), sbInfo.Get()), TMailboxType::Revolving, 0), nodeIndex);
        }
    }


    void SetupStateStorage(TTestActorRuntime& runtime, ui32 nodeIndex, bool firstNode)
    {
        const TActorId ssreplicas[3] = {
            MakeStateStorageReplicaID(runtime.GetNodeId(0), 0),
            MakeStateStorageReplicaID(runtime.GetNodeId(0), 1),
            MakeStateStorageReplicaID(runtime.GetNodeId(0), 2),
        };

        const TActorId breplicas[3] = {
            MakeBoardReplicaID(runtime.GetNodeId(0), 0),
            MakeBoardReplicaID(runtime.GetNodeId(0), 1),
            MakeBoardReplicaID(runtime.GetNodeId(0), 2),
        };

        const TActorId sbreplicas[3] = {
            MakeSchemeBoardReplicaID(runtime.GetNodeId(0), 0),
            MakeSchemeBoardReplicaID(runtime.GetNodeId(0), 1),
            MakeSchemeBoardReplicaID(runtime.GetNodeId(0), 2),
        };

        const TActorId ssproxy = MakeStateStorageProxyID();

        auto ssInfo = GenerateStateStorageInfo(ssreplicas);
        auto sbInfo = GenerateStateStorageInfo(sbreplicas);
        auto bInfo = GenerateStateStorageInfo(breplicas);

        if (!firstNode || nodeIndex == 0) {
            for (ui32 i = 0; i < 3; ++i) {
                runtime.AddLocalService(ssreplicas[i],
                    TActorSetupCmd(CreateStateStorageReplica(ssInfo.Get(), i), TMailboxType::Revolving, 0), nodeIndex);
                runtime.AddLocalService(sbreplicas[i],
                    TActorSetupCmd(CreateSchemeBoardReplica(sbInfo.Get(), i), TMailboxType::Revolving, 0), nodeIndex);
                runtime.AddLocalService(breplicas[i],
                    TActorSetupCmd(CreateStateStorageBoardReplica(bInfo.Get(), i), TMailboxType::Revolving, 0), nodeIndex);
            }
        }

        runtime.AddLocalService(ssproxy,
            TActorSetupCmd(CreateStateStorageProxy(ssInfo.Get(), bInfo.Get(), sbInfo.Get()), TMailboxType::Revolving, 0), nodeIndex);
    }

    static void SetupStateStorageGroups(TTestActorRuntime& runtime, ui32 nodeIndex)
    {
        SetupStateStorage(runtime, nodeIndex, true);
    }

    namespace {

        void AddReplicas(TStateStorageInfo::TRingGroup& group, const TVector<TActorId>& replicas) {
            group.NToSelect = group.NToSelect ? group.NToSelect : replicas.size();
            group.Rings.resize(replicas.size());
            for (size_t i = 0; i < replicas.size(); ++i) {
                // one replica per ring
                group.Rings[i].Replicas.push_back(replicas[i]);
            }
        }

        TIntrusivePtr<TStateStorageInfo> GenerateStateStorageInfo(const TVector<TStateStorageInfo::TRingGroup>& ringGroups) {
            auto info = MakeIntrusive<TStateStorageInfo>();
            info->RingGroups = ringGroups;
            return info;
        }

    }

    TStateStorageSetupper CreateCustomStateStorageSetupper(const TVector<TStateStorageInfo::TRingGroup>& ringGroups, int replicasInRingGroup) {
        THashMap<ui32, TVector<ui32>> ringGroupsIdToNodeIds;
        for (ui32 i = 0; i < ringGroups.size(); ++i) {
            ringGroupsIdToNodeIds[i] = TVector<ui32>(replicasInRingGroup, 0);
        }
        return CreateCustomStateStorageSetupper(ringGroups, ringGroupsIdToNodeIds);
    }

    TStateStorageSetupper CreateCustomStateStorageSetupper(const TVector<TStateStorageInfo::TRingGroup>& ringGroups,
                                                           const THashMap<ui32, TVector<ui32>>& ringGroupIdToNodeIds) {
        return [=](TTestActorRuntime& runtime, ui32 nodeIndex) {
            TSet<ui32> nodes;
            for (const auto& [_, nodeIds] : ringGroupIdToNodeIds) {
                nodes.insert(nodeIds.begin(), nodeIds.end());
            }
            auto ssInfo = GenerateStateStorageInfo(ringGroups);
            auto sbInfo = GenerateStateStorageInfo(ringGroups);
            auto bInfo = GenerateStateStorageInfo(ringGroups);
            for (const auto& [pileId, nodeIds] : ringGroupIdToNodeIds) {
                auto addReplicas = [&](auto& group, auto makeId) {
                    TVector<TActorId> replicas;
                    for (ui32 i = 0; i < nodeIds.size(); ++i) {
                        replicas.emplace_back(makeId(runtime.GetNodeId(nodeIds[i]), nodeIds.size() * pileId + i));
                    }
                    AddReplicas(group, replicas);
                    return replicas;
                };

                auto ssreplicas = addReplicas(ssInfo->RingGroups[pileId], MakeStateStorageReplicaID);
                auto sbreplicas = addReplicas(sbInfo->RingGroups[pileId], MakeSchemeBoardReplicaID);
                auto breplicas = addReplicas(bInfo->RingGroups[pileId], MakeBoardReplicaID);

                auto addLocalServices = [&](const TVector<TActorId>& replicas, auto createCmd, auto* info) {
                    for (ui32 i = 0; i < nodeIds.size(); ++i) {
                        runtime.AddLocalService(
                            replicas[i],
                            TActorSetupCmd(createCmd(info, nodeIds.size() * pileId + i), TMailboxType::Revolving, 0),
                            nodeIndex
                        );
                    }
                };

                if (nodes.contains(nodeIndex)) {
                    addLocalServices(ssreplicas, CreateStateStorageReplica, ssInfo.Get());
                    addLocalServices(sbreplicas, CreateSchemeBoardReplica, sbInfo.Get());
                    addLocalServices(breplicas, CreateStateStorageBoardReplica, bInfo.Get());
                }
            }

            const TActorId ssproxy = MakeStateStorageProxyID();
            runtime.AddLocalService(ssproxy,
                TActorSetupCmd(CreateStateStorageProxy(ssInfo.Get(), bInfo.Get(), sbInfo.Get()), TMailboxType::Revolving, 0),
                nodeIndex
            );
        };
    }

    constexpr int ReplicasInRingGroup = 3;

    TStateStorageSetupper CreateDefaultStateStorageSetupper() {
        return CreateCustomStateStorageSetupper({ TStateStorageInfo::TRingGroup{} }, ReplicasInRingGroup);
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
        runtime.SetDispatchTimeout(storage.UseDisk ? DISK_DISPATCH_TIMEOUT : DEFAULT_DISPATCH_TIMEOUT);

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

        NSharedCache::TSharedCacheConfig defaultSharedCacheConfig;
        defaultSharedCacheConfig.SetMemoryLimit(32_MB);

        for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
            SetupStateStorageGroups(runtime, nodeIndex);
            NKikimrProto::TKeyConfig keyConfig;
            if (const auto it = app.Keys.find(nodeIndex); it != app.Keys.end()) {
                keyConfig = it->second;
            }
            SetupIcb(runtime, nodeIndex, app.ImmediateControlsConfig, app.Icb[nodeIndex]);
            for (const auto& dsProxy : dsProxies) {
                runtime.AddLocalService(
                    MakeBlobStorageProxyID(dsProxy->GetGroupId()),
                    TActorSetupCmd(CreateBlobStorageGroupProxyMockActor(dsProxy), TMailboxType::ReadAsFilled, 0),
                    nodeIndex);
            }
            SetupBSNodeWarden(runtime, nodeIndex, disk.MakeWardenConf(*app.Domains, keyConfig));

            SetupTabletResolver(runtime, nodeIndex);
            SetupTabletPipePerNodeCaches(runtime, nodeIndex, forceFollowers);
            SetupResourceBroker(runtime, nodeIndex, app.ResourceBrokerConfig);
            SetupSharedPageCache(runtime, nodeIndex, sharedCacheConfig ? *sharedCacheConfig : defaultSharedCacheConfig);
            SetupBlobCache(runtime, nodeIndex);
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

            // TabletResolver needs timers for retries
            auto tabletResolverActorId = runtime.GetLocalServiceId(MakeTabletResolverID(), nodeIndex);
            Y_ABORT_UNLESS(tabletResolverActorId, "Missing TabletResolver on node %" PRIu32, nodeIndex);
            runtime.EnableScheduleForActor(tabletResolverActorId);
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

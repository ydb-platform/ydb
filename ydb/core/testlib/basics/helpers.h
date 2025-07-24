#pragma once

#include "appdata.h"
#include "runtime.h"

#include <ydb/core/tablet_flat/shared_sausagecache.h>
#include <ydb/core/util/defs.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/blobstorage/dsproxy/mock/model.h>
#include <ydb/core/blobstorage/nodewarden/node_warden.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_factory.h>

#include <functional>

namespace NKikimr {
namespace NFake {
    struct TStorage {
        bool UseDisk = false;
        ui64 SectorSize = 0;
        ui64 ChunkSize = 0;
        ui64 DiskSize = 0;
        bool FormatDisk = true;
        TString DiskPath;
    };

    struct INode {
        virtual void Birth(ui32 node) noexcept = 0;
    };
}

    const TBlobStorageGroupType::EErasureSpecies BootGroupErasure = TBlobStorageGroupType::ErasureNone;
    using TStateStorageSetupper = std::function<void(TTestActorRuntime&, ui32)>;

    TTabletStorageInfo* CreateTestTabletInfo(ui64 tabletId, TTabletTypes::EType tabletType,
        TBlobStorageGroupType::EErasureSpecies erasure = BootGroupErasure, ui32 groupId = 0);
    TActorId CreateTestBootstrapper(TTestActorRuntime &runtime, TTabletStorageInfo *info,
        std::function<IActor* (const TActorId &, TTabletStorageInfo*)> op, ui32 nodeIndex = 0);
    TActorId StartTestTablet(TTestActorRuntime &runtime, TTabletStorageInfo *info,
        std::function<IActor* (const TActorId &, TTabletStorageInfo*)> op, ui32 nodeIndex = 0);
    NTabletPipe::TClientConfig GetPipeConfigWithRetries();

    void SetupStateStorage(TTestActorRuntime& runtime, ui32 nodeIndex,
                           bool replicasOnFirstNode = false);
    void SetupCustomStateStorage(TTestActorRuntime &runtime, ui32 NToSelect, ui32 nrings, ui32 ringSize);
    TStateStorageSetupper CreateCustomStateStorageSetupper(const TVector<TStateStorageInfo::TRingGroup>& ringGroups, int replicasInRingGroup);
    TStateStorageSetupper CreateCustomStateStorageSetupper(const TVector<TStateStorageInfo::TRingGroup>& ringGroups,
                                                           const THashMap<ui32, TVector<ui32>>& pileIdToNodeIds);
    TStateStorageSetupper CreateDefaultStateStorageSetupper();
    void SetupBSNodeWarden(TTestActorRuntime& runtime, ui32 nodeIndex, TIntrusivePtr<TNodeWardenConfig> nodeWardenConfig);
    void SetupTabletResolver(TTestActorRuntime& runtime, ui32 nodeIndex);
    void SetupTabletPipePerNodeCaches(TTestActorRuntime& runtime, ui32 nodeIndex, bool forceFollowers = false);
    void SetupResourceBroker(TTestActorRuntime& runtime, ui32 nodeIndex, const NKikimrResourceBroker::TResourceBrokerConfig& resourceBrokerConfig);
    void SetupSharedPageCache(TTestActorRuntime& runtime, ui32 nodeIndex, const NSharedCache::TSharedCacheConfig& sharedCacheConfig);
    void SetupNodeWhiteboard(TTestActorRuntime& runtime, ui32 nodeIndex);
    void SetupMonitoringProxy(TTestActorRuntime& runtime, ui32 nodeIndex);
    void SetupGRpcProxyStatus(TTestActorRuntime& runtime, ui32 nodeIndex);
    void SetupNodeTabletMonitor(TTestActorRuntime& runtime, ui32 nodeIndex);
    void SetupSchemeCache(TTestActorRuntime& runtime, ui32 nodeIndex, const TString& root);
    void SetupQuoterService(TTestActorRuntime& runtime, ui32 nodeIndex);
    void SetupSysViewService(TTestActorRuntime& runtime, ui32 nodeIndex);
    void SetupIcb(TTestActorRuntime& runtime, ui32 nodeIndex, const NKikimrConfig::TImmediateControlsConfig& config,
            const TIntrusivePtr<NKikimr::TControlBoard>& icb);

    // StateStorage, NodeWarden, TabletResolver, ResourceBroker, SharedPageCache
    void SetupBasicServices(TTestActorRuntime &runtime, TAppPrepare &app, bool mockDisk = false,
                            NFake::INode *factory = nullptr, NFake::TStorage storage = {}, const NSharedCache::TSharedCacheConfig* sharedCacheConfig = nullptr, bool forceFollowers = false,
                            TVector<TIntrusivePtr<NFake::TProxyDS>> dsProxies = {});

    ///
    class TStrandedPDiskServiceFactory : public IPDiskServiceFactory {
        TTestActorRuntime &Runtime;
    public:
        TStrandedPDiskServiceFactory(TTestActorRuntime &runtime)
            : Runtime(runtime)
        {}

        void Create(const TActorContext &ctx, ui32 pDiskID, const TIntrusivePtr<TPDiskConfig> &cfg,
            const NPDisk::TMainKey &mainKey, ui32 poolId, ui32 nodeId) override;

        virtual ~TStrandedPDiskServiceFactory()
        {}
    };

    TActorId MakeBoardReplicaID(ui32 node, ui32 replicaIndex);

}

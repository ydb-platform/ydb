#pragma once
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/tablet_flat/shared_sausagecache.h>
#include <functional>
namespace NKikimr {
    using TStateStorageSetupper = std::function<void(TTestActorRuntime&, ui32)>;

    TTabletStorageInfo* CreateTestTabletInfo(ui64 tabletId, TTabletTypes::EType tabletType,
        TBlobStorageGroupType::EErasureSpecies erasure = TBlobStorageGroupType::ErasureNone, ui32 groupId = 0);
    TActorId CreateTestBootstrapper(TTestActorRuntime &runtime, TTabletStorageInfo *info,
        std::function<IActor* (const TActorId &, TTabletStorageInfo*)> op, ui32 nodeIndex = 0);
    TActorId StartTestTablet(TTestActorRuntime &runtime, TTabletStorageInfo *info,
        std::function<IActor* (const TActorId &, TTabletStorageInfo*)> op, ui32 nodeIndex = 0);
    NTabletPipe::TClientConfig GetPipeConfigWithRetries();

    void SetupStateStorage(TTestActorRuntime& runtime, ui32 nodeIndex,
                           bool replicasOnFirstNode = false);
    void SetupCustomStateStorage(TTestActorRuntime &runtime, ui32 NToSelect, ui32 nrings, ui32 ringSize, ui32 ringGroups = 1);
    TStateStorageSetupper CreateCustomStateStorageSetupper(const TVector<TStateStorageInfo::TRingGroup>& ringGroups, int replicasInRingGroup);
    TStateStorageSetupper CreateCustomStateStorageSetupper(const TVector<TStateStorageInfo::TRingGroup>& ringGroups,
                                                           const THashMap<ui32, TVector<ui32>>& pileIdToNodeIds);
    TStateStorageSetupper CreateDefaultStateStorageSetupper();
    void SetupTabletResolver(TTestActorRuntime&, ui32 nodeIndex);
    void SetupTabletPipePerNodeCaches(TTestActorRuntime&, ui32 nodeIndex, bool forceFollowers);
    void SetupResourceBroker(TTestActorRuntime&, ui32 nodeIndex, const NKikimrResourceBroker::TResourceBrokerConfig&);
    void SetupNodeWhiteboard(TTestActorRuntime&, ui32 nodeIndex);
    void SetupNodeTabletMonitor(TTestActorRuntime&, ui32 nodeIndex);
    void SetupMonitoringProxy(TTestActorRuntime&, ui32 nodeIndex);
    void SetupSharedPageCache(TTestActorRuntime&, ui32 nodeIndex, const NSharedCache::TSharedCacheConfig&);
    void SetupStateStorageGroups(TTestActorRuntime&, ui32 nodeIndex);
    TActorId MakeBoardReplicaID(ui32 node, ui32 replicaIndex);
}

#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NKikimrBlobStorage {
    class TStorageConfig;
}

namespace NKikimr::NBsController {

using namespace NActors;

struct TClusterBalancingSettings {
    bool Enable = false;
    ui32 MaxReplicatingPDisks = 5;
    ui32 MaxReplicatingVDisks = 40;
    ui32 IterationIntervalMs = 5000;
};

TClusterBalancingSettings ParseClusterBalancingSettings(const std::shared_ptr<const NKikimrBlobStorage::TStorageConfig> storageConfig);

IActor* CreateClusterBalancingActor(const TActorId& controllerId, const TClusterBalancingSettings& settings);

} // NKikimr::NBsController

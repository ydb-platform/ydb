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
    ui64 IterationIntervalMs = 5000;
    ui32 MaxReassignAttemptsPerBucketPerIteration = 5;
    bool PreferLessOccupiedRack = false;
    bool WithAttentionToReplication = false;
};

TClusterBalancingSettings ParseClusterBalancingSettings(const std::shared_ptr<const NKikimrBlobStorage::TStorageConfig> storageConfig);

IActor* CreateClusterBalancingActor(const TActorId& controllerId, const TClusterBalancingSettings& settings);

} // NKikimr::NBsController

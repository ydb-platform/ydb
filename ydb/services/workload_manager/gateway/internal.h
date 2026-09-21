#pragma once

#include <ydb/services/workload_manager/gateway.h>
#include <ydb/services/workload_manager/metadata_subscription/resource_pool_classifier/snapshot.h>

#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/protos/workload_manager_config.pb.h>

#include <ydb/library/actors/core/actorid.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/system/spinlock.h>

#include <memory>


namespace NKikimr::NWorkloadManager::NPrivate {

struct TDatabaseInfo {
    bool Serverless = false;
};

struct TSnapshot {
    TResourcePoolMapPtr Pools;
    std::shared_ptr<const TResourcePoolClassifierSnapshot> Classifiers;
    NKikimrConfig::TFeatureFlags FeatureFlags;
    NKikimrConfig::TWorkloadManagerConfig WorkloadManagerConfig;
    THashMap<TString, TDatabaseInfo> Databases;
    bool EnableResourcePools = false;
    bool EnableResourcePoolsOnServerless = false;

    bool IsResourcePoolsEnabled(const TString& databaseId) const {
        if (!EnableResourcePools) {
            return false;
        }
        if (EnableResourcePoolsOnServerless) {
            return true;
        }
        const auto it = Databases.find(databaseId);
        return it == Databases.end() || !it->second.Serverless;
    }
};

using TSnapshotPtr = std::shared_ptr<const TSnapshot>;


class TWorkloadManagerGateway : public IGateway {
public:
    void OnRegistered(NActors::TActorId cacheActorId, NActors::TActorId workloadManagerServiceId, ui32 nodeId) {
        CacheActorId_ = cacheActorId;
        WorkloadManagerServiceId_ = workloadManagerServiceId;
        NodeId_ = nodeId;
    }

    void PublishSnapshot(TSnapshotPtr snapshot) {
        with_lock (Lock_) {
            Snapshot_ = std::move(snapshot);
        }
    }

    ui32 GetNodeId() const {
        return NodeId_;
    }

    std::shared_ptr<IQueryClassifier> TryCreateQueryClassifier(
        const TString& databaseId, TClassifyContext context) override;

private:
    mutable TAdaptiveLock Lock_;
    TSnapshotPtr Snapshot_;
    NActors::TActorId CacheActorId_;
    NActors::TActorId WorkloadManagerServiceId_;
    ui32 NodeId_ = 0;
};


void RegisterGateway(std::shared_ptr<TWorkloadManagerGateway> gateway, ui32 nodeId);

}

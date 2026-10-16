#include "resource_pool_tracker.h"

#include <ydb/services/workload_manager/common/helpers.h>


namespace NKikimr::NWorkloadManager::NPrivate {

bool TResourcePoolTracker::TrySubscribe(const TString& databaseId, const TString& poolId) {
    const TString poolKey = GetPoolKey(databaseId, poolId);
    if (const auto it = Pools_.find(poolKey); it != Pools_.end() && !it->second.Expired) {
        return false;
    }
    return InFlightFetches_.insert(poolKey).second;
}

std::vector<std::pair<TString, TString>> TResourcePoolTracker::TrySubscribeClassifierPools(const TResourcePoolClassifierSnapshot& classifiers) {
    std::vector<std::pair<TString, TString>> result;
    for (const auto& [databaseId, info] : classifiers.GetResourcePoolClassifierConfigs()) {
        for (const auto& [_, classifier] : info.ByName) {
            if (const auto& poolId = classifier.GetClassifierSettings().ResourcePool) {
                if (TrySubscribe(databaseId, *poolId)) {
                    result.emplace_back(databaseId, *poolId);
                }
            }
        }
    }
    return result;
}

bool TResourcePoolTracker::OnPoolInfo(const TString& databaseId, const TString& poolId,
                                      const std::optional<NResourcePool::TPoolSettings>& config,
                                      const std::optional<NACLib::TSecurityObject>& securityObject)
{
    const TString poolKey = GetPoolKey(databaseId, poolId);
    if (!config) {
        const auto it = Pools_.find(poolKey);
        if (it == Pools_.end()) {
            // Our own fetch returned "not found" — release the in-flight lock for future retries.
            InFlightFetches_.erase(poolKey);
            return false;
        }
        if (it->second.Expired) {
            // Second nullopt confirms the pool is gone
            Pools_.erase(it);
            InFlightFetches_.erase(poolKey);
            return false;
        }
        it->second.Expired = true;
        // Arm a recheck to verify the deletion unless an op is already in flight; hold the lock until the response arrives.
        return InFlightFetches_.insert(poolKey).second;
    }

    auto& poolInfo = Pools_[poolKey];
    poolInfo.Config = *config;
    poolInfo.SecurityObject = securityObject;
    poolInfo.Expired = false;
    InFlightFetches_.erase(poolKey);
    return false;
}

TResourcePoolMapPtr TResourcePoolTracker::BuildSnapshot() const {
    auto pools = std::make_shared<TResourcePoolMap>();
    pools->reserve(Pools_.size());
    for (const auto& [key, info] : Pools_) {
        if (!info.Expired) {
            pools->emplace(key, TResourcePoolEntry{info.Config, info.SecurityObject});
        }
    }
    return pools;
}

}

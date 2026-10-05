#pragma once

#include <ydb/services/workload_manager/metadata_subscription/resource_pool_classifier/snapshot.h>
#include <ydb/services/workload_manager/query_classifier.h>

#include <ydb/core/resource_pools/resource_pool_settings.h>
#include <ydb/library/aclib/aclib.h>

#include <util/generic/string.h>

#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>


namespace NKikimr::NWorkloadManager::NPrivate {

///
/// Tracks resource pool configs for DB.
///
class TResourcePoolTracker {
    struct TPoolInfo {
        NResourcePool::TPoolSettings Config;
        std::optional<NACLib::TSecurityObject> SecurityObject;
        bool Expired = false;  // got nullopt once, recheck in flight; hidden from snapshot
    };

public:
    /// True if the caller must subscribe (TEvAddPool + TEvSubscribeOnPoolChanges).
    /// False if the pool is cached or a subscription is already in flight.
    bool TrySubscribe(const TString& databaseId, const TString& poolId);

    /// TrySubscribe for every pool referenced by classifiers; returns pools to subscribe.
    std::vector<std::pair<TString, TString>> TrySubscribeClassifierPools(const TResourcePoolClassifierSnapshot& classifiers);

    /// Applies TEvUpdatePoolInfo. nullopt config marks the pool expired, a second nullopt erases it.
    /// True if the caller must send TEvSubscribeOnPoolChanges to recheck a possibly deleted pool.
    bool OnPoolInfo(const TString& databaseId, const TString& poolId,
                    const std::optional<NResourcePool::TPoolSettings>& config,
                    const std::optional<NACLib::TSecurityObject>& securityObject);

    /// Non-expired pools keyed by GetPoolKey(databaseId, poolId).
    /// TODO: cache the map and rebuild only after a pool change; now every Rebuild() copies all pools.
    TResourcePoolMapPtr BuildSnapshot() const;

private:
    std::unordered_map<TString, TPoolInfo> Pools_;
    std::unordered_set<TString> InFlightFetches_;
};

}

#include "validators.h"

#include <util/generic/hash_set.h>
#include <util/string/join.h>

#include <cmath>
#include <set>

namespace NKikimr::NConfig {

namespace {

const THashSet<TString> Categories = {
    "scan",
    "compaction",
    "insert",
    "deduplication",
    "normalizer",
};

EValidationResult Fail(std::vector<TString>& errors, TString error) {
    errors.emplace_back(std::move(error));
    return EValidationResult::Error;
}

}   // namespace

EValidationResult ValidateCompositeConveyorConfig(
    const NKikimrConfig::TCompositeConveyorConfig& config,
    std::vector<TString>& errors) {
    THashSet<TString> configuredCategories;
    for (const auto& category : config.GetCategories()) {
        if (!Categories.contains(category.GetName())) {
            return Fail(errors, "unknown composite conveyor category: " + category.GetName());
        }
        if (!configuredCategories.emplace(category.GetName()).second) {
            return Fail(errors, "duplicate composite conveyor category: " + category.GetName());
        }
    }

    THashSet<TString> poolNames;
    for (const auto& pool : config.GetWorkerPools()) {
        if (pool.GetLinks().empty()) {
            return Fail(errors, "composite conveyor worker pool has no category links");
        }
        if (pool.HasWorkersCount()) {
            if (!std::isfinite(pool.GetWorkersCount()) || pool.GetWorkersCount() <= 0) {
                return Fail(errors, "invalid composite conveyor workers count: " + ::ToString(pool.GetWorkersCount()));
            }
        } else if (pool.HasDefaultFractionOfThreadsCount()) {
            const double fraction = pool.GetDefaultFractionOfThreadsCount();
            if (!std::isfinite(fraction) || fraction <= 0 || fraction > 1) {
                return Fail(errors, "invalid composite conveyor workers fraction: " + ::ToString(fraction));
            }
        }

        std::set<TString> linkedCategories;
        for (const auto& link : pool.GetLinks()) {
            if (!Categories.contains(link.GetCategory())) {
                return Fail(errors, "unknown composite conveyor link category: " + link.GetCategory());
            }
            if (!linkedCategories.emplace(link.GetCategory()).second) {
                return Fail(errors, "duplicate composite conveyor link category: " + link.GetCategory());
            }
            if (link.HasWeight() && (!std::isfinite(link.GetWeight()) || link.GetWeight() <= 0)) {
                return Fail(errors, "invalid composite conveyor link weight: " + ::ToString(link.GetWeight()));
            }
        }

        TString poolName = pool.GetName();
        if (!poolName || poolName == "WP::DEFAULT") {
            poolName = "WP::" + JoinSeq("-", linkedCategories);
        }
        if (!poolNames.emplace(poolName).second) {
            return Fail(errors, "duplicate composite conveyor worker pool name: " + poolName);
        }
    }
    return EValidationResult::Ok;
}

}   // namespace NKikimr::NConfig

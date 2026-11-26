#pragma once

#include "kqp_compute_scheduler_service.h"

namespace NKikimr::NKqp::NScheduler {

// Concise setup for existing scheduling tests. Every helper publishes a complete
// configuration, and replacing attributes resets omitted fields to defaults.
class TTestComputeScheduler : public TComputeScheduler {
public:
    using TComputeScheduler::TComputeScheduler;

    void SetTotalCpuLimit(ui64 cpu) {
        auto config = GetStaticTree()->Clone();
        config->TotalCpuLimit = cpu;
        UpdateConfiguration(std::move(config));
    }

    void SetTotalMemoryLimit(ui64 bytes) {
        auto config = GetStaticTree()->Clone();
        config->TotalMemoryLimit = bytes;
        UpdateConfiguration(std::move(config));
    }

    void AddOrUpdateDatabase(const NHdrf::TDatabaseId& databaseId, const NHdrf::TStaticAttributes& attrs) {
        auto config = GetStaticTree()->Clone();
        config->GetOrCreateDatabase(databaseId).SetAttributes(attrs);
        UpdateConfiguration(std::move(config));
    }

    void AddOrUpdatePool(const NHdrf::TDatabaseId& databaseId, const NHdrf::TPoolId& poolId, const NHdrf::TStaticAttributes& attrs) {
        auto config = GetStaticTree()->Clone();
        config->GetOrCreateDatabase(databaseId).GetOrCreatePool(poolId).SetAttributes(attrs);
        UpdateConfiguration(std::move(config));
    }
};

} // namespace NKikimr::NKqp::NScheduler

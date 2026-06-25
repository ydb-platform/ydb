#pragma once

#include "kqp_compute_scheduler_service.h"
#include "kqp_schedulable_task.h"

namespace NKikimr::NKqp::NScheduler {

// Create special query on demand inside each pool - for datashards only
struct TSchedulableRead : TSchedulableTask {
    explicit TSchedulableRead(const NHdrf::NDynamic::TQueryPtr& query);

    bool TryConsumeQuota(TDuration expectedQuota);
    void ReturnQuota(NHPTimer::STime elapsedCycles = 0);

    // Estimate delay until quota becomes available.
    // Must be called only after refilling is already done, i.e. TryConsumeQuota or HasAvailableQuota fails.
    TDuration EstimateQuotaDelay(TDuration expectedQuota) const;

    // Non-consuming availability check: refills the bucket and reports whether any
    // quota is left, without reserving it or touching fair-share usage.
    bool HasAvailableQuota();

    // Returns false if the MaxQuotaMs is zero. It's possible if the resource pool
    // also has TOTAL_CPU_LIMIT_PERCENT = 0.
    bool IsValid() const {
        return MaxQuotaMs != 0;
    }

private:
    // Milliseconds precision - because THPTimer::STime to TDuration has the same precision
    ui64 MaxQuotaMs;
    double QuotaPerSecond;
    ui64 ReservedQuotaMs = 0;
    i64 AvailableQuotaMs = 0;

    mutable ui8 FairShareRetryCount = 0;

    TMonotonic LastRefill;
};

// Thread-unsafe and should be used by a single actor (i.e. Datashard)
class TSchedulableReadFactory {
public:
    explicit TSchedulableReadFactory(TComputeSchedulerPtr scheduler);

    TSchedulableReadPtr Get(const NHdrf::TDatabaseId& databaseId, const NHdrf::TPoolId& poolId) const;

    void CleanupReadsCache() const;

private:
    TComputeSchedulerPtr Scheduler;
    mutable THashMap<std::pair<NHdrf::TDatabaseId, NHdrf::TPoolId>, TSchedulableReadPtr::weak_type> ReadsCache;
};

} // namespace NKikimr::NKqp::NScheduler

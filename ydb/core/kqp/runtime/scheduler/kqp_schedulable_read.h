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
    // Must be called right after TryConsumeQuota fails (refill already done).
    TDuration EstimateQuotaDelay(TDuration expectedQuota) const;

private:
    // Milliseconds precision - because THPTimer::STime to TDuration has the same precision
    ui64 MaxQuotaMs;
    double QuotaPerSecond;
    ui64 ReservedQuotaMs = 0;
    i64 AvailableQuotaMs = 0;

    TMonotonic LastRefill;
};

class TSchedulableReadFactory {
public:
    explicit TSchedulableReadFactory(TComputeSchedulerPtr scheduler);

    TSchedulableReadPtr Get(const NHdrf::TPoolId& poolId) const;

private:
    TComputeSchedulerPtr Scheduler;
};

} // namespace NKikimr::NKqp::NScheduler

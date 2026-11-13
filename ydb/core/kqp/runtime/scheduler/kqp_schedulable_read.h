#pragma once

#include "kqp_compute_scheduler_service.h"
#include "kqp_schedulable_task.h"

namespace NKikimr::NKqp::NScheduler {

// Create special query on demand inside each pool - for datashards only
struct TSchedulableRead : TSchedulableTask {
    // The factory retains the budget independently of readers and their CPU demand.
    struct TQuotaState {
        ui64 MaxQuotaMs = 1000;
        double QuotaPerSecond = 1.0;
        i64 AvailableQuotaMs = 1000;
        TMonotonic LastRefill = TMonotonic::Now();

        void Refill();
    };

    TSchedulableRead(const NHdrf::NDynamic::TQueryPtr& query, std::shared_ptr<TQuotaState> quota);

    bool TryConsumeQuota(TDuration expectedQuota);
    // An admitted first read must finish without quota waits while it holds
    // pipeline dependencies. Reserve and charge its attempts even over quota.
    void ConsumeQuotaForAdmittedRead(TDuration expectedQuota);
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
        return Quota->MaxQuotaMs != 0;
    }

private:
    void ReserveQuota(ui64 expectedQuotaMs);

    // Milliseconds precision - because THPTimer::STime to TDuration has the same precision
    const std::shared_ptr<TQuotaState> Quota;
    ui64 ReservedQuotaMs = 0;

    mutable ui8 FairShareRetryCount = 0;
};

// Thread-unsafe and should be used by a single actor (i.e. Datashard)
class TSchedulableReadFactory {
public:
    explicit TSchedulableReadFactory(TComputeSchedulerPtr scheduler);

    TSchedulableReadPtr Get(const NHdrf::TDatabaseId& databaseId, const NHdrf::TPoolId& poolId) const;

    void CleanupReadsCache() const;

private:
    struct TCachedRead {
        std::shared_ptr<TSchedulableRead::TQuotaState> Quota;
        TSchedulableReadPtr::weak_type Read;
    };

    TComputeSchedulerPtr Scheduler;
    mutable THashMap<NHdrf::TFullPoolId, TCachedRead> ReadsCache;
};

} // namespace NKikimr::NKqp::NScheduler

#include "kqp_schedulable_read.h"

#include "log.h"
#include "tree/dynamic.h"

#include <yql/essentials/utils/yql_panic.h>
#include <yt/yt/core/utilex/random.h>

namespace NKikimr::NKqp::NScheduler {

TSchedulableRead::TSchedulableRead(const NHdrf::NDynamic::TQueryPtr& query)
    : TSchedulableTask(query)
    // TODO: we add demand to virtual query per reading datashard, which is inconvenient.
    //       Because demand persists even if we don't have any pending read requests.
    //       Should actualize demand depending on real requests - in best case scenario: append to real query.
{
    if (query->GetParent()->ReadLimit) {
        MaxQuotaMs = query->GetParent()->ReadLimit->MilliSeconds();
        QuotaPerSecond = MaxQuotaMs / 1000.0;
    } else {
        MaxQuotaMs = 1000;
        QuotaPerSecond = 1.0;
    }

    AvailableQuotaMs = MaxQuotaMs;
    LastRefill = TMonotonic::Now();

    LOG_T("TSchedulableRead MaxQuotaMs: " << MaxQuotaMs);

    YQL_ENSURE(MaxQuotaMs <= 1000);
}

bool TSchedulableRead::TryConsumeQuota(TDuration expectedQuota) {
    // TODO: support update of the pool's read quota on AddOrUpdatePool().
    auto expectedQuotaMs = std::min(expectedQuota.MilliSeconds(), MaxQuotaMs);

    LOG_T("TSchedulableRead ExpectedQuotaMs: " << expectedQuotaMs);

    // Refill quota
    if (const auto now = TMonotonic::Now(); Y_LIKELY(now >= LastRefill)) {
        auto elapsedMs = (now - LastRefill).MilliSeconds();
        AvailableQuotaMs = std::min<i64>(MaxQuotaMs, AvailableQuotaMs + (elapsedMs * QuotaPerSecond));
        LastRefill = now;
    }

    LOG_T("TSchedulableRead AvailableQuotaMs: " << AvailableQuotaMs);

    if (AvailableQuotaMs <= 0 || !TryIncreaseUsage()) {
        return false;
    }

    AvailableQuotaMs -= expectedQuotaMs;
    ReservedQuotaMs = expectedQuotaMs;

    LOG_T("TSchedulableRead ReservedQuotaMs: " << ReservedQuotaMs);

    return true;
}

void TSchedulableRead::ReturnQuota(NHPTimer::STime elapsedCycles) {
    static const double msPerCycle = 1000.0 / NHPTimer::GetCyclesPerSecond();

    Y_ENSURE(ReservedQuotaMs);

    auto ms = static_cast<ui64>(elapsedCycles * msPerCycle);
    AvailableQuotaMs = std::min<i64>(MaxQuotaMs, AvailableQuotaMs + ReservedQuotaMs - ms);
    ReservedQuotaMs = 0;

    LOG_T("TSchedulableRead ReturnedQuotaMs: " << ms);
    LOG_T("TSchedulableRead AvailableQuotaMs: " << AvailableQuotaMs);

    DecreaseUsage(TDuration::MilliSeconds(ms), READ_DEFAULT);
}

TDuration TSchedulableRead::EstimateQuotaDelay(TDuration expectedQuota) const {
    auto expectedQuotaMs = std::min(expectedQuota.MilliSeconds(), MaxQuotaMs);

    if (AvailableQuotaMs >= static_cast<i64>(expectedQuotaMs)) {
        // Quota available, but TryIncreaseUsage() failed (fair-share exhausted)
        return TDuration::MilliSeconds(10) + RandomDuration(TDuration::MilliSeconds(1));
    }

    // Quota deficit — calculate refill time
    i64 deficitMs = static_cast<i64>(expectedQuotaMs) - AvailableQuotaMs;
    ui64 waitMs = static_cast<ui64>(std::ceil(deficitMs / QuotaPerSecond));
    auto delay = TDuration::MilliSeconds(std::max<ui64>(waitMs, 1));

    return delay;
}

TSchedulableReadFactory::TSchedulableReadFactory(TComputeSchedulerPtr scheduler)
    : Scheduler(std::move(scheduler))
{}

TSchedulableReadPtr TSchedulableReadFactory::Get(const NHdrf::TPoolId& poolId) const {
    auto query = Scheduler->GetReadQuery(poolId);
    return std::make_shared<TSchedulableRead>(query);
}

} // namespace NKikimr::NKqp::NScheduler

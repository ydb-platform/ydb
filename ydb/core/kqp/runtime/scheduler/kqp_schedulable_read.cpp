#include "kqp_schedulable_read.h"

#include "tree/dynamic.h"

#include <yql/essentials/utils/yql_panic.h>
#include <yt/yt/core/utilex/random.h>

namespace NKikimr::NKqp::NScheduler {

TSchedulableRead::TSchedulableRead(const NHdrf::NDynamic::TQueryPtr& query)
    : TSchedulableTask(query)
    // TODO: we add demand to virtual query per reading datashard, which is inconvenient.
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

    YQL_ENSURE(MaxQuotaMs <= 1000);
}

bool TSchedulableRead::TryConsumeQuota(TDuration expectedQuota) {
    // TODO: support update of pool's read quota - on AddOrUpdatePool().
    auto expectedQuotaMs = std::min(expectedQuota.MilliSeconds(), MaxQuotaMs);

    // Refill quota
    if (const auto now = TMonotonic::Now(); Y_LIKELY(now >= LastRefill)) {
        auto elapsedMs = (now - LastRefill).MilliSeconds();
        AvailableQuotaMs = std::min<i64>(MaxQuotaMs, AvailableQuotaMs + (elapsedMs * QuotaPerSecond));
        LastRefill = now;
    }

    if (AvailableQuotaMs <= 0  || !TryIncreaseUsage()) {
        return false;
    }

    AvailableQuotaMs -= expectedQuotaMs;
    ReservedQuotaMs = expectedQuotaMs;

    return true;
}

void TSchedulableRead::ReturnQuota(NHPTimer::STime elapsedCycles) {
    // Makes calls idempotent - useful to handle reading errors and cancellations
    if (ReservedQuotaMs == 0) {
        return;
    }

    static const double msPerCycle = 1000.0 / NHPTimer::GetCyclesPerSecond();

    auto ms = static_cast<ui64>(elapsedCycles * msPerCycle);
    AvailableQuotaMs = std::min<i64>(MaxQuotaMs, AvailableQuotaMs + ReservedQuotaMs - ms);
    ReservedQuotaMs = 0;

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

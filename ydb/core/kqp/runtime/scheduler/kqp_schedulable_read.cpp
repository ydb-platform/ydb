#include "kqp_schedulable_read.h"

#include <ydb/library/actors/core/log.h>
#include "tree/dynamic.h"

#include <yql/essentials/utils/yql_panic.h>
#include <yt/yt/core/utilex/random.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KQP_COMPUTE_SCHEDULER

namespace NKikimr::NKqp::NScheduler {

TSchedulableRead::TSchedulableRead(const NHdrf::NDynamic::TQueryPtr& query, std::shared_ptr<TQuotaState> quota)
    : TSchedulableTask(query)
    , Quota(std::move(quota))
    // TODO: we add demand to virtual query per reading datashard, which is inconvenient.
    //       Because demand persists even if we don't have any pending read requests.
    //       Should actualize demand depending on real requests - in best case scenario: append to real query.
{
    Y_ENSURE(Quota);
    // Refill the retained balance at the previous rate before picking up new limits.
    Quota->Refill();
    if (query->GetParent()->ReadLimit) {
        Quota->MaxQuotaMs = query->GetParent()->ReadLimit->MilliSeconds();
        Quota->QuotaPerSecond = Quota->MaxQuotaMs / 1000.0;
    } else {
        Quota->MaxQuotaMs = 1000;
        Quota->QuotaPerSecond = 1.0;
    }

    Quota->AvailableQuotaMs = std::min<i64>(Quota->MaxQuotaMs, Quota->AvailableQuotaMs);

    YDB_LOG_TRACE("Created schedulable read with quota limit",
        {"schedulableReadPtr", uintptr_t(this)},
        {"maxQuotaMs", Quota->MaxQuotaMs});

    YQL_ENSURE(Quota->MaxQuotaMs <= 1000);
}

bool TSchedulableRead::TryConsumeQuota(TDuration expectedQuota) {
    // TODO: support update of the pool's read quota on AddOrUpdatePool().
    auto expectedQuotaMs = std::min(expectedQuota.MilliSeconds(), Quota->MaxQuotaMs);

    YDB_LOG_TRACE("Trying to consume read quota",
        {"schedulableReadPtr", uintptr_t(this)},
        {"expectedQuotaMs", expectedQuotaMs});

    Quota->Refill();

    YDB_LOG_TRACE("Refilled read quota",
        {"schedulableReadPtr", uintptr_t(this)},
        {"availableQuotaMs", Quota->AvailableQuotaMs});

    if (Quota->AvailableQuotaMs <= 0 || !TryIncreaseUsage()) {
        return false;
    }

    ReserveQuota(expectedQuotaMs);
    return true;
}

void TSchedulableRead::ConsumeQuotaForAdmittedRead(TDuration expectedQuota) {
    Quota->Refill();
    IncreaseUsage();
    ReserveQuota(std::min(expectedQuota.MilliSeconds(), Quota->MaxQuotaMs));
}

void TSchedulableRead::ReserveQuota(ui64 expectedQuotaMs) {
    Y_ENSURE(!ReservedQuotaMs);
    FairShareRetryCount = 0;
    Quota->AvailableQuotaMs -= expectedQuotaMs;
    ReservedQuotaMs = expectedQuotaMs;

    YDB_LOG_TRACE("Reserved read quota",
        {"schedulableReadPtr", uintptr_t(this)},
        {"reservedQuotaMs", ReservedQuotaMs});
}

void TSchedulableRead::ReturnQuota(NHPTimer::STime elapsedCycles) {
    static const double msPerCycle = 1000.0 / NHPTimer::GetCyclesPerSecond();

    Y_ENSURE(ReservedQuotaMs);

    auto ms = static_cast<ui64>(elapsedCycles * msPerCycle);
    Quota->AvailableQuotaMs = std::min<i64>(Quota->MaxQuotaMs, Quota->AvailableQuotaMs + ReservedQuotaMs - ms);
    ReservedQuotaMs = 0;

    YDB_LOG_TRACE("Returned unused read quota",
        {"schedulableReadPtr", uintptr_t(this)},
        {"returnedQuotaMs", ms});
    YDB_LOG_TRACE("Updated available read quota after return",
        {"schedulableReadPtr", uintptr_t(this)},
        {"availableQuotaMs", Quota->AvailableQuotaMs});

    DecreaseUsage(TDuration::MilliSeconds(ms), READ_DEFAULT);
}

bool TSchedulableRead::HasAvailableQuota() {
    Quota->Refill();
    return Quota->AvailableQuotaMs > 0;
}

void TSchedulableRead::TQuotaState::Refill() {
    if (const auto now = TMonotonic::Now(); Y_LIKELY(now >= LastRefill)) {
        constexpr ui64 usPerSecond = TDuration::Seconds(1).MicroSeconds();
        const auto elapsedUs = (now - LastRefill).MicroSeconds();
        // Split whole seconds first to avoid overflowing elapsedUs * MaxQuotaMs.
        const auto fractionalCredit = (elapsedUs % usPerSecond) * MaxQuotaMs + RefillRemainder;
        const auto refillMs = (elapsedUs / usPerSecond) * MaxQuotaMs + fractionalCredit / usPerSecond;
        AvailableQuotaMs = std::min<i64>(MaxQuotaMs, AvailableQuotaMs + static_cast<i64>(refillMs));
        // Once full, discard excess credit instead of carrying it into the next burst.
        RefillRemainder = AvailableQuotaMs == static_cast<i64>(MaxQuotaMs) ? 0 : fractionalCredit % usPerSecond;
        LastRefill = now;
    }
}

TDuration TSchedulableRead::EstimateQuotaDelay(TDuration expectedQuota) const {
    const auto expectedQuotaMs = std::min(expectedQuota.MilliSeconds(), Quota->MaxQuotaMs);

    if (Quota->AvailableQuotaMs >= static_cast<i64>(expectedQuotaMs)) {
        // Quota is available, but TryIncreaseUsage() failed (fair-share exhausted) -
        // retry using exponential delays.
        const auto maxRetries = std::bit_width(expectedQuotaMs) - 1;
        if (FairShareRetryCount >= maxRetries) {
            return expectedQuota + RandomDuration(TDuration::MilliSeconds(expectedQuotaMs >> 2)); // jitter +0..25% of expected quota
        }
        return TDuration::MilliSeconds(1 << FairShareRetryCount++);
    }

    // Quota deficit — calculate refill time
    i64 deficitMs = static_cast<i64>(expectedQuotaMs) - Quota->AvailableQuotaMs;
    ui64 waitMs = static_cast<ui64>(std::ceil(deficitMs / Quota->QuotaPerSecond)); // since QuotaPerSecond is double it's safe to divide by zero
    waitMs = std::max<ui64>(waitMs, 1);

    // TODO: use some meaningful value for max delay - now it's magical delay of 1 minute.
    waitMs = std::min<ui64>(waitMs, TDuration::Minutes(1).MilliSeconds());

    return TDuration::MilliSeconds(waitMs);
}

TSchedulableReadFactory::TSchedulableReadFactory(TComputeSchedulerPtr scheduler)
    : Scheduler(std::move(scheduler))
{
    Y_ENSURE(Scheduler);
}

TSchedulableReadPtr TSchedulableReadFactory::Get(const NHdrf::TDatabaseId& databaseId, const NHdrf::TPoolId& poolId) const {
    const NHdrf::TFullPoolId fullPoolId{databaseId, poolId};

    if (auto readIt = ReadsCache.find(fullPoolId); readIt != ReadsCache.end()) {
        if (auto result = readIt->second.Read.lock()) {
            return result;
        }
    }

    if (auto query = Scheduler->GetReadQuery(databaseId, poolId)) {
        auto& cached = ReadsCache[fullPoolId];
        if (!cached.Quota) {
            cached.Quota = std::make_shared<TSchedulableRead::TQuotaState>();
        }
        auto result = std::make_shared<TSchedulableRead>(query, cached.Quota);
        cached.Read = result;
        return result;
    }

    // Potentially in some cases a datashard may receive read request from new Resource Pool
    // before it's registered in Compute Scheduler, then the read query will be nullptr.
    // Just pass empty Schedulable Read further and handle the problem in datashard.
    return {};
}

void TSchedulableReadFactory::CleanupReadsCache() const {
    std::list<NHdrf::TFullPoolId> toRemove;

    for (const auto& read : ReadsCache) {
        if (read.second.Read.expired()) {
            const auto& quota = read.second.Quota;
            quota->Refill();
            // Dropping a partially spent bucket would give the next reader a fresh burst.
            if (quota->AvailableQuotaMs >= static_cast<i64>(quota->MaxQuotaMs)) {
                toRemove.push_back(read.first);
            }
        }
    }

    for (const auto& key : toRemove) {
        ReadsCache.erase(key);
    }
}

} // namespace NKikimr::NKqp::NScheduler

#include "quota_tracker.h"

#include <util/generic/ymath.h>


namespace NKikimr::NPQ {
    namespace {

    constexpr ui64 MILLISECONDS_PER_SECOND = TDuration::Seconds(1).MilliSeconds();

    i64 ClampToI64(const ui64 value) {
        return value > static_cast<ui64>(Max<i64>())
            ? Max<i64>()
            : static_cast<i64>(value);
    }

    } // namespace

    TQuotaTracker::TQuotaTracker(const ui64 maxBurst, const ui64 speedPerSecond, const TInstant timestamp)
        : AvailableQuota(TransfromToQuota(maxBurst))
        , SpeedPerSecond(speedPerSecond)
        , LastUpdateTime(timestamp)
        , MaxBurst(TransfromToQuota(maxBurst))
    {}

    ui64 TQuotaTracker::TransfromToQuota(const ui64 bytesPerSecond) const {
        ui64 result = 0;
        if (__builtin_mul_overflow(bytesPerSecond, MILLISECONDS_PER_SECOND, &result)) {
            return Max<ui64>();
        }

        return result;
    }

    bool TQuotaTracker::UpdateConfigIfChanged(const ui64 maxBurst, const ui64 speedPerSecond) {
        const ui64 newMaxBurst = TransfromToQuota(maxBurst);
        
        if (newMaxBurst != MaxBurst || speedPerSecond != SpeedPerSecond) {
            SpeedPerSecond = speedPerSecond;
            MaxBurst = newMaxBurst;
            AvailableQuota = newMaxBurst;
            RefillRemainder = TDuration::Zero();
            return true;
        }
        return false;
    }

    void TQuotaTracker::Update(const TInstant timestamp) {
        if (timestamp <= LastUpdateTime) {
            return;
        }

        TDuration diff = timestamp - LastUpdateTime;
        LastUpdateTime = timestamp;

        if (AvailableQuota < 0) {
            QuotedTime += diff;
        }

        const TDuration totalRefillTime = RefillRemainder + diff;
        const ui64 wholeMilliseconds = totalRefillTime.MilliSeconds();
        RefillRemainder = totalRefillTime - TDuration::MilliSeconds(wholeMilliseconds);

        ui64 refill = 0;
        if (__builtin_mul_overflow(SpeedPerSecond, wholeMilliseconds, &refill)) {
            AvailableQuota = ClampToI64(MaxBurst);
            return;
        }
        const i64 refillQuota = ClampToI64(refill);
        i64 updatedAvailableQuota = 0;
        if (__builtin_add_overflow(AvailableQuota, refillQuota, &updatedAvailableQuota)) {
            AvailableQuota = ClampToI64(MaxBurst);
            return;
        }

        AvailableQuota = Min<i64>(updatedAvailableQuota, ClampToI64(MaxBurst));
    }

    bool TQuotaTracker::CanExaust(const TInstant timestamp) {
        Update(timestamp);
        return AvailableQuota >= static_cast<i64>(MILLISECONDS_PER_SECOND);
    }

    void TQuotaTracker::Exaust(const ui64 size, const TInstant timestamp) {
        Update(timestamp);
        const i64 exhaustQuota = ClampToI64(TransfromToQuota(size));
        if (__builtin_sub_overflow(AvailableQuota, exhaustQuota, &AvailableQuota)) {
            AvailableQuota = Min<i64>();
        }
        Update(timestamp);
    }

    TDuration TQuotaTracker::GetQuotedTime(const TInstant timestamp) {
        Update(timestamp);
        return QuotedTime;
    }

    ui64 TQuotaTracker::GetTotalSpeed() const {
        return SpeedPerSecond;
    }

} // NKikimr::NPQ




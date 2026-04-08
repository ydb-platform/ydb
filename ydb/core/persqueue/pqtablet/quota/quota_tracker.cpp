#include "quota_tracker.h"

#include <util/generic/ymath.h>


namespace NKikimr::NPQ {
    namespace {

    i64 ClampToI64(const ui64 value) {
        return value > static_cast<ui64>(Max<i64>())
            ? Max<i64>()
            : static_cast<i64>(value);
    }

    } // namespace

    TQuotaTracker::TQuotaTracker(const ui64 maxBurst, const ui64 speedPerSecond, const TInstant timestamp)
        : AvailableSize(maxBurst)
        , SpeedPerSecond(speedPerSecond)
        , LastUpdateTime(timestamp)
        , MaxBurst(maxBurst)
    {}

    bool TQuotaTracker::UpdateConfigIfChanged(const ui64 maxBurst, const ui64 speedPerSecond) {
        if (maxBurst != MaxBurst || speedPerSecond != SpeedPerSecond) {
            SpeedPerSecond = speedPerSecond;
            MaxBurst = maxBurst;
            AvailableSize = maxBurst;
            return true;
        }
        return false;
    }

    void TQuotaTracker::Update(const TInstant timestamp) {
        if (timestamp < LastUpdateTime) {
            return;
        }

        TDuration diff = timestamp - LastUpdateTime;
        LastUpdateTime = timestamp;

        if (AvailableSize < 0) {
            QuotedTime += diff;
        }

        ui64 refill = 0;
        if (__builtin_mul_overflow(SpeedPerSecond, diff.MicroSeconds(), &refill)) {
            AvailableSize = ClampToI64(MaxBurst);
            return;
        }

        const i64 refillBytes = ClampToI64(refill / 1000'000);
        i64 updatedAvailableSize = 0;
        if (__builtin_add_overflow(AvailableSize, refillBytes, &updatedAvailableSize)) {
            AvailableSize = ClampToI64(MaxBurst);
            return;
        }

        AvailableSize = Min<i64>(updatedAvailableSize, ClampToI64(MaxBurst));
    }

    bool TQuotaTracker::CanExaust(const TInstant timestamp) {
        Update(timestamp);
        return AvailableSize > 0;
    }

    void TQuotaTracker::Exaust(const ui64 size, const TInstant timestamp) {
        Update(timestamp);
        AvailableSize -= (i64)size;
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




#include "quota_tracker.h"

#include <util/generic/ymath.h>


namespace NKikimr::NPQ {
    namespace {

    constexpr ui64 MILLISECONDS_PER_SECOND = 1000;

    i64 ClampToI64(const ui64 value) {
        return value > static_cast<ui64>(Max<i64>())
            ? Max<i64>()
            : static_cast<i64>(value);
    }

    } // namespace

    TQuotaTracker::TQuotaTracker(const ui64 maxBurst, const ui64 speedPerSecond, const TInstant timestamp)
        : AvailableSize(ClampToI64(maxBurst))
        , SpeedPerSecond(speedPerSecond)
        , LastUpdateTime(timestamp)
        , MaxBurst(maxBurst)
    {}

    bool TQuotaTracker::UpdateConfigIfChanged(const ui64 maxBurst, const ui64 speedPerSecond) {
        if (maxBurst == MaxBurst && speedPerSecond == SpeedPerSecond) {
            return false;
        }

        SpeedPerSecond = speedPerSecond;
        MaxBurst = maxBurst;
        AvailableSize = ClampToI64(maxBurst);
        ResidualMilliUnits = 0;
        return true;
    }

    void TQuotaTracker::Update(const TInstant timestamp) {
        if (timestamp <= LastUpdateTime) {
            return;
        }

        TDuration diff = timestamp - LastUpdateTime;
        const ui64 milliseconds = diff.MilliSeconds();
        if (milliseconds == 0) {
            return;
        }

        LastUpdateTime = timestamp;

        if (AvailableSize < 0) {
            QuotedTime += diff;
        }

        // speed * dt / 1s in millisecond units so speed * dt fits in ui64
        // for realistic rates. Keep the leftover so 1 unit/s still accumulates
        // across 50ms wake-ups instead of truncating to zero.
        ui64 product = 0;
        if (__builtin_mul_overflow(SpeedPerSecond, milliseconds, &product) ||
            __builtin_add_overflow(ResidualMilliUnits, product, &product))
        {
            ResidualMilliUnits = 0;
            AvailableSize = ClampToI64(MaxBurst);
            return;
        }

        ResidualMilliUnits = product % MILLISECONDS_PER_SECOND;
        const i64 refill = ClampToI64(product / MILLISECONDS_PER_SECOND);
        i64 updated = 0;
        if (__builtin_add_overflow(AvailableSize, refill, &updated)) {
            ResidualMilliUnits = 0;
            AvailableSize = ClampToI64(MaxBurst);
            return;
        }

        const i64 maxBurst = ClampToI64(MaxBurst);
        if (updated >= maxBurst) {
            ResidualMilliUnits = 0;
            AvailableSize = maxBurst;
        } else {
            AvailableSize = updated;
        }
    }

    bool TQuotaTracker::CanExaust(const TInstant timestamp) {
        Update(timestamp);
        return AvailableSize > 0;
    }

    void TQuotaTracker::Exaust(const ui64 size, const TInstant timestamp) {
        Update(timestamp);
        const i64 exhaust = ClampToI64(size);
        if (__builtin_sub_overflow(AvailableSize, exhaust, &AvailableSize)) {
            AvailableSize = Min<i64>();
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

} // namespace NKikimr::NPQ

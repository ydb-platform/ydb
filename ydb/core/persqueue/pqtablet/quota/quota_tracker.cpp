#include "quota_tracker.h"

#include <util/generic/ymath.h>


namespace NKikimr::NPQ {
    namespace {

    constexpr ui64 MILLISECONDS_PER_SECOND = TDuration::Seconds(1).MilliSeconds();
    constexpr ui64 QUOTA_TICK_MILLISECONDS = TDuration::MilliSeconds(50).MilliSeconds();

    i64 ClampToI64(const ui64 value) {
        return value > static_cast<ui64>(Max<i64>())
            ? Max<i64>()
            : static_cast<i64>(value);
    }

    } // namespace

    TQuotaTracker::TQuotaTracker(const ui64 maxBurst, const ui64 speedPerSecond, const TInstant timestamp)
        : AvailableQuota(0)
        , SpeedPerSecond(speedPerSecond)
        , LastUpdateTime(timestamp)
        , MaxBurst(ComputeMaxBurstQuota(maxBurst, speedPerSecond))
    {
        AvailableQuota = ClampToI64(MaxBurst);
    }

    ui64 TQuotaTracker::TransformToQuota(const ui64 bytes) const {
        ui64 result = 0;
        if (__builtin_mul_overflow(bytes, MILLISECONDS_PER_SECOND, &result)) {
            return Max<ui64>();
        }

        return result;
    }

    ui64 TQuotaTracker::ComputeMaxBurstQuota(const ui64 maxBurst, const ui64 speedPerSecond) const {
        // Available is a token bucket that still refills while traffic is going.
        // Capping it at TransformToQuota(burst) therefore dumps ~burst immediately and
        // another ~speed over the next second (2× when burst == speed).
        //
        // Want ~burst bytes over any busy second:
        //   extra = max(0, burst - speed)  — idle dump above the sustained rate
        //   tick  = speed * 50ms           — one WAKE_UP quantum so the limiter can start;
        //                                     20 ticks/s then refill the rest of `speed`
        // burst == speed → cap is one tick (~1.05×, not 2×)
        // burst >  speed → idle dump is the extra, then refill speed
        // burst <  speed → extra is 0, cap is one tick
        ui64 extra = 0;
        if (maxBurst > speedPerSecond) {
            extra = TransformToQuota(maxBurst - speedPerSecond);
        }

        ui64 tick = 0;
        if (__builtin_mul_overflow(speedPerSecond, QUOTA_TICK_MILLISECONDS, &tick)) {
            tick = Max<ui64>();
        }

        ui64 result = 0;
        if (__builtin_add_overflow(extra, tick, &result)) {
            result = Max<ui64>();
        }

        // CanExaust requires at least one full unit; keep the limiter usable at low speed.
        if (speedPerSecond > 0 && result < MILLISECONDS_PER_SECOND) {
            result = MILLISECONDS_PER_SECOND;
        }

        return result;
    }

    bool TQuotaTracker::UpdateConfigIfChanged(const ui64 maxBurst, const ui64 speedPerSecond, const TInstant timestamp) {
        const ui64 newMaxBurst = ComputeMaxBurstQuota(maxBurst, speedPerSecond);

        if (newMaxBurst == MaxBurst && speedPerSecond == SpeedPerSecond) {
            return false;
        }

        Update(timestamp);
        SpeedPerSecond = speedPerSecond;
        MaxBurst = newMaxBurst;
        AvailableQuota = Min<i64>(AvailableQuota, ClampToI64(MaxBurst));
        return true;
    }

    void TQuotaTracker::Update(const TInstant timestamp) {
        if (timestamp.MilliSeconds() <= LastUpdateTime.MilliSeconds()) {
            return;
        }

        TDuration diff = timestamp - LastUpdateTime;
        LastUpdateTime = timestamp;

        if (AvailableQuota < static_cast<i64>(MILLISECONDS_PER_SECOND)) {
            QuotedTime += diff;
        }

        ui64 refill = 0;
        if (__builtin_mul_overflow(SpeedPerSecond, diff.MilliSeconds(), &refill)) {
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
        const i64 exhaustQuota = ClampToI64(TransformToQuota(size));
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

} // namespace NKikimr::NPQ

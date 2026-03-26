#include "quota_tracker.h"


namespace NKikimr::NPQ {
    namespace {
        constexpr size_t MicroSecondsPerSecond = 1000000;
    }

    TQuotaTracker::TQuotaTracker(const ui64 maxBurst, const ui64 speedPerSecond, const TInstant timestamp)
        : AvailableSize(maxBurst * MicroSecondsPerSecond)
        , QuotaSpeed(speedPerSecond)
        , MaxBurst(maxBurst * MicroSecondsPerSecond)
        , LastUpdateTime(timestamp)
    {}

    bool TQuotaTracker::UpdateConfigIfChanged(const ui64 maxBurst, const ui64 speedPerSecond) {
        const auto newMaxBurst = maxBurst * MicroSecondsPerSecond;
        const auto newQuotaSpeed = speedPerSecond;
        if (newMaxBurst != MaxBurst || newQuotaSpeed != QuotaSpeed) {
            AvailableSize = newMaxBurst;
            QuotaSpeed = newQuotaSpeed;
            MaxBurst = newMaxBurst;
            return true;
        }
        return false;
    }

    void TQuotaTracker::Update(const TInstant timestamp) {
        TDuration diff = timestamp - LastUpdateTime;
        LastUpdateTime = timestamp;

        if (AvailableSize < 0) {
            QuotedTime += diff;
        }

        AvailableSize = Min<i64>(AvailableSize + static_cast<i64>(QuotaSpeed) * static_cast<i64>(diff.MicroSeconds()), MaxBurst);
    }

    bool TQuotaTracker::CanExaust(const TInstant timestamp) {
        Update(timestamp);
        return AvailableSize >= (i64)MicroSecondsPerSecond; // a whole quota unit has become available
    }

    void TQuotaTracker::Exaust(const ui64 size, const TInstant timestamp) {
        Update(timestamp);
        AvailableSize -= (i64)size * MicroSecondsPerSecond;
        Update(timestamp);
    }

    TDuration TQuotaTracker::GetQuotedTime(const TInstant timestamp) {
        Update(timestamp);
        return QuotedTime;
    }

    ui64 TQuotaTracker::GetTotalSpeed() const {
        return QuotaSpeed;
    }

} // NKikimr::NPQ




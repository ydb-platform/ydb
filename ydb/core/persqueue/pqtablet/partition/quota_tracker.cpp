#include "quota_tracker.h"


namespace NKikimr::NPQ {
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
        TDuration diff = timestamp - LastUpdateTime;
        LastUpdateTime = timestamp;

        if (AvailableSize < 0) {
            QuotedTime += diff;
        }

        AvailableSize = Min<i64>(AvailableSize + (ui64)SpeedPerSecond * diff.MicroSeconds() / 1000'000, MaxBurst);
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




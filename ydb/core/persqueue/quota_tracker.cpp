#include "quota_tracker.h"


namespace NKikimr::NPQ {
    TQuotaTracker::TQuotaTracker(const ui64 maxBurst, const ui64 speedPerSecond, const TInstant timestamp)
        : AvailableSize(maxBurst)
        , SpeedPerSecond(speedPerSecond)
        , LastUpdateTime(timestamp)
        , MaxBurst(maxBurst)
        , QuotedTime(0)
    {}

    void TQuotaTracker::UpdateConfig(const ui64 maxBurst, const ui64 speedPerSecond) {
        SpeedPerSecond = speedPerSecond;
        MaxBurst = maxBurst;
        AvailableSize = maxBurst;
    }

    void TQuotaTracker::Update(const TInstant timestamp) {
        ui64 ms = (timestamp - LastUpdateTime).MilliSeconds();
        LastUpdateTime = timestamp;

        if (AvailableSize < 0) {
            QuotedTime += ms;
        }

        AvailableSize = Min<i64>(AvailableSize + (ui64)SpeedPerSecond * ms / 1000, MaxBurst);
    }

    bool TQuotaTracker::CanExaust() const {
        return AvailableSize > 0;
    }

    void TQuotaTracker::Exaust(const ui64 size, const TInstant timestamp) {
        Update(timestamp);
        AvailableSize -= (i64)size;
        Update(timestamp);
    }

    ui64 TQuotaTracker::GetQuotedTime() const {
        return QuotedTime;
    }

    ui64 TQuotaTracker::GetTotalSpeed() const {
        return SpeedPerSecond;
    }

} // NKikimr::NPQ




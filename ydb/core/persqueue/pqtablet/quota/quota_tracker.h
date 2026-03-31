#pragma once

#include <util/datetime/base.h>


namespace NKikimr::NPQ {
    class TQuotaTracker {
    public:
        TQuotaTracker(const ui64 maxBurst, const ui64 speedPerSecond, const TInstant timestamp);

        bool UpdateConfigIfChanged(const ui64 maxBurst, const ui64 speedPerSecond);
        void Update(const TInstant timestamp);

        bool CanExaust(const TInstant timestamp) ;
        void Exaust(const ui64 size, const TInstant timestamp);

        TDuration GetQuotedTime(const TInstant timestamp);
        ui64 GetTotalSpeed() const;

    private:
        i64 AvailableSize;
        ui64 QuotaSpeed;
        ui64 MaxBurst;

        TInstant LastUpdateTime;
        TDuration QuotedTime;
    };

} // NKikimr::NPQ

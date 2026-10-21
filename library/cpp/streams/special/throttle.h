#pragma once

#include <util/datetime/base.h>

class TThrottle {
public:
    struct TOptions {
        ui64 MaxUnitsPerInterval;
        TDuration SamplingInterval;

        static TOptions FromMaxPerSecond(ui64 maxUnitsPerSecond, TDuration samplingInterval);
        static TOptions FromMaxPerInterval(ui64 maxUnitsPerInterval, TDuration samplingInterval);

        void Validate() const;

        ui64 GetUnitsPerSecond() const;

        bool operator==(const TOptions& a) const {
            return (MaxUnitsPerInterval == a.MaxUnitsPerInterval) && (SamplingInterval == a.SamplingInterval);
        }

        bool operator!=(const TOptions& a) const {
            return !(*this == a);
        }
    };

public:
    TThrottle(TOptions options);

    // Return value is always in the [1, maxQuotaNeeded] closed interval.
    // Sleeps only if remaining quota is zero, otherwise returns an incomplete chunk.
    [[nodiscard]] ui64 GetQuota(ui64 maxQuotaNeeded);

private:
    void Refill();
    void Reset();

private:
    const TOptions Options;
    TInstant LastRefillTime;
    ui64 QuotaRemaining;
};

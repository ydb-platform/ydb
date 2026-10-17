#include "throttle.h"

TThrottle::TOptions TThrottle::TOptions::FromMaxPerSecond(ui64 maxUnitsPerSecond, TDuration samplingInterval) {
    return FromMaxPerInterval(maxUnitsPerSecond * samplingInterval.MicroSeconds() / 1000000, samplingInterval);
}

TThrottle::TOptions TThrottle::TOptions::FromMaxPerInterval(ui64 maxUnitsPerInterval, TDuration samplingInterval) {
    return {maxUnitsPerInterval, samplingInterval};
}

void TThrottle::TOptions::Validate() const {
    Y_ENSURE(SamplingInterval > TDuration::Zero());
    Y_ENSURE(MaxUnitsPerInterval > 0);
}

ui64 TThrottle::TOptions::GetUnitsPerSecond() const {
    return (MaxUnitsPerInterval * 1000000) / SamplingInterval.MicroSeconds();
}

TThrottle::TThrottle(TOptions options)
    : Options(options)
{
    Options.Validate();
    Reset();
}

ui64 TThrottle::GetQuota(const ui64 maxQuotaNeeded) {
    if (QuotaRemaining == 0) {
        Refill();
    }
    Y_ASSERT(QuotaRemaining > 0);

    ui64 result = Min<ui64>(QuotaRemaining, maxQuotaNeeded);
    QuotaRemaining -= result;
    Y_ASSERT(QuotaRemaining <= Options.MaxUnitsPerInterval);

    Y_ASSERT(result > 0);
    Y_ASSERT(result <= maxQuotaNeeded);
    return result;
}

void TThrottle::Refill() {
    Y_ASSERT(QuotaRemaining == 0);

    TDuration delta = Now() - LastRefillTime;
    if (delta < Options.SamplingInterval) {
        Sleep(Options.SamplingInterval - delta);
    }

    Reset();
}

void TThrottle::Reset() {
    LastRefillTime = Now();
    QuotaRemaining = Options.MaxUnitsPerInterval;
}

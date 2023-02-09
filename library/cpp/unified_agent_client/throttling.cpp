#include "throttling.h"

#include <util/datetime/cputimer.h>

namespace NUnifiedAgent {
    TThrottler::TThrottler(double rate, TDuration updatePeriod)
        : CyclesPerMillisecond(GetCyclesPerMillisecond())
        , UpdatePeriod(updatePeriod.MilliSeconds() * CyclesPerMillisecond)
        , PeriodTokens(updatePeriod.SecondsFloat() * rate)
        , AvailableTokens(0)
        , ExpirationTime(0)
    {
    }

    TThrottler::TThrottler(double rate, double burst)
        : TThrottler(rate, TDuration::Seconds(burst / rate))
    {
    }

    void TThrottler::Consume(double& tokens, TFMaybe<TDuration>& nextCheckDelay) {
        const auto updateTime = UpdateTokens();

        if (tokens <= AvailableTokens) {
            AvailableTokens -= tokens;
            tokens = 0.0;
            nextCheckDelay = Nothing();
        } else {
            tokens -= AvailableTokens;
            AvailableTokens = 0.0;
            nextCheckDelay = TDuration::MicroSeconds((ExpirationTime - updateTime) * 1000 / CyclesPerMillisecond + 1);
        }
    }

    bool TThrottler::TryConsume(double tokens) {
        UpdateTokens();

        if (tokens >  AvailableTokens) {
            return false;
        }
        AvailableTokens -= tokens;
        return true;
    }

    void TThrottler::ConsumeAndWait(double tokens) {
        TFMaybe<TDuration> nextCheckDelay;
        while (true) {
            Consume(tokens, nextCheckDelay);
            if (!nextCheckDelay.Defined()) {
                return;
            }
            Sleep(*nextCheckDelay);
        }
    }

    ui64 TThrottler::UpdateTokens() {
        const auto updateTime = GetCycleCount();
        if (updateTime >= ExpirationTime) {
            if (ExpirationTime == 0) {
                ExpirationTime = updateTime + UpdatePeriod;
            } else {
                ExpirationTime += ((updateTime - ExpirationTime) / UpdatePeriod + 1) * UpdatePeriod;
            }
            AvailableTokens = PeriodTokens;
        }
        return updateTime;
    }
}

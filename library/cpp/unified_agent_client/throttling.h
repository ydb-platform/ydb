#pragma once

#include <library/cpp/unified_agent_client/f_maybe.h>

#include <util/datetime/base.h>

namespace NUnifiedAgent {
    // Comment from a non-author:
    // It is based on something similar to https://en.wikipedia.org/wiki/Token_bucket
    class TThrottler {
    public:
        explicit TThrottler(double rate, TDuration updatePeriod = TDuration::MilliSeconds(100));

        TThrottler(double rate, double burst);

        void Consume(double& tokens, TFMaybe<TDuration>& nextCheckDelay);

        bool TryConsume(double tokens);

        void ConsumeAndWait(double tokens);

    private:
        ui64 UpdateTokens();

    private:
        ui64 CyclesPerMillisecond;
        ui64 UpdatePeriod;
        double PeriodTokens;
        double AvailableTokens;
        ui64 ExpirationTime;
    };
}

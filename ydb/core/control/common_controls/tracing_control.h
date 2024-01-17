#pragma once

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/control/immediate_control_board_sampler.h>
#include <ydb/core/control/immediate_control_board_throttler.h>

namespace NKikimr {

class TTracingControl {
public:
    TTracingControl(TIntrusivePtr<TControlBoard>& icb, TIntrusivePtr<ITimeProvider> timeProvider,
            TIntrusivePtr<IRandomProvider>& randomProvider, TString controlDomain);

    void SampleThrottle(NWilson::TTraceId& traceId) {
        if (!traceId && Sampler.Sample()) {
            traceId = NWilson::TTraceId::NewTraceId(15, 4095);
        }
        if (traceId && Throttler.Throttle()) {
            traceId = {};
        }
    }

private:
    TControlWrapper SamplingPPM;
    TControlWrapper MaxRatePerMinute;
    TControlWrapper MaxBurst;

    TSampler Sampler;
    TThrottler Throttler;
};

} // namespace NKikimr

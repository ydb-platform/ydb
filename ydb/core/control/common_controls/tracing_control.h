#pragma once

#include <ydb/core/control/immediate_control_board_impl.h>
#include <ydb/core/control/immediate_control_board_sampler.h>
#include <ydb/core/control/immediate_control_board_throttler.h>

namespace NKikimr {

class TTracingControl {
public:
    TTracingControl(TIntrusivePtr<TControlBoard>& icb, TString controlDomain)
        : Sampler(SamplingPPM)
        , Throttler(MaxRatePerMinute, MaxBurst)
    {
        icb->RegisterSharedControl(SamplingPPM, controlDomain + ".SamplingPPM");
        icb->RegisterSharedControl(MaxRatePerMinute, controlDomain + ".MaxRatePerMinute");
        icb->RegisterSharedControl(MaxBurst, controlDomain + ".MaxBurst");
    }

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

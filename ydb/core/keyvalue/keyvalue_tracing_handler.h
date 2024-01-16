#pragma once

#include <ydb/core/control/immediate_control_board_impl.h>
#include <ydb/core/control/immediate_control_board_sampler.h>
#include <ydb/core/control/immediate_control_board_throttler.h>

namespace NKikimr {
namespace NKeyValue {

class TKeyValueTracingHandler {
public:
    TKeyValueTracingHandler(TIntrusivePtr<TControlBoard>& icb)
        : Sampler(SamplingPPM)
        , Throttler()
    {
        icb->RegisterSharedControl(SamplingPPM, "TracingControls.KeyValue.SamplingPPM");
    }

    template<class TReq>
    void Handle(TReq::TPtr& ev) {
        if (!ev->TraceId && Sampler.Sample()) {
            ev->TraceId = NWilson::TTraceId::NewTraceId(15, 4095);
        }
        if (ev->TraceId && Throttler.Throttle()) {
            ev->TraceId = {};
        }
    }

private:
    TControlWrapper SamplingPPM;

    TSampler Sampler;
    TThrottler Throttler;
};

} // namespace NKeyValue
} // namespace NKikimr

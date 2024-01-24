#pragma once

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/control/immediate_control_board_sampler.h>
#include <ydb/core/control/immediate_control_board_throttler.h>

namespace NKikimr {

class TTracingControl {
public:
    TTracingControl(TIntrusivePtr<TControlBoard>& icb, TIntrusivePtr<ITimeProvider> timeProvider,
            TIntrusivePtr<IRandomProvider>& randomProvider, TString controlDomain);

    bool SampleThrottle() {
        return Sampler.Sample() && !Throttler.Throttle();
    }

    bool ThrottleExternal() {
        return ExternalThrottler.Throttle();
    }

    ui8 SampledVerbosity() const {
        Y_ABORT("TODO");
    }

private:
    TSampler Sampler;
    TThrottler Throttler;
    TThrottler ExternalThrottler;
};

} // namespace NKikimr

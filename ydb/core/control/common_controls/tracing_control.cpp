#include "tracing_control.h"

#include <ydb/core/control/immediate_control_board_impl.h>
#include <library/cpp/random_provider/random_provider.h>

namespace NKikimr {

TTracingControl::TTracingControl(TIntrusivePtr<TControlBoard>& icb, TIntrusivePtr<ITimeProvider> timeProvider,
        TIntrusivePtr<IRandomProvider>& randomProvider, TString controlDomain)
    : Sampler(SamplingPPM, randomProvider->GenRand64())
    , Throttler(MaxRatePerMinute, MaxBurst, std::move(timeProvider))
{
    icb->RegisterSharedControl(SamplingPPM, controlDomain + ".SamplingPPM");
    icb->RegisterSharedControl(MaxRatePerMinute, controlDomain + ".MaxRatePerMinute");
    icb->RegisterSharedControl(MaxBurst, controlDomain + ".MaxBurst");
}

} // namespace NKikimr

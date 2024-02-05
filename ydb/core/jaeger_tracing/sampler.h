#pragma once

#include "control_wrapper.h"

#include <util/random/fast.h>

namespace NKikimr::NJaegerTracing {

class TSampler {
public:
    TSampler(TControlWrapper samplingPPM, ui64 seed)
        : SamplingPPM(std::move(samplingPPM))
        , Rng(seed)
    {}

    bool Sample() {
        return Rng() % 1'000'000 < SamplingPPM.Get();
    }

private:
    TControlWrapper SamplingPPM;
    TReallyFastRng32 Rng;
};

} // namespace NKikimr::NJaegerTracing

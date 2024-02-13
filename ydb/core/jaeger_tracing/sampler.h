#pragma once

#include <util/random/fast.h>

namespace NKikimr::NJaegerTracing {

class TSampler {
public:
    TSampler(ui64 samplingPPB, ui64 seed)
        : SamplingPPB(samplingPPB)
        , Rng(seed)
    {}

    bool Sample() {
        return Rng() % 1'000'000'000 < SamplingPPB;
    }

private:
    const ui64 SamplingPPB;
    TFastRng64 Rng;
};

} // namespace NKikimr::NJaegerTracing

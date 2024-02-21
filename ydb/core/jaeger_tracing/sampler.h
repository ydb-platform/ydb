#pragma once

#include <util/random/fast.h>

namespace NKikimr::NJaegerTracing {

class TSampler {
public:
    TSampler(double fraction, ui64 seed)
        : SamplingFraction(fraction)
        , Rng(seed)
    {}

    bool Sample() {
        return Rng.GenRandReal1() < SamplingFraction;
    }

private:
    const double SamplingFraction;
    TFastRng64 Rng;
};

} // namespace NKikimr::NJaegerTracing

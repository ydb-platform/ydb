#pragma once

#include <util/random/fast.h>

namespace NKikimr::NJaegerTracing {

class TSampler {
public:
    static constexpr ui64 kParts = 1'000'000'000;

    TSampler(double fraction, ui64 seed)
        : SamplingPPB(fraction * kParts)
        , Rng(seed)
    {}

    bool Sample() {
        return Rng() % kParts < SamplingPPB;
    }

private:
    const ui64 SamplingPPB;
    TFastRng64 Rng;
};

} // namespace NKikimr::NJaegerTracing

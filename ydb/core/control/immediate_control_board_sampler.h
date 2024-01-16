#pragma once

#include <ydb/core/control/immediate_control_board_wrapper.h>

namespace NKikimr {

class TSampler {
public:
    TSampler(TControlWrapper& samplingPPM);

    bool Sample() {
        return Rng() % 1'000'000 < SamplingPPM;
    }

private:
    TControlWrapper& SamplingPPM;
    TReallyFastRng32 Rng;
};

} // namespace NKikimr

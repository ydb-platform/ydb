#include "immediate_control_board_sampler.h"

#include <ydb/core/base/appdata_fwd.h>
#include <library/cpp/random_provider/random_provider.h>

namespace NKikimr {

TSampler::TSampler(TControlWrapper& samplingPPM)
    : SamplingPPM(samplingPPM)
    , Rng(TAppData::RandomProvider->GenRand())
{}

}

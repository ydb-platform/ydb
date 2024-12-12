#include "immediate_control_defaults.h"

namespace NKikimr {

TControlWrapper SlowDiskThresholdDefaultControl =
        TControlWrapper(std::round(DefaultSlowDiskThreshold * 1000), 1, 1'000'000);

TControlWrapper PredictedDelayMultiplierDefaultControl =
        TControlWrapper(std::round(DefaultPredictedDelayMultiplier * 1000), 0, 1'000'000);

TControlWrapper MaxNumOfSlowDisksDefaultControl =
        TControlWrapper(DefaultMaxNumOfSlowDisks, 1, 2);

TControlWrapper MaxNumOfSlowDisksHDDDefaultControl =
        TControlWrapper(DefaultMaxNumOfSlowDisksHDD, 1, 2);

TControlWrapper LongRequestThresholdDefaultControl =
        TControlWrapper(DefaultLongRequestThreshold.MilliSeconds(), 1, 1'000'000);

} // namespace NKikimr

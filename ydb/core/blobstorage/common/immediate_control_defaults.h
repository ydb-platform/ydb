#pragma once

#include "defs.h"
#include <ydb/core/control/immediate_control_board_wrapper.h>

namespace NKikimr {

constexpr bool DefaultEnablePutBatching = true;
constexpr bool DefaultEnableVPatch = false;

constexpr float DefaultSlowDiskThreshold = 2;
constexpr float DefaultPredictedDelayMultiplier = 1;
constexpr TDuration DefaultLongRequestThreshold = TDuration::Seconds(50);
constexpr ui32 DefaultMaxNumOfSlowDisks = 2;
constexpr ui32 DefaultMaxNumOfSlowDisksHDD = 1;

extern TControlWrapper SlowDiskThresholdDefaultControl;
extern TControlWrapper PredictedDelayMultiplierDefaultControl;
extern TControlWrapper MaxNumOfSlowDisksDefaultControl;
extern TControlWrapper MaxNumOfSlowDisksHDDDefaultControl;
extern TControlWrapper LongRequestThresholdDefaultControl;
}

#pragma once

#include "defs.h"
#include <util/datetime/base.h>

namespace NKikimr {

constexpr bool DefaultEnablePutBatching = true;
constexpr bool DefaultEnableVPatch = false;

constexpr double DefaultSlowDiskThreshold = 2;
constexpr double DefaultPredictedDelayMultiplier = 1;
constexpr TDuration DefaultLongRequestThreshold = TDuration::Seconds(50);
constexpr ui32 DefaultMaxNumOfSlowDisks = 2;

}

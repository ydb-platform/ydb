#pragma once

#include "defs.h"
#include <ydb/core/control/lib/immediate_control_board_wrapper.h>

namespace NKikimr {

constexpr bool DefaultEnablePutBatching = true;
constexpr bool DefaultEnableVPatch = false;

constexpr float DefaultSlowDiskThreshold = 2;
constexpr float DefaultPredictedDelayMultiplier = 1;
constexpr TDuration DefaultLongRequestThreshold = TDuration::Seconds(50);
constexpr ui32 DefaultMaxNumOfSlowDisks = 2;
constexpr TDuration DefaultMaxPutTimeout = TDuration::Seconds(60);
constexpr TDuration DefaultDormantTimeout = TDuration::Zero();

extern TControlWrapper SlowDiskThresholdDefaultControl;
extern TControlWrapper PredictedDelayMultiplierDefaultControl;
extern TControlWrapper MaxNumOfSlowDisksDefaultControl;
extern TControlWrapper LongRequestThresholdDefaultControl;
extern TControlWrapper MaxPutTimeoutDefaultControl;
<<<<<<< HEAD
=======
extern TControlWrapper DormantTimeoutDefaultControl;
extern TControlWrapper EnableStorageRetroTraceGenerationDefaultControl;
extern TControlWrapper EnableStorageRetroTraceCollectionSlowRequestsDefaultControl;
extern TControlWrapper EnableChecksumCalcAndValidationOnDsProxyDefaultControl;
>>>>>>> 906d13d9f76 (Implement DSProxy dormant state (#52222))

}

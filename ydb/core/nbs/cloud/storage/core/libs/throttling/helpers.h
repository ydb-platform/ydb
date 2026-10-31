#pragma once

#include <util/datetime/base.h>
#include <util/system/types.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

/*
 *  The following two funcs recalculate maxIops and maxBandwidth so that
 *  we get the aforementioned maxIops IOPS and maxBandwidth bytes/sec
 *  for requestSize=4KiB and requestSize=4MiB respectively
 */

ui64 CalculateThrottlerC1(double maxIops, double maxBandwidth);
ui64 CalculateThrottlerC2(double maxIops, double maxBandwidth);

TDuration SecondsToDuration(double seconds);
TDuration CostPerIO(ui64 maxIops, ui64 maxBandwidth, ui64 byteCount);

}   // namespace NYdb::NBS

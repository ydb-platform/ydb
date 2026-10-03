#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/size_interval.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/timer.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/histogram_counter_options.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/request_counters.h>

namespace NCloud::NBlockStore {

using NYdb::NBS::EHistogramCounterOptions;
using NYdb::NBS::ITimerPtr;
using NYdb::NBS::TRequestCounters;
using NYdb::NBS::TSizeInterval;

////////////////////////////////////////////////////////////////////////////////

TRequestCounters MakeRequestCounters(
    ITimerPtr timer,
    TRequestCounters::EOptions options,
    EHistogramCounterOptions histogramCounterOptions,
    const TVector<TSizeInterval>& executionTimeSizeClasses);

}   // namespace NCloud::NBlockStore

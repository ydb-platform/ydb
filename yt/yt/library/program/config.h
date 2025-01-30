#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/misc/configurable_singleton_def.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class THeapProfilerConfig
    : public NYTree::TYsonStruct
{
public:
    // Sampling rate for tcmalloc in bytes.
    // See https://github.com/google/tcmalloc/blob/master/docs/sampling.md
    std::optional<i64> SamplingRate;

    // Period of update snapshot in heap profiler.
    std::optional<TDuration> SnapshotUpdatePeriod;

    REGISTER_YSON_STRUCT(THeapProfilerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THeapProfilerConfig)

////////////////////////////////////////////////////////////////////////////////

// NB: These functions should not be called from bootstrap
// config validator since logger is not set up yet.
void WarnForUnrecognizedOptions(
    const NLogging::TLogger& logger,
    const NYTree::TYsonStructPtr& config);

void AbortOnUnrecognizedOptions(
    const NLogging::TLogger& logger,
    const NYTree::TYsonStructPtr& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

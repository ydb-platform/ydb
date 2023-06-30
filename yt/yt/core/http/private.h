#pragma once

#include "http.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NHttp {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger HttpLogger("Http");
inline const NProfiling::TProfiler HttpProfiler = NProfiling::TProfiler{"/http"}.WithHot();

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(THttpInput)
DECLARE_REFCOUNTED_CLASS(THttpOutput)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp

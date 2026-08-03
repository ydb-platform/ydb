#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/misc/global.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, JaegerLogger, "Jaeger");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing

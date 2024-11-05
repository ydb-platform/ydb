#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NDns {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, DnsLogger, "Dns");
inline const NProfiling::TProfiler DnsProfiler("/dns");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDns

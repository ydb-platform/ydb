#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger AuthLogger("Auth");
inline const NProfiling::TProfiler AuthProfiler("/auth");

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf OAuthCookieRealm = "oauth:cookie";
constexpr TStringBuf OAuthTokenRealm = "oauth:token";

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth


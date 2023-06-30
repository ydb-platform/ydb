#pragma once

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger RpcServerLogger("RpcServer");
inline const NLogging::TLogger RpcClientLogger("RpcClient");

inline const NProfiling::TProfiler RpcServerProfiler("/rpc/server");
inline const NProfiling::TProfiler RpcClientProfiler("/rpc/client");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

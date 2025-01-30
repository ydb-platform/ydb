#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, BusLogger, "Bus");
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, BusProfiler, "/bus");

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IMessageHandler)

using TConnectionId = TGuid;
using TPacketId = TGuid;

DECLARE_REFCOUNTED_CLASS(TTcpConnection)

DEFINE_ENUM(EConnectionType,
    (Client)
    (Server)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus


#pragma once

#include <yt/yt/core/misc/public.h>

#include <library/cpp/yt/misc/typeid.h>

namespace NYT::NCoreDump {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ICoreDumper)
YT_DECLARE_TYPEID(ICoreDumper)

DECLARE_REFCOUNTED_STRUCT(TCoreDumperConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCoreDump

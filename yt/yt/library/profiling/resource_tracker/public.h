#pragma once

#include <yt/yt/core/misc/configurable_singleton_decl.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TResourceTrackerConfig)

YT_DECLARE_CONFIGURABLE_SINGLETON(TResourceTrackerConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling

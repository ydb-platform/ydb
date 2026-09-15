#pragma once

#include <yt/yt/core/misc/configurable_singleton_decl.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TBaggageManager)

DECLARE_REFCOUNTED_STRUCT(TBaggageManagerDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TBaggageManagerConfig)

YT_DECLARE_RECONFIGURABLE_SINGLETON(TBaggageManagerConfig, TBaggageManagerDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing

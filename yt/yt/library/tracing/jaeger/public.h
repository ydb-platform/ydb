#pragma once

#include <yt/yt/core/misc/configurable_singleton_decl.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSampler)
DECLARE_REFCOUNTED_STRUCT(TSamplerConfig)

DECLARE_REFCOUNTED_STRUCT(TJaegerTracerDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TJaegerTracerConfig)

YT_DECLARE_RECONFIGURABLE_SINGLETON(TJaegerTracerConfig, TJaegerTracerDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing

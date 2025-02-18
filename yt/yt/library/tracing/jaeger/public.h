#pragma once

#include <yt/yt/core/misc/configurable_singleton_decl.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSampler)
DECLARE_REFCOUNTED_CLASS(TSamplerConfig)

DECLARE_REFCOUNTED_CLASS(TJaegerTracerDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TJaegerTracerConfig)

YT_DECLARE_RECONFIGURABLE_SINGLETON(TJaegerTracerConfig, TJaegerTracerDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing

#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSampler)
DECLARE_REFCOUNTED_CLASS(TSamplerConfig)

DECLARE_REFCOUNTED_CLASS(TJaegerTracerDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TJaegerTracerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing

#pragma once

#include "public.h"

#include <yt/yt/core/misc/ref_counted.h>

#include <yt/yt/core/tracing/public.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

struct ITracer
    : public TRefCounted
{
    virtual void Enqueue(TTraceContextPtr trace) = 0;
    virtual void Stop() = 0;
};

DEFINE_REFCOUNTED_TYPE(ITracer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing

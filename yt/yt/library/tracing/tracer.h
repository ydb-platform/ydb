#pragma once

#include "public.h"

#include <yt/yt/core/tracing/public.h>

#include <library/cpp/yt/memory/ref_counted.h>

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

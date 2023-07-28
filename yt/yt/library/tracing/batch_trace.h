#pragma once

#include "public.h"

#include <yt/yt/core/tracing/trace_context.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

//! Batch trace propagates tracing through request batching.
/**
 *  TBatchTrace is not thread safe.
 */
class TBatchTrace
{
public:
    void Join();

    void Join(const TTraceContextPtr& context);

    std::pair<TTraceContextPtr, bool> StartSpan(const TString& spanName);

private:
    std::vector<TTraceContextPtr> Clients_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing

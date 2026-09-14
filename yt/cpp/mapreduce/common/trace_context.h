#pragma once

#include <yt/cpp/mapreduce/interface/fwd.h>

#include <yt/yt/core/tracing/trace_context.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

/// This wrapper is required because NYT::NTracing::TTraceContextPtr is NYT::TIntrusivePtr,
/// and, if we used this pointer in interfaces of `mapreduce/yt` client, a lot of users of this library
/// could get unexpected build errors that `TIntrusivePtr` is ambiguous
/// (from `::` namespace and from `::NYT::` namespace).
/// So we use this wrapper in our interfaces to avoid such problems for users.
struct TTraceContextWrapper
{
    ///
    /// Construct wrapper from NYT::NTracing::TTraceContextPtr (NYT::TIntrusivePtr)
    ///
    /// This constructor is implicit so users can transparently pass NYT::TIntrusivePtr to the functions of
    /// mapreduce/yt client.
    TTraceContextWrapper(const TTraceContextPtr& ptr)
        : Ptr(ptr)
    { }

    /// Wrapped pointer
    TTraceContextPtr Ptr;
};

using TTraceContextWrapperPtr = std::shared_ptr<TTraceContextWrapper>;

TTraceContextWrapperPtr CreateTraceContext(const std::string& name, const TConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing

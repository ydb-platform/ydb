#ifndef TRACE_CONTEXT_INL_H_
#error "Direct inclusion of this file is not allowed, include trace_context.h"
// For the sake of sane code completion.
#include "trace_context.h"
#endif

#include <atomic>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE bool TTraceContext::IsRecorded() const
{
    auto state = State_.load(std::memory_order::relaxed);
    return state == ETraceContextState::Recorded || state == ETraceContextState::Sampled;
}

Y_FORCE_INLINE bool TTraceContext::IsPropagated() const
{
    return Propagated_;
}

Y_FORCE_INLINE bool TTraceContext::IsDebug() const
{
    return Debug_;
}

Y_FORCE_INLINE TTraceId TTraceContext::GetTraceId() const
{
    return TraceId_;
}

Y_FORCE_INLINE TSpanId TTraceContext::GetSpanId() const
{
    return SpanId_;
}

Y_FORCE_INLINE TSpanId TTraceContext::GetParentSpanId() const
{
    return ParentSpanId_;
}

Y_FORCE_INLINE TRequestId TTraceContext::GetRequestId() const
{
    return RequestId_;
}

Y_FORCE_INLINE const TString& TTraceContext::GetSpanName() const
{
    return SpanName_;
}

Y_FORCE_INLINE const TString& TTraceContext::GetLoggingTag() const
{
    return LoggingTag_;
}

Y_FORCE_INLINE const std::optional<TString>& TTraceContext::GetTargetEndpoint() const
{
    return TargetEndpoint_;
}

Y_FORCE_INLINE NProfiling::TCpuDuration TTraceContext::GetElapsedCpuTime() const
{
    return ElapsedCpuTime_.load(std::memory_order::relaxed);
}

template <class T>
void TTraceContext::AddTag(const TString& tagName, const T& tagValue)
{
    if (!IsRecorded()) {
        return;
    }

    using ::ToString;
    AddTag(tagName, ToString(tagValue));
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

extern thread_local TTraceContext* CurrentTraceContext;

TTraceContextPtr SwapTraceContext(TTraceContextPtr newContext);

} // namespace NDetail

Y_FORCE_INLINE TCurrentTraceContextGuard::TCurrentTraceContextGuard(TTraceContextPtr traceContext)
    : Active_(static_cast<bool>(traceContext))
{
    if (Active_) {
        OldTraceContext_ = NDetail::SwapTraceContext(std::move(traceContext));
    }
}

Y_FORCE_INLINE TCurrentTraceContextGuard::TCurrentTraceContextGuard(TCurrentTraceContextGuard&& other)
    : Active_(other.Active_)
    , OldTraceContext_(std::move(other.OldTraceContext_))
{
    other.Active_ = false;
}

Y_FORCE_INLINE TCurrentTraceContextGuard::~TCurrentTraceContextGuard()
{
    Release();
}

Y_FORCE_INLINE bool TCurrentTraceContextGuard::IsActive() const
{
    return Active_;
}

Y_FORCE_INLINE void TCurrentTraceContextGuard::Release()
{
    if (Active_) {
        NDetail::SwapTraceContext(std::move(OldTraceContext_));
        Active_ = false;
    }
}

Y_FORCE_INLINE const TTraceContextPtr& TCurrentTraceContextGuard::GetOldTraceContext() const
{
    return OldTraceContext_;
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TNullTraceContextGuard::TNullTraceContextGuard()
    : Active_(true)
    , OldTraceContext_(NDetail::SwapTraceContext(nullptr))
{ }

Y_FORCE_INLINE TNullTraceContextGuard::TNullTraceContextGuard(TNullTraceContextGuard&& other)
    : Active_(other.Active_)
    , OldTraceContext_(std::move(other.OldTraceContext_))
{
    other.Active_ = false;
}

Y_FORCE_INLINE TNullTraceContextGuard::~TNullTraceContextGuard()
{
    Release();
}

Y_FORCE_INLINE bool TNullTraceContextGuard::IsActive() const
{
    return Active_;
}

Y_FORCE_INLINE void TNullTraceContextGuard::Release()
{
    if (Active_) {
        NDetail::SwapTraceContext(std::move(OldTraceContext_));
        Active_ = false;
    }
}

Y_FORCE_INLINE const TTraceContextPtr& TNullTraceContextGuard::GetOldTraceContext() const
{
    return OldTraceContext_;
}

////////////////////////////////////////////////////////////////////////////////

inline TTraceContextGuard::TTraceContextGuard(TTraceContextPtr traceContext)
    : TraceContextGuard_(std::move(traceContext))
    , FinishGuard_(TryGetCurrentTraceContext())
{ }

////////////////////////////////////////////////////////////////////////////////

inline bool TChildTraceContextGuard::IsRecorded(const TTraceContextPtr& traceContext)
{
    return traceContext && traceContext->IsRecorded();
}

inline TChildTraceContextGuard::TChildTraceContextGuard(
    const TTraceContextPtr& traceContext,
    TString spanName)
    : TraceContextGuard_(IsRecorded(traceContext) ? traceContext->CreateChild(spanName) : nullptr)
    , FinishGuard_(IsRecorded(traceContext) ? TryGetCurrentTraceContext() : nullptr)
{ }

inline TChildTraceContextGuard::TChildTraceContextGuard(
    TString spanName)
    : TChildTraceContextGuard(
        TryGetCurrentTraceContext(),
        std::move(spanName))
{ }

////////////////////////////////////////////////////////////////////////////////

inline TTraceContextFinishGuard::TTraceContextFinishGuard(TTraceContextPtr traceContext)
    : TraceContext_(std::move(traceContext))
{ }

inline TTraceContextFinishGuard::~TTraceContextFinishGuard()
{
    if (TraceContext_) {
        TraceContext_->Finish();
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TTraceContext* TryGetCurrentTraceContext()
{
    return NDetail::CurrentTraceContext;
}

Y_FORCE_INLINE TTraceContext* GetCurrentTraceContext()
{
    YT_ASSERT(NDetail::CurrentTraceContext);
    return NDetail::CurrentTraceContext;
}

Y_FORCE_INLINE TTraceContextPtr CreateTraceContextFromCurrent(TString spanName)
{
    auto* context = TryGetCurrentTraceContext();
    return context ? context->CreateChild(std::move(spanName)) : TTraceContext::NewRoot(std::move(spanName));
}

////////////////////////////////////////////////////////////////////////////////

template <class TFn>
void AnnotateTraceContext(TFn&& fn)
{
    if (auto* traceContext = TryGetCurrentTraceContext(); traceContext && traceContext->IsRecorded()) {
        fn(traceContext);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing

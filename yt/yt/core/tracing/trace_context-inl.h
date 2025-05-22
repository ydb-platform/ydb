#ifndef TRACE_CONTEXT_INL_H_
#error "Direct inclusion of this file is not allowed, include trace_context.h"
// For the sake of sane code completion.
#include "trace_context.h"
#endif

#include "allocation_tags.h"

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/concurrency/propagating_storage.h>

#include <library/cpp/yt/misc/tls.h>

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

Y_FORCE_INLINE const std::string& TTraceContext::GetSpanName() const
{
    return SpanName_;
}

Y_FORCE_INLINE const std::string& TTraceContext::GetLoggingTag() const
{
    return LoggingTag_;
}

Y_FORCE_INLINE const std::optional<std::string>& TTraceContext::GetTargetEndpoint() const
{
    return TargetEndpoint_;
}

Y_FORCE_INLINE NProfiling::TCpuDuration TTraceContext::GetElapsedCpuTime() const
{
    return ElapsedCpuTime_.load(std::memory_order::relaxed);
}

template <class T>
void TTraceContext::AddTag(const std::string& tagName, const T& tagValue)
{
    if (!IsRecorded()) {
        return;
    }

    using ::ToString;
    // TODO(babenko): migrate to std::string
    AddTag(tagName, std::string(ToString(tagValue)));
}

template <typename T>
std::optional<T> TTraceContext::FindAllocationTag(const TAllocationTagKey& key) const
{
    // NB: No lock is needed.
    if (auto list = AllocationTagList_.Acquire()) {
        if (auto optionalValue = list->FindTagValue(key)) {
            return FromString<T>(*optionalValue);
        }
    }
    return std::nullopt;
}

template <typename T>
std::optional<T> TTraceContext::SetAllocationTag(const TAllocationTagKey& key, const T& value)
{
    auto newTagValue = ToString(value);

    auto guard = Guard(AllocationTagsLock_);

    auto newTags = GetAllocationTags();
    if (!newTags.empty()) {
        auto it = std::find_if(
            newTags.begin(),
            newTags.end(),
            [&] (const auto& pair) {
                return pair.first == key;
            });

        std::optional<TAllocationTagValue> oldTagValue;
        if (it != newTags.end()) {
            oldTagValue = std::move(it->second);
            it->second = std::move(newTagValue);
        } else {
            newTags.emplace_back(key, std::move(newTagValue));
        }

        DoSetAllocationTags(std::move(newTags));

        if (oldTagValue) {
            return FromString<T>(*oldTagValue);
        }
    } else {
        DoSetAllocationTags({{key, std::move(newTagValue)}});
    }

    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

YT_DECLARE_THREAD_LOCAL(TTraceContext*, CurrentTraceContext);

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

inline void TTraceContextGuard::Release(
    std::optional<NProfiling::TCpuInstant> finishTime)
{
    FinishGuard_.Release(finishTime);
}

////////////////////////////////////////////////////////////////////////////////

inline bool TChildTraceContextGuard::IsRecorded(const TTraceContextPtr& traceContext)
{
    return traceContext && traceContext->IsRecorded();
}

inline TChildTraceContextGuard::TChildTraceContextGuard(
    const TTraceContextPtr& traceContext,
    const std::string& spanName,
    std::optional<NProfiling::TCpuInstant> startTime)
    : TraceContextGuard_(IsRecorded(traceContext) ? traceContext->CreateChild(spanName, startTime) : nullptr)
    , FinishGuard_(IsRecorded(traceContext) ? TryGetCurrentTraceContext() : nullptr)
{ }

inline TChildTraceContextGuard::TChildTraceContextGuard(
    const std::string& spanName,
    std::optional<NProfiling::TCpuInstant> startTime)
    : TChildTraceContextGuard(
        TryGetCurrentTraceContext(),
        spanName,
        startTime)
{ }

inline void TChildTraceContextGuard::Finish(
    std::optional<NProfiling::TCpuInstant> finishTime)
{
    FinishGuard_.Release(finishTime);
}

////////////////////////////////////////////////////////////////////////////////

inline TTraceContextFinishGuard::TTraceContextFinishGuard(TTraceContextPtr traceContext)
    : TraceContext_(std::move(traceContext))
{ }

inline TTraceContextFinishGuard::~TTraceContextFinishGuard()
{
    Release();
}

inline TTraceContextFinishGuard& TTraceContextFinishGuard::operator=(TTraceContextFinishGuard&& other)
{
    if (this != &other) {
        Release();
        TraceContext_ = std::move(other.TraceContext_);
    }
    return *this;
}

inline void TTraceContextFinishGuard::Release(
    std::optional<NProfiling::TCpuInstant> finishTime)
{
    if (TraceContext_) {
        TraceContext_->Finish(finishTime);
        TraceContext_ = {};
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TTraceContext* TryGetCurrentTraceContext()
{
    return NDetail::CurrentTraceContext();
}

Y_FORCE_INLINE TTraceContext* GetCurrentTraceContext()
{
    YT_ASSERT(NDetail::CurrentTraceContext());
    return NDetail::CurrentTraceContext();
}

Y_FORCE_INLINE TTraceContextPtr CreateTraceContextFromCurrent(const std::string& spanName)
{
    auto* context = TryGetCurrentTraceContext();
    return context ? context->CreateChild(spanName) : TTraceContext::NewRoot(spanName);
}

Y_FORCE_INLINE TTraceContextPtr GetOrCreateTraceContext(const std::string& spanNameIfCreate)
{
    auto* context = TryGetCurrentTraceContext();
    return context ? context : TTraceContext::NewRoot(std::move(spanNameIfCreate));
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

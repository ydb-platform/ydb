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

template <typename TTag>
std::optional<TTag> TTraceContext::DoFindAllocationTag(const TString& key) const
{
    VERIFY_SPINLOCK_AFFINITY(AllocationTagsLock_);

    TAllocationTagsPtr tags;

    {
        // Local guard for copy RefCounted AllocationTags_.
        auto guard = Guard(AllocationTagsAsRefCountedLock_);
        tags = AllocationTags_;
    }

    if (tags) {
        auto valueOpt = tags->FindTagValue(key);

        if (valueOpt.has_value()) {
            return FromString<TTag>(valueOpt.value());
        }
    }

    return std::nullopt;
}

template <typename TTag>
std::optional<TTag> TTraceContext::FindAllocationTag(const TString& key) const
{
    auto readerGuard = ReaderGuard(AllocationTagsLock_);
    return DoFindAllocationTag<TTag>(key);
}

template <typename TTag>
std::optional<TTag> TTraceContext::RemoveAllocationTag(const TString& key)
{
    auto writerGuard = NThreading::WriterGuard(AllocationTagsLock_);
    auto newTags = DoGetAllocationTags();

    auto foundTagIt = std::remove_if(
        newTags.begin(),
        newTags.end(),
        [&key] (const auto& pair) {
            return pair.first == key;
        });

    std::optional<TTag> oldTag;

    if (foundTagIt != newTags.end()) {
        oldTag = FromString<TTag>(foundTagIt->second);
    }

    newTags.erase(foundTagIt, newTags.end());

    DoSetAllocationTags(std::move(newTags));

    return oldTag;
}

template <typename TTag>
std::optional<TTag> TTraceContext::SetAllocationTag(const TString& key, TTag newTag)
{
    auto newTagString = ToString(newTag);

    auto writerGuard = NThreading::WriterGuard(AllocationTagsLock_);
    auto newTags = DoGetAllocationTags();

    if (!newTags.empty()) {
        std::optional<TString> oldTag;

        auto tagIt = std::find_if(
            newTags.begin(),
            newTags.end(),
            [&key] (const auto& pair) {
                return pair.first == key;
            });

        if (tagIt != newTags.end()) {
            oldTag = std::move(tagIt->second);
            tagIt->second = std::move(newTagString);
        } else {
            newTags.emplace_back(key, std::move(newTagString));
        }

        DoSetAllocationTags(std::move(newTags));

        if (oldTag.has_value()) {
            return FromString<TTag>(oldTag.value());
        }
    } else {
        DoSetAllocationTags({{key, std::move(newTagString)}});
    }

    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

YT_DECLARE_THREAD_LOCAL(TTraceContext*, CurrentTraceContext);

TTraceContextPtr SwapTraceContext(TTraceContextPtr newContext, TSourceLocation loc);

} // namespace NDetail

Y_FORCE_INLINE TCurrentTraceContextGuard::TCurrentTraceContextGuard(TTraceContextPtr traceContext, TSourceLocation location)
    : Active_(static_cast<bool>(traceContext))
{
    if (Active_) {
        OldTraceContext_ = NDetail::SwapTraceContext(std::move(traceContext), location);
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
        NDetail::SwapTraceContext(std::move(OldTraceContext_), YT_CURRENT_SOURCE_LOCATION);
        Active_ = false;
    }
}

Y_FORCE_INLINE const TTraceContextPtr& TCurrentTraceContextGuard::GetOldTraceContext() const
{
    return OldTraceContext_;
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TNullTraceContextGuard::TNullTraceContextGuard(TSourceLocation location)
    : Active_(true)
    , OldTraceContext_(NDetail::SwapTraceContext(nullptr, location))
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
        NDetail::SwapTraceContext(std::move(OldTraceContext_), YT_CURRENT_SOURCE_LOCATION);
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
    TString spanName,
    std::optional<NProfiling::TCpuInstant> startTime)
    : TraceContextGuard_(IsRecorded(traceContext) ? traceContext->CreateChild(spanName, startTime) : nullptr)
    , FinishGuard_(IsRecorded(traceContext) ? TryGetCurrentTraceContext() : nullptr)
{ }

inline TChildTraceContextGuard::TChildTraceContextGuard(
    TString spanName,
    std::optional<NProfiling::TCpuInstant> startTime)
    : TChildTraceContextGuard(
        TryGetCurrentTraceContext(),
        std::move(spanName),
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

Y_FORCE_INLINE TTraceContextPtr CreateTraceContextFromCurrent(TString spanName)
{
    auto* context = TryGetCurrentTraceContext();
    return context ? context->CreateChild(std::move(spanName)) : TTraceContext::NewRoot(std::move(spanName));
}

Y_FORCE_INLINE TTraceContextPtr GetOrCreateTraceContext(TString spanNameIfCreate)
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

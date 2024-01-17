#include "async_queue_trace.h"

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

TAsyncQueueTrace::TAsyncQueueTrace(bool lazy)
    : Lazy_(lazy)
{ }

void TAsyncQueueTrace::Join(i64 queueIndex, const TTraceContextPtr& context)
{
    if (!context->IsSampled()) {
        return;
    }

    auto guard = Guard(Lock_);
    YT_VERIFY(Blocked_.empty() || queueIndex > Blocked_.back().first);
    Blocked_.emplace_back(queueIndex, context);

    for (const auto& [span, startIndex] : Background_) {
        if (queueIndex >= startIndex) {
            context->AddAsyncChild(span->GetTraceId());
        }
    }
}

void TAsyncQueueTrace::Join(i64 queueIndex)
{
    auto* traceContext = TryGetCurrentTraceContext();
    if (!traceContext) {
        return;
    }

    Join(queueIndex, traceContext);
}

std::pair<TTraceContextPtr, bool> TAsyncQueueTrace::StartSpan(i64 startIndex, const TString& spanName)
{
    auto traceContext = TTraceContext::NewRoot(spanName);
    traceContext->SetRecorded();

    bool sampled = false;
    if (!Lazy_) {
        sampled = true;
    }

    auto guard = Guard(Lock_);

    for (const auto& [queueIndex, client] : Blocked_) {
        // If startIndex > queueIndex, client is not blocked by this span.
        if (queueIndex >= startIndex && client->AddAsyncChild(traceContext->GetTraceId()) && !sampled) {
            sampled = client->IsSampled();
        }
    }

    if (sampled) {
        Background_[traceContext] = startIndex;
        traceContext->SetSampled();
    }

    return {traceContext, sampled};
}

void TAsyncQueueTrace::Commit(i64 endIndex)
{
    auto guard = Guard(Lock_);
    while (!Blocked_.empty()) {
        auto queueIndex = Blocked_.front().first;
        if (queueIndex <= endIndex) {
            Blocked_.pop_front();
        } else {
            return;
        }
    }
}

void TAsyncQueueTrace::FinishSpan(const TTraceContextPtr& traceContext)
{
    auto guard = Guard(Lock_);
    Background_.erase(traceContext);
}

TAsyncQueueTraceGuard TAsyncQueueTrace::CreateTraceGuard(const TString& spanName, i64 startIndex, std::optional<i64> endIndex)
{
    auto [traceContext, sampled] = StartSpan(startIndex, spanName);
    if (!sampled) {
        return TAsyncQueueTraceGuard{nullptr, nullptr, {}};
    }

    return TAsyncQueueTraceGuard{this, traceContext, endIndex};
}

////////////////////////////////////////////////////////////////////////////////

TAsyncQueueTraceGuard::TAsyncQueueTraceGuard(
    TAsyncQueueTrace* queueTrace,
    const TTraceContextPtr& traceContext,
    std::optional<i64> endIndex)
    : QueueTrace_(queueTrace)
    , TraceContext_(traceContext)
    , EndIndex_(endIndex)
    , TraceContextGuard_(traceContext)
{ }

TAsyncQueueTraceGuard::TAsyncQueueTraceGuard(TAsyncQueueTraceGuard&& other)
    : QueueTrace_(other.QueueTrace_)
    , TraceContext_(std::move(other.TraceContext_))
    , EndIndex_(other.EndIndex_)
    , TraceContextGuard_(std::move(other.TraceContextGuard_))
{
    QueueTrace_ = nullptr;
}

TAsyncQueueTraceGuard::~TAsyncQueueTraceGuard()
{
    if (!QueueTrace_) {
        return;
    }

    QueueTrace_->FinishSpan(TraceContext_);
    if (EndIndex_) {
        QueueTrace_->Commit(*EndIndex_);
    }
}

void TAsyncQueueTraceGuard::OnError()
{
    EndIndex_ = {};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing

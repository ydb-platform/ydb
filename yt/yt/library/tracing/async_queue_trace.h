#pragma once

#include "public.h"

#include <yt/yt/core/tracing/trace_context.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <util/generic/noncopyable.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

//! Async queue trace propagates tracing through async queue.
//
// We assume that multiple clients queue work, each with specific index.
// And single background worker processes queue sequentially or with pipelining.
// On each iteration background worker process range of items in [startIndex, endIndex].
//
// We assume that Join(i) is not blocked by StartSpan(j) where j > i.
//
// In eager mode, background worker is always traced.
//
// In lazy mode, background worker is traced only when there are blocked clients that request sampling.
class TAsyncQueueTrace
{
public:
    TAsyncQueueTrace(bool lazy = true);

    //! Join notifies queue that context is blocked by background processing up to queueIndex.
    //
    // Must be called with increasing queueIndex.
    void Join(i64 queueIndex, const TTraceContextPtr& context);

    //! Same join, but with implicit trace context.
    void Join(i64 queueIndex);

    //! StartSpan creates span that traces background work in the queue.
    std::pair<TTraceContextPtr, bool> StartSpan(i64 startIndex, const TString& spanName);

    //! Notify that trace is finished.
    void FinishSpan(const TTraceContextPtr& traceContext);

    //! Notify that all future calls to StartSpan will have startIndex > endIndex.
    void Commit(i64 endIndex);

    TAsyncQueueTraceGuard CreateTraceGuard(const TString& spanName, i64 startIndex, std::optional<i64> endIndex);

private:
    const bool Lazy_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    std::deque<std::pair<i64, TTraceContextPtr>> Blocked_;
    THashMap<TTraceContextPtr, i64> Background_;
};

////////////////////////////////////////////////////////////////////////////////

class TAsyncQueueTraceGuard
{
public:
    TAsyncQueueTraceGuard(TAsyncQueueTrace* queueTrace, const TTraceContextPtr& traceContext, std::optional<i64> endIndex);
    TAsyncQueueTraceGuard(TAsyncQueueTraceGuard&& other);
    ~TAsyncQueueTraceGuard();

    // Support only move construction. Delete all other copy/move operators.
    TAsyncQueueTraceGuard operator = (TAsyncQueueTraceGuard&& other) = delete;

    void OnError();

private:
    TAsyncQueueTrace* QueueTrace_ = nullptr;
    TTraceContextPtr TraceContext_;
    std::optional<i64> EndIndex_;

    TTraceContextGuard TraceContextGuard_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing

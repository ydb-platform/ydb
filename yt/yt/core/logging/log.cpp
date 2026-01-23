#include "log.h"

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <library/cpp/yt/misc/thread_name.h>

#include <util/system/thread.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

#ifndef _win_

TLoggingContext GetLoggingContext()
{
    auto now = GetCpuInstant();

    auto* traceContext = NTracing::TryGetCurrentTraceContext();
    if (traceContext) {
        traceContext->CheckForLeak(now);
    }

    return TLoggingContext{
        .Instant = now,
        .ThreadId = TThread::CurrentThreadId(),
        .ThreadName = GetCurrentThreadName(),
        .FiberId = NConcurrency::GetCurrentFiberId(),
        .TraceId = traceContext ? traceContext->GetTraceId() : TTraceId{},
        .RequestId = traceContext ? traceContext->GetRequestId() : NTracing::TRequestId(),
        .TraceLoggingTag = traceContext ? traceContext->GetLoggingTag() : TStringBuf(),
    };
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging

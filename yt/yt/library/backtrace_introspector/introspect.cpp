#include "introspect.h"

#include "private.h"

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/concurrency/fiber.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <library/cpp/yt/memory/safe_memory_reader.h>

#include <library/cpp/yt/backtrace/backtrace.h>

#include <library/cpp/yt/backtrace/cursors/libunwind/libunwind_cursor.h>

#include <library/cpp/yt/backtrace/cursors/frame_pointer/frame_pointer_cursor.h>

#include <library/cpp/yt/backtrace/cursors/interop/interop.h>

#include <util/system/yield.h>

namespace NYT::NBacktraceIntrospector {

using namespace NConcurrency;
using namespace NThreading;
using namespace NTracing;
using namespace NBacktrace;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = BacktraceIntrospectorLogger;

////////////////////////////////////////////////////////////////////////////////

std::vector<TFiberIntrospectionInfo> IntrospectFibers()
{
    YT_LOG_INFO("Fiber introspection started");

    YT_LOG_INFO("Collecting waiting fibers backtraces");

    std::vector<TFiberIntrospectionInfo> infos;
    THashSet<TFiberId> waitingFiberIds;
    THashMap<TFiberId, EFiberState> fiberStates;

    auto introspectionAction = [&] (NYT::NConcurrency::TFiber::TFiberList& fibers) {
        for (auto& fiberRef : fibers) {
            auto* fiber = fiberRef.AsFiber();

            auto fiberId = fiber->GetFiberId();
            if (fiberId == InvalidFiberId) {
                continue;
            }

            EmplaceOrCrash(fiberStates, fiberId, EFiberState::Introspecting);

            EFiberState state;

            auto onIntrospectionLockAcquired = [&] {
                YT_LOG_DEBUG("Waiting fiber is successfully locked for introspection (FiberId: %x)",
                    fiberId);

                const auto& propagatingStorage = *NConcurrency::TryGetPropagatingStorage(*fiber->GetFls());
                const auto* traceContext = TryGetTraceContextFromPropagatingStorage(propagatingStorage);

                TFiberIntrospectionInfo info{
                    .State = EFiberState::Waiting,
                    .FiberId = fiberId,
                    .WaitingSince = fiber->GetWaitingSince(),
                    .TraceId = traceContext ? traceContext->GetTraceId() : TTraceId(),
                    .TraceLoggingTag = traceContext ? traceContext->GetLoggingTag() : TString(),
                };

                auto optionalContext = TrySynthesizeLibunwindContextFromMachineContext(*fiber->GetMachineContext());
                if (!optionalContext) {
                    YT_LOG_WARNING("Failed to synthesize libunwind context (FiberId: %x)",
                        fiberId);
                    return;
                }

                TLibunwindCursor cursor(*optionalContext);
                while (!cursor.IsFinished()) {
                    info.Backtrace.push_back(cursor.GetCurrentIP());
                    cursor.MoveNext();
                }

                infos.push_back(std::move(info));
                InsertOrCrash(waitingFiberIds, fiberId);

                YT_LOG_DEBUG("Fiber introspection completed (FiberId: %x)",
                    info.FiberId);
            };
            if (!fiber->TryLockForIntrospection(&state, onIntrospectionLockAcquired)) {
                YT_LOG_DEBUG("Failed to lock fiber for introspection (FiberId: %x, State: %v)",
                    fiberId,
                    state);
                fiberStates[fiberId] = state;
            }
        }
    };

    TFiber::ReadFibers(introspectionAction);

    YT_LOG_INFO("Collecting running fibers backtraces");

    THashSet<TFiberId> runningFiberIds;
    for (auto& info : IntrospectThreads()) {
        if (info.FiberId == InvalidFiberId) {
            continue;
        }

        if (waitingFiberIds.contains(info.FiberId)) {
            continue;
        }

        if (!runningFiberIds.insert(info.FiberId).second) {
            continue;
        }

        infos.push_back(TFiberIntrospectionInfo{
            .State = EFiberState::Running,
            .FiberId = info.FiberId,
            .ThreadId = info.ThreadId,
            .ThreadName = std::move(info.ThreadName),
            .TraceId = info.TraceId,
            .TraceLoggingTag = std::move(info.TraceLoggingTag),
            .Backtrace = std::move(info.Backtrace),
        });
    }

    for (const auto& [fiberId, fiberState] : fiberStates) {
        if (fiberId == InvalidFiberId) {
            continue;
        }
        if (runningFiberIds.contains(fiberId)) {
            continue;
        }
        if (waitingFiberIds.contains(fiberId)) {
            continue;
        }

        infos.push_back(TFiberIntrospectionInfo{
            .State = fiberState,
            .FiberId = fiberId,
        });
    }

    YT_LOG_INFO("Fiber introspection completed");

    return infos;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

void FormatBacktrace(TStringBuilder* builder, const std::vector<const void*>& backtrace)
{
    if (!backtrace.empty()) {
        builder->AppendString("Backtrace:\n");
        SymbolizeBacktrace(
            MakeRange(backtrace),
            [&] (TStringBuf str) {
                builder->AppendFormat("  %v", str);
            });
    }
}

} // namespace

TString FormatIntrospectionInfos(const std::vector<TThreadIntrospectionInfo>& infos)
{
    TStringBuilder builder;
    for (const auto& info : infos) {
        builder.AppendFormat("Thread id: %v\n", info.ThreadId);
        builder.AppendFormat("Thread name: %v\n", info.ThreadName);
        if (info.FiberId != InvalidFiberId) {
            builder.AppendFormat("Fiber id: %x\n", info.FiberId);
        }
        if (info.TraceId) {
            builder.AppendFormat("Trace id: %v\n", info.TraceId);
        }
        if (info.TraceLoggingTag) {
            builder.AppendFormat("Trace logging tag: %v\n", info.TraceLoggingTag);
        }
        FormatBacktrace(&builder, info.Backtrace);
        builder.AppendString("\n");
    }
    return builder.Flush();
}

TString FormatIntrospectionInfos(const std::vector<TFiberIntrospectionInfo>& infos)
{
    TStringBuilder builder;
    for (const auto& info : infos) {
        builder.AppendFormat("Fiber id: %x\n", info.FiberId);
        builder.AppendFormat("State: %v\n", info.State);
        if (info.WaitingSince) {
            builder.AppendFormat("Waiting since: %v\n", info.WaitingSince);
        }
        if (info.ThreadId != InvalidThreadId) {
            builder.AppendFormat("Thread id: %v\n", info.ThreadId);
        }
        if (!info.ThreadName.empty()) {
            builder.AppendFormat("Thread name: %v\n", info.ThreadName);
        }
        if (info.TraceId) {
            builder.AppendFormat("Trace id: %v\n", info.TraceId);
        }
        if (info.TraceLoggingTag) {
            builder.AppendFormat("Trace logging tag: %v\n", info.TraceLoggingTag);
        }
        FormatBacktrace(&builder, info.Backtrace);
        builder.AppendString("\n");
    }
    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBacktraceIntrospector

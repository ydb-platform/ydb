#pragma once

#include "kqp_trace_flow.h"
#include "kqp_trace_future.h"

#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/spinlock.h>

#include <utility>
#include <type_traits>
#include <functional>
#include <optional>

namespace NKikimr::NKqp {

struct TCompileTraceLevels {
    enum : ui8 {
        // Phases and dependency boundaries; restarting loops share one span.
        Phase = TComponentTracingLevels::TQueryProcessor::Detailed,
        // Passes and representation conversions.
        Step = TComponentTracingLevels::TQueryProcessor::Diagnostic,
        // Applied rules and recomputed properties.
        Detail = TComponentTracingLevels::TQueryProcessor::Trace,
    };
};

// Attributes set on a span when it starts.
using TCompileTraceAttributes = TVector<std::pair<TString, TString>>;

// Optimizer identity on each "Optimize query" phase, including fallbacks.
inline constexpr TStringBuf OptimizerAttribute = "ydb.kqp.optimizer";
inline constexpr TStringBuf NewRboOptimizer = "new_rbo";
inline constexpr TStringBuf OldRboOptimizer = "old_rbo";

// One compilation: regions span turns; scopes record synchronous work.
// Only GetCurrentParent is concurrent; other methods run on serial compiler turns.
class TCompileTrace : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TCompileTrace>;

    TCompileTrace(NWilson::TTraceId root, NActors::TActorSystem* actorSystem);
    ~TCompileTrace();

    bool IsEnabled(ui8 verbosity) const {
        return Root && verbosity <= Root.GetVerbosity();
    }

    NWilson::TSpan StartSpan(ui8 verbosity, TString name) const;
    static void EndSpan(NWilson::TSpan& span, bool success = true);

    // Makes the span the parent of the spans started until it is popped.
    void Push(const NWilson::TSpan& span);
    void Pop(const NWilson::TSpan& span);

    // Thread-safe parent lookup; keeps deferred rules whose children need them.
    NWilson::TTraceId GetCurrentParent() const;

    // Opening another region in the same group closes the previous region.
    // Reopening the current region preserves it across pipeline restarts.
    void BeginRegion(ui8 verbosity, const TString& name, const void* group,
        const TCompileTraceAttributes& attributes = {});
    // Ends the region of the group together with the regions opened inside it.
    void EndRegion(TStringBuf name, const void* group, bool success = true);

    // Waits stay open through scheduling; only a compiler turn closes them.
    void BeginWait(TString name, const NWilson::TTraceId& parent,
        const TVector<NWilson::TTraceId>& dependencies, std::function<std::optional<bool>()> completion);
    void ResumeWaits();

    // Close remaining regions and abort outstanding waits. Destruction implies failure.
    void Finish(bool success);

private:
    friend class TCompileTraceRuleScope;
    void Push(const NWilson::TTraceId& id);
    bool Pop(const NWilson::TTraceId& id);
    void EndRegions(size_t count, bool success);

private:
    const NWilson::TTraceId Root;
    NActors::TActorSystem* const ActorSystem;

    struct TParent {
        NWilson::TTraceId Id;
        mutable bool HasChildren = false;
    };
    TVector<TParent> Parents;
    // Guards Parents for GetCurrentParent.
    TAdaptiveLock ParentsLock;

    struct TRegion {
        TString Name;
        const void* Group;
        NWilson::TSpan Span;
    };
    TVector<TRegion> Regions;
    struct TWait {
        NWilson::TSpan Span;
        std::function<std::optional<bool>()> Completion;
    };
    TVector<TWait> Waits;
    bool Finished = false;
};

// Dispatch requests under the current phase, then record their actual wait.
class TCompileTraceDependencies {
public:
    TCompileTraceDependencies(TCompileTrace* trace, TString name);

    NWilson::TSpan Start(TString name);
    template <class T, class TSuccess>
    NThreading::TFuture<T> Await(NThreading::TFuture<T> future, TSuccess success) {
        if (Trace && Started) {
            Trace->BeginWait(Name, Parent, Dependencies, [future, success = std::move(success)]() -> std::optional<bool> {
                if (!future.IsReady()) {
                    return std::nullopt;
                }
                if (future.HasException()) {
                    return false;
                }
                if constexpr (std::is_void_v<T>) {
                    return success();
                } else {
                    return success(future.GetValue());
                }
            });
        }
        return future;
    }

    NThreading::TFuture<void> Await(NThreading::TFuture<void> future) {
        return Await(std::move(future), [] { return true; });
    }

private:
    TCompileTrace* const Trace;
    const TString Name;
    const NWilson::TTraceId Parent;
    TVector<NWilson::TTraceId> Dependencies;
    bool Started = false;
};

// Record result failures without turning them into future exceptions.
void FinishCompileDependency(NWilson::TSpan& span, bool futureSuccess, bool operationSuccess = true);

// Ends a dependency before its consumer resumes, even if compilation has ended.
template <class T, class TSuccess>
NThreading::TFuture<T> TraceCompileDependency(NThreading::TFuture<T> future, NWilson::TSpan span, TSuccess success) {
    return TraceFuture(std::move(future), std::move(span),
        [success = std::move(success)](NWilson::TSpan& span, auto result) mutable {
            if constexpr (std::is_void_v<T>) {
                FinishCompileDependency(span, result, result && success());
            } else {
                FinishCompileDependency(span, result != nullptr, result && success(*result));
            }
        });
}

template <class T>
NThreading::TFuture<T> TraceCompileDependency(NThreading::TFuture<T> future, NWilson::TSpan span) {
    return TraceCompileDependency(std::move(future), std::move(span), [](const auto&...) { return true; });
}

// A synchronous scope; names are constructed only when tracing is enabled.
class TCompileTraceScope {
public:
    TCompileTraceScope(TCompileTrace* trace, ui8 verbosity, TStringBuf name, TStringBuf suffix = {});
    ~TCompileTraceScope();

    TCompileTraceScope(const TCompileTraceScope&) = delete;
    TCompileTraceScope& operator=(const TCompileTraceScope&) = delete;

    // For operations that return an error result without throwing.
    void SetSuccess(bool success) {
        Success = success;
    }

private:
    TCompileTrace* const Trace;
    const int UncaughtExceptions;
    NWilson::TSpan Span;
    bool Success = true;
};

// Emit applied/failed rules and rules needed to parent recorded children.
class TCompileTraceRuleScope {
public:
    TCompileTraceRuleScope(TCompileTrace* trace, const TString& name);
    ~TCompileTraceRuleScope();

    TCompileTraceRuleScope(const TCompileTraceRuleScope&) = delete;
    TCompileTraceRuleScope& operator=(const TCompileTraceRuleScope&) = delete;

    void SetApplied(bool applied) { Applied = applied; }

private:
    TCompileTrace* const Trace;
    const TString Name;
    const int UncaughtExceptions;
    NWilson::TTraceId Parent;
    NWilson::TTraceId Id;
    ui64 StartUnixNano = 0;
    bool Applied = false;
};

} // namespace NKikimr::NKqp

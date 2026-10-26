#pragma once

#include "kqp_trace_flow.h"

#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/spinlock.h>

#include <utility>
#include <memory>
#include <type_traits>

namespace NKikimr::NKqp {

struct TCompileTraceLevels {
    enum : ui8 {
        // Compilation phases: setup, parsing, preparation and optimization of the
        // query, the cost-based join order search, physical query compilation, and
        // waits for metadata and statistics. Traced in production, so a statement has
        // a few of them, whatever its size: a phase that runs in a restarting loop is
        // one span for the whole loop.
        Phase = TComponentTracingLevels::TQueryProcessor::Detailed,
        // Steps of a phase: passes of a loop, RBO stages, conversions between plan
        // representations, the requests of a wait, physical conversion steps, and
        // releasing the compiler after the reply. Their number depends on the
        // optimizer configuration and the tables of the query.
        Step = TComponentTracingLevels::TQueryProcessor::Diagnostic,
        // Applied RBO rules and recomputed plan properties: their number grows
        // with the query.
        Detail = TComponentTracingLevels::TQueryProcessor::Trace,
    };
};

// Spans of a single query compilation, all in the flow of the compiler. Regions
// may stay open across asynchronous waits; scope spans record synchronous work.
// The innermost open region or scope parents new compiler work.
//
// Threading: compiler turns run serially, possibly on different threads. Only
// GetCurrentParent may be called concurrently with them; everything else must be
// called from the compiler turns.
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

    // Parent for spans that other components start during the compilation,
    // e.g. metadata requests of the gateway. Safe to call from any thread.
    // Marks the parent as having children, so that a rule scope that starts
    // them is emitted.
    NWilson::TTraceId GetCurrentParent() const;

    // A region is a span opened and closed by separate calls, e.g. by marker
    // stages placed around groups of transformers of a pipeline. Regions of one
    // group follow each other: opening a region ends the open regions of its
    // group, unless it is open itself (after a restart of the pipeline).
    void BeginRegion(ui8 verbosity, const TString& name, const void* group);
    // Ends the region of the group together with the regions opened inside it.
    void EndRegion(TStringBuf name, const void* group, bool success = true);

    // Ends remaining regions: the compilation ended, also when it was stopped
    // while waiting. This trace records no further regions. A trace destroyed
    // before it is finished ends them as failed: the compilation was abandoned.
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
    bool Finished = false;
};

// Ends a dependency span. A fulfilled future may contain an error result. Keep
// this distinct from an exception: only the latter can short-circuit
// WaitExceptionOrAll.
void FinishCompileDependency(NWilson::TSpan& span, bool futureSuccess, bool operationSuccess = true);

// Ends the dependency span on the future's callback thread, before releasing its
// consumer: end(span, result) ends it, where result points to the value or is
// null for an exception (a bool for a void future). The span runs concurrently
// with the compiler, so it may outlive the compilation.
// Captures no compiler/transformer state, which may be destroyed while waiting.
template <class T, class TEnd>
NThreading::TFuture<T> TraceCompileDependencyWith(NThreading::TFuture<T> future, NWilson::TSpan span, TEnd end) {
    if (!span) {
        return future;
    }
    return future.Apply([span = std::make_shared<NWilson::TSpan>(std::move(span)), end = std::move(end)]
        (const NThreading::TFuture<T>& ready) mutable -> T {
        try {
            ready.TryRethrow();
        } catch (...) {
            if constexpr (std::is_void_v<T>) {
                end(*span, false);
            } else {
                end(*span, static_cast<const T*>(nullptr));
            }
            throw;
        }
        if constexpr (std::is_void_v<T>) {
            end(*span, true);
        } else {
            const T& result = ready.GetValue();
            end(*span, &result);
            return result;
        }
    });
}

template <class T, class TSuccess>
NThreading::TFuture<T> TraceCompileDependency(NThreading::TFuture<T> future, NWilson::TSpan span, TSuccess success) {
    return TraceCompileDependencyWith(std::move(future), std::move(span),
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

// Span that is open for the lifetime of the scope and parents the spans started in it.
// Its name is name + suffix, built only when the scope is recorded.
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

// Reserve the parent before applying a rule, but emit only applied rules,
// failed rules, or rules whose children need a parent: the latter are named
// "(not applied)". Unproductive leaf attempts do not create exported spans.
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
    TInstant StartTime;
    bool Applied = false;
};

} // namespace NKikimr::NKqp

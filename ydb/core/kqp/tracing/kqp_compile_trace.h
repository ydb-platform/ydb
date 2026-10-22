#pragma once

#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
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
        // Compilation phases: parsing, annotation, optimization, physical query compilation.
        Phase = TComponentTracingLevels::TQueryProcessor::Detailed,
        // Steps of a phase: RBO stages, physical conversion steps.
        Step = TComponentTracingLevels::TQueryProcessor::Diagnostic,
        // Applied RBO rules and recomputed plan properties.
        Detail = TComponentTracingLevels::TQueryProcessor::Trace,
    };
};

// A compiler wait may be abandoned on the actor thread while its future is
// completing elsewhere. Both paths share this one-shot span, not actor state.
class TCompileTraceContinuation {
public:
    explicit TCompileTraceContinuation(NWilson::TSpan span);

    NWilson::TTraceId GetTraceId() const;
    NWilson::TTraceId Complete(bool success = true);

private:
    friend class TCompileTrace;
    void Cancel();

    const NWilson::TTraceId Id;
    TAdaptiveLock Lock;
    NWilson::TSpan Span;
    bool Ended = false;
};

// Spans of a single query compilation. Compiler turns run serially but may
// migrate between worker threads. Logical phases may stay open across waits;
// execution spans record individual turns, and scope spans synchronous work.
// The innermost open logical phase or scope parents new compiler work.
//
// Spans ended by the trace carry the CPU time the compilation spent while they
// were open (ydb.cpu_us). It is counted only while the compilation runs, see
// Resume and Suspend, since after an asynchronous wait it may continue on
// another thread.
class TCompileTrace : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TCompileTrace>;

    TCompileTrace(NWilson::TTraceId root, NActors::TActorSystem* actorSystem);
    ~TCompileTrace();

    bool IsEnabled(ui8 verbosity) const {
        return Root && verbosity <= Root.GetVerbosity();
    }

    NWilson::TSpan StartSpan(ui8 verbosity, TString name, const char* component,
        const char* kind = "operation") const;
    // Ends the span, recording the CPU time of the compilation since cpuStart (see GetCpuTime).
    void EndSpan(NWilson::TSpan& span, ui64 cpuStart, bool success = true) const;

    // Makes the span the parent of the spans started until it is popped.
    void Push(const NWilson::TSpan& span);
    void Pop(const NWilson::TSpan& span);

    // Parent for spans that other components start during the compilation,
    // e.g. metadata requests of the gateway. Safe to call from any thread.
    NWilson::TTraceId GetCurrentParent() const;

    // Time on the clock of the innermost span. Wilson ends a span at its start
    // time plus the CPU cycles elapsed since, so spans recorded with these times
    // nest within it.
    TInstant Now() const;

    // Starts the wait for a compiler continuation. Complete it on the future's
    // callback thread, then pass its ID to Resume when the actor handles it.
    std::shared_ptr<TCompileTraceContinuation> StartContinuation(bool waiting = false);
    // Called on the compiler actor's thread by GetAsyncFuture, only for the
    // futures actually returned to the pipeline. Consumed by StartContinuation.
    void AwaitDependency(const NWilson::TTraceId& dependency);

    // Number of the next run of a span that the compilation may repeat, e.g.
    // after a restart of the pipeline, starting from 1.
    ui32 NextIteration(const TString& name);

    // CPU time of the compilation in microseconds, counted while it runs.
    ui64 GetCpuTime() const;
    // Called when the compilation starts and stops running on the current thread.
    void Resume(const NWilson::TTraceId& readyFrom = {});
    void Suspend();

    // A region is a span opened and closed by separate calls, e.g. by marker
    // stages placed around groups of transformers of a pipeline. Regions of one
    // group follow each other: opening a region ends the open regions of its
    // group, unless it is open itself (after a restart of the pipeline).
    // Returns the opened region span to add attributes to, if any.
    NWilson::TSpan* BeginRegion(ui8 verbosity, const TString& name, const char* component, const void* group);
    // Ends the region together with the regions opened inside it.
    void EndRegion(TStringBuf name);

    // Ends remaining regions, abandons the outstanding wait and freezes CPU
    // accounting. Existing run scopes may still unwind; this trace cannot resume.
    void Finish(bool success);

private:
    friend class TCompileTraceDeferredScope;
    NWilson::TTraceId Parent() const;
    // The innermost parent, without marking it used.
    NWilson::TTraceId PeekParent() const;
    void MarkUsed(const NWilson::TTraceId& id);
    void PushId(NWilson::TTraceId id, TInstant start, ui64 cycles);
    bool PopId(const NWilson::TTraceId& id);
    void EndRegions(size_t count, bool success);

private:
    const NWilson::TTraceId Root;
    NActors::TActorSystem* const ActorSystem;

    struct TParent {
        NWilson::TTraceId Id;
        TInstant Start;
        ui64 StartCycles;
        // A deferred scope must be emitted if another span uses it as parent.
        mutable bool Used = false;
    };
    TVector<TParent> Parents;
    // Guards Parents for GetCurrentParent.
    TAdaptiveLock ParentsLock;

    struct TRegion {
        TString Name;
        const void* Group;
        NWilson::TSpan Span;
        ui64 CpuStart;
    };
    TVector<TRegion> Regions;
    THashMap<TString, ui32> Iterations;
    // Names of the regions each group has opened.
    THashMap<const void*, THashSet<TString>> GroupRegions;

    ui64 Cpu = 0;
    ui64 RunningSince = 0;
    ui32 Running = 0;
    bool CpuCounted = false;
    bool Finished = false;
    NWilson::TSpan ExecutionSpan;
    TVector<NWilson::TTraceId> AwaitedDependencies;
    std::shared_ptr<TCompileTraceContinuation> Continuation;
};

// Records on its callback thread that a dependency is ready. A fulfilled
// future may contain an error result. Keep this distinct from an exception:
// only the latter can short-circuit WaitExceptionOrAll.
void MarkCompileDependencyReady(NWilson::TSpan& span, bool futureSuccess);
void FinishCompileDependency(NWilson::TSpan& span, bool futureSuccess, bool operationSuccess = true);

// Ends the dependency span on the future's callback thread, before releasing its
// consumer. The span is marked ready first, then end(span, result) ends it, where
// result points to the value or is null for an exception (a bool for a void future).
// For a value future this transfers its result to the returned future: the
// caller must consume the returned future instead of retaining the original.
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
            MarkCompileDependencyReady(*span, false);
            if constexpr (std::is_void_v<T>) {
                end(*span, false);
            } else {
                end(*span, static_cast<const T*>(nullptr));
            }
            throw;
        }
        MarkCompileDependencyReady(*span, true);
        if constexpr (std::is_void_v<T>) {
            end(*span, true);
        } else {
            auto owned = ready;
            auto result = owned.ExtractValue();
            end(*span, &std::as_const(result));
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

// Reserves a parent ID without constructing an OTLP span for an unsuccessful
// rule attempt. Finish emits applied attempts and attempts that have children.
class TCompileTraceDeferredScope {
public:
    TCompileTraceDeferredScope(TCompileTrace* trace, TString name, const char* component);
    ~TCompileTraceDeferredScope();
    TCompileTraceDeferredScope(const TCompileTraceDeferredScope&) = delete;
    TCompileTraceDeferredScope& operator=(const TCompileTraceDeferredScope&) = delete;

    NWilson::TSpan Finish(bool keep);
    ui64 GetCpuTime() const { return CpuTime; }

private:
    TCompileTrace* Trace;
    const TString Name;
    const char* const Component;
    NWilson::TTraceId Parent;
    NWilson::TTraceId Id;
    TInstant Start;
    ui64 CpuStart = 0;
    ui64 CpuTime = 0;
};

// Span that is open for the lifetime of the scope and parents the spans started in it.
class TCompileTraceScope {
public:
    TCompileTraceScope(TCompileTrace* trace, ui8 verbosity, TString name, const char* component);
    ~TCompileTraceScope();

    TCompileTraceScope(const TCompileTraceScope&) = delete;
    TCompileTraceScope& operator=(const TCompileTraceScope&) = delete;

    NWilson::TSpan& Span() {
        return Span_;
    }

    // For operations that return an error result without throwing.
    void SetSuccess(bool success) {
        Success = success;
    }

private:
    TCompileTrace* const Trace;
    const int UncaughtExceptions;
    NWilson::TSpan Span_;
    ui64 CpuStart = 0;
    bool Success = true;
};

// The compilation runs on the current thread for the lifetime of the scope.
class TCompileTraceRunScope {
public:
    explicit TCompileTraceRunScope(TCompileTrace* trace, const NWilson::TTraceId& readyFrom = {});
    ~TCompileTraceRunScope();

    TCompileTraceRunScope(const TCompileTraceRunScope&) = delete;
    TCompileTraceRunScope& operator=(const TCompileTraceRunScope&) = delete;

private:
    TCompileTrace* const Trace;
};

} // namespace NKikimr::NKqp

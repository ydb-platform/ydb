#include "kqp_compile_trace.h"

#include <ydb/library/actors/core/actorsystem.h>

#include <util/datetime/cputimer.h>
#include <util/system/datetime.h>
#include <util/system/guard.h>
#include <util/system/getpid.h>
#include <util/system/thread.h>

#include <algorithm>

namespace NKikimr::NKqp {

namespace {

void DescribeSpan(NWilson::TSpan& span, const char* component, const char* kind) {
    if (span) {
        if (component) {
            span.Attribute("ydb.code.component", TString(component));
        }
        span.Attribute("ydb.compile.kind", TString(kind));
        span.Attribute("process.pid", static_cast<i64>(GetPID()));
        span.Attribute("thread.id", static_cast<i64>(TThread::CurrentThreadNumericId()));
    }
}

} // namespace

TCompileTraceContinuation::TCompileTraceContinuation(NWilson::TSpan span)
    : Id(span.GetTraceId())
    , Span(std::move(span))
{}

NWilson::TTraceId TCompileTraceContinuation::GetTraceId() const {
    return NWilson::TTraceId(Id);
}

NWilson::TTraceId TCompileTraceContinuation::Complete(bool success) {
    auto guard = Guard(Lock);
    if (std::exchange(Ended, true)) {
        return {};
    }
    Span.Event("Result ready", {{"process.pid", static_cast<i64>(GetPID())},
        {"thread.id", static_cast<i64>(TThread::CurrentThreadNumericId())}});
    Span.Attribute("ydb.compile.future_outcome", TString(success ? "value" : "exception"));
    if (success) {
        Span.EndOk();
    } else {
        Span.EndError("Compiler continuation failed");
    }
    return NWilson::TTraceId(Id);
}

void TCompileTraceContinuation::Cancel() {
    auto guard = Guard(Lock);
    if (std::exchange(Ended, true)) {
        return;
    }
    Span.Attribute("ydb.compile.cancelled", true);
    Span.EndError("Compiler wait abandoned");
}

TCompileTrace::TCompileTrace(NWilson::TTraceId root, NActors::TActorSystem* actorSystem)
    : Root(std::move(root))
    , ActorSystem(actorSystem)
{}

TCompileTrace::~TCompileTrace() {
    Finish(true);
}

NWilson::TTraceId TCompileTrace::Parent() const {
    if (!Parents.empty()) {
        Parents.back().Used = true;
    }
    return NWilson::TTraceId(Parents.empty() ? Root : Parents.back().Id);
}

NWilson::TTraceId TCompileTrace::PeekParent() const {
    auto guard = Guard(ParentsLock);
    return NWilson::TTraceId(Parents.empty() ? Root : Parents.back().Id);
}

void TCompileTrace::MarkUsed(const NWilson::TTraceId& id) {
    auto guard = Guard(ParentsLock);
    const auto it = std::find_if(Parents.rbegin(), Parents.rend(), [&](const TParent& parent) {
        return parent.Id == id;
    });
    if (it != Parents.rend()) {
        it->Used = true;
    }
}

NWilson::TTraceId TCompileTrace::GetCurrentParent() const {
    auto guard = Guard(ParentsLock);
    return Parent();
}

NWilson::TSpan TCompileTrace::StartSpan(ui8 verbosity, TString name, const char* component, const char* kind) const {
    if (!IsEnabled(verbosity)) {
        return {};
    }
    NWilson::TSpan span(verbosity, GetCurrentParent(), std::move(name), NWilson::EFlags::NONE, ActorSystem);
    DescribeSpan(span, component, kind);
    return span;
}

void TCompileTrace::EndSpan(NWilson::TSpan& span, ui64 cpuStart, bool success) const {
    if (!span) {
        return;
    }
    if (CpuCounted) {
        span.Attribute("ydb.cpu_us", static_cast<i64>(GetCpuTime() - cpuStart));
    }
    if (success) {
        span.EndOk();
    } else {
        span.EndError("Compilation failed");
    }
}

void TCompileTrace::Push(const NWilson::TSpan& span) {
    if (span) {
        PushId(span.GetTraceId(), TInstant::Now(), GetCycleCount());
    }
}

void TCompileTrace::PushId(NWilson::TTraceId id, TInstant start, ui64 cycles) {
    auto guard = Guard(ParentsLock);
    Parents.push_back({std::move(id), start, cycles});
}

void TCompileTrace::Pop(const NWilson::TSpan& span) {
    if (span) {
        PopId(span.GetTraceId());
    }
}

bool TCompileTrace::PopId(const NWilson::TTraceId& id) {
    // Asynchronous continuations may interleave sibling spans, so the span is
    // not necessarily the innermost one.
    auto guard = Guard(ParentsLock);
    const auto it = std::find_if(Parents.rbegin(), Parents.rend(), [&](const TParent& parent) {
        return parent.Id == id;
    });
    if (it != Parents.rend()) {
        const bool used = it->Used;
        Parents.erase(std::next(it).base());
        return used;
    }
    return false;
}

TInstant TCompileTrace::Now() const {
    if (Parents.empty()) {
        return TInstant::Now();
    }
    const auto& parent = Parents.back();
    return parent.Start + CyclesToDuration(GetCycleCount() - parent.StartCycles);
}

void TCompileTrace::AwaitDependency(const NWilson::TTraceId& dependency) {
    if (dependency && IsEnabled(TCompileTraceLevels::Step)
        && std::find(AwaitedDependencies.begin(), AwaitedDependencies.end(), dependency) == AwaitedDependencies.end()) {
        AwaitedDependencies.emplace_back(dependency);
    }
}

std::shared_ptr<TCompileTraceContinuation> TCompileTrace::StartContinuation(bool waiting) {
    if (Finished) {
        return {};
    }
    auto span = StartSpan(TCompileTraceLevels::Step, "Compiler continuation", "KqpCompileActor", "async");
    if (span && ExecutionSpan) {
        span.Link(ExecutionSpan.GetTraceId(), {{"ydb.compile.relation", TString("await")}});
    }
    if (span) {
        span.Attribute("ydb.compile.known_dependencies", static_cast<i64>(AwaitedDependencies.size()));
        for (const auto& dependency : AwaitedDependencies) {
            span.Link(dependency, {{"ydb.compile.relation", TString("depends_on")}});
        }
    }
    AwaitedDependencies.clear();
    if (!span) {
        return {};
    }
    span.Attribute("ydb.compile.waiting", waiting);
    if (Continuation) {
        Continuation->Cancel();
    }
    Continuation = std::make_shared<TCompileTraceContinuation>(std::move(span));
    return Continuation;
}

void MarkCompileDependencyReady(NWilson::TSpan& span, bool futureSuccess) {
    span.Event("Dependency ready", {{"process.pid", static_cast<i64>(GetPID())},
        {"thread.id", static_cast<i64>(TThread::CurrentThreadNumericId())}});
    span.Attribute("ydb.compile.future_outcome", TString(futureSuccess ? "value" : "exception"));
}

void FinishCompileDependency(NWilson::TSpan& span, bool futureSuccess, bool operationSuccess) {
    if (futureSuccess && operationSuccess) {
        span.EndOk();
    } else {
        span.EndError(futureSuccess ? "Dependency operation failed" : "Dependency future failed");
    }
}

ui32 TCompileTrace::NextIteration(const TString& name) {
    return ++Iterations[name];
}

ui64 TCompileTrace::GetCpuTime() const {
    return Running ? Cpu + ThreadCPUTime() - RunningSince : Cpu;
}

void TCompileTrace::Resume(const NWilson::TTraceId& readyFrom) {
    if (Finished) {
        return;
    }
    if (Running++ == 0) {
        if (IsEnabled(TCompileTraceLevels::Step)) {
            // Execution is a sibling of logical phases: it must not become
            // their parent merely because a phase starts during this turn.
            ExecutionSpan = NWilson::TSpan(TCompileTraceLevels::Step, NWilson::TTraceId(Root),
                "Compiler execution", NWilson::EFlags::NONE, ActorSystem);
            DescribeSpan(ExecutionSpan, "KqpCompileActor", "execution");
            if (readyFrom) {
                ExecutionSpan.Link(readyFrom, {{"ydb.compile.relation", TString("resume")}});
            }
        }
        RunningSince = ThreadCPUTime();
        CpuCounted = true;
    }
}

void TCompileTrace::Suspend() {
    if (Running > 0 && --Running == 0) {
        const auto elapsed = ThreadCPUTime() - RunningSince;
        Cpu += elapsed;
        if (ExecutionSpan) {
            ExecutionSpan.Attribute("ydb.cpu_us", static_cast<i64>(elapsed));
            ExecutionSpan.EndOk();
        }
    }
}

NWilson::TSpan* TCompileTrace::BeginRegion(ui8 verbosity, const TString& name, const char* component, const void* group) {
    // Opening a region the group has already opened means the pipeline
    // restarted inside the region that is open now.
    const bool restart = GroupRegions[group].contains(name);
    TString restartedIn;
    for (size_t i = 0; i < Regions.size(); ++i) {
        if (Regions[i].Group != group) {
            continue;
        }
        if (Regions[i].Name == name) {
            return nullptr;
        }
        if (restart) {
            restartedIn = Regions[i].Name;
            // The restart interrupts the nested regions of other groups too.
            for (size_t j = i; j < Regions.size(); ++j) {
                Regions[j].Span.Attribute("ydb.compile.restarted", true);
            }
        }
        EndRegions(Regions.size() - i, true);
        break;
    }
    GroupRegions[group].insert(name);
    const ui32 iteration = NextIteration(name);
    auto span = StartSpan(verbosity, name, component);
    if (!span) {
        return nullptr;
    }
    span.Attribute("ydb.compile.iteration", static_cast<i64>(iteration));
    if (!restartedIn.empty()) {
        span.Attribute("ydb.compile.restart_from", restartedIn);
    }
    Push(span);
    Regions.push_back({name, group, std::move(span), GetCpuTime()});
    return &Regions.back().Span;
}

void TCompileTrace::EndRegion(TStringBuf name) {
    for (size_t i = Regions.size(); i > 0; --i) {
        if (Regions[i - 1].Name == name) {
            EndRegions(Regions.size() - i + 1, true);
            return;
        }
    }
}

void TCompileTrace::EndRegions(size_t count, bool success) {
    for (; count > 0; --count) {
        auto& region = Regions.back();
        Pop(region.Span);
        EndSpan(region.Span, region.CpuStart, success);
        Regions.pop_back();
    }
}

void TCompileTrace::Finish(bool success) {
    if (std::exchange(Finished, true)) {
        return;
    }
    AwaitedDependencies.clear();
    if (Continuation) {
        Continuation->Cancel();
        Continuation.reset();
    }
    EndRegions(Regions.size(), success);
    // Fallback can replace this trace inside an active run scope. Close its
    // execution now, before the replacement trace starts counting the same work.
    if (Running) {
        Running = 1;
        Suspend();
    }
}

TCompileTraceDeferredScope::TCompileTraceDeferredScope(TCompileTrace* trace, TString name, const char* component)
    : Trace(trace && trace->IsEnabled(TCompileTraceLevels::Detail) ? trace : nullptr)
    , Name(std::move(name))
    , Component(component)
{
    if (Trace) {
        // The parent is used only if this scope is emitted, see Finish.
        Parent = Trace->PeekParent();
        Id = Parent.Span(TCompileTraceLevels::Detail);
        Start = Trace->Now();
        CpuStart = Trace->GetCpuTime();
        Trace->PushId(NWilson::TTraceId(Id), Start, GetCycleCount());
    }
}

TCompileTraceDeferredScope::~TCompileTraceDeferredScope() {
    if (Trace) {
        auto span = Finish(true);
        span.EndError("Compilation failed");
    }
}

NWilson::TSpan TCompileTraceDeferredScope::Finish(bool keep) {
    auto* trace = std::exchange(Trace, nullptr);
    if (!trace) {
        return {};
    }
    const auto end = trace->Now();
    CpuTime = trace->GetCpuTime() - CpuStart;
    const bool hasChildren = trace->PopId(Id);
    if (!keep && !hasChildren) {
        return {};
    }
    trace->MarkUsed(Parent);
    auto span = NWilson::TSpan::ConstructTerminated(Parent, Id, Start, end,
        NWilson::NTraceProto::Status::STATUS_CODE_OK, Name);
    if (trace->ActorSystem) {
        span.Attribute("node_id", trace->ActorSystem->NodeId);
    }
    DescribeSpan(span, Component, "scope");
    if (trace->CpuCounted) {
        span.Attribute("ydb.cpu_us", static_cast<i64>(CpuTime));
    }
    return span;
}

TCompileTraceScope::TCompileTraceScope(TCompileTrace* trace, ui8 verbosity, TString name, const char* component)
    : Trace(trace && trace->IsEnabled(verbosity) ? trace : nullptr)
    , UncaughtExceptions(std::uncaught_exceptions())
{
    if (Trace) {
        Span_ = Trace->StartSpan(verbosity, std::move(name), component, "scope");
        Trace->Push(Span_);
        CpuStart = Trace->GetCpuTime();
    }
}

TCompileTraceScope::~TCompileTraceScope() {
    if (!Trace) {
        return;
    }
    Trace->Pop(Span_);
    Trace->EndSpan(Span_, CpuStart, Success && std::uncaught_exceptions() == UncaughtExceptions);
}

TCompileTraceRunScope::TCompileTraceRunScope(TCompileTrace* trace, const NWilson::TTraceId& readyFrom)
    : Trace(trace)
{
    if (Trace) {
        Trace->Resume(readyFrom);
    }
}

TCompileTraceRunScope::~TCompileTraceRunScope() {
    if (Trace) {
        Trace->Suspend();
    }
}

} // namespace NKikimr::NKqp

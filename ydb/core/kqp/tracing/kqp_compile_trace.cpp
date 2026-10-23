#include "kqp_compile_trace.h"

#include <ydb/library/actors/core/actorsystem.h>

#include <util/system/datetime.h>
#include <util/system/guard.h>

#include <algorithm>

namespace NKikimr::NKqp {

TCompileTrace::TCompileTrace(NWilson::TTraceId root, NActors::TActorSystem* actorSystem)
    : Root(std::move(root))
    , ActorSystem(actorSystem)
{}

TCompileTrace::~TCompileTrace() {
    Finish(true);
}

NWilson::TTraceId TCompileTrace::GetCurrentParent() const {
    auto guard = Guard(ParentsLock);
    return NWilson::TTraceId(Parents.empty() ? Root : Parents.back().Id);
}

NWilson::TSpan TCompileTrace::StartSpan(ui8 verbosity, TString name) const {
    if (!IsEnabled(verbosity)) {
        return {};
    }
    return NWilson::TSpan(verbosity, GetCurrentParent(), std::move(name), NWilson::EFlags::NONE, ActorSystem);
}

void TCompileTrace::EndSpan(NWilson::TSpan& span, bool success) {
    if (!span) {
        return;
    }
    if (success) {
        span.EndOk();
    } else {
        span.EndError("Compilation failed");
    }
}

void TCompileTrace::Push(const NWilson::TSpan& span) {
    if (span) {
        auto guard = Guard(ParentsLock);
        Parents.push_back({span.GetTraceId(), TInstant::Now(), GetCycleCount()});
    }
}

void TCompileTrace::Pop(const NWilson::TSpan& span) {
    if (!span) {
        return;
    }
    // Phases stay open across asynchronous waits, so the span is not
    // necessarily the innermost one.
    auto guard = Guard(ParentsLock);
    const auto id = span.GetTraceId();
    const auto it = std::find_if(Parents.rbegin(), Parents.rend(), [&](const TParent& parent) {
        return parent.Id == id;
    });
    if (it != Parents.rend()) {
        Parents.erase(std::next(it).base());
    }
}

TInstant TCompileTrace::Now() const {
    if (Parents.empty()) {
        return TInstant::Now();
    }
    const auto& parent = Parents.back();
    return parent.Start + CyclesToDuration(GetCycleCount() - parent.StartCycles);
}

void TCompileTrace::RecordSpan(ui8 verbosity, TString name, TInstant start) const {
    if (!IsEnabled(verbosity)) {
        return;
    }
    const auto parent = GetCurrentParent();
    auto span = NWilson::TSpan::ConstructTerminated(parent, parent.Span(verbosity), start, Now(),
        NWilson::NTraceProto::Status::STATUS_CODE_OK, std::move(name));
    if (ActorSystem) {
        span.Attribute("node_id", ActorSystem->NodeId);
    }
    span.End();
}

void FinishCompileDependency(NWilson::TSpan& span, bool futureSuccess, bool operationSuccess) {
    if (futureSuccess && operationSuccess) {
        span.EndOk();
    } else {
        span.EndError(futureSuccess ? "Dependency operation failed" : "Dependency future failed");
    }
}

void TCompileTrace::BeginRegion(ui8 verbosity, const TString& name, const void* group) {
    if (Finished) {
        return;
    }
    for (size_t i = 0; i < Regions.size(); ++i) {
        if (Regions[i].Group != group) {
            continue;
        }
        if (Regions[i].Name == name) {
            // Reopened after a restart of the pipeline inside the region.
            return;
        }
        EndRegions(Regions.size() - i, true);
        break;
    }
    auto span = StartSpan(verbosity, name);
    if (!span) {
        return;
    }
    Push(span);
    Regions.push_back({name, group, std::move(span)});
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
        EndSpan(region.Span, success);
        Regions.pop_back();
    }
}

void TCompileTrace::Finish(bool success) {
    if (!std::exchange(Finished, true)) {
        EndRegions(Regions.size(), success);
    }
}

TCompileTraceScope::TCompileTraceScope(TCompileTrace* trace, ui8 verbosity, TString name)
    : Trace(trace && trace->IsEnabled(verbosity) ? trace : nullptr)
    , UncaughtExceptions(std::uncaught_exceptions())
{
    if (Trace) {
        Span = Trace->StartSpan(verbosity, std::move(name));
        Trace->Push(Span);
    }
}

TCompileTraceScope::~TCompileTraceScope() {
    if (!Trace) {
        return;
    }
    Trace->Pop(Span);
    TCompileTrace::EndSpan(Span, Success && std::uncaught_exceptions() == UncaughtExceptions);
}

} // namespace NKikimr::NKqp

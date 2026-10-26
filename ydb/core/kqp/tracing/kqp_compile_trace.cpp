#include "kqp_compile_trace.h"

#include <ydb/library/actors/core/actorsystem.h>

#include <util/system/guard.h>

#include <algorithm>

namespace NKikimr::NKqp {

TCompileTrace::TCompileTrace(NWilson::TTraceId root, NActors::TActorSystem* actorSystem)
    : Root(std::move(root))
    , ActorSystem(actorSystem)
{}

TCompileTrace::~TCompileTrace() {
    Finish(false);
}

NWilson::TTraceId TCompileTrace::GetCurrentParent() const {
    auto guard = Guard(ParentsLock);
    if (!Parents.empty()) {
        Parents.back().HasChildren = true;
    }
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
        Push(span.GetTraceId());
    }
}

void TCompileTrace::Pop(const NWilson::TSpan& span) {
    if (span) {
        Pop(span.GetTraceId());
    }
}

void TCompileTrace::Push(const NWilson::TTraceId& id) {
    auto guard = Guard(ParentsLock);
    Parents.push_back({NWilson::TTraceId(id)});
}

bool TCompileTrace::Pop(const NWilson::TTraceId& id) {
    // Regions stay open across asynchronous waits, so the span is not
    // necessarily the innermost one.
    auto guard = Guard(ParentsLock);
    const auto it = std::find_if(Parents.rbegin(), Parents.rend(), [&](const TParent& parent) {
        return parent.Id == id;
    });
    if (it != Parents.rend()) {
        const bool hasChildren = it->HasChildren;
        Parents.erase(std::next(it).base());
        return hasChildren;
    }
    return false;
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

void TCompileTrace::EndRegion(TStringBuf name, const void* group, bool success) {
    for (size_t i = Regions.size(); i > 0; --i) {
        if (Regions[i - 1].Group == group && Regions[i - 1].Name == name) {
            EndRegions(Regions.size() - i + 1, success);
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

TCompileTraceScope::TCompileTraceScope(TCompileTrace* trace, ui8 verbosity, TStringBuf name, TStringBuf suffix)
    : Trace(trace && trace->IsEnabled(verbosity) ? trace : nullptr)
    , UncaughtExceptions(std::uncaught_exceptions())
{
    if (Trace) {
        Span = Trace->StartSpan(verbosity, TString::Join(name, suffix));
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

TCompileTraceRuleScope::TCompileTraceRuleScope(TCompileTrace* trace, const TString& name)
    : Trace(trace && trace->IsEnabled(TCompileTraceLevels::Detail) ? trace : nullptr)
    , Name(Trace ? name : TString())
    , UncaughtExceptions(std::uncaught_exceptions())
{
    if (Trace) {
        Parent = Trace->GetCurrentParent();
        Id = Parent.Span(TCompileTraceLevels::Detail);
        // The span is emitted later: sample the clock of the spans around it.
        StartUnixNano = NWilson::TSpan::NowUnixNano();
        Trace->Push(Id);
    }
}

TCompileTraceRuleScope::~TCompileTraceRuleScope() {
    if (!Trace) {
        return;
    }
    const auto endUnixNano = NWilson::TSpan::NowUnixNano();
    const bool hasChildren = Trace->Pop(Id);
    const bool success = std::uncaught_exceptions() == UncaughtExceptions;
    if (Applied || hasChildren || !success) {
        auto span = NWilson::TSpan::ConstructTerminated(Parent, Id, StartUnixNano, endUnixNano,
            success ? NWilson::NTraceProto::Status::STATUS_CODE_OK : NWilson::NTraceProto::Status::STATUS_CODE_ERROR,
            "Rule: " + Name + (Applied || !success ? "" : " (not applied)"), Trace->ActorSystem);
        if (Trace->ActorSystem) {
            span.Attribute("node_id", Trace->ActorSystem->NodeId);
        }
        span.End();
    }
}

} // namespace NKikimr::NKqp

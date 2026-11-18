#include "decode_observer.h"
#include "processor_stats_adapter.h"
#include "program_observer.h"

#include <ydb/core/formats/arrow/program/collection.h>
#include <ydb/core/formats/arrow/program/graph_execute.h>

#include <util/generic/hash_set.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NKikimr::NOlap::NStats::NScan {
namespace {

std::optional<ui64> KnownRows(const NArrow::NSSA::TAccessorsCollection& resources) {
    if (!resources.GetFilterUsage() && !resources.GetFilter().IsTotalAllowFilter()) {
        const auto rows = resources.GetFilter().GetFilteredCountIfKnown();
        return rows ? std::optional<ui64>(*rows) : std::nullopt;
    }
    const auto rows = resources.GetRecordsCountActualOptional();
    return rows ? std::optional<ui64>(*rows) : std::nullopt;
}

void Number(NColumnShardStats::TNumericObservation& observation, std::optional<ui64> value) {
    observation.SetApplicability(NColumnShardStats::APPLICABLE);
    auto& coverage = *observation.MutableCoverage();
    coverage.SetApplicable(1);
    coverage.SetMeasured(value ? 1 : 0);
    if (value) {
        observation.SetValue(*value);
        coverage.ClearMissingReason();
    } else {
        observation.ClearValue();
        coverage.SetMissingReason(NColumnShardStats::NOT_REPORTED);
    }
}

}   // namespace

TProgramObserver::TProgramObserver(std::unique_ptr<TLocalStats> localStats, THashMap<ui32, TString> columnNames,
    THashMap<ui32, TString> expressions, THashMap<ui64, EProgramPhase> phases, THashMap<ui64, ui64> nodeOrder)
    : LocalStats(std::move(localStats))
    , DecodeObserver(LocalStats ? std::make_unique<TDecodeObserver>(*LocalStats) : nullptr)
    , ColumnNames(std::move(columnNames))
    , Expressions(std::move(expressions))
    , Phases(std::move(phases))
    , NodeOrder(std::move(nodeOrder))
{
    RefreshCurrentNode();
}

TProgramObserver::~TProgramObserver() {
    LocalStats.reset();
}

NArrow::NAccessor::IDecodeObserver* TProgramObserver::GetDecodeObserver() const noexcept {
    return DecodeObserver.get();
}

TCallHandle TProgramObserver::GetCurrentCallHandle() const noexcept {
    TCallHandle phaseCall;
    if (CurrentPhase) {
        const auto phase = PhaseScopes.find(*CurrentPhase);
        if (phase != PhaseScopes.end() && phase->second.Scope) {
            phaseCall = phase->second.Scope->GetHandle();
        }
    }
    for (auto frame = ActiveFrames.rbegin(); frame != ActiveFrames.rend(); ++frame) {
        if ((*frame)->Scope && !(*frame)->Finished) {
            const auto call = (*frame)->Scope->GetHandle();
            for (auto ancestor = phaseCall; ancestor; ancestor = ancestor.GetParent()) {
                if (ancestor == call) {
                    return phaseCall;
                }
            }
            return call;
        }
    }
    return phaseCall ? phaseCall : ParentCall;
}

TProgramObserver::TFrame* TProgramObserver::FindFrame(ui64 nodeId) noexcept {
    const auto it = Frames.find(nodeId);
    return it == Frames.end() ? nullptr : it->second.get();
}

EProgramPhase TProgramObserver::PhaseFor(ui64 nodeId, const NArrow::NSSA::IResourceProcessor& processor) const {
    using NArrow::NSSA::EProcessorType;
    EProgramPhase phase = EProgramPhase::FetchColumns;
    if (const auto it = Phases.find(nodeId); it != Phases.end()) {
        phase = it->second;
    } else {
        switch (TProcessorStatsAdapter(processor).GetObservedType()) {
            case EProcessorType::Filter:
                phase = EProgramPhase::Filter;
                break;
            case EProcessorType::Aggregation:
                phase = EProgramPhase::Aggregation;
                break;
            case EProcessorType::Projection:
                phase = EProgramPhase::Projection;
                break;
            case EProcessorType::CheckIndexData:
            case EProcessorType::CheckHeaderData:
                phase = EProgramPhase::IndexCheck;
                break;
            default:
                break;
        }
    }
    return phase;
}

TOperationNode* TProgramObserver::FindPhaseNode(EProgramPhase phase) const noexcept {
    if (!LocalStats) {
        return nullptr;
    }
    TStringBuf key;
    switch (phase) {
        case EProgramPhase::Filter:
            key = "phase/Filter";
            break;
        case EProgramPhase::Aggregation:
            key = "phase/Aggregation";
            break;
        case EProgramPhase::Projection:
            key = "phase/Projection";
            break;
        case EProgramPhase::FetchColumns:
            key = "phase/FetchColumns";
            break;
        case EProgramPhase::IndexCheck:
            return &LocalStats->GetRoot();
    }
    return LocalStats->GetRoot().FindChild(key);
}

TOperationNode* TProgramObserver::PhaseNode(EProgramPhase phase) {
    // Physical index groups belong to the provider that knows the index identity.
    if (phase == EProgramPhase::IndexCheck) {
        return &LocalStats->GetRoot();
    }
    if (auto* node = FindPhaseNode(phase)) {
        return node;
    }
    NColumnShardStats::TOperation definition;
    switch (phase) {
        case EProgramPhase::Filter:
            definition.SetName("Filter");
            definition.MutableFilter()->SetKind(NColumnShardStats::PREDICATE);
            break;
        case EProgramPhase::Aggregation:
            definition.SetName("Aggregation");
            definition.MutableAggregation();
            break;
        case EProgramPhase::Projection:
            definition.SetName("Projection");
            definition.MutableProjection();
            break;
        case EProgramPhase::FetchColumns:
            definition.SetName("FetchColumns");
            definition.MutableFetchColumns();
            break;
        case EProgramPhase::IndexCheck:
            return &LocalStats->GetRoot();
    }
    definition.SetKey(TStringBuilder() << "phase/" << definition.GetName());
    return LocalStats->AddChild(LocalStats->GetRoot(), definition);
}

TOperationNode* TProgramObserver::FrameNode(const TFrame& frame) const {
    auto* phase = FindPhaseNode(frame.Phase);
    if (!phase || !frame.MeasureExecution) {
        return phase;
    }
    return phase->FindChild(TStringBuilder() << "ssa/" << frame.NodeId);
}

void TProgramObserver::FinishPhase(EProgramPhase phase) {
    const auto it = PhaseScopes.find(phase);
    if (it == PhaseScopes.end() || !it->second.Scope) {
        return;
    }
    auto& state = it->second;
    state.Scope->Observe([&](auto& observation) {
        Number(observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableOutputRows), state.LastOutputRows);
        Number(observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableOutputBytes), std::nullopt);
    });
    state.Scope->Finish();
    state.Scope.reset();
    state.PendingCalls = 0;
    state.LastOutputRows.reset();
}

void TProgramObserver::ActivatePhase(TFrame& frame, const NArrow::NSSA::TAccessorsCollection& resources) {
    if (CurrentPhase && *CurrentPhase != frame.Phase) {
        const auto previous = PhaseScopes.find(*CurrentPhase);
        if (previous != PhaseScopes.end() && previous->second.Scope && !previous->second.PendingCalls) {
            FinishPhase(*CurrentPhase);
        }
    }
    CurrentPhase.reset();
    if (frame.Phase == EProgramPhase::IndexCheck) {
        return;
    }
    auto* node = FindPhaseNode(frame.Phase);
    if (!node) {
        return;
    }
    auto& state = PhaseScopes[frame.Phase];
    if (!state.Scope) {
        state.Scope = std::make_shared<TOperationScope>(LocalStats->Measure(node, GetCurrentCallHandle()));
        state.Scope->Observe([&](auto& observation) {
            Number(observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableInputRows), KnownRows(resources));
            Number(observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableInputBytes), std::nullopt);
        });
    }
    state.Scope->Resume();
    CurrentPhase = frame.Phase;
}

void TProgramObserver::FinishObservedExecution() {
    for (auto& [phase, state] : PhaseScopes) {
        Y_UNUSED(state);
        FinishPhase(phase);
    }
    CurrentPhase.reset();
}

TOperationNode* TProgramObserver::GetCurrentNode() const {
    for (auto it = ActiveFrames.rbegin(); it != ActiveFrames.rend(); ++it) {
        if (auto* node = FrameNode(**it)) {
            return node;
        }
    }
    return LocalStats ? &LocalStats->GetRoot() : nullptr;
}

void TProgramObserver::RefreshCurrentNode() {
    if (DecodeObserver) {
        DecodeObserver->SetParent(GetCurrentNode());
        if (ActiveFrames.empty()) {
            DecodeObserver->SetParentCall(GetCurrentCallHandle());
        }
    }
}

void TProgramObserver::OnEnter(ui64 nodeId, const NArrow::NSSA::IResourceProcessor& processor, const NArrow::NSSA::TAccessorsCollection&) {
    if (!LocalStats) {
        return;
    }
    for (auto it = ActiveFrames.rbegin(); it != ActiveFrames.rend(); ++it) {
        if ((*it)->Scope && !(*it)->Finished) {
            (*it)->Scope->Suspend();
            break;
        }
    }
    auto* frame = FindFrame(nodeId);
    if (!frame) {
        auto owned = std::make_unique<TFrame>();
        owned->NodeId = nodeId;
        owned->Phase = PhaseFor(nodeId, processor);
        auto* phase = PhaseNode(owned->Phase);
        auto* node = phase ? phase->FindChild(TStringBuilder() << "ssa/" << nodeId) : nullptr;
        if (!node) {
            if (auto definition = TProcessorStatsAdapter(processor).DescribeOperation(nodeId, ColumnNames, Expressions)) {
                const auto order = NodeOrder.find(nodeId);
                node = phase
                           ? LocalStats->AddChild(*phase, *definition, order == NodeOrder.end() ? std::optional<ui64>{ nodeId } : order->second)
                           : nullptr;
            }
        }
        owned->MeasureExecution = node != nullptr;
        frame = owned.get();
        Frames.emplace(nodeId, std::move(owned));
    }
    ActiveFrames.emplace_back(frame);
    RefreshCurrentNode();
}

void TProgramObserver::OnExecute(ui64 nodeId, const NArrow::NSSA::TAccessorsCollection& resources) {
    if (DecodeObserver) {
        DecodeObserver->SetParent(GetCurrentNode());
        DecodeObserver->SetParentCall(GetCurrentCallHandle());
    }
    auto* frame = FindFrame(nodeId);
    if (frame) {
        ActivatePhase(*frame, resources);
    }
    if (frame && frame->MeasureExecution && !frame->Finished) {
        if (!frame->Scope) {
            frame->Scope =
                std::make_shared<TOperationScope>(LocalStats->Measure(FrameNode(*frame), GetCurrentCallHandle(), EChildWorkAccounting::Exclude));
        }
        frame->Scope->Resume();
        if (!frame->InputObserved) {
            const auto rows = KnownRows(resources);
            frame->Scope->Observe([&](auto& observation) {
                Number(observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableInputRows), rows);
                Number(observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableInputBytes), std::nullopt);
            });
            frame->InputObserved = rows.has_value();
        }
    }
    RefreshCurrentNode();
}

void TProgramObserver::OnResult(
    ui64 nodeId, const NArrow::NSSA::TAccessorsCollection& resources, const TConclusion<NArrow::NSSA::TExecutionResult>& result) {
    auto* frame = FindFrame(nodeId);
    if (frame) {
        if (const auto phase = PhaseScopes.find(frame->Phase); phase != PhaseScopes.end() && phase->second.Scope) {
            auto& state = phase->second;
            const bool pending = result.IsSuccess() && result->IsPending();
            if (pending != frame->Pending) {
                if (pending) {
                    ++state.PendingCalls;
                } else if (state.PendingCalls) {
                    --state.PendingCalls;
                }
                frame->Pending = pending;
            }
            if (!pending) {
                state.LastOutputRows = result.IsSuccess() ? KnownRows(resources) : std::nullopt;
            }
            state.Scope->Suspend();
        }
    }
    if (!frame || !frame->Scope || frame->Finished) {
        return;
    }
    if (result.IsSuccess() && result->IsPending()) {
        frame->Scope->Suspend();
        return;
    }
    const auto rows = result.IsSuccess() ? KnownRows(resources) : std::nullopt;
    frame->Scope->Observe([&](auto& observation) {
        Number(observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableOutputRows), rows);
        Number(observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableOutputBytes), std::nullopt);
    });
    frame->Scope->Finish();
    frame->Finished = true;
}

void TProgramObserver::OnExit(ui64 nodeId, const NArrow::NSSA::TAccessorsCollection&, bool) {
    if (ActiveFrames.empty() || ActiveFrames.back()->NodeId != nodeId) {
        return;
    }
    const bool finished = ActiveFrames.back()->Finished || (!ActiveFrames.back()->Scope && !ActiveFrames.back()->Pending);
    ActiveFrames.pop_back();
    if (finished) {
        Frames.erase(nodeId);
    }
    RefreshCurrentNode();
}

void TProgramObserver::OnException(ui64 nodeId) {
    if (auto* frame = FindFrame(nodeId)) {
        if (const auto phase = PhaseScopes.find(frame->Phase); phase != PhaseScopes.end() && phase->second.Scope) {
            phase->second.Scope->Suspend();
            phase->second.LastOutputRows.reset();
        }
    }
    if (auto* frame = FindFrame(nodeId); frame && frame->Scope && !frame->Finished) {
        frame->Scope->Finish();
        frame->Finished = true;
    }
}

THashMap<ui64, EProgramPhase> TProgramObserver::DescribePhases(const NArrow::NSSA::NGraph::NExecution::TCompiledGraph& graph) {
    constexpr ui8 filter = 1;
    constexpr ui8 aggregation = 2;
    constexpr ui8 index = 4;
    constexpr ui8 projection = 8;
    const auto boundary = [&](const NArrow::NSSA::IResourceProcessor& processor) -> ui8 {
        switch (TProcessorStatsAdapter(processor).GetObservedType()) {
            case NArrow::NSSA::EProcessorType::Filter:
                return filter;
            case NArrow::NSSA::EProcessorType::Aggregation:
                return aggregation;
            case NArrow::NSSA::EProcessorType::Projection:
                return projection;
            case NArrow::NSSA::EProcessorType::CheckIndexData:
            case NArrow::NSSA::EProcessorType::CheckHeaderData:
                return index;
            default:
                return 0;
        }
    };
    THashMap<ui64, size_t> remaining;
    THashMap<ui64, ui8> consumers;
    std::vector<const NArrow::NSSA::NGraph::NExecution::TCompiledGraph::TNode*> ready;
    for (const auto& [id, node] : graph.GetNodes()) {
        size_t outputs = 0;
        for (const auto* output : node->GetOutputEdges()) {
            outputs += graph.GetNodes().contains(output->GetIdentifier());
        }
        remaining.emplace(id, outputs);
        consumers.emplace(id, boundary(*node->GetProcessor()));
        if (!outputs) {
            ready.emplace_back(node.get());
        }
    }
    THashMap<ui64, EProgramPhase> phases;
    while (!ready.empty()) {
        const auto* node = ready.back();
        ready.pop_back();
        const ui64 id = node->GetIdentifier();
        const ui8 own = boundary(*node->GetProcessor());
        const ui8 mask = own ? own : consumers.at(id);
        const auto phase = own == projection     ? EProgramPhase::Projection
                           : mask & filter       ? EProgramPhase::Filter
                           : mask & index        ? EProgramPhase::IndexCheck
                           : mask == aggregation ? EProgramPhase::Aggregation
                                                 : EProgramPhase::FetchColumns;
        phases.emplace(id, phase);
        for (const auto* input : node->GetInputEdges()) {
            const ui64 inputId = input->GetIdentifier();
            if (!graph.GetNodes().contains(inputId)) {
                continue;
            }
            consumers.at(inputId) |= mask;
            if (!--remaining.at(inputId)) {
                ready.emplace_back(input);
            }
        }
    }
    return phases;
}

std::shared_ptr<TProgramObserver> TProgramObserver::Create(std::unique_ptr<TLocalStats> localStats,
    const NArrow::NSSA::NGraph::NExecution::TCompiledGraph& graph, const NArrow::NSSA::IColumnResolver& resolver) {
    if (!localStats) {
        return nullptr;
    }
    THashMap<ui32, TString> names;
    for (const auto& [id, node] : graph.GetNodes()) {
        Y_UNUSED(id);
        for (const auto& columns : { node->GetProcessor()->GetInput(), node->GetProcessor()->GetOutput() }) {
            for (const auto& column : columns) {
                const ui32 columnId = column.GetColumnId();
                if (!names.contains(columnId)) {
                    auto name = resolver.GetColumnName(columnId, false);
                    if (name.empty()) {
                        name = TStringBuilder() << "ssa_" << columnId;
                    }
                    names.emplace(columnId, std::move(name));
                }
            }
        }
    }
    auto expressions = names;
    THashMap<ui64, ui64> nodeOrder;
    THashSet<ui64> visited;
    std::function<void(const NArrow::NSSA::NGraph::NExecution::TCompiledGraph::TNode&)> describe;
    describe = [&](const auto& node) {
        if (!visited.emplace(node.GetIdentifier()).second) {
            return;
        }
        for (const auto& input : node.GetInputEdges()) {
            describe(*input);
        }
        nodeOrder.emplace(node.GetIdentifier(), nodeOrder.size());
        for (auto&& [columnId, expression] : TProcessorStatsAdapter(*node.GetProcessor()).DescribeExpressions(names)) {
            if (node.GetProcessor()->GetProcessorType() == NArrow::NSSA::EProcessorType::Const) {
                names[columnId] = expression;
            }
            expressions[columnId] = std::move(expression);
        }
    };
    for (const auto& [id, node] : graph.GetNodes()) {
        Y_UNUSED(id);
        describe(*node);
    }
    return std::make_shared<TProgramObserver>(
        std::move(localStats), std::move(names), std::move(expressions), DescribePhases(graph), std::move(nodeOrder));
}

}   // namespace NKikimr::NOlap::NStats::NScan

#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

#include <optional>

namespace NKikimr {
namespace NKqp {

// Main shape this handles:
// A: Map [ to <- from ] == becomes ==>  Producer [ from renamed to to ]
// B: `- Producer
//
// Also handles a dead-source append alias as a producer rename:
// A: Map [ to := from ] == becomes ==>  Producer [ from renamed to to ]
// B: `- Producer
//
// Where Producer is Read, Map output, or Aggregate result. This is intentionally
// separate from rename-to-append: these cases need the source name hidden because
// it is live-forbidden above A or dead after A.

namespace {

using TRenameMap = THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>;

struct TRenameCandidate {
    size_t Index = 0;
    TInfoUnit From;
    TInfoUnit To;
};

bool CanRewriteResidualTopMap(
    const TIntrusivePtr<TOpMap>& topMap,
    size_t renameIdx,
    const TInfoUnit& from,
    const TInfoUnit& to)
{
    for (size_t idx = 0; idx < topMap->MapElements.size(); ++idx) {
        if (idx == renameIdx) {
            continue;
        }

        const auto& element = topMap->MapElements[idx];
        // A residual semantic rename from either name would hide the pushed output.
        if (element.IsRename() && (element.GetRename() == from || element.GetRename() == to)) {
            return false;
        }
    }

    return true;
}

bool RenameNeedsPush(const TIntrusivePtr<TOpMap>& topMap, const TMapElement& element, const TInfoUnitSet& liveOut) {
    return liveOut.contains(element.GetElementName()) ||
        GetForbidden(topMap.get()).contains(element.GetRename());
}

bool TryBuildRenameCandidate(
    const TIntrusivePtr<TOpMap>& topMap,
    size_t idx,
    const TInfoUnitSet& liveOut,
    TRenameCandidate& candidate)
{
    const auto& element = topMap->MapElements[idx];
    candidate.Index = idx;
    candidate.To = element.GetElementName();

    if (element.IsRename()) {
        candidate.From = element.GetRename();
        return RenameNeedsPush(topMap, element, liveOut);
    }

    if (!element.IsColumnAccess()) {
        return false;
    }

    candidate.From = element.GetColumnAccess();
    return liveOut.contains(candidate.To) && !liveOut.contains(candidate.From);
}

std::optional<TRenameCandidate> FindRenameCandidate(const TIntrusivePtr<TOpMap>& topMap) {
    const auto& liveOut = GetLiveOut(topMap.get());

    for (size_t idx = 0; idx < topMap->MapElements.size(); ++idx) {
        TRenameCandidate candidate;
        if (TryBuildRenameCandidate(topMap, idx, liveOut, candidate) && candidate.From != candidate.To) {
            return candidate;
        }
    }

    return std::nullopt;
}

TVector<TMapElement> BuildResidualTopMapElements(
    const TIntrusivePtr<TOpMap>& topMap,
    const TRenameCandidate& candidate)
{
    const TRenameMap renameMap{{candidate.From, candidate.To}};

    TVector<TMapElement> residualElements;
    residualElements.reserve(topMap->MapElements.size() - 1);
    for (size_t idx = 0; idx < topMap->MapElements.size(); ++idx) {
        if (idx == candidate.Index) {
            continue;
        }

        auto element = topMap->MapElements[idx];
        if (!element.IsRename()) {
            element.SetExpression(element.GetExpression().ApplyRenames(renameMap));
        }
        residualElements.push_back(std::move(element));
    }

    return residualElements;
}

bool ProducesAggregateResult(const TIntrusivePtr<TOpAggregate>& aggregate, const TInfoUnit& iu) {
    for (const auto& traits : aggregate->AggregationTraitsList) {
        if (traits.ResultColName == iu) {
            return true;
        }
    }
    return false;
}

bool TryRenameReadOutput(const TIntrusivePtr<TOpRead>& read, const TRenameCandidate& candidate) {
    if (!read->IsSingleConsumer()) {
        return false;
    }

    for (auto& output : read->OutputIUs) {
        if (output == candidate.From) {
            output = candidate.To;
            return true;
        }
    }

    return false;
}

bool TryRenameMapOutput(const TIntrusivePtr<TOpMap>& map, const TRenameCandidate& candidate, TExprContext& ctx) {
    auto* outputElement = map->FindOutputElement(candidate.From);
    if (!map->IsSingleConsumer() || !outputElement) {
        return false;
    }

    // Do not turn `from := to` into `to := to` inside the same map. The target
    // name may be hidden there by another semantic rename.
    if (!outputElement->IsRename() &&
        outputElement->IsColumnAccess() &&
        outputElement->GetColumnAccess() == candidate.To) {
        return false;
    }

    map->RenameProducedIUs({{candidate.From, candidate.To}}, ctx);
    return true;
}

bool TryRenameAggregateResult(const TIntrusivePtr<TOpAggregate>& aggregate, const TRenameCandidate& candidate, TExprContext& ctx) {
    if (!aggregate->IsSingleConsumer() || !ProducesAggregateResult(aggregate, candidate.From)) {
        return false;
    }

    aggregate->RenameProducedIUs({{candidate.From, candidate.To}}, ctx);
    return true;
}

bool TryRenameProducerOutput(
    const TIntrusivePtr<IOperator>& producer,
    const TRenameCandidate& candidate,
    TExprContext& ctx)
{
    switch (producer->Kind) {
        case EOperator::Source:
            return TryRenameReadOutput(CastOperator<TOpRead>(producer), candidate);
        case EOperator::Map:
            return TryRenameMapOutput(CastOperator<TOpMap>(producer), candidate, ctx);
        case EOperator::Aggregate:
            return TryRenameAggregateResult(CastOperator<TOpAggregate>(producer), candidate, ctx);
        default:
            return false;
    }
}

void FinishRenamePush(
    TIntrusivePtr<IOperator>& input,
    const TIntrusivePtr<TOpMap>& topMap,
    const TRenameCandidate& candidate,
    TRBOContext& ctx,
    TPlanProps& props)
{
    topMap->MapElements = BuildResidualTopMapElements(topMap, candidate);
    props.Subplans.RenameReferences({{candidate.From, candidate.To}}, ctx.ExprCtx);

    if (topMap->MapElements.empty()) {
        input = topMap->GetInput();
    }
}

} // anonymous namespace

bool TPushRenameIntoProducerRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (input->Kind != EOperator::Map) {
        return false;
    }

    auto topMap = CastOperator<TOpMap>(input);
    const auto candidate = FindRenameCandidate(topMap);
    if (!candidate ||
        !topMap->IsSingleConsumer() ||
        !CanRewriteResidualTopMap(topMap, candidate->Index, candidate->From, candidate->To)) {
        return false;
    }

    if (!TryRenameProducerOutput(topMap->GetInput(), *candidate, ctx.ExprCtx)) {
        return false;
    }

    FinishRenamePush(input, topMap, *candidate, ctx, props);
    return true;
}

} // namespace NKqp
} // namespace NKikimr

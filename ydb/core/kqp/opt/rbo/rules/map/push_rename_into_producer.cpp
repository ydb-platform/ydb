#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

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

bool CanRewriteResidualTopMap(const TIntrusivePtr<TOpMap>& topMap, size_t renameIdx, const TInfoUnit& from, const TInfoUnit& to) {
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

TVector<TMapElement> BuildResidualTopMapElements(const TIntrusivePtr<TOpMap>& topMap, size_t renameIdx, const TInfoUnit& from, const TInfoUnit& to) {
    const TRenameMap renameMap{{from, to}};

    TVector<TMapElement> residualElements;
    residualElements.reserve(topMap->MapElements.size() - 1);
    for (size_t idx = 0; idx < topMap->MapElements.size(); ++idx) {
        if (idx == renameIdx) {
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

bool TryRenameReadOutput(const TIntrusivePtr<TOpRead>& read, const TInfoUnit& from, const TInfoUnit& to) {
    if (!read->IsSingleConsumer()) {
        return false;
    }

    for (auto& output : read->OutputIUs) {
        if (output == from) {
            output = to;
            return true;
        }
    }

    return false;
}

bool TryRenameMapOutput(const TIntrusivePtr<TOpMap>& map, const TInfoUnit& from, const TInfoUnit& to, TExprContext& ctx) {
    auto* outputElement = map->FindOutputElement(from);
    if (!map->IsSingleConsumer() || !outputElement) {
        return false;
    }

    // Do not turn `from := to` into `to := to` inside the same map. The target
    // name may be hidden there by another semantic rename.
    if (!outputElement->IsRename() &&
        outputElement->IsColumnAccess() &&
        outputElement->GetColumnAccess() == to) {
        return false;
    }

    map->RenameProducedIUs({{from, to}}, ctx);
    return true;
}

bool TryRenameAggregateResult(const TIntrusivePtr<TOpAggregate>& aggregate, const TInfoUnit& from, const TInfoUnit& to, TExprContext& ctx) {
    if (!aggregate->IsSingleConsumer()) {
        return false;
    }

    bool producesResult = false;
    for (const auto& traits : aggregate->AggregationTraitsList) {
        producesResult |= traits.ResultColName == from;
    }
    if (!producesResult) {
        return false;
    }

    aggregate->RenameProducedIUs({{from, to}}, ctx);
    return true;
}

bool TryRenameProducerOutput(const TIntrusivePtr<IOperator>& producer, const TInfoUnit& from, const TInfoUnit& to, TExprContext& ctx) {
    switch (producer->Kind) {
        case EOperator::Source:
            return TryRenameReadOutput(CastOperator<TOpRead>(producer), from, to);
        case EOperator::Map:
            return TryRenameMapOutput(CastOperator<TOpMap>(producer), from, to, ctx);
        case EOperator::Aggregate:
            return TryRenameAggregateResult(CastOperator<TOpAggregate>(producer), from, to, ctx);
        default:
            return false;
    }
}

} // anonymous namespace

bool TPushRenameIntoProducerRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Map;
}

bool TPushRenameIntoProducerRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (input->Kind != EOperator::Map) {
        return false;
    }

    auto topMap = CastOperator<TOpMap>(input);
    if (!topMap->IsSingleConsumer()) {
        return false;
    }

    const auto& liveOut = GetLiveOut(topMap.get());
    const auto& forbidden = GetForbidden(topMap.get());

    for (size_t idx = 0; idx < topMap->MapElements.size(); ++idx) {
        const auto& element = topMap->MapElements[idx];
        const auto to = element.GetElementName();
        TInfoUnit from;

        if (element.IsRename()) {
            from = element.GetRename();
            if (!liveOut.contains(to) && !forbidden.contains(from)) {
                continue;
            }
        } else {
            if (!element.IsColumnAccess()) {
                continue;
            }

            from = element.GetColumnAccess();
            if (!liveOut.contains(to) || liveOut.contains(from)) {
                continue;
            }
        }

        if (from == to ||
            !CanRewriteResidualTopMap(topMap, idx, from, to) ||
            !TryRenameProducerOutput(topMap->GetInput(), from, to, ctx.ExprCtx)) {
            continue;
        }

        topMap->MapElements = BuildResidualTopMapElements(topMap, idx, from, to);
        props.Subplans.RenameReferences({{from, to}}, ctx.ExprCtx);
        if (topMap->MapElements.empty()) {
            input = topMap->GetInput();
        }
        return true;
    }

    return false;
}

} // namespace NKqp
} // namespace NKikimr

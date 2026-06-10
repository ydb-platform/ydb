#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

namespace NKikimr {
namespace NKqp {

namespace {

bool CanRewriteResidualTopMap(
    const TIntrusivePtr<TOpMap>& topMap,
    size_t appendIdx,
    const TInfoUnit& from,
    const TInfoUnit& to)
{
    for (size_t idx = 0; idx < topMap->MapElements.size(); ++idx) {
        if (idx == appendIdx) {
            continue;
        }

        const auto& element = topMap->MapElements[idx];
        if (element.GetElementName() == to) {
            return false;
        }
        if (element.IsRename() && (element.GetRename() == from || element.GetRename() == to)) {
            return false;
        }
    }

    return true;
}

void RewriteResidualTopMapInputs(TVector<TMapElement>& elements, const TInfoUnit& from, const TInfoUnit& to) {
    THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> renameMap{{from, to}};

    for (auto& element : elements) {
        if (!element.IsRename()) {
            element.SetExpression(element.GetExpression().ApplyRenames(renameMap));
        }
    }
}

void RenameAggregateKey(TOpAggregate& aggregate, const TInfoUnit& from, const TInfoUnit& to) {
    for (auto& key : aggregate.KeyColumns) {
        if (key == from) {
            key = to;
        }
    }

    if (!aggregate.IsDistinctAll()) {
        return;
    }

    for (auto& traits : aggregate.AggregationTraitsList) {
        if (traits.OriginalColName == from) {
            traits.OriginalColName = to;
        }
        if (traits.ResultColName == from) {
            traits.ResultColName = to;
        }
    }
}

} // anonymous namespace

TIntrusivePtr<IOperator> TPushAppendThroughAggregateRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);

    if (input->Kind != EOperator::Map) {
        return input;
    }

    auto topMap = CastOperator<TOpMap>(input);
    if (topMap->GetInput()->Kind != EOperator::Aggregate) {
        return input;
    }

    auto aggregate = CastOperator<TOpAggregate>(topMap->GetInput());
    if (!aggregate->IsSingleConsumer()) {
        return input;
    }

    for (size_t appendIdx = 0; appendIdx < topMap->MapElements.size(); ++appendIdx) {
        const auto& mapElement = topMap->MapElements[appendIdx];
        if (!topMap->IsExtractableAppend(mapElement) || !mapElement.IsColumnAccess()) {
            continue;
        }

        const auto from = mapElement.GetColumnAccess();
        const auto to = mapElement.GetElementName();
        const auto liveIt = props.LiveOut.find(topMap.get());
        if (liveIt == props.LiveOut.end() || !liveIt->second.contains(to) || liveIt->second.contains(from)) {
            continue;
        }

        if (!ContainsInfoUnit(aggregate->KeyColumns, from) ||
            !CanRewriteResidualTopMap(topMap, appendIdx, from, to)) {
            continue;
        }

        const auto oldAggregateInput = aggregate->GetInput();
        const auto oldKeys = aggregate->KeyColumns;
        const auto oldTraits = aggregate->AggregationTraitsList;
        const auto aggregateInputIUs = oldAggregateInput->GetOutputIUs();
        if (!mapElement.DependsOnlyOn(aggregateInputIUs) || ContainsInfoUnit(aggregateInputIUs, to)) {
            continue;
        }

        auto pushedMap = MakeIntrusive<TOpMap>(oldAggregateInput, topMap->Pos, TVector<TMapElement>{mapElement});
        if (HasOutputConflicts(pushedMap->GetOutputIUs())) {
            continue;
        }

        aggregate->SetInput(pushedMap);
        RenameAggregateKey(*aggregate, from, to);
        if (HasOutputConflicts(aggregate->GetOutputIUs())) {
            aggregate->SetInput(oldAggregateInput);
            aggregate->KeyColumns = oldKeys;
            aggregate->AggregationTraitsList = oldTraits;
            continue;
        }

        TVector<TMapElement> topElements;
        topElements.reserve(topMap->MapElements.size() - 1);
        for (size_t idx = 0; idx < topMap->MapElements.size(); ++idx) {
            if (idx != appendIdx) {
                topElements.push_back(topMap->MapElements[idx]);
            }
        }
        RewriteResidualTopMapInputs(topElements, from, to);

        if (topElements.empty()) {
            if (!CanReplaceInParents(topMap, aggregate, props)) {
                aggregate->SetInput(oldAggregateInput);
                aggregate->KeyColumns = oldKeys;
                aggregate->AggregationTraitsList = oldTraits;
                return input;
            }
            return aggregate;
        }

        auto newTopMap = MakeIntrusive<TOpMap>(aggregate, topMap->Pos, topElements, topMap->Ordered);
        if (!CanExposeOutput(topMap, newTopMap->GetOutputIUs(), props)) {
            aggregate->SetInput(oldAggregateInput);
            aggregate->KeyColumns = oldKeys;
            aggregate->AggregationTraitsList = oldTraits;
            return input;
        }

        return newTopMap;
    }

    return input;
}

} // namespace NKqp
} // namespace NKikimr

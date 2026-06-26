#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>
#include <ydb/core/kqp/opt/rbo/rules/map/projection_pruning_helpers.h>

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

void RenameAggregateKey(
    TVector<TInfoUnit>& keyColumns,
    TVector<TOpAggregationTraits>& traitsList,
    bool distinctAll,
    const TInfoUnit& from,
    const TInfoUnit& to)
{
    for (auto& key : keyColumns) {
        if (key == from) {
            key = to;
        }
    }

    if (!distinctAll) {
        return;
    }

    for (auto& traits : traitsList) {
        if (traits.OriginalColName == from) {
            traits.OriginalColName = to;
        }
        if (traits.ResultColName == from) {
            traits.ResultColName = to;
        }
    }
}

TVector<TInfoUnit> BuildAggregateOutput(
    const TVector<TInfoUnit>& keyColumns,
    const TVector<TOpAggregationTraits>& traitsList,
    bool distinctAll)
{
    TVector<TInfoUnit> output;
    if (!distinctAll) {
        output = keyColumns;
    }
    for (const auto& traits : traitsList) {
        output.push_back(traits.ResultColName);
    }
    return output;
}

} // anonymous namespace

TIntrusivePtr<IOperator> TPushAppendThroughAggregateRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

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
        const auto& liveOut = GetLiveOut(topMap.get());
        if (!liveOut.contains(to) || liveOut.contains(from)) {
            continue;
        }

        if (!ContainsInfoUnit(aggregate->KeyColumns, from) ||
            !CanRewriteResidualTopMap(topMap, appendIdx, from, to)) {
            continue;
        }

        const auto oldAggregateInput = aggregate->GetInput();
        const auto aggregateInputIUs = oldAggregateInput->GetOutputIUs();
        if (!mapElement.DependsOnlyOn(aggregateInputIUs) || ContainsInfoUnit(aggregateInputIUs, to)) {
            continue;
        }

        const auto pushedOutput = BuildMapOutput(aggregateInputIUs, TVector<TMapElement>{mapElement});
        if (MakeInfoUnitSet(pushedOutput).size() != pushedOutput.size()) {
            continue;
        }

        auto newKeys = aggregate->KeyColumns;
        auto newTraits = aggregate->AggregationTraitsList;
        RenameAggregateKey(newKeys, newTraits, aggregate->IsDistinctAll(), from, to);
        const auto aggregateOutput = BuildAggregateOutput(newKeys, newTraits, aggregate->IsDistinctAll());
        if (MakeInfoUnitSet(aggregateOutput).size() != aggregateOutput.size()) {
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
            if (!IUSetIntersect(aggregateOutput, GetForbidden(topMap.get())).empty()) {
                return input;
            }
            auto pushedMap = MakeIntrusive<TOpMap>(oldAggregateInput, topMap->Pos, TVector<TMapElement>{mapElement});
            pushedMap->Props.OutputIUs = pushedOutput;
            aggregate->SetInput(pushedMap);
            aggregate->KeyColumns = std::move(newKeys);
            aggregate->AggregationTraitsList = std::move(newTraits);
            aggregate->Props.OutputIUs = aggregateOutput;
            return aggregate;
        }

        const auto newTopOutput = BuildMapOutput(aggregateOutput, topElements);
        if (MakeInfoUnitSet(newTopOutput).size() != newTopOutput.size() ||
            !IUSetIntersect(newTopOutput, GetForbidden(topMap.get())).empty()) {
            return input;
        }

        auto pushedMap = MakeIntrusive<TOpMap>(oldAggregateInput, topMap->Pos, TVector<TMapElement>{mapElement});
        pushedMap->Props.OutputIUs = pushedOutput;
        aggregate->SetInput(pushedMap);
        aggregate->KeyColumns = std::move(newKeys);
        aggregate->AggregationTraitsList = std::move(newTraits);
        aggregate->Props.OutputIUs = aggregateOutput;
        auto newTopMap = MakeIntrusive<TOpMap>(aggregate, topMap->Pos, topElements, topMap->Ordered);
        newTopMap->Props.OutputIUs = newTopOutput;
        return newTopMap;
    }

    return input;
}

} // namespace NKqp
} // namespace NKikimr

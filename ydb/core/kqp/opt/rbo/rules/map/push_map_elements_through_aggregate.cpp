#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

namespace NKikimr {
namespace NKqp {

// Main shape this handles:
// A: Map [ alias_key <- key ]  == becomes ==>  Aggregate [ key: alias_key ]
// B: `- Aggregate [ key ]                       `- Map [ alias_key <- key ]
// C:    `- input                                    `- input
//
// Aggregate result aliases use a direct result-trait rename:
//
// A: Map [ alias_sum <- sum ]  == becomes ==>  Aggregate [ alias_sum: sum(...) ]
// B: `- Aggregate [ sum: sum(...) ]
//
// Append aliases of aggregate keys use the same shape when the original key is
// dead above the map:
//
// A: Map [ alias_key := key ]  == becomes ==>  Aggregate [ key: alias_key ]
// B: `- Aggregate [ key ]                       `- Map [ alias_key <- key ]
// C:    `- input                                    `- input
//
// Caveats:
// 1.
// A: Map [ alias_key := key ] -- append aliases are pushed only when "key" is
// B: `- Aggregate [ key ]        not needed above A; the rewrite makes the
// C:    `- input                 aggregate output "alias_key" instead of "key".
//
// 2.
// A: Map [ alias_key <- key ] -- move prevented if Aggregate B has multiple
// B: `- Aggregate B             consumers; otherwise changing B's output metadata
// C:    `- input                would also affect Parent2.
// D: Parent2
// E: `- Aggregate B
//
// 3.
// A: Map [ alias_key <- key, copy <- alias_key ]
// B: `- Aggregate [ key ]      -- move left alone; pushing only alias_key below
// C:    `- input                  the aggregate would make copy hide alias_key.

namespace {

using TRenameMap = THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>;

struct TCommonResultRenamePlan {
    TRenameMap RenameMap;
    THashMap<TOpMap*, TVector<TMapElement>> Residuals;
};

bool AddOutputColumn(TVector<TInfoUnit>& output, TInfoUnitSet& outputSet, const TInfoUnit& column) {
    if (outputSet.contains(column)) {
        return false;
    }

    output.push_back(column);
    outputSet.insert(column);
    return true;
}

TInfoUnit RenameInfoUnit(const TInfoUnit& iu, const TRenameMap& renameMap) {
    const auto it = renameMap.find(iu);
    if (it == renameMap.end()) {
        return iu;
    }
    return it->second;
}

bool BuildAggregateOutputAfterRename(
    const TOpAggregate& aggregate,
    const TRenameMap& renameMap,
    TVector<TInfoUnit>& output)
{
    output.clear();
    TInfoUnitSet outputSet;

    if (!aggregate.DistinctAll) {
        output.reserve(aggregate.KeyColumns.size() + aggregate.AggregationTraitsList.size());
        for (const auto& key : aggregate.KeyColumns) {
            if (!AddOutputColumn(output, outputSet, RenameInfoUnit(key, renameMap))) {
                return false;
            }
        }
    } else {
        output.reserve(aggregate.AggregationTraitsList.size());
    }

    for (const auto& trait : aggregate.AggregationTraitsList) {
        if (!AddOutputColumn(output, outputSet, RenameInfoUnit(trait.ResultColName, renameMap))) {
            return false;
        }
    }
    return true;
}

bool ValidateAndRewriteResidualMap(
    TVector<TMapElement>& residualElements,
    const TVector<TInfoUnit>& inputColumns,
    const TRenameMap& renameMap)
{
    TInfoUnitSet changedNames;
    for (const auto& [from, to] : renameMap) {
        changedNames.insert(from);
        changedNames.insert(to);
    }

    TInfoUnitSet renameSources;
    for (auto& element : residualElements) {
        if (element.IsRename()) {
            const auto from = element.GetRename();
            if (changedNames.contains(from)) {
                return false;
            }
            renameSources.insert(from);
        } else {
            element.SetExpression(element.GetExpression().ApplyRenames(renameMap));
        }
    }

    TInfoUnitSet outputSet;
    for (const auto& inputColumn : inputColumns) {
        if (!renameSources.contains(inputColumn)) {
            if (outputSet.contains(inputColumn)) {
                return false;
            }
            outputSet.insert(inputColumn);
        }
    }

    for (const auto& element : residualElements) {
        if (outputSet.contains(element.GetElementName())) {
            return false;
        }
        outputSet.insert(element.GetElementName());
    }
    return true;
}

bool CanPushToAggregateInput(
    const TOpAggregate& aggregate,
    const TVector<TInfoUnit>& aggregateInputIUs,
    const TInfoUnit& from,
    const TInfoUnit& to)
{
    return ContainsInfoUnit(aggregate.KeyColumns, from) &&
        (from == to || !ContainsInfoUnit(aggregateInputIUs, to));
}

bool IsAggregateResult(const TOpAggregate& aggregate, const TInfoUnit& iu) {
    return AnyOf(aggregate.AggregationTraitsList, [&](const auto& trait) {
        return trait.ResultColName == iu;
    });
}

bool ShouldPushKeyRename(
    const TIntrusivePtr<TOpMap>& topMap,
    const TMapElement& element,
    const TInfoUnitSet& liveOut)
{
    return liveOut.contains(element.GetElementName()) ||
        GetForbidden(topMap.get()).contains(element.GetRename());
}

bool ShouldPushKeyAppend(
    const TMapElement& element,
    const TInfoUnitSet& liveOut)
{
    return liveOut.contains(element.GetElementName()) &&
        !liveOut.contains(element.GetColumnAccess());
}

bool TryBuildCommonResultRenamePlan(
    const TIntrusivePtr<TOpMap>& topMap,
    const TIntrusivePtr<TOpAggregate>& aggregate,
    TCommonResultRenamePlan& plan)
{
    plan.RenameMap.clear();
    plan.Residuals.clear();

    for (const auto& element : topMap->MapElements) {
        if (!element.IsRename()) {
            continue;
        }

        const auto from = element.GetRename();
        const auto to = element.GetElementName();
        if (from != to &&
            !ContainsInfoUnit(aggregate->KeyColumns, from) &&
            IsAggregateResult(*aggregate, from) &&
            !plan.RenameMap.contains(from)) {
            plan.RenameMap.emplace(from, to);
        }
    }

    TVector<TInfoUnit> renamedAggregateOutput;
    if (plan.RenameMap.empty() ||
        !BuildAggregateOutputAfterRename(*aggregate, plan.RenameMap, renamedAggregateOutput)) {
        return false;
    }

    for (const auto& [parent, parentIdx] : aggregate->Parents) {
        if (parent->Kind != EOperator::Map ||
            parentIdx >= parent->Children.size() ||
            parent->Children[parentIdx].Get() != aggregate.Get()) {
            return false;
        }

        auto* map = static_cast<TOpMap*>(parent);
        TVector<TMapElement> residualElements;
        residualElements.reserve(map->MapElements.size());
        TInfoUnitSet matchedSources;
        for (auto element : map->MapElements) {
            if (element.IsRename()) {
                const auto it = plan.RenameMap.find(element.GetRename());
                if (it != plan.RenameMap.end() && it->second == element.GetElementName()) {
                    matchedSources.insert(element.GetRename());
                    continue;
                }
            }
            residualElements.push_back(std::move(element));
        }

        if (matchedSources.size() != plan.RenameMap.size() ||
            !ValidateAndRewriteResidualMap(residualElements, renamedAggregateOutput, plan.RenameMap)) {
            return false;
        }

        plan.Residuals.emplace(map, std::move(residualElements));
    }

    return plan.Residuals.size() == aggregate->Parents.size();
}

} // anonymous namespace

TIntrusivePtr<IOperator>
TPushMapElementsThroughAggregateRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (input->Kind != EOperator::Map) {
        return input;
    }

    auto topMap = CastOperator<TOpMap>(input);
    if (topMap->GetInput()->Kind != EOperator::Aggregate) {
        return input;
    }

    auto aggregate = CastOperator<TOpAggregate>(topMap->GetInput());
    if (!aggregate->IsSingleConsumer()) {
        TCommonResultRenamePlan plan;
        if (!TryBuildCommonResultRenamePlan(topMap, aggregate, plan)) {
            return input;
        }

        aggregate->RenameProducedIUs(plan.RenameMap, ctx.ExprCtx);
        auto currentResidual = std::move(plan.Residuals.at(topMap.Get()));
        for (auto& [map, residualElements] : plan.Residuals) {
            map->MapElements = std::move(residualElements);
        }
        props.Subplans.RenameReferences(plan.RenameMap, ctx.ExprCtx);

        if (currentResidual.empty()) {
            return aggregate;
        }
        return MakeIntrusive<TOpMap>(aggregate, topMap->Pos, currentResidual, topMap->Ordered);
    }

    const auto aggregateInput = aggregate->GetInput();
    const auto aggregateInputIUs = aggregateInput->GetOutputIUs();
    const auto& liveOut = GetLiveOut(topMap.get());

    TVector<TMapElement> pushedElements;
    TVector<TMapElement> topElements;
    TRenameMap keyRenameMap;
    TRenameMap producedRenameMap;

    for (auto mapElement : topMap->MapElements) {
        TInfoUnit from;
        const auto to = mapElement.GetElementName();

        if (mapElement.IsRename()) {
            from = mapElement.GetRename();
            if (!ShouldPushKeyRename(topMap, mapElement, liveOut)) {
                topElements.push_back(std::move(mapElement));
                continue;
            }
        } else if (mapElement.IsColumnAccess()) {
            from = mapElement.GetColumnAccess();
            if (!ShouldPushKeyAppend(mapElement, liveOut)) {
                topElements.push_back(std::move(mapElement));
                continue;
            }
            mapElement.SetIsRename(true);
        } else {
            topElements.push_back(std::move(mapElement));
            continue;
        }

        if (CanPushToAggregateInput(*aggregate, aggregateInputIUs, from, to) &&
            !producedRenameMap.contains(from)) {
            pushedElements.push_back(std::move(mapElement));
            if (from != to) {
                keyRenameMap.emplace(from, to);
                producedRenameMap.emplace(from, to);
            }
            continue;
        }

        if (!ContainsInfoUnit(aggregate->KeyColumns, from) &&
            IsAggregateResult(*aggregate, from) &&
            from != to &&
            !producedRenameMap.contains(from)) {
            producedRenameMap.emplace(from, to);
            continue;
        }

        topElements.push_back(std::move(mapElement));
    }

    TVector<TInfoUnit> renamedAggregateOutput;
    if (producedRenameMap.empty() ||
        !BuildAggregateOutputAfterRename(*aggregate, producedRenameMap, renamedAggregateOutput) ||
        !ValidateAndRewriteResidualMap(topElements, renamedAggregateOutput, producedRenameMap)) {
        return input;
    }

    if (!pushedElements.empty()) {
        auto pushedMap = MakeIntrusive<TOpMap>(aggregateInput, topMap->Pos, pushedElements);
        aggregate->SetInput(pushedMap);
    }
    aggregate->RenameProducedIUs(producedRenameMap, ctx.ExprCtx);
    aggregate->RenameUsedIUs(keyRenameMap, ctx.ExprCtx);
    props.Subplans.RenameReferences(producedRenameMap, ctx.ExprCtx);

    if (topElements.empty()) {
        return aggregate;
    }

    return MakeIntrusive<TOpMap>(aggregate, topMap->Pos, topElements, topMap->Ordered);
}

} // namespace NKqp
} // namespace NKikimr

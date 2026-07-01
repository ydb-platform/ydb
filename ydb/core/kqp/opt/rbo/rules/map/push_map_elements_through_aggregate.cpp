#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

namespace NKikimr {
namespace NKqp {

// Main shape this handles:
// A: Map [ alias_key <- key ]  == becomes ==>  Aggregate [ key: alias_key ]
// B: `- Aggregate [ key ]                       `- Map [ alias_key <- key ]
// C:    `- input                                    `- input
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
// B: `- Aggregate B             consumers; otherwise changing B's key metadata
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

bool HasResidualRenameFromChangedKey(const TVector<TMapElement>& topElements, const TRenameMap& renameMap) {
    TInfoUnitSet changedKeys;
    for (const auto& [from, to] : renameMap) {
        changedKeys.insert(from);
        changedKeys.insert(to);
    }

    for (const auto& element : topElements) {
        if (element.IsRename() && changedKeys.contains(element.GetRename())) {
            return true;
        }
    }
    return false;
}

void RewriteResidualTopMapInputs(TVector<TMapElement>& elements, const TRenameMap& renameMap) {
    for (auto& element : elements) {
        if (!element.IsRename()) {
            element.SetExpression(element.GetExpression().ApplyRenames(renameMap));
        }
    }
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
        return input;
    }

    const auto aggregateInput = aggregate->GetInput();
    const auto aggregateInputIUs = aggregateInput->GetOutputIUs();
    const auto& liveOut = GetLiveOut(topMap.get());

    TVector<TMapElement> pushedElements;
    TVector<TMapElement> topElements;
    TRenameMap renameMap;

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

        if (!CanPushToAggregateInput(*aggregate, aggregateInputIUs, from, to) ||
            renameMap.contains(from)) {
            topElements.push_back(std::move(mapElement));
            continue;
        }

        pushedElements.push_back(std::move(mapElement));
        if (from != to) {
            renameMap.emplace(from, to);
        }
    }

    if (pushedElements.empty() || HasResidualRenameFromChangedKey(topElements, renameMap)) {
        return input;
    }

    RewriteResidualTopMapInputs(topElements, renameMap);

    auto pushedMap = MakeIntrusive<TOpMap>(aggregateInput, topMap->Pos, pushedElements);
    aggregate->SetInput(pushedMap);
    aggregate->RenameProducedIUs(renameMap, ctx.ExprCtx);
    aggregate->RenameUsedIUs(renameMap, ctx.ExprCtx);
    props.Subplans.RenameReferences(renameMap, ctx.ExprCtx);

    if (topElements.empty()) {
        return aggregate;
    }

    return MakeIntrusive<TOpMap>(aggregate, topMap->Pos, topElements, topMap->Ordered);
}

} // namespace NKqp
} // namespace NKikimr

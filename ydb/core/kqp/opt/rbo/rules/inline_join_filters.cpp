#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {
    
// Inline join filters. In case of inner join, replace the join with a filter on top of inner or cross join
// More complex logic for other types of joins

TIntrusivePtr<IOperator> TInlineJoinFiltersRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Join) {
        return input;
    }

    auto join = CastOperator<TOpJoin>(input);
    if (join->JoinFilters.empty()) {
        return input;
    }

    // In case of inner join, we push the join filters above the join
    if (join->JoinKind == "Inner") {
        auto filterExpr = MakeConjunction(join->JoinFilters);
        auto newFilter = MakeIntrusive<TOpFilter>(join, input->Pos, filterExpr);

        join->JoinFilters = {};

        // Now that we pushed the filters out of the join, the join might turn into a cross-join
        if (join->JoinKeys.empty()) {
            join->JoinKind = "Cross";
        }

        return newFilter;
    }

    // We only support various left joins now
    if (join->JoinKind != "Left" && join->JoinKind != "LeftSemi" && join->JoinKind != "LeftOnly") {
        return input;
    }

    auto innerJoin = MakeIntrusive<TOpJoin>(join->GetLeftInput(), join->GetRightInput(), join->Pos, "Inner", join->JoinKeys);
    auto filterExpr = MakeConjunction(join->JoinFilters);
    auto newFilter = MakeIntrusive<TOpFilter>(innerJoin, input->Pos, filterExpr);

    // We need to remap the appropriate side of the output columns, so we can join on the same columns again
    // without confilcts
    auto renameIUs = join->GetLeftInput()->GetOutputIUs();

    TVector<TMapElement> mapElements;
    THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> renameMap;
    for (const auto& iu : newFilter->GetOutputIUs()) {

        if (std::find(renameIUs.begin(), renameIUs.end(), iu) != renameIUs.end()) {
            auto newVar = TInfoUnit("_rbo_arg_" + std::to_string(props.InternalVarIdx++));
            mapElements.push_back(TMapElement(newVar, iu, join->Pos, &ctx.ExprCtx, &props));
            renameMap[iu] = newVar;
        } else {
            mapElements.push_back(TMapElement(iu, iu, join->Pos, &ctx.ExprCtx, &props));
        }
    }

    auto map = MakeIntrusive<TOpMap>(newFilter, join->Pos, mapElements, true);

    TVector<std::pair<TInfoUnit, TInfoUnit>> newJoinKeys;
    for (const auto& [leftKey, _] : join->JoinKeys) {
        newJoinKeys.push_back(std::make_pair(leftKey, renameMap.at(leftKey)));
    }

    auto result = MakeIntrusive<TOpJoin>(join->GetLeftInput(), map, join->Pos, join->JoinKind, newJoinKeys);
    
    return result;
}
}
}
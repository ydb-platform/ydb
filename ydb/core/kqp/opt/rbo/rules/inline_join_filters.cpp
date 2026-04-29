#include "kqp_rules_include.h"

namespace {

using namespace NKikimr::NKqp;

// Create a mapping from a list of IUs to new synthetic variables
THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> MakeRenameMap(const TVector<TInfoUnit>& IUs, int& varIdx) {
    THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> result;
    for (const auto& iu: IUs) {
        result[iu] = TInfoUnit("_rbo_arg_" + std::to_string(varIdx++));
    }
    return result;
}

// Rename join keys of the right side of the join using a specified rename map
TVector<std::pair<TInfoUnit, TInfoUnit>> RemapJoinKeysRightSide(const TVector<std::pair<TInfoUnit, TInfoUnit>>& joinKeys, 
    const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap) {

        TVector<std::pair<TInfoUnit, TInfoUnit>> newJoinKeys;

        for (const auto& [leftKey, rightKey] : joinKeys) {
            if (renameMap.contains(rightKey)) {
                newJoinKeys.push_back(std::make_pair(leftKey, renameMap.at(rightKey)));
            } else {
                newJoinKeys.push_back(std::make_pair(leftKey, rightKey));
            }
    }
    return newJoinKeys;

}

// Build a projecting map operator that renames output columns wrt the rename map, or copies them in
// the ouput if they're not in the map
TIntrusivePtr<TOpMap> MakeMapFromRenames(TIntrusivePtr<IOperator> input,
    const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, 
    TPositionHandle pos, 
    TExprContext *ctx, 
    TPlanProps *props) {

    TVector<TMapElement> mapElements;
    for (const auto& iu : input->GetOutputIUs()) {
        auto fromIU = iu;
        auto toIU = iu;

        if (renameMap.contains(iu)) {
            toIU = renameMap.at(iu);
        }

        mapElements.push_back(TMapElement(toIU, iu, pos, ctx, props));
    }

    return MakeIntrusive<TOpMap>(input, pos, mapElements, true);
}


}

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

    // Build an inner join, but in case of LeftSemi and LeftOnly, the right side may contain duplicate IUs
    // which will break the plan. So we rename them
    auto commonIUs = IUSetIntersect(join->GetLeftInput()->GetOutputIUs(), join->GetRightInput()->GetOutputIUs());
    TIntrusivePtr<IOperator> rightInput = join->GetRightInput();

    auto rightRenameMap = MakeRenameMap(commonIUs, props.InternalVarIdx);
    auto newInnerJoinKeys = RemapJoinKeysRightSide(join->JoinKeys, rightRenameMap);

    if (rightRenameMap.size()) {
        rightInput = MakeMapFromRenames(join->GetRightInput(), rightRenameMap, join->Pos, &ctx.ExprCtx, &props);
    }

    auto innerJoin = MakeIntrusive<TOpJoin>(join->GetLeftInput(), rightInput, join->Pos, "Inner", newInnerJoinKeys);
    auto filterExpr = MakeConjunction(join->JoinFilters);

    auto newFilter = MakeIntrusive<TOpFilter>(innerJoin, input->Pos, filterExpr);

    // We need to remap the appropriate side of the output columns, so we can join on the same columns again
    // without confilcts

    auto topCommonIUs = IUSetIntersect(join->GetLeftInput()->GetOutputIUs(), innerJoin->GetOutputIUs());

    auto renameMap = MakeRenameMap(topCommonIUs, props.InternalVarIdx);
    auto map = MakeMapFromRenames(newFilter, renameMap, join->Pos, &ctx.ExprCtx, &props);
    auto newJoinKeys = RemapJoinKeysRightSide(join->JoinKeys, renameMap);
    auto result = MakeIntrusive<TOpJoin>(join->GetLeftInput(), map, join->Pos, join->JoinKind, newJoinKeys);
    
    return result;
}
}
}
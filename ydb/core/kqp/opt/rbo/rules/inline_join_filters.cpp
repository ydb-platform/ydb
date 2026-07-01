#include "kqp_rules_include.h"
#include "join_common.h"

namespace {

using namespace NKikimr::NKqp;
using namespace NKikimr::NKqp::NJoinRules;

bool CheckNonNullKeys(const TIntrusivePtr<IOperator> &input, const TVector<TInfoUnit>& columns) {
    auto itemType = input->Type->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    for (const auto & column : columns) {
        const auto* columnType = itemType->FindItemType(column.GetFullName());
        // A key column may be absent from the row type when downstream alias rewrites have renamed
        // it but the propagated KeyColumns metadata still references the old name. In that case we
        // cannot prove the key is non-null (nor build a valid join on it), so bail out of the rewrite.
        if (!columnType || columnType->IsOptionalOrNull()) {
            return false;
        }
    }
    return true;
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

    // In case of inner or cross join, we push the join filters above the join
    if (join->JoinKind == "Inner" || join->JoinKind == "Cross") {
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
        Y_ENSURE(false, TStringBuilder() << "Join filter in unsupported join type: " << join->JoinKind);
        return input;
    }

    THashSet<TInfoUnit, TInfoUnit::THashFunction> usedIUs;
    AddUsedIUs(usedIUs, join->GetLeftInput()->GetOutputIUs());
    AddUsedIUs(usedIUs, join->GetRightInput()->GetOutputIUs());
    for (const auto& [leftKey, rightKey] : join->JoinKeys) {
        usedIUs.insert(leftKey);
        usedIUs.insert(rightKey);
    }
    for (const auto& joinFilter : join->JoinFilters) {
        AddUsedIUs(usedIUs, joinFilter.GetInputIUs(false, true));
    }

    // Build an inner join, but in case of LeftSemi and LeftOnly, the right side may contain duplicate IUs
    // which will break the plan. So we rename them
    auto commonIUs = IUSetIntersect(join->GetLeftInput()->GetOutputIUs(), join->GetRightInput()->GetOutputIUs());
    auto rightRenameMap = MakeRenameMap(commonIUs, props.InternalVarIdx, usedIUs);
    auto innerJoin = MakeJoinWithRightRenames(
        join->GetLeftInput(), join->GetRightInput(), join->Pos, "Inner", join->JoinKeys, {}, rightRenameMap, ctx.ExprCtx, props);
    auto filterExpr = MakeConjunction(join->JoinFilters);

    auto newFilter = MakeIntrusive<TOpFilter>(innerJoin, input->Pos, filterExpr);

    // We need to remap the appropriate side of the output columns, so we can join on the same columns again
    // without confilcts

    auto topCommonIUs = IUSetIntersect(join->GetLeftInput()->GetOutputIUs(), innerJoin->GetOutputIUs());

    auto renameMap = MakeRenameMap(topCommonIUs, props.InternalVarIdx, usedIUs);

    // The join will be on the keys of lhs, we just need to check that all the keys are non-null
    // We don't support nullable keys at this stage
    auto keyColumns = join->GetLeftInput()->Props.Metadata->KeyColumns;
    if (keyColumns.empty()) {
        Y_ENSURE(false, "No key columns when inlining join filter");
    }

    if (!CheckNonNullKeys(join->GetLeftInput(), keyColumns)) {
        Y_ENSURE(false, "During join filter inlining the keys on the left side cannot be null");
    }

    TVector<std::pair<TInfoUnit, TInfoUnit>> newJoinKeys;
    for (const auto & column : keyColumns) {
        newJoinKeys.push_back(std::make_pair(column, column));
    }

    auto result = MakeJoinWithRightRenames(join->GetLeftInput(), newFilter, join->Pos, join->JoinKind, newJoinKeys, {}, renameMap, ctx.ExprCtx, props);
    
    return result;
}
}
}

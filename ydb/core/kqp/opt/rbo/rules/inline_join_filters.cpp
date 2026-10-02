#include "kqp_rules_include.h"

namespace {

using namespace NKikimr::NKqp;

bool CheckNonNullKeys(const TIntrusivePtr<IOperator> &input, const TOrderedIUs<>& columns) {
    auto itemType = input->Type->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    for (const auto & column : columns.Items()) {
        const auto* columnType = itemType->FindItemType(ToString(column));
        if (!columnType || columnType->IsOptionalOrNull()) {
            return false;
        }
    }
    return true;
}

}

namespace NKikimr {
namespace NKqp {

bool TInlineJoinFiltersRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Join;
}

// Inline join filters. Temporarily inline join filters only of there are no equi-join conditions in the join

TIntrusivePtr<IOperator> TInlineJoinFiltersRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    if (input->Kind != EOperator::Join) {
        return input;
    }

    auto join = CastOperator<TOpJoin>(input);
    if (join->JoinFilters.empty()) {
        return input;
    }

    // Inner with empty keys - cross.
    const bool isRealCrossJoin = join->JoinKind == "Cross" || (join->JoinKind == "Inner" && join->JoinKeys.Items().empty());
    const bool usingBlockJoin = ctx.KqpCtx.Config->GetUseBlockHashJoin();
    const bool usingBlockCrossJoin = usingBlockJoin && ctx.KqpCtx.Config->GetUseBlockHashJoinForCross();

    // Do not inline filters for cross join.
    if (isRealCrossJoin && usingBlockCrossJoin) {
        join->JoinKind = "Cross";
        return join;
    }

    // We inline join filters in the following cases:
    // - There implementation is a lookup join or reverse lookup join
    // - There are no equi-join conditions in the join
    // - We're not using BlockJoin, which supports join filters

    // Lookup join is not supported for join filters.
    const bool isLookupJoin = join->Props.JoinAlgo == EJoinAlgoType::LookupJoin || join->Props.JoinAlgo == EJoinAlgoType::LookupJoinReverse;
    bool containsEquiJoinConditions = !join->JoinKeys.Items().empty();
    for (const auto& f : join->JoinFilters) {
        if (f.MaybeEquiJoinCondition()) {
            containsEquiJoinConditions = true;
        }
    }

    if (!isRealCrossJoin && usingBlockJoin && !isLookupJoin && containsEquiJoinConditions) {
        return input;
    }

    // In case of inner or cross join, we push the join filters above the join
    if (join->JoinKind == "Inner" || join->JoinKind == "Cross") {
        auto filterExpr = MakeConjunction(join->JoinFilters);
        auto newFilter = MakeIntrusive<TOpFilter>(join, input->Pos, filterExpr);

        join->JoinFilters = {};

        // Now that we pushed the filters out of the join, the join might turn into a cross-join
        if (join->JoinKeys.Items().empty()) {
            join->JoinKind = "Cross";
        }

        return newFilter;
    }

    // We only support various left joins now
    if (join->JoinKind != "Left" && join->JoinKind != "LeftSemi" && join->JoinKind != "LeftOnly") {
        Y_ENSURE(false, TStringBuilder() << "Join filter in unsupported join type: " << join->JoinKind);
        return input;
    }

    // The inner join reads the left input again: a Replicate port gives it its own IDs,
    // so we can join on the same columns again without conflicts
    auto hub = TReplicate::Create(join->GetLeftInput(), join->Pos, props.InfoUnitRegistry);
    auto outerLeftInput = hub->AddOutput();
    auto innerLeftInput = hub->AddOutput();
    const auto renameMap = innerLeftInput->GetRebindings();

    // Build an inner join
    const auto joinKind = join->JoinKeys.Items().empty() ? "Cross" : "Inner";
    TJoinIUs innerJoinKeys;
    for (const auto& [leftKey, rightKey, equalNulls] : join->JoinKeys.Items()) {
        innerJoinKeys.Add({renameMap.At(leftKey), rightKey, equalNulls});
    }
    auto innerJoin = MakeIntrusive<TOpJoin>(innerLeftInput, join->GetRightInput(), join->Pos, joinKind, innerJoinKeys);
    auto filterExpr = MakeConjunction(join->JoinFilters).ApplyRenames(renameMap);

    auto newFilter = MakeIntrusive<TOpFilter>(innerJoin, input->Pos, filterExpr);

    // The join will be on the keys of lhs, we just need to check that all the keys are non-null
    // We don't support nullable keys at this stage
    auto keyColumns = join->GetLeftInput()->Props.Metadata->KeyColumns;
    if (keyColumns.Items().empty()) {
        Y_ENSURE(false, "No key columns when inlining join filter");
    }

    if (!CheckNonNullKeys(join->GetLeftInput(), keyColumns)) {
        Y_ENSURE(false, "During join filter inlining the keys on the left side cannot be null");
    }

    TJoinIUs newJoinKeys;
    for (const auto & column : keyColumns.Items()) {
        newJoinKeys.Add(column, renameMap.At(column));
    }

    // Subplans called from the join filters now read the inner left input.
    for (const auto call : join->GetSubplanIUs(props.Subplans)) {
        props.Subplans.RebindInputs(call, renameMap);
    }

    auto result = MakeIntrusive<TOpJoin>(outerLeftInput, newFilter, join->Pos, join->JoinKind, newJoinKeys);

    return result;
}
}
}

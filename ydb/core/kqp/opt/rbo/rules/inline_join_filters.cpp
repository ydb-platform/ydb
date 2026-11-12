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
    return input->Kind == EOperator::Join && !CastOperator<TOpJoin>(input)->JoinFilters.empty()
        && (!FullJoinsOnly || CastOperator<TOpJoin>(input)->JoinKind == "Full");
}

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
    const bool forceInlining = ctx.KqpCtx.Config->GetEnableInlineJoinFiltersAfterCBO();

    // Do not inline filters for cross join, unless we force the inlining with a flag
    if (isRealCrossJoin && usingBlockCrossJoin && !forceInlining) {
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

    const bool isFullJoin = join->JoinKind == "Full";
    const bool supportsNativeFilters =
        !isRealCrossJoin && !isFullJoin && usingBlockJoin && !isLookupJoin && containsEquiJoinConditions;
    if (supportsNativeFilters && !forceInlining) {
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

    if (join->JoinKind != "Left" && join->JoinKind != "LeftSemi" && join->JoinKind != "LeftOnly" && !isFullJoin) {
        Y_ENSURE(false, TStringBuilder() << "Join filter in unsupported join type: " << join->JoinKind);
        return input;
    }

    // Joining the matches back to the left input needs a non-null unique key.
    // If forced expansion cannot do that, keep native filters when supported.
    const auto keyColumns = join->GetLeftInput()->Props.Metadata->KeyColumns;
    const bool hasNonNullKeys = !keyColumns.Items().empty() && CheckNonNullKeys(join->GetLeftInput(), keyColumns);
    if (supportsNativeFilters && !hasNonNullKeys) {
        return input;
    }
    Y_ENSURE(!keyColumns.Items().empty(), "No key columns when inlining join filter");
    Y_ENSURE(hasNonNullKeys, "During join filter inlining the keys on the left side cannot be null");

    TOrderedIUs<> rightKeyColumns;
    if (isFullJoin) {
        rightKeyColumns = join->GetRightInput()->Props.Metadata->KeyColumns;
        Y_ENSURE(!rightKeyColumns.Items().empty(), "No right key columns when inlining full join filter");
        Y_ENSURE(CheckNonNullKeys(join->GetRightInput(), rightKeyColumns),
            "During full join filter inlining the keys on the right side cannot be null");
    }

    // The inner join reads the left input again: a Replicate port gives it its own IDs,
    // so we can join on the same columns again without conflicts
    auto hub = TReplicate::Create(join->GetLeftInput(), join->Pos, props.InfoUnitRegistry);
    auto outerLeftInput = hub->AddOutput();
    auto innerLeftInput = hub->AddOutput();
    auto renameMap = innerLeftInput->GetRebindings();
    TIntrusivePtr<IOperator> innerRightInput = join->GetRightInput();
    TIntrusivePtr<IOperator> outerRightInput;
    if (isFullJoin) {
        auto rightHub = TReplicate::Create(join->GetRightInput(), join->Pos, props.InfoUnitRegistry);
        outerRightInput = rightHub->AddOutput();
        auto rightPort = rightHub->AddOutput();
        for (const auto& [source, target] : rightPort->GetRebindings().Items()) {
            renameMap.Add(source, target);
        }
        innerRightInput = rightPort;
    }

    // Build an inner join
    const auto joinKind = join->JoinKeys.Items().empty() ? "Cross" : "Inner";
    TJoinIUs innerJoinKeys;
    for (const auto& [leftKey, rightKey, equalNulls] : join->JoinKeys.Items()) {
        innerJoinKeys.Add({renameMap.At(leftKey), Substitute(rightKey, renameMap), equalNulls});
    }
    auto innerJoin = MakeIntrusive<TOpJoin>(innerLeftInput, innerRightInput, join->Pos, joinKind, innerJoinKeys);
    auto filterExpr = MakeConjunction(join->JoinFilters).ApplyRenames(renameMap);

    auto newFilter = MakeIntrusive<TOpFilter>(innerJoin, input->Pos, filterExpr);

    TJoinIUs newJoinKeys;
    for (const auto & column : keyColumns.Items()) {
        newJoinKeys.Add(column, renameMap.At(column));
    }

    // Subplans called from the join filters now read the inner inputs.
    for (const auto call : join->GetSubplanIUs(props.Subplans)) {
        props.Subplans.RebindInputs(call, renameMap);
    }

    auto result = MakeIntrusive<TOpJoin>(outerLeftInput, newFilter, join->Pos, isFullJoin ? "Left" : join->JoinKind, newJoinKeys);

    if (isFullJoin) {
        // The left join restores unmatched left rows. Join the accepted right keys
        // back to the original right input to restore unmatched right rows too.
        // Original IDs come from the outer inputs; inner copies are pruned later.
        TJoinIUs rightJoinKeys;
        for (const auto column : rightKeyColumns.Items()) {
            rightJoinKeys.Add(renameMap.At(column), column);
        }
        result = MakeIntrusive<TOpJoin>(result, outerRightInput, join->Pos, "Full", rightJoinKeys);
    }

    return result;
}
}
}

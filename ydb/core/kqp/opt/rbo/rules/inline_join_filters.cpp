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

    auto innerJoin = MakeIntrusive<TOpJoin>(join->GetLeftInput(), join->GetRightInput(), join->Pos, "Inner", join->JoinKeys);
    auto filterExpr = MakeConjunction(join->JoinFilters);
    if (join->JoinKind == "LeftOnly" || join->JoinKind == "RightOnly") {
        filterExpr = MakeNegation(filterExpr);
    }

    auto newFilter = MakeIntrusive<TOpFilter>(innerJoin, input->Pos, filterExpr);
    // If this is one of the left joins, we build an inner join with the non-equijoin filter on top, and then add a left join on the
    // equi-join predicate. The only small exception is left-only join, we negate the predicate
    if (join->JoinKind == "Left" || join->JoinKind == "LeftSemi" || join->JoinKind == "LeftOnly") {
        return MakeIntrusive<TOpJoin>(innerJoin, join->GetRightInput(), join->Pos, join->JoinKind, join->JoinKeys);
    }

    // Symmetrically with righ joins
    // FIXME: Maybe we should just flip all the right joins before this
    else if (join->JoinKind == "Right" || join->JoinKind == "RightSemi" || join->JoinKind == "RightOnly") {
        return MakeIntrusive<TOpJoin>(innerJoin, join->GetRightInput(), join->Pos, join->JoinKind, join->JoinKeys);
    }

    // For full outerjoin we need to add two joins
    else if (join->JoinKind == "Full"){
        auto leftJoin = MakeIntrusive<TOpJoin>(join->GetLeftInput(), innerJoin, join->Pos, "Left", join->JoinKeys);
        auto rightJoin = MakeIntrusive<TOpJoin>(leftJoin, join->GetRightInput(), join->Pos, "Left", join->JoinKeys);
        return rightJoin;
    }

    // We don't suppport Exclusion Join
    else {
        Y_ENSURE(false, TStringBuilder() << "Unsupported join kind for inlining join filers: " << join->JoinKind);
    }
}
}
}
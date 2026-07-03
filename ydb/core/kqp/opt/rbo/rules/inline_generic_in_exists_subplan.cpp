#include "kqp_rules_include.h"

#include <ydb/core/kqp/opt/rbo/map_renames.h>

namespace {

using namespace NKikimr::NKqp;
using namespace NKikimr::NKqp::NMapRenames;

bool CheckNonNullKeys(const TIntrusivePtr<IOperator> &input, const TVector<TInfoUnit>& columns) {
    auto itemType = input->Type->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    for (const auto & column : columns) {
        auto columnType = itemType->FindItemType(column.GetFullName());
        if (columnType->IsOptionalOrNull()) {
            return false;
        }
    }
    return true;
}

}

namespace NKikimr {
namespace NKqp {
    
TIntrusivePtr<IOperator> TInlineGenericInExistsSubplanRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (input->Kind != EOperator::Filter) {
        return input;
    }

    // Check that the filter lambda contains at least one in/exists subplan
    auto filter = CastOperator<TOpFilter>(input);
    auto subplanIUs = filter->FilterExpr.GetInputIUs(true, false);
    TVector<TInfoUnit> inOrExistsSubplans;

    for (auto subplanIU : subplanIUs) {
        if(props.Subplans.PlanMap.contains(subplanIU)) {
            auto subplanEntry = props.Subplans.PlanMap.at(subplanIU);
            if (subplanEntry.Type == ESubplanType::IN_SUBPLAN || subplanEntry.Type == ESubplanType::EXISTS) {
                inOrExistsSubplans.push_back(subplanIU);
            }
        } 
    }

    if (inOrExistsSubplans.empty()) {
        return input;
    }

    // Now we will pick the first subplan IU and join its subplan before filter
    // Then we'll remove the subplan from subplans list and rebuild the filter expression
    // so the current iu is no longer marked as SubplanIU

    auto subplanIU = inOrExistsSubplans[0];
    auto subplanEntry = props.Subplans.PlanMap.at(subplanIU);
    TIntrusivePtr<IOperator> join;
    TVector<std::pair<TInfoUnit, TInfoUnit>> extraJoinKeys;
    auto uncorrSubplan = CastOperator<IOperator>(subplanEntry.Plan);
    const auto subPlanKind = uncorrSubplan->Kind;
    TVector<TExpression> joinFilters;


    // If its a correlated subplan with filters pulled up, build join conditions from the pulled up filter
    if (subPlanKind == EOperator::Filter && CastOperator<TOpFilter>(uncorrSubplan)->GetInput()->Kind == EOperator::AddDependencies) {
        auto subplanFilter = CastOperator<TOpFilter>(subplanEntry.Plan);
        auto addDeps = CastOperator<TOpAddDependencies>(subplanFilter->GetInput());
        uncorrSubplan = addDeps->GetInput();
        auto subplanConjuncts = subplanFilter->FilterExpr.SplitConjunct();

        for (const auto& conj : subplanConjuncts) {
            if (conj.MaybeEquiJoinCondition()) {

                auto jc = TEquiJoinCondition(conj);
                if (std::find(addDeps->Dependencies.begin(), addDeps->Dependencies.end(), jc.GetLeftIU()) != addDeps->Dependencies.end()) {
                    extraJoinKeys.push_back(std::make_pair(jc.GetLeftIU(), jc.GetRightIU()));
                } else if (std::find(addDeps->Dependencies.begin(), addDeps->Dependencies.end(), jc.GetRightIU()) != addDeps->Dependencies.end()) {
                    extraJoinKeys.push_back(std::make_pair(jc.GetRightIU(), jc.GetLeftIU()));
                } else {
                    Y_ENSURE(false, "Correlated filter missing join condition");
                }
            } else {
                joinFilters.push_back(conj);
            }

        }
    }

    // If we have a correlated subplan where pull up didn't succeed, throw an exception
    else if (subPlanKind == EOperator::Filter && subplanEntry.DependentIUs.size()) {
        Y_ENSURE(false, "Decorrelation via filter pull up didn't succeed");
    }

    // We build an inner join when processing IN subplan or a correlated EXISTS subplan
    // Then we compute the count of rows in this inner join and left join the result with the main
    // plan. The final column is thus nullable, so we wrap it in a coalesce

    auto zero = MakeConstant("Uint64", "0", filter->Pos, &ctx.ExprCtx);

    if (subplanEntry.Type == ESubplanType::IN_SUBPLAN || !extraJoinKeys.empty()) {
        auto leftInput = filter->GetInput();
        auto rightInput = uncorrSubplan;

        const auto commonIUs = IUSetIntersect(leftInput->GetOutputIUs(), rightInput->GetOutputIUs());
        const auto rightRenamings = MakeRenameMap(commonIUs, props.InternalVarIdx);
        if (!rightRenamings.empty()) {
            rightInput = MakeMapFromRenames(rightInput, rightRenamings, filter->Pos, ctx.ExprCtx, props);
            extraJoinKeys = RemapRightJoinKeys(extraJoinKeys, rightRenamings);
            for (auto& joinFilter : joinFilters) {
                joinFilter = joinFilter.ApplyRenames(rightRenamings);
            }
        }

        TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys;
        auto planIUs = rightInput->GetOutputIUs();

        for (size_t i = 0; i < subplanEntry.Tuple.size(); i++) {
            joinKeys.push_back(std::make_pair(subplanEntry.Tuple[i], planIUs[i]));
        }

        // Fetch keys
        auto keyColumns = leftInput->Props.Metadata->KeyColumns;
        Y_ENSURE(!keyColumns.empty(), "Cannot inline a join filter because key columns are missing");
        Y_ENSURE(CheckNonNullKeys(leftInput, keyColumns), "Key columns cannot be optional when decorrelating generic IN/EXISTS");

        // Build the join
        joinKeys.insert(joinKeys.begin(), extraJoinKeys.begin(), extraJoinKeys.end());
        join = MakeIntrusive<TOpJoin>(leftInput, rightInput, input->Pos, "Inner", joinKeys, joinFilters);

        // Build the counting aggregate and use a map operator to compute count > 0
        auto countResult = TInfoUnit("_rbo_arg_" + std::to_string(props.InternalVarIdx++), true);
        TOpAggregationTraits aggFunction(keyColumns[0], "count", countResult);
        TVector<TOpAggregationTraits> aggs = {aggFunction};
        auto agg = MakeIntrusive<TOpAggregate>(join, aggs, keyColumns, EOpPhase::Final, false, filter->Pos);

        // create a map that computes count > 0 and assigns is to the subplan output variable
        auto comparePredicate = MakeBinaryPredicate(">", MakeColumnAccess(countResult, filter->Pos, &ctx.ExprCtx, &props), zero);
        TVector<TMapElement> mapElements;
        mapElements.emplace_back(subplanIU, comparePredicate);
        auto compareResMap = MakeIntrusive<TOpMap>(agg, filter->Pos, mapElements, false);

        // make a left join with the main plan on the keys of the plan
        // fail if the keys don't exist or some are nullable

        auto topCommonIUs = IUSetIntersect(filter->GetInput()->GetOutputIUs(), compareResMap->GetOutputIUs());

        auto renamings = MakeRenameMap(topCommonIUs, props.InternalVarIdx);

        TVector<std::pair<TInfoUnit, TInfoUnit>> newJoinKeys;
        for (const auto & column : keyColumns) {
            newJoinKeys.push_back(std::make_pair(column, column));
        }

        join = MakeJoinWithRightRenames(filter->GetInput(), compareResMap, filter->Pos, "Left", newJoinKeys, {}, renamings, ctx.ExprCtx, props);

    }
    // uncorrelated EXISTS
    else {
        auto limit = MakeIntrusive<TOpLimit>(uncorrSubplan, filter->Pos, MakeConstant("Uint64", "1", filter->Pos, &ctx.ExprCtx), EOpPhase::Undefined);

        auto countResult = TInfoUnit("_rbo_arg_" + std::to_string(props.InternalVarIdx++), true);
        TVector<TMapElement> countMapElements;
        countMapElements.emplace_back(countResult, zero);
        auto countMap = MakeIntrusive<TOpMap>(limit, filter->Pos, countMapElements, true);

        TOpAggregationTraits aggFunction(countResult, "count", countResult);
        TVector<TOpAggregationTraits> aggs = {aggFunction};
        TVector<TInfoUnit> keyColumns;

        auto agg = MakeIntrusive<TOpAggregate>(countMap, aggs, keyColumns, EOpPhase::Final, false, filter->Pos);

        auto comparePredicate = MakeBinaryPredicate("!=", MakeColumnAccess(countResult, filter->Pos, &ctx.ExprCtx, &props), zero);
        TVector<TMapElement> mapElements;
        mapElements.emplace_back(subplanIU, comparePredicate);

        auto map = MakeIntrusive<TOpMap>(agg, filter->Pos, mapElements, true);

        TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys;
        join = MakeIntrusive<TOpJoin>(filter->GetInput(), map, filter->Pos, "Cross", joinKeys, joinFilters);
    }

    props.Subplans.Remove(subplanIU);

    // Otherwise, we need to pack the remaining conjuncts back into the filter
    return MakeIntrusive<TOpFilter>(join, filter->Pos, TExpression(filter->FilterExpr.GetLambda(), &ctx.ExprCtx, &props));
}
}
}

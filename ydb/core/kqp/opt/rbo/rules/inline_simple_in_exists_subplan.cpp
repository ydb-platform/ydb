#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {
    
std::shared_ptr<IOperator> TInlineSimpleInExistsSubplanRule::SimpleMatchAndApply(const std::shared_ptr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (input->Kind != EOperator::Filter || props.PgSyntax) {
        return input;
    }

    // Check that the filter lambda is a conjunction of one or more elements
    auto filter = CastOperator<TOpFilter>(input);
    auto lambdaBody = filter->FilterExpr.Node->ChildPtr(1);

    if (!TCoAnd::Match(lambdaBody.Get()) && !TCoNot::Match(lambdaBody.Get()) && !TCoMember::Match(lambdaBody.Get())) {
        return input;
    }

    // Decompose the conjunction into individual conjuncts
    auto conjuncts = filter->FilterExpr.SplitConjunct();

    // Find the first conjunct that is a simple in or exists subplan
    bool negated = false;
    TInfoUnit iu;
    TSubplanEntry subplanEntry;
    size_t conjunctIdx;

    for (conjunctIdx = 0; conjunctIdx < conjuncts.size(); conjunctIdx++) {
        auto maybeSubplan = conjuncts[conjunctIdx].GetExpressionBody();

        if (TCoNot::Match(maybeSubplan.Get())) {
            maybeSubplan = maybeSubplan->ChildPtr(0);
            negated = true;
        }
        if (TCoMember::Match(maybeSubplan.Get())) {
            auto name = TString(maybeSubplan->ChildPtr(1)->Content());
            iu = TInfoUnit(name);
            if (props.Subplans.PlanMap.contains(iu)) {
                subplanEntry = props.Subplans.PlanMap.at(iu);
                if (subplanEntry.Type == ESubplanType::IN_SUBPLAN || subplanEntry.Type == ESubplanType::EXISTS) {
                    break;
                }
            }
        }
    }

    if (conjunctIdx == conjuncts.size()) {
        return input;
    }

    std::shared_ptr<IOperator> join;
    TVector<std::pair<TInfoUnit, TInfoUnit>> extraJoinKeys;
    std::shared_ptr<IOperator> uncorrSubplan = subplanEntry.Plan;

    if (subplanEntry.Plan->Kind == EOperator::Filter && CastOperator<TOpFilter>(subplanEntry.Plan)->GetInput()->Kind == EOperator::AddDependencies) {
        auto subplanFilter = CastOperator<TOpFilter>(subplanEntry.Plan);
        auto addDeps = CastOperator<TOpAddDependencies>(subplanFilter->GetInput());
        uncorrSubplan = addDeps->GetInput();
        auto subplanConjuncts = subplanFilter->FilterExpr.SplitConjunct();

        for (const auto & conj : subplanConjuncts) {
            if (!conj.MaybeJoinCondition()) {
                Y_ENSURE(false, "Expected a filter with only join conditions");
            }

            auto jc = TJoinCondition(conj);

            if (std::find(addDeps->Dependencies.begin(), addDeps->Dependencies.end(), jc.GetLeftIU()) != addDeps->Dependencies.end()) {
                extraJoinKeys.push_back(std::make_pair(jc.GetLeftIU(), jc.GetRightIU()));
            } else if (std::find(addDeps->Dependencies.begin(), addDeps->Dependencies.end(), jc.GetRightIU()) != addDeps->Dependencies.end()) {
                extraJoinKeys.push_back(std::make_pair(jc.GetRightIU(), jc.GetLeftIU()));
            } else {
                Y_ENSURE(false, "Correlated filter missing join condition");
            }   
        }
    }

    // We build a semi-join or a left-only join when processing IN subplan
    if (subplanEntry.Type == ESubplanType::IN_SUBPLAN || !extraJoinKeys.empty()) {
        auto leftJoinInput = filter->GetInput();
        auto joinKind = negated ? "LeftOnly" : "LeftSemi";

        TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys;

        auto planIUs = uncorrSubplan->GetOutputIUs();

        for (size_t i = 0; i < subplanEntry.Tuple.size(); i++) {
            joinKeys.push_back(std::make_pair(subplanEntry.Tuple[i], planIUs[i]));
        }

        joinKeys.insert(joinKeys.begin(), extraJoinKeys.begin(), extraJoinKeys.end());

        join = std::make_shared<TOpJoin>(leftJoinInput, uncorrSubplan, input->Pos, joinKind, joinKeys);
        conjuncts.erase(conjuncts.begin() + conjunctIdx);
    }
    // EXISTS and NOT EXISTS
    else {
        auto limit = std::make_shared<TOpLimit>(uncorrSubplan, filter->Pos, MakeConstant("Uint64", "1", filter->Pos, &ctx.ExprCtx));

        auto countResult = TInfoUnit("_rbo_arg_" + std::to_string(props.InternalVarIdx++), true);
        TVector<TMapElement> countMapElements;
        auto zero = MakeConstant("Uint64", "0", filter->Pos, &ctx.ExprCtx);
        countMapElements.emplace_back(countResult, zero);
        auto countMap = std::make_shared<TOpMap>(limit, filter->Pos, countMapElements, true);

        TOpAggregationTraits aggFunction(countResult, "count", countResult);
        TVector<TOpAggregationTraits> aggs = {aggFunction};
        TVector<TInfoUnit> keyColumns;

        auto agg = std::make_shared<TOpAggregate>(countMap, aggs, keyColumns, EAggregationPhase::Final, false, filter->Pos);
        const TString compareCallable = negated ? "==" : "!=";

        auto comparePredicate = MakeBinaryPredicate(compareCallable, MakeColumnAccess(countResult, filter->Pos, &ctx.ExprCtx, &props), zero);
        TVector<TMapElement> mapElements;
        auto compareResult = TInfoUnit("_rbo_arg_" + std::to_string(props.InternalVarIdx++), true);
        mapElements.emplace_back(compareResult, comparePredicate);
        auto map = std::make_shared<TOpMap>(agg, filter->Pos, mapElements, true);

        TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys;
        join = std::make_shared<TOpJoin>(filter->GetInput(), map, filter->Pos, "Cross", joinKeys);

        conjuncts[conjunctIdx] = MakeColumnAccess(compareResult, filter->Pos, &ctx.ExprCtx, &props);
    }

    props.Subplans.Remove(iu);
    // If there was a single conjunct, we can get rid of the filter completely
    if (conjuncts.empty()) {
        return join;
    }

    // Otherwise, we need to pack the remaining conjuncts back into the filter
    return std::make_shared<TOpFilter>(join, filter->Pos, MakeConjunction(conjuncts, props.PgSyntax));
}
}
}
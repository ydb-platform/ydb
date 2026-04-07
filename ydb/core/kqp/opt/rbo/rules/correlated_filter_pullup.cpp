#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

// Pull up correlated filter inside a subplan
// We match the parent of the filter, currently we support only Map and Aggregate

bool TPullUpCorrelatedFilterRule::MatchAndApply(TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Map && input->Kind != EOperator::Aggregate) {
        return false;
    }

    auto unaryOp = CastOperator<IUnaryOperator>(input);

    if (unaryOp->GetInput()->Kind != EOperator::Filter || !unaryOp->GetInput()->IsSingleConsumer()) {
        return false;
    }

    auto filter = CastOperator<TOpFilter>(unaryOp->GetInput());

    if (filter->GetInput()->Kind != EOperator::AddDependencies || !filter->GetInput()->IsSingleConsumer()) {
        return false;
    }

    auto deps = CastOperator<TOpAddDependencies>(filter->GetInput());

    auto conjuncts = filter->FilterExpr.SplitConjunct();
    TVector<TExpression> joinConditions;
    TVector<TExpression> otherFilters;
    for (auto const & conj : conjuncts) {
        if (conj.MaybeJoinCondition()) {
            joinConditions.push_back(conj);
        } else {
            otherFilters.push_back(conj);
        }
    }
    
    if (joinConditions.empty()) {
        return false;
    }

    // Select a subset of join conditions that cover all dependencies
    TVector<TExpression> dependentSubset;
    TVector<TExpression> remainderSubset;
    THashSet<TInfoUnit, TInfoUnit::THashFunction> correlated;

    for (const auto & jc : joinConditions) {
        TJoinCondition cond(jc);
        if (std::find(deps->Dependencies.begin(), deps->Dependencies.end(), cond.GetLeftIU()) != deps->Dependencies.end()) {
            dependentSubset.push_back(jc);
            correlated.insert(cond.GetRightIU());
        }
        else if (std::find(deps->Dependencies.begin(), deps->Dependencies.end(), cond.GetRightIU()) != deps->Dependencies.end()) {
            dependentSubset.push_back(jc);
            correlated.insert(cond.GetLeftIU());
        } else {
            remainderSubset.push_back(jc);
        }
    }

    // Make sure all dependencies are covered in this filter
    // FIXME: This is a current limitation
    if (correlated.size() != deps->Dependencies.size()) {
        return false;
    }

    // Split the predicate into a remaining and new part with only dependent conditions
    TIntrusivePtr<IOperator> remainingFilter = deps->GetInput();
    if (!otherFilters.empty() || !remainderSubset.empty()) {
        auto newConjuncts = otherFilters;
        newConjuncts.insert(newConjuncts.end(), remainderSubset.begin(), remainderSubset.end());
        auto expr = MakeConjunction(newConjuncts, props.PgSyntax);
        remainingFilter = MakeIntrusive<TOpFilter>(deps->GetInput(), remainingFilter->Pos, expr);
    }

    auto newExpr = MakeConjunction(dependentSubset, props.PgSyntax);

    if (input->Kind == EOperator::Map) {
        auto map = CastOperator<TOpMap>(input);

        // Stop if we compute something from one of the dependent columns, except for Just
        for (const auto & mapEl : map->MapElements) {
            for (const auto & iu : mapEl.GetExpression().GetInputIUs(false, true)) {
                if (correlated.contains(iu)) {
                    if (!mapEl.IsRename() && !mapEl.GetExpression().IsSingleCallable({"Just"})) {
                        return false;
                    }
                }
            }
        }

        TVector<TInfoUnit> addToMap;

        // Add all dependend columns to projecting map output
        auto mapOutput = map->GetOutputIUs();
        for (const auto & iu : correlated) {
            if (std::find(mapOutput.begin(), mapOutput.end(), iu) == mapOutput.end()) {
                addToMap.push_back(iu);
            }
        }

        // First we add needed columns to the map, if its a projection map
        if (!addToMap.empty() && map->Project) {
            for (const auto & add : addToMap) {
                map->MapElements.push_back(TMapElement(add, add, map->Pos, &ctx.ExprCtx, &props));
            }
        }

        filter->FilterExpr = newExpr;
        map->SetInput(remainingFilter);
        deps->SetInput(map);
        input = filter;

        return true;

    } else if (input->Kind == EOperator::Aggregate) {
        auto aggregate = CastOperator<TOpAggregate>(input);

        // Add all missing dependent columns to the group by list
        for (const auto & corr : correlated) {
            if (std::find(aggregate->KeyColumns.begin(), aggregate->KeyColumns.end(), corr) == aggregate->KeyColumns.end()) {
                aggregate->KeyColumns.push_back(corr);
            }
        }

        filter->FilterExpr = newExpr;
        aggregate->SetInput(remainingFilter);
        deps->SetInput(aggregate);
        input = filter;

        return true;
    } else {
        return false;
    }

}

}
}
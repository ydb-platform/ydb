#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {
    
// Rewrite a single scalar subplan into a cross-join for uncorrelated queries
// or into a left join for correlated (assuming at most one tuple in the output of each subquery)
// FIXME: Need to do correct general case decorellation in the future

bool TInlineScalarSubplanRule::MatchAndApply(TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    auto subplanIUs = input->GetSubplanIUs(props);
    TVector<TInfoUnit> scalarIUs;
    for (const auto& iu : subplanIUs) {
        auto subplanEntry = props.Subplans.PlanMap.at(iu);
        if (subplanEntry.Type == ESubplanType::EXPR) {
            scalarIUs.push_back(iu);
            break;
        }
    }

    if (scalarIUs.empty()) {
        return false;
    }

    auto scalarIU = scalarIUs[0];
    auto subplanEntry = props.Subplans.PlanMap.at(scalarIU);
    auto subplan = CastOperator<IOperator>(subplanEntry.Plan);
    auto subplanResIU = GetSubplanResultIUs(subplan)[0];
    auto subplanResType = subplan->GetIUType(subplanResIU);

    Y_ENSURE(MatchOperator<IUnaryOperator>(input));
    auto unaryOp = CastOperator<IUnaryOperator>(input);

    auto child = unaryOp->GetInput();

    // Check whether this is a correlated subplan with filter pushed up
    // FIXME: if the filter got stuck we will crash later in the optimizer
    if (subplan->Kind == EOperator::Filter && CastOperator<TOpFilter>(subplan)->GetInput()->Kind == EOperator::AddDependencies) {
        auto subplanFilter = CastOperator<TOpFilter>(subplan);
        auto addDeps = CastOperator<TOpAddDependencies>(subplanFilter->GetInput());
        auto uncorrSubplan = addDeps->GetInput();

        TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys;
        TVector<TExpression> joinFilters;
        THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> subplanOutputRenames;

        auto leftIUs = child->GetOutputIUs();
        auto rightIUs = uncorrSubplan->GetOutputIUs();
        THashSet<TInfoUnit, TInfoUnit::THashFunction> usedIUs;
        usedIUs.insert(leftIUs.begin(), leftIUs.end());
        usedIUs.insert(rightIUs.begin(), rightIUs.end());
        auto makeInternalIU = [&]() {
            for (;;) {
                auto iu = TInfoUnit("_rbo_arg_" + std::to_string(props.InternalVarIdx++), false);
                if (usedIUs.insert(iu).second) {
                    return iu;
                }
            }
        };

        for (const auto& iu : rightIUs) {
            if (ContainsInfoUnit(leftIUs, iu) && !subplanOutputRenames.contains(iu)) {
                subplanOutputRenames.emplace(iu, makeInternalIU());
            }
        }

        auto conjuncts = subplanFilter->FilterExpr.SplitConjunct();

        for (const auto & conj : conjuncts) {
            if (!conj.MaybeEquiJoinCondition()) {
                joinFilters.push_back(conj);
                continue;
            }

            TEquiJoinCondition jc(conj);
            TInfoUnit leftKey = jc.GetLeftIU();
            TInfoUnit rightKey = jc.GetRightIU();

            if (std::find(addDeps->Dependencies.begin(), addDeps->Dependencies.end(), rightKey) != addDeps->Dependencies.end()) {
                std::swap(leftKey, rightKey);
            } else if (std::find(addDeps->Dependencies.begin(), addDeps->Dependencies.end(), leftKey) == addDeps->Dependencies.end()) {
                Y_ENSURE(false, "Correlated filter missing join condition");
            }

            if (ContainsInfoUnit(leftIUs, rightKey)) {
                const auto renameIt = subplanOutputRenames.find(rightKey);
                if (renameIt != subplanOutputRenames.end()) {
                    rightKey = renameIt->second;
                } else {
                    auto newKey = makeInternalIU();
                    subplanOutputRenames.emplace(rightKey, newKey);
                    rightKey = newKey;
                }
            }

            joinKeys.push_back(std::make_pair(leftKey, rightKey));
        }

        if (!subplanOutputRenames.empty()) {
            TVector<TMapElement> renameElements;
            renameElements.reserve(subplanOutputRenames.size());
            for (const auto& [from, to] : subplanOutputRenames) {
                renameElements.emplace_back(to, from, subplan->Pos, &ctx.ExprCtx, &props);
            }
            uncorrSubplan = MakeIntrusive<TOpMap>(uncorrSubplan, subplan->Pos, renameElements);

            // Non-equi conjuncts were captured before the output rename; rewrite
            // them too, otherwise they keep references to hidden right-side IUs.
            for (auto& joinFilter : joinFilters) {
                joinFilter = joinFilter.ApplyRenames(subplanOutputRenames);
            }
        }

        auto joinedSubplanResIU = subplanResIU;
        if (const auto renameIt = subplanOutputRenames.find(joinedSubplanResIU); renameIt != subplanOutputRenames.end()) {
            joinedSubplanResIU = renameIt->second;
        }

        auto leftJoin = MakeIntrusive<TOpJoin>(child, uncorrSubplan, subplan->Pos, "Left", joinKeys, joinFilters);

        if (input->Kind == EOperator::Filter) {
            auto outerFilter = CastOperator<TOpFilter>(input);
            outerFilter->FilterExpr = outerFilter->FilterExpr.ApplyRenames({{scalarIU, joinedSubplanResIU}});
            outerFilter->SetInput(leftJoin);
        } else {
            TVector<TMapElement> renameElements;
            renameElements.emplace_back(scalarIU, joinedSubplanResIU, subplan->Pos, &ctx.ExprCtx, &props);
            auto rename = MakeIntrusive<TOpMap>(leftJoin, subplan->Pos, renameElements);
            unaryOp->SetInput(rename);
        }
    }

    // If its a correlated subplan where filter pull up didn't succeed, throw an exception
    else if (subplanEntry.DependentIUs.size()) {
        Y_ENSURE(false, "Decorrelation via filter pull up didn't succeed");
    }

    // Otherwise we assume an uncorrelated supbplan
    // Here we don't assume at most one tuple from the subplan
    else {
        auto emptySource = MakeIntrusive<TOpEmptySource>(subplan->Pos);

        TVector<TMapElement> mapElements;

        // FIXME: This works only for postgres types, because they are null-compatible
        // For YQL types we will need to handle optionality
        mapElements.emplace_back(scalarIU, MakeNothing(subplan->Pos, subplanResType, &ctx.ExprCtx));
        auto map = MakeIntrusive<TOpMap>(emptySource, subplan->Pos, mapElements);

        TVector<TMapElement> renameElements;
        renameElements.emplace_back(scalarIU, subplanResIU, subplan->Pos, &ctx.ExprCtx, &props);
        auto rename = MakeIntrusive<TOpMap>(subplan, subplan->Pos, renameElements);
        rename->Props.EnsureAtMostOne = true;

        auto unionAll = MakeIntrusive<TOpUnionAll>(rename, map, subplan->Pos, true);

        auto limit = MakeIntrusive<TOpLimit>(unionAll, subplan->Pos, MakeConstant("Uint64", "1", subplan->Pos, &ctx.ExprCtx), EOpPhase::Undefined);
    
        TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys;
        auto cross = MakeIntrusive<TOpJoin>(child, limit, subplan->Pos, "Cross", joinKeys);
        unaryOp->SetInput(cross);
    }

    props.Subplans.Remove(scalarIU);

    return true;
}
}
}

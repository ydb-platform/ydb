#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {
    
// Rewrite a single scalar subplan into a cross-join for uncorrelated queries
// or into a left join for correlated (assuming at most one tuple in the output of each subquery)
// FIXME: Need to do correct general case decorellation in the future

bool TInlineScalarSubplanRule::MatchAndApply(std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
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
    auto subplan = props.Subplans.PlanMap.at(scalarIU).Plan;
    auto subplanResIU = subplan->GetOutputIUs()[0];
    auto subplanResType = subplan->GetIUType(subplanResIU);

    auto unaryOp = std::dynamic_pointer_cast<IUnaryOperator>(input);
    Y_ENSURE(unaryOp);

    auto child = unaryOp->GetInput();

    // Check whether this is a correlated subplan with filter pushed up
    // FIXME: if the filter got stuck we will crash later in the optimizer
    if (subplan->Kind == EOperator::Filter && CastOperator<TOpFilter>(subplan)->GetInput()->Kind == EOperator::AddDependencies) {
        auto filter = CastOperator<TOpFilter>(subplan);
        auto addDeps = CastOperator<TOpAddDependencies>(filter->GetInput());
        auto uncorrSubplan = addDeps->GetInput();

        TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys;

        TVector<TMapElement> mappings;
        mappings.push_back(TMapElement(subplanResIU, subplanResIU, filter->Pos, &ctx.ExprCtx, &props));

        auto leftIUs = child->GetOutputIUs();
        bool conflictsWithLeft = false;

        auto conjuncts = filter->FilterExpr.SplitConjunct();

        for (const auto & conj : conjuncts) {
            if (!conj.MaybeJoinCondition()) {
                continue;
            }

            TJoinCondition jc(conj);
            TInfoUnit leftKey = jc.GetLeftIU();
            TInfoUnit rightKey = jc.GetRightIU();

            if (std::find(addDeps->Dependencies.begin(), addDeps->Dependencies.end(), rightKey) != addDeps->Dependencies.end()) {
                std::swap(leftKey, rightKey);
            } else if (std::find(addDeps->Dependencies.begin(), addDeps->Dependencies.end(), leftKey) == addDeps->Dependencies.end()) {
                Y_ENSURE(false, "Correlated filter missing join condition");
            }

            if (std::find(leftIUs.begin(), leftIUs.end(), rightKey) != leftIUs.end()) {
                auto newKey = TInfoUnit("_rbo_arg_" + std::to_string(props.InternalVarIdx++), false);
                mappings.push_back(TMapElement(newKey, rightKey, filter->Pos, &ctx.ExprCtx, &props));
                rightKey = newKey;
                conflictsWithLeft = true;
            } else {
                mappings.push_back(TMapElement(rightKey, rightKey, filter->Pos, &ctx.ExprCtx, &props));
            }

            joinKeys.push_back(std::make_pair(leftKey, rightKey));
        }

        if (conflictsWithLeft) {
            uncorrSubplan = std::make_shared<TOpMap>(uncorrSubplan, uncorrSubplan->Pos, mappings, true);
        }

        auto leftJoin = std::make_shared<TOpJoin>(child, uncorrSubplan, subplan->Pos, "Left", joinKeys);

        TVector<TMapElement> renameElements;
        renameElements.emplace_back(scalarIU, subplanResIU, subplan->Pos, &ctx.ExprCtx, &props);
        auto rename = std::make_shared<TOpMap>(leftJoin, subplan->Pos, renameElements, false);
        unaryOp->SetInput(rename);
    }

    // Otherwise we assume an uncorrelated supbplan
    // Here we don't assume at most one tuple from the subplan
    else {
        auto emptySource = std::make_shared<TOpEmptySource>(subplan->Pos);

        TVector<TMapElement> mapElements;

        // FIXME: This works only for postgres types, because they are null-compatible
        // For YQL types we will need to handle optionality
        mapElements.emplace_back(scalarIU, MakeNothing(subplan->Pos, subplanResType, &ctx.ExprCtx));
        auto map = std::make_shared<TOpMap>(emptySource, subplan->Pos, mapElements, true);

        TVector<TMapElement> renameElements;
        renameElements.emplace_back(scalarIU, subplanResIU, subplan->Pos, &ctx.ExprCtx, &props);
        auto rename = std::make_shared<TOpMap>(subplan, subplan->Pos, renameElements, true);
        rename->Props.EnsureAtMostOne = true;

        auto unionAll = std::make_shared<TOpUnionAll>(rename, map, subplan->Pos, true);

        auto limit = std::make_shared<TOpLimit>(unionAll, subplan->Pos, MakeConstant("Uint64", "1", subplan->Pos, &ctx.ExprCtx));
    
        TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys;
        auto cross = std::make_shared<TOpJoin>(child, limit, subplan->Pos, "Cross", joinKeys);
        unaryOp->SetInput(cross);
    }

    props.Subplans.Remove(scalarIU);

    return true;
}
}
}
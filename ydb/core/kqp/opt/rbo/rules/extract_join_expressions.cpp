#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

// Currently we only extract simple expressions where there is only one variable on either side

bool TExtractJoinExpressionsRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Filter && CastOperator<TOpFilter>(input)->GetInput()->Kind == EOperator::Join;
}

bool TExtractJoinExpressionsRule::MatchAndApply(TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    if (input->Kind != EOperator::Filter) {
        return false;
    }

    auto filter = CastOperator<TOpFilter>(input);
    if (filter->GetInput()->Kind != EOperator::Join) {
        return false;
    }

    auto join = CastOperator<TOpJoin>(filter->GetInput());
    auto leftInput = join->GetLeftInput();
    auto rightInput = join->GetRightInput();

    auto conjuncts = filter->GetFilterExpression().SplitConjunct();

    TVector<TExpression> newConjuncts;
    TMapIUs mapElements;

    for (auto & c : conjuncts) {
        if (c.MaybeEquiJoinCondition()) {
            newConjuncts.push_back(c);
        }
        else if (c.MaybeExprEquiJoinCondition()) {
            // Check that the condition won't be pushed to either side of the join
            auto exprIUs = c.GetInputIUs();
            if (exprIUs.IsSubsetOf(leftInput->GetOutputIUs()) || exprIUs.IsSubsetOf(rightInput->GetOutputIUs())){
                newConjuncts.push_back(c);
                continue;
            }

            TEquiJoinCondition cond(c);
            TMappedIUs<TExprNode::TPtr> definitions;
            TNodeOnNodeOwnedMap replaceMap;
            if (cond.ExtractExpressions(replaceMap, definitions)) {
                for (const auto& [iu, expr] : definitions.Items()) {
                    mapElements.Add(iu, TExpression(expr, &ctx.ExprCtx, &props));
                }
                newConjuncts.push_back(c.ApplyReplaceMap(replaceMap, ctx));
            } else {
                newConjuncts.push_back(c);
            }
        } else {
            newConjuncts.push_back(c);
        }
    }

    if (mapElements.Keys().Empty()) {
        return false;
    }

    filter->SetFilterExpression(MakeConjunction(newConjuncts, props.PgSyntax));
    auto newMap = MakeIntrusive<TOpMap>(join, input->Pos, std::move(mapElements), true);
    filter->SetInput(std::move(newMap));
    return true;
}
}
}

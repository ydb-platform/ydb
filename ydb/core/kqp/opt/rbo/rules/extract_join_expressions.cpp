#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

// Currently we only extract simple expressions where there is only one variable on either side

bool TExtractJoinExpressionsRule::MatchAndApply(TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(props);

    if (input->Kind != EOperator::Filter) {
        return false;
    }

    auto filter = CastOperator<TOpFilter>(input);
    auto conjuncts = filter->FilterExpr.SplitConjunct();

    TVector<TExpression> newConjuncts;
    TVector<TMapElement> mapElements;

    for (auto & c : conjuncts) {
        if (c.MaybeJoinCondition(false)) {
            newConjuncts.push_back(c);
        }
        else if (c.MaybeJoinCondition(true)) {
            TJoinCondition cond(c);
            TVector<std::pair<TInfoUnit, TExprNode::TPtr>> renameMap;
            TNodeOnNodeOwnedMap replaceMap;
            if (cond.ExtractExpressions(replaceMap, renameMap)) {
                for (auto const & [iu, expr] : renameMap) {
                    mapElements.emplace_back(iu, TExpression(expr, &ctx.ExprCtx, &props));
                }
                newConjuncts.push_back(c.ApplyReplaceMap(replaceMap, ctx));
            } else {
                newConjuncts.push_back(c);
            }
        } else {
            newConjuncts.push_back(c);
        }
    }

    if (mapElements.empty()) {
        return false;
    }

    filter->FilterExpr = MakeConjunction(newConjuncts, props.PgSyntax);
    auto newMap = MakeIntrusive<TOpMap>(filter->GetInput(), input->Pos, mapElements, false);
    filter->SetInput(newMap);
    return true;
}
}
}
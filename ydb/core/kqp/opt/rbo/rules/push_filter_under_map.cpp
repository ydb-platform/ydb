#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

bool TPushFilterUnderMapRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Filter &&
        input->GetChildren().front()->Kind == EOperator::Map;
}

TIntrusivePtr<IOperator> TPushFilterUnderMapRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {

    if (input->Kind != EOperator::Filter) {
        return input;
    }

    auto filter = CastOperator<TOpFilter>(input);
    if (filter->GetInput()->Kind != EOperator::Map) {
        return input;
    }

    auto map = CastOperator<TOpMap>(filter->GetInput());

    auto conjuncts = filter->GetFilterExpression().SplitConjunct();
    TVector<TExpression> pushedFilters;
    TVector<TExpression> remainingFilters;

    const auto& newMapColumns = map->GetMapElements().Keys();

    for (const auto & c : conjuncts) {
        if (ReferencesUnresolvedSubplan(c, props)) {
            remainingFilters.push_back(c);
            continue;
        }
        // UNION type reconciliation can lift a non-null column with Just.
        // Substitute that total, deterministic conversion into the predicate
        // so a filter can cross the Map without dropping NULL semantics.
        TNodeOnNodeOwnedMap replacements;
        const auto row = c.GetLambda()->Head().ChildPtr(0);
        VisitExpr(c.GetExpressionBody(), [&](const TExprNode::TPtr& node) {
            if (!node->IsCallable("Member") || node->Child(0) != row.Get()) { return true; }
            const auto* element = map->GetMapElements().Find(GetMemberId(*node));
            if (element && element->GetExpression().IsSingleCallable({"Just"})
                && element->DependsOnlyOn(map->GetInput()->GetOutputIUs())) {
                const auto& expression = element->GetExpression();
                replacements.emplace(node.Get(), ctx.ExprCtx.ReplaceNode(expression.GetExpressionBody(),
                    expression.GetLambda()->Head().Head(), row));
            }
            return false;
        });
        const auto pushed = replacements.empty() ? c : c.ApplyReplaceMap(replacements, ctx);
        if (!pushed.GetInputIUs(false, true).HasAny(newMapColumns)) {
            pushedFilters.push_back(pushed);
        } else {
            remainingFilters.push_back(c);
        }
    }

    if (pushedFilters.empty()) {
        return input;
    }

    filter->SetInput(map->GetInput());
    filter->SetFilterExpression(MakeConjunction(pushedFilters, props.PgSyntax));
    map->SetInput(filter);

    if (remainingFilters.size()) {
        auto pushedFilterExpr = MakeConjunction(remainingFilters, props.PgSyntax);
        return MakeIntrusive<TOpFilter>(map, map->Pos, pushedFilterExpr);
    } else {
        return map;
    }
}
}
}

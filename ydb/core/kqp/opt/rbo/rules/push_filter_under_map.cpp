#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

bool TPushFilterUnderMapRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Filter &&
        input->GetChildren().front()->Kind == EOperator::Map;
}

TIntrusivePtr<IOperator> TPushFilterUnderMapRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {

    Y_UNUSED(ctx);

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
        if (!ReferencesUnresolvedSubplan(c, props) && !c.GetInputIUs(false, true).HasAny(newMapColumns)) {
            pushedFilters.push_back(c);
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

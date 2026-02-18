#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

TIntrusivePtr<IOperator> TPushFilterUnderMapRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {

    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Filter) {
        return input;
    }

    auto filter = CastOperator<TOpFilter>(input);
    if (filter->GetInput()->Kind != EOperator::Map) {
        return input;
    }

    auto map = CastOperator<TOpMap>(filter->GetInput());

    if (map->Project) {
        return input;
    }

    auto conjuncts = filter->FilterExpr.SplitConjunct();
    TVector<TExpression> pushedFilters;
    TVector<TExpression> remainingFilters;

    TVector<TInfoUnit> newMapColumns;
    for (const auto & mapEl : map->MapElements) {
        newMapColumns.push_back(mapEl.GetElementName());
    }

    for (const auto & c : conjuncts) {
        if (IUSetIntersect(c.GetInputIUs(false,true), newMapColumns).empty()){
            pushedFilters.push_back(c);
        } else {
            remainingFilters.push_back(c);
        }
    }

    if (pushedFilters.empty()) {
        return input;
    }

    filter->SetInput(map->GetInput());
    filter->FilterExpr = MakeConjunction(pushedFilters, props.PgSyntax);
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
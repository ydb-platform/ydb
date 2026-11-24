#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

bool TPushFilterThroughUnionAllRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Filter && input->GetChildren().front()->Kind == EOperator::UnionAll;
}

TIntrusivePtr<IOperator> TPushFilterThroughUnionAllRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);

    if (input->Kind != EOperator::Filter) {
        return input;
    }

    auto filter = CastOperator<TOpFilter>(input);
    if (filter->GetInput()->Kind != EOperator::UnionAll) {
        return input;
    }

    auto unionAll = CastOperator<TOpUnionAll>(filter->GetInput());
    const auto& outputs = unionAll->GetColumns().Keys();

    const auto conjuncts = filter->GetFilterExpression().SplitConjunct();
    TVector<TExpression> pushedFilters;
    TVector<TExpression> remainingFilters;

    // A pushed conjunct is renamed to the columns of each input, so it can only use the union outputs.
    for (const auto& conj : conjuncts) {
        if (!ReferencesUnresolvedSubplan(conj, props) && conj.GetInputIUs(true, true).IsSubsetOf(outputs)) {
            pushedFilters.push_back(conj);
        } else {
            remainingFilters.push_back(conj);
        }
    }

    if (pushedFilters.empty()) {
        return input;
    }

    const auto pushedFilterExpr = MakeConjunction(pushedFilters, props.PgSyntax);
    TVector<TIntrusivePtr<IOperator>> newInputs;
    newInputs.reserve(unionAll->GetInputs().size());
    for (size_t index = 0; index < unionAll->GetInputs().size(); ++index) {
        TSubstitutions renames;
        for (const auto& [output, row] : unionAll->GetColumns().Items()) {
            renames.Add(output, row.Inputs[index]);
        }
        newInputs.push_back(MakeIntrusive<TOpFilter>(unionAll->GetInput(index), filter->Pos, pushedFilterExpr.ApplyRenames(renames)));
    }
    unionAll->SetInputs(std::move(newInputs));

    if (remainingFilters.empty()) {
        return unionAll;
    }

    return MakeIntrusive<TOpFilter>(unionAll, filter->Pos, MakeConjunction(remainingFilters, props.PgSyntax));
}

} // namespace NKqp
} // namespace NKikimr

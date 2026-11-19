#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

bool TPushFilterThroughAggregateRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Filter &&
        input->GetChildren().front()->Kind == EOperator::Aggregate &&
        !CastOperator<TOpAggregate>(*input->GetChildren().front()).GetKeyColumns().Items().empty();
}

TIntrusivePtr<IOperator> TPushFilterThroughAggregateRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {

    Y_UNUSED(ctx);

    if (input->Kind != EOperator::Filter) {
        return input;
    }

    auto filter = CastOperator<TOpFilter>(input);
    if (filter->GetInput()->Kind != EOperator::Aggregate) {
        return input;
    }

    auto aggregate = CastOperator<TOpAggregate>(filter->GetInput());

    // We cannot push filter through scalar aggregate.
    if (aggregate->GetKeyColumns().Items().empty()) {
        return input;
    }

    TSubstitutions keys;
    if (aggregate->IsDistinctAll()) {
        const auto& keyColumns = aggregate->GetKeyColumns().Unordered();
        for (const auto& [output, traits] : aggregate->GetAggregationTraits().Items()) {
            if (traits.AggFunction == "distinct" && keyColumns.Contains(traits.Input)) {
                keys.Add(output, traits.Input);
            }
        }
    } else {
        for (const auto iu : aggregate->GetKeyColumns().Items()) {
            keys.Add(iu, iu);
        }
    }

    auto conjuncts = filter->GetFilterExpression().SplitConjunct();
    TVector<TExpression> pushedFilters;
    TVector<TExpression> remainingFilters;

    for (const auto& conj : conjuncts) {
        if (!ReferencesUnresolvedSubplan(conj, props) && conj.GetInputIUs(false, true).IsSubsetOf(keys.Keys())) {
            pushedFilters.push_back(aggregate->IsDistinctAll() ? conj.ApplyRenames(keys) : conj);
        } else {
            remainingFilters.push_back(conj);
        }
    }

    if (pushedFilters.empty()) {
        return input;
    }

    filter->SetInput(aggregate->GetInput());
    filter->SetFilterExpression(MakeConjunction(pushedFilters, props.PgSyntax));
    aggregate->SetInput(filter);

    if (remainingFilters.size()) {
        auto remainingFilterExpr = MakeConjunction(remainingFilters, props.PgSyntax);
        return MakeIntrusive<TOpFilter>(aggregate, aggregate->Pos, remainingFilterExpr);
    } else {
        return aggregate;
    }
}
}
}

#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>

namespace NKikimr::NKqp {

namespace {

bool IsSuitableToExpandDistinctAggregation(const TIntrusivePtr<IOperator>& input) {
    if (input->GetKind() != EOperator::Aggregate) {
        return false;
    }

    const auto& aggTraitsList = CastOperator<TOpAggregate>(input)->GetAggregationTraits();
    return std::any_of(aggTraitsList.begin(), aggTraitsList.end(), [](const TOpAggregationTraits& aggTraits) { return aggTraits.Distinct; });
}

} // anonymous namespace

TIntrusivePtr<IOperator> TExpandDistinctAggregationRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& rboCtx, TPlanProps& props) {
    Y_UNUSED(props);
    Y_UNUSED(rboCtx);

    if (!IsSuitableToExpandDistinctAggregation(input)) {
        return input;
    }

    const auto aggregate = CastOperator<TOpAggregate>(input);
    const auto& aggTraitsList = aggregate->GetAggregationTraits();
    Y_ENSURE(aggTraitsList.size() == 1, "Multiple distinct is not supported.");
    const auto& aggTraits = aggTraitsList.front();
    TVector<TInfoUnit> distinctKeys = aggregate->GetKeyColumns();

    // Split into distinct and original aggregation.
    TVector<TOpAggregationTraits> distinctTraitsList;
    for (const auto& key: distinctKeys) {
        distinctTraitsList.emplace_back(TOpAggregationTraits(key, "distinct", key));
    }
    distinctTraitsList.emplace_back(TOpAggregationTraits(aggTraits.OriginalColName, "distinct", aggTraits.OriginalColName));
    distinctKeys.emplace_back(aggTraits.OriginalColName);

    const TIntrusivePtr<IOperator> distinctAggregation =
        MakeIntrusive<TOpAggregate>(aggregate->GetInput(), distinctTraitsList, distinctKeys, EOpPhase::Undefined,
                                    /*distinctAll=*/true, input->Pos);
    TOpAggregationTraits aggregationTraits = aggTraits;
    aggregationTraits.Distinct = false;
    const TVector<TOpAggregationTraits> newAggTraitsList{aggregationTraits};
    return MakeIntrusive<TOpAggregate>(distinctAggregation, newAggTraitsList, aggregate->GetKeyColumns(), EOpPhase::Undefined, /*distinctAll=*/false,
                                       input->Pos);
}

} // namespace NKikimr::NKqp

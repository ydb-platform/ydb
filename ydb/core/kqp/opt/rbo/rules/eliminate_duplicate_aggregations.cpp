#include "kqp_rules_include.h"

namespace NKikimr::NKqp {

namespace {

// The lambda the Map below the aggregate computes `input` with, or nullptr for any other input.
const TExprNode* FindAggregationInputLambda(const IOperator& input, TInfoUnitId iu) {
    if (input.Kind != EOperator::Map) {
        return nullptr;
    }
    const auto* element = CastOperator<TOpMap>(input).FindOutputElement(iu);
    return element ? element->GetExpression().GetLambda().Get() : nullptr;
}

// Whether two aggregations compute the same value: the same function of the same input column, or of two
// equal expressions, as two COUNT(*) are.
bool IsSameAggregation(const TOpAggregationTraits& left, const TOpAggregationTraits& right, const IOperator& input) {
    if (left.AggFunction != right.AggFunction || left.Distinct != right.Distinct || left.Unwrap != right.Unwrap) {
        return false;
    }
    if (left.Input == right.Input) {
        return true;
    }
    const auto* leftLambda = FindAggregationInputLambda(input, left.Input);
    const auto* rightLambda = FindAggregationInputLambda(input, right.Input);
    return leftLambda && rightLambda && CompareExprTrees(leftLambda, rightLambda);
}

} // anonymous namespace

bool TEliminateDuplicateAggregationsRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    if (input->Kind != EOperator::Aggregate) {
        return false;
    }
    const auto& aggregate = CastOperator<TOpAggregate>(*input);
    // DISTINCT outputs its keys, and a split aggregate merges the states of its inputs.
    if (aggregate.IsDistinctAll() || aggregate.GetAggregationPhase() != EOpPhase::Undefined) {
        return false;
    }
    // Identical aggregations have the same function.
    THashSet<TStringBuf> functions;
    for (const auto& [output, traits] : aggregate.GetAggregationTraits().Items()) {
        if (!functions.insert(traits.AggFunction).second) {
            return true;
        }
    }
    return false;
}

TIntrusivePtr<IOperator> TEliminateDuplicateAggregationsRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input,
                                                                                  TRBOContext& ctx, TPlanProps& props) {
    const auto aggregate = CastOperator<TOpAggregate>(input);

    TAggregationIUs aggregations;
    TMapIUs copies;
    for (const auto& [output, traits] : aggregate->GetAggregationTraits().Items()) {
        const auto& kept = aggregations.Items();
        const auto same = FindIf(
            kept, [&](const auto& item) { return IsSameAggregation(item.second, traits, *aggregate->GetInput()); });
        if (same != kept.end()) {
            copies.Add(output, MakeColumnAccess(same->first, aggregate->Pos, &ctx.ExprCtx, &props));
        } else {
            aggregations.Add(output, traits);
        }
    }
    if (copies.Keys().Empty()) {
        return input;
    }

    // As in the old optimizer, the aggregate computes each aggregation once, and the Map copies the result to the
    // outputs of the others.
    aggregate->SetAggregationTraits(std::move(aggregations));
    return MakeIntrusive<TOpMap>(aggregate, aggregate->Pos, std::move(copies));
}

} // namespace NKikimr::NKqp

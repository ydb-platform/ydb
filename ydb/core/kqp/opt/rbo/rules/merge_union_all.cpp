#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

namespace {

bool CanMergeInput(const TOpUnionAll& unionAll, const TIntrusivePtr<IOperator>& input) {
    if (input->Kind != EOperator::UnionAll) {
        return false;
    }

    if (!input->IsSingleConsumer()) {
        return false;
    }

    const auto innerUnionAll = CastOperator<TOpUnionAll>(input);
    if (unionAll.Ordered || innerUnionAll->Ordered) {
        return false;
    }

    for (const auto& column : unionAll.Columns) {
        if (!ContainsInfoUnit(innerUnionAll->Columns, column)) {
            return false;
        }
    }

    return true;
}

} // anonymous namespace

bool TMergeUnionAllRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    if (input->Kind != EOperator::UnionAll) {
        return false;
    }

    for (const auto& child : input->Children) {
        if (child->Kind == EOperator::UnionAll) {
            return true;
        }
    }
    return false;
}

bool TMergeUnionAllRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::UnionAll) {
        return false;
    }

    auto unionAll = CastOperator<TOpUnionAll>(input);

    TVector<TIntrusivePtr<IOperator>> newInputs;
    newInputs.reserve(unionAll->Children.size());
    bool merged = false;
    for (const auto& child : unionAll->Children) {
        if (!CanMergeInput(*unionAll, child)) {
            newInputs.push_back(child);
            continue;
        }

        // Splice the inner union branches in place, preserving their order.
        for (const auto& innerChild : child->Children) {
            newInputs.push_back(innerChild);
        }
        merged = true;
    }

    if (!merged) {
        return false;
    }

    unionAll->SetInputs(std::move(newInputs));
    return true;
}

} // namespace NKqp
} // namespace NKikimr

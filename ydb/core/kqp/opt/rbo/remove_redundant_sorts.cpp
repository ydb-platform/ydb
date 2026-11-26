#include "kqp_rbo.h"

#include <util/generic/hash_set.h>

namespace NKikimr::NKqp {

namespace {

// Whether an operator needs the order of its input rows, given whether its consumers need the order of its output.
bool NeedsInputOrder(const IOperator& op, bool outputOrderNeeded) {
    switch (op.Kind) {
        case EOperator::Map:
        case EOperator::Replicate:
            return outputOrderNeeded;
        case EOperator::Limit:
            return CastOperator<TOpLimit>(op).OrderedInput || outputOrderNeeded;
        // A filter does not keep the order of its input, as in YQL, where a Filter over a sorted input is unordered.
        case EOperator::Filter:
        case EOperator::Sort:
        case EOperator::Aggregate:
        case EOperator::GroupingSets:
        case EOperator::Join:
            return false;
        case EOperator::UnionAll:
            return CastOperator<TOpUnionAll>(op).Ordered && outputOrderNeeded;
        default:
            // Windows, writes and the other operators keep the order of their input.
            return true;
    }
}

bool IsSortWithoutLimit(const IOperator& op) {
    return op.Kind == EOperator::Sort && !CastOperator<TOpSort>(op).IsTopSort();
}

} // anonymous namespace

TRemoveRedundantSortsStage::TRemoveRedundantSortsStage(TString stageName)
    : IRBOStage(std::move(stageName))
{
    Props = ERuleProperties::RequireParents;
}

void TRemoveRedundantSortsStage::RunStage(TOpRoot& root, TRBOContext& ctx) {
    THashSet<const IOperator*> orderNeeded;
    if (!root.PlanProps.UnorderedResult) {
        orderNeeded.insert(root.GetInput().Get());
    }

    // Subqueries are inlined by now, so the plan has no subplans to follow.
    // A reversed postorder visits every consumer of an operator, including each Replicate port, before the operator.
    const TOpTraversal traversal(IterateSubtree(root.GetInput().Get()).begin());
    TVector<TIntrusivePtr<IOperator>> sorts;
    for (auto it = traversal.rbegin(); it != traversal.rend(); ++it) {
        const auto& op = *it->Current;
        const bool outputOrderNeeded = orderNeeded.contains(&op);
        // A shared sort has no single input slot to replace.
        if (IsSortWithoutLimit(op) && !outputOrderNeeded && op.Parents.size() == 1 && op.Parents.front().first &&
            op.Parents.front().first->Kind != EOperator::Replicate) {
            sorts.emplace_back(it->Current);
        }

        if (NeedsInputOrder(op, outputOrderNeeded)) {
            for (const auto* child : op.GetChildren()) {
                orderNeeded.insert(child);
            }
        }
    }

    if (sorts.empty()) {
        return;
    }

    // The sorts go from the top down. A sort right below another removed sort is skipped together with it.
    THashSet<const IOperator*> removed;
    for (const auto& sort : sorts) {
        removed.insert(sort.Get());
    }
    for (const auto& sort : sorts) {
        const auto [parent, childIndex] = sort->Parents.front();
        if (removed.contains(parent)) {
            continue;
        }
        auto input = CastOperator<TOpSort>(sort)->GetInput();
        while (removed.contains(input.Get())) {
            input = CastOperator<TOpSort>(input)->GetInput();
        }
        parent->SetChild(childIndex, std::move(input));
    }

    FinishLogicalRewrite(root, ctx.ExprCtx);
}

} // namespace NKikimr::NKqp

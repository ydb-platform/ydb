#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

namespace {

bool CanMergeInput(const TOpUnionAll& unionAll, const TIntrusivePtr<IOperator>& input) {
    if (input->Kind != EOperator::UnionAll) {
        return false;
    }

    const auto innerUnionAll = CastOperator<TOpUnionAll>(input);
    if (unionAll.Ordered || innerUnionAll->Ordered) {
        return false;
    }

    return true;
}

} // anonymous namespace

bool TMergeUnionAllRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    if (input->Kind != EOperator::UnionAll) {
        return false;
    }

    for (const auto& child : input->GetChildren()) {
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
    newInputs.reserve(unionAll->GetInputs().size());
    bool merged = false;
    for (const auto& child : unionAll->GetInputs()) {
        if (!CanMergeInput(*unionAll, child)) {
            newInputs.push_back(child);
            continue;
        }

        // Splice the inner union branches in place, preserving their order.
        for (const auto& innerChild : child->GetChildren()) {
            newInputs.push_back(innerChild);
        }
        merged = true;
    }

    if (!merged) {
        return false;
    }

    // Compose each output row with the rows of the merged inner unions.
    TUnionAllIUs columns(TUnionInputPolicy{newInputs.size()});
    for (const auto& [output, row] : unionAll->GetColumns().Items()) {
        TUnionInputRow newRow;
        newRow.Inputs.reserve(newInputs.size());
        for (size_t index = 0; index < row.Inputs.size(); ++index) {
            const auto& child = unionAll->GetInput(index);
            if (!CanMergeInput(*unionAll, child)) {
                newRow.Inputs.push_back(row.Inputs[index]);
                continue;
            }

            const auto* innerRow = CastOperator<TOpUnionAll>(child)->GetColumns().Find(row.Inputs[index]);
            Y_ENSURE(innerRow, "Missing inner UnionAll binding");
            newRow.Inputs.insert(newRow.Inputs.end(), innerRow->Inputs.begin(), innerRow->Inputs.end());
        }
        columns.Add(output, std::move(newRow));
    }

    unionAll->SetInputs(std::move(newInputs));
    unionAll->SetColumns(std::move(columns));
    return true;
}

} // namespace NKqp
} // namespace NKikimr

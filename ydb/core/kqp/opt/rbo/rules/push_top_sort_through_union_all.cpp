#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

namespace {

// The maximum limit keeps every row, so a top sort in each input would cut nothing.
bool IsMaxLimit(const TExpression& limit) {
    const auto body = limit.GetExpressionBody();
    ui64 value = 0;
    return TCoUint64::Match(body.Get()) && TryFromString(TCoUint64(body).Literal().Value(), value) &&
           value == std::numeric_limits<ui64>::max();
}

// Finds the union all under the sort, possibly behind a map, and expresses the sort keys in its columns.
TIntrusivePtr<TOpUnionAll> FindUnionAll(const TOpSort& sort, TSortIUs& keys) {
    auto input = sort.GetInput();
    if (input->Kind == EOperator::Map) {
        const auto map = CastOperator<TOpMap>(input);
        for (size_t i = 0; i < keys.Items().size(); ++i) {
            const auto [column, order] = keys.Items()[i];
            if (const auto* element = map->GetMapElements().Find(column)) {
                if (!element->IsColumnAccess()) {
                    return nullptr;
                }
                keys.ReplaceAt(i, element->GetColumnAccess(), order);
            }
        }
        input = map->GetInput();
    }

    if (input->Kind != EOperator::UnionAll) {
        return nullptr;
    }

    const auto unionAll = CastOperator<TOpUnionAll>(input);
    for (const auto& [column, order] : keys.Items()) {
        if (!unionAll->GetColumns().Find(column)) {
            return nullptr;
        }
    }
    return unionAll;
}

} // anonymous namespace

bool TPushTopSortThroughUnionAllRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    if (input->Kind != EOperator::Sort || !CastOperator<TOpSort>(*input).IsTopSort()) {
        return false;
    }

    const auto& sortInput = input->GetChildren().front();
    return sortInput->Kind == EOperator::UnionAll ||
           (sortInput->Kind == EOperator::Map && sortInput->GetChildren().front()->Kind == EOperator::UnionAll);
}

TIntrusivePtr<IOperator> TPushTopSortThroughUnionAllRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    const auto sort = CastOperator<TOpSort>(input);
    const auto& limit = *sort->LimitCond;
    // Each input evaluates the limit again, so it cannot use columns or subqueries.
    if (IsMaxLimit(limit) || !limit.GetInputIUs(true, true).Empty()) {
        return input;
    }

    auto keys = sort->GetSortElements();
    const auto unionAll = FindUnionAll(*sort, keys);
    if (!unionAll) {
        return input;
    }

    TVector<TIntrusivePtr<IOperator>> newInputs;
    newInputs.reserve(unionAll->GetInputs().size());
    bool pushed = false;
    for (size_t index = 0; index < unionAll->GetInputs().size(); ++index) {
        const auto& child = unionAll->GetInput(index);
        // An input with its own top sort is bounded already. This also stops the rule from applying again.
        if (child->Kind == EOperator::Sort && CastOperator<TOpSort>(*child).IsTopSort()) {
            newInputs.push_back(child);
            continue;
        }

        TSubstitutions renames;
        for (const auto& [output, row] : unionAll->GetColumns().Items()) {
            renames.Add(output, row.Inputs[index]);
        }
        auto inputSort = MakeIntrusive<TOpSort>(child, sort->Pos, keys, limit);
        inputSort->RenameUsedIUs(renames);
        newInputs.push_back(std::move(inputSort));
        pushed = true;
    }

    if (!pushed) {
        return input;
    }

    unionAll->SetInputs(std::move(newInputs));

    // The sort above the union stays: it merges the inputs, and it keeps the final order and count.
    // A rule counts as applied only when it returns a different node, so this is a copy of the sort.
    return MakeIntrusive<TOpSort>(sort->GetInput(), sort->Pos, sort->Props, sort->GetSortElements(), sort->LimitCond, sort->GetSortPhase());
}

} // namespace NKqp
} // namespace NKikimr

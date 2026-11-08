#pragma once

#include "kqp_ast.h"

namespace NKikimr::NKqp::NNewYql {

// Visit scalar children only. A query has a new relational scope and a lambda
// has a new lexical scope, so callers handle these boundaries explicitly.
// Returning false prunes a relational value already replaced by a binding.
template <class TVisitor>
void VisitScalar(const TExpression& expression, const TVisitor& visitor) {
    if (!visitor(expression)) { return; }
    const auto visit = [&](const TExpressionPtr& child) {
        if (child) { VisitScalar(*child, visitor); }
    };
    std::visit([&](const auto& value) {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, TCall>) {
            for (const auto& argument : value.Arguments) { visit(argument.Expression); }
            visit(value.Filter);
            if (value.Over) {
                if (const auto* window = std::get_if<TWindowSpecification>(&value.Over->Value)) {
                    for (const auto& key : window->PartitionBy) { visit(key.Expression); }
                    for (const auto& key : window->OrderBy) { visit(key.Expression); }
                }
            }
        } else if constexpr (std::is_same_v<T, TUnary>) { visit(value.Operand);
        } else if constexpr (std::is_same_v<T, TBinary>) { visit(value.Left); visit(value.Right);
        } else if constexpr (std::is_same_v<T, TMember>) { visit(value.Object);
        } else if constexpr (std::is_same_v<T, TIndex>) { visit(value.Object); visit(value.Index);
        } else if constexpr (std::is_same_v<T, TTuple>) {
            for (const auto& item : value.Elements) { visit(item.Expression); }
        } else if constexpr (std::is_same_v<T, TList>) {
            for (const auto& item : value.Elements) { visit(item); }
        } else if constexpr (std::is_same_v<T, TCase>) {
            visit(value.Operand);
            for (const auto& branch : value.Branches) { visit(branch.When); visit(branch.Then); }
            visit(value.Else);
        } else if constexpr (std::is_same_v<T, TCast>) { visit(value.Value);
        } else if constexpr (std::is_same_v<T, TBetween>) { visit(value.Value); visit(value.Lower); visit(value.Upper);
        } else if constexpr (std::is_same_v<T, TIn>) { visit(value.Value); visit(value.Collection);
        } else if constexpr (std::is_same_v<T, TPatternMatch>) { visit(value.Value); visit(value.Pattern); visit(value.Escape); }
    }, expression.Value);
}

} // namespace NKikimr::NKqp::NNewYql

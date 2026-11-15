#include "predicate.h"

#include <unordered_map>

namespace NKikimr::NKqp::NFrontend {
namespace {
using O = EOpcode;
using K = ETypeKind;

bool IsRejectedLiteral(const TExprNode& node) {
    return node.Op == O::Literal && (std::holds_alternative<std::monostate>(node.Value) ||
        (std::holds_alternative<bool>(node.Value) && !std::get<bool>(node.Value)));
}

// Only these operands inherit the filter's TRUE-versus-rejected observation.
// The left operand of AND must retain FALSE versus NULL: that distinction
// decides whether its right operand runs. NOT, comparisons, and scalar calls
// likewise observe values, so their operands are left intact.
std::span<const TExprId> PredicateInputs(const TExpressionArena& arena, TExprId id) {
    const auto& node = arena[id];
    if (node.Type.Kind != K::Bool || node.Type.OptionalDepth > 1) return {};
    // A present optional wrapper cannot change whether a Boolean selects a
    // row. Retain the wrapped expression itself, including its evaluation.
    if (node.Op == O::OptionalWrap) return node.Arguments;
    switch (node.Op) {
        case O::Convert:
            return arena[node.Arguments[0]].Type.Required() == node.Type.Required()
                ? std::span<const TExprId>(node.Arguments) : std::span<const TExprId>{};
        case O::Or: return node.Arguments;
        case O::And: return std::span(node.Arguments).subspan(1);
        case O::If: return std::span(node.Arguments).subspan(1);
        case O::Coalesce:
            if (node.Arguments.size() == 2 && IsRejectedLiteral(arena[node.Arguments[1]]))
                return std::span(node.Arguments).first(1);
            [[fallthrough]];
        default: return {};
    }
}
}

TExprId PrepareFilterPredicate(TExpressionArena& arena, TExprId root) {
    if (PredicateInputs(arena, root).empty()) return root;
    std::unordered_map<TExprId, TExprId> prepared;
    std::vector<std::pair<TExprId, bool>> pending{{root, false}};
    while (!pending.empty()) {
        const auto [id, ready] = pending.back();
        pending.pop_back();
        if (prepared.contains(id)) continue;
        const auto inputs = PredicateInputs(arena, id);
        if (inputs.empty()) { prepared.emplace(id, id); continue; }
        if (!ready) {
            pending.emplace_back(id, true);
            for (auto it = inputs.rbegin(); it != inputs.rend(); ++it) pending.emplace_back(*it, false);
            continue;
        }
        // Constructors append nodes and may invalidate references into the arena.
        const auto node = arena[id];
        TExprId result = id;
        if (node.Op == O::Convert || node.Op == O::Coalesce || node.Op == O::OptionalWrap) {
            result = prepared.at(node.Arguments[0]);
        } else if (node.Op == O::And || node.Op == O::Or) {
            const auto left = node.Op == O::And ? node.Arguments[0] : prepared.at(node.Arguments[0]);
            const auto right = prepared.at(node.Arguments[1]);
            // In a TRUE-versus-rejected context, a FALSE or NULL operand
            // of OR contributes no selected rows. The other operand still
            // runs exactly when it did before, including checks and calls.
            if (node.Op == O::Or && IsRejectedLiteral(arena[left])) result = right;
            else if (node.Op == O::Or && IsRejectedLiteral(arena[right])) result = left;
            else if (left != node.Arguments[0] || right != node.Arguments[1])
                result = arena.Binary(node.Op, left, right, node.Position);
        } else if (node.Op == O::If) {
            const auto left = prepared.at(node.Arguments[1]), right = prepared.at(node.Arguments[2]);
            // IF(condition, true, rejected) observes exactly the filter's
            // truth test. Keep the condition's evaluation, including errors.
            // Other conditionals retain their distinct optimizer admission.
            if (arena[left].Op == O::Literal && std::holds_alternative<bool>(arena[left].Value) &&
                std::get<bool>(arena[left].Value) && IsRejectedLiteral(arena[right]))
                result = node.Arguments[0];
            else if (left != node.Arguments[1] || right != node.Arguments[2])
                result = arena.If(node.Arguments[0], left, right, node.Position);
        }
        prepared.emplace(id, result);
    }
    return prepared.at(root);
}

} // namespace NKikimr::NKqp::NFrontend

#include "kqp_native_olap.h"

#include <optional>
#include <unordered_map>
#include <unordered_set>

namespace NKikimr::NKqp {
namespace NDetail {
using K = NFrontend::ETypeKind;
using O = NFrontend::EOpcode;

TStringBuf OlapBinaryName(O op) {
    switch (op) {
        case O::Equal: return "eq";
        case O::NotEqual: return "neq";
        case O::Less: return "lt";
        case O::LessEqual: return "lte";
        case O::Greater: return "gt";
        case O::GreaterEqual: return "gte";
        case O::Add: return "+";
        case O::Subtract: return "-";
        case O::Multiply: return "*";
        case O::Divide: return "/";
        case O::Modulo: return "%";
        case O::Coalesce: return "??";
        case O::Contains: return "string_contains";
        case O::StartsWith: return "starts_with";
        case O::EndsWith: return "ends_with";
        default: return {};
    }
}

bool OlapIdentityConversion(const NFrontend::TExpressionArena& arena, const NFrontend::TExprNode& node) {
    if (node.Op != O::Convert) return false;
    const auto from = arena[node.Arguments[0]].Type;
    return from.Required() == node.Type.Required() || (from.Kind == K::Utf8 && node.Type.Kind == K::String);
}

} // namespace NDetail

namespace {
using K = NFrontend::ETypeKind;
using O = NFrontend::EOpcode;
using NDetail::OlapBinaryName;
using NDetail::OlapIdentityConversion;

bool OlapNodeSupported(const NFrontend::TExpressionArena& arena, const NFrontend::TExprNode& node, bool filterTruth) {
    if (node.Type.IsCollection() || node.Type.OptionalDepth > 1 || node.Type.IsDocument() || node.Type.Kind == K::Decimal || node.Type.Kind == K::Calendar || node.Type.Kind == K::Null) return false;
    if (OlapIdentityConversion(arena, node)) return true;
    if (node.Op >= O::Add && node.Op <= O::Modulo && !node.Type.IsNumeric()) {
        const bool temporal = (node.Op == O::Add || node.Op == O::Subtract) &&
            (node.Type.IsDate() || node.Type.IsInterval());
        if (!temporal) return false;
    }
    if (!OlapBinaryName(node.Op).empty()) {
        if (node.Arguments.size() != 2) return false;
        // The existing storage kernels cannot consume the same column twice.
        auto column = [&](NFrontend::TExprId id) -> std::optional<NFrontend::TColumnId> {
            while (OlapIdentityConversion(arena, arena[id])) id = arena[id].Arguments[0];
            return arena[id].Op == O::Column ? std::optional{arena[id].ColumnId} : std::nullopt;
        };
        const auto left = column(node.Arguments[0]), right = column(node.Arguments[1]);
        return !left || !right || *left != *right;
    }
    switch (node.Op) {
        case O::Literal: return !std::holds_alternative<std::monostate>(node.Value);
        case O::Not:
            // The storage NOT kernel takes a required Boolean. At a filter
            // truth boundary emission coalesces its operand to true; scalar
            // uses must preserve a NULL result through scalar apply.
            return filterTruth || !node.Type.OptionalDepth;
        case O::Column: case O::Parameter: case O::And: case O::Or: case O::If:
            return true;
        case O::Negate: case O::Abs: return node.Type.IsNumeric();
        case O::IsNull: case O::IsNotNull: return arena[node.Arguments[0]].Op == O::Column;
        default: return false;
    }
}

bool OlapScalarNodeSupported(const NFrontend::TExprNode& node) {
    if (node.Type.IsCollection() || node.Type.OptionalDepth > 1 || node.Type.IsDocument() ||
        node.Type.Kind == K::Decimal || node.Type.Kind == K::Calendar) return false;
    if (!OlapBinaryName(node.Op).empty()) return true;
    switch (node.Op) {
        case O::Literal: case O::Column: case O::Parameter: case O::Convert: case O::Cast:
        case O::Negate: case O::Not: case O::And: case O::Or: case O::If:
        case O::IsNull: case O::IsNotNull: case O::Abs: case O::Length: case O::OptionalWrap:
        case O::Find: case O::Substring: case O::Concat: case O::Like: case O::ILike:
        case O::RegexMatch: case O::RegexGrep:
        case O::Lower: case O::Upper: case O::InResult:
            return true;
        default: return false;
    }
}

} // namespace

bool CanLowerNativeOlapPredicate(const NFrontend::TExpressionArena& arena, NFrontend::TExprId root, bool allowScalarApply) {
    if (arena[root].Type.Kind != K::Bool) return false;
    const auto& predicate = arena[root];
    if (predicate.Op >= O::Equal && predicate.Op <= O::GreaterEqual) {
        // A comparison with a NULL constant cannot select a row; preserve the
        // existing policy of leaving it out of the storage filter program.
        for (const auto arg : predicate.Arguments)
            if (arena[arg].Op == O::Literal && std::holds_alternative<std::monostate>(arena[arg].Value)) return false;
    }
    struct TSupport {
        bool HasColumn = false;
        unsigned Checked = 0;
    };
    struct TPending {
        NFrontend::TExprId Id;
        bool Ready;
        bool FilterTruth;
    };
    std::unordered_map<NFrontend::TExprId, TSupport> supported;
    std::vector<TPending> pending{{root, false, true}};
    while (!pending.empty()) {
        const auto [id, ready, filterTruth] = pending.back(); pending.pop_back();
        const unsigned mode = filterTruth ? 1 : 2;
        if (const auto found = supported.find(id); found != supported.end() && (found->second.Checked & mode)) continue;
        const auto& node = arena[id];
        if (node.Op >= O::Equal && node.Op <= O::GreaterEqual) {
            // Preserve the storage collector's comparison-type policy for
            // both kernels and scalar apply. In particular, Bool and Uuid
            // comparisons are compute predicates, even though native scalar
            // emission can execute them.
            for (const auto arg : node.Arguments) {
                const auto type = arena[arg].Type;
                if (!(type.IsNumeric() || type.IsString() || type.IsDate() || type.IsInterval()) || type.Kind == K::Decimal)
                    return false;
            }
        }
        if (!OlapNodeSupported(arena, node, filterTruth) && !(allowScalarApply && OlapScalarNodeSupported(node))) return false;
        if ((node.Op == O::IsNull || node.Op == O::IsNotNull) && arena[node.Arguments[0]].Op == O::Column) {
            // Storage null checks consume the column's nullness, not its value.
            // Like the existing collector's Exists(Member), they also support
            // columns whose values cannot participate in storage expressions.
            auto& result = supported[id];
            result.HasColumn = true;
            result.Checked |= mode;
            continue;
        }
        if (!ready) {
            pending.push_back({id, true, filterTruth});
            const bool childTruth = filterTruth && (node.Op == O::And || node.Op == O::Or);
            for (auto arg : node.Arguments) pending.push_back({arg, false, childTruth});
            continue;
        }
        bool column = node.Op == O::Column;
        for (auto arg : node.Arguments) {
            // Storage streaming logic needs a row count before combining
            // branches. Keep scalar-only Boolean branches in compute.
            if (!allowScalarApply && (node.Op == O::And || node.Op == O::Or) && !supported.at(arg).HasColumn) return false;
            column |= supported.at(arg).HasColumn;
        }
        auto& result = supported[id];
        result.HasColumn = column;
        result.Checked |= mode;
    }
    return supported.at(root).HasColumn;
}


namespace {

// The storage splitter's strictness barrier is independent of whether a
// particular scalar operation can be executed in storage. Checked operations
// and runtime UDF contracts without strictness stop first-pass reordering.
bool IsStrictNativeOlapExpression(const NFrontend::TExpressionArena& arena, NFrontend::TExprId root,
    std::unordered_map<NFrontend::TExprId, bool>& strict) {
    std::vector<std::pair<NFrontend::TExprId, bool>> pending{{root, false}};
    while (!pending.empty()) {
        const auto [id, ready] = pending.back(); pending.pop_back();
        if (strict.contains(id)) continue;
        const auto& node = arena[id];
        switch (node.Op) {
            case O::Ensure: case O::Unwrap:
            case O::Like: case O::ILike: case O::RegexMatch: case O::RegexGrep:
            case O::UrlHost: case O::UrlCutWWW: case O::JsonValue:
                strict.emplace(id, false);
                continue;
            default: break;
        }
        if (!ready) {
            pending.emplace_back(id, true);
            for (const auto arg : node.Arguments) pending.emplace_back(arg, false);
            continue;
        }
        bool value = true;
        for (const auto arg : node.Arguments) value &= strict.at(arg);
        strict.emplace(id, value);
    }
    return strict.at(root);
}

} // namespace

TNativeOlapPredicateParts SplitNativeOlapPredicate(const NFrontend::TExpressionArena& arena,
    NFrontend::TExprId root, bool allowScalarApply, bool allowRegexp) {
    TNativeOlapPredicateParts result;
    if (CanPushNativeOlapPredicate(arena, root, false, allowRegexp)) {
        result.Pushed.push_back(root);
        return result;
    }

    bool blocked = false;
    std::unordered_map<NFrontend::TExprId, bool> strict;
    std::vector<NFrontend::TExprId> pending{root};
    while (!pending.empty()) {
        const auto id = pending.back(); pending.pop_back();
        const auto& node = arena[id];
        // Common scalar normalization presents conjunctions in source order.
        // Flatten the native binary representation without exposing OR/IF arms.
        if (node.Op == O::And) {
            pending.push_back(node.Arguments[1]);
            pending.push_back(node.Arguments[0]);
            continue;
        }
        if (!blocked && CanPushNativeOlapPredicate(arena, id, false, allowRegexp)) {
            result.Pushed.push_back(id);
        } else {
            blocked |= !IsStrictNativeOlapExpression(arena, id, strict);
            result.Remaining.push_back(id);
        }
    }
    if (allowScalarApply) {
        // The previous rule retries each remaining conjunct independently;
        // its first-pass strictness barrier does not span these retries.
        std::vector<NFrontend::TExprId> remaining;
        remaining.reserve(result.Remaining.size());
        for (const auto id : result.Remaining) {
            if (CanPushNativeOlapPredicate(arena, id, true, allowRegexp)) result.Pushed.push_back(id);
            else remaining.push_back(id);
        }
        result.Remaining = std::move(remaining);
    }
    return result;
}

bool CanLowerNativeOlapProjection(const NFrontend::TExpressionArena& arena, NFrontend::TExprId root) {
    const auto& node = arena[root];
    if (node.Op != O::JsonValue || arena[node.Arguments[0]].Op != O::Column) return false;
    // The storage path accessor accepts members and array indexes, but not
    // wildcard selectors. Quoted asterisks are ordinary member names. Native
    // path validation already excludes escapes inside quoted member names.
    char quote = 0;
    for (const char c : node.Name) {
        if (quote) { if (c == quote) quote = 0; }
        else if (c == '\'' || c == '"') quote = c;
        else if (c == '*') return false;
    }
    return true;
}

bool CanPushNativeOlapPredicate(const NFrontend::TExpressionArena& arena, NFrontend::TExprId root,
    bool allowScalarApply, bool allowRegexp) {
    if (!CanLowerNativeOlapPredicate(arena, root, allowScalarApply)) return false;
    const auto comparison = [](O op) {
        return (op >= O::Equal && op <= O::GreaterEqual) ||
            op == O::Contains || op == O::StartsWith || op == O::EndsWith;
    };
    const auto boolLiteral = [](const NFrontend::TExprNode& node) {
        return node.Op == O::Literal && node.Type.Kind == K::Bool && std::holds_alternative<bool>(node.Value);
    };
    // The old collector distinguishes complete predicates from comparison
    // operands. In particular a Boolean member is an operand, not an admitted
    // predicate by itself. Storage execution capability must not broaden that
    // optimizer policy. IDs plus use context bound traversal of shared DAGs.
    std::vector<std::pair<NFrontend::TExprId, bool>> pending{{root, true}};
    std::unordered_set<std::uint64_t> visited;
    while (!pending.empty()) {
        const auto [id, predicate] = pending.back(); pending.pop_back();
        if (!visited.insert((std::uint64_t{id} << 1) | predicate).second) continue;
        const auto& node = arena[id];
        bool childPredicates = false;
        if (node.Op == O::RegexMatch || node.Op == O::RegexGrep || node.Op == O::Like || node.Op == O::ILike) {
            const bool like = node.Op == O::Like || node.Op == O::ILike;
            if (!allowScalarApply || (!like && !allowRegexp)) return false;
        } else if (OlapIdentityConversion(arena, node)) childPredicates = predicate;
        else if (predicate) {
            if (comparison(node.Op)) { /* Comparison operands are scalar values. */ }
            else {
                switch (node.Op) {
                    case O::Literal: if (!boolLiteral(node)) return false; break;
                    case O::IsNull: case O::IsNotNull:
                        if (arena[node.Arguments[0]].Op != O::Column) return false;
                        break;
                    case O::Not: case O::And: case O::Or: childPredicates = true; break;
                    case O::Coalesce:
                        if (!allowScalarApply && (node.Arguments.size() != 2 ||
                            !comparison(arena[node.Arguments[0]].Op) || !boolLiteral(arena[node.Arguments[1]]))) return false;
                        childPredicates = true;
                        break;
                    case O::If: case O::OptionalWrap:
                        if (!allowScalarApply) return false;
                        childPredicates = true;
                        break;
                    default: return false;
                }
            }
        } else {
            switch (node.Op) {
                case O::Literal: case O::Column: case O::Parameter: break;
                case O::OptionalWrap: continue;
                case O::If:
                    for (const auto branch : {1u, 2u}) {
                        const auto& value = arena[node.Arguments[branch]];
                        if (value.Op == O::Literal && std::holds_alternative<std::monostate>(value.Value)) return false;
                    }
                    [[fallthrough]];
                case O::Coalesce:
                    if (!allowScalarApply) return false;
                    // The collector admits these scalar forms directly. The
                    // capability check above still validates every descendant.
                    continue;
                case O::Convert: case O::Cast: if (!allowScalarApply) return false; break;
                case O::Lower: case O::Upper:
                    // Optional String UDF arguments use the existing
                    // collector's IfPresent scalar-apply operand contract.
                    // Required UDF operands remain outside that admission.
                    if (!allowScalarApply || !node.Type.OptionalDepth) return false;
                    continue;
                case O::Negate: case O::Abs: case O::Length:
                case O::Add: case O::Subtract: case O::Multiply: case O::Divide: case O::Modulo: break;
                default: return false;
            }
        }
        for (const auto arg : node.Arguments) pending.emplace_back(arg, childPredicates);
    }
    return true;
}

} // namespace NKikimr::NKqp

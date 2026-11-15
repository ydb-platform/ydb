#include "kqp_native_range_bounds.h"
#include "kqp_expression.h"
#include "frontend/predicate.h"

#include <cmath>
#include <limits>
#include <type_traits>
#include <utility>
#include <unordered_map>

namespace NKikimr::NKqp {

NFrontend::TExprId StripNativeOptionalLift(const NFrontend::TExpressionArena& arena, NFrontend::TExprId id) {
    using O = NFrontend::EOpcode;
    while (arena[id].Op == O::Convert && arena[id].Type.Required() == arena[arena[id].Arguments[0]].Type.Required()) {
        id = arena[id].Arguments[0];
    }
    return id;
}

NFrontend::TExprId PrepareNativeRangePredicate(NFrontend::TExpressionArena& arena, NFrontend::TExprId root) {
    using namespace NFrontend;
    using O = EOpcode;
    std::unordered_map<TExprId, TExprId> prepared;
    std::vector<std::pair<TExprId, bool>> pending{{root, false}};
    while (!pending.empty()) {
        const auto [id, ready] = pending.back(); pending.pop_back();
        if (prepared.contains(id)) continue;
        const auto& node = arena[id];
        const bool coalescedTruth = node.Op == O::Coalesce && node.Type.Kind == ETypeKind::Bool &&
            node.Arguments.size() == 2 && arena[node.Arguments[1]].Op == O::Literal &&
            std::holds_alternative<bool>(arena[node.Arguments[1]].Value) && !std::get<bool>(arena[node.Arguments[1]].Value);
        if (coalescedTruth) {
            // Range extraction observes selected rows, including for the
            // left conjunct. The execution predicate remains unchanged.
            const auto value = node.Arguments[0];
            if (!ready) {
                pending.emplace_back(id, true);
                pending.emplace_back(value, false);
            } else prepared.emplace(id, prepared.at(value));
        } else if (node.Op == O::And || node.Op == O::Or) {
            if (!ready) {
                pending.emplace_back(id, true);
                for (auto arg : node.Arguments) if (!prepared.contains(arg)) pending.emplace_back(arg, false);
                continue;
            }
            const auto op = node.Op;
            const auto position = node.Position;
            const auto left = prepared.at(node.Arguments[0]), right = prepared.at(node.Arguments[1]);
            const bool changed = left != node.Arguments[0] || right != node.Arguments[1];
            prepared.emplace(id, changed ? arena.Binary(op, left, right, position) : id);
        } else if (node.Op == O::Literal && node.Type.Kind == ETypeKind::Bool &&
            std::holds_alternative<std::monostate>(node.Value)) {
            // A positive range predicate observes NULL as a rejected row.
            // This also removes the dead arm of nullable IN comparisons.
            prepared.emplace(id, arena.Literal({ETypeKind::Bool}, false, node.Position));
        } else if (node.Op == O::Not || node.Op == O::NotEqual) {
            const auto& comparison = node.Op == O::Not ? arena[node.Arguments[0]] : node;
            auto op = comparison.Op;
            if (node.Op == O::Not) {
                switch (op) {
                    case O::Equal: op = O::NotEqual; break;
                    case O::NotEqual: op = O::Equal; break;
                    case O::Less: op = O::GreaterEqual; break;
                    case O::LessEqual: op = O::Greater; break;
                    case O::Greater: op = O::LessEqual; break;
                    case O::GreaterEqual: op = O::Less; break;
                    default: prepared.emplace(id, id); continue;
                }
            }
            const auto left = comparison.Arguments[0], right = comparison.Arguments[1];
            const auto type = arena[left].Type;
            // Complementing an ordered comparison is invalid for NaN. Keep
            // floating/decimal and non-scalar predicates in their original form.
            if (type.OptionalDepth > 1 || !(type.IsIntegral() || type.IsString() || type.IsDate() ||
                type.IsInterval() || type.Kind == ETypeKind::Bool || type.Kind == ETypeKind::Uuid)) {
                prepared.emplace(id, id);
                continue;
            }
            const auto position = node.Position;
            if (op == O::NotEqual) {
                const auto less = arena.Binary(O::Less, left, right, position);
                const auto greater = arena.Binary(O::Greater, left, right, position);
                prepared.emplace(id, arena.Binary(O::Or, less, greater, position));
            } else {
                prepared.emplace(id, arena.Binary(op, left, right, position));
            }
        } else if (node.Op == O::InResult && arena[node.Arguments[0]].Op == O::Column &&
            !arena[node.Arguments[0]].Type.IsCollection() && arena[node.Arguments[0]].Type.OptionalDepth <= 1) {
            // The old extractor accepts a scalar key member here. Computed
            // lookups and tuple keys retain their original admission boundary.
            const auto lookup = node.Arguments[0], match = node.Arguments[1];
            const auto position = node.Position;
            const auto condition = arena.Unary(O::IsNotNull, lookup, position);
            const auto value = PrepareFilterPredicate(arena, match);
            prepared.emplace(id, arena.Binary(O::And, condition, value, position));
        } else {
            // In particular, converting a membership below NOT would turn
            // NULL into false before negation and select different rows.
            prepared.emplace(id, id);
        }
    }
    const auto result = prepared.at(root);
    return result == root ? root : arena.FoldConstants(std::span(&result, 1)).front();
}

bool HasUnorderedNativeKeys(NFrontend::TType type) {
    using K = NFrontend::ETypeKind;
    return type.Kind == K::Float || type.Kind == K::Double || type.Kind == K::Decimal;
}

namespace {

// NaN is the largest storage key, but is unordered in SQL comparisons. A
// parameter bound therefore needs a native validity condition. Comparisons
// against absent parameters are UNKNOWN and produce no range. IS NULL points
// bypass this check because they explicitly select the finite NULL storage key.
std::optional<NFrontend::TExprId> NativeBoundCondition(NFrontend::TExpressionArena& arena, NFrontend::TExprId value) {
    using namespace NFrontend;
    value = StripNativeOptionalLift(arena, value);
    const auto& node = arena[value];
    if (!HasUnorderedNativeKeys(node.Type)) {
        if (node.Type.OptionalDepth) return arena.Unary(EOpcode::IsNotNull, value);
        return {};
    }
    if (node.Op == EOpcode::Literal) {
        if (std::holds_alternative<std::monostate>(node.Value)) return {};
        const bool nan = node.Type.Kind == ETypeKind::Decimal
            ? std::get<TDecimal>(node.Value).Coefficient == TDecimal::NaN() : std::isnan(std::get<double>(node.Value));
        if (!nan) return {};
        return arena.Literal({ETypeKind::Bool}, false);
    }
    const auto valid = arena.Binary(EOpcode::Equal, value, value);
    if (!arena[valid].Type.OptionalDepth) return valid;
    const NFrontend::TExprId alternatives[]{valid, arena.Literal({ETypeKind::Bool}, false)};
    return arena.Coalesce(alternatives);
}

// Storage bounds use the key's exact type. Integer comparisons in the native
// IR compare mathematical values across widths/signedness: never round them
// through Double or narrow an out-of-range value into the key domain.
template <typename T>
bool FitsNativeKey(const NFrontend::TScalar& value) {
    if (const auto* v = std::get_if<std::int64_t>(&value)) return std::in_range<T>(*v);
    if (const auto* v = std::get_if<std::uint64_t>(&value)) return std::in_range<T>(*v);
    return false;
}

template <class T>
std::pair<NFrontend::TScalar, NFrontend::TScalar> IntegerLimits() {
    using Value = std::conditional_t<std::is_signed_v<T>, std::int64_t, std::uint64_t>;
    return {Value{std::numeric_limits<T>::min()}, Value{std::numeric_limits<T>::max()}};
}

std::pair<NFrontend::TScalar, NFrontend::TScalar> IntegerLimits(NFrontend::TType type) {
    using K = NFrontend::ETypeKind;
    switch (type.Kind) {
        case K::Int8: return IntegerLimits<std::int8_t>();
        case K::Int16: return IntegerLimits<std::int16_t>();
        case K::Int32: return IntegerLimits<std::int32_t>();
        case K::Int64: return IntegerLimits<std::int64_t>();
        case K::Uint8: return IntegerLimits<std::uint8_t>();
        case K::Uint16: return IntegerLimits<std::uint16_t>();
        case K::Uint32: return IntegerLimits<std::uint32_t>();
        case K::Uint64: return IntegerLimits<std::uint64_t>();
        default: throw NFrontend::TFrontendError({}, "Range limits require an integer key");
    }
}

bool IsTotalIntegerConversion(NFrontend::TType source, NFrontend::TType target) {
    const auto integer = [](const NFrontend::TScalar& value) -> __int128 {
        if (const auto* number = std::get_if<std::int64_t>(&value)) return *number;
        return std::get<std::uint64_t>(value);
    };
    const auto [sourceMin, sourceMax] = IntegerLimits(source);
    const auto [targetMin, targetMax] = IntegerLimits(target);
    return integer(sourceMin) >= integer(targetMin) && integer(sourceMax) <= integer(targetMax);
}

bool IsLosslessFloatingPromotion(NFrontend::TType source, NFrontend::TType target) {
    using K = NFrontend::ETypeKind;
    if (source.Kind == K::Float && target.Kind == K::Double) return true;
    if (!source.IsIntegral() || (target.Kind != K::Float && target.Kind != K::Double)) return false;
    const unsigned digits = target.Kind == K::Float ? std::numeric_limits<float>::digits : std::numeric_limits<double>::digits;
    const __int128 limit = __int128{1} << digits;
    const auto [minimum, maximum] = IntegerLimits(source);
    if (source.IsSigned()) return std::get<std::int64_t>(minimum) >= -limit && std::get<std::int64_t>(maximum) <= limit;
    return std::get<std::uint64_t>(maximum) <= limit;
}

NFrontend::TExprId RequiredPredicate(NFrontend::TExpressionArena& arena, NFrontend::TExprId predicate,
    NFrontend::TSourcePosition position) {
    if (!arena[predicate].Type.OptionalDepth) return predicate;
    const NFrontend::TExprId alternatives[]{predicate, arena.Literal({NFrontend::ETypeKind::Bool}, false, position)};
    return arena.Coalesce(alternatives, position);
}

std::optional<NFrontend::TExprId> NativeBoundValue(const TExpression& expression, NFrontend::TType keyType) {
    using namespace NFrontend;
    using K = ETypeKind;
    auto& arena = *expression.GetNativeArena();
    auto id = StripNativeOptionalLift(arena, expression.GetNativeRoot());
    const auto& node = arena[id];
    keyType = keyType.Required();
    if (node.Op != EOpcode::Literal) {
        if (!node.Repeatable || !expression.GetRawInputIUs().Empty()) return {};
        const auto type = node.Type;
        const auto position = node.Position;
        if (type.OptionalDepth > 1) return {};
        // A total integer widening remains a native scalar bound. Emission
        // materializes computed bounds using the existing range transaction.
        if (type.Required() != keyType && !(type.IsIntegral() && keyType.IsIntegral() &&
            IsTotalIntegerConversion(type, keyType))) return {};
        // The range's validity condition guards this unwrap. Both range and
        // lookup emission evaluate values only in the condition's true branch.
        if (type.OptionalDepth) id = arena.Call("Unwrap", std::span{&id, 1}, position);
        return arena.Convert(id, keyType, position);
    }
    if (std::holds_alternative<std::monostate>(node.Value)) return {};
    if (node.Type == keyType) return id;
    if (node.Type.Required() == keyType || (node.Type.Kind == K::Utf8 && keyType.Kind == K::String))
        return arena.Literal(keyType, node.Value, node.Position);
    if (!node.Type.IsIntegral() || !keyType.IsIntegral()) return {};
    bool fits = false;
    switch (keyType.Kind) {
        case K::Int8: fits = FitsNativeKey<i8>(node.Value); break;
        case K::Int16: fits = FitsNativeKey<i16>(node.Value); break;
        case K::Int32: fits = FitsNativeKey<i32>(node.Value); break;
        case K::Int64: fits = FitsNativeKey<i64>(node.Value); break;
        case K::Uint8: fits = FitsNativeKey<ui8>(node.Value); break;
        case K::Uint16: fits = FitsNativeKey<ui16>(node.Value); break;
        case K::Uint32: fits = FitsNativeKey<ui32>(node.Value); break;
        case K::Uint64: fits = FitsNativeKey<ui64>(node.Value); break;
        default: break;
    }
    if (!fits) return {};
    if (keyType.IsSigned()) {
        const auto value = std::holds_alternative<std::int64_t>(node.Value) ? std::get<std::int64_t>(node.Value)
            : static_cast<std::int64_t>(std::get<std::uint64_t>(node.Value));
        return arena.Literal(keyType, value, node.Position);
    }
    const auto value = std::holds_alternative<std::uint64_t>(node.Value) ? std::get<std::uint64_t>(node.Value)
        : static_cast<std::uint64_t>(std::get<std::int64_t>(node.Value));
    return arena.Literal(keyType, value, node.Position);
}

} // namespace

NFrontend::TExprId StripNativeRangeKeyPromotion(const NFrontend::TExpressionArena& arena,
    NFrontend::TExprId id, NFrontend::EOpcode comparison) {
    using O = NFrontend::EOpcode;
    id = StripNativeOptionalLift(arena, id);
    // Native numeric comparisons carry their implicit promotions explicitly.
    // Equality can use the original key only when promotion preserves every
    // distinct key. Explicit user casts and lossy promotions stay predicates.
    if (comparison == O::Equal) {
        while (arena[id].Op == O::Convert &&
            IsLosslessFloatingPromotion(arena[arena[id].Arguments[0]].Type, arena[id].Type)) {
            id = StripNativeOptionalLift(arena, arena[id].Arguments[0]);
        }
    }
    return id;
}

std::optional<TNativeRangeBound> BuildNativeRangeBound(const TExpression& expression,
    NFrontend::TType keyType, NFrontend::EOpcode comparison) {
    using namespace NFrontend;
    using O = EOpcode;
    auto& arena = *expression.GetNativeArena();
    const auto id = StripNativeOptionalLift(arena, expression.GetNativeRoot());
    const auto type = arena[id].Type;
    const auto position = arena[id].Position;
    keyType = keyType.Required();
    if (const auto value = NativeBoundValue(expression, keyType)) {
        return TNativeRangeBound{*value, NativeBoundCondition(arena, id),
            comparison == O::Equal || comparison == O::LessEqual || comparison == O::GreaterEqual};
    }
    // Preserve literal-NULL admission: it is not a comparison endpoint.
    // IS NULL has a separate point representation owned by the range rule.
    if (arena[id].Op == O::Literal && std::holds_alternative<std::monostate>(arena[id].Value)) return {};
    if (type.OptionalDepth > 1 || !arena[id].Repeatable || !expression.GetRawInputIUs().Empty()) return {};
    if (comparison == O::Equal && IsLosslessFloatingPromotion(keyType, type)) {
        const auto cast = arena.Cast(id, keyType.Optional(), position);
        // A safe cast alone truncates fractional values. Round-tripping to
        // the comparison type additionally proves exact representability.
        const auto restored = arena.Convert(cast, type.Optional(), position);
        const auto valid = RequiredPredicate(arena, arena.Binary(O::Equal, id, restored, position), position);
        return TNativeRangeBound{arena.Unary(O::Unwrap, cast, position), valid, true};
    }
    if (!type.IsIntegral() || !keyType.IsIntegral()) return {};

    // A possibly narrowing cast remains optional. Equality uses it only when
    // representable; inequalities clamp the appropriate end of the key domain.
    const auto cast = arena.Cast(id, keyType.Optional(), position);
    if (comparison == O::Equal) {
        return TNativeRangeBound{arena.Unary(O::Unwrap, cast, position),
            arena.Unary(O::IsNotNull, cast, position), true};
    }
    const bool lower = comparison == O::Greater || comparison == O::GreaterEqual;
    const bool strict = comparison == O::Less || comparison == O::Greater;
    if (!lower && comparison != O::Less && comparison != O::LessEqual) return {};
    const auto [minimum, maximum] = IntegerLimits(keyType);
    const auto edge = arena.Literal(keyType, lower ? maximum : minimum, position);
    const auto guardOp = lower ? (strict ? O::Less : O::LessEqual) : (strict ? O::Greater : O::GreaterEqual);
    const auto valid = RequiredPredicate(arena, arena.Binary(guardOp, id, edge, position), position);
    auto value = cast;
    if (strict) {
        const TScalar one = keyType.IsSigned() ? TScalar{std::int64_t{1}} : TScalar{std::uint64_t{1}};
        value = arena.Binary(lower ? O::Add : O::Subtract, value, arena.Literal(keyType, one, position), position);
    }
    // The validity guard excludes overflow at the strict boundary. An absent
    // cast in the other direction denotes the whole non-null key domain.
    const TExprId alternatives[]{value, arena.Literal(keyType, lower ? minimum : maximum, position)};
    return TNativeRangeBound{arena.Coalesce(alternatives, position), valid, true};
}

} // namespace NKikimr::NKqp

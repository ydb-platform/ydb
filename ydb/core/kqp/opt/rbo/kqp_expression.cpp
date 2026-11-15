#include "kqp_expression.h"
#include "kqp_rbo_utils.h"
#include "frontend/quoted.h"
#include <array>
#include <unordered_set>

namespace NKikimr::NKqp {
namespace {
TString QuoteString(TStringBuf value) {
    return TString(NFrontend::QuoteStringLiteral(std::string_view(value.data(), value.size())));
}

bool IsBinaryCallable(TStringBuf callable) {
    return callable == "+" || callable == "-" || callable == "*" || callable == "/" || callable == "%" ||
        callable == "==" || callable == "!=" || callable == "<" || callable == "<=" || callable == ">" || callable == ">=" ||
        callable == "DecimalAdd" || callable == "DecimalSub" || callable == "DecimalMul" || callable == "DecimalDiv";
}

TString GetBinaryOperator(TStringBuf callable) {
    if (callable == "DecimalAdd") {
        return "+";
    }
    if (callable == "DecimalSub") {
        return "-";
    }
    if (callable == "DecimalMul") {
        return "*";
    }
    if (callable == "DecimalDiv") {
        return "/";
    }
    return TString(callable);
}

TString GetLogicOperator(TStringBuf callable) {
    if (callable == "And") {
        return "AND";
    }
    if (callable == "Or") {
        return "OR";
    }
    if (callable == "Xor") {
        return "XOR";
    }
    return TString(callable);
}

TString FormatExpressionDependencies(const TExpression& expr, const TInfoUnitRegistry& registry) {
    const auto& deps = expr.GetInputIUs(true, true);
    if (deps.Empty()) {
        return "<expr>";
    }
    TStringBuilder text;
    text << "<expr depends on: ";
    TStringBuf separator;
    for (const auto id : deps) {
        text << separator << registry.GetDisplayName(id);
        separator = ",";
    }
    return text << ">";
}
} // namespace


TExpression TExpression::FromNative(std::shared_ptr<NFrontend::TExpressionArena> arena,
    NFrontend::TExprId root, TPlanProps* props, TPositionHandle pos) {
    Y_ENSURE(arena, "Native expression requires its arena");
    (void)(*arena)[root];
    TExpression result;
    result.NativeArena_ = std::move(arena);
    result.NativeRoot_ = root;
    result.NativePos_ = pos;
    result.PlanProps = props;
    return result;
}

namespace {

using TNativeArena = NFrontend::TExpressionArena;
using TNativeId = NFrontend::TExprId;
using TNativeOp = NFrontend::EOpcode;

TStringBuf NativeOpName(TNativeOp op) {
    switch (op) {
        case TNativeOp::Literal: return "Literal";
        case TNativeOp::Column: return "Member";
        case TNativeOp::Parameter: return "Parameter";
        case TNativeOp::Convert: return "Convert";
        case TNativeOp::Cast: return "SafeCast";
        case TNativeOp::Negate: return "Minus";
        case TNativeOp::Not: return "Not";
        case TNativeOp::Add: return "+";
        case TNativeOp::Subtract: return "-";
        case TNativeOp::Multiply: return "*";
        case TNativeOp::Divide: return "/";
        case TNativeOp::Modulo: return "%";
        case TNativeOp::Equal: return "==";
        case TNativeOp::NotEqual: return "!=";
        case TNativeOp::Less: return "<";
        case TNativeOp::LessEqual: return "<=";
        case TNativeOp::Greater: return ">";
        case TNativeOp::GreaterEqual: return ">=";
        case TNativeOp::And: return "And";
        case TNativeOp::Or: return "Or";
        case TNativeOp::IsNull: return "IsNull";
        case TNativeOp::HasNull: return "HasNull";
        case TNativeOp::IsNotNull: return "Exists";
        case TNativeOp::Coalesce: return "Coalesce";
        case TNativeOp::If: return "If";
        case TNativeOp::Abs: return "Abs";
        case TNativeOp::Length: return "Length";
        case TNativeOp::Lower: return "Lower";
        case TNativeOp::Upper: return "Upper";
        case TNativeOp::Contains: return "Contains";
        case TNativeOp::StartsWith: return "StartsWith";
        case TNativeOp::EndsWith: return "EndsWith";
        case TNativeOp::Substring: return "Substring";
        case TNativeOp::Concat: return "Concat";
        case TNativeOp::Ensure: return "Ensure";
        case TNativeOp::StablePickle: return "StablePickle";
        case TNativeOp::Find: return "Find";
        case TNativeOp::CalendarSplit: return "CalendarSplit";
        case TNativeOp::CalendarDate: return "CalendarDate";
        case TNativeOp::CalendarDatetime: return "CalendarDatetime";
        case TNativeOp::CalendarTimestamp: return "CalendarTimestamp";
        case TNativeOp::CalendarYear: return "CalendarYear";
        case TNativeOp::CalendarMinute: return "CalendarMinute";
        case TNativeOp::TemporalSeconds: return "TemporalSeconds";
        case TNativeOp::UrlHost: return "UrlHost";
        case TNativeOp::UrlCutWWW: return "UrlCutWWW";
        case TNativeOp::CalendarShiftMonths: return "CalendarShiftMonths";
        case TNativeOp::CalendarShiftYears: return "CalendarShiftYears";
        case TNativeOp::JsonValue: return "JsonValue";
        case TNativeOp::Unwrap: return "Unwrap";
        case TNativeOp::Collection: return "Collection";
        case TNativeOp::InCollection: return "IN";
        case TNativeOp::InResult: return "InResult";
        case TNativeOp::RandomUuid: return "RandomUuid";
        case TNativeOp::RandomNumber: return "RandomNumber";
        case TNativeOp::Like: return "Like";
        case TNativeOp::ILike: return "ILike";
        case TNativeOp::RegexMatch: return "RegexMatch";
        case TNativeOp::RegexGrep: return "RegexGrep";
        case TNativeOp::TupleElement: return "TupleElement";
        case TNativeOp::Member: return "Member";
        case TNativeOp::ListElement: return "ListElement";
        case TNativeOp::ListFromRange: return "ListFromRange";
        case TNativeOp::ListConcat: return "ListConcat";
        case TNativeOp::ListLength: return "ListLength";
        case TNativeOp::ListHasItems: return "ListHasItems";
        case TNativeOp::Minimum: return "Min";
        case TNativeOp::Maximum: return "Max";
        case TNativeOp::BitNot: return "BitNot";
        case TNativeOp::BitAnd: return "BitAnd";
        case TNativeOp::BitOr: return "BitOr";
        case TNativeOp::BitXor: return "BitXor";
        case TNativeOp::ShiftLeft: return "ShiftLeft";
        case TNativeOp::ShiftRight: return "ShiftRight";
        case TNativeOp::IsNotDistinct: return "IsNotDistinctFrom";
        case TNativeOp::BitCast: return "BitCast";
        case TNativeOp::OptionalWrap: return "Just";
        case TNativeOp::UnicodeLower: return "Unicode::ToLower";
        case TNativeOp::UnicodeUpper: return "Unicode::ToUpper";
    }
    Y_UNREACHABLE();
}

TExpression NativePart(const TExpression& expression, TNativeId id) {
    return TExpression::FromNative(expression.GetNativeArena(), id, expression.PlanProps, expression.Pos());
}

TNativeId UnwrapNativeOptionalLift(const TNativeArena& arena, TNativeId id) {
    while (arena[id].Op == TNativeOp::Convert &&
        arena[id].Type.Required() == arena[arena[id].Arguments[0]].Type.Required()) {
        id = arena[id].Arguments[0];
    }
    return id;
}

std::optional<NFrontend::TType> NativeDecimalJoinKeyType(const TNativeArena& arena, const NFrontend::TExprNode& equality) {
    const auto a = arena[equality.Arguments[0]].Type, b = arena[equality.Arguments[1]].Type;
    if (a.Kind != NFrontend::ETypeKind::Decimal || b.Kind != NFrontend::ETypeKind::Decimal || a.Scale == b.Scale) return {};
    // Retain an integral digit so decimal casts admit both wholly fractional
    // and integral domains. Exactness checks below reject any lost digits.
    const auto scale = std::min<ui8>(std::max(a.Scale, b.Scale), 34);
    const unsigned precision = std::max(a.Precision - a.Scale, b.Precision - b.Scale) + scale;
    return NFrontend::TType{NFrontend::ETypeKind::Decimal, false, static_cast<ui8>(std::min(precision, 35u)), scale};
}

TNativeId ImportNative(TNativeArena& arena, const TExpression& expression) {
    if (&arena == expression.GetNativeArena().get()) return expression.GetNativeRoot();
    return arena.Import(*expression.GetNativeArena(), expression.GetNativeRoot(),
        [&](TInfoUnitId id, NFrontend::TType type, NFrontend::TSourcePosition position) {
            return arena.Column(id, type, position);
        });
}

TVector<TExpression> SplitNative(const TExpression& expression, TNativeOp op) {
    TVector<TExpression> result;
    TVector<TNativeId> pending{expression.GetNativeRoot()};
    std::unordered_set<TNativeId> visited;
    const auto& arena = *expression.GetNativeArena();
    while (!pending.empty()) {
        // Lifting Bool to Bool? preserves its truth value. Look through it
        // when splitting predicates, including a lifted nested AND/OR.
        const auto id = UnwrapNativeOptionalLift(arena, pending.back());
        pending.pop_back();
        if (!visited.insert(id).second) {
            continue;
        }
        const auto& node = arena[id];
        if (node.Op == op) {
            for (auto it = node.Arguments.rbegin(); it != node.Arguments.rend(); ++it) {
                pending.push_back(*it);
            }
        } else {
            result.push_back(NativePart(expression, id));
        }
    }
    return result;
}

std::optional<TString> FormatNative(const TNativeArena& arena, TNativeId id,
    const TInfoUnitRegistry* registry, unsigned depth = 0) {
    if (depth > 12) {
        return {};
    }
    const auto& node = arena[id];
    if (node.Op == TNativeOp::Column) {
        return registry ? registry->GetDisplayName(node.ColumnId) : TStringBuilder() << "$" << node.ColumnId;
    }
    if (node.Op == TNativeOp::Literal) {
        if (node.Type.IsString()) if (const auto* value = std::get_if<std::string>(&node.Value))
            return QuoteString(TStringBuf(value->data(), value->size()));
        return TString(NFrontend::FormatValue({node.Type, node.Value}));
    }
    if (node.Op == TNativeOp::Parameter) {
        return TStringBuilder() << "$" << node.Name;
    }
    if (node.Op == TNativeOp::Convert || node.Op == TNativeOp::Cast ||
        node.Op == TNativeOp::Unwrap || node.Op == TNativeOp::OptionalWrap)
        return FormatNative(arena, node.Arguments[0], registry, depth + 1);
    const auto callable = NativeOpName(node.Op);
    const auto argument = [&](TNativeId child, bool parentheses) -> std::optional<TString> {
        auto text = FormatNative(arena, child, registry, depth + 1);
        const auto op = arena[child].Op;
        if (text && parentheses && (IsBinaryCallable(NativeOpName(op)) || op == TNativeOp::And || op == TNativeOp::Or))
            return TStringBuilder() << "(" << *text << ")";
        return text;
    };
    if (IsBinaryCallable(callable) || node.Op == TNativeOp::And || node.Op == TNativeOp::Or) {
        const bool binary = IsBinaryCallable(callable);
        const auto left = argument(node.Arguments[0], binary), right = argument(node.Arguments[1], binary);
        if (!left || !right) return {};
        return TStringBuilder() << *left << " " << (binary ? GetBinaryOperator(callable) : GetLogicOperator(callable)) << " " << *right;
    }
    if (node.Op == TNativeOp::Not) {
        const auto value = argument(node.Arguments[0], true);
        if (!value) return {};
        return TStringBuilder() << "NOT " << *value;
    }
    TStringBuilder result;
    result << callable << "(";
    for (std::size_t i = 0; i < node.Arguments.size(); ++i) {
        const auto argument = FormatNative(arena, node.Arguments[i], registry, depth + 1);
        if (!argument) {
            return {};
        }
        if (i) {
            result << ", ";
        }
        result << *argument;
    }
    if (node.Op == TNativeOp::TupleElement) result << ", " << std::get<std::uint64_t>(node.Value);
    if (node.Op == TNativeOp::JsonValue || node.Op == TNativeOp::Like || node.Op == TNativeOp::ILike) result << ", " << QuoteString(TStringBuf(node.Name.data(), node.Name.size()));
    return result << ")";
}

} // namespace

TExpression TExpression::WithNativeColumnTypes(const std::function<NFrontend::TType(TInfoUnitId)>& typeOf) const {
    Y_ENSURE(IsNative());
    const auto root = NativeArena_->RewriteColumns(NativeRoot_,
        [&](TNativeId original, TInfoUnitId id, NFrontend::TType type, NFrontend::TSourcePosition position) {
            const auto target = typeOf(id);
            return target == type ? original : NativeArena_->Column(id, target, position);
        });
    return root == NativeRoot_ ? *this : FromNative(NativeArena_, root, PlanProps, NativePos_);
}

TVector<TExpression> TExpression::SplitConjunct() const {

    return SplitNative(*this, TNativeOp::And);
}

TVector<TExpression> TExpression::SplitDisjunct() const {

    return SplitNative(*this, TNativeOp::Or);
}

bool TExpression::IsColumnAccess() const {

    return (*NativeArena_)[NativeRoot_].Op == TNativeOp::Column;
}

std::optional<TInfoUnitId> TExpression::GetColumnId(bool allowOptionalLift) const {

    const auto root = allowOptionalLift ? UnwrapNativeOptionalLift(*NativeArena_, NativeRoot_) : NativeRoot_;
    const auto& node = (*NativeArena_)[root];
    return node.Op == TNativeOp::Column ? std::optional{node.ColumnId} : std::nullopt;
}

bool TExpression::IsSingleCallable(const THashSet<TString>& allowedCallables) const {

    const auto& node = (*NativeArena_)[NativeRoot_];
    return node.Arguments.size() == 1 && allowedCallables.contains(TString(NativeOpName(node.Op))) &&
        (*NativeArena_)[node.Arguments[0]].Op == TNativeOp::Column;
}

bool TExpression::MaybeEquiJoinConditionInternal(bool includeExpressions) const {

    const auto& node = (*NativeArena_)[UnwrapNativeOptionalLift(*NativeArena_, NativeRoot_)];
    if (node.Op != TNativeOp::Equal) return false;
    const auto left = NativePart(*this, UnwrapNativeOptionalLift(*NativeArena_, node.Arguments[0]));
    const auto right = NativePart(*this, UnwrapNativeOptionalLift(*NativeArena_, node.Arguments[1]));
    return includeExpressions ? !left.GetInputIUs(true, false).Empty() && !right.GetInputIUs(true, false).Empty()
        : left.IsColumnAccess() && right.IsColumnAccess() && !NativeDecimalJoinKeyType(*NativeArena_, node);
}

bool TExpression::MaybeConstantCondition() const {

    const auto& node = (*NativeArena_)[NativeRoot_];
    if (node.Op < TNativeOp::Equal || node.Op > TNativeOp::GreaterEqual) {
        return false;
    }
    return GetRawInputIUs().Size() == 1 &&
        (NativePart(*this, node.Arguments[0]).GetRawInputIUs().Empty() || NativePart(*this, node.Arguments[1]).GetRawInputIUs().Empty());
}

const TUnorderedIUs& TExpression::GetRawInputIUs() const {

    if (!NativeIUsComputed_) {
        std::unordered_set<TNativeId> visited;
        std::vector<TNativeId> pending{NativeRoot_};
        while (!pending.empty()) {
            const auto id = pending.back();
            pending.pop_back();
            if (!visited.insert(id).second) {
                continue;
            }
            const auto& node = (*NativeArena_)[id];
            if (node.Op == TNativeOp::Column) {
                RawInputIUs.Add(node.ColumnId);
            }
            pending.insert(pending.end(), node.Arguments.begin(), node.Arguments.end());
        }
        NativeIUsComputed_ = true;
    }
    return RawInputIUs;
}

TExpression TExpression::ApplyRenames(const TSubstitutions& substitutions) const {

    bool changed = false;
    for (auto id : GetRawInputIUs()) {
        changed |= Substitute(id, substitutions) != id;
    }
    if (!changed) {
        return *this;
    }
    const auto root = NativeArena_->RewriteColumns(NativeRoot_,
        [&](TNativeId original, TInfoUnitId id, NFrontend::TType type, NFrontend::TSourcePosition position) {
            const auto renamed = Substitute(id, substitutions);
            return renamed == id ? original : NativeArena_->Column(renamed, type, position);
        });
    return FromNative(NativeArena_, root, PlanProps, NativePos_);
}

std::optional<TExpression> TExpression::TryExtractCommonConjuncts() const {

    const auto factored = NativeArena_->FactorCommonConjuncts(NativeRoot_);
    if (factored == NativeRoot_) return std::nullopt;
    return NativePart(*this, factored);
}

TString TExpression::ToString() const {

    if (auto text = FormatNative(*NativeArena_, NativeRoot_, PlanProps ? &PlanProps->InfoUnitRegistry : nullptr)) {
        return *text;
    }
    return TStringBuilder() << "NativeExpression(" << NativeRoot_ << ", " << GetNativeType().ToString() << ")";
}

TString TExpression::ToExplainString(const TInfoUnitRegistry& registry) const {

    if (auto text = FormatNative(*NativeArena_, NativeRoot_, &registry)) {
        return *text;
    }
    return FormatExpressionDependencies(*this, registry);
}

std::optional<ui64> TExpression::GetUint64Literal() const {
    const auto& node = (*NativeArena_)[NativeRoot_];
    if (node.Op == NFrontend::EOpcode::Literal && node.Type == NFrontend::TType{NFrontend::ETypeKind::Uint64})
        return std::get<std::uint64_t>(node.Value);
    return std::nullopt;
}

bool TExpression::MaybeEquiJoinCondition() const {
    return MaybeEquiJoinConditionInternal(false);
}

bool TExpression::MaybeExprEquiJoinCondition() const {
    return MaybeEquiJoinConditionInternal(true);
}

const TUnorderedIUs& TExpression::GetInputIUs(bool includeSubplanVars, bool includeCorrelatedDeps) const {
    const auto& rawInputIUs = GetRawInputIUs();
    if (rawInputIUs.Empty()) {
        return rawInputIUs;
    }

    Y_ENSURE(PlanProps, "Plan properties null for an expression with members");
    if (PlanProps->Subplans.Empty()) {
        return rawInputIUs;
    }

    const auto calls = PlanProps->Subplans.CallsIn(rawInputIUs);
    if (calls.Empty()) {
        return rawInputIUs;
    }

    // Only direct references are cached. Subplan additions/removals and changes
    // to their dependencies must be visible without changing the expression AST.
    ResolvedInputIUs = rawInputIUs;
    if (!includeSubplanVars) {
        ResolvedInputIUs.Subtract(calls);
    }
    if (includeCorrelatedDeps) {
        for (const auto call : calls) {
            const auto& subplan = PlanProps->Subplans.At(call);
            ResolvedInputIUs.UnionWith(subplan.Tuple.Unordered());
            ResolvedInputIUs.UnionWith(subplan.DependentIUs);
        }
    }
    return ResolvedInputIUs;
}

TEquiJoinCondition::TEquiJoinCondition(const TExpression& expr) : Expr(expr) {

    const auto& node = (*Expr.GetNativeArena())[UnwrapNativeOptionalLift(*Expr.GetNativeArena(), Expr.GetNativeRoot())];
    Y_ENSURE(node.Arguments.size() == 2, "Non-binary native join condition");
    const auto left = NativePart(Expr, UnwrapNativeOptionalLift(*Expr.GetNativeArena(), node.Arguments[0]));
    const auto right = NativePart(Expr, UnwrapNativeOptionalLift(*Expr.GetNativeArena(), node.Arguments[1]));
    LeftIUs = left.GetInputIUs(false, true);
    RightIUs = right.GetInputIUs(false, true);
    IncludesExpressions = !left.IsColumnAccess() || !right.IsColumnAccess() || NativeDecimalJoinKeyType(*Expr.GetNativeArena(), node).has_value();
    return;
}

TInfoUnitId TEquiJoinCondition::GetLeftIU() const {
    Y_ENSURE(LeftIUs.Size() == 1);
    return *LeftIUs.begin();
}

TInfoUnitId TEquiJoinCondition::GetRightIU() const {
    Y_ENSURE(RightIUs.Size() == 1);
    return *RightIUs.begin();
}

std::optional<TExpression> TEquiJoinCondition::ExtractExpressions(TMappedIUs<TExpression>& expressions) {
    Y_ENSURE(Expr.IsNative() && Expr.PlanProps, "Native equality requires plan properties");
    if (!IncludesExpressions) return std::nullopt;
    auto& arena = *Expr.GetNativeArena();
    // Copy child IDs before appending definitions can relocate arena nodes.
    const auto equality = arena[UnwrapNativeOptionalLift(arena, Expr.GetNativeRoot())];
    const auto operands = equality.Arguments;
    const auto keyType = NativeDecimalJoinKeyType(arena, equality);
    std::unordered_map<TNativeId, TNativeId> replacements;
    for (auto side : operands) {
        side = UnwrapNativeOptionalLift(arena, side);
        auto key = side;
        if (keyType) {
            // Scalar Decimal equality compares scales directly. Hash keys need
            // identical coefficients before the existing shuffle stage, not a
            // conversion inside the physical join after partitioning.
            const auto type = arena[side].Type;
            key = arena.Cast(side, *keyType, equality.Position);
            if (type.Scale > keyType->Scale || type.Precision - type.Scale + keyType->Scale > keyType->Precision) {
                // A value needing more integral or fractional digits cannot
                // equal a value from the other key domain. Keep it unmatched
                // rather than letting rounding/saturation create a match.
                key = arena.If(arena.Binary(TNativeOp::Equal, side, key, equality.Position), key,
                    arena.Literal(arena[key].Type.Optional(), {}, equality.Position), equality.Position);
            }
        }
        if ((arena[key].Op == TNativeOp::Column && key == side) || replacements.contains(side)) continue;
        const auto column = Expr.PlanProps->InfoUnitRegistry.AddGenerated("join_key");
        expressions.Add(column, NativePart(Expr, key));
        replacements.emplace(side, arena.Column(column, arena[key].Type, equality.Position));
    }
    return NativePart(Expr, arena.Rewrite(Expr.GetNativeRoot(), [&](TNativeId id) {
        const auto found = replacements.find(id);
        return found == replacements.end() ? id : found->second;
    }));
}

TExpression MakeColumnAccess(TInfoUnitId column, NFrontend::TType type,
    TPositionHandle pos, TPlanProps* props) {
    Y_ENSURE(props && props->NativeExpressions, "A column requires its native query arena");
    const auto& arena = props->NativeExpressions;
    return TExpression::FromNative(arena, arena->Column(column, type), props, pos);
}

TExpression MakeConstant(const TString& type, const TString& value, TPositionHandle pos, TPlanProps* props) {
    auto arena = props && props->NativeExpressions ? props->NativeExpressions : std::make_shared<NFrontend::TExpressionArena>();
    const auto text = arena->Literal({NFrontend::ETypeKind::String}, std::string(value.data(), value.size()));
    const NFrontend::TExprId args[]{text};
    return TExpression::FromNative(arena, arena->Call(std::string_view(type.data(), type.size()), args), props, pos);
}

TExpression MakeConjunction(const TVector<TExpression>& vec) {

    Y_ENSURE(!vec.empty(), "Native conjunction requires nonempty scalar operands");
    if (vec.size() == 1) {
        return vec.front();
    }
    auto arena = vec.front().GetNativeArena();
    auto root = ImportNative(*arena, vec.front());
    auto* props = vec.front().PlanProps;
    for (std::size_t i = 1; i < vec.size(); ++i) {
        root = arena->Binary(TNativeOp::And, root, ImportNative(*arena, vec[i]));
        if (vec[i].PlanProps) {
            props = vec[i].PlanProps;
        }
    }
    return TExpression::FromNative(std::move(arena), root, props, vec.front().Pos());
}

TExpression MakeNegation(const TExpression& expr) {

    auto arena = expr.GetNativeArena();
    const auto root = arena->Unary(TNativeOp::Not, expr.GetNativeRoot());
    return TExpression::FromNative(std::move(arena), root, expr.PlanProps, expr.Pos());
}

TExpression MakeBinaryPredicate(const TString& callable, const TExpression& left, const TExpression& right) {

    auto arena = left.GetNativeArena();
    const auto lhs = ImportNative(*arena, left);
    const auto rhs = ImportNative(*arena, right);
    if (callable == "Coalesce") {
        const TNativeId args[]{lhs, rhs};
        const auto root = arena->Coalesce(args);
        return TExpression::FromNative(std::move(arena), root, left.PlanProps ? left.PlanProps : right.PlanProps, left.Pos());
    }
    std::optional<TNativeOp> op;
    for (auto candidate : {TNativeOp::Equal, TNativeOp::NotEqual, TNativeOp::Less, TNativeOp::LessEqual,
        TNativeOp::Greater, TNativeOp::GreaterEqual, TNativeOp::And, TNativeOp::Or, TNativeOp::Add, TNativeOp::Subtract,
        TNativeOp::Multiply, TNativeOp::Divide, TNativeOp::Modulo}) {
        if (NativeOpName(candidate) == callable) {
            op = candidate;
            break;
        }
    }
    Y_ENSURE(op, "Unsupported native binary operation: " << callable);
    const auto root = arena->Binary(*op, lhs, rhs);
    return TExpression::FromNative(std::move(arena), root, left.PlanProps ? left.PlanProps : right.PlanProps, left.Pos());
}

TExpression MakeUnaryCallable(const TString& callable, const TExpression& arg) {

    auto arena = arg.GetNativeArena();
    const auto input = arg.GetNativeRoot();
    TNativeId root;
    if (callable == "Not") {
        root = arena->Unary(TNativeOp::Not, input);
    } else if (callable == "Exists") {
        root = arena->Unary(TNativeOp::IsNotNull, input);
    } else if (callable == "StablePickle") {
        root = arena->Unary(TNativeOp::StablePickle, input);
    } else if (callable == "Unwrap") {
        root = arena->Unary(TNativeOp::Unwrap, input);
    } else {
        const std::array<TNativeId, 1> arguments{input};
        root = arena->Call(std::string_view(callable.data(), callable.size()), arguments);
    }
    return TExpression::FromNative(std::move(arena), root, arg.PlanProps, arg.Pos());
}

TExpression MakeEnsure(const TExpression& value, const TExpression& predicate, const TString& message) {

    auto arena = value.GetNativeArena();
    const auto input = ImportNative(*arena, value), condition = ImportNative(*arena, predicate);
    const auto root = arena->Ensure(input, condition, std::string(message.data(), message.size()));
    return TExpression::FromNative(std::move(arena), root,
        value.PlanProps ? value.PlanProps : predicate.PlanProps, value.Pos());
}

} // namespace NKikimr::NKqp

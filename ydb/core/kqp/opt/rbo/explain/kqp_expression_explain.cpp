#include "../kqp_expression.h"

#include <yql/essentials/ast/yql_ast_escaping.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_type_ops.h>
#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/types/uuid/uuid.h>

#include <util/stream/str.h>
#include <util/string/ascii.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/escape.h>

#include <cstring>
#include <optional>

namespace NKikimr::NKqp {

namespace {

using namespace NYql::NNodes;

// Past this many printed nodes the remaining subtrees are printed as f(columns they depend on).
// This bounds the output of wide expressions and of DAGs, whose shared nodes are printed
// once per use.
constexpr size_t MaxPrintedNodes = 256;

// Items of a list or a call past this are elided with a count.
constexpr size_t MaxListItems = 16;

// Binding strength, weakest first. The order follows the YQL grammar.
enum class EPrecedence {
    Or,
    And,
    Xor,
    Equality,   // ==, !=, IN, IS [NOT] NULL, IS [NOT] DISTINCT FROM
    Comparison, // <, <=, >, >=
    Bitwise,
    Additive,
    Multiplicative,
    Concat,
    Unary,
    Primary,
};

struct TPrinted {
    TPrinted(TString text, EPrecedence precedence = EPrecedence::Primary, TStringBuf op = {})
        : Text(std::move(text))
        , Precedence(precedence)
        , Operator(op)
    {
    }

    TString Text;
    EPrecedence Precedence;
    // The root operator, so that a chain of the same operator needs no parentheses.
    TStringBuf Operator;
};

struct TOperator {
    TStringBuf Symbol;
    EPrecedence Precedence;
};

const THashMap<TStringBuf, TOperator>& BinaryOperators() {
    static const THashMap<TStringBuf, TOperator> operators = {
        {"==", {"==", EPrecedence::Equality}},
        {"!=", {"!=", EPrecedence::Equality}},
        {"AggrEquals", {"IS NOT DISTINCT FROM", EPrecedence::Equality}},
        {"AggrNotEquals", {"IS DISTINCT FROM", EPrecedence::Equality}},
        {"IsNotDistinctFrom", {"IS NOT DISTINCT FROM", EPrecedence::Equality}},
        {"IsDistinctFrom", {"IS DISTINCT FROM", EPrecedence::Equality}},
        {"<", {"<", EPrecedence::Comparison}},
        {"<=", {"<=", EPrecedence::Comparison}},
        {">", {">", EPrecedence::Comparison}},
        {">=", {">=", EPrecedence::Comparison}},
        {"BitAnd", {"&", EPrecedence::Bitwise}},
        {"BitOr", {"|", EPrecedence::Bitwise}},
        {"BitXor", {"^", EPrecedence::Bitwise}},
        {"ShiftLeft", {"<<", EPrecedence::Bitwise}},
        {"ShiftRight", {">>", EPrecedence::Bitwise}},
        {"+", {"+", EPrecedence::Additive}},
        {"-", {"-", EPrecedence::Additive}},
        {"DecimalAdd", {"+", EPrecedence::Additive}},
        {"DecimalSub", {"-", EPrecedence::Additive}},
        {"*", {"*", EPrecedence::Multiplicative}},
        {"/", {"/", EPrecedence::Multiplicative}},
        {"%", {"%", EPrecedence::Multiplicative}},
        {"DecimalMul", {"*", EPrecedence::Multiplicative}},
        {"DecimalDiv", {"/", EPrecedence::Multiplicative}},
        {"DecimalMod", {"%", EPrecedence::Multiplicative}},
        {"Concat", {"||", EPrecedence::Concat}},
    };
    return operators;
}

bool IsLogical(EPrecedence precedence) {
    return precedence <= EPrecedence::Xor;
}

bool IsComparison(EPrecedence precedence) {
    return precedence == EPrecedence::Equality || precedence == EPrecedence::Comparison;
}

bool IsArithmetic(EPrecedence precedence) {
    return precedence == EPrecedence::Additive || precedence == EPrecedence::Multiplicative;
}

// Only the precedence everyone knows is relied upon: logic below comparisons below
// arithmetic, and * above +. Mixed AND/OR/XOR, chained comparisons, and bitwise or ||
// next to other operators are always parenthesized.
bool NeedsParentheses(const TPrinted& operand, EPrecedence parent, TStringBuf parentOperator, bool rightOperand) {
    const auto child = operand.Precedence;
    if (parent == EPrecedence::Unary) {
        return child < EPrecedence::Primary;
    }
    if (child >= EPrecedence::Unary) {
        return false;
    }
    if (IsLogical(parent)) {
        return IsLogical(child) && operand.Operator != parentOperator;
    }
    if (IsComparison(parent)) {
        return IsLogical(child) || IsComparison(child);
    }
    if (IsArithmetic(parent) && IsArithmetic(child)) {
        return child < parent || (child == parent && rightOperand);
    }
    return operand.Operator != parentOperator || rightOperand;
}

TString Parenthesize(const TPrinted& operand, bool parenthesize) {
    if (!parenthesize) {
        return operand.Text;
    }
    return TStringBuilder() << "(" << operand.Text << ")";
}

// Adds the surrounding quotes itself.
TString QuoteString(TStringBuf value) {
    TStringStream out;
    EscapeArbitraryAtom(value, '"', &out);
    return out.Str();
}

TString QuoteIdentifier(TStringBuf name) {
    bool plain = !name.empty() && (IsAsciiAlpha(name.front()) || name.front() == '_');
    for (const char c : name) {
        plain = plain && (IsAsciiAlnum(c) || c == '_');
    }
    // Keywords excluded by id_expr / id_expr_in in SQLv1Antlr4.g.in, plus
    // literals. A qualifier also starts an expression, e.g. `not`.column.
    static constexpr TStringBuf reserved[] = {
        "ALL", "ANY", "AS", "ASSUME", "ASYMMETRIC", "BETWEEN", "BITCAST", "CASE", "CAST", "COMPACT", "CUBE",
        "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "DISTINCT", "EMPTY_ACTION", "ERASE", "EXCEPT", "EXISTS",
        "FALSE", "FROM", "FULL", "HAVING", "HOP", "INTERSECT", "JSON_EXISTS", "JSON_QUERY", "JSON_VALUE", "LIMIT",
        "NOT", "NULL", "PROCESS", "REDUCE", "RETURN", "RETURNING", "ROLLUP", "SELECT", "STREAM", "SYMMETRIC",
        "TRUE", "UNBOUNDED", "UNION", "WHEN", "WHERE", "WINDOW", "WITHOUT",
    };
    for (const auto keyword : reserved) {
        if (AsciiEqualsIgnoreCase(name, keyword)) {
            plain = false;
            break;
        }
    }
    if (plain) {
        return TString(name);
    }
    TStringBuilder text;
    text << '`';
    for (const char c : EscapeC(name)) {
        if (c == '`') {
            text << '`';
        }
        text << c;
    }
    return text << '`';
}

std::optional<TString> InternalValueText(NUdf::EDataSlot slot, NUdf::TUnboxedValuePod value) {
    // Keep the conversion's allocations off the caller's allocator, if any.
    NMiniKQL::TScopedAlloc alloc(__LOCATION__);
    const NUdf::TUnboxedValue text = NMiniKQL::ValueToString(slot, value);
    if (!text.HasValue()) {
        return {};
    }
    return TString(text.AsStringRef());
}

template <typename T>
std::optional<TString> InternalValueText(NUdf::EDataSlot slot, const TExprNode& atom) {
    T value;
    if (atom.Flags() & TNodeFlags::BinaryContent) {
        if (atom.Content().size() != sizeof(T)) {
            return {};
        }
        std::memcpy(&value, atom.Content().data(), sizeof(T));
    } else if (!TryFromString(atom.Content(), value)) {
        return {};
    }
    return InternalValueText(slot, NUdf::TUnboxedValuePod(value));
}

template <typename T>
std::optional<TString> InternalTzValueText(NUdf::EDataSlot slot, const TExprNode& atom,
    bool (*deserialize)(TStringBuf, T&, ui16&))
{
    T value;
    ui16 timezone;
    if (atom.Flags() & TNodeFlags::BinaryContent) {
        if (!deserialize(atom.Content(), value, timezone)) {
            return {};
        }
    } else {
        const auto content = atom.Content();
        const auto comma = content.find(',');
        if (comma == TStringBuf::npos || !TryFromString(content.SubStr(0, comma), value)) {
            return {};
        }
        const auto id = NMiniKQL::FindTimezoneId(content.SubStr(comma + 1));
        if (!id) {
            return {};
        }
        timezone = *id;
    }
    NUdf::TUnboxedValuePod unboxed(value);
    unboxed.SetTimezoneId(timezone);
    if (!NMiniKQL::IsValidValue(slot, unboxed)) {
        return {};
    }
    return InternalValueText(slot, unboxed);
}

// A literal may hold its raw bytes, and a date or time literal its internal value, e.g. 18262
// for Date 2020-01-01 after constant folding. Print the text it would be written with.
TString LiteralText(const TExprNode& literal) {
    const auto& atom = literal.Head();
    const auto slot = NUdf::FindDataSlot(literal.Content());
    if (literal.IsCallable("Uuid") && atom.Content().size() == NUuid::UUID_LEN) {
        return NUuid::UuidBytesToString(atom.Content());
    }
    const bool binary = atom.Flags() & TNodeFlags::BinaryContent;
    const bool dateTime = slot && (NUdf::GetDataTypeInfo(*slot).Features
        & (NUdf::EDataTypeFeatures::DateType | NUdf::EDataTypeFeatures::TimeIntervalType | NUdf::EDataTypeFeatures::TzDateType));
    if (!slot || (!binary && (!dateTime || NMiniKQL::IsValidStringValue(*slot, atom.Content())))) {
        return TString(atom.Content());
    }

    std::optional<TString> text;
    switch (*slot) {
        case NUdf::EDataSlot::Bool: text = InternalValueText<bool>(*slot, atom); break;
        case NUdf::EDataSlot::Int8: text = InternalValueText<i8>(*slot, atom); break;
        case NUdf::EDataSlot::Uint8: text = InternalValueText<ui8>(*slot, atom); break;
        case NUdf::EDataSlot::Int16: text = InternalValueText<i16>(*slot, atom); break;
        case NUdf::EDataSlot::Uint16: text = InternalValueText<ui16>(*slot, atom); break;
        case NUdf::EDataSlot::Int32: text = InternalValueText<i32>(*slot, atom); break;
        case NUdf::EDataSlot::Uint32: text = InternalValueText<ui32>(*slot, atom); break;
        case NUdf::EDataSlot::Int64: text = InternalValueText<i64>(*slot, atom); break;
        case NUdf::EDataSlot::Uint64: text = InternalValueText<ui64>(*slot, atom); break;
        case NUdf::EDataSlot::Float: text = InternalValueText<float>(*slot, atom); break;
        case NUdf::EDataSlot::Double: text = InternalValueText<double>(*slot, atom); break;
        case NUdf::EDataSlot::Date: text = InternalValueText<ui16>(*slot, atom); break;
        case NUdf::EDataSlot::Datetime: text = InternalValueText<ui32>(*slot, atom); break;
        case NUdf::EDataSlot::Timestamp: text = InternalValueText<ui64>(*slot, atom); break;
        case NUdf::EDataSlot::Interval: text = InternalValueText<i64>(*slot, atom); break;
        case NUdf::EDataSlot::Date32: text = InternalValueText<i32>(*slot, atom); break;
        case NUdf::EDataSlot::Datetime64: text = InternalValueText<i64>(*slot, atom); break;
        case NUdf::EDataSlot::Timestamp64: text = InternalValueText<i64>(*slot, atom); break;
        case NUdf::EDataSlot::Interval64: text = InternalValueText<i64>(*slot, atom); break;
        case NUdf::EDataSlot::TzDate: text = InternalTzValueText(*slot, atom, NMiniKQL::DeserializeTzDate); break;
        case NUdf::EDataSlot::TzDatetime: text = InternalTzValueText(*slot, atom, NMiniKQL::DeserializeTzDatetime); break;
        case NUdf::EDataSlot::TzTimestamp: text = InternalTzValueText(*slot, atom, NMiniKQL::DeserializeTzTimestamp); break;
        case NUdf::EDataSlot::TzDate32: text = InternalTzValueText(*slot, atom, NMiniKQL::DeserializeTzDate32); break;
        case NUdf::EDataSlot::TzDatetime64: text = InternalTzValueText(*slot, atom, NMiniKQL::DeserializeTzDatetime64); break;
        case NUdf::EDataSlot::TzTimestamp64: text = InternalTzValueText(*slot, atom, NMiniKQL::DeserializeTzTimestamp64); break;
        default: break;
    }
    return text.value_or(TString(atom.Content()));
}

bool IsFalse(const TExprNode& node) {
    return node.IsCallable("Bool") && node.ChildrenSize() == 1 && node.Head().IsAtom() && LiteralText(node) == "false";
}

std::optional<TPrinted> PrintLiteral(const TExprNode& node) {
    if (node.IsCallable({"Nothing", "Null"})) {
        return TPrinted{"NULL"};
    }
    if (node.ChildrenSize() == 0 || !node.Head().IsAtom()) {
        return {};
    }

    const auto value = LiteralText(node);
    if (TCoIntegralCtor::Match(&node) || node.IsCallable({"Float", "Double", "Decimal", "Bool"})) {
        // To its neighbours a negative number is a unary minus: -(-1), not --1.
        return TPrinted{TString(value), value.StartsWith('-') ? EPrecedence::Unary : EPrecedence::Primary};
    }
    if (node.IsCallable({"String", "Utf8"})) {
        return TPrinted{QuoteString(value)};
    }
    if (TCoDataCtor::Match(&node)) {
        return TPrinted{TStringBuilder() << node.Content() << "(" << QuoteString(value) << ")"};
    }
    return {};
}

std::optional<TString> PrintType(const TExprNode& type, size_t depth = 0) {
    if (depth >= MaxPrintedNodes) {
        return {};
    }
    if (type.IsCallable("DataType") && type.ChildrenSize() >= 1) {
        // Decimal carries its precision and scale as further atoms.
        TVector<TStringBuf> parts;
        for (const auto& child : type.Children()) {
            if (!child->IsAtom()) {
                return {};
            }
            parts.push_back(child->Content());
        }
        TStringBuilder name;
        name << parts[0];
        for (size_t i = 1; i < parts.size(); ++i) {
            name << (i == 1 ? "(" : ",") << parts[i];
        }
        if (parts.size() > 1) {
            name << ")";
        }
        return name;
    }
    if (type.IsCallable("OptionalType") && type.ChildrenSize() == 1) {
        if (auto item = PrintType(type.Head(), depth + 1)) {
            return *item + "?";
        }
        return {};
    }
    // Other types use the ordinary callable printer so shared compound types
    // are expanded under the same node budget as value expressions.
    return {};
}

class TPrinter {
public:
    TPrinter(const TExprNode& lambda, const TInfoUnitRegistry& registry, const TPlanProps* props)
        : Row(lambda.Head().Head())
        , Registry(registry)
        , Props(props)
    {
        // Parameters retain their public names. Reserve them before assigning
        // local names, including when a parameter is first used inside a lambda.
        THashSet<const TExprNode*> visited;
        TVector<const TExprNode*> pending{&lambda.Tail()};
        while (!pending.empty()) {
            const auto* node = pending.back();
            pending.pop_back();
            if (!visited.insert(node).second) {
                continue;
            }
            if (node->IsCallable("Parameter") && node->ChildrenSize() >= 1 && node->Head().IsAtom()) {
                const auto name = node->Head().Content();
                UsedArgumentNames.insert(TString(name.StartsWith('$') ? name.SubStr(1) : name));
            }
            for (const auto& child : node->Children()) {
                pending.push_back(child.Get());
            }
        }
    }

    TPrinted Print(const TExprNode::TPtr& node) {
        if (Budget == 0) {
            return Dependencies(node);
        }
        --Budget;
        if (IsWrapper(*node)) {
            return Print(node->HeadPtr());
        }

        switch (node->Type()) {
            case TExprNode::Atom:
                return {TString(node->Content())};
            case TExprNode::Argument:
                return {ArgumentName(*node)};
            case TExprNode::List:
                return {TStringBuilder() << "(" << Items(*node, 0) << ")"};
            case TExprNode::Lambda:
                return {TStringBuilder() << "(" << Arguments(node->Head()) << ") -> (" << Items(*node, 1) << ")"};
            case TExprNode::Callable:
                return Callable(node);
            default:
                return Dependencies(node);
        }
    }

private:
    // One optional layer over a known non-null value adds no useful detail.
    // Nested optionals, conversions, and Unwrap have observable semantics.
    static bool IsWrapper(const TExprNode& node) {
        if (!node.IsCallable("Just") || node.ChildrenSize() != 1) {
            return false;
        }
        if (const auto* type = node.Head().GetTypeAnn().Get()) {
            const auto kind = type->GetKind();
            return kind != ETypeAnnotationKind::Optional && kind != ETypeAnnotationKind::Null
                && kind != ETypeAnnotationKind::Pg && kind != ETypeAnnotationKind::Universal;
        }
        return TCoDataCtor::Match(&node.Head());
    }

    static const TExprNode& SkipWrappers(const TExprNode& node) {
        const auto* current = &node;
        while (IsWrapper(*current)) {
            current = &current->Head();
        }
        return *current;
    }

    TString ArgumentName(const TExprNode& argument) {
        if (const auto* name = ArgumentNames.FindPtr(&argument)) {
            return *name;
        }
        TString base(argument.Content().StartsWith('$') ? argument.Content().SubStr(1) : argument.Content());
        if (base.empty()) {
            base = "arg";
        }
        TString name = base;
        for (size_t suffix = 2; !UsedArgumentNames.insert(name).second; ++suffix) {
            name = TStringBuilder() << base << "_" << suffix;
        }
        TString result = TStringBuilder() << "$" << QuoteIdentifier(name);
        ArgumentNames.emplace(&argument, result);
        return result;
    }

    TString ColumnName(TInfoUnitId id) const {
        const auto name = Registry.GetDisplayName(id);
        const auto alias = Registry.Get(id).GetAlias();
        if (alias.empty()) {
            return QuoteIdentifier(name);
        }
        // Display-name disambiguation adds a suffix to the column, not the alias.
        return TStringBuilder() << QuoteIdentifier(alias) << "." << QuoteIdentifier(TStringBuf(name).SubStr(alias.size() + 1));
    }

    static bool IsOptionalToString(const TExprNode& node) {
        if (!node.IsCallable({"Map", "FlatMap"}) || node.ChildrenSize() != 2 || !node.Tail().IsLambda()
            || node.Tail().ChildrenSize() != 2 || node.Tail().Head().ChildrenSize() != 1)
        {
            return false;
        }
        const auto* type = node.Head().GetTypeAnn().Get();
        if (!type || type->GetKind() != ETypeAnnotationKind::Optional
            || type->Cast<TOptionalExprType>()->GetItemType()->GetKind() != ETypeAnnotationKind::Data)
        {
            return false;
        }
        const auto* body = &node.Tail().Tail();
        if (node.IsCallable("FlatMap")) {
            if (!body->IsCallable("Just") || body->ChildrenSize() != 1) {
                return false;
            }
            body = &body->Head();
        }
        return body->IsCallable("ToString") && body->ChildrenSize() == 1
            && &body->Head() == node.Tail().Head().Child(0);
    }

    TPrinted Callable(const TExprNode::TPtr& node) {
        if (auto literal = PrintLiteral(*node)) {
            return *literal;
        }

        const auto size = node->ChildrenSize();
        if (node->IsCallable("Member") && size == 2 && node->Tail().IsAtom()) {
            if (&node->Head() == &Row) {
                return {ColumnName(GetMemberId(*node))};
            }
            return Field(node->HeadPtr(), QuoteIdentifier(node->Tail().Content()));
        }
        if (node->IsCallable("Nth") && size == 2 && node->Tail().IsAtom()) {
            return Field(node->HeadPtr(), node->Tail().Content());
        }
        if (node->IsCallable("Parameter") && size >= 1 && node->Head().IsAtom()) {
            const auto name = node->Head().Content();
            return {TStringBuilder() << "$" << QuoteIdentifier(name.StartsWith('$') ? name.SubStr(1) : name)};
        }
        if (const auto* op = BinaryOperators().FindPtr(node->Content()); op && size == 2) {
            return Binary(op->Symbol, op->Precedence, node->HeadPtr(), node->TailPtr());
        }
        if (node->IsCallable("Or") && size >= 2) {
            return Logical("OR", EPrecedence::Or, *node);
        }
        if (node->IsCallable("And") && size >= 2) {
            return Logical("AND", EPrecedence::And, *node);
        }
        if (node->IsCallable("Xor") && size >= 2) {
            return Logical("XOR", EPrecedence::Xor, *node);
        }
        if (node->IsCallable("Not") && size == 1) {
            return Not(node->HeadPtr());
        }
        if (node->IsCallable("Exists") && size == 1) {
            return Postfix(node->HeadPtr(), "IS NOT NULL");
        }
        if (node->IsCallable("SqlIn") && size >= 2) {
            return In(*node, "IN");
        }
        if (node->IsCallable("Minus") && size == 1) {
            return Prefix("-", node->HeadPtr());
        }
        if (node->IsCallable("Plus") && size == 1) {
            return Prefix("+", node->HeadPtr());
        }
        if (node->IsCallable("BitNot") && size == 1) {
            return Prefix("~", node->HeadPtr());
        }
        if (node->IsCallable("SafeCast") && size >= 2) {
            if (auto type = PrintType(*node->Child(1))) {
                return {TStringBuilder() << "CAST(" << Print(node->HeadPtr()).Text << " AS " << *type << ")"};
            }
        }
        if (node->IsCallable("Convert") && size == 2) {
            const auto type = node->Tail().IsAtom() ? std::optional<TString>(TString(node->Tail().Content())) : PrintType(node->Tail());
            if (type) {
                return {TStringBuilder() << "Convert(" << Print(node->HeadPtr()).Text << ", " << *type << ")"};
            }
        }
        if (node->IsCallable("If") && size == 3) {
            // If treats a NULL condition as false, so Coalesce(p, false) reads as p.
            auto condition = node->HeadPtr();
            const auto& unwrapped = SkipWrappers(*condition);
            if (unwrapped.IsCallable("Coalesce") && unwrapped.ChildrenSize() == 2 && IsFalse(unwrapped.Tail())) {
                condition = unwrapped.HeadPtr();
            }
            return {TStringBuilder() << "If(" << Print(condition).Text << ", " << Print(node->ChildPtr(1)).Text << ", "
                << Print(node->ChildPtr(2)).Text << ")"};
        }
        if (IsOptionalToString(*node)) {
            // This direct conversion propagates NULL and prints its input once.
            // Keep general maps explicit: their null guard matters, and substituting
            // repeated arguments can expand a small DAG exponentially.
            return {TStringBuilder() << "ToString(" << Print(node->HeadPtr()).Text << ")"};
        }
        const auto isUnconfiguredUdf = [](const TExprNode& udf) {
            return udf.IsCallable("Udf") && udf.ChildrenSize() >= 1 && udf.Head().IsAtom()
                && (udf.ChildrenSize() <= TCoUdf::idx_RunConfigValue || udf.Child(TCoUdf::idx_RunConfigValue)->IsCallable("Void"))
                && (udf.ChildrenSize() <= TCoUdf::idx_TypeConfig
                    || (udf.Child(TCoUdf::idx_TypeConfig)->IsAtom() && udf.Child(TCoUdf::idx_TypeConfig)->Content().empty()));
        };
        if (node->IsCallable("Apply") && size >= 1 && isUnconfiguredUdf(node->Head())) {
            // YQL spells Module.Function as Module::Function.
            // Configured UDFs use the generic printer to retain patterns, options, etc.
            TString name(node->Head().Head().Content());
            if (const auto dot = name.find('.'); dot != TString::npos) {
                name.replace(dot, 1, "::");
            }
            return {Call(name, *node, 1)};
        }
        if (node->IsCallable("AsList")) {
            return {TStringBuilder() << "[" << Items(*node, 0) << "]"};
        }
        if (node->IsCallable("AsStruct")) {
            return {TStringBuilder() << "<|" << Items(*node, 0, [&](const TExprNode::TPtr& item) {
                if (item->IsList() && item->ChildrenSize() == 2 && item->Head().IsAtom()) {
                    return TString(TStringBuilder() << QuoteIdentifier(item->Head().Content()) << ": " << Print(item->TailPtr()).Text);
                }
                return Print(item).Text;
            }) << "|>"};
        }
        return {Call(node->Content(), *node, 0)};
    }

    TPrinted Field(const TExprNode::TPtr& base, TStringBuf name) {
        const auto printed = Print(base);
        return {TStringBuilder() << Parenthesize(printed, printed.Precedence < EPrecedence::Primary) << "." << name};
    }

    TPrinted Binary(TStringBuf symbol, EPrecedence precedence, const TExprNode::TPtr& left, const TExprNode::TPtr& right) {
        const auto lhs = Print(left);
        const auto rhs = Print(right);
        return {
            TStringBuilder()
                << Parenthesize(lhs, NeedsParentheses(lhs, precedence, symbol, false)) << " " << symbol << " "
                << Parenthesize(rhs, NeedsParentheses(rhs, precedence, symbol, true)),
            precedence,
            symbol,
        };
    }

    TPrinted Logical(TStringBuf symbol, EPrecedence precedence, const TExprNode& node) {
        TStringBuilder text;
        for (ui32 i = 0; i < node.ChildrenSize(); ++i) {
            if (i != 0) {
                text << " " << symbol << " ";
            }
            if (i != 0 && Budget == 0) {
                text << "... (" << node.ChildrenSize() - i << " more)";
                break;
            }
            const auto operand = Print(node.ChildPtr(i));
            text << Parenthesize(operand, NeedsParentheses(operand, precedence, symbol, false));
        }
        return {text, precedence, symbol};
    }

    TPrinted Not(const TExprNode::TPtr& operand) {
        const auto& unwrapped = SkipWrappers(*operand);
        if (unwrapped.IsCallable("Exists") && unwrapped.ChildrenSize() == 1) {
            return Postfix(unwrapped.HeadPtr(), "IS NULL");
        }
        // SQL IS NOT NULL is translated into a double negation.
        if (unwrapped.IsCallable("Not") && unwrapped.ChildrenSize() == 1) {
            const auto& inner = SkipWrappers(unwrapped.Head());
            if (inner.IsCallable("Exists") && inner.ChildrenSize() == 1) {
                return Postfix(inner.HeadPtr(), "IS NOT NULL");
            }
        }
        if (unwrapped.IsCallable("SqlIn") && unwrapped.ChildrenSize() >= 2) {
            return In(unwrapped, "NOT IN");
        }
        return Prefix("NOT", operand);
    }

    TPrinted Prefix(TStringBuf symbol, const TExprNode::TPtr& operand) {
        const auto printed = Print(operand);
        const auto separator = symbol == "NOT" ? " " : "";
        return {
            TStringBuilder() << symbol << separator << Parenthesize(printed, NeedsParentheses(printed, EPrecedence::Unary, symbol, false)),
            EPrecedence::Unary,
            symbol,
        };
    }

    TPrinted Postfix(const TExprNode::TPtr& operand, TStringBuf suffix) {
        const auto printed = Print(operand);
        return {
            TStringBuilder() << Parenthesize(printed, NeedsParentheses(printed, EPrecedence::Equality, suffix, false)) << " " << suffix,
            EPrecedence::Equality,
            suffix,
        };
    }

    TPrinted In(const TExprNode& in, TStringBuf symbol) {
        const auto lookup = Print(in.ChildPtr(TCoSqlIn::idx_Lookup));
        const auto& collection = in.ChildPtr(TCoSqlIn::idx_Collection);
        TString values;
        if (collection->IsCallable("AsList") || collection->IsList()) {
            values = TStringBuilder() << "(" << Items(*collection, 0) << ")";
        } else {
            const auto printed = Print(collection);
            values = Parenthesize(printed, printed.Precedence < EPrecedence::Primary);
        }
        return {
            TStringBuilder() << Parenthesize(lookup, NeedsParentheses(lookup, EPrecedence::Equality, symbol, false)) << " " << symbol << " " << values,
            EPrecedence::Equality,
            symbol,
        };
    }

    TString Call(TStringBuf name, const TExprNode& node, ui32 from) {
        return TStringBuilder() << name << "(" << Items(node, from) << ")";
    }

    TString Arguments(const TExprNode& arguments) {
        return Items(arguments, 0, [&](const TExprNode::TPtr& argument) { return ArgumentName(*argument); });
    }

    template <typename TPrintItem>
    TString Items(const TExprNode& node, ui32 from, TPrintItem&& printItem) {
        TStringBuilder text;
        for (ui32 i = from; i < node.ChildrenSize(); ++i) {
            if (i != from) {
                text << ", ";
            }
            if (i - from == MaxListItems || (i != from && Budget == 0)) {
                text << "... (" << node.ChildrenSize() - i << " more)";
                break;
            }
            text << printItem(node.ChildPtr(i));
        }
        return text;
    }

    TString Items(const TExprNode& node, ui32 from) {
        return Items(node, from, [&](const TExprNode::TPtr& item) { return Print(item).Text; });
    }

    TPrinted Dependencies(const TExprNode::TPtr& node) {
        TUnorderedIUs dependencies;
        THashSet<const TExprNode*> visited;
        THashSet<const TExprNode*> locals;
        TVector<const TExprNode*> arguments;
        TVector<const TExprNode*> pending{node.Get()};
        // The cut subtree can be arbitrarily deep. Collect dependencies without
        // recursing, including free lambda arguments used inside the subtree.
        while (!pending.empty()) {
            const auto* child = pending.back();
            pending.pop_back();
            if (!visited.insert(child).second) {
                continue;
            }
            if (child->IsCallable("Member") && child->ChildrenSize() == 2 && child->Tail().IsAtom() && &child->Head() == &Row) {
                dependencies.Add(GetMemberId(*child));
                continue;
            }
            if (child->IsLambda()) {
                for (const auto& argument : child->Head().Children()) {
                    locals.insert(argument.Get());
                }
            } else if (child->IsArgument() && child != &Row) {
                arguments.push_back(child);
            }
            for (size_t i = child->ChildrenSize(); i != 0; --i) {
                pending.push_back(child->Child(i - 1));
            }
        }
        // A subquery also depends on the columns it is called with.
        if (Props) {
            for (const auto call : Props->Subplans.CallsIn(dependencies)) {
                const auto& subplan = Props->Subplans.At(call);
                dependencies.UnionWith(subplan.Tuple.Unordered());
                dependencies.UnionWith(subplan.DependentIUs);
            }
        }
        TStringBuilder text;
        text << "f(";
        TStringBuf separator;
        for (const auto id : dependencies) {
            text << separator << ColumnName(id);
            separator = ", ";
        }
        for (const auto* argument : arguments) {
            if (!locals.contains(argument)) {
                text << separator << ArgumentName(*argument);
                separator = ", ";
            }
        }
        return {text << ")"};
    }

    const TExprNode& Row;
    const TInfoUnitRegistry& Registry;
    const TPlanProps* Props;
    size_t Budget = MaxPrintedNodes;
    THashMap<const TExprNode*, TString> ArgumentNames;
    THashSet<TString> UsedArgumentNames;
};

} // anonymous namespace

TString TExpression::ToExplainString(const TInfoUnitRegistry& registry) const {
    const auto& lambda = Node;
    if (!lambda || !lambda->IsLambda() || lambda->ChildrenSize() != 2 || lambda->Head().ChildrenSize() != 1) {
        return "f()";
    }
    return TPrinter(*lambda, registry, PlanProps).Print(lambda->TailPtr()).Text;
}

} // namespace NKikimr::NKqp

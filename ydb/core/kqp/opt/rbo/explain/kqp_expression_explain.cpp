#include "../kqp_expression.h"

#include <yql/essentials/ast/yql_ast_escaping.h>

#include <util/stream/str.h>
#include <util/string/cast.h>

namespace NKikimr::NKqp {
namespace {

using namespace NYql::NNodes;

TString QuoteString(TStringBuf value) {
    TStringStream out;
    out << '"';
    EscapeArbitraryAtom(value, '"', &out);
    out << '"';
    return out.Str();
}

std::optional<TString> FormatSimpleExpression(TExprNode::TPtr node, const TExprNode* row, const TInfoUnitRegistry& registry, ui32 depth = 0);

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

bool NeedParens(TExprNode::TPtr node) {
    return node->IsCallable() && (IsBinaryCallable(node->Content()) || node->IsCallable({"And", "Or", "Xor"}));
}

std::optional<TString> FormatAtomLiteral(TExprNode::TPtr node) {
    if (!node->IsCallable() || node->ChildrenSize() == 0 || !node->Child(0)->IsAtom()) {
        return {};
    }

    const auto callable = node->Content();
    const auto value = node->Child(0)->Content();
    if (callable == "String" || callable == "Utf8") {
        return QuoteString(value);
    }
    if (callable == "Bool") {
        return ToString(value);
    }
    if (callable == "Date" || callable == "Datetime" || callable == "Timestamp") {
        return TStringBuilder() << callable << "(" << value << ")";
    }
    if (callable == "Decimal") {
        return ToString(value);
    }
    if (callable == "Int8" || callable == "Int16" || callable == "Int32" || callable == "Int64" ||
        callable == "Uint8" || callable == "Uint16" || callable == "Uint32" || callable == "Uint64" ||
        callable == "Float" || callable == "Double")
    {
        return ToString(value);
    }

    return {};
}

std::optional<TString> FormatSimpleBinary(TExprNode::TPtr node, const TExprNode* row, const TInfoUnitRegistry& registry, ui32 depth) {
    if (node->ChildrenSize() != 2) {
        return {};
    }

    auto left = FormatSimpleExpression(node->ChildPtr(0), row, registry, depth + 1);
    auto right = FormatSimpleExpression(node->ChildPtr(1), row, registry, depth + 1);
    if (!left || !right) {
        return {};
    }

    if (NeedParens(node->ChildPtr(0))) {
        left = TStringBuilder() << "(" << *left << ")";
    }
    if (NeedParens(node->ChildPtr(1))) {
        right = TStringBuilder() << "(" << *right << ")";
    }

    return TStringBuilder() << *left << " " << GetBinaryOperator(node->Content()) << " " << *right;
}

std::optional<TString> FormatSimpleLogic(TExprNode::TPtr node, const TExprNode* row, const TInfoUnitRegistry& registry, ui32 depth) {
    TVector<TString> parts;
    for (const auto& child : node->Children()) {
        auto part = FormatSimpleExpression(child, row, registry, depth + 1);
        if (!part) {
            return {};
        }
        parts.push_back(*part);
    }

    TStringBuilder result;
    const auto logicOp = GetLogicOperator(node->Content());
    for (size_t i = 0; i < parts.size(); ++i) {
        if (i != 0) {
            result << " " << logicOp << " ";
        }
        result << parts[i];
    }
    return result;
}

std::optional<TString> FormatSimpleExpression(TExprNode::TPtr node, const TExprNode* row, const TInfoUnitRegistry& registry, ui32 depth) {
    if (!node || depth > 12) {
        return {};
    }
    if (node->IsLambda()) {
        return node->ChildrenSize() >= 2 ? FormatSimpleExpression(node->ChildPtr(1), row, registry, depth + 1) : std::optional<TString>();
    }
    if (!node->IsCallable()) {
        return {};
    }
    if (auto literal = FormatAtomLiteral(node)) {
        return literal;
    }
    if (node->IsCallable("Member")) {
        if (node->ChildrenSize() == 2 && node->Child(1)->IsAtom()) {
            if (node->Child(0) == row) {
                return registry.GetDisplayName(GetMemberId(*node));
            }
            return TString(node->Child(1)->Content());
        }
        return {};
    }
    if (node->IsCallable({"SafeCast", "Just", "Unwrap", "Convert"})) {
        return node->ChildrenSize() >= 1 ? FormatSimpleExpression(node->ChildPtr(0), row, registry, depth + 1) : std::optional<TString>();
    }
    if (node->IsCallable("Coalesce")) {
        if (node->ChildrenSize() != 2) {
            return {};
        }
        auto value = FormatSimpleExpression(node->ChildPtr(0), row, registry, depth + 1);
        auto fallback = FormatSimpleExpression(node->ChildPtr(1), row, registry, depth + 1);
        if (!value || !fallback) {
            return {};
        }
        return TStringBuilder() << "Coalesce(" << *value << ", " << *fallback << ")";
    }
    if (IsBinaryCallable(node->Content())) {
        return FormatSimpleBinary(node, row, registry, depth);
    }
    if (node->IsCallable({"And", "Or", "Xor"})) {
        return FormatSimpleLogic(node, row, registry, depth);
    }
    if (node->IsCallable("Not")) {
        if (node->ChildrenSize() != 1) {
            return {};
        }
        auto value = FormatSimpleExpression(node->ChildPtr(0), row, registry, depth + 1);
        if (!value) {
            return {};
        }
        if (NeedParens(node->ChildPtr(0))) {
            return TStringBuilder() << "NOT (" << *value << ")";
        }
        return TStringBuilder() << "NOT " << *value;
    }

    return {};
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

TString TExpression::ToExplainString(const TInfoUnitRegistry& registry) const {
    const auto* row = Node && Node->IsLambda() && Node->Head().ChildrenSize() == 1 ? &Node->Head().Head() : nullptr;
    if (auto simple = FormatSimpleExpression(Node, row, registry)) {
        return *simple;
    }
    return FormatExpressionDependencies(*this, registry);
}

} // namespace NKikimr::NKqp

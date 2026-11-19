#include "qlfilter.h"

#include <yt/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>

#include <yql/essentials/core/yql_expr_type_annotation.h>

#include <yql/essentials/utils/log/log.h>

#include <yql/essentials/utils/parse_double.h>

#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/generic/ylimits.h>

#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/type.h>

#include <cmath>
#include <utility>

namespace NYql {

using namespace NNodes;

namespace {

bool GetIntegerConstraints(const EDataSlot dataSlot, bool& isSigned, ui64& minValueAbs, ui64& maxValueAbs) {
    switch (dataSlot) {
        case EDataSlot::Uint8:
            isSigned = false;
            minValueAbs = 0;
            maxValueAbs = Max<ui8>();
            return true;
        case EDataSlot::Uint16:
            isSigned = false;
            minValueAbs = 0;
            maxValueAbs = Max<ui16>();
            return true;
        case EDataSlot::Uint32:
            isSigned = false;
            minValueAbs = 0;
            maxValueAbs = Max<ui32>();
            return true;
        case EDataSlot::Uint64:
            isSigned = false;
            minValueAbs = 0;
            maxValueAbs = Max<ui64>();
            return true;
        case EDataSlot::Int8:
            isSigned = true;
            minValueAbs = (ui64)Max<i8>() + 1;
            maxValueAbs = (ui64)Max<i8>();
            return true;
        case EDataSlot::Int16:
            isSigned = true;
            minValueAbs = (ui64)Max<i16>() + 1;
            maxValueAbs = (ui64)Max<i16>();
            return true;
        case EDataSlot::Int32:
            isSigned = true;
            minValueAbs = (ui64)Max<i32>() + 1;
            maxValueAbs = (ui64)Max<i32>();
            return true;
        case EDataSlot::Int64:
            isSigned = true;
            minValueAbs = (ui64)Max<i64>() + 1;
            maxValueAbs = (ui64)Max<i64>();
            return true;
        default:
            return false;
    }
}

void QuoteColumnForQL(const TStringBuf& columnName, TStringBuilder& result) {
    result << '`';
    if (!columnName.Contains('`') && !columnName.Contains('\\')) {
        result << columnName;
    } else {
        for (const auto c : columnName) {
            if (c == '`' || c == '\\') {
                result << '\\';
            }
            result << c;
        }
    }
    result << '`';
}

void ConvertComparisonForQL(const TStringBuf& opName, TStringBuilder& result) {
    if (opName == "==") {
        result << '=';
    } else {
        result << opName;
    }
}

TMaybe<TString> ConvertValueForQL(const TExprNode::TPtr& node) {
    if (const auto maybeData = TMaybeNode<TCoDataCtor>(node)) {
        const auto atom = maybeData.Cast().Literal();
        if (atom.Ref().Flags() & TNodeFlags::BinaryContent) {
            YQL_CLOG(ERROR, ProviderYt) << "YtQLFilter: unsupported binary content in const value";
            return {};
        }
        const TString value(atom.Value());
        if (TCoIntegralCtor::Match(node.Get()) || TCoBool::Match(node.Get())) {
            return {value};
        }
        if (TCoFloat::Match(node.Get()) || TCoDouble::Match(node.Get())) {
            auto literal = ConvertFloatingValueForQL(value, TCoFloat::Match(node.Get()));
            if (!literal) {
                YQL_CLOG(ERROR, ProviderYt) << "YtQLFilter: unsupported const value " << value;
            }
            return literal;
        }
        if (TCoString::Match(node.Get()) || TCoUtf8::Match(node.Get())) {
            return {value.Quote()};
        }
    }
    YQL_CLOG(ERROR, ProviderYt) << "YtQLFilter: unexpected type of const value " << node->Dump();
    return {};
}

TMaybe<bool> OptimizePossibleOutOfBounds(const TStringBuf& opName, EDataSlot columnDataSlot, const TExprNode::TPtr& intValue) {
    bool columnsIsSigned;
    ui64 minValueAbs;
    ui64 maxValueAbs;
    if (!GetIntegerConstraints(columnDataSlot, columnsIsSigned, minValueAbs, maxValueAbs)) {
        return {};
    }

    const TMaybeNode<TCoIntegralCtor> maybeIntValue(intValue);
    if (!maybeIntValue) {
        return {};
    }

    bool hasSign;
    bool isSigned;
    ui64 valueAbs;
    ExtractIntegralValue(maybeIntValue.Ref(), false, hasSign, isSigned, valueAbs);

    if (!hasSign && valueAbs > maxValueAbs) {
        // Value is greater than maximum.
        if (opName == ">" || opName == ">=" || opName == "==") {
            return {false};
        } else {
            return {true};
        }
    }
    if (hasSign && valueAbs > minValueAbs) {
        // Value is less than minimum.
        if (opName == "<" || opName == "<=" || opName == "==") {
            return {false};
        } else {
            return {true};
        }
    }
    return {};
}

// Returns the generated QL expression tree depth (references and literals have depth one),
// or Nothing() if the expression cannot be generated.
// Depth mirrors the QL parser's AST nesting; see yt/yt/library/query/base/parser.ypp.
// Use = NULL / != NULL throughout for compatibility with YT 25.1 and lower expression
// depth: IS NOT NULL becomes NOT(is_null(...)), whose constraints YT 25.1 cannot infer.
TMaybe<ui32> GenerateInputQueryComparison(const TStringBuf& opName, const TExprNode::TPtr& column, TExprNode::TPtr value, const TMaybe<bool>& nullValue, TStringBuilder& result) {
    for (auto maybeJust = TMaybeNode<TCoJust>(value); maybeJust;) {
        value = maybeJust.Cast().Input().Ptr();
        maybeJust = TMaybeNode<TCoJust>(value);
    }

    if (TMaybeNode<TCoNull>(value) || TMaybeNode<TCoNothing>(value)) {
        YQL_ENSURE(nullValue.Defined(), "YtQLFilter: optional type without coalesce is not supported");
        if (nullValue.GetRef()) {
            result << "TRUE";
        } else {
            result << "FALSE";
        }
        return 1;
    }

    bool columnIsOptional = false;
    const TDataExprType* dataType = nullptr;
    const bool columnHasDataType = IsDataOrOptionalOfData(column->GetTypeAnn(), columnIsOptional, dataType);
    if (columnIsOptional) {
        YQL_ENSURE(nullValue.Defined(), "YtQLFilter: optional type without coalesce is not supported");
    }

    YQL_ENSURE(columnHasDataType, "YtQLFilter: unsupported type of column " << column->Dump());
    YQL_ENSURE(dataType);
    const EDataSlot dataSlot = dataType->Cast<TDataExprType>()->GetSlot();
    const TMaybe<bool> constantFilter = OptimizePossibleOutOfBounds(opName, dataSlot, value);

    const auto columnName = column->ChildPtr(1)->Content();
    if (!constantFilter.Defined()) {
        // Value is in the range, comparison is not constant.
        bool addNullComparison = false;
        if (columnIsOptional) {
            // QL orders NULL below every non-NULL value.
            const bool comparisonResultForNull = opName == "<" || opName == "<=" || opName == "!=";
            if (comparisonResultForNull && !nullValue.GetRef()) {
                QuoteColumnForQL(columnName, result);
                result << " != NULL AND ";
                addNullComparison = true;
            } else if (!comparisonResultForNull && nullValue.GetRef()) {
                QuoteColumnForQL(columnName, result);
                result << " = NULL OR ";
                addNullComparison = true;
            }
        }
        const bool addNanComparison = IsDataTypeFloat(dataSlot) && (opName == ">" || opName == ">=");
        if (addNanComparison) {
            result << "(";
        }
        QuoteColumnForQL(columnName, result);
        result << " ";
        ConvertComparisonForQL(opName, result);
        const auto valueStr = ConvertValueForQL(value);
        if (!valueStr.Defined()) {
            return {};
        }
        result << " " << valueStr.GetRef();
        if (addNanComparison) {
            // NaN is above infinity in chunk constraints; keep IEEE comparisons under NOT.
            result << " AND ";
            QuoteColumnForQL(columnName, result);
            result << " <= inf)";
        }

        const ui32 valueDepth = valueStr->StartsWith('-') || valueStr->StartsWith('+') ? 2 : 1;
        const ui32 comparisonDepth = 1 + Max(1U, valueDepth) + (addNanComparison ? 1 : 0);
        return addNullComparison
            ? 1 + Max(2U, comparisonDepth)
            : comparisonDepth;
    } else if (constantFilter.GetRef()) {
        // Value is out of the range, comparison is always TRUE.
        if (columnIsOptional && !nullValue.GetRef()) {
            // Handle comparison with NULL as FALSE.
            QuoteColumnForQL(columnName, result);
            result << " != NULL";
            return 2;
        } else {
            result << "TRUE";
            return 1;
        }
    } else {
        // Value is out of the range, comparison is always FALSE.
        if (columnIsOptional && nullValue.GetRef()) {
            // Handle comparison with NULL as TRUE.
            QuoteColumnForQL(columnName, result);
            result << " = NULL";
            return 2;
        } else {
            result << "FALSE";
            return 1;
        }
    }
}

TMaybe<ui32> GenerateInputQueryComparison(const TCoCompare& op, const TMaybe<bool>& nullValue, TStringBuilder& result) {
    YQL_ENSURE(op.Ref().IsCallable({"<", "<=", ">", ">=", "==", "!="}));
    const auto left = op.Left().Ptr();
    const auto right = op.Right().Ptr();
    if (left->IsCallable("Member")) {
        return GenerateInputQueryComparison(op.CallableName(), left, right, nullValue, result);
    } else {
        YQL_ENSURE(right->IsCallable("Member"));
        auto invertedOp = op.CallableName();
        if (invertedOp == "<") {
            invertedOp = ">";
        } else if (invertedOp == "<=") {
            invertedOp = ">=";
        } else if (invertedOp == ">") {
            invertedOp = "<";
        } else if (invertedOp == ">=") {
            invertedOp = "<=";
        }
        return GenerateInputQueryComparison(invertedOp, right, left, nullValue, result);
    }
}

TMaybe<ui32> GenerateInputQueryWhereExpression(const TExprNode::TPtr& node, TStringBuilder& result);

struct TInputQueryLogicalOperand {
    TString Expression;
    ui32 Depth;
};

using TInputQueryLogicalOperands = TVector<TInputQueryLogicalOperand>;

ui32 GenerateInputQueryBalancedLogicalExpression(
    TInputQueryLogicalOperands::const_iterator begin,
    TInputQueryLogicalOperands::const_iterator end,
    TStringBuf op,
    TStringBuilder& result)
{
    YQL_ENSURE(begin != end);
    if (end - begin == 1) {
        result << begin->Expression;
        return begin->Depth;
    }

    const auto middle = begin + (end - begin) / 2;
    result << "(";
    const auto leftDepth = GenerateInputQueryBalancedLogicalExpression(begin, middle, op, result);
    result << ") " << op << " (";
    const auto rightDepth = GenerateInputQueryBalancedLogicalExpression(middle, end, op, result);
    result << ")";
    return 1 + Max(leftDepth, rightDepth);
}

ui32 GetInputQueryLinearLogicalExpressionDepth(const TInputQueryLogicalOperands& operands) {
    YQL_ENSURE(!operands.empty());
    ui32 depth = operands.front().Depth;
    for (auto it = operands.cbegin() + 1; it != operands.cend(); ++it) {
        depth = 1 + Max(depth, it->Depth);
    }
    return depth;
}

void GenerateInputQueryLinearLogicalExpression(
    const TInputQueryLogicalOperands& operands,
    TStringBuf op,
    TStringBuilder& result)
{
    YQL_ENSURE(!operands.empty());
    result << "(" << operands.front().Expression << ")";
    for (auto it = operands.cbegin() + 1; it != operands.cend(); ++it) {
        result << " " << op << " (" << it->Expression << ")";
    }
}

TMaybe<ui32> GenerateInputQueryLogicalExpression(
    const TExprNode::TListType& operandNodes,
    TStringBuf op,
    TStringBuilder& result)
{
    TInputQueryLogicalOperands operands;
    operands.reserve(operandNodes.size());
    for (const auto& operandNode : operandNodes) {
        TStringBuilder expression;
        const auto depth = GenerateInputQueryWhereExpression(operandNode, expression);
        if (!depth) {
            return {};
        }
        operands.push_back({std::move(expression), *depth});
    }

    TStringBuilder balancedExpression;
    const ui32 balancedDepth = GenerateInputQueryBalancedLogicalExpression(
        operands.cbegin(),
        operands.cend(),
        op,
        balancedExpression);
    const ui32 linearDepth = GetInputQueryLinearLogicalExpressionDepth(operands);

    if (balancedDepth <= linearDepth) {
        result << balancedExpression;
        return balancedDepth;
    }

    GenerateInputQueryLinearLogicalExpression(operands, op, result);
    return linearDepth;
}

TMaybe<ui32> GenerateInputQueryWhereExpression(const TExprNode::TPtr& node, TStringBuilder& result) {
    if (const auto maybeCompare = TMaybeNode<TCoCompare>(node)) {
        return GenerateInputQueryComparison(maybeCompare.Cast(), {}, result);
    } else if (node->IsCallable("Not")) {
        const auto child = node->ChildPtr(0);
        if (child->IsCallable("Exists")) {
            // Do not generate NOT (x IS NOT NULL).
            result << "(";
            const auto childDepth = GenerateInputQueryWhereExpression(child->ChildPtr(0), result);
            if (!childDepth) {
                return {};
            }
            result << ") = NULL";
            return *childDepth + 1;
        } else {
            result << "NOT (";
            const auto childDepth = GenerateInputQueryWhereExpression(child, result);
            if (!childDepth) {
                return {};
            }
            result << ")";
            return *childDepth + 1;
        }
    } else if (node->IsCallable("Exists")) {
        result << "(";
        const auto childDepth = GenerateInputQueryWhereExpression(node->ChildPtr(0), result);
        if (!childDepth) {
            return {};
        }
        result << ") != NULL";
        return *childDepth + 1;
    } else if (node->IsCallable({"And", "Or"})) {
        const TStringBuf op = node->IsCallable("And") ? "AND" : "OR";
        return GenerateInputQueryLogicalExpression(node->ChildrenList(), op, result);
    } else if (node->IsCallable("Coalesce")) {
        YQL_ENSURE(node->ChildrenSize() == 2);
        const auto op = TMaybeNode<TCoCompare>(node->Child(0)).Cast();
        const auto nullValueStr = TMaybeNode<TCoBool>(node->Child(1)).Cast().Literal().Value();
        const TMaybe<bool> nullValue(IsTrue(nullValueStr));
        return GenerateInputQueryComparison(op, nullValue, result);
    } else if (const auto maybeBool = TMaybeNode<TCoBool>(node)) {
        result << maybeBool.Cast().Literal().Value();
        return 1;
    } else if (node->IsCallable("Member")) {
        const auto columnName = node->ChildPtr(1)->Content();
        QuoteColumnForQL(columnName, result);
        return 1;
    } else {
        YQL_ENSURE(false, "unexpected node type");
    }
    return {};
}

} // namespace

TMaybe<TString> ConvertFloatingValueForQL(TStringBuf value, bool isFloat) {
    double parsed;
    // QL supports infinities, but not NaN literals.
    // TODO(YQLOVERYT-568): fold comparisons with NaN in the core optimizer.
    if (!TryDoubleFromString(value, parsed) || std::isnan(parsed)) {
        return {};
    }
    if (isFloat) {
        // Match YQL's Float constructor, then widen exactly to QL Double.
        parsed = static_cast<float>(parsed);
    }
    // The double overload round-trips exactly through QL's parser and normalizes forms such as hex.
    auto literal = FloatToString(parsed);
    // Keep integral-looking floating-point values out of the QL integer lexer.
    if (std::isfinite(parsed) && !literal.Contains('.') && !literal.Contains('e')) {
        literal += ".0";
    }
    return {literal};
}

TMaybe<TString> GenerateInputQuery(const TExprNode::TPtr& qlFilterNode, ui32 depthLimit) {
    YQL_ENSURE(qlFilterNode && qlFilterNode->IsCallable("YtQLFilter"));
    TStringBuilder result;
    result << "* WHERE ";
    const TYtQLFilter qlFilter(qlFilterNode);
    const auto depth = GenerateInputQueryWhereExpression(qlFilter.Predicate().Body().Ptr(), result);
    if (!depth) {
        YQL_CLOG(INFO, ProviderYt)  << __FUNCTION__ << ": Ignore YtQLFilter";
        return {};
    }
    if (depthLimit != 0 && *depth > depthLimit) {
        YQL_CLOG(INFO, ProviderYt) << __FUNCTION__ << ": Ignore YtQLFilter, expression is too deep (depth: " << *depth << ", limit: " << depthLimit << ")";
        return {};
    }
    YQL_CLOG(INFO, ProviderYt)  << __FUNCTION__ << ": Got input_query for YtQLFilter\n" << result;
    return {result};
}

} // namespace NYql

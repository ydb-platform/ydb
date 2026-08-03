#include "kqp_olap_expr_inspection.h"

#include <ydb/library/yql/utils/plan/plan_utils.h>

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_opt_utils.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/string/printf.h>
#include <util/string/vector.h>

#include <optional>
#include <utility>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

TString GetOlapColumnName(TStringBuf columnName, bool stripAliasPrefix) {
    if (!stripAliasPrefix) {
        return TString(columnName);
    }

    const auto it = columnName.find('.');
    if (it == TStringBuf::npos) {
        return TString(columnName);
    }
    return TString(columnName.substr(it + 1));
}

namespace {

const THashMap<TString, TString>& ComparisonSigns() {
    static const THashMap<TString, TString> signs = {
        {"eq", " == "},
        {"neq", " != "},
        {"lt", " < "},
        {"lte", " <= "},
        {"gt", " > "},
        {"gte", " >= "}
    };
    return signs;
}

const THashMap<TString, TString>& SubstringFormats() {
    static const THashMap<TString, TString> formats = {
        {"string_contains", "%s LIKE \"%%%s%%\""},
        {"starts_with", "%s LIKE \"%s%%\""},
        {"ends_with", "%s LIKE \"%%%s\""}
    };
    return formats;
}

bool IsAtomListComparison(const TCoAtomList& list, TString* columnName = nullptr) {
    const auto listPtr = list.Ptr();
    const size_t listSize = listPtr->Children().size();
    if (listSize != 3 && listSize != 4) {
        return false;
    }

    const auto op = TString(listPtr->Child(0)->Content());
    if (!ComparisonSigns().contains(op) && !SubstringFormats().contains(op)) {
        return false;
    }

    if (columnName) {
        *columnName = TString(listPtr->Child(1)->Content());
    }
    return true;
}

bool IsAtomListComparison(const TExprNode::TPtr& node) {
    if (!node || !node->IsList() || (node->ChildrenSize() != 3 && node->ChildrenSize() != 4) || !node->Child(0)->IsAtom()) {
        return false;
    }

    const auto op = TString(node->Child(0)->Content());
    return ComparisonSigns().contains(op) || SubstringFormats().contains(op);
}

void AddColumn(TOlapFilterInspection& inspection, const TString& columnName) {
    if (!columnName.empty()) {
        inspection.Columns.insert(columnName);
    }
}

void InspectOlapExpressionImpl(const TExprNode::TPtr& node, TOlapFilterInspection& inspection) {
    if (!node) {
        return;
    }

    if (const auto maybeFilter = TMaybeNode<TKqpOlapFilter>(node)) {
        inspection.HasOlapFilter = true;
        const auto filter = maybeFilter.Cast();
        InspectOlapExpressionImpl(filter.Input().Ptr(), inspection);
        InspectOlapExpressionImpl(filter.Condition().Ptr(), inspection);
        return;
    }

    if (const auto maybeProjections = TMaybeNode<TKqpOlapProjections>(node)) {
        inspection.HasOlapProjections = true;
        const auto projections = maybeProjections.Cast();
        InspectOlapExpressionImpl(projections.Input().Ptr(), inspection);
        for (const auto& projectionNode : projections.Projections()) {
            const auto projection = TExprBase(projectionNode).Cast<TKqpOlapProjection>();
            InspectOlapExpressionImpl(projection.OlapOperation().Ptr(), inspection);
        }
        return;
    }

    if (const auto maybeColumnArg = TMaybeNode<TKqpOlapApplyColumnArg>(node)) {
        inspection.HasOlapApply = true;
        AddColumn(inspection, TString(maybeColumnArg.Cast().ColumnName().StringValue()));
        return;
    }

    if (node->IsCallable("Member")) {
        if (node->ChildrenSize() == 2 && node->Child(1)->IsAtom()) {
            AddColumn(inspection, TString(node->Child(1)->Content()));
        }
        return;
    }

    if (const auto maybeJsonValue = TMaybeNode<TKqpOlapJsonValue>(node)) {
        AddColumn(inspection, TString(maybeJsonValue.Cast().Column().StringValue()));
        return;
    }

    if (const auto maybeJsonExists = TMaybeNode<TKqpOlapJsonExists>(node)) {
        AddColumn(inspection, TString(maybeJsonExists.Cast().Column().StringValue()));
        return;
    }

    if (const auto maybeExists = TMaybeNode<TKqpOlapFilterExists>(node)) {
        AddColumn(inspection, TString(maybeExists.Cast().Column().StringValue()));
        return;
    }

    if (const auto maybeUnaryOp = TMaybeNode<TKqpOlapFilterUnaryOp>(node)) {
        InspectOlapExpressionImpl(maybeUnaryOp.Cast().Arg().Ptr(), inspection);
        return;
    }

    if (const auto maybeBinaryOp = TMaybeNode<TKqpOlapFilterBinaryOp>(node)) {
        const auto binaryOp = maybeBinaryOp.Cast();
        InspectOlapExpressionImpl(binaryOp.Left().Ptr(), inspection);
        InspectOlapExpressionImpl(binaryOp.Right().Ptr(), inspection);
        return;
    }

    if (const auto maybeTernaryOp = TMaybeNode<TKqpOlapFilterTernaryOp>(node)) {
        const auto ternaryOp = maybeTernaryOp.Cast();
        InspectOlapExpressionImpl(ternaryOp.First().Ptr(), inspection);
        InspectOlapExpressionImpl(ternaryOp.Second().Ptr(), inspection);
        InspectOlapExpressionImpl(ternaryOp.Third().Ptr(), inspection);
        return;
    }

    if (const auto maybeNot = TMaybeNode<TKqpOlapNot>(node)) {
        InspectOlapExpressionImpl(maybeNot.Cast().Value().Ptr(), inspection);
        return;
    }

    if (TKqpOlapAnd::Match(node.Get()) || TKqpOlapOr::Match(node.Get()) || TKqpOlapXor::Match(node.Get())) {
        for (const auto& child : node->ChildrenList()) {
            InspectOlapExpressionImpl(child, inspection);
        }
        return;
    }

    if (const auto maybeAtomList = TMaybeNode<TCoAtomList>(node)) {
        TString columnName;
        if (IsAtomListComparison(maybeAtomList.Cast(), &columnName)) {
            AddColumn(inspection, columnName);
            return;
        }

        for (const auto& child : node->ChildrenList()) {
            InspectOlapExpressionImpl(child, inspection);
        }
        return;
    }

    if (const auto maybeApply = TMaybeNode<TKqpOlapApply>(node)) {
        inspection.HasOlapApply = true;
        InspectOlapExpressionImpl(maybeApply.Cast().Lambda().Ptr(), inspection);
        return;
    }

    for (const auto& child : node->ChildrenList()) {
        InspectOlapExpressionImpl(child, inspection);
    }
}

TString AtomListValue(const TExprNode& node, bool quoteString) {
    if (node.ChildrenSize() >= 1) {
        auto value = TString(node.Child(0)->Content());
        if (quoteString && TString(node.Content()) == "String") {
            value = TStringBuilder() << '"' << value << '"';
        }
        return value;
    }

    if (node.ChildrenSize() == 0 && node.Content()) {
        return TString(node.Content());
    }

    return {};
}

TString FormatOlapFilterExpr(const TExprNode::TPtr& node) {
    if (!node) {
        return {};
    }

    TVector<TString> parts;
    if (TMaybeNode<TKqpOlapNot>(node)) {
        parts.emplace_back("NOT");
    } else if (auto maybeList = TMaybeNode<TCoAtomList>(node)) {
        auto listPtr = maybeList.Cast().Ptr();
        TString columnName;
        if (IsAtomListComparison(maybeList.Cast(), &columnName)) {
            const auto op = TString(listPtr->Child(0)->Content());

            if (ComparisonSigns().contains(op)) {
                const auto value = AtomListValue(*listPtr->Child(2), true);
                return TStringBuilder() << columnName << ComparisonSigns().at(op) << value;
            }
            if (SubstringFormats().contains(op)) {
                const auto value = AtomListValue(*listPtr->Child(2), false);
                return Sprintf(SubstringFormats().at(op).c_str(), columnName.c_str(), value.c_str());
            }
        }
    } else if (auto olapApply = TMaybeNode<TKqpOlapApply>(node)) {
        try {
            return NPlanUtils::ExtractPredicate(olapApply.Cast().Lambda()).Body;
        } catch (...) {
            return {};
        }
    }

    for (const auto& child : node->Children()) {
        auto childStr = FormatOlapFilterExpr(child);
        if (!childStr.empty()) {
            parts.push_back(std::move(childStr));
        }
    }

    TString delim = " ";
    if (TMaybeNode<TKqpOlapAnd>(node)) {
        delim = " AND ";
    } else if (TMaybeNode<TKqpOlapOr>(node)) {
        delim = " OR ";
    }
    return JoinStrings(parts, delim);
}

TString NormalizeLiteralText(TString value) {
    if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
        return value.substr(1, value.size() - 2);
    }
    return value;
}

bool IsNothingLike(const TExprNode::TPtr& node) {
    if (!node) {
        return false;
    }

    if (node->IsCallable("Nothing")) {
        return true;
    }

    if (node->IsCallable({"Just", "SafeCast", "StrictCast", "Convert", "ToString", "FromPg", "ToPg"}) && node->ChildrenSize() > 0) {
        return IsNothingLike(node->HeadPtr());
    }

    return false;
}

std::optional<TString> FormatLiteralValue(const TExprNode::TPtr& node) {
    if (!node || IsNothingLike(node)) {
        return std::nullopt;
    }

    if (node->IsCallable({"Just", "SafeCast", "StrictCast", "Convert", "ToString", "FromPg", "ToPg"}) && node->ChildrenSize() > 0) {
        return FormatLiteralValue(node->HeadPtr());
    }

    if (auto data = TExprBase(node).Maybe<TCoDataCtor>()) {
        return NormalizeLiteralText(TString(data.Cast().Literal().Value()));
    }

    if (auto pg = TExprBase(node).Maybe<TCoPgConst>()) {
        return NormalizeLiteralText(TString(pg.Cast().Value().Value()));
    }

    return std::nullopt;
}

struct TExplainRangeBound {
    std::optional<TString> Value;
    bool Inclusive = false;
};

struct TExplainColumnRange {
    TExplainRangeBound From;
    TExplainRangeBound To;
};

using TExplainRange = TVector<TExplainColumnRange>;
using TExplainRanges = TVector<TExplainRange>;

TExplainColumnRange MakeFullColumnRange() {
    return {};
}

// Bound values are formatted strings, so ordering is unknown: intersection is only representable
// when one side is unbounded or both sides carry the same value.
std::optional<TExplainRangeBound> IntersectBound(const TExplainRangeBound& left, const TExplainRangeBound& right) {
    if (!left.Value) {
        return right;
    }
    if (!right.Value) {
        return left;
    }
    if (*left.Value != *right.Value) {
        return std::nullopt;
    }

    return TExplainRangeBound{.Value = left.Value, .Inclusive = left.Inclusive && right.Inclusive};
}

std::optional<TExplainColumnRange> IntersectColumnRange(const TExplainColumnRange& left, const TExplainColumnRange& right) {
    auto from = IntersectBound(left.From, right.From);
    auto to = IntersectBound(left.To, right.To);
    if (!from || !to) {
        return std::nullopt;
    }

    return TExplainColumnRange{.From = std::move(*from), .To = std::move(*to)};
}

std::optional<TExplainRange> IntersectExplainRange(const TExplainRange& left, const TExplainRange& right) {
    TExplainRange result;
    const size_t size = Max(left.size(), right.size());
    result.reserve(size);

    for (size_t i = 0; i < size; ++i) {
        const auto& leftColumn = i < left.size() ? left[i] : MakeFullColumnRange();
        const auto& rightColumn = i < right.size() ? right[i] : MakeFullColumnRange();
        auto column = IntersectColumnRange(leftColumn, rightColumn);
        if (!column) {
            return std::nullopt;
        }
        result.push_back(std::move(*column));
    }

    return result;
}

std::optional<TExplainRanges> IntersectExplainRanges(const TExplainRanges& left, const TExplainRanges& right) {
    TExplainRanges result;
    for (const auto& leftRange : left) {
        for (const auto& rightRange : right) {
            auto range = IntersectExplainRange(leftRange, rightRange);
            if (!range) {
                return std::nullopt;
            }
            result.push_back(std::move(*range));
        }
    }
    return result;
}

TExplainRanges MultiplyExplainRanges(const TExplainRanges& left, const TExplainRanges& right) {
    TExplainRanges result;
    for (const auto& leftRange : left) {
        for (const auto& rightRange : right) {
            TExplainRange range = leftRange;
            range.insert(range.end(), rightRange.begin(), rightRange.end());
            result.push_back(std::move(range));
        }
    }
    return result;
}

std::optional<TExplainRanges> ParseAsRangeForExplain(const TExprNode::TPtr& node) {
    TExplainRanges result;
    for (const auto& range : node->ChildrenList()) {
        if (!range->IsList() || range->ChildrenSize() != 2) {
            return std::nullopt;
        }

        const auto& from = range->Head();
        const auto& to = range->Tail();
        if (!from.IsList() || !to.IsList() || from.ChildrenSize() == 0 || from.ChildrenSize() != to.ChildrenSize()) {
            return std::nullopt;
        }

        const size_t columnCount = from.ChildrenSize() - 1;
        auto fromInclusive = FormatLiteralValue(from.ChildPtr(columnCount));
        auto toInclusive = FormatLiteralValue(to.ChildPtr(columnCount));
        if (!fromInclusive || !toInclusive) {
            return std::nullopt;
        }

        TExplainRange explainRange;
        explainRange.reserve(columnCount);
        for (size_t i = 0; i < columnCount; ++i) {
            TExplainColumnRange columnRange;
            if (!IsNothingLike(from.ChildPtr(i))) {
                columnRange.From.Value = FormatLiteralValue(from.ChildPtr(i));
                if (!columnRange.From.Value) {
                    return std::nullopt;
                }
            }
            if (!IsNothingLike(to.ChildPtr(i))) {
                columnRange.To.Value = FormatLiteralValue(to.ChildPtr(i));
                if (!columnRange.To.Value) {
                    return std::nullopt;
                }
            }
            columnRange.From.Inclusive = *fromInclusive == "1";
            columnRange.To.Inclusive = *toInclusive == "1";
            explainRange.push_back(std::move(columnRange));
        }

        result.push_back(std::move(explainRange));
    }
    return result;
}

std::optional<TExplainRanges> ParseRangeForExplain(const TExprNode::TPtr& node) {
    const TString op = TString(node->Head().Content());
    if (op == "Exists" || op == "NotExists" || op == "StartsWith" || op == "NotStartsWith") {
        return std::nullopt;
    }

    auto value = FormatLiteralValue(node->ChildPtr(1));
    if (!value) {
        return std::nullopt;
    }

    auto makeSingleColumnRange = [](TExplainRangeBound from, TExplainRangeBound to) {
        TExplainRange range;
        range.push_back(TExplainColumnRange{.From = std::move(from), .To = std::move(to)});
        return range;
    };

    if (op == "==" || op == "===") {
        TExplainRangeBound bound{.Value = value, .Inclusive = true};
        return TExplainRanges{makeSingleColumnRange(bound, bound)};
    }
    if (op == "!=") {
        TExplainRangeBound bound{.Value = value, .Inclusive = false};
        return TExplainRanges{
            makeSingleColumnRange({}, bound),
            makeSingleColumnRange(bound, {}),
        };
    }
    if (op == "<" || op == "<=") {
        return TExplainRanges{makeSingleColumnRange({}, {.Value = value, .Inclusive = op == "<="})};
    }
    if (op == ">" || op == ">=") {
        return TExplainRanges{makeSingleColumnRange({.Value = value, .Inclusive = op == ">="}, {})};
    }

    return std::nullopt;
}

std::optional<TExplainRanges> ParseRangesForExplain(const TExprNode::TPtr& node) {
    if (!node) {
        return std::nullopt;
    }

    if (node->IsCallable("RangeFinalize") || node->IsCallable("RangeToPg")) {
        return ParseRangesForExplain(node->HeadPtr());
    }

    if (node->IsCallable("RangeEmpty")) {
        return TExplainRanges{};
    }

    if (node->IsCallable("AsRange")) {
        return ParseAsRangeForExplain(node);
    }

    if (node->IsCallable("RangeFor")) {
        return ParseRangeForExplain(node);
    }

    if (node->IsCallable("RangeUnion")) {
        TExplainRanges result;
        for (const auto& child : node->ChildrenList()) {
            auto childRanges = ParseRangesForExplain(child);
            if (!childRanges) {
                return std::nullopt;
            }
            result.insert(result.end(), childRanges->begin(), childRanges->end());
        }
        return result;
    }

    if (node->IsCallable("RangeIntersect")) {
        std::optional<TExplainRanges> result;
        for (const auto& child : node->ChildrenList()) {
            auto childRanges = ParseRangesForExplain(child);
            if (!childRanges) {
                return std::nullopt;
            }
            if (!result) {
                result = std::move(childRanges);
            } else {
                result = IntersectExplainRanges(*result, *childRanges);
                if (!result) {
                    return std::nullopt;
                }
            }
        }
        if (result) {
            return result;
        }
        return TExplainRanges{};
    }

    if (node->IsCallable("RangeMultiply")) {
        TExplainRanges result{{}};
        for (size_t i = 1; i < node->ChildrenSize(); ++i) {
            auto childRanges = ParseRangesForExplain(node->ChildPtr(i));
            if (!childRanges) {
                return std::nullopt;
            }
            result = MultiplyExplainRanges(result, *childRanges);
        }
        return result;
    }

    return std::nullopt;
}

TExprNode::TPtr RenameColumnsImpl(const TExprNode::TPtr& node, const THashMap<TString, TString>& renameMap, TExprContext& ctx) {
    if (!node) {
        return node;
    }

    if (node->IsCallable("Member") && node->ChildrenSize() == 2 && node->Child(1)->IsAtom()) {
        const auto it = renameMap.find(TString(node->Child(1)->Content()));
        if (it != renameMap.end()) {
            auto children = node->ChildrenList();
            children[1] = ctx.NewAtom(node->Child(1)->Pos(), it->second);
            return ctx.ChangeChildren(*node, std::move(children));
        }
        return node;
    }

    bool changed = false;
    auto children = node->ChildrenList();
    for (ui32 i = 0; i < children.size(); ++i) {
        if (IsAtomListComparison(node) && i == 1 && children[i]->IsAtom()) {
            const auto it = renameMap.find(TString(children[i]->Content()));
            if (it != renameMap.end()) {
                children[i] = ctx.NewAtom(children[i]->Pos(), it->second);
                changed = true;
                continue;
            }
        }

        auto renamed = RenameColumnsImpl(children[i], renameMap, ctx);
        if (renamed != children[i]) {
            children[i] = std::move(renamed);
            changed = true;
        }
    }

    return changed ? ctx.ChangeChildren(*node, std::move(children)) : node;
}

} // anonymous namespace

TOlapFilterInspection TOlapFilterInspector::Inspect(const TExprNode::TPtr& node) {
    TOlapFilterInspection inspection;
    InspectOlapExpressionImpl(node, inspection);
    return inspection;
}

TOlapFilterInspection TOlapFilterInspector::InspectLambda(const TExprNode::TPtr& lambda) {
    TOlapFilterInspection inspection;
    if (!lambda) {
        return inspection;
    }

    try {
        InspectOlapExpressionImpl(lambda, inspection);
    } catch (...) {
        inspection.RequiresAllInputColumns = true;
    }
    return inspection;
}

TExprNode::TPtr TOlapFilterInspector::RenameColumns(
    const TExprNode::TPtr& node,
    const THashMap<TString, TString>& renameMap,
    TExprContext& ctx)
{
    if (renameMap.empty()) {
        return node;
    }
    return RenameColumnsImpl(node, renameMap, ctx);
}

TString TOlapFilterInspector::Format(const TKqpOlapFilter& filter) {
    auto result = FormatOlapFilterExpr(filter.Condition().Ptr());
    if (auto maybeInnerFilter = TMaybeNode<TKqpOlapFilter>(filter.Input().Ptr())) {
        return TStringBuilder() << '(' << Format(maybeInnerFilter.Cast()) << ") AND (" << result << ')';
    }
    return result;
}

TOlapFilterInspection InspectOlapExpression(const TExprNode::TPtr& node) {
    return TOlapFilterInspector::Inspect(node);
}

TOlapFilterInspection InspectOlapProcessLambda(const TExprNode::TPtr& lambda) {
    return TOlapFilterInspector::InspectLambda(lambda);
}

TString FormatOlapFilter(const TKqpOlapFilter& filter) {
    return TOlapFilterInspector::Format(filter);
}

TVector<TString> BuildReadRangeDescriptions(const TExprNode::TPtr& ranges, const TVector<TString>& keyColumns, size_t usedPrefixLen) {
    auto parsedRanges = ParseRangesForExplain(ranges);
    if (!parsedRanges) {
        return {};
    }

    TVector<TString> result;
    for (const auto& range : *parsedRanges) {
        size_t columnCount = Min(range.size(), keyColumns.size());
        columnCount = Min(columnCount, usedPrefixLen);
        for (size_t i = 0; i < columnCount; ++i) {
            const auto& columnRange = range[i];
            if (!columnRange.From.Value && !columnRange.To.Value) {
                continue;
            }

            TStringBuilder desc;
            desc << GetOlapColumnName(keyColumns[i], true) << " "
                 << (columnRange.From.Inclusive ? "[" : "(")
                 << (columnRange.From.Value ? *columnRange.From.Value : "-∞") << ", "
                 << (columnRange.To.Value ? *columnRange.To.Value : "+∞")
                 << (columnRange.To.Inclusive ? "]" : ")");
            result.push_back(desc);
        }
    }
    return result;
}

} // namespace NKikimr::NKqp::NOpt

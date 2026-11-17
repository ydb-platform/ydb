#include "kqp_read_ranges_explain.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>

#include <util/generic/algorithm.h>
#include <util/string/builder.h>

#include <optional>

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

    if (node->IsCallable({"Just", "SafeCast", "StrictCast", "Convert", "ToString"}) && node->ChildrenSize() > 0) {
        return IsNothingLike(node->HeadPtr());
    }

    return false;
}

std::optional<TString> FormatLiteralValue(const TExprNode::TPtr& node) {
    if (!node || IsNothingLike(node)) {
        return std::nullopt;
    }

    if (node->IsCallable({"Just", "SafeCast", "StrictCast", "Convert", "ToString"}) && node->ChildrenSize() > 0) {
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

} // namespace

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

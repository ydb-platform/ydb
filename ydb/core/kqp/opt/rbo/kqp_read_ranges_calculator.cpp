#include "kqp_read_ranges_calculator.h"

#include "kqp_read_ranges_sql_in.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>

#include <util/generic/algorithm.h>
#include <util/generic/deque.h>
#include <util/string/cast.h>

namespace NKikimr::NKqp::NOpt::NReadRanges {

using namespace NYql;
using NUdf::EDataSlot;

namespace {

// The calculation follows the runtime: RangeFor as ExpandRangeFor and BuildNormalRangeLambdaRaw in
// yql/essentials/core/yql_opt_range.cpp build it, the range algebra as yql/essentials/minikql/comp_nodes/mkql_range.cpp
// computes it, and IN as the predicate extractor builds it. Any step it cannot follow exactly fails the whole
// calculation, so its result is either exact or absent.
class TRangeCalculator {
public:
    std::optional<TRangeList> Calculate(const TExprNode::TPtr& node) {
        auto result = CalculateNode(node);
        if (Failed_) {
            return std::nullopt;
        }
        return result;
    }

private:
    std::optional<TRangeList> CalculateNode(const TExprNode::TPtr& node) {
        auto result = DoCalculateNode(node);
        if (Failed_ || (result && OverBudget(result->Ranges.size()))) {
            return std::nullopt;
        }
        return result;
    }

    std::optional<TRangeList> DoCalculateNode(const TExprNode::TPtr& node) {
        if (const auto in = MatchSqlInRanges(node)) {
            return CalculateSqlIn(*in);
        }
        if (node->IsCallable({"RangeFinalize", "RangeToPg"}) && node->ChildrenSize() == 1) {
            auto list = CalculateNode(node->HeadPtr());
            if (!list) {
                return std::nullopt;
            }
            return node->IsCallable("RangeFinalize") ? Finalize(std::move(*list)) : list;
        }
        if (node->IsCallable("If") && node->ChildrenSize() == 3) {
            // A condition that does not depend on the row, such as a literal or a parameter.
            const auto condition = ParseKeyValue(node->HeadPtr());
            if (!condition || condition->Kind != TKeyValue::EKind::Data || condition->Slot != EDataSlot::Bool) {
                return std::nullopt;
            }
            return CalculateNode(condition->Value.Get<bool>() ? node->ChildPtr(1) : node->ChildPtr(2));
        }
        if (node->IsCallable("RangeEmpty")) {
            return ParseRangeEmpty(*node);
        }
        if (node->IsCallable("AsRange")) {
            return ParseAsRange(*node);
        }
        if (node->IsCallable("RangeFor") && node->ChildrenSize() == 3) {
            return ExpandRangeFor(node->Head().Content(), node->ChildPtr(1), node->Tail());
        }
        if (node->IsCallable({"RangeUnion", "RangeIntersect"}) && node->ChildrenSize() > 0) {
            auto lists = CalculateChildren(*node, 0);
            if (!lists) {
                return std::nullopt;
            }
            for (const auto& list : *lists) {
                if (list.Width != lists->front().Width) {
                    return std::nullopt;
                }
            }
            return node->IsCallable("RangeUnion") ? Union(std::move(*lists)) : Intersect(std::move(*lists));
        }
        if (node->IsCallable("RangeMultiply") && node->ChildrenSize() > 1) {
            ui64 limit;
            if (!node->Head().IsCallable("Uint64") || !TryFromString(node->Head().Head().Content(), limit)) {
                return std::nullopt;
            }
            auto lists = CalculateChildren(*node, 1);
            if (!lists) {
                return std::nullopt;
            }
            return Multiply(limit, std::move(*lists));
        }
        return std::nullopt;
    }

    std::optional<TVector<TRangeList>> CalculateChildren(const TExprNode& node, size_t start) {
        TVector<TRangeList> result;
        for (size_t i = start; i < node.ChildrenSize(); ++i) {
            auto list = CalculateNode(node.ChildPtr(i));
            if (!list) {
                return std::nullopt;
            }
            result.push_back(std::move(*list));
        }
        return result;
    }

    std::optional<TRangeList> CalculateSqlIn(const TSqlInRanges& in) {
        const size_t width = in.Columns.size();
        if (!in.Items || OverBudget(in.Items->size())) {
            return std::nullopt;
        }
        if (in.Limit && in.Items->size() > *in.Limit) {
            return FullRange(width);
        }
        if (in.Items->empty()) {
            return TRangeList{.Width = width};
        }
        TVector<TRangeList> items;
        for (const auto& item : *in.Items) {
            TVector<TRangeList> columns;
            for (const auto& column : in.Columns) {
                const auto value = in.GetValue(item, column);
                auto list = value ? ExpandRangeFor(column.Op, value, *column.Type) : std::nullopt;
                if (!list) {
                    return std::nullopt;
                }
                columns.push_back(std::move(*list));
            }
            items.push_back(Multiply(in.ItemLimit, std::move(columns)));
            if (Failed_) {
                return std::nullopt;
            }
        }
        return Union(std::move(items));
    }

    // RangeFor

    // A boundary element of one column: a value, a finite NULL or an infinity.
    static TBoundaryElement Element(const TKeyValue& value) {
        return {.Value = value};
    }

    static TBoundaryElement Infinity() {
        return {.Infinite = true};
    }

    // Ranges start from the first non-NULL value: the boundary is an empty Optional cast to the nullable key, a finite NULL.
    static TBoundaryElement MinusInf(const TKeyType& type) {
        const bool nullable = type.OptionalDepth > 0 || !type.Slot;
        return nullable ? Element(TKeyValue{}) : Infinity();
    }

    // NaN is ordered after every other value, so a range up to infinity ends before it unless it should include NaN.
    static TBoundaryElement PlusInf(const TKeyType& type, bool excludeNaN = true) {
        return excludeNaN && HasNaN(type) ? Element(MakeNaN(type)) : Infinity();
    }

    static TRangeList Single(const TBoundaryElement& left, bool leftIncluded, const TBoundaryElement& right, bool rightIncluded) {
        return TRangeList{
            .Ranges = {{*MakeBoundary({left}, leftIncluded, /*isLeft=*/true), *MakeBoundary({right}, rightIncluded, /*isLeft=*/false)}},
            .Width = 1,
        };
    }

    static TRangeList Empty() {
        return TRangeList{.Width = 1};
    }

    // ExpandRangeFor: the value is cast to the key type, and a value with no exact counterpart in the key type makes
    // == empty, != everything, and < or > the range of the value rounded to the key type.
    std::optional<TRangeList> ExpandRangeFor(TStringBuf op, const TExprNode::TPtr& valueNode, const TExprNode& typeNode) {
        const auto type = ParseKeyType(typeNode);
        if (!type || (type->Slot && NUdf::GetDataTypeInfo(*type->Slot).Features & NUdf::EDataTypeFeatures::TzDateType)) {
            return std::nullopt;
        }
        if (op == "Exists" || op == "NotExists") {
            // The predicate extractor builds them only for a nullable key.
            const TBoundaryElement null = Element(TKeyValue{});
            return op == "NotExists" ? Single(null, true, null, true) : Single(null, false, PlusInf(*type, /*excludeNaN=*/false), false);
        }

        const auto value = ParseKeyValue(valueNode);
        if (!value) {
            return std::nullopt;
        }
        if (op == "StartsWith" || op == "NotStartsWith") {
            return ExpandStartsWith(op == "StartsWith", *value, *type);
        }

        const auto cast = CastToKey(*value, *type);
        switch (cast.Status) {
            case TConversion::EStatus::Unknown:
                return std::nullopt;
            case TConversion::EStatus::Value:
                return ExpandNormalRange(op, cast.Value, *type);
            case TConversion::EStatus::Null:
                break;
        }
        if (op == "==" || op == "===") {
            return Empty();
        }
        if (op == "!=") {
            return Single(MinusInf(*type), false, PlusInf(*type, /*excludeNaN=*/false), false);
        }
        if (value->Kind == TKeyValue::EKind::Null) {
            return Empty();
        }
        const bool down = op.StartsWith("<");
        const auto rounded = RoundToKey(*value, *type, down);
        switch (rounded.Status) {
            case TConversion::EStatus::Unknown:
                return std::nullopt;
            case TConversion::EStatus::Null:
                return Empty();
            case TConversion::EStatus::Value:
                return ExpandNormalRange(down ? "<=" : ">=", rounded.Value, *type);
        }
        return std::nullopt;
    }

    // StartsWith(p) is [p, NextValue(p)), or [p, +inf) when nothing follows p. NotStartsWith is the rest.
    std::optional<TRangeList> ExpandStartsWith(bool startsWith, const TKeyValue& value, const TKeyType& type) {
        if (!type.Slot || (*type.Slot != EDataSlot::String && *type.Slot != EDataSlot::Utf8)) {
            return std::nullopt;
        }
        const auto cast = CastToKey(value, type);
        if (cast.Status == TConversion::EStatus::Unknown) {
            return std::nullopt;
        }
        if (cast.Status == TConversion::EStatus::Null) {
            return startsWith ? Empty() : Single(MinusInf(type), false, PlusInf(type, /*excludeNaN=*/false), false);
        }
        const auto& prefix = cast.Value;
        const auto next = NextKeyValue(prefix);
        if (startsWith) {
            auto from = ExpandNormalRange(">=", prefix, type);
            if (!next) {
                return from;
            }
            return Intersect({std::move(*from), std::move(*ExpandNormalRange("<", *next, type))});
        }
        auto before = ExpandNormalRange("<", prefix, type);
        if (!next) {
            return before;
        }
        return Union({std::move(*before), std::move(*ExpandNormalRange(">=", *next, type))});
    }

    // BuildNormalRangeLambdaRaw for a key value of the key type.
    std::optional<TRangeList> ExpandNormalRange(TStringBuf op, const TKeyValue& key, const TKeyType& type) {
        const auto value = Element(key);
        if (op == "!=") {
            auto ranges = Single(MinusInf(type), false, value, false);
            ranges.Ranges.push_back(Single(value, false, PlusInf(type, /*excludeNaN=*/false), false).Ranges.front());
            if (!HasNaN(type)) {
                return ranges;
            }
            // NaN is not equal to anything, NaN included.
            const auto nan = Element(MakeNaN(type));
            return Union({std::move(ranges), Single(nan, true, nan, true)});
        }
        if (op == "===") {
            // A wide point [v, v + 1) of an integral or date key, which RangeFinalize turns back into the point v.
            if (key.Kind == TKeyValue::EKind::Data
                && NUdf::GetDataTypeInfo(key.Slot).Features & (NUdf::EDataTypeFeatures::IntegralType | NUdf::EDataTypeFeatures::DateType)) {
                const auto end = WidePointEnd(key);
                return end ? Single(value, true, Element(*end), false) : Single(value, true, value, true);
            }
            op = "==";
        }
        if (op == "==") {
            auto point = Single(value, true, value, true);
            if (!HasNaN(type)) {
                return point;
            }
            // NaN is not equal to itself.
            return Intersect({std::move(point), Single(MinusInf(type), false, PlusInf(type), false)});
        }
        if (op == "<" || op == "<=") {
            return Single(MinusInf(type), false, value, op == "<=");
        }
        if (op == ">" || op == ">=") {
            return Single(value, op == ">=", PlusInf(type), false);
        }
        return std::nullopt;
    }

    // Ordering. Comparing values whose order is unknown fails the whole calculation.

    int CompareValues(const TKeyValue& left, const TKeyValue& right) {
        const auto cmp = CompareKeyValues(left, right);
        return cmp ? *cmp : Fail();
    }

    int CompareComponents(const TComponent& left, const TComponent& right) {
        if (left.Inf != right.Inf) {
            return left.Inf < right.Inf ? -1 : 1;
        }
        return left.Inf == 0 ? CompareValues(left.Value, right.Value) : 0;
    }

    int CompareBoundaries(const TBoundary& left, const TBoundary& right) {
        if (left.Components.size() != right.Components.size()) {
            return Fail();
        }
        for (size_t i = 0; i < left.Components.size(); ++i) {
            if (const int cmp = CompareComponents(left.Components[i], right.Components[i])) {
                return cmp;
            }
        }
        return (left.Included > right.Included) - (left.Included < right.Included);
    }

    int CompareRanges(const TRange& left, const TRange& right) {
        if (const int cmp = CompareBoundaries(left.Left, right.Left)) {
            return cmp;
        }
        return CompareBoundaries(left.Right, right.Right);
    }

    bool RangeLess(const TRange& left, const TRange& right) {
        return CompareRanges(left, right) < 0;
    }

    const TBoundary& MaxBoundary(const TBoundary& a, const TBoundary& b) {
        return CompareBoundaries(a, b) < 0 ? b : a;
    }

    const TBoundary& MinBoundary(const TBoundary& a, const TBoundary& b) {
        return CompareBoundaries(a, b) < 0 ? a : b;
    }

    // Range algebra

    bool RangeIsEmpty(const TRange& range) {
        if (CompareBoundaries(range.Left, range.Right) >= 0) {
            return true;
        }
        for (size_t i = 0; i < range.Left.Components.size(); ++i) {
            if (CompareComponents(range.Left.Components[i], range.Right.Components[i])) {
                return false;
            }
        }
        return range.Left.Included == 0 || range.Right.Included == 0;
    }

    // Assumes a <= b.
    bool RangeCanMerge(const TRange& a, const TRange& b) {
        const auto& lefts = b.Left;
        const auto& rights = a.Right;
        if (CompareBoundaries(lefts, rights) > 0) {
            return false;
        }
        for (size_t i = 0; i < lefts.Components.size(); ++i) {
            if (CompareComponents(lefts.Components[i], rights.Components[i]) < 0) {
                return true;
            }
        }
        return lefts.Included != 0 || rights.Included != 0;
    }

    void Normalize(TRangeList& list) {
        auto& ranges = list.Ranges;
        StableSort(ranges, [&](const TRange& a, const TRange& b) { return RangeLess(a, b); });
        EraseIf(ranges, [&](const TRange& range) { return RangeIsEmpty(range); });
        ranges.erase(std::unique(ranges.begin(), ranges.end(), [&](const TRange& a, const TRange& b) {
            return CompareRanges(a, b) == 0;
        }), ranges.end());
    }

    TRangeList Union(TVector<TRangeList> lists) {
        TRangeList merged{.Width = lists.front().Width};
        for (auto& list : lists) {
            Normalize(list);
            merged.Ranges.insert(merged.Ranges.end(), list.Ranges.begin(), list.Ranges.end());
        }
        StableSort(merged.Ranges, [&](const TRange& a, const TRange& b) { return RangeLess(a, b); });

        TRangeList result{.Width = merged.Width};
        for (auto& range : merged.Ranges) {
            if (!result.Ranges.empty() && RangeCanMerge(result.Ranges.back(), range)) {
                auto& current = result.Ranges.back();
                current.Right = MaxBoundary(current.Right, range.Right);
            } else {
                result.Ranges.push_back(std::move(range));
            }
        }
        return result;
    }

    TRangeList Intersect(TVector<TRangeList> lists) {
        for (auto& list : lists) {
            Normalize(list);
        }
        TDeque<TRange> current(lists.front().Ranges.begin(), lists.front().Ranges.end());
        for (size_t i = 1; i < lists.size(); ++i) {
            TDeque<TRange> next(lists[i].Ranges.begin(), lists[i].Ranges.end());
            TDeque<TRange> result;
            while (!current.empty() && !next.empty()) {
                auto* minInput = &current;
                auto* maxInput = &next;
                if (!RangeLess(current.front(), next.front())) {
                    std::swap(minInput, maxInput);
                }
                const auto& minRange = minInput->front();
                const auto& maxRange = maxInput->front();
                TRange intersected{maxRange.Left, MinBoundary(minRange.Right, maxRange.Right)};
                if (!RangeIsEmpty(intersected)) {
                    result.push_back(std::move(intersected));
                    if (CompareBoundaries(minRange.Right, maxRange.Right) < 0) {
                        minInput->pop_front();
                    } else {
                        maxInput->pop_front();
                    }
                } else {
                    minInput->pop_front();
                }
            }
            current = std::move(result);
        }
        return TRangeList{.Ranges = TVector<TRange>(current.begin(), current.end()), .Width = lists.front().Width};
    }

    TRangeList Multiply(ui64 limit, TVector<TRangeList> lists) {
        size_t width = 0;
        for (auto& list : lists) {
            Normalize(list);
            width += list.Width;
        }
        if (lists.size() == 1 && lists.front().Ranges.size() > limit) {
            return FullRange(width);
        }

        TRangeList current = std::move(lists.front());
        for (size_t i = 1; i < lists.size(); ++i) {
            if (lists[i].Ranges.empty()) {
                return TRangeList{.Width = width};
            }
            const bool withinLimit = DoMultiply(limit, current, lists[i]);
            if (Failed_) {
                return current;
            }
            if (!withinLimit) {
                // Over the limit: the remaining columns are left unconstrained.
                for (auto& range : current.Ranges) {
                    range = AppendInfs(range, width - current.Width);
                }
                current.Width = width;
                break;
            }
        }
        return current;
    }

    bool DoMultiply(ui64 limit, TRangeList& current, const TRangeList& next) {
        TRangeList result{.Width = current.Width + next.Width};
        for (const auto& range : current.Ranges) {
            if (RangeIsPoint(range)) {
                if (result.Ranges.size() + next.Ranges.size() > limit) {
                    return false;
                }
                if (OverBudget(result.Ranges.size() + next.Ranges.size())) {
                    return true;
                }
                for (const auto& suffix : next.Ranges) {
                    result.Ranges.push_back({Append(range.Left, suffix.Left), Append(range.Right, suffix.Right)});
                }
            } else {
                if (result.Ranges.size() + 1 > limit) {
                    return false;
                }
                result.Ranges.push_back(AppendInfs(range, next.Width));
            }
        }
        current = std::move(result);
        return true;
    }

    bool RangeIsPoint(const TRange& range) {
        if (!range.Left.Included || !range.Right.Included) {
            return false;
        }
        for (size_t i = 0; i < range.Left.Components.size(); ++i) {
            if (CompareComponents(range.Left.Components[i], range.Right.Components[i])) {
                return false;
            }
        }
        return true;
    }

    static TBoundary Append(const TBoundary& first, const TBoundary& second) {
        TBoundary result = first;
        result.Components.insert(result.Components.end(), second.Components.begin(), second.Components.end());
        // An appended infinity keeps the inclusion of the prefix.
        if (second.Components.empty() || second.Components.front().Inf == 0) {
            result.Included = second.Included;
        }
        return result;
    }

    static TBoundary AppendInfs(const TBoundary& boundary, bool isLeft, size_t count) {
        TBoundary result = boundary;
        const i32 sign = GetInfSign(boundary.HasPrefix(), boundary.Included != 0, isLeft);
        result.Components.insert(result.Components.end(), count, TComponent{.Inf = sign});
        return result;
    }

    static TRange AppendInfs(const TRange& range, size_t count) {
        return {AppendInfs(range.Left, /*isLeft=*/true, count), AppendInfs(range.Right, /*isLeft=*/false, count)};
    }

    static TRangeList FullRange(size_t width) {
        return TRangeList{.Ranges = {AppendInfs(TRange{}, width)}, .Width = width};
    }

    TRangeList Finalize(TRangeList list) {
        Normalize(list);
        for (auto& range : list.Ranges) {
            if (CanConvertToPointRange(range)) {
                // The result is in the external form, where the infinity signs follow from the inclusion.
                if (range.Left.Included) {
                    range.Right = range.Left;
                    range.Right.Included = 1;
                } else {
                    range.Left = range.Right;
                    range.Left.Included = -1;
                }
                for (auto* boundary : {&range.Left, &range.Right}) {
                    const bool isLeft = boundary == &range.Left;
                    for (auto& component : boundary->Components) {
                        if (component.Inf) {
                            component.Inf = GetInfSign(boundary->HasPrefix(), true, isLeft);
                        }
                    }
                }
            }
        }
        return list;
    }

    // [x, x + 1) and (x - 1, x] of an integral or date column are the point x.
    bool CanConvertToPointRange(const TRange& range) {
        if (!range.Left.Included == !range.Right.Included) {
            return false;
        }
        auto finiteCount = [](const TBoundary& boundary) {
            size_t result = 0;
            while (result < boundary.Components.size() && boundary.Components[result].Inf == 0) {
                ++result;
            }
            return result;
        };
        const size_t count = finiteCount(range.Left);
        if (count == 0 || finiteCount(range.Right) != count) {
            return false;
        }
        for (size_t i = 0; i + 1 < count; ++i) {
            if (CompareValues(range.Left.Components[i].Value, range.Right.Components[i].Value)) {
                return false;
            }
        }
        const auto end = WidePointEnd(range.Left.Components[count - 1].Value);
        return end && CompareValues(*end, range.Right.Components[count - 1].Value) == 0;
    }

    int Fail() {
        Failed_ = true;
        return 0;
    }

    bool OverBudget(size_t rangeCount) {
        if (rangeCount > MaxCalculatedRanges) {
            Fail();
        }
        return Failed_;
    }

    bool Failed_ = false;
};

} // namespace

std::optional<TRangeList> CalculateRanges(const TExprNode::TPtr& ranges) {
    return TRangeCalculator().Calculate(ranges);
}

} // namespace NKikimr::NKqp::NOpt::NReadRanges

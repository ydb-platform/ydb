#include "kqp_read_ranges_explain.h"

#include "kqp_olap_expr_inspection.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/public/udf/udf_data_type.h>

#include <util/charset/utf8.h>
#include <util/generic/algorithm.h>
#include <util/generic/deque.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/escape.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

// The ranges are computed as MiniKQL computes them (yql/essentials/minikql/comp_nodes/mkql_range.cpp
// for the range algebra, yql/essentials/core/yql_opt_range.cpp for RangeFor), over literal bounds.
// When a step depends on a value unknown at compile time, or on an order the literals do not define,
// the whole description is unknown rather than approximate.

using NUdf::EDataSlot;
using NUdf::EDataTypeFeatures;

bool HasFeatures(EDataSlot slot, ui32 features) {
    return NUdf::GetDataTypeInfo(slot).Features & features;
}

// Integral, date and interval literals are decimal integers of at most 64 bits.
struct TInteger {
    bool Negative = false;
    ui64 Magnitude = 0;
};

std::optional<TInteger> ParseInteger(TStringBuf text) {
    TInteger result;
    if (text.SkipPrefix("-")) {
        result.Negative = true;
    }
    if (!TryFromString(text, result.Magnitude)) {
        return std::nullopt;
    }
    result.Negative = result.Negative && result.Magnitude != 0;
    return result;
}

int CompareIntegers(TInteger left, TInteger right) {
    if (left.Negative != right.Negative) {
        return left.Negative ? -1 : 1;
    }
    if (left.Magnitude == right.Magnitude) {
        return 0;
    }
    return (left.Magnitude < right.Magnitude) != left.Negative ? -1 : 1;
}

std::optional<TInteger> NextInteger(TInteger value) {
    if (value.Negative) {
        return TInteger{.Negative = value.Magnitude > 1, .Magnitude = value.Magnitude - 1};
    }
    if (value.Magnitude == Max<ui64>()) {
        return std::nullopt;
    }
    return TInteger{.Magnitude = value.Magnitude + 1};
}

TString FormatInteger(TInteger value) {
    return TStringBuilder() << (value.Negative ? "-" : "") << value.Magnitude;
}

// The value range of an integral type, for casts between integral types.
std::optional<std::pair<TInteger, TInteger>> IntegralBounds(EDataSlot slot) {
    auto signedBounds = [](ui64 max) {
        return std::pair{TInteger{.Negative = true, .Magnitude = max + 1}, TInteger{.Magnitude = max}};
    };
    auto unsignedBounds = [](ui64 max) {
        return std::pair{TInteger{}, TInteger{.Magnitude = max}};
    };
    switch (slot) {
        case EDataSlot::Int8: return signedBounds(Max<i8>());
        case EDataSlot::Uint8: return unsignedBounds(Max<ui8>());
        case EDataSlot::Int16: return signedBounds(Max<i16>());
        case EDataSlot::Uint16: return unsignedBounds(Max<ui16>());
        case EDataSlot::Int32: return signedBounds(Max<i32>());
        case EDataSlot::Uint32: return unsignedBounds(Max<ui32>());
        case EDataSlot::Int64: return signedBounds(Max<i64>());
        case EDataSlot::Uint64: return unsignedBounds(Max<ui64>());
        default: return std::nullopt;
    }
}

// The largest value of a type for which RangeFor '=== builds the wide point [v, v + 1).
std::optional<TInteger> WidePointMax(EDataSlot slot) {
    switch (slot) {
        case EDataSlot::Date: return TInteger{.Magnitude = NUdf::MAX_DATE - 1};
        case EDataSlot::Datetime: return TInteger{.Magnitude = NUdf::MAX_DATETIME - 1};
        case EDataSlot::Timestamp: return TInteger{.Magnitude = NUdf::MAX_TIMESTAMP - 1};
        case EDataSlot::Date32: return TInteger{.Magnitude = NUdf::MAX_DATE32};
        case EDataSlot::Datetime64: return TInteger{.Magnitude = NUdf::MAX_DATETIME64};
        case EDataSlot::Timestamp64: return TInteger{.Magnitude = NUdf::MAX_TIMESTAMP64};
        default: {
            const auto bounds = IntegralBounds(slot);
            return bounds ? std::optional(bounds->second) : std::nullopt;
        }
    }
}

bool IsPgIntegral(TStringBuf pgType) {
    return pgType == "int2" || pgType == "int4" || pgType == "int8";
}

bool IsPgNumeric(TStringBuf pgType) {
    return IsPgIntegral(pgType) || pgType == "float4" || pgType == "float8" || pgType == "numeric";
}

// A finite key value. NULL sorts before every other value, as an empty Optional does in MiniKQL.
struct TKeyValue {
    enum class EKind {
        Null,
        Data,
        Pg,
    };

    EKind Kind = EKind::Null;
    EDataSlot Slot = EDataSlot::Bool;
    TString PgType;
    TString Text;
};

TString QuoteText(TStringBuf text) {
    if (!IsUtf(text)) {
        return TStringBuilder() << '"' << EscapeC(text) << '"';
    }
    // Keep valid UTF-8 readable, escaping only what breaks the quoted form.
    TStringBuilder result;
    result << '"';
    for (const char c : text) {
        if (c == '"' || c == '\\') {
            result << '\\' << c;
        } else if (static_cast<unsigned char>(c) < 0x20 || c == 0x7f) {
            result << EscapeC(TStringBuf(&c, 1));
        } else {
            result << c;
        }
    }
    result << '"';
    return result;
}

TString FormatKeyValue(const TKeyValue& value) {
    if (value.Kind == TKeyValue::EKind::Null) {
        return "NULL";
    }
    const bool quoted = value.Kind == TKeyValue::EKind::Pg
        ? !IsPgNumeric(value.PgType)
        : HasFeatures(value.Slot, EDataTypeFeatures::StringType);
    return quoted ? QuoteText(value.Text) : value.Text;
}

// A type expression, as RangeFor and casts name it.
struct TKeyType {
    std::optional<EDataSlot> Slot;
    TString PgType;
    ui32 OptionalDepth = 0;
};

std::optional<TKeyType> ParseKeyType(const TExprNode& node) {
    if (node.IsCallable("OptionalType") && node.ChildrenSize() == 1) {
        auto result = ParseKeyType(node.Head());
        if (result) {
            ++result->OptionalDepth;
        }
        return result;
    }
    if (node.IsCallable("DataType") && node.ChildrenSize() == 1 && node.Head().IsAtom()) {
        if (const auto slot = NUdf::FindDataSlot(node.Head().Content())) {
            return TKeyType{.Slot = *slot};
        }
    }
    if (node.IsCallable("PgType") && node.ChildrenSize() == 1 && node.Head().IsAtom()) {
        return TKeyType{.PgType = TString(node.Head().Content())};
    }
    return std::nullopt;
}

// Casts a non-NULL value to a key type. nullopt if the cast result is not known.
std::optional<TKeyValue> CastKeyValue(const TKeyValue& value, const TKeyType& type) {
    if (value.Kind == TKeyValue::EKind::Null) {
        return value;
    }
    if (!type.Slot) {
        if (value.Kind == TKeyValue::EKind::Pg && value.PgType == type.PgType) {
            return value;
        }
        return std::nullopt;
    }
    if (value.Kind != TKeyValue::EKind::Data) {
        return std::nullopt;
    }

    const auto target = *type.Slot;
    auto result = value;
    result.Slot = target;
    if (value.Slot == target) {
        return result;
    }
    if (value.Slot == EDataSlot::Utf8 && target == EDataSlot::String) {
        return result;
    }
    if (value.Slot == EDataSlot::String && target == EDataSlot::Utf8 && IsUtf(value.Text)) {
        return result;
    }

    const auto bounds = IntegralBounds(target);
    const auto integer = ParseInteger(value.Text);
    if (bounds && integer && HasFeatures(value.Slot, EDataTypeFeatures::IntegralType)
        && CompareIntegers(bounds->first, *integer) <= 0 && CompareIntegers(*integer, bounds->second) <= 0) {
        return result;
    }
    return std::nullopt;
}

// A literal key value, possibly NULL. nullopt if the value is not a literal.
std::optional<TKeyValue> ParseKeyValue(const TExprNode::TPtr& node) {
    if (node->IsCallable("Nothing")) {
        return TKeyValue{};
    }
    if (node->IsCallable("Just") && node->ChildrenSize() == 1) {
        return ParseKeyValue(node->HeadPtr());
    }
    if (node->IsCallable({"SafeCast", "StrictCast"}) && node->ChildrenSize() == 2) {
        auto value = ParseKeyValue(node->HeadPtr());
        const auto type = ParseKeyType(node->Tail());
        return value && type ? CastKeyValue(*value, *type) : std::nullopt;
    }
    if (node->IsCallable("ToString") && node->ChildrenSize() == 1) {
        auto value = ParseKeyValue(node->HeadPtr());
        if (value && value->Kind == TKeyValue::EKind::Data && value->Slot == EDataSlot::Utf8) {
            value->Slot = EDataSlot::String;
            return value;
        }
        return std::nullopt;
    }
    if (const auto data = TMaybeNode<TCoDataCtor>(node)) {
        const auto slot = NUdf::FindDataSlot(node->Content());
        if (!slot) {
            return std::nullopt;
        }
        return TKeyValue{.Kind = TKeyValue::EKind::Data, .Slot = *slot, .Text = TString(data.Cast().Literal().Value())};
    }
    if (const auto pg = TMaybeNode<TCoPgConst>(node)) {
        const auto type = pg.Cast().Type().Ptr();
        if (!type->IsCallable("PgType") || type->ChildrenSize() != 1) {
            return std::nullopt;
        }
        return TKeyValue{.Kind = TKeyValue::EKind::Pg, .PgType = TString(type->Head().Content()),
                         .Text = TString(pg.Cast().Value().Value())};
    }
    return std::nullopt;
}

// A boundary element in the AsRange form: an empty outer Optional is an infinity, anything else is
// a key value. Only Just of an empty Optional is a finite NULL.
struct TBoundaryElement {
    bool Infinite = false;
    TKeyValue Value;
};

std::optional<TBoundaryElement> ParseBoundaryElement(const TExprNode::TPtr& node) {
    if (node->IsCallable("Nothing") && node->ChildrenSize() == 1) {
        if (node->Head().IsCallable("PgType")) {
            return TBoundaryElement{};
        }
        return TBoundaryElement{.Infinite = true};
    }
    if (node->IsCallable({"SafeCast", "StrictCast"}) && node->ChildrenSize() == 2 && node->Head().IsCallable("Nothing")) {
        // Casting an empty Optional to a nested Optional gives Just(NULL).
        const auto type = ParseKeyType(node->Tail());
        if (!type || type->OptionalDepth == 0) {
            return std::nullopt;
        }
        return TBoundaryElement{.Infinite = type->OptionalDepth == 1};
    }
    auto value = ParseKeyValue(node);
    if (!value) {
        return std::nullopt;
    }
    return TBoundaryElement{.Value = std::move(*value)};
}

i32 GetInfSign(bool hasPrefix, bool isIncluded, bool isLeft) {
    if (!hasPrefix || isIncluded) {
        return isLeft ? -1 : 1;
    }
    return isLeft ? 1 : -1;
}

// A range boundary in the MiniKQL internal form: for each column an infinity marker
// (-1 = -inf, +1 = +inf, 0 = Value), then the inclusion (-1 = '[', +1 = ']', 0 = excluded).
struct TComponent {
    i32 Inf = 0;
    TKeyValue Value;
};

struct TBoundary {
    TVector<TComponent> Components;
    i32 Included = 0;

    bool HasPrefix() const {
        return !Components.empty() && Components.front().Inf == 0;
    }
};

struct TRange {
    TBoundary Left;
    TBoundary Right;
};

struct TRangeList {
    TVector<TRange> Ranges;
    size_t Width = 0;
};

std::optional<TBoundary> ConvertToInternal(const TVector<TBoundaryElement>& elements, bool included, bool isLeft) {
    TBoundary result;
    const bool hasPrefix = !elements.empty() && !elements.front().Infinite;
    bool tail = false;
    for (const auto& element : elements) {
        tail = tail || element.Infinite;
        if (!element.Infinite && tail) {
            return std::nullopt;
        }
        result.Components.push_back({
            .Inf = element.Infinite ? GetInfSign(hasPrefix, included, isLeft) : 0,
            .Value = element.Value,
        });
    }
    result.Included = included ? (isLeft ? -1 : 1) : 0;
    return result;
}

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
        if (node->IsCallable({"RangeFinalize", "RangeToPg"}) && node->ChildrenSize() == 1) {
            auto list = CalculateNode(node->HeadPtr());
            if (!list) {
                return std::nullopt;
            }
            return node->IsCallable("RangeFinalize") ? Finalize(std::move(*list)) : list;
        }
        if (node->IsCallable("RangeEmpty")) {
            return ParseRangeEmpty(*node);
        }
        if (node->IsCallable("AsRange")) {
            return ParseAsRange(*node);
        }
        if (node->IsCallable("RangeFor") && node->ChildrenSize() == 3) {
            return ParseRangeFor(*node);
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

    // Parsing

    static std::optional<TRangeList> ParseRangeEmpty(const TExprNode& node) {
        if (node.ChildrenSize() == 0) {
            return TRangeList{};
        }
        if (node.Head().IsCallable("TupleType")) {
            return TRangeList{.Width = node.Head().ChildrenSize()};
        }
        return TRangeList{.Width = 1};
    }

    static std::optional<TRangeList> ParseAsRange(const TExprNode& node) {
        TRangeList result;
        for (const auto& range : node.Children()) {
            if (!range->IsList() || range->ChildrenSize() != 2) {
                return std::nullopt;
            }
            auto left = ParseBoundary(range->Head(), /*isLeft=*/true);
            auto right = ParseBoundary(range->Tail(), /*isLeft=*/false);
            if (!left || !right || left->Components.size() != right->Components.size()) {
                return std::nullopt;
            }
            if (!result.Ranges.empty() && left->Components.size() != result.Width) {
                return std::nullopt;
            }
            result.Width = left->Components.size();
            result.Ranges.push_back({std::move(*left), std::move(*right)});
        }
        return result;
    }

    static std::optional<TBoundary> ParseBoundary(const TExprNode& node, bool isLeft) {
        if (!node.IsList() || node.ChildrenSize() < 2 || !node.Tail().IsCallable("Int32")) {
            return std::nullopt;
        }
        i32 included;
        if (!TryFromString(node.Tail().Head().Content(), included)) {
            return std::nullopt;
        }
        TVector<TBoundaryElement> elements;
        for (size_t i = 0; i + 1 < node.ChildrenSize(); ++i) {
            auto element = ParseBoundaryElement(node.ChildPtr(i));
            if (!element) {
                return std::nullopt;
            }
            elements.push_back(std::move(*element));
        }
        return ConvertToInternal(elements, included != 0, isLeft);
    }

    // Mirrors ExpandRangeFor and BuildNormalRangeLambdaRaw for the key types whose ranges do not depend on NaNs,
    // timezones or rounding.
    static std::optional<TRangeList> ParseRangeFor(const TExprNode& node) {
        const auto op = node.Head().Content();
        const auto keyType = ParseKeyType(node.Tail());
        if (!keyType || op == "StartsWith" || op == "NotStartsWith") {
            return std::nullopt;
        }
        if (keyType->Slot && HasFeatures(*keyType->Slot, EDataTypeFeatures::FloatType | EDataTypeFeatures::DecimalType
                                                        | EDataTypeFeatures::TzDateType)) {
            return std::nullopt;
        }

        const bool nullableKey = keyType->OptionalDepth > 0 || !keyType->Slot;
        const TBoundaryElement infinity{.Infinite = true};
        const TBoundaryElement null{};
        // Ranges start from the first non-NULL value; an empty Optional cast to a nullable key is Just(NULL).
        const auto minusInf = nullableKey ? null : infinity;

        TRangeList result{.Width = 1};
        auto add = [&](const TBoundaryElement& left, bool leftIncluded, const TBoundaryElement& right, bool rightIncluded) {
            result.Ranges.push_back({
                *ConvertToInternal({left}, leftIncluded, /*isLeft=*/true),
                *ConvertToInternal({right}, rightIncluded, /*isLeft=*/false),
            });
        };

        if (op == "Exists" || op == "NotExists") {
            if (!nullableKey) {
                return std::nullopt;
            }
            if (op == "NotExists") {
                add(null, true, null, true);
            } else {
                add(null, false, infinity, false);
            }
            return result;
        }

        const auto parsed = ParseKeyValue(node.ChildPtr(1));
        const auto value = parsed ? CastKeyValue(*parsed, *keyType) : std::nullopt;
        if (!value) {
            return std::nullopt;
        }
        if (value->Kind == TKeyValue::EKind::Null) {
            if (op == "!=") {
                add(minusInf, false, infinity, false);
            }
            return result;
        }

        const TBoundaryElement key{.Value = *value};
        if (op == "===" && keyType->Slot && HasFeatures(*keyType->Slot, EDataTypeFeatures::IntegralType | EDataTypeFeatures::DateType)) {
            // A wide point [v, v + 1), which RangeFinalize turns back into a point.
            const auto integer = ParseInteger(value->Text);
            const auto max = WidePointMax(*keyType->Slot);
            if (!integer || !max) {
                return std::nullopt;
            }
            if (CompareIntegers(*integer, *max) == 0) {
                add(key, true, key, true);
            } else {
                auto next = key;
                next.Value.Text = FormatInteger(*NextInteger(*integer));
                add(key, true, next, false);
            }
        } else if (op == "==" || op == "===") {
            add(key, true, key, true);
        } else if (op == "!=") {
            add(minusInf, false, key, false);
            add(key, false, infinity, false);
        } else if (op == "<" || op == "<=") {
            add(minusInf, false, key, op == "<=");
        } else if (op == ">" || op == ">=") {
            add(key, op == ">=", infinity, false);
        } else {
            return std::nullopt;
        }
        return result;
    }

    // Ordering. Comparing values whose order is unknown fails the whole calculation.

    int CompareValues(const TKeyValue& left, const TKeyValue& right) {
        using EKind = TKeyValue::EKind;
        if (left.Kind == EKind::Null || right.Kind == EKind::Null) {
            return int(right.Kind == EKind::Null) - int(left.Kind == EKind::Null);
        }
        if (left.Kind != right.Kind) {
            return Fail();
        }

        bool integral;
        if (left.Kind == EKind::Pg) {
            if (left.PgType != right.PgType) {
                return Fail();
            }
            integral = IsPgIntegral(left.PgType);
        } else {
            const ui32 numbers = EDataTypeFeatures::IntegralType | EDataTypeFeatures::DateType | EDataTypeFeatures::TimeIntervalType;
            const bool bothIntegral = HasFeatures(left.Slot, EDataTypeFeatures::IntegralType)
                && HasFeatures(right.Slot, EDataTypeFeatures::IntegralType);
            if (!bothIntegral && left.Slot != right.Slot) {
                const bool strings = HasFeatures(left.Slot, EDataTypeFeatures::StringType)
                    && HasFeatures(right.Slot, EDataTypeFeatures::StringType);
                if (!strings) {
                    return Fail();
                }
            }
            if (HasFeatures(left.Slot, EDataTypeFeatures::StringType)) {
                const int cmp = left.Text.compare(right.Text);
                return (cmp > 0) - (cmp < 0);
            }
            if (left.Slot == EDataSlot::Bool) {
                bool l, r;
                if (!TryFromString(left.Text, l) || !TryFromString(right.Text, r)) {
                    return Fail();
                }
                return int(l) - int(r);
            }
            integral = HasFeatures(left.Slot, numbers);
        }

        if (integral) {
            const auto l = ParseInteger(left.Text);
            const auto r = ParseInteger(right.Text);
            return l && r ? CompareIntegers(*l, *r) : Fail();
        }
        return left.Text == right.Text ? 0 : Fail();
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

    int Fail() {
        Failed_ = true;
        return 0;
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
            if (!DoMultiply(limit, current, lists[i])) {
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

        const auto& left = range.Left.Components[count - 1].Value;
        const auto& right = range.Right.Components[count - 1].Value;
        if (left.Kind != TKeyValue::EKind::Data || right.Kind != TKeyValue::EKind::Data
            || !HasFeatures(left.Slot, EDataTypeFeatures::IntegralType | EDataTypeFeatures::DateType)) {
            return false;
        }
        for (size_t i = 0; i + 1 < count; ++i) {
            if (CompareValues(range.Left.Components[i].Value, range.Right.Components[i].Value)) {
                return false;
            }
        }

        const auto l = ParseInteger(left.Text);
        const auto r = ParseInteger(right.Text);
        if (!l || !r) {
            return Fail();
        }
        const auto next = NextInteger(*l);
        return next && CompareIntegers(*next, *r) == 0;
    }

    bool Failed_ = false;
};

TString FormatBoundary(const TBoundary& boundary, bool isLeft, const TVector<TString>& keyColumns) {
    // Columns past the boundary are unconstrained, with the same sign as an infinity in the boundary.
    const i32 tailSign = GetInfSign(boundary.HasPrefix(), boundary.Included != 0, isLeft);
    TStringBuilder result;
    result << '{';
    for (size_t i = 0; i < keyColumns.size(); ++i) {
        if (i) {
            result << ", ";
        }
        result << GetOlapColumnName(keyColumns[i], true) << ": ";
        const i32 inf = i < boundary.Components.size() ? boundary.Components[i].Inf : tailSign;
        if (inf == 0) {
            result << FormatKeyValue(boundary.Components[i].Value);
        } else {
            result << (inf < 0 ? "-∞" : "+∞");
        }
    }
    result << '}';
    return result;
}

} // namespace

std::optional<TVector<TString>> BuildReadRangeDescriptions(const TExprNode::TPtr& ranges, const TVector<TString>& keyColumns) {
    if (!ranges || keyColumns.empty()) {
        return std::nullopt;
    }
    const auto list = TRangeCalculator().Calculate(ranges);
    if (!list || list->Width > keyColumns.size()) {
        return std::nullopt;
    }

    TVector<TString> result;
    for (const auto& range : list->Ranges) {
        result.push_back(TStringBuilder()
            << (range.Left.Included ? '[' : '(') << FormatBoundary(range.Left, true, keyColumns) << ", "
            << FormatBoundary(range.Right, false, keyColumns) << (range.Right.Included ? ']' : ')'));
    }
    return result;
}

} // namespace NKikimr::NKqp::NOpt

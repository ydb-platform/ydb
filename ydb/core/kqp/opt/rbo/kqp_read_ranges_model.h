#pragma once

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <util/generic/array_ref.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>

// Parsing and formatting of literal key ranges. All range arithmetic is performed by MiniKQL.
namespace NKikimr::NKqp::NOpt::NReadRanges {

// A finite key value. NULL is ordered before every other value, as an empty Optional is in MiniKQL.
struct TKeyValue {
    enum class EKind {
        Null,
        Data,
        Pg,
        Parameter, // a value only described by name, never computed
    };

    EKind Kind = EKind::Null;
    NYql::NUdf::EDataSlot Slot = NYql::NUdf::EDataSlot::Bool;
    ui8 Precision = 0;
    ui8 Scale = 0;
    TString PgType;
    TString Bytes;                      // String, Utf8 and Uuid data, Pg text, a parameter name
    NYql::NUdf::TUnboxedValuePod Value; // every other data type
};

// A literal key value, possibly NULL. Does not evaluate casts or other expressions.
std::optional<TKeyValue> ParseKeyValue(const NYql::TExprNode::TPtr& node);

TString FormatKeyValue(const TKeyValue& value);

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

// The infinity a boundary continues with past its columns.
i32 GetInfSign(bool hasPrefix, bool isIncluded, bool isLeft);

// A boundary element: an empty outer Optional is an infinity, anything else is a key value.
struct TBoundaryElement {
    bool Infinite = false;
    TKeyValue Value;
};

std::optional<TBoundary> MakeBoundary(const TVector<TBoundaryElement>& elements, bool included, bool isLeft);

// AsRange and RangeEmpty, the literal forms of ranges.
std::optional<TRangeList> ParseAsRange(const NYql::TExprNode& node);
TRangeList ParseRangeEmpty(const NYql::TExprNode& node);

// One interval over all key columns, for example "[{a: 1, b: 10}, {a: 1, b: 20})".
TString FormatRange(const TRange& range, TConstArrayRef<TString> keyColumns);

} // namespace NKikimr::NKqp::NOpt::NReadRanges

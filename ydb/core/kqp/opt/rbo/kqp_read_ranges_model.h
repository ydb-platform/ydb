#pragma once

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <util/generic/array_ref.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>

// The values and ranges explain describes the key ranges of a read with. Values of a data type keep their runtime
// representation and are ordered by the runtime comparator, so explain orders them exactly as MiniKQL does.
namespace NKikimr::NKqp::NOpt::NReadRanges {

// The type of a key column, as RangeFor and casts name it.
struct TKeyType {
    std::optional<NYql::NUdf::EDataSlot> Slot; // a data type, or else a Pg type
    ui8 Precision = 0;                         // of a Decimal
    ui8 Scale = 0;
    TString PgType;
    ui32 OptionalDepth = 0;
};

std::optional<TKeyType> ParseKeyType(const NYql::TExprNode& node);

// Float and Decimal keys have NaN, which compares unequal to everything and is ordered after every other value.
bool HasNaN(const TKeyType& type);

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

// A literal key value, possibly NULL or cast. nullopt if the value is not a literal or its cast is not known.
std::optional<TKeyValue> ParseKeyValue(const NYql::TExprNode::TPtr& node);

TKeyValue MakeNaN(const TKeyType& type);

// The result of converting a value to a key type: the value, NULL, or unknown when explain does not know the result.
struct TConversion {
    enum class EStatus {
        Value,
        Null,
        Unknown,
    };

    EStatus Status = EStatus::Unknown;
    TKeyValue Value;
};

// StrictCast of a value to the key type, which is NULL when the value has no exact counterpart.
TConversion CastToKey(const TKeyValue& value, const TKeyType& type);

// RoundDown or RoundUp of a value to the key type, for a value StrictCast makes NULL.
TConversion RoundToKey(const TKeyValue& value, const TKeyType& type, bool down);

// The end of the wide point [value, value + 1) that RangeFor '=== builds for an integral or date value. nullopt for
// the largest value, whose wide point is the point itself, or for a value of another type.
std::optional<TKeyValue> WidePointEnd(const TKeyValue& value);

// NextValue of a String or Utf8 value: the smallest value that does not start with it. nullopt if there is none.
std::optional<TKeyValue> NextKeyValue(const TKeyValue& value);

// nullopt if the order of the values is not known.
std::optional<int> CompareKeyValues(const TKeyValue& left, const TKeyValue& right);

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

#include "position_independent_value.h"

#include "position_independent_value_transfer.h"

#ifndef YT_COMPILING_UDF

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/yt/string/format.h>

#endif

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

void TPIValue::SetStringPosition(const char* string)
{
    NQueryClient::SetStringPosition(this, string);
}

TStringBuf TPIValue::AsStringBuf() const
{
    return TStringBuf(GetStringPosition(*this), Length);
}

TFingerprint GetFarmFingerprint(const TPIValue& value)
{
    TUnversionedValue asUnversioned{};
    MakeUnversionedFromPositionIndependent(&asUnversioned, value);
    return GetFarmFingerprint(asUnversioned);
}

TFingerprint GetFarmFingerprint(const TPIValue* begin, const TPIValue* end)
{
    auto asUnversionedRange = BorrowFromPI(TPIValueRange(begin, static_cast<size_t>(end - begin)));
    return GetFarmFingerprint(NTableClient::TUnversionedValueRange(
        asUnversionedRange.Begin(),
        asUnversionedRange.Size()));
}

////////////////////////////////////////////////////////////////////////////////

void PrintTo(const TPIValue& value, ::std::ostream* os)
{
    TUnversionedValue asUnversioned{};
    MakeUnversionedFromPositionIndependent(&asUnversioned, value);
    *os << ToString(asUnversioned);
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TPIValue& value, bool valueOnly)
{
    TUnversionedValue asUnversioned{};
    MakeUnversionedFromPositionIndependent(&asUnversioned, value);
    return ToString(asUnversioned, valueOnly);
}

////////////////////////////////////////////////////////////////////////////////

static_assert(sizeof(TUnversionedValue) == sizeof(TPIValue), "Structs must have equal size");

////////////////////////////////////////////////////////////////////////////////

void MakePositionIndependentSentinelValue(TPIValue* result, EValueType type, int id, EValueFlags flags)
{
    auto asUnversioned = MakeUnversionedSentinelValue(type, id, flags);
    MakePositionIndependentFromUnversioned(result, asUnversioned);
}

void MakePositionIndependentNullValue(TPIValue* result, int id, EValueFlags flags)
{
    auto asUnversioned = MakeUnversionedNullValue(id, flags);
    MakePositionIndependentFromUnversioned(result, asUnversioned);
}

void MakePositionIndependentInt64Value(TPIValue* result, i64 value, int id, EValueFlags flags)
{
    auto asUnversioned = MakeUnversionedInt64Value(value, id, flags);
    MakePositionIndependentFromUnversioned(result, asUnversioned);
}

void MakePositionIndependentUint64Value(TPIValue* result, ui64 value, int id, EValueFlags flags)
{
    auto asUnversioned = MakeUnversionedUint64Value(value, id, flags);
    MakePositionIndependentFromUnversioned(result, asUnversioned);
}

void MakePositionIndependentDoubleValue(TPIValue* result, double value, int id, EValueFlags flags)
{
    auto asUnversioned = MakeUnversionedDoubleValue(value, id, flags);
    MakePositionIndependentFromUnversioned(result, asUnversioned);
}

void MakePositionIndependentBooleanValue(TPIValue* result, bool value, int id, EValueFlags flags)
{
    auto asUnversioned = MakeUnversionedBooleanValue(value, id, flags);
    MakePositionIndependentFromUnversioned(result, asUnversioned);
}

void MakePositionIndependentStringLikeValue(TPIValue* result, EValueType valueType, TStringBuf value, int id, EValueFlags flags)
{
    auto asUnversioned = MakeUnversionedStringLikeValue(valueType, value, id, flags);
    MakePositionIndependentFromUnversioned(result, asUnversioned);
}

void MakePositionIndependentStringValue(TPIValue* result, TStringBuf value, int id, EValueFlags flags)
{
    auto asUnversioned = MakeUnversionedStringValue(value, id, flags);
    MakePositionIndependentFromUnversioned(result, asUnversioned);
}

void MakePositionIndependentAnyValue(TPIValue* result, TStringBuf value, int id, EValueFlags flags)
{
    auto asUnversioned = MakeUnversionedAnyValue(value, id, flags);
    MakePositionIndependentFromUnversioned(result, asUnversioned);
}

void MakePositionIndependentCompositeValue(TPIValue* result, TStringBuf value, int id, EValueFlags flags)
{
    auto asUnversioned = MakeUnversionedCompositeValue(value, id, flags);
    MakePositionIndependentFromUnversioned(result, asUnversioned);
}

void MakePositionIndependentValueHeader(TPIValue* result, EValueType type, int id, EValueFlags flags)
{
    auto asUnversioned = MakeUnversionedValueHeader(type, id, flags);
    MakePositionIndependentFromUnversioned(result, asUnversioned);
}

////////////////////////////////////////////////////////////////////////////////

int CompareRowValues(const TPIValue& lhs, const TPIValue& rhs)
{
    TUnversionedValue lhsAsUnversioned{};
    MakeUnversionedFromPositionIndependent(&lhsAsUnversioned, lhs);

    TUnversionedValue rhsAsUnversioned{};
    MakeUnversionedFromPositionIndependent(&rhsAsUnversioned, rhs);

    return CompareRowValues(lhsAsUnversioned, rhsAsUnversioned);
}

int CompareRows(const TPIValue* lhsBegin, const TPIValue* lhsEnd, const TPIValue* rhsBegin, const TPIValue* rhsEnd)
{
    auto* lhsCurrent = lhsBegin;
    auto* rhsCurrent = rhsBegin;
    while (lhsCurrent != lhsEnd && rhsCurrent != rhsEnd) {
        int result = CompareRowValues(*lhsCurrent++, *rhsCurrent++);
        if (result != 0) {
            return result;
        }
    }
    return static_cast<int>(lhsEnd - lhsBegin) - static_cast<int>(rhsEnd - rhsBegin);
}

////////////////////////////////////////////////////////////////////////////////

void ToAny(TPIValue* result, TPIValue* value, TRowBuffer* rowBuffer)
{
    auto unversionedResult = BorrowFromPI(result);
    auto unversionedValue = BorrowFromPI(value);
    *unversionedResult.GetValue() = EncodeUnversionedAnyValue(
        *unversionedValue.GetValue(),
        rowBuffer->GetPool());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

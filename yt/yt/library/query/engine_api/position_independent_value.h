#pragma once

#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/unversioned_value.h>

#include <library/cpp/yt/farmhash/farm_hash.h>
#include <library/cpp/yt/yson_string/public.h>

#include <util/system/defaults.h>

namespace NYT::NQueryClient {

using namespace NYT::NTableClient;

////////////////////////////////////////////////////////////////////////////////

union TPositionIndependentValueData
{
    //! |Int64| value.
    i64 Int64;
    //! |Uint64| value.
    ui64 Uint64;
    //! |Double| value.
    double Double;
    //! |Boolean| value.
    bool Boolean;
    //! Offset for |String| type or YSON-encoded value for |Any| type.
    //! NB: string is not zero-terminated, so never use it as a TString.
    //! Use #TPositionIndependentValue::AsStringBuf() instead.
    ptrdiff_t StringOffset;
};

static_assert(
    sizeof(TPositionIndependentValueData) == 8,
    "TPositionIndependentValueData has to be exactly 8 bytes.");

struct TPositionIndependentValue
    : public TNonCopyable
{
    //! Column id w.r.t. the name table.
    ui16 Id;

    //! Column type.
    EValueType Type;

    //! Various bit-packed flags.
    EValueFlags Flags;

    //! Length of a variable-sized value (only meaningful for string-like types).
    ui32 Length;

    //! Payload.
    TPositionIndependentValueData Data;

    //! Assuming #IsStringLikeType(Type), return string data as a TStringBuf.
    TStringBuf AsStringBuf() const;

    TPositionIndependentValue() = default;

    void SetStringPosition(const char* string);
};

static_assert(
    sizeof(TPositionIndependentValue) == 16,
    "TPositionIndependentValue has to be exactly 16 bytes.");
static_assert(
    std::is_trivial_v<TPositionIndependentValue>,
    "TPositionIndependentValue must be a POD type.");

using TPIValue = TPositionIndependentValue;
using TPIValueData = TPositionIndependentValueData;
using TPIValueRange = TRange<TPIValue>;
using TMutablePIValueRange = TMutableRange<TPIValue>;
using TPIRowRange = std::pair<TPIValueRange, TPIValueRange>;

////////////////////////////////////////////////////////////////////////////////

//! Computes FarmHash forever-fixed fingerprint for a given TPIValue.
TFingerprint GetFarmFingerprint(const TPIValue& value);

//! Computes FarmHash forever-fixed fingerprint for a given set of values.
TFingerprint GetFarmFingerprint(const TPIValue* begin, const TPIValue* end);

////////////////////////////////////////////////////////////////////////////////

//! Debug printer for Gtest unittests.
void PrintTo(const TPIValue& value, ::std::ostream* os);

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TPIValue& value, TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

void MakeUnversionedFromPositionIndependent(TUnversionedValue* destination, const TPIValue& source);

void MakePositionIndependentFromUnversioned(TPIValue* destination, const TUnversionedValue& source);

void CopyPositionIndependent(TPIValue* destination, const TPIValue& source);

////////////////////////////////////////////////////////////////////////////////

void MakePositionIndependentSentinelValue(TPIValue* result, EValueType type, int id = 0, EValueFlags flags = EValueFlags::None);

void MakePositionIndependentNullValue(TPIValue* result, int id = 0, EValueFlags flags = EValueFlags::None);

void MakePositionIndependentInt64Value(TPIValue* result, i64 value, int id = 0, EValueFlags flags = EValueFlags::None);

void MakePositionIndependentUint64Value(TPIValue* result, ui64 value, int id = 0, EValueFlags flags = EValueFlags::None);

void MakePositionIndependentDoubleValue(TPIValue* result, double value, int id = 0, EValueFlags flags = EValueFlags::None);

void MakePositionIndependentBooleanValue(TPIValue* result, bool value, int id = 0, EValueFlags flags = EValueFlags::None);

void MakePositionIndependentStringLikeValue(TPIValue* result, EValueType valueType, TStringBuf value, int id = 0, EValueFlags flags = EValueFlags::None);

void MakePositionIndependentStringValue(TPIValue* result, TStringBuf value, int id = 0, EValueFlags flags = EValueFlags::None);

void MakePositionIndependentAnyValue(TPIValue* result, TStringBuf value, int id = 0, EValueFlags flags = EValueFlags::None);

void MakePositionIndependentCompositeValue(TPIValue* result, TStringBuf value, int id = 0, EValueFlags flags = EValueFlags::None);

void MakePositionIndependentValueHeader(TPIValue* result, EValueType type, int id = 0, EValueFlags flags = EValueFlags::None);

////////////////////////////////////////////////////////////////////////////////

int CompareRowValues(const TPIValue& lhs, const TPIValue& rhs);

int CompareRows(const TPIValue* lhsBegin, const TPIValue* lhsEnd, const TPIValue* rhsBegin, const TPIValue* rhsEnd);

int CompareRowValues(const TPIValue& lhs, const TPIValue& rhs);

////////////////////////////////////////////////////////////////////////////////

void ToAny(TPIValue* result, TPIValue* value, TRowBuffer* rowBuffer);

////////////////////////////////////////////////////////////////////////////////

template <class T>
T FromPositionIndependentValue(const TPIValue& positionIndependentValue);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

#define POSITION_INDEPENDENT_VALUE_INL_H
#include "position_independent_value-inl.h"
#undef POSITION_INDEPENDENT_VALUE_INL_H

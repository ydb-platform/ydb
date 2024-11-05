#pragma once

#include "public.h"
#include "logical_type.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 DateUpperBound = 49'673ull;
constexpr ui64 DatetimeUpperBound = DateUpperBound * 86'400ull;
constexpr ui64 TimestampUpperBound = DatetimeUpperBound * 1'000'000ull;

constexpr i64 Date32UpperBound = 53'375'808ll;
constexpr i64 Date32LowerBound = -Date32UpperBound - 1;
constexpr i64 Datetime64UpperBound = Date32UpperBound * 86'400ll;
constexpr i64 Datetime64LowerBound = Date32LowerBound * 86'400ll;
constexpr i64 Timestamp64UpperBound = Datetime64UpperBound * 1'000'000ll;
constexpr i64 Timestamp64LowerBound = Datetime64LowerBound * 1'000'000ll;
constexpr i64 Interval64UpperBound = Timestamp64UpperBound - Timestamp64LowerBound + 1;

////////////////////////////////////////////////////////////////////////////////

template <ESimpleLogicalValueType type>
Y_FORCE_INLINE void ValidateSimpleLogicalType(i64 value);

template <ESimpleLogicalValueType type>
Y_FORCE_INLINE void ValidateSimpleLogicalType(ui64 value);

template <ESimpleLogicalValueType type>
Y_FORCE_INLINE void ValidateSimpleLogicalType(double value);

template <ESimpleLogicalValueType type>
Y_FORCE_INLINE void ValidateSimpleLogicalType(bool value);

template <ESimpleLogicalValueType type>
Y_FORCE_INLINE void ValidateSimpleLogicalType(TStringBuf value);

// Validates complex logical type yson representation.
void ValidateComplexLogicalType(TStringBuf ysonData, const TLogicalTypePtr& type);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

#define VALIDATE_LOGICAL_TYPE_INL_H_
#include "validate_logical_type-inl.h"
#undef VALIDATE_LOGICAL_TYPE_INL_H_

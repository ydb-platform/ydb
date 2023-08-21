#pragma once

#include "public.h"
#include "logical_type.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 DateUpperBound = 49673ull;
constexpr ui64 DatetimeUpperBound = DateUpperBound * 86400ull;
constexpr ui64 TimestampUpperBound = DatetimeUpperBound * 1000000ull;

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

#ifndef VALIDATE_LOGICAL_TYPE_INL_H_
#error "Direct inclusion of this file is not allowed, include validate_logical_type.h"
// For the sake of sane code completion.
#include "validate_logical_type.h"
#endif

#include "logical_type.h"

#include <yt/yt/core/misc/error.h>

#include <util/charset/utf8.h>

#include <cmath>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <typename TNumber>
static Y_FORCE_INLINE void ValidateNumericRange(TNumber value, TNumber min, TNumber max)
{
    static_assert(std::is_same_v<TNumber, i64> || std::is_same_v<TNumber, ui64> || std::is_same_v<TNumber, double>);
    if (value < min || value > max) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::SchemaViolation,
            "Value %v is out of allowed range [%v, %v]",
            value,
            min,
            max);
    }
}

template <ESimpleLogicalValueType type>
static constexpr auto GetLogicalTypeMax()
{
    // Ints
    if constexpr (type == ESimpleLogicalValueType::Int8) {
        return static_cast<i64>(Max<i8>());
    } else if constexpr (type == ESimpleLogicalValueType::Int16) {
        return static_cast<i64>(Max<i16>());
    } else if constexpr (type == ESimpleLogicalValueType::Int32) {
        return static_cast<i64>(Max<i32>());
    } else if constexpr (type == ESimpleLogicalValueType::Int64) {
        return static_cast<i64>(Max<i64>());
    } else if constexpr (type == ESimpleLogicalValueType::Uint8) { // Uints
        return static_cast<ui64>(Max<ui8>());
    } else if constexpr (type == ESimpleLogicalValueType::Uint16) {
        return static_cast<ui64>(Max<ui16>());
    } else if constexpr (type == ESimpleLogicalValueType::Uint32) {
        return static_cast<ui64>(Max<ui32>());
    } else if constexpr (type == ESimpleLogicalValueType::Uint64) {
        return static_cast<ui64>(Max<ui64>());
    } else if constexpr (type == ESimpleLogicalValueType::Date) { // Time types
        return static_cast<ui64>(DateUpperBound - 1);
    } else if constexpr (type == ESimpleLogicalValueType::Datetime) {
        return static_cast<ui64>(DatetimeUpperBound - 1);
    } else if constexpr (type == ESimpleLogicalValueType::Timestamp) {
        return static_cast<ui64>(TimestampUpperBound - 1);
    } else if constexpr (type == ESimpleLogicalValueType::Interval) {
        return static_cast<i64>(TimestampUpperBound - 1);
    } else if constexpr (type == ESimpleLogicalValueType::Float) { // Floating point
        return static_cast<double>(Max<float>());
    } else if constexpr (type == ESimpleLogicalValueType::Date32) { // Wide time types
        return static_cast<i64>(Date32UpperBound - 1);
    } else if constexpr (type == ESimpleLogicalValueType::Datetime64) {
        return static_cast<i64>(Datetime64UpperBound - 1);
    } else if constexpr (type == ESimpleLogicalValueType::Timestamp64) {
        return static_cast<i64>(Timestamp64UpperBound - 1);
    } else if constexpr (type == ESimpleLogicalValueType::Interval64) {
        return static_cast<i64>(Interval64UpperBound - 1);
    } else {
        // silly replacement for static_assert(false, ...);
        static_assert(type == ESimpleLogicalValueType::Int8, "unsupported type");
    }
}

template <ESimpleLogicalValueType type>
static constexpr auto GetLogicalTypeMin()
{
    // Ints
    if constexpr (type == ESimpleLogicalValueType::Int8) {
        return static_cast<i64>(Min<i8>());
    } else if constexpr (type == ESimpleLogicalValueType::Int16) {
        return static_cast<i64>(Min<i16>());
    } else if constexpr (type == ESimpleLogicalValueType::Int32) {
        return static_cast<i64>(Min<i32>());
    } else if constexpr (type == ESimpleLogicalValueType::Int64) {
        return static_cast<i64>(Min<i64>());
    } else if constexpr (type == ESimpleLogicalValueType::Uint8) { // Uints
        return static_cast<ui64>(Min<ui8>());
    } else if constexpr (type == ESimpleLogicalValueType::Uint16) {
        return static_cast<ui64>(Min<ui16>());
    } else if constexpr (type == ESimpleLogicalValueType::Uint32) {
        return static_cast<ui64>(Min<ui32>());
    } else if constexpr (type == ESimpleLogicalValueType::Uint64) {
        return static_cast<ui64>(Min<ui64>());
    } else if constexpr (type == ESimpleLogicalValueType::Date) { // Time types
        return static_cast<ui64>(0);
    } else if constexpr (type == ESimpleLogicalValueType::Datetime) {
        return static_cast<ui64>(0);
    } else if constexpr (type == ESimpleLogicalValueType::Timestamp) {
        return static_cast<ui64>(0);
    } else if constexpr (type == ESimpleLogicalValueType::Interval) {
        return static_cast<i64>(-TimestampUpperBound + 1);
    } else if constexpr (type == ESimpleLogicalValueType::Float) { // Floating point
        return static_cast<double>(std::numeric_limits<float>::lowest());
    } else if constexpr (type == ESimpleLogicalValueType::Date32) { // Wide time types
        return static_cast<i64>(Date32LowerBound);
    } else if constexpr (type == ESimpleLogicalValueType::Datetime64) {
        return static_cast<i64>(Datetime64LowerBound);
    } else if constexpr (type == ESimpleLogicalValueType::Timestamp64) {
        return static_cast<i64>(Timestamp64LowerBound);
    } else if constexpr (type == ESimpleLogicalValueType::Interval64) {
        return static_cast<i64>(-Interval64UpperBound + 1);
    } else {
        // silly replacement for static_assert(false, ...);
        static_assert(type == ESimpleLogicalValueType::Int8, "unsupported type");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <ESimpleLogicalValueType type>
Y_FORCE_INLINE void ValidateSimpleLogicalType(i64 value)
{
    if constexpr (
        type == ESimpleLogicalValueType::Int8 ||
        type == ESimpleLogicalValueType::Int16 ||
        type == ESimpleLogicalValueType::Int32 ||
        type == ESimpleLogicalValueType::Interval ||
        type == ESimpleLogicalValueType::Date32 ||
        type == ESimpleLogicalValueType::Datetime64 ||
        type == ESimpleLogicalValueType::Timestamp64 ||
        type == ESimpleLogicalValueType::Interval64)
    {
        NDetail::ValidateNumericRange(
            value,
            NDetail::GetLogicalTypeMin<type>(),
            NDetail::GetLogicalTypeMax<type>());
    } else {
        static_assert(type == ESimpleLogicalValueType::Int64, "Bad logical type");
        // Do nothing since Int64 doesn't require validation
    }
}

template <ESimpleLogicalValueType type>
Y_FORCE_INLINE void ValidateSimpleLogicalType(ui64 value)
{
    if constexpr (
        type == ESimpleLogicalValueType::Uint8 ||
        type == ESimpleLogicalValueType::Uint16 ||
        type == ESimpleLogicalValueType::Uint32 ||
        type == ESimpleLogicalValueType::Date ||
        type == ESimpleLogicalValueType::Datetime ||
        type == ESimpleLogicalValueType::Timestamp)
    {
        NDetail::ValidateNumericRange(
            value,
            NDetail::GetLogicalTypeMin<type>(),
            NDetail::GetLogicalTypeMax<type>());
    } else {
        static_assert(type == ESimpleLogicalValueType::Uint64, "Bad logical type");
        // Do nothing since Uint64 doesn't require validation
    }
}

template <ESimpleLogicalValueType type>
Y_FORCE_INLINE void ValidateSimpleLogicalType(double value)
{
    if constexpr (type == ESimpleLogicalValueType::Float) {
        if (!std::isinf(value) && !std::isnan(value)) {
            NDetail::ValidateNumericRange(
                value,
                NDetail::GetLogicalTypeMin<type>(),
                NDetail::GetLogicalTypeMax<type>());
        }
    } else if constexpr (type == ESimpleLogicalValueType::Double)  {
        // do nothing
    } else {
        static_assert(type == ESimpleLogicalValueType::Double, "Bad logical type");
    }
}

template <ESimpleLogicalValueType type>
Y_FORCE_INLINE void ValidateSimpleLogicalType(bool /*value*/)
{
    if constexpr (type == ESimpleLogicalValueType::Boolean)  {
        // do nothing
    } else {
        static_assert(type == ESimpleLogicalValueType::Boolean, "Bad logical type");
    }
}

template <ESimpleLogicalValueType type>
void ValidateSimpleLogicalType(TStringBuf value)
{
    if constexpr (type == ESimpleLogicalValueType::String)  {
        // do nothing
    } else if constexpr (type == ESimpleLogicalValueType::Utf8) {
        if (UTF8Detect(value.data(), value.size()) == NotUTF8) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::SchemaViolation,
                "Not a valid utf8 string");
        }
    } else if constexpr (type == ESimpleLogicalValueType::Uuid) {
        if (value.size() != 16) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::SchemaViolation,
                "Not a valid Uuid");
        }
    } else {
        static_assert(type == ESimpleLogicalValueType::String, "Bad logical type");
    }
}

template <>
void ValidateSimpleLogicalType<ESimpleLogicalValueType::Json>(TStringBuf value);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

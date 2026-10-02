// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#pragma once
#include "arrow_clickhouse_types.h"

#include <Common/SipHash.h>
#include <Common/Arena.h>

#include <common/StringRef.h>

/// Common helper methods for implementation of different columns.

namespace CH
{

/// Counts how many bytes of `filt` are greater than zero.
size_t countBytesInFilter(const uint8_t * filt, size_t start, size_t end);
size_t countBytesInFilterWithNull(const uint8_t * filt, const uint8_t * null_map, size_t start, size_t end);

template <typename T>
inline StringRef serializeNumberIntoArena(T value, Arena & arena, char const *& begin)
{
    auto * pos = arena.allocContinue(sizeof(T), begin);
    unalignedStore<T>(pos, value);
    return StringRef(pos, sizeof(T));
}

inline StringRef serializeStringIntoArena(const StringRef & str, Arena & arena, char const *& begin)
{
    StringRef res;
    res.size = sizeof(str.size) + str.size;
    char * pos = arena.allocContinue(res.size, begin);
    memcpy(pos, &str.size, sizeof(str.size));
    memcpy(pos + sizeof(str.size), str.data, str.size);
    res.data = pos;
    return res;
}

inline StringRef serializeFixedStringIntoArena(const StringRef & str, Arena & arena, char const *& begin)
{
    auto * pos = arena.allocContinue(str.size, begin);
    memcpy(pos, str.data, str.size);
    return StringRef(pos, str.size);
}

inline StringRef serializeDecimalIntoArena(const StringRef & str, Arena & arena, char const *& begin)
{
    auto * pos = arena.allocContinue(str.size, begin);
    memcpy(pos, str.data, str.size);
    return StringRef(pos, str.size);
}

template <typename T>
inline bool insertNumber(MutableColumn & column, T value)
{
    if constexpr (std::is_same_v<T, UInt8>)
        return assert_cast<MutableColumnUInt8 &>(column).Append(value).ok();
    else if constexpr (std::is_same_v<T, UInt16>)
        return assert_cast<MutableColumnUInt16 &>(column).Append(value).ok();
    else if constexpr (std::is_same_v<T, UInt32>)
        return assert_cast<MutableColumnUInt32 &>(column).Append(value).ok();
    else if constexpr (std::is_same_v<T, UInt64>)
        return assert_cast<MutableColumnUInt64 &>(column).Append(value).ok();
    else if constexpr (std::is_same_v<T, Int8>)
        return assert_cast<MutableColumnInt8 &>(column).Append(value).ok();
    else if constexpr (std::is_same_v<T, Int16>)
        return assert_cast<MutableColumnInt16 &>(column).Append(value).ok();
    else if constexpr (std::is_same_v<T, Int32>)
        return assert_cast<MutableColumnInt32 &>(column).Append(value).ok();
    else if constexpr (std::is_same_v<T, Int64>)
        return assert_cast<MutableColumnInt64 &>(column).Append(value).ok();
    else if constexpr (std::is_same_v<T, float>)
        return assert_cast<MutableColumnFloat32 &>(column).Append(value).ok();
    else if constexpr (std::is_same_v<T, double>)
        return assert_cast<MutableColumnFloat64 &>(column).Append(value).ok();

    throw Exception("unexpected type");
}

template <typename T>
inline bool insertSameSizeNumber(MutableColumn & column, T value)
{
    if constexpr (std::is_same_v<T, UInt8>)
        return assert_same_size_cast<MutableColumnUInt8 &>(column).Append(value).ok();
    else if constexpr (std::is_same_v<T, UInt16>)
        return assert_same_size_cast<MutableColumnUInt16 &>(column).Append(value).ok();
    else if constexpr (std::is_same_v<T, UInt32>)
        return assert_same_size_cast<MutableColumnUInt32 &>(column).Append(value).ok();
    else if constexpr (std::is_same_v<T, UInt64>)
        return assert_same_size_cast<MutableColumnUInt64 &>(column).Append(value).ok();
    else if constexpr (std::is_same_v<T, Int8>)
        return assert_same_size_cast<MutableColumnInt8 &>(column).Append(value).ok();
    else if constexpr (std::is_same_v<T, Int16>)
        return assert_same_size_cast<MutableColumnInt16 &>(column).Append(value).ok();
    else if constexpr (std::is_same_v<T, Int32>)
        return assert_same_size_cast<MutableColumnInt32 &>(column).Append(value).ok();
    else if constexpr (std::is_same_v<T, Int64>)
        return assert_same_size_cast<MutableColumnInt64 &>(column).Append(value).ok();
    else if constexpr (std::is_same_v<T, float>)
        return assert_same_size_cast<MutableColumnFloat32 &>(column).Append(value).ok();
    else if constexpr (std::is_same_v<T, double>)
        return assert_same_size_cast<MutableColumnFloat64 &>(column).Append(value).ok();

    throw Exception("unexpected type");
}

inline bool insertTimestamp(MutableColumn & column, Int64 value)
{
    return assert_cast<MutableColumnTimestamp &>(column).Append(value).ok();
}

inline bool insertDuration(MutableColumn & column, Int64 value)
{
    return assert_cast<MutableColumnDuration &>(column).Append(value).ok();
}

inline bool insertString(MutableColumn & column, const StringRef & value)
{
    return assert_cast<MutableColumnBinary &>(column).Append(arrow::util::string_view{value.data, value.size}).ok();
}

inline bool insertFixedString(MutableColumn & column, const StringRef & value)
{
    return assert_cast<MutableColumnFixedString &>(column).Append(arrow::util::string_view{value.data, value.size}).ok();
}

inline bool insertDecimal(MutableColumn & column, const StringRef & value)
{
    return assert_cast<MutableColumnDecimal &>(column).Append(arrow::util::string_view{value.data, value.size}).ok();
}

template <typename DataType>
inline const char * deserializeNumberFromArena(arrow::NumericBuilder<DataType> & column, const char * pos)
{
    using T = typename arrow::TypeTraits<DataType>::CType;

    T value = unalignedLoad<T>(pos);
    column.Append(value).ok();
    return pos + sizeof(T);
}

inline const char * deserializeStringFromArena(MutableColumnBinary & column, const char * pos)
{
    const size_t string_size = unalignedLoad<size_t>(pos);
    pos += sizeof(string_size);

    column.Append(pos, string_size).ok();
    return pos + string_size;
}

inline const char * deserializeStringFromArena(MutableColumnFixedString & column, const char * pos)
{
    column.Append(pos).ok();
    return pos + column.byte_width();
}

inline const char * deserializeDecimalFromArena(MutableColumnDecimal & column, const char * pos)
{
    column.Append(pos).ok();
    return pos + column.byte_width();
}

bool insertData(MutableColumn & column, const StringRef & value, const arrow::Type::type typeId);
StringRef serializeValueIntoArena(const IColumn& column, size_t row, Arena & pool, char const *& begin);
const char * deserializeAndInsertFromArena(MutableColumn& column, const char * pos, const arrow::Type::type typeId);
void updateHashWithValue(const IColumn& column, size_t row, SipHash & hash);
MutableColumnPtr createMutableColumn(const DataTypePtr & type);
uint32_t fixedContiguousSize(const DataTypePtr & type);

}

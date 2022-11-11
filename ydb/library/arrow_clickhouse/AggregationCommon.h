// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#pragma once
#include "arrow_clickhouse_types.h"

#include <array>

#include <Columns/ColumnsCommon.h>
#include <Common/HashTable/Hash.h>
#include <Common/memcpySmall.h>

#include <common/StringRef.h>

#if defined(__SSSE3__) && !defined(MEMORY_SANITIZER)
#include <tmmintrin.h>
#endif


namespace CH
{

/// When packing the values of nullable columns at a given row, we have to
/// store the fact that these values are nullable or not. This is achieved
/// by encoding this information as a bitmap. Let S be the size in bytes of
/// a packed values binary blob and T the number of bytes we may place into
/// this blob, the size that the bitmap shall occupy in the blob is equal to:
/// ceil(T/8). Thus we must have: S = T + ceil(T/8). Below we indicate for
/// each value of S, the corresponding value of T, and the bitmap size:
///
/// 32,28,4
/// 16,14,2
/// 8,7,1
/// 4,3,1
/// 2,1,1
///

namespace
{

template <typename T>
constexpr auto getBitmapSize()
{
    return
        (sizeof(T) == 32) ?
            4 :
        (sizeof(T) == 16) ?
            2 :
        ((sizeof(T) == 8) ?
            1 :
        ((sizeof(T) == 4) ?
            1 :
        ((sizeof(T) == 2) ?
            1 :
        0)));
}

template <typename DataType>
static inline size_t ALWAYS_INLINE packFixedKey(char * __restrict dst, const IColumn * column, size_t row)
{
    using ColumnType = typename arrow::TypeTraits<DataType>::ArrayType;
    using CType = typename arrow::TypeTraits<DataType>::CType;

    auto * typed_column = assert_cast<const ColumnType *>(column);
    memcpy(dst, typed_column->raw_values() + row, sizeof(CType));
    return sizeof(CType);
}

static inline size_t ALWAYS_INLINE packFixedStringKey(char * __restrict bytes, const IColumn * column, size_t row)
{
    auto * typed_column = assert_cast<const ColumnFixedString *>(column);
    int32_t key_size = typed_column->byte_width();
    memcpy(bytes, typed_column->raw_values() + row * key_size, key_size);
    return key_size;
}

static inline size_t ALWAYS_INLINE switchPackFixedKey(char * __restrict bytes, const IColumn * column, size_t row)
{
    switch (column->type_id())
    {
        case arrow::Type::UINT8:
            return packFixedKey<DataTypeUInt8>(bytes, column, row);
        case arrow::Type::UINT16:
            return packFixedKey<DataTypeUInt16>(bytes, column, row);
        case arrow::Type::UINT32:
            return packFixedKey<DataTypeUInt32>(bytes, column, row);
        case arrow::Type::UINT64:
            return packFixedKey<DataTypeUInt64>(bytes, column, row);
        case arrow::Type::INT8:
            return packFixedKey<DataTypeInt8>(bytes, column, row);
        case arrow::Type::INT16:
            return packFixedKey<DataTypeInt16>(bytes, column, row);
        case arrow::Type::INT32:
            return packFixedKey<DataTypeInt32>(bytes, column, row);
        case arrow::Type::INT64:
            return packFixedKey<DataTypeInt64>(bytes, column, row);
        case arrow::Type::FLOAT:
            return packFixedKey<DataTypeFloat32>(bytes, column, row);
        case arrow::Type::DOUBLE:
            return packFixedKey<DataTypeFloat64>(bytes, column, row);
        case arrow::Type::TIMESTAMP:
            return packFixedKey<DataTypeTimestamp>(bytes, column, row);
        case arrow::Type::DURATION:
            return packFixedKey<DataTypeDuration>(bytes, column, row);
        // TODO: TIME32 DATE64 TIME64 INTERVAL_DAY_TIME DECIMAL128 DECIMAL256
        default:
            return packFixedStringKey(bytes, column, row);
    }
    return 0;
}

}

/// Pack into a binary blob of type T a set of fixed-size keys. Granted that all the keys fit into the
/// binary blob, they are disposed in it consecutively.
template <typename T>
static inline T ALWAYS_INLINE packFixed(
    size_t i, size_t keys_size, const ColumnRawPtrs & key_columns)
{
    T key{};
    char * bytes = reinterpret_cast<char *>(&key);
    size_t offset = 0;

    for (size_t j = 0; j < keys_size; ++j)
    {
        const IColumn * column = key_columns[j];
        offset += switchPackFixedKey(bytes + offset, column, i);
    }

    return key;
}

template <typename T>
using KeysNullMap = std::array<UInt8, getBitmapSize<T>()>;

/// Similar as above but supports nullable values.
template <typename T>
static inline T ALWAYS_INLINE packFixed(
    size_t i, size_t keys_size, const ColumnRawPtrs & key_columns, const KeysNullMap<T> & bitmap)
{
    union
    {
        T key;
        char bytes[sizeof(key)] = {};
    };

    size_t offset = 0;

    static constexpr auto bitmap_size = std::tuple_size<KeysNullMap<T>>::value;
    static constexpr bool has_bitmap = bitmap_size > 0;

    if (has_bitmap)
    {
        memcpy(bytes + offset, bitmap.data(), bitmap_size * sizeof(UInt8));
        offset += bitmap_size;
    }

    for (size_t j = 0; j < keys_size; ++j)
    {
        bool is_null;

        if (!has_bitmap)
            is_null = false;
        else
        {
            size_t bucket = j / 8;
            size_t off = j % 8;
            is_null = ((bitmap[bucket] >> off) & 1) == 1;
        }

        if (is_null)
            continue;

        const IColumn * column = key_columns[j];
        offset += switchPackFixedKey(bytes + offset, column, i);
    }

    return key;
}


/// Hash a set of keys into a UInt128 value.
static inline UInt128 ALWAYS_INLINE hash128(size_t row, size_t keys_size, const ColumnRawPtrs & key_columns)
{
    UInt128 key;
    SipHash hash;

    for (size_t j = 0; j < keys_size; ++j)
        updateHashWithValue(*key_columns[j], row, hash);

    hash.get128(key);
    return key;
}


/** Serialize keys into a continuous chunk of memory.
  */
static inline StringRef ALWAYS_INLINE serializeKeysToPoolContiguous(
    size_t row, size_t keys_size, const ColumnRawPtrs & key_columns, Arena & pool)
{
    const char * begin = nullptr;

    size_t sum_size = 0;
    for (size_t j = 0; j < keys_size; ++j)
        sum_size += serializeValueIntoArena(*key_columns[j], row, pool, begin).size;

    return {begin, sum_size};
}

}

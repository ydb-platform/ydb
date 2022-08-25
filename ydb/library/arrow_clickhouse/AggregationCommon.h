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

}

template<typename T, size_t step>
void fillFixedBatch(size_t num_rows, const T * source, T * dest)
{
    for (size_t i = 0; i < num_rows; ++i)
    {
        *dest = *source;
        ++source;
        dest += step;
    }
}

/// Move keys of size T into binary blob, starting from offset.
/// It is assumed that offset is aligned to sizeof(T).
/// Example: sizeof(key) = 16, sizeof(T) = 4, offset = 8
/// out[0] : [--------****----]
/// out[1] : [--------****----]
/// ...
template<typename T, typename Key>
void fillFixedBatch(size_t keys_size, const ColumnRawPtrs & key_columns, const Sizes & key_sizes, PaddedPODArray<Key> & out, size_t & offset)
{
    for (size_t i = 0; i < keys_size; ++i)
    {
        if (key_sizes[i] == sizeof(T))
        {
            const auto * column = key_columns[i];
            size_t num_rows = column->length();
            out.resize_fill(num_rows);
#if 0
            /// Note: here we violate strict aliasing.
            /// It should be ok as log as we do not reffer to any value from `out` before filling.
            const char * source = assert_cast<const ColumnVectorHelper *>(column)->getRawDataBegin<sizeof(T)>();
            T * dest = reinterpret_cast<T *>(reinterpret_cast<char *>(out.data()) + offset);
            fillFixedBatch<T, sizeof(Key) / sizeof(T)>(num_rows, reinterpret_cast<const T *>(source), dest);
            offset += sizeof(T);
#else
            T * dest = reinterpret_cast<T *>(reinterpret_cast<char *>(out.data()) + offset);
            switch (sizeof(T))
            {
                case 1:
                case 2:
                case 4:
                case 8:
                {
                    const uint8_t * source = assert_cast<const ColumnUInt8 *>(column)->raw_values();
                    fillFixedBatch<T, sizeof(Key) / sizeof(T)>(num_rows, reinterpret_cast<const T *>(source), dest);
                    break;
                }
                default:
                {
                    const uint8_t * source = assert_cast<const ColumnFixedString *>(column)->raw_values();
                    fillFixedBatch<T, sizeof(Key) / sizeof(T)>(num_rows, reinterpret_cast<const T *>(source), dest);
                    break;
                }
            }
            offset += sizeof(T);
#endif
        }
    }
}

/// Pack into a binary blob of type T a set of fixed-size keys. Granted that all the keys fit into the
/// binary blob. Keys are placed starting from the longest one.
template <typename T>
void packFixedBatch(size_t keys_size, const ColumnRawPtrs & key_columns, const Sizes & key_sizes, PaddedPODArray<T> & out)
{
    size_t offset = 0;
    fillFixedBatch<UInt128>(keys_size, key_columns, key_sizes, out, offset);
    fillFixedBatch<UInt64>(keys_size, key_columns, key_sizes, out, offset);
    fillFixedBatch<UInt32>(keys_size, key_columns, key_sizes, out, offset);
    fillFixedBatch<UInt16>(keys_size, key_columns, key_sizes, out, offset);
    fillFixedBatch<UInt8>(keys_size, key_columns, key_sizes, out, offset);
}

template <typename T>
using KeysNullMap = std::array<UInt8, getBitmapSize<T>()>;

/// Pack into a binary blob of type T a set of fixed-size keys. Granted that all the keys fit into the
/// binary blob, they are disposed in it consecutively.
template <typename T>
static inline T ALWAYS_INLINE packFixed(
    size_t i, size_t keys_size, const ColumnRawPtrs & key_columns, const Sizes & key_sizes)
{
    T key{};
    char * bytes = reinterpret_cast<char *>(&key);
    size_t offset = 0;

    for (size_t j = 0; j < keys_size; ++j)
    {
        size_t index = i;
        const IColumn * column = key_columns[j];

        switch (key_sizes[j])
        {
            case 1:
                {
                    memcpy(bytes + offset, assert_cast<const ColumnUInt8 *>(column)->raw_values() + index, 1);
                    offset += 1;
                }
                break;
            case 2:
                if constexpr (sizeof(T) >= 2)   /// To avoid warning about memcpy exceeding object size.
                {
                    memcpy(bytes + offset, assert_cast<const ColumnUInt16 *>(column)->raw_values() + index, 2);
                    offset += 2;
                }
                break;
            case 4:
                if constexpr (sizeof(T) >= 4)
                {
                    memcpy(bytes + offset, assert_cast<const ColumnUInt32 *>(column)->raw_values() + index, 4);
                    offset += 4;
                }
                break;
            case 8:
                if constexpr (sizeof(T) >= 8)
                {
                    memcpy(bytes + offset, assert_cast<const ColumnUInt64 *>(column)->raw_values() + index, 8);
                    offset += 8;
                }
                break;
            default:
                memcpy(bytes + offset, assert_cast<const ColumnFixedString *>(column)->raw_values() + index * key_sizes[j], key_sizes[j]);
                offset += key_sizes[j];
        }
    }

    return key;
}

/// Similar as above but supports nullable values.
template <typename T>
static inline T ALWAYS_INLINE packFixed(
    size_t i, size_t keys_size, const ColumnRawPtrs & key_columns, const Sizes & key_sizes,
    const KeysNullMap<T> & bitmap)
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

        switch (key_sizes[j])
        {
            case 1:
                memcpy(bytes + offset, assert_cast<const ColumnUInt8 *>(key_columns[j])->raw_values() + i, 1);
                offset += 1;
                break;
            case 2:
                memcpy(bytes + offset, assert_cast<const ColumnUInt16 *>(key_columns[j])->raw_values() + i, 2);
                offset += 2;
                break;
            case 4:
                memcpy(bytes + offset, assert_cast<const ColumnUInt32 *>(key_columns[j])->raw_values() + i, 4);
                offset += 4;
                break;
            case 8:
                memcpy(bytes + offset, assert_cast<const ColumnUInt64 *>(key_columns[j])->raw_values() + i, 8);
                offset += 8;
                break;
            default:
                memcpy(bytes + offset, assert_cast<const ColumnFixedString *>(key_columns[j])->raw_values() + i * key_sizes[j], key_sizes[j]);
                offset += key_sizes[j];
        }
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


/// Copy keys to the pool. Then put into pool StringRefs to them and return the pointer to the first.
static inline StringRef * ALWAYS_INLINE placeKeysInPool(
    size_t keys_size, StringRefs & keys, Arena & pool)
{
    for (size_t j = 0; j < keys_size; ++j)
    {
        char * place = pool.alloc(keys[j].size);
        memcpySmallAllowReadWriteOverflow15(place, keys[j].data, keys[j].size);
        keys[j].data = place;
    }

    /// Place the StringRefs on the newly copied keys in the pool.
    char * res = pool.alignedAlloc(keys_size * sizeof(StringRef), alignof(StringRef));
    memcpySmallAllowReadWriteOverflow15(res, keys.data(), keys_size * sizeof(StringRef));

    return reinterpret_cast<StringRef *>(res);
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


/** Pack elements with shuffle instruction.
  * See the explanation in ColumnsHashing.h
  */
#if defined(__SSSE3__) && !defined(MEMORY_SANITIZER)
template <typename T>
static T inline packFixedShuffle(
    const char * __restrict * __restrict srcs,
    size_t num_srcs,
    const size_t * __restrict elem_sizes,
    size_t idx,
    const uint8_t * __restrict masks)
{
    assert(num_srcs > 0);

    __m128i res = _mm_shuffle_epi8(
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(srcs[0] + elem_sizes[0] * idx)),
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(masks)));

    for (size_t i = 1; i < num_srcs; ++i)
    {
        res = _mm_xor_si128(res,
            _mm_shuffle_epi8(
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(srcs[i] + elem_sizes[i] * idx)),
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(&masks[i * sizeof(T)]))));
    }

    T out;
    __builtin_memcpy(&out, &res, sizeof(T));
    return out;
}
#endif

}

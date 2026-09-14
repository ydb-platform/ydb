// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#ifdef __SSE2__
    #include <emmintrin.h>
#endif

#include <Columns/ColumnsCommon.h>
#include <Common/HashTable/HashSet.h>
#include <Common/PODArray.h>


namespace CH
{

#if defined(__SSE2__) && defined(__POPCNT__)
/// Transform 64-byte mask to 64-bit mask.
static UInt64 toBits64(const Int8 * bytes64)
{
    static const __m128i zero16 = _mm_setzero_si128();
    UInt64 res =
        static_cast<UInt64>(_mm_movemask_epi8(_mm_cmpeq_epi8(
            _mm_loadu_si128(reinterpret_cast<const __m128i *>(bytes64)), zero16)))
        | (static_cast<UInt64>(_mm_movemask_epi8(_mm_cmpeq_epi8(
            _mm_loadu_si128(reinterpret_cast<const __m128i *>(bytes64 + 16)), zero16))) << 16)
        | (static_cast<UInt64>(_mm_movemask_epi8(_mm_cmpeq_epi8(
            _mm_loadu_si128(reinterpret_cast<const __m128i *>(bytes64 + 32)), zero16))) << 32)
        | (static_cast<UInt64>(_mm_movemask_epi8(_mm_cmpeq_epi8(
            _mm_loadu_si128(reinterpret_cast<const __m128i *>(bytes64 + 48)), zero16))) << 48);

    return ~res;
}
#endif

size_t countBytesInFilter(const uint8_t * filt, size_t start, size_t end)
{
    size_t count = 0;

    /** NOTE: In theory, `filt` should only contain zeros and ones.
      * But, just in case, here the condition > 0 (to signed bytes) is used.
      * It would be better to use != 0, then this does not allow SSE2.
      */

    const Int8 * pos = reinterpret_cast<const Int8 *>(filt);
    pos += start;

    const Int8 * end_pos = pos + (end - start);

#if defined(__SSE2__)
    const Int8 * end_pos64 = pos + (end - start) / 64 * 64;

    for (; pos < end_pos64; pos += 64)
        count += std::popcount(toBits64(pos));

    /// TODO Add duff device for tail?
#endif

    for (; pos < end_pos; ++pos)
        count += *pos != 0;

    return count;
}

size_t countBytesInFilterWithNull(const uint8_t * filt, const uint8_t * null_map, size_t start, size_t end)
{
    size_t count = 0;

    /** NOTE: In theory, `filt` should only contain zeros and ones.
      * But, just in case, here the condition > 0 (to signed bytes) is used.
      * It would be better to use != 0, then this does not allow SSE2.
      */

    const Int8 * pos = reinterpret_cast<const Int8 *>(filt) + start;
    const Int8 * pos2 = reinterpret_cast<const Int8 *>(null_map) + start;
    const Int8 * end_pos = pos + (end - start);

#if defined(__SSE2__)
    const Int8 * end_pos64 = pos + (end - start) / 64 * 64;

    for (; pos < end_pos64; pos += 64, pos2 += 64)
        count += std::popcount(toBits64(pos) & ~toBits64(pos2));

        /// TODO Add duff device for tail?
#endif

    for (; pos < end_pos; ++pos, ++pos2)
        count += (*pos & ~*pos2) != 0;

    return count;
}

namespace
{
    /// Implementation details of filterArraysImpl function, used as template parameter.
    /// Allow to build or not to build offsets array.

    struct ResultOffsetsBuilder
    {
        PaddedPODArray<UInt64> & res_offsets;
        XColumn::Offset current_src_offset = 0;

        explicit ResultOffsetsBuilder(PaddedPODArray<UInt64> * res_offsets_) : res_offsets(*res_offsets_) {}

        void reserve(ssize_t result_size_hint, size_t src_size)
        {
            res_offsets.reserve(result_size_hint > 0 ? result_size_hint : src_size);
        }

        void insertOne(size_t array_size)
        {
            current_src_offset += array_size;
            res_offsets.push_back(current_src_offset);
        }

        template <size_t SIMD_BYTES>
        void insertChunk(
            const XColumn::Offset * src_offsets_pos,
            bool first,
            XColumn::Offset chunk_offset,
            size_t chunk_size)
        {
            const auto offsets_size_old = res_offsets.size();
            res_offsets.resize(offsets_size_old + SIMD_BYTES);
            memcpy(&res_offsets[offsets_size_old], src_offsets_pos, SIMD_BYTES * sizeof(XColumn::Offset));

            if (!first)
            {
                /// difference between current and actual offset
                const auto diff_offset = chunk_offset - current_src_offset;

                if (diff_offset > 0)
                {
                    auto * res_offsets_pos = &res_offsets[offsets_size_old];

                    /// adjust offsets
                    for (size_t i = 0; i < SIMD_BYTES; ++i)
                        res_offsets_pos[i] -= diff_offset;
                }
            }
            current_src_offset += chunk_size;
        }
    };

    struct NoResultOffsetsBuilder
    {
        explicit NoResultOffsetsBuilder(PaddedPODArray<UInt64> *) {}
        void reserve(ssize_t, size_t) {}
        void insertOne(size_t) {}

        template <size_t SIMD_BYTES>
        void insertChunk(
            const XColumn::Offset *,
            bool,
            XColumn::Offset,
            size_t)
        {
        }
    };


    template <typename T, typename ResultOffsetsBuilder>
    void filterArraysImplGeneric(
        const PaddedPODArray<T> & src_elems, const PaddedPODArray<UInt64> & src_offsets,
        PaddedPODArray<T> & res_elems, PaddedPODArray<UInt64> * res_offsets,
        const XColumn::Filter & filt, ssize_t result_size_hint)
    {
        const size_t size = src_offsets.size();
        if (size != filt.size())
            throw Exception("Size of filter doesn't match size of column.");

        ResultOffsetsBuilder result_offsets_builder(res_offsets);

        if (result_size_hint)
        {
            result_offsets_builder.reserve(result_size_hint, size);

            if (result_size_hint < 0)
                res_elems.reserve(src_elems.size());
            else if (result_size_hint < 1000000000 && src_elems.size() < 1000000000)    /// Avoid overflow.
                res_elems.reserve((result_size_hint * src_elems.size() + size - 1) / size);
        }

        const UInt8 * filt_pos = filt.data();
        const auto * filt_end = filt_pos + size;

        const auto * offsets_pos = src_offsets.data();
        const auto * offsets_begin = offsets_pos;

        /// copy array ending at *end_offset_ptr
        const auto copy_array = [&] (const XColumn::Offset * offset_ptr)
        {
            const auto arr_offset = offset_ptr == offsets_begin ? 0 : offset_ptr[-1];
            const auto arr_size = *offset_ptr - arr_offset;

            result_offsets_builder.insertOne(arr_size);

            const auto elems_size_old = res_elems.size();
            res_elems.resize(elems_size_old + arr_size);
            memcpy(&res_elems[elems_size_old], &src_elems[arr_offset], arr_size * sizeof(T));
        };

    #ifdef __SSE2__
        const __m128i zero_vec = _mm_setzero_si128();
        static constexpr size_t SIMD_BYTES = 16;
        const auto * filt_end_aligned = filt_pos + size / SIMD_BYTES * SIMD_BYTES;

        while (filt_pos < filt_end_aligned)
        {
            UInt16 mask = _mm_movemask_epi8(_mm_cmpeq_epi8(
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(filt_pos)),
                zero_vec));
            mask = ~mask;

            if (mask == 0)
            {
                /// SIMD_BYTES consecutive rows do not pass the filter
            }
            else if (mask == 0xffff)
            {
                /// SIMD_BYTES consecutive rows pass the filter
                const auto first = offsets_pos == offsets_begin;

                const auto chunk_offset = first ? 0 : offsets_pos[-1];
                const auto chunk_size = offsets_pos[SIMD_BYTES - 1] - chunk_offset;

                result_offsets_builder.template insertChunk<SIMD_BYTES>(offsets_pos, first, chunk_offset, chunk_size);

                /// copy elements for SIMD_BYTES arrays at once
                const auto elems_size_old = res_elems.size();
                res_elems.resize(elems_size_old + chunk_size);
                memcpy(&res_elems[elems_size_old], &src_elems[chunk_offset], chunk_size * sizeof(T));
            }
            else
            {
                for (size_t i = 0; i < SIMD_BYTES; ++i)
                    if (filt_pos[i])
                        copy_array(offsets_pos + i);
            }

            filt_pos += SIMD_BYTES;
            offsets_pos += SIMD_BYTES;
        }
    #endif

        while (filt_pos < filt_end)
        {
            if (*filt_pos)
                copy_array(offsets_pos);

            ++filt_pos;
            ++offsets_pos;
        }
    }
}

bool insertData(MutableColumn & column, const StringRef & value, const arrow::Type::type typeId)
{
    switch (typeId)
    {
        case arrow::Type::UINT8:
            return insertNumber(column, unalignedLoad<UInt8>(value.data));
        case arrow::Type::UINT16:
            return insertNumber(column, unalignedLoad<UInt16>(value.data));
        case arrow::Type::UINT32:
            return insertNumber(column, unalignedLoad<UInt32>(value.data));
        case arrow::Type::UINT64:
            return insertNumber(column, unalignedLoad<UInt64>(value.data));

        case arrow::Type::INT8:
            return insertNumber(column, unalignedLoad<Int8>(value.data));
        case arrow::Type::INT16:
            return insertNumber(column, unalignedLoad<Int16>(value.data));
        case arrow::Type::INT32:
            return insertNumber(column, unalignedLoad<Int32>(value.data));
        case arrow::Type::INT64:
            return insertNumber(column, unalignedLoad<Int64>(value.data));

        case arrow::Type::FLOAT:
            return insertNumber(column, unalignedLoad<float>(value.data));
        case arrow::Type::DOUBLE:
            return insertNumber(column, unalignedLoad<double>(value.data));

        case arrow::Type::FIXED_SIZE_BINARY:
            return insertFixedString(column, value);

        case arrow::Type::STRING:
        case arrow::Type::BINARY:
            return insertString(column, value);

        case arrow::Type::TIMESTAMP:
            return insertTimestamp(column, unalignedLoad<Int64>(value.data));
        case arrow::Type::DURATION:
            return insertDuration(column, unalignedLoad<Int64>(value.data));
        case arrow::Type::DECIMAL:
            return insertDecimal(column, value);

        case arrow::Type::EXTENSION: // AggregateColumn
            break; // TODO

        default:
            break;
    }

    throw Exception(std::string(__FUNCTION__) + " unexpected type " + column.type()->ToString());
}

StringRef serializeValueIntoArena(const IColumn& column, size_t row, Arena & pool, char const *& begin)
{
    switch (column.type_id())
    {
        case arrow::Type::UINT8:
            return serializeNumberIntoArena(assert_cast<const ColumnUInt8 &>(column).Value(row), pool, begin);
        case arrow::Type::UINT16:
            return serializeNumberIntoArena(assert_cast<const ColumnUInt16 &>(column).Value(row), pool, begin);
        case arrow::Type::UINT32:
            return serializeNumberIntoArena(assert_cast<const ColumnUInt32 &>(column).Value(row), pool, begin);
        case arrow::Type::UINT64:
            return serializeNumberIntoArena(assert_cast<const ColumnUInt64 &>(column).Value(row), pool, begin);

        case arrow::Type::INT8:
            return serializeNumberIntoArena(assert_cast<const ColumnInt8 &>(column).Value(row), pool, begin);
        case arrow::Type::INT16:
            return serializeNumberIntoArena(assert_cast<const ColumnInt16 &>(column).Value(row), pool, begin);
        case arrow::Type::INT32:
            return serializeNumberIntoArena(assert_cast<const ColumnInt32 &>(column).Value(row), pool, begin);
        case arrow::Type::INT64:
            return serializeNumberIntoArena(assert_cast<const ColumnInt64 &>(column).Value(row), pool, begin);

        case arrow::Type::FLOAT:
            return serializeNumberIntoArena(assert_cast<const ColumnFloat32 &>(column).Value(row), pool, begin);
        case arrow::Type::DOUBLE:
            return serializeNumberIntoArena(assert_cast<const ColumnFloat64 &>(column).Value(row), pool, begin);

        case arrow::Type::FIXED_SIZE_BINARY:
        {
            auto str = assert_cast<const ColumnFixedString &>(column).GetView(row);
            return serializeFixedStringIntoArena(StringRef(str.data(), str.size()), pool, begin);
        }
        case arrow::Type::STRING:
        case arrow::Type::BINARY:
        {
            auto str = assert_cast<const ColumnBinary &>(column).GetView(row);
            return serializeStringIntoArena(StringRef(str.data(), str.size()), pool, begin);
        }

        case arrow::Type::TIMESTAMP:
            return serializeNumberIntoArena(assert_cast<const ColumnTimestamp &>(column).Value(row), pool, begin);
        case arrow::Type::DURATION:
            return serializeNumberIntoArena(assert_cast<const ColumnDuration &>(column).Value(row), pool, begin);
        case arrow::Type::DECIMAL:
        {
            auto str = assert_cast<const ColumnDecimal &>(column).GetView(row);
            return serializeDecimalIntoArena(StringRef(str.data(), str.size()), pool, begin);
        }

        case arrow::Type::EXTENSION: // AggregateColumn
            break; // TODO

        default:
            break;
    }

    throw Exception(std::string(__FUNCTION__) + " unexpected type " + column.type()->ToString());
}

const char * deserializeAndInsertFromArena(MutableColumn& column, const char * pos, const arrow::Type::type typeId)
{
    switch (typeId)
    {
        case arrow::Type::UINT8:
            return deserializeNumberFromArena(assert_cast<MutableColumnUInt8 &>(column), pos);
        case arrow::Type::UINT16:
            return deserializeNumberFromArena(assert_cast<MutableColumnUInt16 &>(column), pos);
        case arrow::Type::UINT32:
            return deserializeNumberFromArena(assert_cast<MutableColumnUInt32 &>(column), pos);
        case arrow::Type::UINT64:
            return deserializeNumberFromArena(assert_cast<MutableColumnUInt64 &>(column), pos);

        case arrow::Type::INT8:
            return deserializeNumberFromArena(assert_cast<MutableColumnInt8 &>(column), pos);
        case arrow::Type::INT16:
            return deserializeNumberFromArena(assert_cast<MutableColumnInt16 &>(column), pos);
        case arrow::Type::INT32:
            return deserializeNumberFromArena(assert_cast<MutableColumnInt32 &>(column), pos);
        case arrow::Type::INT64:
            return deserializeNumberFromArena(assert_cast<MutableColumnInt64 &>(column), pos);

        case arrow::Type::FLOAT:
            return deserializeNumberFromArena(assert_cast<MutableColumnFloat32 &>(column), pos);
        case arrow::Type::DOUBLE:
            return deserializeNumberFromArena(assert_cast<MutableColumnFloat64 &>(column), pos);

        case arrow::Type::FIXED_SIZE_BINARY:
            return deserializeStringFromArena(assert_cast<MutableColumnFixedString &>(column), pos);

        case arrow::Type::STRING:
        case arrow::Type::BINARY:
            return deserializeStringFromArena(assert_cast<MutableColumnBinary &>(column), pos);

        case arrow::Type::TIMESTAMP:
            return deserializeNumberFromArena(assert_cast<MutableColumnTimestamp &>(column), pos);
        case arrow::Type::DURATION:
            return deserializeNumberFromArena(assert_cast<MutableColumnDuration &>(column), pos);
        case arrow::Type::DECIMAL:
            return deserializeDecimalFromArena(assert_cast<MutableColumnDecimal &>(column), pos);

        case arrow::Type::EXTENSION: // AggregateColumn
            break; // TODO

        default:
            break;
    }

    throw Exception(std::string(__FUNCTION__) + " unexpected type " + column.type()->ToString());
}

void updateHashWithValue(const IColumn& column, size_t row, SipHash & hash)
{
    switch (column.type_id())
    {
        case arrow::Type::UINT8:
            return hash.update(assert_cast<const ColumnUInt8 &>(column).Value(row));
        case arrow::Type::UINT16:
            return hash.update(assert_cast<const ColumnUInt16 &>(column).Value(row));
        case arrow::Type::UINT32:
            return hash.update(assert_cast<const ColumnUInt32 &>(column).Value(row));
        case arrow::Type::UINT64:
            return hash.update(assert_cast<const ColumnUInt64 &>(column).Value(row));

        case arrow::Type::INT8:
            return hash.update(assert_cast<const ColumnInt8 &>(column).Value(row));
        case arrow::Type::INT16:
            return hash.update(assert_cast<const ColumnInt16 &>(column).Value(row));
        case arrow::Type::INT32:
            return hash.update(assert_cast<const ColumnInt32 &>(column).Value(row));
        case arrow::Type::INT64:
            return hash.update(assert_cast<const ColumnInt64 &>(column).Value(row));

        case arrow::Type::FLOAT:
            return hash.update(assert_cast<const ColumnFloat32 &>(column).Value(row));
        case arrow::Type::DOUBLE:
            return hash.update(assert_cast<const ColumnFloat64 &>(column).Value(row));

        case arrow::Type::FIXED_SIZE_BINARY:
        {
            auto str = assert_cast<const ColumnFixedString &>(column).GetView(row);
            return hash.update(str.data(), str.size());
        }
        case arrow::Type::STRING:
        case arrow::Type::BINARY:
        {
            auto str = assert_cast<const ColumnBinary &>(column).GetView(row);
            return hash.update(str.data(), str.size());
        }

        case arrow::Type::TIMESTAMP:
            return hash.update(assert_cast<const ColumnTimestamp &>(column).Value(row));
        case arrow::Type::DURATION:
            return hash.update(assert_cast<const ColumnDuration &>(column).Value(row));
        case arrow::Type::DECIMAL:
            return hash.update(assert_cast<const ColumnDecimal &>(column).Value(row));

        case arrow::Type::EXTENSION: // AggregateColumn
            break; // TODO

        default:
            break;
    }

    throw Exception(std::string(__FUNCTION__) + " unexpected type " + column.type()->ToString());
}

MutableColumnPtr createMutableColumn(const DataTypePtr & type)
{
    switch (type->id())
    {
        case arrow::Type::UINT8:
            return std::make_shared<MutableColumnUInt8>();
        case arrow::Type::UINT16:
            return std::make_shared<MutableColumnUInt16>();
        case arrow::Type::UINT32:
            return std::make_shared<MutableColumnUInt32>();
        case arrow::Type::UINT64:
            return std::make_shared<MutableColumnUInt64>();

        case arrow::Type::INT8:
            return std::make_shared<MutableColumnInt8>();
        case arrow::Type::INT16:
            return std::make_shared<MutableColumnInt16>();
        case arrow::Type::INT32:
            return std::make_shared<MutableColumnInt32>();
        case arrow::Type::INT64:
            return std::make_shared<MutableColumnInt64>();

        case arrow::Type::FLOAT:
            return std::make_shared<MutableColumnFloat32>();
        case arrow::Type::DOUBLE:
            return std::make_shared<MutableColumnFloat64>();

        case arrow::Type::FIXED_SIZE_BINARY:
            return std::make_shared<MutableColumnFixedString>(type);

        case arrow::Type::BINARY:
            return std::make_shared<MutableColumnBinary>();
        case arrow::Type::STRING:
            return std::make_shared<MutableColumnString>();

        case arrow::Type::TIMESTAMP:
            return std::make_shared<MutableColumnTimestamp>(type, arrow::default_memory_pool());
        case arrow::Type::DURATION:
            return std::make_shared<MutableColumnDuration>(type, arrow::default_memory_pool());
        case arrow::Type::DECIMAL:
            return std::make_shared<MutableColumnDecimal>(type, arrow::default_memory_pool());

        case arrow::Type::EXTENSION: // AggregateColumn
            break; // TODO: do we really need it here?

        default:
            break;
    }

    throw Exception(std::string(__FUNCTION__) + " unexpected type " + type->ToString());
}

uint32_t fixedContiguousSize(const DataTypePtr & type)
{
    switch (type->id())
    {
        case arrow::Type::UINT8:
            return 1;
        case arrow::Type::UINT16:
            return 2;
        case arrow::Type::UINT32:
            return 4;
        case arrow::Type::UINT64:
            return 8;
        case arrow::Type::INT8:
            return 1;
        case arrow::Type::INT16:
            return 2;
        case arrow::Type::INT32:
            return 4;
        case arrow::Type::INT64:
            return 8;
        case arrow::Type::FLOAT:
            return 4;
        case arrow::Type::DOUBLE:
            return 8;

        case arrow::Type::FIXED_SIZE_BINARY:
            return std::static_pointer_cast<DataTypeFixedString>(type)->byte_width();

        case arrow::Type::STRING:
        case arrow::Type::BINARY:
            break;

        case arrow::Type::TIMESTAMP:
            return 8;
        case arrow::Type::DURATION:
            return 8;
        case arrow::Type::DECIMAL:
            return std::static_pointer_cast<DataTypeDecimal>(type)->byte_width();

        case arrow::Type::EXTENSION: // AggregateColumn
            break;

        default:
            break;
    }

    return 0;
}

}

#pragma once

#include <yt/yt/core/test_framework/framework.h>


#include <yt/yt/client/table_client/columnar.h>
#include <yt/yt/client/table_client/config.h>
#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/row_batch.h>

#include <yt_proto/yt/client/table_chunk_format/proto/column_meta.pb.h>

#include <iostream>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

void ExpectSchemafulRowsEqual(TUnversionedRow expected, TUnversionedRow actual);

void ExpectSchemalessRowsEqual(TUnversionedRow expected, TUnversionedRow actual, int keyColumnCount);

void ExpectSchemafulRowsEqual(TVersionedRow expected, TVersionedRow actual);

void CheckResult(std::vector<TVersionedRow> expected, IVersionedReaderPtr reader, bool filterOutNullRows = false);

template <class TExpectedRow, class TActualRow>
void CheckSchemafulResult(const std::vector<TExpectedRow>& expected, const std::vector<TActualRow>& actual)
{
    ASSERT_EQ(expected.size(), actual.size());
    for (int i = 0; i < std::ssize(expected); ++i) {
        ExpectSchemafulRowsEqual(expected[i], actual[i]);
    }
}

template <class TExpectedRow, class TActualRow>
void CheckSchemalessResult(
    TRange<TExpectedRow> expected,
    TRange<TActualRow> actual,
    int keyColumnCount)
{
    ASSERT_EQ(expected.size(), actual.size());
    for (int i = 0; i < std::ssize(expected); ++i) {
        ExpectSchemalessRowsEqual(expected[i], actual[i], keyColumnCount);
    }
}

template <class TRow, class TReader>
void CheckSchemalessResult(
    const std::vector<TRow>& expected,
    TIntrusivePtr<TReader> reader,
    int keyColumnCount,
    TRowBatchReadOptions options = {})
{
    size_t offset = 0;
    while (auto batch = reader->Read(options)) {
        auto actual = batch->MaterializeRows();
        if (actual.empty()) {
            ASSERT_TRUE(reader->GetReadyEvent().Get().IsOK());
            continue;
        }

        i64 dataWeight = 0;
        for (int i = 0; i < std::ssize(actual) - 1; i++) {
            dataWeight += GetDataWeight(actual[i]);
        }

        ASSERT_LT(dataWeight, options.MaxDataWeightPerRead);
        ASSERT_LE(std::ssize(actual), options.MaxRowsPerRead);

        CheckSchemalessResult(
            MakeRange(expected).Slice(offset, std::min(expected.size(), offset + actual.size())),
            actual,
            keyColumnCount);
        offset += actual.size();
    }

    ASSERT_EQ(offset, expected.size());
}

std::vector<std::pair<ui32, ui32>> GetTimestampIndexRanges(
    TRange<NTableClient::TVersionedRow> rows,
    NTableClient::TTimestamp timestamp);

template <class T>
void AppendVector(std::vector<T>* data, const std::vector<T> toAppend)
{
    data->insert(data->end(), toAppend.begin(), toAppend.end());
}

template <class T>
TRange<T> GetTypedData(const NTableClient::IUnversionedColumnarRowBatch::TValueBuffer& buffer)
{
    return MakeRange(
        reinterpret_cast<const T*>(buffer.Data.Begin()),
        reinterpret_cast<const T*>(buffer.Data.End()));
}

inline bool GetBit(const NTableClient::IUnversionedColumnarRowBatch::TValueBuffer& buffer, int index)
{
    return (buffer.Data[index / 8] & (1 << (index % 8))) != 0;
}

inline bool GetBit(TRef data, int index)
{
    return (data[index / 8] & (1 << (index % 8))) != 0;
}

inline bool GetBit(const NTableClient::IUnversionedColumnarRowBatch::TBitmap& bitmap, int index)
{
    return GetBit(bitmap.Data, index);
}

inline void SetBit(TMutableRef data, int index, bool value)
{
    auto& byte = data[index / 8];
    auto mask = 1ULL << (index % 8);
    if (value) {
        byte |= mask;
    } else {
        byte &= ~mask;
    }
}

inline void ResolveRleEncoding(
    const NTableClient::IUnversionedColumnarRowBatch::TColumn*& column,
    i64& index)
{
    if (!column->Rle) {
        return;
    }

    YT_ASSERT(column->Values->BitWidth == 64);
    YT_ASSERT(!column->Values->ZigZagEncoded);
    auto rleIndexes = GetTypedData<ui64>(*column->Values);
    index = TranslateRleIndex(rleIndexes, index);
    column = column->Rle->ValueColumn;
}

inline bool IsColumnValueNull(
    const NTableClient::IUnversionedColumnarRowBatch::TColumn* column,
    int index)
{
    return column->NullBitmap && GetBit(*column->NullBitmap, index);
}

inline bool ResolveDictionaryEncoding(
    const NTableClient::IUnversionedColumnarRowBatch::TColumn*& column,
    i64& index)
{
    if (!column->Dictionary) {
        return true;
    }

    const auto& dictionary = *column->Dictionary;
    YT_ASSERT(dictionary.ZeroMeansNull);
    YT_ASSERT(column->Values->BitWidth == 32);
    YT_ASSERT(!column->Values->ZigZagEncoded);
    index = static_cast<i64>(GetTypedData<ui32>(*column->Values)[index]) - 1;
    if (index < 0) {
        return false;
    }
    column = column->Dictionary->ValueColumn;
    return true;
}

inline TStringBuf DecodeStringFromColumn(
    const NTableClient::IUnversionedColumnarRowBatch::TColumn& column,
    i64 index)
{
    const auto& strings = *column.Strings;
    YT_ASSERT(strings.AvgLength);
    YT_ASSERT(column.Values->BitWidth == 32);
    YT_ASSERT(column.Values->ZigZagEncoded);

    auto getOffset = [&] (i64 index) {
        return  (index == 0)
            ? 0
            : *strings.AvgLength * index + ZigZagDecode64(GetTypedData<ui32>(*column.Values)[index - 1]);
    };

    i64 offset = getOffset(index);
    i64 nextOffset = getOffset(index + 1);
    return TStringBuf(strings.Data.Begin() + offset, strings.Data.Begin() + nextOffset);
}

template <class T>
T DecodeIntegerFromColumn(
    const NTableClient::IUnversionedColumnarRowBatch::TColumn& column,
    i64 index)
{
    YT_ASSERT(column.Values->BitWidth == 64);
    auto value = GetTypedData<ui64>(*column.Values)[index];
    value += column.Values->BaseValue;
    if (column.Values->ZigZagEncoded) {
        value = static_cast<ui64>(ZigZagDecode64(value));
    }
    return static_cast<T>(value);
}

template <typename T>
T DecodeFloatingPointFromColumn(
    const NTableClient::IUnversionedColumnarRowBatch::TColumn& column,
    i64 index)
{
    static_assert(std::is_floating_point_v<T>);

    YT_ASSERT(column.Values->BitWidth == sizeof(T) * 8);
    return GetTypedData<T>(*column.Values)[index];
}

inline bool DecodeBoolFromColumn(
    const NTableClient::IUnversionedColumnarRowBatch::TColumn& column,
    i64 index)
{
    YT_ASSERT(column.Values->BitWidth == 1);
    return GetBit(*column.Values, index);
}

std::vector<TUnversionedRow> CreateFilteredRangedRows(
    const std::vector<TUnversionedRow>& initial,
    TNameTablePtr writeNameTable,
    TNameTablePtr readNameTable,
    TColumnFilter columnFilter,
    NChunkClient::TLegacyReadRange readRange,
    TChunkedMemoryPool* pool,
    int keyColumnCount);

void PrintTo(const TColumnarStatistics& statistics, std::ostream* os);

NTableChunkFormat::NProto::TSegmentMeta CreateSimpleSegmentMeta();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient


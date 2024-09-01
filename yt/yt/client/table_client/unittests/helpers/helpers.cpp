#include "helpers.h"

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/table_client/columnar_statistics.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/versioned_reader.h>

namespace NYT::NTableClient {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

void CheckEqual(const TUnversionedValue& expected, const TUnversionedValue& actual)
{
    // Fast path.
    if (TBitwiseUnversionedValueEqual()(expected, actual)) {
        return;
    }

    SCOPED_TRACE(Format("Expected: %v; Actual: %v", expected, actual));
    ASSERT_TRUE(TBitwiseUnversionedValueEqual()(expected, actual));
}

void CheckEqual(const TVersionedValue& expected, const TVersionedValue& actual)
{
    // Fast path.
    if (TBitwiseVersionedValueEqual()(expected, actual)) {
        return;
    }

    SCOPED_TRACE(Format("Expected: %v; Actual: %v", expected, actual));
    ASSERT_TRUE(TBitwiseVersionedValueEqual()(expected, actual));
}

void ExpectSchemafulRowsEqual(TUnversionedRow expected, TUnversionedRow actual)
{
    // Fast path.
    if (TBitwiseUnversionedRowEqual()(expected, actual)) {
        return;
    }

    SCOPED_TRACE(Format("Expected: %v; Actual: %v", expected, actual));

    ASSERT_EQ(static_cast<bool>(expected), static_cast<bool>(actual));
    if (!expected || !actual) {
        return;
    }
    ASSERT_EQ(expected.GetCount(), actual.GetCount());

    for (int valueIndex = 0; valueIndex < static_cast<int>(expected.GetCount()); ++valueIndex) {
        SCOPED_TRACE(Format("Value index %v", valueIndex));
        CheckEqual(expected[valueIndex], actual[valueIndex]);
    }
}

void ExpectSchemalessRowsEqual(TUnversionedRow expected, TUnversionedRow actual, int keyColumnCount)
{
    // Fast path.
    if (TBitwiseUnversionedRowEqual()(expected, actual)) {
        return;
    }

    SCOPED_TRACE(Format("Expected: %v; Actual: %v", expected, actual));

    ASSERT_EQ(static_cast<bool>(expected), static_cast<bool>(actual));
    if (!expected || !actual) {
        return;
    }
    ASSERT_EQ(expected.GetCount(), actual.GetCount());

    for (int valueIndex = 0; valueIndex < keyColumnCount; ++valueIndex) {
        SCOPED_TRACE(Format("Value index %v", valueIndex));
        CheckEqual(expected[valueIndex], actual[valueIndex]);
    }

    for (int valueIndex = keyColumnCount; valueIndex < static_cast<int>(expected.GetCount()); ++valueIndex) {
        SCOPED_TRACE(Format("Value index %v", valueIndex));

        // Find value with the same id. Since this in schemaless read, value positions can be different.
        bool found = false;
        for (int index = keyColumnCount; index < static_cast<int>(expected.GetCount()); ++index) {
            if (expected[valueIndex].Id == actual[index].Id) {
                CheckEqual(expected[valueIndex], actual[index]);
                found = true;
                break;
            }
        }
        ASSERT_TRUE(found);
    }
}

void ExpectSchemafulRowsEqual(TVersionedRow expected, TVersionedRow actual)
{
    // Fast path.
    if (TBitwiseVersionedRowEqual()(expected, actual)) {
        return;
    }

    SCOPED_TRACE(Format("Expected: %v; Actual: %v", expected, actual));

    ASSERT_EQ(static_cast<bool>(expected), static_cast<bool>(actual));
    if (!expected || !actual) {
        return;
    }

    ASSERT_EQ(expected.GetWriteTimestampCount(), actual.GetWriteTimestampCount());
    for (int i = 0; i < expected.GetWriteTimestampCount(); ++i) {
        SCOPED_TRACE(Format("Write Timestamp %v", i));
        ASSERT_EQ(expected.WriteTimestamps()[i], actual.WriteTimestamps()[i]);
    }

    ASSERT_EQ(expected.GetDeleteTimestampCount(), actual.GetDeleteTimestampCount());
    for (int i = 0; i < expected.GetDeleteTimestampCount(); ++i) {
        SCOPED_TRACE(Format("Delete Timestamp %v", i));
        ASSERT_EQ(expected.DeleteTimestamps()[i], actual.DeleteTimestamps()[i]);
    }

    ASSERT_EQ(expected.GetKeyCount(), actual.GetKeyCount());
    for (int index = 0; index < expected.GetKeyCount(); ++index) {
        SCOPED_TRACE(Format("Key index %v", index));
        CheckEqual(expected.Keys()[index], actual.Keys()[index]);
    }

    ASSERT_EQ(expected.GetValueCount(), actual.GetValueCount());
    for (int index = 0; index < expected.GetValueCount(); ++index) {
        SCOPED_TRACE(Format("Value index %v", index));
        CheckEqual(expected.Values()[index], actual.Values()[index]);
    }
}

void CheckResult(std::vector<TVersionedRow> expected, IVersionedReaderPtr reader, bool filterOutNullRows)
{
    auto it = expected.begin();
    std::vector<TVersionedRow> actual;
    actual.reserve(100);

    while (auto batch = reader->Read({.MaxRowsPerRead = 20})) {
        if (batch->IsEmpty()) {
            ASSERT_TRUE(reader->GetReadyEvent().Get().IsOK());
            continue;
        }

        auto range = batch->MaterializeRows();
        std::vector<TVersionedRow> actual(range.begin(), range.end());

        if (filterOutNullRows) {
            actual.erase(
                std::remove_if(
                    actual.begin(),
                    actual.end(),
                    [] (TVersionedRow row) {
                        return !row;
                    }),
                actual.end());
        }

        std::vector<TVersionedRow> ex(it, std::min(it + actual.size(), expected.end()));

        CheckSchemafulResult(ex, actual);
        it += ex.size();
    }

    ASSERT_TRUE(it == expected.end());
}

std::vector<std::pair<ui32, ui32>> GetTimestampIndexRanges(
    TRange<TVersionedRow> rows,
    TTimestamp timestamp)
{
    std::vector<std::pair<ui32, ui32>> indexRanges;
    for (auto row : rows) {
        // Find delete timestamp.
        auto deleteTimestamp = NTableClient::NullTimestamp;
        for (auto currentTimestamp : row.DeleteTimestamps()) {
            if (currentTimestamp <= timestamp) {
                deleteTimestamp = std::max(currentTimestamp, deleteTimestamp);
            }
        }

        int lowerTimestampIndex = 0;
        while (lowerTimestampIndex < row.GetWriteTimestampCount() &&
               row.WriteTimestamps()[lowerTimestampIndex] > timestamp)
        {
            ++lowerTimestampIndex;
        }

        int upperTimestampIndex = lowerTimestampIndex;
        while (upperTimestampIndex < row.GetWriteTimestampCount() &&
               row.WriteTimestamps()[upperTimestampIndex] > deleteTimestamp)
        {
            ++upperTimestampIndex;
        }

        indexRanges.push_back(std::pair(lowerTimestampIndex, upperTimestampIndex));
    }
    return indexRanges;
}

std::vector<TUnversionedRow> CreateFilteredRangedRows(
    const std::vector<TUnversionedRow>& initial,
    TNameTablePtr writeNameTable,
    TNameTablePtr readNameTable,
    TColumnFilter columnFilter,
    TLegacyReadRange readRange,
    TChunkedMemoryPool* pool,
    int keyColumnCount)
{
    std::vector<TUnversionedRow> rows;

    int lowerRowIndex = 0;
    if (readRange.LowerLimit().HasRowIndex()) {
        lowerRowIndex = readRange.LowerLimit().GetRowIndex();
    }

    int upperRowIndex = initial.size();
    if (readRange.UpperLimit().HasRowIndex()) {
        upperRowIndex = readRange.UpperLimit().GetRowIndex();
    }

    auto fulfillLowerKeyLimit = [&] (TUnversionedRow row) {
        return !readRange.LowerLimit().HasLegacyKey() ||
            CompareValueRanges(
                row.FirstNElements(keyColumnCount),
                readRange.LowerLimit().GetLegacyKey().Elements()) >= 0;
    };

    auto fulfillUpperKeyLimit = [&] (TUnversionedRow row) {
        return !readRange.UpperLimit().HasLegacyKey() ||
        CompareValueRanges(
            row.FirstNElements(keyColumnCount),
            readRange.UpperLimit().GetLegacyKey().Elements()) < 0;
    };

    for (int rowIndex = lowerRowIndex; rowIndex < upperRowIndex; ++rowIndex) {
        auto initialRow = initial[rowIndex];
        if (fulfillLowerKeyLimit(initialRow) && fulfillUpperKeyLimit(initialRow)) {
            auto row = TMutableUnversionedRow::Allocate(pool, initialRow.GetCount());
            int count = 0;
            for (const auto* it = initialRow.Begin(); it != initialRow.End(); ++it) {
                auto name = writeNameTable->GetName(it->Id);
                auto readerId = readNameTable->GetId(name);

                if (columnFilter.ContainsIndex(readerId)) {
                    row[count] = *it;
                    row[count].Id = readerId;
                    ++count;
                }
            }
            row.SetCount(count);
            rows.push_back(row);
        }
    }

    return rows;
}

void PrintTo(const TColumnarStatistics& statistics, std::ostream* os)
{
    *os
        << "ColumnDataWeights: "
        << ::testing::PrintToString(statistics.ColumnDataWeights) << "\n"
        << "TimestampTotalWeight: "
        << ::testing::PrintToString(statistics.TimestampTotalWeight) << "\n"
        << "LegacyChunkDataWeight: "
        << ::testing::PrintToString(statistics.LegacyChunkDataWeight) << "\n"
        << "ColumnMinValues: "
        << ::testing::PrintToString(statistics.ColumnMinValues) << "\n"
        << "ColumnMaxValues: "
        << ::testing::PrintToString(statistics.ColumnMaxValues) << "\n"
        << "ColumnNonNullValueCounts: "
        << ::testing::PrintToString(statistics.ColumnNonNullValueCounts) << "\n"
        << "ChunkRowCount: "
        << ::testing::PrintToString(statistics.ChunkRowCount) << "\n"
        << "LegacyChunkRowCount: "
        << ::testing::PrintToString(statistics.LegacyChunkRowCount) << "\n";

    if (!statistics.LargeStatistics.Empty()) {
        *os << "ColumnHyperLogLogDigests: [\n";
        for (const auto& hyperLogLog : statistics.LargeStatistics.ColumnHyperLogLogDigests) {
            *os << "    ";
            *os << ToString(hyperLogLog);
            *os << "\n";
        }
        *os << "]\n";
    }
}

NTableChunkFormat::NProto::TSegmentMeta CreateSimpleSegmentMeta()
{
    NTableChunkFormat::NProto::TSegmentMeta segmentMeta;
    segmentMeta.set_version(1);
    segmentMeta.set_type(2);
    segmentMeta.set_row_count(3);
    segmentMeta.set_block_index(4);
    segmentMeta.set_offset(5);
    segmentMeta.set_chunk_row_count(6);
    segmentMeta.set_size(7);

    {
        auto* meta = segmentMeta.MutableExtension(NTableChunkFormat::NProto::TTimestampSegmentMeta::timestamp_segment_meta);
        meta->set_min_timestamp(0);
        meta->set_expected_writes_per_row(0);
        meta->set_expected_deletes_per_row(0);
    }

    {
        auto* meta = segmentMeta.MutableExtension(NTableChunkFormat::NProto::TIntegerSegmentMeta::integer_segment_meta);
        meta->set_min_value(0);
    }

    {
        auto* meta = segmentMeta.MutableExtension(NTableChunkFormat::NProto::TStringSegmentMeta::string_segment_meta);
        meta->set_expected_length(0);
    }

    {
        auto* meta = segmentMeta.MutableExtension(NTableChunkFormat::NProto::TDenseVersionedSegmentMeta::dense_versioned_segment_meta);
        meta->set_expected_values_per_row(0);
    }

    {
        auto* meta = segmentMeta.MutableExtension(NTableChunkFormat::NProto::TSchemalessSegmentMeta::schemaless_segment_meta);
        meta->set_expected_bytes_per_row(0);
    }
    return segmentMeta;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

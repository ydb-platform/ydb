#include "columnshard__stats_scan.h"

namespace NKikimr::NColumnShard {

NKikimr::NOlap::TPartialReadResult TStatsIterator::GetBatch() {
    // Take next raw batch
    auto batch = FillStatsBatch();

    // Extract the last row's PK
    auto keyBatch = NArrow::ExtractColumns(batch, KeySchema);
    auto lastKey = keyBatch->Slice(keyBatch->num_rows() - 1, 1);

    ApplyRangePredicates(batch);

    // Leave only requested columns
    auto resultBatch = NArrow::ExtractColumns(batch, ResultSchema);

    NOlap::TPartialReadResult out{
        .ResultBatch = std::move(resultBatch),
        .LastReadKey = std::move(lastKey)
    };

    if (ReadMetadata->Program) {
        auto status = ApplyProgram(out.ResultBatch, *ReadMetadata->Program, NArrow::GetCustomExecContext());
        if (!status.ok()) {
            out.ErrorString = status.message();
        }
    }
    return out;
}

std::shared_ptr<arrow::RecordBatch> TStatsIterator::FillStatsBatch() {
    ui64 numRows = 0;
    numRows += NUM_KINDS * IndexStats.size();

    TVector<ui32> allColumnIds;
    for (const auto& c : PrimaryIndexStatsSchema.Columns) {
        allColumnIds.push_back(c.second.Id);
    }
    std::sort(allColumnIds.begin(), allColumnIds.end());
    auto schema = NOlap::MakeArrowSchema(PrimaryIndexStatsSchema.Columns, allColumnIds);
    auto builders = NArrow::MakeBuilders(schema, numRows);

    while (!IndexStats.empty()) {
        auto it = Reverse ? std::prev(IndexStats.end()) : IndexStats.begin();
        const auto& stats = it->second;
        Y_VERIFY(stats);
        AppendStats(builders, it->first, *stats);
        IndexStats.erase(it);
    }

    auto columns = NArrow::Finish(std::move(builders));
    return arrow::RecordBatch::Make(schema, numRows, columns);
}

void TStatsIterator::ApplyRangePredicates(std::shared_ptr<arrow::RecordBatch>& batch) {
    NArrow::TColumnFilter filter = ReadMetadata->GetPKRangesFilter().BuildFilter(batch);
    filter.Apply(batch);
}

void TStatsIterator::AppendStats(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, ui64 pathId, const NOlap::TColumnEngineStats& stats) {
    using TUInt64 = arrow::UInt64Type::c_type;
    using TUInt32 = arrow::UInt32Type::c_type;

    TUInt64 pathIds[NUM_KINDS] = { pathId, pathId, pathId, pathId, pathId };
    /// It's in sync with TPortionMeta::EProduced
    TUInt32 kinds[NUM_KINDS] = {
        (ui32)NOlap::TPortionMeta::INSERTED,
        (ui32)NOlap::TPortionMeta::COMPACTED,
        (ui32)NOlap::TPortionMeta::SPLIT_COMPACTED,
        (ui32)NOlap::TPortionMeta::INACTIVE,
        (ui32)NOlap::TPortionMeta::EVICTED
    };
    ui64 tabletId = ReadMetadata->TabletId;
    TUInt64 tabletIds[NUM_KINDS] = { tabletId, tabletId, tabletId, tabletId, tabletId };
    TUInt64 rows[NUM_KINDS] = {
        (ui64)stats.Inserted.Rows,
        (ui64)stats.Compacted.Rows,
        (ui64)stats.SplitCompacted.Rows,
        (ui64)stats.Inactive.Rows,
        (ui64)stats.Evicted.Rows
    };
    TUInt64 bytes[NUM_KINDS] = {
        (ui64)stats.Inserted.Bytes,
        (ui64)stats.Compacted.Bytes,
        (ui64)stats.SplitCompacted.Bytes,
        (ui64)stats.Inactive.Bytes,
        (ui64)stats.Evicted.Bytes
    };
    TUInt64 rawBytes[NUM_KINDS] = {
        (ui64)stats.Inserted.RawBytes,
        (ui64)stats.Compacted.RawBytes,
        (ui64)stats.SplitCompacted.RawBytes,
        (ui64)stats.Inactive.RawBytes,
        (ui64)stats.Evicted.RawBytes
    };
    TUInt64 portions[NUM_KINDS] = {
        (ui64)stats.Inserted.Portions,
        (ui64)stats.Compacted.Portions,
        (ui64)stats.SplitCompacted.Portions,
        (ui64)stats.Inactive.Portions,
        (ui64)stats.Evicted.Portions
    };
    TUInt64 blobs[NUM_KINDS] = {
        (ui64)stats.Inserted.Blobs,
        (ui64)stats.Compacted.Blobs,
        (ui64)stats.SplitCompacted.Blobs,
        (ui64)stats.Inactive.Blobs,
        (ui64)stats.Evicted.Blobs
    };

    if (Reverse) {
        std::reverse(std::begin(pathIds), std::end(pathIds));
        std::reverse(std::begin(kinds), std::end(kinds));
        std::reverse(std::begin(tabletIds), std::end(tabletIds));
        std::reverse(std::begin(rows), std::end(rows));
        std::reverse(std::begin(bytes), std::end(bytes));
        std::reverse(std::begin(rawBytes), std::end(rawBytes));
        std::reverse(std::begin(portions), std::end(portions));
        std::reverse(std::begin(blobs), std::end(blobs));
    }

    NArrow::Append<arrow::UInt64Type>(*builders[0], pathIds, NUM_KINDS);
    NArrow::Append<arrow::UInt32Type>(*builders[1], kinds, NUM_KINDS);
    NArrow::Append<arrow::UInt64Type>(*builders[2], tabletIds, NUM_KINDS);
    NArrow::Append<arrow::UInt64Type>(*builders[3], rows, NUM_KINDS);
    NArrow::Append<arrow::UInt64Type>(*builders[4], bytes, NUM_KINDS);
    NArrow::Append<arrow::UInt64Type>(*builders[5], rawBytes, NUM_KINDS);
    NArrow::Append<arrow::UInt64Type>(*builders[6], portions, NUM_KINDS);
    NArrow::Append<arrow::UInt64Type>(*builders[7], blobs, NUM_KINDS);
}

}


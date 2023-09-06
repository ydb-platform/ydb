#include "columnshard__stats_scan.h"

namespace NKikimr::NColumnShard {

std::optional<NOlap::TPartialReadResult> TStatsIterator::GetBatch() {
    // Take next raw batch
    auto batch = FillStatsBatch();

    // Extract the last row's PK
    auto keyBatch = NArrow::ExtractColumns(batch, KeySchema);
    auto lastKey = keyBatch->Slice(keyBatch->num_rows() - 1, 1);

    ApplyRangePredicates(batch);

    // Leave only requested columns
    auto resultBatch = NArrow::ExtractColumns(batch, ResultSchema);

    NOlap::TPartialReadResult out(nullptr, resultBatch, lastKey);

    out.ApplyProgram(ReadMetadata->GetProgram());
    return std::move(out);
}

std::shared_ptr<arrow::RecordBatch> TStatsIterator::FillStatsBatch() {
    ui64 numRows = 0;
    numRows += NOlap::TColumnEngineStats::GetRecordsCount() * IndexStats.size();

    std::vector<ui32> allColumnIds;
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
    auto kinds = stats.GetKinds();
    auto pathIds = stats.GetConstValues<arrow::UInt64Type::c_type>(pathId);
    auto tabletIds = stats.GetConstValues<arrow::UInt64Type::c_type>(ReadMetadata->TabletId);
    auto rows = stats.GetRowsValues();
    auto bytes = stats.GetBytesValues();
    auto rawBytes = stats.GetRawBytesValues();
    auto portions = stats.GetPortionsValues();
    auto blobs = stats.GetBlobsValues();

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

    NArrow::Append<arrow::UInt64Type>(*builders[0], pathIds);
    NArrow::Append<arrow::UInt32Type>(*builders[1], kinds);
    NArrow::Append<arrow::UInt64Type>(*builders[2], tabletIds);
    NArrow::Append<arrow::UInt64Type>(*builders[3], rows);
    NArrow::Append<arrow::UInt64Type>(*builders[4], bytes);
    NArrow::Append<arrow::UInt64Type>(*builders[5], rawBytes);
    NArrow::Append<arrow::UInt64Type>(*builders[6], portions);
    NArrow::Append<arrow::UInt64Type>(*builders[7], blobs);
}

}


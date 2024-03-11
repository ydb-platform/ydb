#include "columnshard__stats_scan.h"

namespace NKikimr::NColumnShard {

std::optional<NOlap::TPartialReadResult> TStatsIterator::GetBatch() {
    // Take next raw batch
    auto batch = FillStatsBatch();

    // Extract the last row's PK
    auto keyBatch = NArrow::ExtractColumns(batch, KeySchema);
    auto lastKey = keyBatch->Slice(keyBatch->num_rows() - 1, 1);

    ApplyRangePredicates(batch);
    if (!batch->num_rows()) {
        return {};
    }
    // Leave only requested columns
    auto resultBatch = NArrow::ExtractColumns(batch, ResultSchema);
    NArrow::TStatusValidator::Validate(ReadMetadata->GetProgram().ApplyProgram(resultBatch));
    if (!resultBatch->num_rows()) {
        return {};
    }
    NOlap::TPartialReadResult out(resultBatch, lastKey);

    return std::move(out);
}

std::shared_ptr<arrow::RecordBatch> TStatsIterator::FillStatsBatch() {
    std::vector<std::shared_ptr<NOlap::TPortionInfo>> portions;
    ui32 recordsCount = 0;
    while (IndexPortions.size()) {
        auto& i = IndexPortions.front();
        recordsCount += i->Records.size();
        portions.emplace_back(i);
        IndexPortions.pop_front();
        if (recordsCount > 10000) {
            break;
        }
    }
    std::vector<ui32> allColumnIds;
    for (const auto& c : PrimaryIndexStatsSchema.Columns) {
        allColumnIds.push_back(c.second.Id);
    }
    std::sort(allColumnIds.begin(), allColumnIds.end());
    auto schema = NOlap::MakeArrowSchema(PrimaryIndexStatsSchema.Columns, allColumnIds);
    auto builders = NArrow::MakeBuilders(schema, recordsCount);

    for (auto&& p: portions) {
        AppendStats(builders, *p);
    }

    auto columns = NArrow::Finish(std::move(builders));
    return arrow::RecordBatch::Make(schema, recordsCount, columns);
}

void TStatsIterator::ApplyRangePredicates(std::shared_ptr<arrow::RecordBatch>& batch) {
    NArrow::TColumnFilter filter = ReadMetadata->GetPKRangesFilter().BuildFilter(batch);
    filter.Apply(batch);
}

void TStatsIterator::AppendStats(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, const NOlap::TPortionInfo& portion) {
    auto portionSchema = ReadMetadata->GetLoadSchema(portion.GetMinSnapshot());
    {
        std::vector<const NOlap::TColumnRecord*> records;
        for (auto&& r : portion.Records) {
            records.emplace_back(&r);
        }
        if (Reverse) {
            std::reverse(records.begin(), records.end());
        }
        for (auto&& r : records) {
            NArrow::Append<arrow::UInt64Type>(*builders[0], portion.GetPathId());
            const std::string prod = ::ToString(portion.GetMeta().Produced);
            NArrow::Append<arrow::StringType>(*builders[1], prod);
            NArrow::Append<arrow::UInt64Type>(*builders[2], ReadMetadata->TabletId);
            NArrow::Append<arrow::UInt64Type>(*builders[3], r->GetMeta().GetNumRowsVerified());
            NArrow::Append<arrow::UInt64Type>(*builders[4], r->GetMeta().GetRawBytesVerified());
            NArrow::Append<arrow::UInt64Type>(*builders[5], portion.GetPortionId());
            NArrow::Append<arrow::UInt64Type>(*builders[6], r->GetChunkIdx());
            NArrow::Append<arrow::StringType>(*builders[7], ReadMetadata->GetColumnNameDef(r->GetColumnId()).value_or("undefined"));
            NArrow::Append<arrow::UInt32Type>(*builders[8], r->GetColumnId());
            std::string blobIdString = portion.GetBlobId(r->GetBlobRange().GetBlobIdxVerified()).ToStringLegacy();
            NArrow::Append<arrow::StringType>(*builders[9], blobIdString);
            NArrow::Append<arrow::UInt64Type>(*builders[10], r->BlobRange.Offset);
            NArrow::Append<arrow::UInt64Type>(*builders[11], r->BlobRange.Size);
            NArrow::Append<arrow::BooleanType>(*builders[12], !portion.HasRemoveSnapshot() || ReadMetadata->GetRequestSnapshot() < portion.GetRemoveSnapshot());

            const auto tierName = portionSchema->GetIndexInfo().GetEntityStorageId(r->GetColumnId(), portion.GetMeta().GetTierName());
            std::string strTierName(tierName.data(), tierName.size());
            NArrow::Append<arrow::StringType>(*builders[13], strTierName);
            NArrow::Append<arrow::StringType>(*builders[14], "COL");
        }
    }
    {
        std::vector<const NOlap::TIndexChunk*> indexes;
        for (auto&& r : portion.GetIndexes()) {
            indexes.emplace_back(&r);
        }
        if (Reverse) {
            std::reverse(indexes.begin(), indexes.end());
        }
        for (auto&& r : indexes) {
            NArrow::Append<arrow::UInt64Type>(*builders[0], portion.GetPathId());
            const std::string prod = ::ToString(portion.GetMeta().Produced);
            NArrow::Append<arrow::StringType>(*builders[1], prod);
            NArrow::Append<arrow::UInt64Type>(*builders[2], ReadMetadata->TabletId);
            NArrow::Append<arrow::UInt64Type>(*builders[3], r->GetRecordsCount());
            NArrow::Append<arrow::UInt64Type>(*builders[4], r->GetRawBytes());
            NArrow::Append<arrow::UInt64Type>(*builders[5], portion.GetPortionId());
            NArrow::Append<arrow::UInt64Type>(*builders[6], r->GetChunkIdx());
            NArrow::Append<arrow::StringType>(*builders[7], ReadMetadata->GetEntityName(r->GetIndexId()).value_or("undefined"));
            NArrow::Append<arrow::UInt32Type>(*builders[8], r->GetIndexId());
            std::string blobIdString = portion.GetBlobId(r->GetBlobRange().GetBlobIdxVerified()).ToStringLegacy();
            NArrow::Append<arrow::StringType>(*builders[9], blobIdString);
            NArrow::Append<arrow::UInt64Type>(*builders[10], r->GetBlobRange().Offset);
            NArrow::Append<arrow::UInt64Type>(*builders[11], r->GetBlobRange().Size);
            NArrow::Append<arrow::BooleanType>(*builders[12], !portion.HasRemoveSnapshot() || ReadMetadata->GetRequestSnapshot() < portion.GetRemoveSnapshot());
            const auto tierName = portionSchema->GetIndexInfo().GetEntityStorageId(r->GetIndexId(), portion.GetMeta().GetTierName());
            std::string strTierName(tierName.data(), tierName.size());
            NArrow::Append<arrow::StringType>(*builders[13], strTierName);
            NArrow::Append<arrow::StringType>(*builders[14], "IDX");
        }
    }
}

}

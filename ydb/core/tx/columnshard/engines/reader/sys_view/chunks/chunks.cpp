#include "chunks.h"
#include <ydb/core/formats/arrow/switch/switch_type.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>

namespace NKikimr::NOlap::NReader::NSysView::NChunks {

void TStatsIterator::AppendStats(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, const TPortionInfo& portion) const {
    auto portionSchema = ReadMetadata->GetLoadSchemaVerified(portion);
    const std::string prod = ::ToString(portion.GetMeta().Produced);
    const bool activity = !portion.IsRemovedFor(ReadMetadata->GetRequestSnapshot());
    {
        std::vector<const TColumnRecord*> records;
        for (auto&& r : portion.Records) {
            records.emplace_back(&r);
        }
        if (Reverse) {
            std::reverse(records.begin(), records.end());
        }
        for (auto&& r : records) {
            NArrow::Append<arrow::UInt64Type>(*builders[0], portion.GetPathId());
            NArrow::Append<arrow::StringType>(*builders[1], prod);
            NArrow::Append<arrow::UInt64Type>(*builders[2], ReadMetadata->TabletId);
            NArrow::Append<arrow::UInt64Type>(*builders[3], r->GetMeta().GetNumRows());
            NArrow::Append<arrow::UInt64Type>(*builders[4], r->GetMeta().GetRawBytes());
            NArrow::Append<arrow::UInt64Type>(*builders[5], portion.GetPortionId());
            NArrow::Append<arrow::UInt64Type>(*builders[6], r->GetChunkIdx());
            NArrow::Append<arrow::StringType>(*builders[7], ReadMetadata->GetColumnNameDef(r->GetColumnId()).value_or("undefined"));
            NArrow::Append<arrow::UInt32Type>(*builders[8], r->GetColumnId());
            std::string blobIdString = portion.GetBlobId(r->GetBlobRange().GetBlobIdxVerified()).ToStringLegacy();
            NArrow::Append<arrow::StringType>(*builders[9], blobIdString);
            NArrow::Append<arrow::UInt64Type>(*builders[10], r->BlobRange.Offset);
            NArrow::Append<arrow::UInt64Type>(*builders[11], r->BlobRange.Size);
            NArrow::Append<arrow::BooleanType>(*builders[12], activity);

            const auto tierName = portionSchema->GetIndexInfo().GetEntityStorageId(r->GetColumnId(), portion.GetMeta().GetTierName());
            std::string strTierName(tierName.data(), tierName.size());
            NArrow::Append<arrow::StringType>(*builders[13], strTierName);
            NArrow::Append<arrow::StringType>(*builders[14], "COL");
        }
    }
    {
        std::vector<const TIndexChunk*> indexes;
        for (auto&& r : portion.GetIndexes()) {
            indexes.emplace_back(&r);
        }
        if (Reverse) {
            std::reverse(indexes.begin(), indexes.end());
        }
        for (auto&& r : indexes) {
            NArrow::Append<arrow::UInt64Type>(*builders[0], portion.GetPathId());
            NArrow::Append<arrow::StringType>(*builders[1], prod);
            NArrow::Append<arrow::UInt64Type>(*builders[2], ReadMetadata->TabletId);
            NArrow::Append<arrow::UInt64Type>(*builders[3], r->GetRecordsCount());
            NArrow::Append<arrow::UInt64Type>(*builders[4], r->GetRawBytes());
            NArrow::Append<arrow::UInt64Type>(*builders[5], portion.GetPortionId());
            NArrow::Append<arrow::UInt64Type>(*builders[6], r->GetChunkIdx());
            NArrow::Append<arrow::StringType>(*builders[7], ReadMetadata->GetEntityName(r->GetIndexId()).value_or("undefined"));
            NArrow::Append<arrow::UInt32Type>(*builders[8], r->GetIndexId());
            if (auto bRange = r->GetBlobRangeOptional()) {
                std::string blobIdString = portion.GetBlobId(bRange->GetBlobIdxVerified()).ToStringLegacy();
                NArrow::Append<arrow::StringType>(*builders[9], blobIdString);
                NArrow::Append<arrow::UInt64Type>(*builders[10], bRange->Offset);
                NArrow::Append<arrow::UInt64Type>(*builders[11], bRange->Size);
            } else if (auto bData = r->GetBlobDataOptional()) {
                NArrow::Append<arrow::StringType>(*builders[9], "INPLACE");
                NArrow::Append<arrow::UInt64Type>(*builders[10], 0);
                NArrow::Append<arrow::UInt64Type>(*builders[11], bData->size());
            }
            NArrow::Append<arrow::BooleanType>(*builders[12], activity);
            const auto tierName = portionSchema->GetIndexInfo().GetEntityStorageId(r->GetIndexId(), portion.GetMeta().GetTierName());
            std::string strTierName(tierName.data(), tierName.size());
            NArrow::Append<arrow::StringType>(*builders[13], strTierName);
            NArrow::Append<arrow::StringType>(*builders[14], "IDX");
        }
    }
}

std::unique_ptr<TScanIteratorBase> TReadStatsMetadata::StartScan(const std::shared_ptr<TReadContext>& readContext) const {
    return std::make_unique<TStatsIterator>(readContext->GetReadMetadataPtrVerifiedAs<TReadStatsMetadata>());
}

std::vector<std::pair<TString, NKikimr::NScheme::TTypeInfo>> TReadStatsMetadata::GetKeyYqlSchema() const {
    return GetColumns(TStatsIterator::StatsSchema, TStatsIterator::StatsSchema.KeyColumns);
}

std::shared_ptr<NKikimr::NOlap::NReader::NSysView::NAbstract::TReadStatsMetadata> TConstructor::BuildMetadata(const NColumnShard::TColumnShard* self, const TReadDescription& read) const {
    auto* index = self->GetIndexOptional();
    return std::make_shared<TReadStatsMetadata>(index ? index->CopyVersionedIndexPtr() : nullptr, self->TabletID(),
        IsReverse ? TReadMetadataBase::ESorting::DESC : TReadMetadataBase::ESorting::ASC,
        read.GetProgram(), index ? index->GetVersionedIndex().GetLastSchema() : nullptr, read.GetSnapshot());
}

bool TStatsIterator::AppendStats(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, NAbstract::TGranuleMetaView& granule) const {
    ui64 recordsCount = 0;
    while (auto portion = granule.PopFrontPortion()) {
        recordsCount += portion->GetRecords().size() + portion->GetIndexes().size();
        AppendStats(builders, *portion);
        if (recordsCount > 10000) {
            break;
        }
    }
    return granule.GetPortions().size();
}

ui32 TStatsIterator::PredictRecordsCount(const NAbstract::TGranuleMetaView& granule) const {
    ui32 recordsCount = 0;
    for (auto&& portion : granule.GetPortions()) {
        recordsCount += portion->GetRecords().size() + portion->GetIndexes().size();
        if (recordsCount > 10000) {
            break;
        }
    }
    return recordsCount;
}

}

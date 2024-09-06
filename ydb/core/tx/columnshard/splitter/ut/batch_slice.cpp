#include "batch_slice.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/splitter/simple.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>

#include <ydb/library/accessor/validator.h>

namespace NKikimr::NOlap {

TBatchSerializedSlice::TBatchSerializedSlice(const std::shared_ptr<arrow::RecordBatch>& batch, NArrow::NSplitter::ISchemaDetailInfo::TPtr schema,
    std::shared_ptr<NColumnShard::TSplitterCounters> counters, const NSplitter::TSplitSettings& settings)
    : TBase(TValidator::CheckNotNull(batch)->num_rows(), schema, counters)
    , Batch(batch) {
    Y_ABORT_UNLESS(batch);
    Data.reserve(batch->num_columns());
    for (auto&& i : batch->schema()->fields()) {
        TSplittedEntity c(schema->GetColumnId(i->name()));
        Data.emplace_back(std::move(c));
    }

    ui32 idx = 0;
    for (auto&& i : batch->columns()) {
        auto& c = Data[idx];
        auto columnSaver = schema->GetColumnSaver(c.GetEntityId());
        auto stats = schema->GetColumnSerializationStats(c.GetEntityId());
        NKikimr::NArrow::NSplitter::TSimpleSplitter splitter(columnSaver);
        splitter.SetStats(stats);
        std::vector<std::shared_ptr<IPortionDataChunk>> chunks;
        for (auto&& i : splitter.Split(i, Schema->GetField(c.GetEntityId()), settings.GetMaxBlobSize())) {
            NOlap::TSimpleColumnInfo columnInfo(c.GetEntityId(), Schema->GetField(c.GetEntityId()),
                Schema->GetColumnSaver(c.GetEntityId()).GetSerializer(), true, false, nullptr);
            chunks.emplace_back(std::make_shared<NOlap::NChunks::TChunkPreparation>(i.GetSerializedChunk(),
                std::make_shared<NArrow::NAccessor::TTrivialArray>(i.GetSlicedBatch()->column(0)), TChunkAddress(c.GetEntityId(), 0),
                columnInfo));
        }
        c.SetChunks(chunks);
        Size += c.GetSize();
        ++idx;
    }
}

std::vector<TBatchSerializedSlice> TBatchSerializedSlice::BuildSimpleSlices(const std::shared_ptr<arrow::RecordBatch>& batch,
    const NSplitter::TSplitSettings& settings, const std::shared_ptr<NColumnShard::TSplitterCounters>& counters,
    const NArrow::NSplitter::ISchemaDetailInfo::TPtr& schemaInfo) {
    std::vector<TBatchSerializedSlice> slices;
    auto stats = schemaInfo->GetBatchSerializationStats(batch);
    ui32 recordsCount = settings.GetMinRecordsCount();
    if (stats) {
        const ui32 recordsCountForMinSize =
            stats->PredictOptimalPackRecordsCount(batch->num_rows(), settings.GetMinBlobSize()).value_or(recordsCount);
        const ui32 recordsCountForMaxPortionSize =
            stats->PredictOptimalPackRecordsCount(batch->num_rows(), settings.GetMaxPortionSize()).value_or(recordsCount);
        recordsCount = std::min(recordsCountForMaxPortionSize, std::max(recordsCount, recordsCountForMinSize));
    }
    auto linearSplitInfo = NKikimr::NArrow::NSplitter::TSimpleSplitter::GetOptimalLinearSplitting(batch->num_rows(), recordsCount);
    for (auto it = linearSplitInfo.StartIterator(); it.IsValid(); it.Next()) {
        std::shared_ptr<arrow::RecordBatch> current = batch->Slice(it.GetPosition(), it.GetCurrentPackSize());
        TBatchSerializedSlice slice(current, schemaInfo, counters, settings);
        slices.emplace_back(std::move(slice));
    }
    return slices;
}

}   // namespace NKikimr::NOlap

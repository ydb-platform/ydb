#include "merger.h"

#include "abstract/merger.h"
#include "plain/logic.h"
#include "sparsed/logic.h"

#include <ydb/core/formats/arrow/reader/merger.h>
#include <ydb/core/formats/arrow/serializer/native.h>
#include <ydb/core/tx/columnshard/splitter/batch_slice.h>

#include <ydb/library/formats/arrow/simple_builder/array.h>
#include <ydb/library/formats/arrow/simple_builder/filler.h>

namespace NKikimr::NOlap::NCompaction {

class TResultBatchPage {
private:
    std::map<ui32, TColumnPortionResult> ColumnChunks;
    std::optional<ui32> RecordsCount;

public:
    void AddColumn(const ui32 columnId, const TColumnPortionResult& data) {
        AFL_VERIFY(ColumnChunks.emplace(columnId, data).second);
        if (!RecordsCount) {
            RecordsCount = data.GetRecordsCount();
        } else {
            AFL_VERIFY(*RecordsCount == data.GetRecordsCount())("new", data.GetRecordsCount())("control", RecordsCount);
        }
    }

    TGeneralSerializedSlice BuildSlice(const std::shared_ptr<TFilteredSnapshotSchema>& resultFiltered,
        const std::shared_ptr<NArrow::NSplitter::TSerializationStats>& stats,
        const std::shared_ptr<NColumnShard::TSplitterCounters>& counters) const {
        std::shared_ptr<TDefaultSchemaDetails> schemaDetails(new TDefaultSchemaDetails(resultFiltered, stats));
        THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>> portionColumns;
        for (auto&& [cId, cPage] : ColumnChunks) {
            portionColumns.emplace(cId, cPage.GetChunks());
        }
        return TGeneralSerializedSlice(portionColumns, schemaDetails, counters);
    }
};

class TResultBatchData {
private:
    std::vector<TResultBatchPage> Pages;

public:
    std::vector<TGeneralSerializedSlice> BuildSlices(const std::shared_ptr<TFilteredSnapshotSchema>& resultFiltered,
        const std::shared_ptr<NArrow::NSplitter::TSerializationStats>& stats, const std::shared_ptr<NColumnShard::TSplitterCounters>& counters) {
        std::vector<TGeneralSerializedSlice> result;
        for (auto&& i : Pages) {
            result.emplace_back(i.BuildSlice(resultFiltered, stats, counters));
        }
        return result;
    }

    void AddColumnChunks(const ui32 columnId, const std::vector<TColumnPortionResult>& chunks) {
        AFL_VERIFY(chunks.size());
        if (Pages.size()) {
            AFL_VERIFY(Pages.size() == chunks.size())("new", chunks.size())("control", Pages.size());
        } else {
            Pages.resize(chunks.size());
        }
        for (ui32 i = 0; i < chunks.size(); ++i) {
            Pages[i].AddColumn(columnId, chunks[i]);
        }
    }
};

std::vector<TWritePortionInfoWithBlobsResult> TMerger::Execute(const std::shared_ptr<NArrow::NSplitter::TSerializationStats>& stats,
    const NArrow::NMerger::TIntervalPositions& checkPoints, const std::shared_ptr<TFilteredSnapshotSchema>& resultFiltered,
    const TInternalPathId pathId, const std::optional<ui64> shardingActualVersion) {
    AFL_VERIFY(Batches.size() == Filters.size());
    std::vector<std::shared_ptr<arrow::RecordBatch>> batchResults;
    {
        arrow::FieldVector indexFields;
        indexFields.emplace_back(IColumnMerger::PortionIdField);
        indexFields.emplace_back(IColumnMerger::PortionRecordIndexField);
        if (resultFiltered->HasColumnId((ui32)IIndexInfo::ESpecialColumn::DELETE_FLAG)) {
            IIndexInfo::AddDeleteFields(indexFields);
        }
        IIndexInfo::AddSnapshotFields(indexFields);
        auto dataSchema = std::make_shared<arrow::Schema>(indexFields);
        NArrow::NMerger::TMergePartialStream mergeStream(
            resultFiltered->GetIndexInfo().GetReplaceKey(), dataSchema, false, IIndexInfo::GetSnapshotColumnNames());

        ui32 idx = 0;
        for (auto&& batch : Batches) {
            {
                NArrow::NConstruction::IArrayBuilder::TPtr column =
                    std::make_shared<NArrow::NConstruction::TSimpleArrayConstructor<NArrow::NConstruction::TIntConstFiller<arrow::UInt16Type>>>(
                        IColumnMerger::PortionIdFieldName, idx);
                batch->AddField(IColumnMerger::PortionIdField, column->BuildArray(batch->num_rows())).Validate();
            }
            {
                NArrow::NConstruction::IArrayBuilder::TPtr column =
                    std::make_shared<NArrow::NConstruction::TSimpleArrayConstructor<NArrow::NConstruction::TIntSeqFiller<arrow::UInt32Type>>>(
                        IColumnMerger::PortionRecordIndexFieldName);
                batch->AddField(IColumnMerger::PortionRecordIndexField, column->BuildArray(batch->num_rows())).Validate();
            }
            mergeStream.AddSource(batch, Filters[idx]);
            ++idx;
        }
        batchResults = mergeStream.DrainAllParts(checkPoints, indexFields);
    }

    std::vector<TResultBatchData> chunkGroups;
    chunkGroups.resize(batchResults.size());

    using TColumnData = std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>;
    THashMap<ui32, TColumnData> columnsData;
    {
        ui32 batchIdx = 0;
        for (auto&& p : Batches) {
            ui32 columnIdx = 0;
            for (auto&& i : p->GetSchema()->GetFields()) {
                const std::optional<ui32> columnId = resultFiltered->GetIndexInfo().GetColumnIdOptional(i->name());
                if (columnId) {
                    auto it = columnsData.find(*columnId);
                    if (it == columnsData.end()) {
                        it = columnsData.emplace(*columnId, TColumnData(Batches.size())).first;
                    }
                    it->second[batchIdx] = p->GetColumnVerified(columnIdx);
                }
                ++columnIdx;
            }
            ++batchIdx;
        }
    }

    TMergingContext mergingContext(batchResults, Batches);

    for (auto&& [columnId, columnData] : columnsData) {
        if (columnId == (ui32)IIndexInfo::ESpecialColumn::WRITE_ID &&
            (!HasAppData() || !AppDataVerified().FeatureFlags.GetEnableInsertWriteIdSpecialColumnCompatibility())) {
            continue;
        }
        const TString& columnName = resultFiltered->GetIndexInfo().GetColumnName(columnId);
        NActors::TLogContextGuard logGuard(NActors::TLogContextBuilder::Build()("field_name", columnName));
        auto columnInfo = stats->GetColumnInfo(columnId);

        TColumnMergeContext commonContext(
            columnId, resultFiltered, NSplitter::TSplitSettings().GetExpectedUnpackColumnChunkRawSize(), columnInfo);
        if (OptimizationWritingPackMode) {
            commonContext.MutableSaver().AddSerializerWithBorder(
                100, std::make_shared<NArrow::NSerialization::TNativeSerializer>(arrow::Compression::type::UNCOMPRESSED));
            commonContext.MutableSaver().AddSerializerWithBorder(
                Max<ui32>(), std::make_shared<NArrow::NSerialization::TNativeSerializer>(arrow::Compression::type::LZ4_FRAME));
        }

        THolder<IColumnMerger> merger =
            IColumnMerger::TFactory::MakeHolder(commonContext.GetLoader()->GetAccessorConstructor().GetClassName(), commonContext);
        AFL_VERIFY(!!merger)("problem", "cannot create merger")(
            "class_name", commonContext.GetLoader()->GetAccessorConstructor().GetClassName());
        merger->Start(columnData, mergingContext);

        ui32 batchIdx = 0;
        for (auto&& batchResult : batchResults) {
            const ui32 portionRecordsCountLimit =
                batchResult->num_rows() / (batchResult->num_rows() / NSplitter::TSplitSettings().GetExpectedRecordsCountOnPage() + 1) + 1;

            TChunkMergeContext context(portionRecordsCountLimit, batchIdx, batchResult->num_rows(), Context.Counters);
            chunkGroups[batchIdx].AddColumnChunks(columnId, merger->Execute(context, mergingContext));
            ++batchIdx;
        }
    }
    ui32 batchIdx = 0;

    const auto groups =
        resultFiltered->GetIndexInfo().GetEntityGroupsByStorageId(IStoragesManager::DefaultStorageId, *SaverContext.GetStoragesManager());
    std::vector<TWritePortionInfoWithBlobsResult> result;
    for (auto&& columnChunks : chunkGroups) {
        auto batchResult = batchResults[batchIdx];
        ++batchIdx;
        std::vector<TGeneralSerializedSlice> batchSlices = columnChunks.BuildSlices(resultFiltered, stats, Context.Counters.SplitterCounters);
        NArrow::NSplitter::TSimilarPacker slicer(PortionExpectedSize);
        auto packs = slicer.Split(batchSlices);

        std::shared_ptr<TDefaultSchemaDetails> schemaDetails(new TDefaultSchemaDetails(resultFiltered, stats));
        ui32 recordIdx = 0;
        for (auto&& i : packs) {
            TGeneralSerializedSlice slicePrimary(std::move(i));
            auto dataWithSecondary =
                resultFiltered->GetIndexInfo()
                    .AppendIndexes(slicePrimary.GetPortionChunksToHash(), SaverContext.GetStoragesManager(), slicePrimary.GetRecordsCount())
                    .DetachResult();
            TGeneralSerializedSlice slice(dataWithSecondary.GetExternalData(), schemaDetails, Context.Counters.SplitterCounters);

            auto b = batchResult->Slice(recordIdx, slice.GetRecordsCount());
            const ui32 deletionsCount = IIndexInfo::CalcDeletions(b, false);
            auto constructor = TWritePortionInfoWithBlobsConstructor::BuildByBlobs(slice.GroupChunksByBlobs(groups),
                dataWithSecondary.GetSecondaryInplaceData(), pathId, resultFiltered->GetVersion(), resultFiltered->GetSnapshot(),
                SaverContext.GetStoragesManager(), EPortionType::Compacted);

            NArrow::TFirstLastSpecialKeys primaryKeys(slice.GetFirstLastPKBatch(resultFiltered->GetIndexInfo().GetReplaceKey()));
            NArrow::TMinMaxSpecialKeys snapshotKeys(b, TIndexInfo::ArrowSchemaSnapshot());
            constructor.GetPortionConstructor().MutablePortionConstructor().AddMetadata(
                *resultFiltered, deletionsCount, primaryKeys, snapshotKeys);
            constructor.GetPortionConstructor().MutablePortionConstructor().MutableMeta().SetTierName(IStoragesManager::DefaultStorageId);
            if (shardingActualVersion) {
                constructor.GetPortionConstructor().MutablePortionConstructor().SetShardingVersion(*shardingActualVersion);
            }
            result.emplace_back(std::move(constructor));
            recordIdx += slice.GetRecordsCount();
        }
    }
    return result;
}

}   // namespace NKikimr::NOlap::NCompaction

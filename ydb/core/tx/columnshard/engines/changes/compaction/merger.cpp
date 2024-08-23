#include "merger.h"

#include "abstract/merger.h"
#include "plain/logic.h"
#include "sparsed/logic.h"

#include <ydb/core/formats/arrow/reader/merger.h>
#include <ydb/core/formats/arrow/serializer/native.h>
#include <ydb/core/formats/arrow/simple_builder/array.h>
#include <ydb/core/formats/arrow/simple_builder/filler.h>
#include <ydb/core/tx/columnshard/splitter/batch_slice.h>

namespace NKikimr::NOlap::NCompaction {

std::vector<TWritePortionInfoWithBlobsResult> TMerger::Execute(const std::shared_ptr<NArrow::NSplitter::TSerializationStats>& stats,
    const NArrow::NMerger::TIntervalPositions& checkPoints, const std::shared_ptr<TFilteredSnapshotSchema>& resultFiltered, const ui64 pathId,
    const std::optional<ui64> shardingActualVersion) {
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
//            AFL_VERIFY(batch->GetColumnsCount() == resultFiltered->GetColumnsCount())("data", batch->GetColumnsCount())(
//                                                       "schema", resultFiltered->GetColumnsCount());
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

    std::vector<std::map<ui32, std::vector<TColumnPortionResult>>> chunkGroups;
    chunkGroups.resize(batchResults.size());
    for (auto&& columnId : resultFiltered->GetColumnIds()) {
        const TString& columnName = resultFiltered->GetIndexInfo().GetColumnName(columnId);
        NActors::TLogContextGuard logGuard(
            NActors::TLogContextBuilder::Build()("field_name", columnName));
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

        bool foundColumn = false;
        {
            std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> parts;
            for (auto&& p : Batches) {
                parts.emplace_back(p->GetAccessorByNameOptional(columnName));
                if (parts.back()) {
                    foundColumn = true;
                }
            }

            merger->Start(parts);
        }
        if (!foundColumn) {
            continue;
        }
        std::map<std::string, std::vector<NCompaction::TColumnPortionResult>> columnChunks;
        ui32 batchIdx = 0;
        for (auto&& batchResult : batchResults) {
            const ui32 portionRecordsCountLimit =
                batchResult->num_rows() / (batchResult->num_rows() / NSplitter::TSplitSettings().GetExpectedRecordsCountOnPage() + 1) + 1;

            TChunkMergeContext context(portionRecordsCountLimit);

            chunkGroups[batchIdx][columnId] = merger->Execute(context, batchResult);
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
        Y_ABORT_UNLESS(columnChunks.size());

        for (auto&& i : columnChunks) {
            if (i.second.size() != columnChunks.begin()->second.size()) {
                for (ui32 p = 0; p < std::min<ui32>(columnChunks.begin()->second.size(), i.second.size()); ++p) {
                    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("p_first", columnChunks.begin()->second[p].DebugString())(
                        "p", i.second[p].DebugString());
                }
            }
            AFL_VERIFY(i.second.size() == columnChunks.begin()->second.size())("first", columnChunks.begin()->second.size())(
                                              "current", i.second.size())("first_name", columnChunks.begin()->first)("current_name", i.first);
        }
        auto columnSnapshotPlanStepIdx = batchResult->GetColumnByName(TIndexInfo::SPEC_COL_PLAN_STEP);
        auto columnSnapshotTxIdx = batchResult->GetColumnByName(TIndexInfo::SPEC_COL_TX_ID);
        Y_ABORT_UNLESS(columnSnapshotPlanStepIdx);
        Y_ABORT_UNLESS(columnSnapshotTxIdx);
        Y_ABORT_UNLESS(columnSnapshotPlanStepIdx->type_id() == arrow::UInt64Type::type_id);
        Y_ABORT_UNLESS(columnSnapshotTxIdx->type_id() == arrow::UInt64Type::type_id);

        std::vector<TGeneralSerializedSlice> batchSlices;
        std::shared_ptr<TDefaultSchemaDetails> schemaDetails(new TDefaultSchemaDetails(resultFiltered, stats));

        for (ui32 i = 0; i < columnChunks.begin()->second.size(); ++i) {
            THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>> portionColumns;
            for (auto&& p : columnChunks) {
                portionColumns.emplace(p.first, p.second[i].GetChunks());
            }
            batchSlices.emplace_back(portionColumns, schemaDetails, Context.Counters.SplitterCounters);
        }
        NArrow::NSplitter::TSimilarPacker slicer(NSplitter::TSplitSettings().GetExpectedPortionSize());
        auto packs = slicer.Split(batchSlices);

        ui32 recordIdx = 0;
        for (auto&& i : packs) {
            TGeneralSerializedSlice slicePrimary(std::move(i));
            auto dataWithSecondary = resultFiltered->GetIndexInfo()
                                         .AppendIndexes(slicePrimary.GetPortionChunksToHash(), SaverContext.GetStoragesManager())
                                         .DetachResult();
            TGeneralSerializedSlice slice(dataWithSecondary.GetExternalData(), schemaDetails, Context.Counters.SplitterCounters);

            auto b = batchResult->Slice(recordIdx, slice.GetRecordsCount());
            const ui32 deletionsCount = IIndexInfo::CalcDeletions(b, false);
            auto constructor = TWritePortionInfoWithBlobsConstructor::BuildByBlobs(slice.GroupChunksByBlobs(groups),
                dataWithSecondary.GetSecondaryInplaceData(), pathId, resultFiltered->GetVersion(), resultFiltered->GetSnapshot(),
                SaverContext.GetStoragesManager());

            NArrow::TFirstLastSpecialKeys primaryKeys(slice.GetFirstLastPKBatch(resultFiltered->GetIndexInfo().GetReplaceKey()));
            NArrow::TMinMaxSpecialKeys snapshotKeys(b, TIndexInfo::ArrowSchemaSnapshot());
            constructor.GetPortionConstructor().AddMetadata(*resultFiltered, deletionsCount, primaryKeys, snapshotKeys);
            constructor.GetPortionConstructor().MutableMeta().SetTierName(IStoragesManager::DefaultStorageId);
            if (shardingActualVersion) {
                constructor.GetPortionConstructor().SetShardingVersion(*shardingActualVersion);
            }
            result.emplace_back(std::move(constructor));
            recordIdx += slice.GetRecordsCount();
        }
    }
    return result;
}

}   // namespace NKikimr::NOlap::NCompaction

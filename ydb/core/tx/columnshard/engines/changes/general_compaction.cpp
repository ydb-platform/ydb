
#include "general_compaction.h"

#include "compaction/column_cursor.h"
#include "compaction/column_portion_chunk.h"
#include "compaction/merge_context.h"
#include "compaction/merged_column.h"
#include "counters/general.h"

#include <ydb/core/formats/arrow/simple_builder/array.h>
#include <ydb/core/formats/arrow/simple_builder/filler.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/portions/with_blobs.h>
#include <ydb/core/tx/columnshard/engines/reader/common/read_filter_merger.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/null_column.h>
#include <ydb/core/tx/columnshard/splitter/batch_slice.h>
#include <ydb/core/tx/columnshard/splitter/settings.h>

namespace NKikimr::NOlap::NCompaction {

void TGeneralCompactColumnEngineChanges::BuildAppendedPortionsByFullBatches(TConstructionContext& context, std::vector<TPortionInfoWithBlobs>&& portions) noexcept {
    std::vector<std::shared_ptr<arrow::RecordBatch>> batchResults;
    auto resultSchema = context.SchemaVersions.GetLastSchema();
    {
        auto resultDataSchema = resultSchema->GetIndexInfo().ArrowSchemaWithSpecials();
        NIndexedReader::TMergePartialStream mergeStream(resultSchema->GetIndexInfo().GetReplaceKey(), resultDataSchema, false);
        for (auto&& i : portions) {
            auto dataSchema = context.SchemaVersions.GetSchema(i.GetPortionInfo().GetMinSnapshot());
            auto batch = i.GetBatch(dataSchema, *resultSchema);
            batch = resultSchema->NormalizeBatch(*dataSchema, batch);
            Y_DEBUG_ABORT_UNLESS(NArrow::IsSortedAndUnique(batch, resultSchema->GetIndexInfo().GetReplaceKey()));
            mergeStream.AddSource(batch, nullptr);
        }
        batchResults = mergeStream.DrainAllParts(CheckPoints, resultDataSchema->fields());
    }
    Y_ABORT_UNLESS(batchResults.size());
    for (auto&& b : batchResults) {
        auto portions = MakeAppendedPortions(b, GranuleMeta->GetPathId(), resultSchema->GetSnapshot(), GranuleMeta.get(), context, {});
        Y_ABORT_UNLESS(portions.size());
        for (auto& portion : portions) {
            AppendedPortions.emplace_back(std::move(portion));
        }
    }
}

void TGeneralCompactColumnEngineChanges::BuildAppendedPortionsByChunks(TConstructionContext& context, std::vector<TPortionInfoWithBlobs>&& portions) noexcept {
    static const TString portionIdFieldName = "$$__portion_id";
    static const TString portionRecordIndexFieldName = "$$__portion_record_idx";
    static const std::shared_ptr<arrow::Field> portionIdField = std::make_shared<arrow::Field>(portionIdFieldName, std::make_shared<arrow::UInt16Type>());
    static const std::shared_ptr<arrow::Field> portionRecordIndexField = std::make_shared<arrow::Field>(portionRecordIndexFieldName, std::make_shared<arrow::UInt32Type>());

    auto resultSchema = context.SchemaVersions.GetLastSchema();

    std::vector<std::string> pkFieldNames = resultSchema->GetIndexInfo().GetReplaceKey()->field_names();
    std::set<std::string> pkFieldNamesSet(pkFieldNames.begin(), pkFieldNames.end());
    for (auto&& i : TIndexInfo::GetSpecialColumnNames()) {
        pkFieldNamesSet.emplace(i);
    }

    std::vector<std::shared_ptr<arrow::RecordBatch>> batchResults;
    {
        arrow::FieldVector indexFields;
        indexFields.emplace_back(portionIdField);
        indexFields.emplace_back(portionRecordIndexField);
        for (auto&& i : TIndexInfo::ArrowSchemaSnapshot()->fields()) {
            indexFields.emplace_back(i);
        }
        auto dataSchema = std::make_shared<arrow::Schema>(indexFields);
        NIndexedReader::TMergePartialStream mergeStream(resultSchema->GetIndexInfo().GetReplaceKey(), dataSchema, false);
        ui32 idx = 0;
        for (auto&& i : portions) {
            auto dataSchema = context.SchemaVersions.GetSchema(i.GetPortionInfo().GetMinSnapshot());
            auto batch = i.GetBatch(dataSchema, *resultSchema, pkFieldNamesSet);
            {
                NArrow::NConstruction::IArrayBuilder::TPtr column = std::make_shared<NArrow::NConstruction::TSimpleArrayConstructor<NArrow::NConstruction::TIntConstFiller<arrow::UInt16Type>>>(portionIdFieldName, idx++);
                batch = NArrow::TStatusValidator::GetValid(batch->AddColumn(batch->num_columns(), portionIdField, column->BuildArray(batch->num_rows())));
            }
            {
                NArrow::NConstruction::IArrayBuilder::TPtr column = std::make_shared<NArrow::NConstruction::TSimpleArrayConstructor<NArrow::NConstruction::TIntSeqFiller<arrow::UInt32Type>>>(portionRecordIndexFieldName);
                batch = NArrow::TStatusValidator::GetValid(batch->AddColumn(batch->num_columns(), portionRecordIndexField, column->BuildArray(batch->num_rows())));
            }
            Y_DEBUG_ABORT_UNLESS(NArrow::IsSortedAndUnique(batch, resultSchema->GetIndexInfo().GetReplaceKey()));
            mergeStream.AddSource(batch, nullptr);
        }
        batchResults = mergeStream.DrainAllParts(CheckPoints, indexFields);
    }
    Y_ABORT_UNLESS(batchResults.size());

    std::shared_ptr<TSerializationStats> stats = std::make_shared<TSerializationStats>();
    for (auto&& i : SwitchedPortions) {
        stats->Merge(i.GetSerializationStat(*resultSchema));
    }

    std::vector<std::map<ui32, std::vector<TColumnPortionResult>>> chunkGroups;
    chunkGroups.resize(batchResults.size());
    for (auto&& columnId : resultSchema->GetIndexInfo().GetColumnIds()) {
        NActors::TLogContextGuard logGuard(NActors::TLogContextBuilder::Build()("field_name", resultSchema->GetIndexInfo().GetColumnName(columnId)));
        auto columnInfo = stats->GetColumnInfo(columnId);
        auto resultField = resultSchema->GetIndexInfo().GetColumnFieldVerified(columnId);

        std::vector<TPortionColumnCursor> cursors;
        for (auto&& p : portions) {
            auto dataSchema = context.SchemaVersions.GetSchema(p.GetPortionInfo().GetMinSnapshot());
            auto loader = dataSchema->GetColumnLoaderOptional(columnId);
            std::vector<const TColumnRecord*> records;
            std::vector<std::shared_ptr<IPortionDataChunk>> chunks;
            if (!p.ExtractColumnChunks(columnId, records, chunks)) {
                AFL_VERIFY(!loader);
                records = {nullptr};
                chunks.emplace_back(std::make_shared<NChunks::TNullChunkPreparation>(columnId, p.GetPortionInfo().GetRecordsCount(), resultField, resultSchema->GetColumnSaver(columnId)));
                loader = resultSchema->GetColumnLoaderVerified(columnId);
            }
            AFL_VERIFY(!!loader);
            cursors.emplace_back(TPortionColumnCursor(chunks, records, loader, p.GetPortionInfo().GetPortionId()));
        }

        ui32 batchesRecordsCount = 0;
        ui32 columnRecordsCount = 0;
        std::map<std::string, std::vector<TColumnPortionResult>> columnChunks;
        ui32 batchIdx = 0;
        for (auto&& batchResult : batchResults) {
            const ui32 portionRecordsCountLimit = batchResult->num_rows() / (batchResult->num_rows() / NSplitter::TSplitSettings().GetExpectedRecordsCountOnPage() + 1) + 1;
            TColumnMergeContext context(columnId, resultSchema, portionRecordsCountLimit, NSplitter::TSplitSettings().GetExpectedUnpackColumnChunkRawSize(), columnInfo);
            TMergedColumn mColumn(context);

            auto columnPortionIdx = batchResult->GetColumnByName(portionIdFieldName);
            auto columnPortionRecordIdx = batchResult->GetColumnByName(portionRecordIndexFieldName);
            auto columnSnapshotPlanStepIdx = batchResult->GetColumnByName(TIndexInfo::SPEC_COL_PLAN_STEP);
            auto columnSnapshotTxIdx = batchResult->GetColumnByName(TIndexInfo::SPEC_COL_TX_ID);
            Y_ABORT_UNLESS(columnPortionIdx && columnPortionRecordIdx && columnSnapshotPlanStepIdx && columnSnapshotTxIdx);
            Y_ABORT_UNLESS(columnPortionIdx->type_id() == arrow::UInt16Type::type_id);
            Y_ABORT_UNLESS(columnPortionRecordIdx->type_id() == arrow::UInt32Type::type_id);
            Y_ABORT_UNLESS(columnSnapshotPlanStepIdx->type_id() == arrow::UInt64Type::type_id);
            Y_ABORT_UNLESS(columnSnapshotTxIdx->type_id() == arrow::UInt64Type::type_id);
            const arrow::UInt16Array& pIdxArray = static_cast<const arrow::UInt16Array&>(*columnPortionIdx);
            const arrow::UInt32Array& pRecordIdxArray = static_cast<const arrow::UInt32Array&>(*columnPortionRecordIdx);

            AFL_VERIFY(batchResult->num_rows() == pIdxArray.length());
            std::optional<ui16> predPortionIdx;
            for (ui32 idx = 0; idx < pIdxArray.length(); ++idx) {
                const ui16 portionIdx = pIdxArray.Value(idx);
                const ui32 portionRecordIdx = pRecordIdxArray.Value(idx);
                auto& cursor = cursors[portionIdx];
                cursor.Next(portionRecordIdx, mColumn);
                if (predPortionIdx && portionIdx != *predPortionIdx) {
                    cursors[*predPortionIdx].Fetch(mColumn);
                }
                if (idx + 1 == pIdxArray.length()) {
                    cursor.Fetch(mColumn);
                }
                predPortionIdx = portionIdx;
            }
            chunkGroups[batchIdx][columnId] = mColumn.BuildResult();
            batchesRecordsCount += batchResult->num_rows();
            columnRecordsCount += mColumn.GetRecordsCount();
            AFL_VERIFY(batchResult->num_rows() == mColumn.GetRecordsCount());
            ++batchIdx;
        }
        AFL_VERIFY(columnRecordsCount == batchesRecordsCount)("mCount", columnRecordsCount)("bCount", batchesRecordsCount);

    }
    ui32 batchIdx = 0;

    const auto groups = resultSchema->GetIndexInfo().GetEntityGroupsByStorageId(IStoragesManager::DefaultStorageId, *SaverContext.GetStoragesManager());
    for (auto&& columnChunks : chunkGroups) {
        auto batchResult = batchResults[batchIdx];
        ++batchIdx;
        Y_ABORT_UNLESS(columnChunks.size());

        for (auto&& i : columnChunks) {
            if (i.second.size() != columnChunks.begin()->second.size()) {
                for (ui32 p = 0; p < std::min<ui32>(columnChunks.begin()->second.size(), i.second.size()); ++p) {
                    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("p_first", columnChunks.begin()->second[p].DebugString())("p", i.second[p].DebugString());
                }
            }
            AFL_VERIFY(i.second.size() == columnChunks.begin()->second.size())("first", columnChunks.begin()->second.size())("current", i.second.size())("first_name", columnChunks.begin()->first)("current_name", i.first);
        }

        std::vector<TGeneralSerializedSlice> batchSlices;
        std::shared_ptr<TDefaultSchemaDetails> schemaDetails(new TDefaultSchemaDetails(resultSchema, stats));

        for (ui32 i = 0; i < columnChunks.begin()->second.size(); ++i) {
            THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>> portionColumns;
            for (auto&& p : columnChunks) {
                portionColumns.emplace(p.first, p.second[i].GetChunks());
            }
            resultSchema->GetIndexInfo().AppendIndexes(portionColumns);
            batchSlices.emplace_back(portionColumns, schemaDetails, context.Counters.SplitterCounters);
        }
        TSimilarPacker slicer(NSplitter::TSplitSettings().GetExpectedPortionSize());
        auto packs = slicer.Split(batchSlices);

        ui32 recordIdx = 0;
        for (auto&& i : packs) {
            TGeneralSerializedSlice slice(std::move(i));
            auto b = batchResult->Slice(recordIdx, slice.GetRecordsCount());
            AppendedPortions.emplace_back(TPortionInfoWithBlobs::BuildByBlobs(slice.GroupChunksByBlobs(groups), nullptr, GranuleMeta->GetPathId(), 
                resultSchema->GetSnapshot(), SaverContext.GetStoragesManager()));
            AppendedPortions.back().FillStatistics(resultSchema->GetIndexInfo());
            NArrow::TFirstLastSpecialKeys primaryKeys(slice.GetFirstLastPKBatch(resultSchema->GetIndexInfo().GetReplaceKey()));
            NArrow::TMinMaxSpecialKeys snapshotKeys(b, TIndexInfo::ArrowSchemaSnapshot());
            AppendedPortions.back().GetPortionInfo().AddMetadata(*resultSchema, primaryKeys, snapshotKeys, IStoragesManager::DefaultStorageId);
            recordIdx += slice.GetRecordsCount();
        }
    }
}

TConclusionStatus TGeneralCompactColumnEngineChanges::DoConstructBlobs(TConstructionContext& context) noexcept {
    i64 portionsSize = 0;
    i64 portionsCount = 0;
    i64 insertedPortionsSize = 0;
    i64 compactedPortionsSize = 0;
    i64 otherPortionsSize = 0;
    for (auto&& i : SwitchedPortions) {
        if (i.GetMeta().GetProduced() == TPortionMeta::EProduced::INSERTED) {
            insertedPortionsSize += i.GetBlobBytes();
        } else if (i.GetMeta().GetProduced() == TPortionMeta::EProduced::SPLIT_COMPACTED) {
            compactedPortionsSize += i.GetBlobBytes();
        } else {
            otherPortionsSize += i.GetBlobBytes();
        }
        portionsSize += i.GetBlobBytes();
        ++portionsCount;
    }
    NChanges::TGeneralCompactionCounters::OnPortionsKind(insertedPortionsSize, compactedPortionsSize, otherPortionsSize);
    NChanges::TGeneralCompactionCounters::OnRepackPortions(portionsCount, portionsSize);

    {
        std::vector<TPortionInfoWithBlobs> portions = TPortionInfoWithBlobs::RestorePortions(SwitchedPortions, Blobs, context.SchemaVersions, SaverContext.GetStoragesManager());
        if (!HasAppData() || AppDataVerified().ColumnShardConfig.GetUseChunkedMergeOnCompaction()) {
            BuildAppendedPortionsByChunks(context, std::move(portions));
        } else {
            BuildAppendedPortionsByFullBatches(context, std::move(portions));
        }
    }

    if (IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD)) {
        TStringBuilder sbSwitched;
        sbSwitched << "";
        for (auto&& p : SwitchedPortions) {
            sbSwitched << p.DebugString() << ";";
        }
        sbSwitched << "";

        TStringBuilder sbAppended;
        for (auto&& p : AppendedPortions) {
            sbAppended << p.GetPortionInfo().DebugString() << ";";
        }
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "blobs_created_diff")("appended", sbAppended)("switched", sbSwitched);
    }
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "blobs_created")("appended", AppendedPortions.size())("switched", SwitchedPortions.size());

    return TConclusionStatus::Success();
}

void TGeneralCompactColumnEngineChanges::DoWriteIndexOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) {
    TBase::DoWriteIndexOnComplete(self, context);
    if (self) {
        self->IncCounter(context.FinishedSuccessfully ? NColumnShard::COUNTER_SPLIT_COMPACTION_SUCCESS : NColumnShard::COUNTER_SPLIT_COMPACTION_FAIL);
        self->IncCounter(NColumnShard::COUNTER_SPLIT_COMPACTION_BLOBS_WRITTEN, context.BlobsWritten);
        self->IncCounter(NColumnShard::COUNTER_SPLIT_COMPACTION_BYTES_WRITTEN, context.BytesWritten);
    }
}

void TGeneralCompactColumnEngineChanges::DoStart(NColumnShard::TColumnShard& self) {
    TBase::DoStart(self);
    auto& g = *GranuleMeta;
    self.CSCounters.OnSplitCompactionInfo(g.GetAdditiveSummary().GetCompacted().GetPortionsSize(), g.GetAdditiveSummary().GetCompacted().GetPortionsCount());
}

NColumnShard::ECumulativeCounters TGeneralCompactColumnEngineChanges::GetCounterIndex(const bool isSuccess) const {
    return isSuccess ? NColumnShard::COUNTER_COMPACTION_SUCCESS : NColumnShard::COUNTER_COMPACTION_FAIL;
}

void TGeneralCompactColumnEngineChanges::AddCheckPoint(const NIndexedReader::TSortableBatchPosition& position, const bool include, const bool validationDuplications) {
    AFL_VERIFY(CheckPoints.emplace(position, include).second || !validationDuplications);
}

std::shared_ptr<TGeneralCompactColumnEngineChanges::IMemoryPredictor> TGeneralCompactColumnEngineChanges::BuildMemoryPredictor() {
    if (!HasAppData() || AppDataVerified().ColumnShardConfig.GetUseChunkedMergeOnCompaction()) {
        return std::make_shared<TMemoryPredictorChunkedPolicy>();
    } else {
        return std::make_shared<TMemoryPredictorSimplePolicy>();
    }
}

ui64 TGeneralCompactColumnEngineChanges::TMemoryPredictorChunkedPolicy::AddPortion(const TPortionInfo& portionInfo) {
    SumMemoryFix += portionInfo.GetRecordsCount() * (2 * sizeof(ui64) + sizeof(ui32) + sizeof(ui16));
    ++PortionsCount;
    THashMap<ui32, ui64> maxChunkSizeByColumn;
    for (auto&& i : portionInfo.GetRecords()) {
        SumMemoryFix += i.BlobRange.Size;
        auto it = maxChunkSizeByColumn.find(i.GetColumnId());
        if (it == maxChunkSizeByColumn.end()) {
            maxChunkSizeByColumn.emplace(i.GetColumnId(), i.GetMeta().GetRawBytesVerified());
        } else {
            if (it->second < i.GetMeta().GetRawBytesVerified()) {
                it->second = i.GetMeta().GetRawBytesVerified();
            }
        }
    }

    SumMemoryDelta = 0;
    for (auto&& i : maxChunkSizeByColumn) {
        MaxMemoryByColumnChunk[i.first] += i.second;
        SumMemoryDelta = std::max(SumMemoryDelta, MaxMemoryByColumnChunk[i.first]);
    }

    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("memory_prediction_after", SumMemoryFix + SumMemoryDelta)("portion_info", portionInfo.DebugString());
    return SumMemoryFix + SumMemoryDelta;
}

}

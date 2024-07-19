
#include "general_compaction.h"

#include "compaction/column_cursor.h"
#include "compaction/column_portion_chunk.h"
#include "compaction/merge_context.h"
#include "compaction/merged_column.h"
#include "counters/general.h"

#include <ydb/core/formats/arrow/reader/merger.h>
#include <ydb/core/formats/arrow/simple_builder/array.h>
#include <ydb/core/formats/arrow/simple_builder/filler.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/portions/read_with_blobs.h>
#include <ydb/core/tx/columnshard/engines/portions/write_with_blobs.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/null_column.h>
#include <ydb/core/tx/columnshard/splitter/batch_slice.h>
#include <ydb/core/tx/columnshard/splitter/settings.h>

namespace NKikimr::NOlap::NCompaction {

void TGeneralCompactColumnEngineChanges::BuildAppendedPortionsByFullBatches(
    TConstructionContext& context, std::vector<TReadPortionInfoWithBlobs>&& portions) noexcept {
    std::vector<std::shared_ptr<arrow::RecordBatch>> batchResults;
    auto resultSchema = context.SchemaVersions.GetLastSchema();
    auto shardingActual = context.SchemaVersions.GetShardingInfoActual(GranuleMeta->GetPathId());
    {
        auto resultDataSchema = resultSchema->GetIndexInfo().ArrowSchemaWithSpecials();
        NArrow::NMerger::TMergePartialStream mergeStream(
            resultSchema->GetIndexInfo().GetReplaceKey(), resultDataSchema, false, IIndexInfo::GetSnapshotColumnNames());

        THashSet<ui64> portionsInUsage;
        for (auto&& i : portions) {
            AFL_VERIFY(portionsInUsage.emplace(i.GetPortionInfo().GetPortionId()).second);
        }

        for (auto&& i : portions) {
            auto dataSchema = i.GetPortionInfo().GetSchema(context.SchemaVersions);
            auto batch = i.RestoreBatch(dataSchema, *resultSchema);
            batch = resultSchema->NormalizeBatch(*dataSchema, batch).DetachResult();
            IIndexInfo::NormalizeDeletionColumn(*batch);
            auto filter = BuildPortionFilter(shardingActual, batch, i.GetPortionInfo(), portionsInUsage, resultSchema);
            mergeStream.AddSource(batch, filter);
        }
        batchResults = mergeStream.DrainAllParts(CheckPoints, resultDataSchema->fields());
    }
    Y_ABORT_UNLESS(batchResults.size());
    for (auto&& b : batchResults) {
        auto portions = MakeAppendedPortions(b, GranuleMeta->GetPathId(), resultSchema->GetSnapshot(), GranuleMeta.get(), context, {});
        Y_ABORT_UNLESS(portions.size());
        for (auto& portion : portions) {
            if (shardingActual) {
                portion.GetPortionConstructor().SetShardingVersion(shardingActual->GetSnapshotVersion());
            }
            AppendedPortions.emplace_back(std::move(portion));
        }
    }
}

std::shared_ptr<NArrow::TColumnFilter> TGeneralCompactColumnEngineChanges::BuildPortionFilter(
    const std::optional<NKikimr::NOlap::TGranuleShardingInfo>& shardingActual, const std::shared_ptr<NArrow::TGeneralContainer>& batch,
    const TPortionInfo& pInfo, const THashSet<ui64>& portionsInUsage, const ISnapshotSchema::TPtr& resultSchema) const {
    std::shared_ptr<NArrow::TColumnFilter> filter;
    auto table = batch->BuildTableVerified();
    if (shardingActual && pInfo.NeedShardingFilter(*shardingActual)) {
        filter = shardingActual->GetShardingInfo()->GetFilter(table);
    }
    NArrow::TColumnFilter filterDeleted = NArrow::TColumnFilter::BuildAllowFilter();
    if (pInfo.GetMeta().GetDeletionsCount()) {
        auto col = table->GetColumnByName(TIndexInfo::SPEC_COL_DELETE_FLAG);
        AFL_VERIFY(col);
        AFL_VERIFY(col->type()->id() == arrow::Type::BOOL);
        for (auto&& c : col->chunks()) {
            auto bCol = static_pointer_cast<arrow::BooleanArray>(c);
            for (ui32 i = 0; i < bCol->length(); ++i) {
                filterDeleted.Add(!bCol->GetView(i));
            }
        }
        NArrow::TColumnFilter filterCorrection = NArrow::TColumnFilter::BuildDenyFilter();
        auto pkSchema = resultSchema->GetIndexInfo().GetReplaceKey();
        NArrow::NMerger::TRWSortableBatchPosition pos(batch, 0, pkSchema->field_names(), {}, false);
        ui32 posCurrent = 0;
        auto excludedIntervalsInfo = GranuleMeta->GetPortionsIndex().GetIntervalFeatures(pInfo, portionsInUsage);
        for (auto&& i : excludedIntervalsInfo.GetExcludedIntervals()) {
            NArrow::NMerger::TSortableBatchPosition startForFound(i.GetStart().ToBatch(pkSchema), 0, pkSchema->field_names(), {}, false);
            NArrow::NMerger::TSortableBatchPosition finishForFound(i.GetFinish().ToBatch(pkSchema), 0, pkSchema->field_names(), {}, false);
            auto foundStart =
                NArrow::NMerger::TSortableBatchPosition::FindPosition(pos, pos.GetPosition(), batch->num_rows() - 1, startForFound, true);
            AFL_VERIFY(foundStart);
            AFL_VERIFY(!foundStart->IsLess())("pos", pos.DebugJson())("start", startForFound.DebugJson())("found", foundStart->DebugString());
            auto foundFinish =
                NArrow::NMerger::TSortableBatchPosition::FindPosition(pos, pos.GetPosition(), batch->num_rows() - 1, finishForFound, false);
            AFL_VERIFY(foundFinish);
            AFL_VERIFY(foundFinish->GetPosition() >= foundStart->GetPosition());
            if (foundFinish->GetPosition() > foundStart->GetPosition()) {
                AFL_VERIFY(!foundFinish->IsGreater())("pos", pos.DebugJson())("finish", finishForFound.DebugJson())(
                    "found", foundFinish->DebugString());
            }
            filterCorrection.Add(foundStart->GetPosition() - posCurrent, false);
            if (foundFinish->IsGreater()) {
                filterCorrection.Add(foundFinish->GetPosition() - foundStart->GetPosition(), true);
                posCurrent = foundFinish->GetPosition();
            } else {
                filterCorrection.Add(foundFinish->GetPosition() - foundStart->GetPosition() + 1, true);
                posCurrent = foundFinish->GetPosition() + 1;
            }
        }
        AFL_VERIFY(filterCorrection.Size() <= batch->num_rows());
        filterCorrection.Add(false, batch->num_rows() - filterCorrection.Size());
        filterDeleted = filterDeleted.Or(filterCorrection);
    }
    if (filter) {
        *filter = filter->And(filterDeleted);
    } else if (!filterDeleted.IsTotalAllowFilter()) {
        filter = std::make_shared<NArrow::TColumnFilter>(std::move(filterDeleted));
    }
    return filter;
}

void TGeneralCompactColumnEngineChanges::BuildAppendedPortionsByChunks(
    TConstructionContext& context, std::vector<TReadPortionInfoWithBlobs>&& portions) noexcept {
    static const TString portionIdFieldName = "$$__portion_id";
    static const TString portionRecordIndexFieldName = "$$__portion_record_idx";
    static const std::shared_ptr<arrow::Field> portionIdField =
        std::make_shared<arrow::Field>(portionIdFieldName, std::make_shared<arrow::UInt16Type>());
    static const std::shared_ptr<arrow::Field> portionRecordIndexField =
        std::make_shared<arrow::Field>(portionRecordIndexFieldName, std::make_shared<arrow::UInt32Type>());

    auto resultSchema = context.SchemaVersions.GetLastSchema();
    auto shardingActual = context.SchemaVersions.GetShardingInfoActual(GranuleMeta->GetPathId());

    std::vector<std::string> pkFieldNames = resultSchema->GetIndexInfo().GetReplaceKey()->field_names();
    std::set<std::string> pkFieldNamesSet(pkFieldNames.begin(), pkFieldNames.end());
    for (auto&& i : TIndexInfo::GetSnapshotColumnNames()) {
        pkFieldNamesSet.emplace(i);
    }
    pkFieldNamesSet.emplace(TIndexInfo::SPEC_COL_DELETE_FLAG);

    std::vector<std::shared_ptr<arrow::RecordBatch>> batchResults;
    {
        arrow::FieldVector indexFields;
        indexFields.emplace_back(portionIdField);
        indexFields.emplace_back(portionRecordIndexField);
        IIndexInfo::AddSpecialFields(indexFields);
        auto dataSchema = std::make_shared<arrow::Schema>(indexFields);
        NArrow::NMerger::TMergePartialStream mergeStream(
            resultSchema->GetIndexInfo().GetReplaceKey(), dataSchema, false, IIndexInfo::GetSnapshotColumnNames());
        THashSet<ui64> usedPortionIds;
        for (auto&& i : portions) {
            AFL_VERIFY(usedPortionIds.emplace(i.GetPortionInfo().GetPortionId()).second);
        }

        ui32 idx = 0;
        for (auto&& i : portions) {
            auto dataSchema = i.GetPortionInfo().GetSchema(context.SchemaVersions);
            auto batch = i.RestoreBatch(dataSchema, *resultSchema, pkFieldNamesSet);
            {
                NArrow::NConstruction::IArrayBuilder::TPtr column =
                    std::make_shared<NArrow::NConstruction::TSimpleArrayConstructor<NArrow::NConstruction::TIntConstFiller<arrow::UInt16Type>>>(
                        portionIdFieldName, idx++);
                batch->AddField(portionIdField, column->BuildArray(batch->num_rows())).Validate();
            }
            {
                NArrow::NConstruction::IArrayBuilder::TPtr column =
                    std::make_shared<NArrow::NConstruction::TSimpleArrayConstructor<NArrow::NConstruction::TIntSeqFiller<arrow::UInt32Type>>>(
                        portionRecordIndexFieldName);
                batch->AddField(portionRecordIndexField, column->BuildArray(batch->num_rows())).Validate();
            }
            IIndexInfo::NormalizeDeletionColumn(*batch);
            std::shared_ptr<NArrow::TColumnFilter> filter =
                BuildPortionFilter(shardingActual, batch, i.GetPortionInfo(), usedPortionIds, resultSchema);
            mergeStream.AddSource(batch, filter);
        }
        batchResults = mergeStream.DrainAllParts(CheckPoints, indexFields);
    }

    std::shared_ptr<TSerializationStats> stats = std::make_shared<TSerializationStats>();
    for (auto&& i : SwitchedPortions) {
        stats->Merge(i.GetSerializationStat(*resultSchema));
    }

    std::vector<std::map<ui32, std::vector<TColumnPortionResult>>> chunkGroups;
    chunkGroups.resize(batchResults.size());
    for (auto&& columnId : resultSchema->GetIndexInfo().GetColumnIds()) {
        NActors::TLogContextGuard logGuard(
            NActors::TLogContextBuilder::Build()("field_name", resultSchema->GetIndexInfo().GetColumnName(columnId)));
        auto columnInfo = stats->GetColumnInfo(columnId);
        auto resultField = resultSchema->GetIndexInfo().GetColumnFieldVerified(columnId);

        std::vector<TPortionColumnCursor> cursors;
        for (auto&& p : portions) {
            auto dataSchema = p.GetPortionInfo().GetSchema(context.SchemaVersions);
            auto loader = dataSchema->GetColumnLoaderOptional(columnId);
            std::vector<const TColumnRecord*> records;
            std::vector<std::shared_ptr<IPortionDataChunk>> chunks;
            if (!p.ExtractColumnChunks(columnId, records, chunks)) {
                if (!loader) {
                    loader = resultSchema->GetColumnLoaderVerified(columnId);
                } else {
                    AFL_VERIFY(dataSchema->IsSpecialColumnId(columnId));
                }
                chunks.emplace_back(std::make_shared<NChunks::TDefaultChunkPreparation>(columnId, p.GetPortionInfo().GetRecordsCount(),
                    p.GetPortionInfo().GetColumnRawBytes({ columnId }), resultField, resultSchema->GetDefaultValueVerified(columnId),
                    resultSchema->GetColumnSaver(columnId)));
                records = { nullptr };
            }
            AFL_VERIFY(!!loader);
            cursors.emplace_back(TPortionColumnCursor(chunks, records, loader, p.GetPortionInfo().GetPortionId()));
        }

        ui32 batchesRecordsCount = 0;
        ui32 columnRecordsCount = 0;
        std::map<std::string, std::vector<TColumnPortionResult>> columnChunks;
        ui32 batchIdx = 0;
        for (auto&& batchResult : batchResults) {
            const ui32 portionRecordsCountLimit =
                batchResult->num_rows() / (batchResult->num_rows() / NSplitter::TSplitSettings().GetExpectedRecordsCountOnPage() + 1) + 1;
            TColumnMergeContext context(
                columnId, resultSchema, portionRecordsCountLimit, NSplitter::TSplitSettings().GetExpectedUnpackColumnChunkRawSize(), columnInfo);
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

    const auto groups =
        resultSchema->GetIndexInfo().GetEntityGroupsByStorageId(IStoragesManager::DefaultStorageId, *SaverContext.GetStoragesManager());
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

        std::vector<TGeneralSerializedSlice> batchSlices;
        std::shared_ptr<TDefaultSchemaDetails> schemaDetails(new TDefaultSchemaDetails(resultSchema, stats));

        for (ui32 i = 0; i < columnChunks.begin()->second.size(); ++i) {
            THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>> portionColumns;
            for (auto&& p : columnChunks) {
                portionColumns.emplace(p.first, p.second[i].GetChunks());
            }
            batchSlices.emplace_back(portionColumns, schemaDetails, context.Counters.SplitterCounters);
        }
        TSimilarPacker slicer(NSplitter::TSplitSettings().GetExpectedPortionSize());
        auto packs = slicer.Split(batchSlices);

        ui32 recordIdx = 0;
        for (auto&& i : packs) {
            TGeneralSerializedSlice slicePrimary(std::move(i));
            auto dataWithSecondary =
                resultSchema->GetIndexInfo().AppendIndexes(slicePrimary.GetPortionChunksToHash(), SaverContext.GetStoragesManager()).DetachResult();
            TGeneralSerializedSlice slice(dataWithSecondary.GetExternalData(), schemaDetails, context.Counters.SplitterCounters);

            auto b = batchResult->Slice(recordIdx, slice.GetRecordsCount());
            const ui32 deletionsCount = IIndexInfo::CalcDeletions(b, true);
            auto constructor = TWritePortionInfoWithBlobsConstructor::BuildByBlobs(slice.GroupChunksByBlobs(groups),
                dataWithSecondary.GetSecondaryInplaceData(), GranuleMeta->GetPathId(),
                resultSchema->GetVersion(), resultSchema->GetSnapshot(), SaverContext.GetStoragesManager());

            NArrow::TFirstLastSpecialKeys primaryKeys(slice.GetFirstLastPKBatch(resultSchema->GetIndexInfo().GetReplaceKey()));
            NArrow::TMinMaxSpecialKeys snapshotKeys(b, TIndexInfo::ArrowSchemaSnapshot());
            constructor.GetPortionConstructor().AddMetadata(*resultSchema, deletionsCount, primaryKeys, snapshotKeys);
            constructor.GetPortionConstructor().MutableMeta().SetTierName(IStoragesManager::DefaultStorageId);
            if (shardingActual) {
                constructor.GetPortionConstructor().SetShardingVersion(shardingActual->GetSnapshotVersion());
            }
            AppendedPortions.emplace_back(std::move(constructor));
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
            insertedPortionsSize += i.GetTotalBlobBytes();
        } else if (i.GetMeta().GetProduced() == TPortionMeta::EProduced::SPLIT_COMPACTED) {
            compactedPortionsSize += i.GetTotalBlobBytes();
        } else {
            otherPortionsSize += i.GetTotalBlobBytes();
        }
        portionsSize += i.GetTotalBlobBytes();
        ++portionsCount;
    }
    NChanges::TGeneralCompactionCounters::OnPortionsKind(insertedPortionsSize, compactedPortionsSize, otherPortionsSize);
    NChanges::TGeneralCompactionCounters::OnRepackPortions(portionsCount, portionsSize);

    {
        std::vector<TReadPortionInfoWithBlobs> portions =
            TReadPortionInfoWithBlobs::RestorePortions(SwitchedPortions, Blobs, context.SchemaVersions);
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
            sbAppended << p.GetPortionConstructor().DebugString() << ";";
        }
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "blobs_created_diff")("appended", sbAppended)("switched", sbSwitched);
    }
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "blobs_created")("appended", AppendedPortions.size())(
        "switched", SwitchedPortions.size());

    return TConclusionStatus::Success();
}

void TGeneralCompactColumnEngineChanges::DoWriteIndexOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) {
    TBase::DoWriteIndexOnComplete(self, context);
    if (self) {
        self->IncCounter(
            context.FinishedSuccessfully ? NColumnShard::COUNTER_SPLIT_COMPACTION_SUCCESS : NColumnShard::COUNTER_SPLIT_COMPACTION_FAIL);
        self->IncCounter(NColumnShard::COUNTER_SPLIT_COMPACTION_BLOBS_WRITTEN, context.BlobsWritten);
        self->IncCounter(NColumnShard::COUNTER_SPLIT_COMPACTION_BYTES_WRITTEN, context.BytesWritten);
    }
}

void TGeneralCompactColumnEngineChanges::DoStart(NColumnShard::TColumnShard& self) {
    TBase::DoStart(self);
    auto& g = *GranuleMeta;
    self.CSCounters.OnSplitCompactionInfo(
        g.GetAdditiveSummary().GetCompacted().GetTotalPortionsSize(), g.GetAdditiveSummary().GetCompacted().GetPortionsCount());
}

NColumnShard::ECumulativeCounters TGeneralCompactColumnEngineChanges::GetCounterIndex(const bool isSuccess) const {
    return isSuccess ? NColumnShard::COUNTER_COMPACTION_SUCCESS : NColumnShard::COUNTER_COMPACTION_FAIL;
}

void TGeneralCompactColumnEngineChanges::AddCheckPoint(
    const NArrow::NMerger::TSortableBatchPosition& position, const bool include, const bool validationDuplications) {
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
            maxChunkSizeByColumn.emplace(i.GetColumnId(), i.GetMeta().GetRawBytes());
        } else {
            if (it->second < i.GetMeta().GetRawBytes()) {
                it->second = i.GetMeta().GetRawBytes();
            }
        }
    }

    SumMemoryDelta = 0;
    for (auto&& i : maxChunkSizeByColumn) {
        MaxMemoryByColumnChunk[i.first] += i.second;
        SumMemoryDelta = std::max(SumMemoryDelta, MaxMemoryByColumnChunk[i.first]);
    }

    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("memory_prediction_after", SumMemoryFix + SumMemoryDelta)(
        "portion_info", portionInfo.DebugString());
    return SumMemoryFix + SumMemoryDelta;
}

}   // namespace NKikimr::NOlap::NCompaction

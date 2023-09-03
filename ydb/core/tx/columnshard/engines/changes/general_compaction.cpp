#include "general_compaction.h"
#include "compaction/column_portion_chunk.h"
#include "compaction/column_cursor.h"
#include "compaction/merge_context.h"
#include "compaction/merged_column.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/portions/with_blobs.h>
#include <ydb/core/tx/columnshard/splitter/batch_slice.h>
#include <ydb/core/tx/columnshard/splitter/rb_splitter.h>
#include <ydb/core/formats/arrow/simple_builder/array.h>
#include <ydb/core/formats/arrow/simple_builder/filler.h>

namespace NKikimr::NOlap::NCompaction {

TConclusionStatus TGeneralCompactColumnEngineChanges::DoConstructBlobs(TConstructionContext& context) noexcept {
    const ui64 pathId = GranuleMeta->GetPathId();
    std::vector<TPortionInfoWithBlobs> portions = TPortionInfoWithBlobs::RestorePortions(SwitchedPortions, Blobs);
    std::optional<TSnapshot> maxSnapshot;
    for (auto&& i : SwitchedPortions) {
        if (!maxSnapshot || *maxSnapshot < i.GetMinSnapshot()) {
            maxSnapshot = i.GetMinSnapshot();
        }
    }
    Y_VERIFY(maxSnapshot);

    static const TString portionIdFieldName = "$$__portion_id";
    static const TString portionRecordIndexFieldName = "$$__portion_record_idx";

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    auto resultSchema = context.SchemaVersions.GetLastSchema();
    std::vector<std::string> pkFieldNames = resultSchema->GetIndexInfo().GetReplaceKey()->field_names();
    std::set<std::string> pkFieldNamesSet(pkFieldNames.begin(), pkFieldNames.end());
    for (auto&& i : TIndexInfo::GetSpecialColumnNames()) {
        pkFieldNamesSet.emplace(i);
    }
    ui32 idx = 0;
    for (auto&& i : portions) {
        auto dataSchema = context.SchemaVersions.GetSchema(i.GetPortionInfo().GetMinSnapshot());
        auto batch = i.GetBatch(dataSchema, *resultSchema, pkFieldNamesSet);
        {
            NArrow::NConstruction::IArrayBuilder::TPtr column = std::make_shared<NArrow::NConstruction::TSimpleArrayConstructor<NArrow::NConstruction::TIntConstFiller<arrow::UInt16Type>>>(portionIdFieldName, idx++);
            batch =
                NArrow::TStatusValidator::GetValid(
                    batch->AddColumn(batch->num_columns(),
                        std::make_shared<arrow::Field>(portionIdFieldName, std::make_shared<arrow::UInt16Type>()),
                        column->BuildArray(batch->num_rows()))
                );
        }
        {
            NArrow::NConstruction::IArrayBuilder::TPtr column = std::make_shared<NArrow::NConstruction::TSimpleArrayConstructor<NArrow::NConstruction::TIntSeqFiller<arrow::UInt32Type>>>(portionRecordIndexFieldName);
            batch =
                NArrow::TStatusValidator::GetValid(
                    batch->AddColumn(batch->num_columns(),
                        std::make_shared<arrow::Field>(portionRecordIndexFieldName, std::make_shared<arrow::UInt32Type>()),
                        column->BuildArray(batch->num_rows()))
                );
        }
        batches.emplace_back(batch);
        Y_VERIFY_DEBUG(NArrow::IsSortedAndUnique(batch, resultSchema->GetIndexInfo().GetReplaceKey()));
    }

    auto merged = NArrow::MergeSortedBatches(batches, resultSchema->GetIndexInfo().SortReplaceDescription(), Max<size_t>());
    Y_VERIFY(merged.size() == 1);
    auto batchResult = merged.front();

    auto columnPortionIdx = batchResult->GetColumnByName(portionIdFieldName);
    auto columnPortionRecordIdx = batchResult->GetColumnByName(portionRecordIndexFieldName);
    Y_VERIFY(columnPortionIdx && columnPortionRecordIdx);
    Y_VERIFY(columnPortionIdx->type_id() == arrow::UInt16Type::type_id);
    Y_VERIFY(columnPortionRecordIdx->type_id() == arrow::UInt32Type::type_id);
    const arrow::UInt16Array& pIdxArray = static_cast<const arrow::UInt16Array&>(*columnPortionIdx);
    const arrow::UInt32Array& pRecordIdxArray = static_cast<const arrow::UInt32Array&>(*columnPortionRecordIdx);

    const ui32 portionRecordsCountLimit = batchResult->num_rows() / (batchResult->num_rows() / 10000 + 1) + 1;

    TSerializationStats stats;
    for (auto&& i : SwitchedPortions) {
        stats.Merge(i.GetSerializationStat(*resultSchema));
    }

    std::map<std::string, std::vector<TColumnPortionResult>> columnChunks;
    const auto saverContext = GetSaverContext(pathId);

    for (auto&& f : resultSchema->GetSchema()->fields()) {
        const ui32 columnId = resultSchema->GetColumnId(f->name());
        auto columnInfo = stats.GetColumnInfo(columnId);
        Y_VERIFY(columnInfo);
        TColumnMergeContext context(resultSchema, portionRecordsCountLimit, 50 * 1024 * 1024, f, *columnInfo, saverContext);
        TMergedColumn mColumn(context);
        auto c = batchResult->GetColumnByName(f->name());
        if (c) {
            mColumn.AppendSlice(c);
        } else {
            std::vector<TPortionColumnCursor> cursors;
            auto loader = resultSchema->GetColumnLoader(f->name());
            for (auto&& p : portions) {
                cursors.emplace_back(TPortionColumnCursor(p, columnId, loader));
            }
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
        }
        columnChunks[f->name()] = mColumn.BuildResult();
    }

    Y_VERIFY(columnChunks.size());

    for (auto&& i : columnChunks) {
        AFL_VERIFY(i.second.size() == columnChunks.begin()->second.size())("first", columnChunks.begin()->second.size())("current", i.second.size())("first_name", columnChunks.begin()->first)("current_name", i.first);
    }

    std::vector<TGeneralSerializedSlice> batchSlices;
    std::shared_ptr<TDefaultSchemaDetails> schemaDetails(new TDefaultSchemaDetails(resultSchema, saverContext, std::move(stats)));

    for (ui32 i = 0; i < columnChunks.begin()->second.size(); ++i) {
        std::map<ui32, std::vector<IPortionColumnChunk::TPtr>> portionColumns;
        for (auto&& p : columnChunks) {
            portionColumns.emplace(resultSchema->GetColumnId(p.first), p.second[i].GetChunks());
        }
        batchSlices.emplace_back(portionColumns, schemaDetails, context.Counters.SplitterCounters, GetSplitSettings());
    }

    TSimilarSlicer slicer(4 * 1024 * 1024);
    auto packs = slicer.Split(batchSlices);

    ui32 recordIdx = 0;
    for (auto&& i : packs) {
        TGeneralSerializedSlice slice(std::move(i));
        std::vector<std::vector<IPortionColumnChunk::TPtr>> chunksByBlobs = slice.GroupChunksByBlobs();
        auto b = batchResult->Slice(recordIdx, slice.GetRecordsCount());
        AppendedPortions.emplace_back(TPortionInfoWithBlobs::BuildByBlobs(chunksByBlobs, nullptr, GranuleMeta->GetGranuleId(), *maxSnapshot));
        AppendedPortions.back().GetPortionInfo().AddMetadata(*resultSchema, b, saverContext.GetTierName());
    }

    return TConclusionStatus::Success();
}

void TGeneralCompactColumnEngineChanges::DoWriteIndexComplete(NColumnShard::TColumnShard& self, TWriteIndexCompleteContext& context) {
    TBase::DoWriteIndexComplete(self, context);
    self.IncCounter(context.FinishedSuccessfully ? NColumnShard::COUNTER_SPLIT_COMPACTION_SUCCESS : NColumnShard::COUNTER_SPLIT_COMPACTION_FAIL);
    self.IncCounter(NColumnShard::COUNTER_SPLIT_COMPACTION_BLOBS_WRITTEN, context.BlobsWritten);
    self.IncCounter(NColumnShard::COUNTER_SPLIT_COMPACTION_BYTES_WRITTEN, context.BytesWritten);
}

void TGeneralCompactColumnEngineChanges::DoStart(NColumnShard::TColumnShard& self) {
    TBase::DoStart(self);
    auto& g = *GranuleMeta;
    self.CSCounters.OnSplitCompactionInfo(g.GetAdditiveSummary().GetCompacted().GetPortionsSize(), g.GetAdditiveSummary().GetCompacted().GetPortionsCount());
}

NColumnShard::ECumulativeCounters TGeneralCompactColumnEngineChanges::GetCounterIndex(const bool isSuccess) const {
    return isSuccess ? NColumnShard::COUNTER_COMPACTION_SUCCESS : NColumnShard::COUNTER_COMPACTION_FAIL;
}

}


#include "general_compaction.h"

#include "counters/general.h"
#include "compaction/merger.h"

#include <ydb/core/protos/counters_columnshard.pb.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NOlap::NCompaction {

std::shared_ptr<NArrow::TColumnFilter> TGeneralCompactColumnEngineChanges::BuildPortionFilter(
    const std::optional<NKikimr::NOlap::TGranuleShardingInfo>& shardingActual, const std::shared_ptr<NArrow::TGeneralContainer>& batch,
    const TPortionInfo& pInfo, const THashSet<ui64>& portionsInUsage, const ISnapshotSchema::TPtr& resultSchema) const {
    std::shared_ptr<NArrow::TColumnFilter> filter;
    if (shardingActual && pInfo.NeedShardingFilter(*shardingActual)) {
        std::set<std::string> fieldNames;
        for (auto&& i : shardingActual->GetShardingInfo()->GetColumnNames()) {
            fieldNames.emplace(i);
        }
        auto table = batch->BuildTableVerified(fieldNames);
        AFL_VERIFY(table);
        filter = shardingActual->GetShardingInfo()->GetFilter(table);
    }
    NArrow::TColumnFilter filterDeleted = NArrow::TColumnFilter::BuildAllowFilter();
    if (pInfo.GetMeta().GetDeletionsCount()) {
        auto table = batch->BuildTableVerified(std::set<std::string>({ TIndexInfo::SPEC_COL_DELETE_FLAG }));
        AFL_VERIFY(table);
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
    auto resultSchema = context.SchemaVersions.GetLastSchema();
    auto shardingActual = context.SchemaVersions.GetShardingInfoActual(GranuleMeta->GetPathId());

    std::shared_ptr<TSerializationStats> stats = std::make_shared<TSerializationStats>();
    std::shared_ptr<TFilteredSnapshotSchema> resultFiltered;
    NCompaction::TMerger merger(context, SaverContext);
    {
        std::set<ui32> pkColumnIds;
        {
            auto pkColumnIdsVector = IIndexInfo::AddSnapshotFieldIds(resultSchema->GetIndexInfo().GetPKColumnIds());
            pkColumnIds = std::set<ui32>(pkColumnIdsVector.begin(), pkColumnIdsVector.end());
        }
        std::set<ui32> dataColumnIds;
        {
            {
                THashMap<ui64, ISnapshotSchema::TPtr> schemas;
                for (auto& portion : SwitchedPortions) {
                    auto dataSchema = portion.GetSchema(context.SchemaVersions);
                    schemas.emplace(dataSchema->GetVersion(), dataSchema);
                }
                dataColumnIds = ISnapshotSchema::GetColumnsWithDifferentDefaults(schemas, resultSchema);
            }
            for (auto&& i : SwitchedPortions) {
                stats->Merge(i.GetSerializationStat(*resultSchema));
                if (dataColumnIds.size() != resultSchema->GetColumnsCount()) {
                    for (auto id : i.GetColumnIds()) {
                        if (resultSchema->HasColumnId(id)) {
                            dataColumnIds.emplace(id);
                        }
                    }
                }
            }
            AFL_VERIFY(dataColumnIds.size() <= resultSchema->GetColumnsCount());
            if (dataColumnIds.contains((ui32)IIndexInfo::ESpecialColumn::DELETE_FLAG)) {
                pkColumnIds.emplace((ui32)IIndexInfo::ESpecialColumn::DELETE_FLAG);
            }
        }

        resultFiltered = std::make_shared<TFilteredSnapshotSchema>(resultSchema, dataColumnIds);
        {
            auto seqDataColumnIds = dataColumnIds;
            for (auto&& i : pkColumnIds) {
                AFL_VERIFY(seqDataColumnIds.erase(i));
            }
            THashSet<ui64> usedPortionIds;
            for (auto&& i : portions) {
                AFL_VERIFY(usedPortionIds.emplace(i.GetPortionInfo().GetPortionId()).second);
            }

            for (auto&& i : portions) {
                auto blobsSchema = i.GetPortionInfo().GetSchema(context.SchemaVersions);
                auto batch = i.RestoreBatch(*blobsSchema, *resultFiltered, seqDataColumnIds);
                std::shared_ptr<NArrow::TColumnFilter> filter =
                    BuildPortionFilter(shardingActual, batch, i.GetPortionInfo(), usedPortionIds, resultFiltered);
                merger.AddBatch(batch, filter);
            }
        }
    }

    std::optional<ui64> shardingActualVersion;
    if (shardingActual) {
        shardingActualVersion = shardingActual->GetSnapshotVersion();
    }
    AppendedPortions = merger.Execute(stats, CheckPoints, resultFiltered, GranuleMeta->GetPathId(), shardingActualVersion);
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
        BuildAppendedPortionsByChunks(context, std::move(portions));
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
    const NArrow::NMerger::TSortableBatchPosition& position, const bool include) {
    CheckPoints.InsertPosition(position, include);
}

std::shared_ptr<TGeneralCompactColumnEngineChanges::IMemoryPredictor> TGeneralCompactColumnEngineChanges::BuildMemoryPredictor() {
    return std::make_shared<TMemoryPredictorChunkedPolicy>();
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

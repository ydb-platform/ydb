#include "general_compaction.h"
#include "merge_subset.h"

#include "compaction/merger.h"
#include "counters/general.h"

#include <ydb/core/protos/counters_columnshard.pb.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/priorities/usage/service.h>

namespace NKikimr::NOlap::NCompaction {

std::vector<TWritePortionInfoWithBlobsResult> TGeneralCompactColumnEngineChanges::BuildAppendedPortionsByChunks(TConstructionContext& context,
    std::vector<TPortionToMerge>&& portions, const std::shared_ptr<TFilteredSnapshotSchema>& resultFiltered,
    const std::shared_ptr<NArrow::NSplitter::TSerializationStats>& stats) noexcept {
    auto shardingActual = context.SchemaVersions.GetShardingInfoActual(GranuleMeta->GetPathId());
    if (portions.empty()) {
        return {};
    }

    NCompaction::TMerger merger(context, SaverContext);
    merger.SetPortionExpectedSize(PortionExpectedSize);
    for (auto&& i : portions) {
        merger.AddBatch(i.GetBatch(), i.GetFilter());
    }

    std::optional<ui64> shardingActualVersion;
    if (shardingActual) {
        shardingActualVersion = shardingActual->GetSnapshotVersion();
    }
    auto result = merger.Execute(stats, CheckPoints, resultFiltered, GranuleMeta->GetPathId(), shardingActualVersion);
    for (auto&& p : result) {
        p.GetPortionConstructor().MutablePortionConstructor().MutableMeta().UpdateRecordsMeta(NPortion::EProduced::SPLIT_COMPACTED);
    }
    return result;
}

TConclusionStatus TGeneralCompactColumnEngineChanges::DoConstructBlobs(TConstructionContext& context) noexcept {
    TSimplePortionsGroupInfo insertedPortions;
    TSimplePortionsGroupInfo compactedPortions;
    THashMap<ui32, TSimplePortionsGroupInfo> portionGroups;
    for (auto&& i : SwitchedPortions) {
        portionGroups[i->GetMeta().GetCompactionLevel()].AddPortion(i);
        if (i->GetMeta().GetProduced() == TPortionMeta::EProduced::INSERTED) {
            insertedPortions.AddPortion(i);
        } else if (i->GetMeta().GetProduced() == TPortionMeta::EProduced::SPLIT_COMPACTED) {
            compactedPortions.AddPortion(i);
        } else {
            AFL_VERIFY(false);
        }
    }
    NChanges::TGeneralCompactionCounters::OnRepackPortions(insertedPortions + compactedPortions);
    NChanges::TGeneralCompactionCounters::OnRepackInsertedPortions(insertedPortions);
    NChanges::TGeneralCompactionCounters::OnRepackCompactedPortions(compactedPortions);
    if (GetPortionsToMove().GetTargetCompactionLevel()) {
        NChanges::TGeneralCompactionCounters::OnRepackPortionsByLevel(portionGroups, *GetPortionsToMove().GetTargetCompactionLevel());
    }

    {
        auto accessors = GetPortionDataAccessors(SwitchedPortions);
        std::set<ui32> seqDataColumnIds;
        std::shared_ptr<TFilteredSnapshotSchema> resultFiltered = context.BuildResultFiltered(accessors, seqDataColumnIds);
        std::shared_ptr<NArrow::NSplitter::TSerializationStats> stats = std::make_shared<NArrow::NSplitter::TSerializationStats>();
        for (auto&& accessor : accessors) {
            stats->Merge(accessor.GetSerializationStat(*resultFiltered));
        }

        std::vector<TReadPortionInfoWithBlobs> portions = TReadPortionInfoWithBlobs::RestorePortions(accessors, Blobs, context.SchemaVersions);
        THashSet<ui64> usedPortionIds;
        std::vector<std::shared_ptr<ISubsetToMerge>> currentToMerge;
        for (auto&& i : portions) {
            AFL_VERIFY(usedPortionIds.emplace(i.GetPortionInfo().GetPortionId()).second);
            currentToMerge.emplace_back(std::make_shared<TReadPortionToMerge>(std::move(i), GranuleMeta));
        }
        auto shardingActual = context.SchemaVersions.GetShardingInfoActual(GranuleMeta->GetPathId());
        while (true) {
            std::vector<TPortionToMerge> toMerge;
            ui64 sumMemory = 0;
            ui64 totalSumMemory = 0;
            std::vector<std::shared_ptr<ISubsetToMerge>> appendedToMerge;
            ui32 subsetsCount = 0;
            for (auto&& i : currentToMerge) {
                if (NYDBTest::TControllers::GetColumnShardController()->CheckPortionsToMergeOnCompaction(
                        sumMemory + i->GetColumnMaxChunkMemory(), subsetsCount) &&
                    subsetsCount > 1) {
                    auto merged = BuildAppendedPortionsByChunks(context, std::move(toMerge), resultFiltered, stats);
                    if (merged.size()) {
                        appendedToMerge.emplace_back(std::make_shared<TWritePortionsToMerge>(std::move(merged)));
                    }
                    toMerge.clear();
                    sumMemory = 0;
                    subsetsCount = 0;
                }
                sumMemory += i->GetColumnMaxChunkMemory();
                totalSumMemory += i->GetColumnMaxChunkMemory();
                auto mergePortions = i->BuildPortionsToMerge(context, seqDataColumnIds, resultFiltered, usedPortionIds);
                toMerge.insert(toMerge.end(), mergePortions.begin(), mergePortions.end());
                ++subsetsCount;
            }
            if (toMerge.size()) {
                auto merged = BuildAppendedPortionsByChunks(context, std::move(toMerge), resultFiltered, stats);
                if (appendedToMerge.size()) {
                    if (merged.size()) {
                        appendedToMerge.emplace_back(std::make_shared<TWritePortionsToMerge>(std::move(merged)));
                    }
                } else {
                    context.Counters.OnCompactionCorrectMemory(totalSumMemory);
                    AppendedPortions = std::move(merged);
                    break;
                }
            }
            if (!appendedToMerge.size()) {
                break;
            }
            context.Counters.OnCompactionHugeMemory(totalSumMemory, appendedToMerge.size());
            currentToMerge = std::move(appendedToMerge);
        }
    }

    if (IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD)) {
        TStringBuilder sbSwitched;
        sbSwitched << "";
        for (auto&& p : SwitchedPortions) {
            sbSwitched << p->DebugString() << ";";
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
        self->Counters.GetTabletCounters()->OnCompactionWriteIndexCompleted(
            context.FinishedSuccessfully, context.BlobsWritten, context.BytesWritten);
    }
}

void TGeneralCompactColumnEngineChanges::DoStart(NColumnShard::TColumnShard& self) {
    AFL_VERIFY(PrioritiesAllocationGuard);
    TBase::DoStart(self);
    auto& g = *GranuleMeta;
    self.Counters.GetCSCounters().OnSplitCompactionInfo(
        g.GetAdditiveSummary().GetCompacted().GetTotalPortionsSize(), g.GetAdditiveSummary().GetCompacted().GetPortionsCount());
}

NColumnShard::ECumulativeCounters TGeneralCompactColumnEngineChanges::GetCounterIndex(const bool isSuccess) const {
    return isSuccess ? NColumnShard::COUNTER_COMPACTION_SUCCESS : NColumnShard::COUNTER_COMPACTION_FAIL;
}

void TGeneralCompactColumnEngineChanges::AddCheckPoint(const NArrow::NMerger::TSortableBatchPosition& position, const bool include) {
    CheckPoints.InsertPosition(position, include);
}

std::shared_ptr<TGeneralCompactColumnEngineChanges::IMemoryPredictor> TGeneralCompactColumnEngineChanges::BuildMemoryPredictor() {
    return std::make_shared<TMemoryPredictorChunkedPolicy>();
}

ui64 TGeneralCompactColumnEngineChanges::TMemoryPredictorChunkedPolicy::AddPortion(const TPortionInfo::TConstPtr& portionInfo) {
    SumMemoryFix += portionInfo->GetRecordsCount() * (2 * sizeof(ui64) + sizeof(ui32) + sizeof(ui16)) + portionInfo->GetTotalBlobBytes();
    SumMemoryRaw += portionInfo->GetTotalRawBytes();
    return SumMemoryFix + std::min<ui64>(SumMemoryRaw,
                              NYDBTest::TControllers::GetColumnShardController()->GetConfig().GetMemoryLimitMergeOnCompactionRawData());
}

}   // namespace NKikimr::NOlap::NCompaction

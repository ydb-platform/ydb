#include "general_compaction.h"

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

class ISubsetToMerge {
private:
    virtual std::vector<TPortionToMerge> DoBuildPortionsToMerge(const TConstructionContext& context, const std::set<ui32>& seqDataColumnIds,
        const std::shared_ptr<TFilteredSnapshotSchema>& resultFiltered, const THashSet<ui64>& usedPortionIds) const = 0;

public:
    virtual ~ISubsetToMerge() = default;
    std::vector<TPortionToMerge> BuildPortionsToMerge(const TConstructionContext& context, const std::set<ui32>& seqDataColumnIds,
        const std::shared_ptr<TFilteredSnapshotSchema>& resultFiltered, const THashSet<ui64>& usedPortionIds) const {
        return DoBuildPortionsToMerge(context, seqDataColumnIds, resultFiltered, usedPortionIds);
    }
    virtual ui64 GetColumnMaxChunkMemory() const = 0;
};

class TReadPortionToMerge: public ISubsetToMerge {
private:
    TReadPortionInfoWithBlobs ReadPortion;
    const std::shared_ptr<TGranuleMeta> GranuleMeta;

    std::shared_ptr<NArrow::TColumnFilter> BuildPortionFilter(const std::optional<NKikimr::NOlap::TGranuleShardingInfo>& shardingActual,
        const std::shared_ptr<NArrow::TGeneralContainer>& batch, const TPortionInfo& pInfo, const THashSet<ui64>& portionsInUsage) const {
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
            if (pInfo.HasInsertWriteId()) {
                AFL_VERIFY(pInfo.GetMeta().GetDeletionsCount() == pInfo.GetRecordsCount());
                filterDeleted = NArrow::TColumnFilter::BuildDenyFilter();
            } else {
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
            }
            if (GranuleMeta->GetPortionsIndex().HasOlderIntervals(pInfo, portionsInUsage)) {
                filterDeleted = NArrow::TColumnFilter::BuildAllowFilter();
            }
        }
        if (filter) {
            *filter = filter->And(filterDeleted);
        } else if (!filterDeleted.IsTotalAllowFilter()) {
            filter = std::make_shared<NArrow::TColumnFilter>(std::move(filterDeleted));
        }
        return filter;
    }

    virtual std::vector<TPortionToMerge> DoBuildPortionsToMerge(const TConstructionContext& context, const std::set<ui32>& seqDataColumnIds,
        const std::shared_ptr<TFilteredSnapshotSchema>& resultFiltered, const THashSet<ui64>& usedPortionIds) const override {
        auto blobsSchema = ReadPortion.GetPortionInfo().GetSchema(context.SchemaVersions);
        auto batch = ReadPortion.RestoreBatch(*blobsSchema, *resultFiltered, seqDataColumnIds, false).DetachResult();
        auto shardingActual = context.SchemaVersions.GetShardingInfoActual(GranuleMeta->GetPathId());
        std::shared_ptr<NArrow::TColumnFilter> filter = BuildPortionFilter(shardingActual, batch, ReadPortion.GetPortionInfo(), usedPortionIds);
        return { TPortionToMerge(batch, filter) };
    }

public:
    TReadPortionToMerge(TReadPortionInfoWithBlobs&& rPortion, const std::shared_ptr<TGranuleMeta>& granuleMeta)
        : ReadPortion(std::move(rPortion))
        , GranuleMeta(granuleMeta) {
    }

    virtual ui64 GetColumnMaxChunkMemory() const override {
        ui64 result = 0;
        for (auto&& i : ReadPortion.GetChunks()) {
            result = std::max<ui64>(result, i.second->GetRawBytesVerified());
        }
        return result;
    }
};

class TWritePortionsToMerge: public ISubsetToMerge {
private:
    std::vector<TWritePortionInfoWithBlobsResult> WritePortions;

    virtual std::vector<TPortionToMerge> DoBuildPortionsToMerge(const TConstructionContext& context, const std::set<ui32>& seqDataColumnIds,
        const std::shared_ptr<TFilteredSnapshotSchema>& resultFiltered, const THashSet<ui64>& /*usedPortionIds*/) const override {
        std::vector<TPortionToMerge> result;
        for (auto&& i : WritePortions) {
            auto blobsSchema = i.GetPortionResult().GetPortionInfo().GetSchema(context.SchemaVersions);
            auto batch = i.RestoreBatch(*blobsSchema, *resultFiltered, seqDataColumnIds, false).DetachResult();
            result.emplace_back(TPortionToMerge(batch, nullptr));
        }
        return result;
    }

    virtual ui64 GetColumnMaxChunkMemory() const override {
        ui64 result = 0;
        for (auto&& wPortion : WritePortions) {
            for (auto&& i : wPortion.GetPortionResult().GetRecordsVerified()) {
                result = std::max<ui64>(result, i.GetMeta().GetRawBytes());
            }
        }
        return result;
    }

public:
    TWritePortionsToMerge(std::vector<TWritePortionInfoWithBlobsResult>&& portions)
        : WritePortions(std::move(portions)) {
        ui32 idx = 0;
        for (auto&& i : WritePortions) {
            i.GetPortionConstructor().MutablePortionConstructor().SetPortionId(++idx);
            i.GetPortionConstructor().MutablePortionConstructor().MutableMeta().SetCompactionLevel(0);
            i.RegisterFakeBlobIds();
            i.FinalizePortionConstructor();
        }
    }
};

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
                    appendedToMerge.emplace_back(std::make_shared<TWritePortionsToMerge>(
                        BuildAppendedPortionsByChunks(context, std::move(toMerge), resultFiltered, stats)));
                    toMerge.clear();
                    sumMemory = 0;
                }
                sumMemory += i->GetColumnMaxChunkMemory();
                totalSumMemory += i->GetColumnMaxChunkMemory();
                auto mergePortions = i->BuildPortionsToMerge(context, seqDataColumnIds, resultFiltered, usedPortionIds);
                toMerge.insert(toMerge.end(), mergePortions.begin(), mergePortions.end());
                ++subsetsCount;
            }
            if (toMerge.size() > 1) {
                auto merged = BuildAppendedPortionsByChunks(context, std::move(toMerge), resultFiltered, stats);
                if (appendedToMerge.size()) {
                    appendedToMerge.emplace_back(std::make_shared<TWritePortionsToMerge>(std::move(merged)));
                } else {
                    context.Counters.OnCompactionCorrectMemory(totalSumMemory);
                    AppendedPortions = std::move(merged);
                    break;
                }
            } else {
                AFL_VERIFY(appendedToMerge.size());
                AFL_VERIFY(currentToMerge.size());
                appendedToMerge.emplace_back(currentToMerge.back());
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

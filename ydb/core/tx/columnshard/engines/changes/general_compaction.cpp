#include "general_compaction.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/storage/granule.h>

namespace NKikimr::NOlap {

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

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    auto resultSchema = context.SchemaVersions.GetLastSchema();
    for (auto&& i : portions) {
        auto dataSchema = context.SchemaVersions.GetSchema(i.GetPortionInfo().GetMinSnapshot());
        batches.emplace_back(i.GetBatch(dataSchema, *resultSchema));
        Y_VERIFY(NArrow::IsSorted(batches.back(), resultSchema->GetIndexInfo().GetReplaceKey()));
    }

    auto merged = NArrow::MergeSortedBatches(batches, resultSchema->GetIndexInfo().SortReplaceDescription(), Max<size_t>());
    Y_VERIFY(merged.size() == 1);
    auto batchResult = merged.front();
    AppendedPortions = MakeAppendedPortions(pathId, batchResult, GranuleMeta->GetGranuleId(), *maxSnapshot, GranuleMeta.get(), context);

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

#include "in_granule_compaction.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/storage/granule.h>

namespace NKikimr::NOlap {

namespace {

TConclusionStatus InitInGranuleMerge(const TMark& granuleMark, std::vector<TPortionInfo>& portions, const TCompactionLimits& limits,
    TMarksGranules& marksGranules) {
    ui32 insertedCount = 0;

    THashSet<ui64> filtered;
    THashSet<ui64> goodCompacted;
    THashSet<ui64> nextToGood;
    {
        TMap<NArrow::TReplaceKey, std::vector<const TPortionInfo*>> points;

        for (const auto& portionInfo : portions) {
            if (portionInfo.IsInserted()) {
                ++insertedCount;
            } else if (portionInfo.BlobsSizes().second >= limits.GoodBlobSize) {
                goodCompacted.insert(portionInfo.GetPortion());
            }

            const NArrow::TReplaceKey& start = portionInfo.IndexKeyStart();
            const NArrow::TReplaceKey& end = portionInfo.IndexKeyEnd();

            points[start].push_back(&portionInfo);
            points[end].push_back(nullptr);
        }

        ui32 countInBucket = 0;
        ui64 bucketStartPortion = 0;
        bool isGood = false;
        int sum = 0;
        for (const auto& [key, vec] : points) {
            for (const auto* portionInfo : vec) {
                if (portionInfo) {
                    ++sum;
                    ui64 currentPortion = portionInfo->GetPortion();
                    if (!bucketStartPortion) {
                        bucketStartPortion = currentPortion;
                    }
                    ++countInBucket;

                    ui64 prevIsGood = isGood;
                    isGood = goodCompacted.contains(currentPortion);
                    if (prevIsGood && !isGood) {
                        nextToGood.insert(currentPortion);
                    }
                } else {
                    --sum;
                }
            }

            if (!sum) { // count(start) == count(end), start new range
                Y_VERIFY(bucketStartPortion);

                if (countInBucket == 1) {
                    // We do not want to merge big compacted portions with inserted ones if there's no intersections.
                    if (isGood) {
                        filtered.insert(bucketStartPortion);
                    }
                }
                countInBucket = 0;
                bucketStartPortion = 0;
            }
        }
    }

    Y_VERIFY(insertedCount);

    // Nothing to filter. Leave portions as is, no borders needed.
    if (filtered.empty() && goodCompacted.empty()) {
        return TConclusionStatus::Success();
    }

    // It's a map for SliceIntoGranules(). We use fake granule ids here to slice batch with borders.
    // We could merge inserted portions alltogether and slice result with filtered borders to prevent intersections.
    std::vector<TMark> borders;
    borders.push_back(granuleMark);

    std::vector<TPortionInfo> tmp;
    tmp.reserve(portions.size());
    for (auto& portionInfo : portions) {
        ui64 curPortion = portionInfo.GetPortion();

        // Prevent merge of compacted portions with no intersections
        if (filtered.contains(curPortion)) {
            const auto& start = portionInfo.IndexKeyStart();
            borders.emplace_back(TMark(start));
        } else {
            // nextToGood borders potentially split good compacted portions into 2 parts:
            // the first one without intersections and the second with them
            if (goodCompacted.contains(curPortion) || nextToGood.contains(curPortion)) {
                const auto& start = portionInfo.IndexKeyStart();
                borders.emplace_back(TMark(start));
            }

            tmp.emplace_back(std::move(portionInfo));
        }
    }
    tmp.swap(portions);

    if (borders.size() == 1) {
        Y_VERIFY(borders[0] == granuleMark);
        borders.clear();
    }

    marksGranules = TMarksGranules(std::move(borders));
    return TConclusionStatus::Success();
}

} // namespace

std::pair<std::shared_ptr<arrow::RecordBatch>, TSnapshot> TInGranuleCompactColumnEngineChanges::CompactInOneGranule(ui64 granule,
    const std::vector<TPortionInfo>& portions,
    const THashMap<TBlobRange, TString>& blobs, TConstructionContext& context) const {
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    batches.reserve(portions.size());
    auto resultSchema = context.SchemaVersions.GetLastSchema();

    TSnapshot maxSnapshot = resultSchema->GetSnapshot();
    for (auto& portionInfo : portions) {
        Y_VERIFY(!portionInfo.Empty());
        Y_VERIFY(portionInfo.GetGranule() == granule);
        auto blobSchema = context.SchemaVersions.GetSchema(portionInfo.GetMinSnapshot());
        auto batch = portionInfo.AssembleInBatch(*blobSchema, *resultSchema, blobs);
        batches.push_back(batch);
        if (maxSnapshot < portionInfo.GetMinSnapshot()) {
            maxSnapshot = portionInfo.GetMinSnapshot();
        }
    }

    auto sortedBatch = NArrow::CombineSortedBatches(batches, resultSchema->GetIndexInfo().SortReplaceDescription());
    Y_VERIFY_DEBUG(NArrow::IsSortedAndUnique(sortedBatch, resultSchema->GetIndexInfo().GetReplaceKey()));

    return std::make_pair(sortedBatch, maxSnapshot);
}

TConclusion<std::vector<TString>> TInGranuleCompactColumnEngineChanges::DoConstructBlobs(TConstructionContext& context) noexcept {
    const ui64 pathId = GranuleMeta->GetPathId();
    std::vector<TString> blobs;
    auto& switchedPortions = SwitchedPortions;
    Y_VERIFY(switchedPortions.size());

    const ui64 granule = switchedPortions[0].GetGranule();
    auto [batch, maxSnapshot] = CompactInOneGranule(granule, switchedPortions, Blobs, context);

    auto resultSchema = context.SchemaVersions.GetLastSchema();
    std::vector<TPortionInfo> portions;
    if (!MergeBorders.Empty()) {
        Y_VERIFY(MergeBorders.GetOrderedMarks().size() > 1);
        auto slices = MergeBorders.SliceIntoGranules(batch, resultSchema->GetIndexInfo());
        portions.reserve(slices.size());

        for (auto& [_, slice] : slices) {
            if (!slice || slice->num_rows() == 0) {
                continue;
            }
            auto tmp = MakeAppendedPortions(pathId, slice, granule, maxSnapshot, blobs, GranuleMeta.get(), context);
            for (auto&& portionInfo : tmp) {
                portions.emplace_back(std::move(portionInfo));
            }
        }
    } else {
        portions = MakeAppendedPortions(pathId, batch, granule, maxSnapshot, blobs, GranuleMeta.get(), context);
    }

    Y_VERIFY(portions.size() > 0);
    Y_VERIFY(AppendedPortions.empty());
    // Set appended portions.
    AppendedPortions.swap(portions);

    return blobs;
}

void TInGranuleCompactColumnEngineChanges::DoWriteIndexComplete(NColumnShard::TColumnShard& self, TWriteIndexCompleteContext& context) {
    TBase::DoWriteIndexComplete(self, context);
    self.IncCounter(NColumnShard::COUNTER_COMPACTION_BLOBS_WRITTEN, context.BlobsWritten);
    self.IncCounter(NColumnShard::COUNTER_COMPACTION_BYTES_WRITTEN, context.BytesWritten);
}

void TInGranuleCompactColumnEngineChanges::DoStart(NColumnShard::TColumnShard& self) {
    TBase::DoStart(self);
    auto& g = *GranuleMeta;
    self.CSCounters.OnInternalCompactionInfo(g.GetAdditiveSummary().GetOther().GetPortionsSize(), g.GetAdditiveSummary().GetOther().GetPortionsCount());
    Y_VERIFY(InitInGranuleMerge(SrcGranule.Mark, SwitchedPortions, Limits, MergeBorders).Ok());
}

NColumnShard::ECumulativeCounters TInGranuleCompactColumnEngineChanges::GetCounterIndex(const bool isSuccess) const {
    return isSuccess ? NColumnShard::COUNTER_COMPACTION_SUCCESS : NColumnShard::COUNTER_COMPACTION_FAIL;
}

}

#include "in_granule_compaction.h"

namespace NKikimr::NOlap {

std::pair<std::shared_ptr<arrow::RecordBatch>, TSnapshot> TInGranuleCompactColumnEngineChanges::CompactInOneGranule(ui64 granule,
    const std::vector<TPortionInfo>& portions,
    const THashMap<TBlobRange, TString>& blobs, TConstructionContext& context) const {
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    batches.reserve(portions.size());

    auto resultSchema = context.SchemaVersions.GetLastSchema();

    TSnapshot maxSnapshot = resultSchema->GetSnapshot();
    for (auto& portionInfo : portions) {
        Y_VERIFY(!portionInfo.Empty());
        Y_VERIFY(portionInfo.Granule() == granule);
        auto blobSchema = context.SchemaVersions.GetSchema(portionInfo.GetSnapshot());
        auto batch = portionInfo.AssembleInBatch(*blobSchema, *resultSchema, blobs);
        batches.push_back(batch);
        if (portionInfo.GetSnapshot() > maxSnapshot) {
            maxSnapshot = portionInfo.GetSnapshot();
        }
    }

    auto sortedBatch = NArrow::CombineSortedBatches(batches, resultSchema->GetIndexInfo().SortReplaceDescription());
    Y_VERIFY_DEBUG(NArrow::IsSortedAndUnique(sortedBatch, resultSchema->GetIndexInfo().GetReplaceKey()));

    return std::make_pair(sortedBatch, maxSnapshot);
}

TConclusion<std::vector<TString>> TInGranuleCompactColumnEngineChanges::DoConstructBlobs(TConstructionContext& context) noexcept {
    const ui64 pathId = SrcGranule->PathId;
    std::vector<TString> blobs;
    auto& switchedPortions = SwitchedPortions;
    Y_VERIFY(switchedPortions.size());

    ui64 granule = switchedPortions[0].Granule();
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
            auto tmp = MakeAppendedPortions(pathId, slice, granule, maxSnapshot, blobs, GetGranuleMeta(), context);
            for (auto&& portionInfo : tmp) {
                portions.emplace_back(std::move(portionInfo));
            }
        }
    } else {
        portions = MakeAppendedPortions(pathId, batch, granule, maxSnapshot, blobs, GetGranuleMeta(), context);
    }

    Y_VERIFY(portions.size() > 0);
    Y_VERIFY(AppendedPortions.empty());
    // Set appended portions.
    AppendedPortions.swap(portions);

    return blobs;
}

}

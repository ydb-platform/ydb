#include "merge_subset.h"

namespace NKikimr::NOlap::NCompaction {

std::shared_ptr<NArrow::TColumnFilter> ISubsetToMerge::BuildPortionFilter(const std::optional<TGranuleShardingInfo>& shardingActual,
    const std::shared_ptr<NArrow::TGeneralContainer>& batch, const TPortionInfo& pInfo, const THashSet<ui64>& portionsInUsage,
    const bool useDeletionFilter) const {
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
    if (pInfo.GetMeta().GetDeletionsCount() && useDeletionFilter) {
        if (pInfo.GetPortionType() == EPortionType::Written) {
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

std::vector<TPortionToMerge> TReadPortionToMerge::DoBuildPortionsToMerge(const TConstructionContext& context,
    const std::set<ui32>& seqDataColumnIds, const std::shared_ptr<TFilteredSnapshotSchema>& resultFiltered, const THashSet<ui64>& usedPortionIds,
    const bool useDeletionFilter) const {
    auto blobsSchema = ReadPortion.GetPortionInfo().GetSchema(context.SchemaVersions);
    auto batch = ReadPortion.RestoreBatch(*blobsSchema, *resultFiltered, seqDataColumnIds, false).DetachResult();
    auto shardingActual = context.SchemaVersions.GetShardingInfoActual(GranuleMeta->GetPathId());
    std::shared_ptr<NArrow::TColumnFilter> filter =
        BuildPortionFilter(shardingActual, batch, ReadPortion.GetPortionInfo(), usedPortionIds, useDeletionFilter);
    return { TPortionToMerge(batch, filter) };
}

std::vector<TPortionToMerge> TWritePortionsToMerge::DoBuildPortionsToMerge(const TConstructionContext& context,
    const std::set<ui32>& seqDataColumnIds, const std::shared_ptr<TFilteredSnapshotSchema>& resultFiltered, const THashSet<ui64>& usedPortionIds,
    const bool useDeletionFilter) const {
    std::vector<TPortionToMerge> result;
    for (auto&& i : WritePortions) {
        auto blobsSchema = i.GetPortionResult().GetPortionInfo().GetSchema(context.SchemaVersions);
        auto batch = i.RestoreBatch(*blobsSchema, *resultFiltered, seqDataColumnIds, false).DetachResult();
        std::shared_ptr<NArrow::TColumnFilter> filter =
            BuildPortionFilter(std::nullopt, batch, i.GetPortionResult().GetPortionInfo(), usedPortionIds, useDeletionFilter);
        result.emplace_back(TPortionToMerge(batch, filter));
    }
    return result;
}

ui64 TWritePortionsToMerge::GetColumnMaxChunkMemory() const {
    ui64 result = 0;
    for (auto&& wPortion : WritePortions) {
        for (auto&& i : wPortion.GetPortionResult().GetRecordsVerified()) {
            result = std::max<ui64>(result, i.GetMeta().GetRawBytes());
        }
    }
    return result;
}

TWritePortionsToMerge::TWritePortionsToMerge(
    std::vector<TWritePortionInfoWithBlobsResult>&& portions, const std::shared_ptr<TGranuleMeta>& granuleMeta)
    : TBase(granuleMeta)
    , WritePortions(std::move(portions)) {
    ui32 idx = 0;
    for (auto&& i : WritePortions) {
        i.GetPortionConstructor().MutablePortionConstructor().SetPortionId(++idx);
        i.GetPortionConstructor().MutablePortionConstructor().MutableMeta().SetCompactionLevel(0);
        i.RegisterFakeBlobIds();
        i.FinalizePortionConstructor(TSnapshot::Zero());
    }
}

}   // namespace NKikimr::NOlap::NCompaction

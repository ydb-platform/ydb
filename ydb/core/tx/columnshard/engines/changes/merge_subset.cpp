#include "merge_subset.h"

#include <ydb/core/formats/arrow/reader/position.h>

namespace NKikimr::NOlap::NCompaction {

namespace {

using TKeyInterval = NGranule::NPortionsIndex::TPortionsIndex::TKeyInterval;

// Allow filter for the records of `batch` (sorted by primary key) whose key lies inside one of `intervals` (sorted and disjoint).
NArrow::TColumnFilter BuildKeyIntervalsFilter(
    const std::shared_ptr<NArrow::TGeneralContainer>& batch, const std::vector<TKeyInterval>& intervals) {
    NArrow::TColumnFilter result = NArrow::TColumnFilter::BuildDenyFilter();
    const ui32 recordsCount = batch->num_rows();
    if (!recordsCount || intervals.empty()) {
        return result;
    }
    NArrow::NMerger::TRWSortableBatchPosition position(batch, 0, intervals.front().first.GetSchema()->field_names(), {}, false);
    ui32 current = 0;
    for (const auto& [start, finish] : intervals) {
        if (current == recordsCount) {
            break;
        }
        const auto foundStart =
            NArrow::NMerger::TSortableBatchPosition::FindBound(position, current, recordsCount - 1, start.BuildSortablePosition(), false);
        if (!foundStart) {
            // All the remaining records precede this interval and the following ones.
            break;
        }
        const ui32 first = foundStart->GetPosition();
        const auto foundFinish =
            NArrow::NMerger::TSortableBatchPosition::FindBound(position, first, recordsCount - 1, finish.BuildSortablePosition(), true);
        const ui32 end = foundFinish ? foundFinish->GetPosition() : recordsCount;
        result.Add(false, first - current);
        result.Add(true, end - first);
        current = end;
    }
    result.Add(false, recordsCount - current);
    return result;
}

}   // namespace

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
        // A deletion marker has to be kept only while some portion outside of this task may still hold an older record with the
        // same key. Elsewhere the marker (together with the records it shadows inside the task) is dropped from the result.
        const std::vector<TKeyInterval> olderIntervals =
            NGranule::NPortionsIndex::TPortionsIndex::GetOlderIntervals(*PortionsIndexSnapshot, pInfo, portionsInUsage);
        const NArrow::TSimpleRow keyStart = pInfo.IndexKeyStart();
        const NArrow::TSimpleRow keyEnd = pInfo.IndexKeyEnd();
        const bool coveredByOlder = std::any_of(olderIntervals.begin(), olderIntervals.end(), [&](const TKeyInterval& interval) {
            return interval.first <= keyStart && keyEnd <= interval.second;
        });
        if (!coveredByOlder) {
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
            if (olderIntervals.size()) {
                filterDeleted = filterDeleted.Or(BuildKeyIntervalsFilter(batch, olderIntervals));
            }
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

TWritePortionsToMerge::TWritePortionsToMerge(std::vector<TWritePortionInfoWithBlobsResult>&& portions,
    const std::shared_ptr<TGranuleMeta>& granuleMeta, const NGranule::NPortionsIndex::TPortionsIndex::TPortionsSnapshot& portionsIndexSnapshot)
    : TBase(granuleMeta, portionsIndexSnapshot)
    , WritePortions(std::move(portions))
{
    ui32 idx = 0;
    for (auto&& i : WritePortions) {
        i.GetPortionConstructor().MutablePortionConstructor().SetPortionId(++idx);
        i.GetPortionConstructor().MutablePortionConstructor().MutableMeta().SetCompactionLevel(0);
        i.RegisterFakeBlobIds();
        i.FinalizePortionConstructor(TSnapshot::Zero());
    }
}

}   // namespace NKikimr::NOlap::NCompaction

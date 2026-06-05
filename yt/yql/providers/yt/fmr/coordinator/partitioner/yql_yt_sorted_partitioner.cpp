#include "yql_yt_sorted_partitioner.h"

#include <yt/yql/providers/yt/fmr/utils/comparator/yql_yt_binary_yson_compare_impl.h>

#include <unordered_set>

namespace NYql::NFmr {

TFmrTableKeysRange TSortedPartitioner::GetReadRangeFromSlices(const std::vector<TSlice>& slices, bool /*isLastRange*/) {
    YQL_ENSURE(!slices.empty());
    TFmrTableKeysRange taskRange{.IsEmpty = false};
    auto minSliceReadRange = slices[0].RangeForRead, maxSliceReadRange = slices[slices.size() - 1].RangeForRead;
    YQL_ENSURE(minSliceReadRange.FirstKeysBound.Defined());
    YQL_ENSURE(maxSliceReadRange.LastKeysBound.Defined());
    taskRange.SetFirstKeysBound(*minSliceReadRange.FirstKeysBound, minSliceReadRange.IsFirstBoundInclusive);
    taskRange.SetLastKeysBound(*maxSliceReadRange.LastKeysBound, maxSliceReadRange.IsLastBoundInclusive);
    return taskRange;
}

TTaskTableInputRef TSortedPartitioner::CreateTaskInputFromSlices(
    const std::vector<TSlice>& slices,
    const std::vector<TFmrTableRef>& inputTables,
    bool isLastRange
) {
    return CreateTaskInputFromSlicesImpl(slices, inputTables, isLastRange);
}

void TSortedPartitioner::ChangeLeftKeyBoundaryIfNeeded(
    TFmrTableKeysBoundary& leftKey,
    bool& leftInclusive,
    const TPartitionerFilterBoundary& filterBoundary
) {
    if (filterBoundary.FilterBoundary > leftKey) {
        leftKey = filterBoundary.FilterBoundary;
        leftInclusive = filterBoundary.IsInclusive;
    } else if (filterBoundary.FilterBoundary == leftKey) {
        leftInclusive = leftInclusive && filterBoundary.IsInclusive;
    }
}

void TSortedPartitioner::ChangeRightKeyBoundaryIfNeeded(TFmrTableKeysBoundary& rightKey, const TFmrTableKeysBoundary& taskRangeLastKey) {
    if (taskRangeLastKey < rightKey) {
        rightKey = taskRangeLastKey;
    }
}

void TSortedPartitioner::ExtendChunksPerTable(std::unordered_map<TString, std::vector<TChunkUnit>>&) {
    return;
}

TSortedPartitioner::TSortedPartitioner(
    const std::unordered_map<TFmrTableId, std::vector<TString>>& partIdsForTables,
    const std::unordered_map<TString, std::vector<TChunkStats>>& partIdStats,
    TSortingColumns keyColumns,
    const TSortedPartitionSettings& settings
)
    : TSortedPartitionerBase(partIdsForTables, partIdStats, keyColumns, settings.FmrPartitionSettings)
{
}

} // namespace NYql::NFmr

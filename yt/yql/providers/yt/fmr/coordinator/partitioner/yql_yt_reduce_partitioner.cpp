#include "yql_yt_reduce_partitioner.h"

#include "yql_yt_sorted_partitioner_base.h"

#include <yt/yql/providers/yt/fmr/utils/comparator/yql_yt_binary_yson_compare_impl.h>
#include <library/cpp/yson/node/node_io.h>

namespace NYql::NFmr {

namespace {

// Binary YSON key rows may contain arbitrary non-UTF8 bytes; text YSON escapes them, keeping error messages valid UTF-8.
TString FormatKeyRow(const TString& binaryYsonRow) {
    return NYT::NodeToYsonString(NYT::NodeFromYsonString(binaryYsonRow));
}

} // namespace

TReducePartitioner::TReducePartitioner(
    const std::unordered_map<TFmrTableId, std::vector<TString>>& partIdsForTables,
    const std::unordered_map<TString, std::vector<TChunkStats>>& partIdStats,
    const TSortingColumns& sortColumns,
    const TSortingColumns& groupColumns,
    const TReducePartitionSettings& settings
)
    : TSortedPartitionerBase(partIdsForTables, partIdStats, sortColumns, settings.FmrPartitionSettings)
    , SortColumns_(sortColumns)
    , GroupColumns_(groupColumns)
    , NumGroupColumns_(groupColumns.Columns.size())
    , GroupIsFullKey_(groupColumns.Columns.size() == sortColumns.Columns.size())
    , Settings_(settings)
{
    YQL_ENSURE(Settings_.MaxKeySizePerPart <= Settings_.FmrPartitionSettings.MaxDataWeightPerPart);
    YQL_ENSURE(GroupColumns_.Columns.size() <= SortColumns_.Columns.size(),
        "reduce group columns must be a prefix of sort columns");
}

bool TReducePartitioner::InSameReduceGroup(const TFmrTableKeysBoundary& lhs, const TFmrTableKeysBoundary& rhs) const {
    // Fast path when there is no SortBy tail: the group key is the whole sort key, so an exact
    // compare is both correct and cheapest. Otherwise compare only the group prefix, reusing the
    // already-parsed markups (no YSON round-trip).
    return GroupIsFullKey_
        ? lhs == rhs
        : CompareKeyRowPrefix(lhs, rhs, NumGroupColumns_) == 0;
}

TFmrTableKeysRange TReducePartitioner::GetReadRangeFromSlices(const std::vector<TSlice>& slices, bool isLastRange) {
    YQL_ENSURE(!slices.empty());
    TFmrTableKeysRange taskRange{.IsEmpty = false};
    auto minSliceReadRange = slices[0].RangeForRead, maxSliceReadRange = slices[slices.size() - 1].RangeForRead;
    YQL_ENSURE(minSliceReadRange.FirstKeysBound.Defined());
    YQL_ENSURE(maxSliceReadRange.LastKeysBound.Defined());

    auto taskRangeLeftBorder = *minSliceReadRange.FirstKeysBound;
    bool taskRangeLeftBorderInclusive = minSliceReadRange.IsFirstBoundInclusive;
    if (LeftBoundary_.Defined()) {
        taskRangeLeftBorder = *LeftBoundary_;
        taskRangeLeftBorderInclusive = true;
    }

    auto taskRangeRightBorder = *maxSliceReadRange.LastKeysBound;
    bool taskRangeRightBorderInclusive = maxSliceReadRange.IsLastBoundInclusive;
    LeftBoundary_ = Nothing();
    if (maxSliceReadRange.IsLastBoundInclusive) {
        // Carry the full last key to the next job and make this job's right border exclusive. The
        // reduce reader constrains task boundaries on the reduce-group prefix only (see
        // numBoundaryKeyColumns), so a full key here still keeps the whole reduce group together in
        // the next job - we only additionally have to carry that group's chunks (see the
        // group-aware collection below), which the previous exact-key match failed to do.
        LeftBoundary_ = *maxSliceReadRange.LastKeysBound;
        if (!isLastRange) {
            taskRangeRightBorderInclusive = false;
        }
    }

    YQL_ENSURE(taskRangeLeftBorderInclusive, " task range left border should always be true for reduce paritioner");

    if (taskRangeLeftBorder == taskRangeRightBorder && !taskRangeRightBorderInclusive) {
        ythrow yexception() << "Key " << FormatKeyRow(taskRangeLeftBorder.Row) << " takes all slice and is too large, cannot partition";
    }

    taskRange.SetFirstKeysBound(taskRangeLeftBorder, taskRangeLeftBorderInclusive);
    taskRange.SetLastKeysBound(taskRangeRightBorder, taskRangeRightBorderInclusive);

    // Carry every chunk that reaches into the boundary reduce group (compared on the group prefix,
    // not the full sort key) so the next job - which now owns the whole group - gets all of its
    // chunks, including build-side chunks whose last row is an early sortBy tiebreaker value.
    std::unordered_map<TString, std::vector<TChunkUnit>> chunksByTable;
    std::unordered_map<TString, std::unordered_set<TString>> seenChunksByTable;

    for (const auto& slice : slices) {
        for (const auto& [tableId, sliceChunks] : slice.ChunksByTable) {
            auto& out = chunksByTable[tableId];
            auto& seen = seenChunksByTable[tableId];
            for (const auto& chunk : sliceChunks) {
                if (!chunk.KeyRange.IsLastBoundInclusive || !InSameReduceGroup(*chunk.KeyRange.LastKeysBound, taskRangeRightBorder)) {
                    continue;
                }

                TString chunkKey = TStringBuilder() << chunk.PartId << "#" << chunk.ChunkIndex;
                if (!seen.insert(chunkKey).second) {
                    continue;
                }
                out.push_back(chunk);
            }
        }
    }
    LeftBoundaryChunks_.push(chunksByTable);

    return taskRange;
}

void TReducePartitioner::CheckMaxKeySizePerSlices(const std::vector<TSlice>& slices) {
    ui64 maxKeySizePerPart = Settings_.MaxKeySizePerPart;
    YQL_ENSURE(!slices.empty());
    TMaybe<TFmrTableKeysBoundary> curLeftBorder;
    ui64 currentFilledSlicesWeight = CurrentLastKeyWeight_;
    for (const auto& slice: slices) {
        if (!curLeftBorder.Defined()) {
            curLeftBorder = slice.RangeForRead.FirstKeysBound;
        }
        YQL_ENSURE(curLeftBorder.Defined());
        YQL_ENSURE(slice.RangeForRead.LastKeysBound.Defined());
        auto curRightBorder = *slice.RangeForRead.LastKeysBound;

        if (*curLeftBorder != curRightBorder) {
            if (currentFilledSlicesWeight > maxKeySizePerPart) {
                ythrow yexception() << "Key " << FormatKeyRow(curLeftBorder->Row) << " weighs at least " << currentFilledSlicesWeight << " bytes which is larget than maxKeySizePerPart - " << maxKeySizePerPart << " - cannot partition";
            }
            curLeftBorder = Nothing();
            currentFilledSlicesWeight = 0;
        } else {
            currentFilledSlicesWeight += slice.Weight;
        }
    }
    if (currentFilledSlicesWeight > maxKeySizePerPart) {
        ythrow yexception() << "Key " << FormatKeyRow(curLeftBorder->Row) << " weighs at least " << currentFilledSlicesWeight << " bytes which is larget than maxKeySizePerPart - " << maxKeySizePerPart;
    }
    CurrentLastKeyWeight_ = currentFilledSlicesWeight;
}

void TReducePartitioner::ChangeLeftKeyBoundaryIfNeeded(
    TFmrTableKeysBoundary&,
    bool& isLeftInclusive,
    const TPartitionerFilterBoundary&
) {
    isLeftInclusive = true;
}

void TReducePartitioner::ChangeRightKeyBoundaryIfNeeded(
    TFmrTableKeysBoundary& rightKey,
    bool& /*isRightInclusive*/,
    const TFmrTableKeysBoundary& taskRangeLastKey
) {
    rightKey = taskRangeLastKey;
}

void TReducePartitioner::ExtendChunksPerTable(std::unordered_map<TString, std::vector<TChunkUnit>>& chunksByTable) {
    if (LeftBoundaryChunks_.size() < 2) {
        return;
    }

    auto leftBoundaryChunks = LeftBoundaryChunks_.front();
    LeftBoundaryChunks_.pop();
    if (leftBoundaryChunks.empty()) {
        return;
    }

    for (auto& [tableId, chunks]: leftBoundaryChunks) {
        if (!chunksByTable.contains(tableId)) {
            chunksByTable[tableId] = chunks;
        } else {
            // extending current chunks for table with all other non-intersecting elements from LeftBoundaryChunks_.
            auto currentChunksByTable = chunksByTable[tableId];
            std::unordered_set<TString> chunkKeys;
            std::vector<TChunkUnit> neededChunks;
            for (auto& chunk: currentChunksByTable) {
                TString chunkKey = TStringBuilder() << chunk.PartId << "#" << chunk.ChunkIndex;
                chunkKeys.emplace(chunkKey);
            }
            for (auto& chunk: chunks) {
                auto curChunkKey = TStringBuilder() << chunk.PartId << "#" << chunk.ChunkIndex;
                if (!chunkKeys.contains(curChunkKey)) {
                    neededChunks.emplace_back(chunk);
                    chunkKeys.emplace(curChunkKey);
                }
            }
            // since all needed chunks are from left boundary, write them to beginning of existing chunks.
            neededChunks.insert(neededChunks.end(), currentChunksByTable.begin(), currentChunksByTable.end());
            chunksByTable[tableId] = neededChunks;
        }
    }
}

TTaskTableInputRef TReducePartitioner::CreateTaskInputFromSlices(
    const std::vector<TSlice>& slices,
    const std::vector<TFmrTableRef>& inputTables,
    bool isLastRange
) {
    CheckMaxKeySizePerSlices(slices);
    return CreateTaskInputFromSlicesImpl(slices, inputTables, isLastRange);
}

} // namespace NYql::NFmr

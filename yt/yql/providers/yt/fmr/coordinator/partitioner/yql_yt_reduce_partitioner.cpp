#include "yql_yt_reduce_partitioner.h"

#include "yql_yt_sorted_partitioner_base.h"

#include <yt/yql/providers/yt/fmr/utils/comparator/yql_yt_binary_yson_compare_impl.h>

namespace NYql::NFmr {

TReducePartitioner::TReducePartitioner(
    const std::unordered_map<TFmrTableId, std::vector<TString>>& partIdsForTables,
    const std::unordered_map<TString, std::vector<TChunkStats>>& partIdStats,
    const TSortingColumns& reduceBy,
    const TReducePartitionSettings& settings
)
    : TSortedPartitionerBase(partIdsForTables, partIdStats, reduceBy, settings.FmrPartitionSettings)
    , ReduceBy_(reduceBy)
    , Settings_(settings)
{
    YQL_ENSURE(Settings_.MaxKeySizePerPart <= Settings_.FmrPartitionSettings.MaxDataWeightPerPart);
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
        // don't take last key if inclusive, because we need to guarantee that all reduce keys are in the same job.
        LeftBoundary_ = *maxSliceReadRange.LastKeysBound;
        if (!isLastRange) {
            taskRangeRightBorderInclusive = false;
        }
    }

    YQL_ENSURE(taskRangeLeftBorderInclusive, " task range left border should always be true for reduce paritioner");

    if (taskRangeLeftBorder == taskRangeRightBorder && !taskRangeRightBorderInclusive) {
        ythrow yexception() << "Key " << taskRangeLeftBorder.Row << " takes all slice and is too large, cannot partition";
    }

    taskRange.SetFirstKeysBound(taskRangeLeftBorder, taskRangeLeftBorderInclusive);
    taskRange.SetLastKeysBound(taskRangeRightBorder, taskRangeRightBorderInclusive);

    std::unordered_map<TString, std::vector<TChunkUnit>> chunksByTable;
    std::unordered_map<TString, std::unordered_set<TString>> seenChunksByTable;

    for (const auto& slice : slices) {
        for (const auto& [tableId, sliceChunks] : slice.ChunksByTable) {
            auto& out = chunksByTable[tableId];
            auto& seen = seenChunksByTable[tableId];
            for (const auto& chunk : sliceChunks) {
                if (chunk.KeyRange.LastKeysBound != taskRangeRightBorder || !chunk.KeyRange.IsLastBoundInclusive) {
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
                ythrow yexception() << "Key " << curLeftBorder->Row << " weighs at least " << currentFilledSlicesWeight << " bytes which is larget than maxKeySizePerPart - " << maxKeySizePerPart << " - cannot partition";
            }
            curLeftBorder = Nothing();
            currentFilledSlicesWeight = 0;
        } else {
            currentFilledSlicesWeight += slice.Weight;
        }
    }
    if (currentFilledSlicesWeight > maxKeySizePerPart) {
        ythrow yexception() << "Key " << curLeftBorder->Row << " weighs at least " << currentFilledSlicesWeight << " bytes which is larget than maxKeySizePerPart - " << maxKeySizePerPart;
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

void TReducePartitioner::ChangeRightKeyBoundaryIfNeeded(TFmrTableKeysBoundary& rightKey, const TFmrTableKeysBoundary& taskRangeLastKey) {
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

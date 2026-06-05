#include "yql_yt_sorted_partitioner_base.h"

#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {


void TChunkContainer::Push(TChunkUnit chunk) {
    Chunks_.push_back(std::move(chunk));
}

bool TChunkContainer::IsEmpty() const {
    return Chunks_.empty();
}

const std::vector<TChunkUnit>& TChunkContainer::GetChunks() const {
    return Chunks_;
}

void TChunkContainer::Clear() {
    Chunks_.clear();
    KeysRange_ = TFmrTableKeysRange{.IsEmpty = true};
}

const TFmrTableKeysRange& TChunkContainer::GetKeysRange() const {
    return KeysRange_;
}

void TChunkContainer::UpdateKeyRange(const TFmrTableKeysRange& KeyRange) {
    if (KeyRange.IsEmpty) {
        return;
    }

    if (KeysRange_.IsEmpty) {
        KeysRange_.IsEmpty = false;
        KeysRange_.SetFirstKeysBound(*KeyRange.FirstKeysBound, KeyRange.IsFirstBoundInclusive);
        KeysRange_.SetLastKeysBound(*KeyRange.LastKeysBound, KeyRange.IsLastBoundInclusive);
    }

    if (!KeysRange_.IsFirstKeySet()) {
        KeysRange_.SetFirstKeysBound(*KeyRange.FirstKeysBound, KeyRange.IsFirstBoundInclusive);
    } else {
        bool isInclusive = true;
        if (*KeysRange_.FirstKeysBound < *KeyRange.FirstKeysBound) {
            isInclusive = KeysRange_.IsFirstBoundInclusive;
        } else if (*KeysRange_.FirstKeysBound > *KeyRange.FirstKeysBound) {
            isInclusive = KeyRange.IsFirstBoundInclusive;
        } else {
            isInclusive = KeysRange_.IsFirstBoundInclusive || KeyRange.IsFirstBoundInclusive;
        }
        KeysRange_.SetFirstKeysBound(std::min(*KeysRange_.FirstKeysBound, *KeyRange.FirstKeysBound), isInclusive);
    }

    if (!KeysRange_.IsLastKeySet()) {
        KeysRange_.SetLastKeysBound(*KeyRange.LastKeysBound, KeyRange.IsLastBoundInclusive);
    } else {
        bool isInclusive = true;
        if (*KeysRange_.LastKeysBound < *KeyRange.LastKeysBound) {
            isInclusive = KeysRange_.IsLastBoundInclusive;
        } else if (*KeysRange_.LastKeysBound > *KeyRange.LastKeysBound) {
            isInclusive = KeyRange.IsLastBoundInclusive;
        } else {
            isInclusive = KeysRange_.IsLastBoundInclusive && KeyRange.IsLastBoundInclusive;
        }
        KeysRange_.SetLastKeysBound(std::min(*KeysRange_.LastKeysBound, *KeyRange.LastKeysBound), isInclusive);
    }
}

TFmrTablesChunkPool::TFmrTablesChunkPool(
    const std::vector<TFmrTableRef>& inputTables,
    const std::unordered_map<TFmrTableId, std::vector<TString>>& partIdsForTables,
    const std::unordered_map<TString, std::vector<TChunkStats>>& partIdStats,
    const TSortingColumns& KeyColumns
)
    : PartIdsForTables_(partIdsForTables)
    , PartIdStats_(partIdStats)
    , KeyColumns_(KeyColumns)
{
    InitTableInputs(inputTables);
}

void TFmrTablesChunkPool::InitTableInputs(const std::vector<TFmrTableRef>& inputTables) {
    TableOrder_.clear();
    TableOrder_.reserve(inputTables.size());

    for (const auto& table : inputTables) {
        const TString& tableId = table.FmrTableId.Id;
        Y_ENSURE(
            PartIdsForTables_.contains(tableId),
            "No partitions metadata found for input FMR table: " << tableId);

        const auto& partIds = PartIdsForTables_.at(tableId);
        Y_ENSURE(
            !partIds.empty(),
            "SortedPartitioner requires at least one partition for input table: " << tableId);
        TableOrder_.push_back(tableId);

        std::vector<TChunkUnit> chunks;
        ui64 tableChunkCount = 0;
        for (const auto& partId : partIds) {
            Y_ENSURE(
                PartIdStats_.contains(partId),
                "No chunk stats found for partId: " << partId << " (table: " << tableId << ")");

            const auto& stats = PartIdStats_.at(partId);
            tableChunkCount += stats.size();
            for (ui64 chunkIdx = 0; chunkIdx < stats.size(); ++chunkIdx) {
                const auto& chunk = stats[chunkIdx];
                Y_ENSURE(chunk.SortedChunkStats.IsSorted, "Every FMR chunk inside SortedPartitioner must be sorted");
                if (chunk.SortedChunkStats.FirstRowKeys.IsUndefined() || chunk.SortedChunkStats.LastRowKeys.IsUndefined()) {
                    Error_ = TFmrError{
                        .Component = EFmrComponent::Coordinator,
                        .Reason = EFmrErrorReason::RestartQuery,
                        .ErrorMessage = "Every FMR chunk inside SortedPartitioner must have first and last key bounds"
                    };
                    return;
                }

                auto firstBound = MakeKeyBound(chunk.SortedChunkStats.FirstRowKeys, KeyColumns_);
                auto lastBound = MakeKeyBound(chunk.SortedChunkStats.LastRowKeys, KeyColumns_);
                TFmrTableKeysRange KeyRange{.IsEmpty = false};
                KeyRange.SetFirstKeysBound(std::move(firstBound), true);
                KeyRange.SetLastKeysBound(std::move(lastBound), true);

                chunks.push_back(TChunkUnit{
                    .TableId = tableId,
                    .PartId = partId,
                    .ChunkIndex = chunkIdx,
                    .DataWeight = chunk.DataWeight,
                    .KeyRange = KeyRange,
                });
            }
        }
        Y_ENSURE(
            tableChunkCount > 0,
            "SortedPartitioner requires at least one chunk for input table: " << tableId);

        std::stable_sort(chunks.begin(), chunks.end(), [](const TChunkUnit& a, const TChunkUnit& b) {
            if (*a.KeyRange.FirstKeysBound != *b.KeyRange.FirstKeysBound) {
                return *a.KeyRange.FirstKeysBound < *b.KeyRange.FirstKeysBound;
            }
            return *a.KeyRange.LastKeysBound < *b.KeyRange.LastKeysBound;
        });

        TableInputs_[tableId] = std::deque<TChunkUnit>(chunks.begin(), chunks.end());

        if (!chunks.empty()) {
            TPartitionerFilterBoundary FilterBoundary{.FilterBoundary = *chunks.front().KeyRange.FirstKeysBound, .IsInclusive = true};
            FilterBoundaries_[tableId] = FilterBoundary;
        }
    }
}

void TFmrTablesChunkPool::SetError(TFmrError error) {
    Error_ = std::move(error);
}

TMaybe<TFmrError> TFmrTablesChunkPool::GetError() const {
    return Error_;
}

bool TFmrTablesChunkPool::IsNotEmpty() const {
    return std::any_of(TableInputs_.begin(), TableInputs_.end(),
        [](const auto& pair) { return !pair.second.empty(); });
}

void TFmrTablesChunkPool::PutBack(TChunkUnit chunk) {
    TableInputs_[chunk.TableId].push_front(std::move(chunk));
}

void TFmrTablesChunkPool::UpdateFilterBoundary(const TString& tableId, const TPartitionerFilterBoundary& filterBoundary) {
    auto [it, inserted] = FilterBoundaries_.try_emplace(tableId, filterBoundary);
    if (!inserted) {
        it->second = GetMaxFilterBoundary(tableId, filterBoundary);
    }
}

TMaybe<TPartitionerFilterBoundary> TFmrTablesChunkPool::GetFilterBoundary(const TString& tableId) const {
    auto it = FilterBoundaries_.find(tableId);
    if (it == FilterBoundaries_.end()) {
        return Nothing();
    }
    return TMaybe<TPartitionerFilterBoundary>(it->second);
}

TFmrTableKeysRange TFmrTablesChunkPool::GetEffectiveKeysRange(const TChunkUnit& chunk) const {
    auto FilterBoundary = GetFilterBoundary(chunk.TableId);
    if (FilterBoundary.Defined()) {
        auto tmpFilterBoundary = TPartitionerFilterBoundary{.FilterBoundary=*chunk.KeyRange.FirstKeysBound, .IsInclusive=true};
        const auto maxKey = GetMaxFilterBoundary(chunk.TableId, tmpFilterBoundary);
        TFmrTableKeysRange out{.IsEmpty = false};
        out.SetFirstKeysBound(maxKey.FilterBoundary, maxKey.IsInclusive);
        out.SetLastKeysBound(*chunk.KeyRange.LastKeysBound, chunk.KeyRange.IsLastBoundInclusive);
        return out;
    }
    return chunk.KeyRange;
}

const std::vector<TString>& TFmrTablesChunkPool::GetTableOrder() const {
    return TableOrder_;
}

TMaybe<TChunkUnit> TFmrTablesChunkPool::ReadNextChunk(const TString& tableId) {
    auto it = TableInputs_.find(tableId);
    if (it == TableInputs_.end()) {
        return Nothing();
    }
    auto& chunkQueue = it->second;
    if (chunkQueue.empty()) {
        return Nothing();
    }

    TChunkUnit chunk = chunkQueue.front();
    chunkQueue.pop_front();
    return chunk;
}

TPartitionerFilterBoundary TFmrTablesChunkPool::GetMaxFilterBoundary(const TString& tableId, const TPartitionerFilterBoundary& filterBoundary) const {
    auto it = FilterBoundaries_.find(tableId);
    if (it == FilterBoundaries_.end() || it->second.FilterBoundary < filterBoundary.FilterBoundary) {
        return filterBoundary;
    } else if (it->second.FilterBoundary == filterBoundary.FilterBoundary) {
        if (!filterBoundary.IsInclusive) {
            return filterBoundary;
        } else {
            return it->second;
        }
    }
    return it->second;
}

TSortedPartitionerBase::TSortedPartitionerBase(
    const std::unordered_map<TFmrTableId, std::vector<TString>>& partIdsForTables,
    const std::unordered_map<TString, std::vector<TChunkStats>>& partIdStats,
    const TSortingColumns& keyColumns,
    const TFmrPartitionerSettings& fmrPartitionSettings
)
    : PartIdsForTables_(partIdsForTables)
    , PartIdStats_(partIdStats)
    , KeyColumns_(keyColumns)
    , FmrPartitionSettings_(fmrPartitionSettings)
{
}

TReadSliceResult TSortedPartitionerBase::ReadSlice(TFmrTablesChunkPool& chunkPool) {
    TChunkContainer container;
    std::vector<TFmrTableKeysRange> effectiveRanges;
    effectiveRanges.reserve(chunkPool.GetTableOrder().size());

    for (const auto& tableId : chunkPool.GetTableOrder()) {
        auto chunk = chunkPool.ReadNextChunk(tableId);
        if (!chunk.Defined()) {
            continue;
        }
        auto effectiveRange = chunkPool.GetEffectiveKeysRange(*chunk);
        container.Push(std::move(*chunk));
        container.UpdateKeyRange(effectiveRange);
        effectiveRanges.push_back(std::move(effectiveRange));
    }

    if (container.IsEmpty()) {
        return TReadSliceResult{};
    }
    const TFmrTableKeysRange& rangeForRead = container.GetKeysRange();

    const TFmrTableKeysBoundary& sepKey = *rangeForRead.LastKeysBound;

    TSlice slice;
    slice.RangeForRead = rangeForRead;

    const auto& chunks = container.GetChunks();

    for (size_t i = 0; i < chunks.size(); ++i) {
        const auto& chunk = chunks[i];
        const auto& effectiveRange = effectiveRanges[i];

        slice.PerTableLeft[chunk.TableId] = TPartitionerFilterBoundary{
            .FilterBoundary = *effectiveRange.FirstKeysBound,
            .IsInclusive = effectiveRange.IsFirstBoundInclusive
        };

        const TFmrTableKeysRange intersection = rangeForRead.GetIntersection(effectiveRange);
        if (intersection.IsEmpty) {
            chunkPool.PutBack(chunk);
            continue;
        }

        slice.ChunksByTable[chunk.TableId].push_back(chunk);

        const TFmrTableKeysBoundary& interFirst = *intersection.FirstKeysBound;
        const TFmrTableKeysBoundary& effLast = *effectiveRange.LastKeysBound;

        if (interFirst <= sepKey && sepKey < effLast) {
            chunkPool.UpdateFilterBoundary(chunk.TableId, TPartitionerFilterBoundary{.FilterBoundary = sepKey, .IsInclusive = false});
            chunkPool.PutBack(chunk);
        } else if (interFirst <= sepKey && sepKey == effLast) {
            slice.Weight += chunk.DataWeight;
            chunkPool.UpdateFilterBoundary(chunk.TableId, TPartitionerFilterBoundary{.FilterBoundary = sepKey, .IsInclusive = true});
        } else {
            YQL_CLOG(WARN, FastMapReduce) << "FMR fallback to YT: undefined behaviour in ReadSlice, intersection doesn't reach separator key";
            return TReadSliceResult{.Error = TFmrError{
                .Component = EFmrComponent::Coordinator,
                .Reason = EFmrErrorReason::FallbackOperation,
                .ErrorMessage = "Undefined behaviour in ReadSlice: intersection doesn't reach separator key"
            }};
        }
    }

    return TReadSliceResult{.Slice = std::move(slice)};
}

ui64 TSortedPartitionerBase::CollectFmrTotalWeight(const std::vector<TFmrTableRef>& inputTables) {
    ui64 totalWeight = 0;

    for (const auto& table : inputTables) {
        const TString& tableId = table.FmrTableId.Id;
        const auto& partIds = PartIdsForTables_.at(tableId);
        for (const auto& partId : partIds) {
            const auto& stats = PartIdStats_.at(partId);
            for (const auto& chunkStat : stats) {
                totalWeight += chunkStat.DataWeight;
            }
        }
    }

    return totalWeight;
}

TTaskTableInputRef TSortedPartitionerBase::CreateTaskInputFromSlicesImpl(
    const std::vector<TSlice>& slices,
    const std::vector<TFmrTableRef>& inputTables,
    bool isLastRange
) {
    TFmrTableKeysRange taskRange = GetReadRangeFromSlices(slices, isLastRange);
    std::unordered_map<TString, std::vector<TChunkUnit>> chunksByTable;
    std::unordered_map<TString, TPartitionerFilterBoundary> perTableLeft;
    std::unordered_map<TString, std::unordered_set<TString>> seenChunksByTable;

    for (const auto& slice : slices) {
        for (const auto& [tableId, sliceChunks] : slice.ChunksByTable) {
            auto& out = chunksByTable[tableId];
            auto& seen = seenChunksByTable[tableId];
            for (const auto& chunk : sliceChunks) {
                TString chunkKey = TStringBuilder() << chunk.PartId << "#" << chunk.ChunkIndex;
                if (!seen.insert(chunkKey).second) {
                    continue;
                }
                out.push_back(chunk);
            }
        }

        for (const auto& [tableId, sliceFilterBoundary] : slice.PerTableLeft) {
            auto it = perTableLeft.find(tableId);
            if (it == perTableLeft.end()) {
                perTableLeft.emplace(tableId, sliceFilterBoundary);
                continue;
            }
            auto& currentFilterBoundary = it->second;
            if (sliceFilterBoundary.FilterBoundary < currentFilterBoundary.FilterBoundary) {
                currentFilterBoundary = sliceFilterBoundary;
            } else if (sliceFilterBoundary.FilterBoundary == currentFilterBoundary.FilterBoundary) {
                currentFilterBoundary.IsInclusive = currentFilterBoundary.IsInclusive || sliceFilterBoundary.IsInclusive;
            }
        }
    }

    ExtendChunksPerTable(chunksByTable);

    TTaskTableInputRef taskInput;
    taskInput.Inputs.reserve(inputTables.size());

    const TFmrTableKeysBoundary& taskLastKey = *taskRange.LastKeysBound;

    for (const auto& t : inputTables) {
        const TString& tableId = t.FmrTableId.Id;
        auto it = chunksByTable.find(tableId);
        if (it == chunksByTable.end() || it->second.empty()) {
            continue;
        }
        const auto& chunks = it->second;

        std::vector<TTableRange> tableRanges;
        tableRanges.reserve(chunks.size());
        std::unordered_map<TString, size_t> byPartIndex;
        byPartIndex.reserve(chunks.size());
        std::unordered_map<TString, std::vector<ui64>> chunkIndexesByPart;
        chunkIndexesByPart.reserve(chunks.size());
        for (const auto& c : chunks) {
            auto [it, inserted] = byPartIndex.try_emplace(c.PartId, tableRanges.size());
            if (inserted) {
                tableRanges.push_back(TTableRange{
                    .PartId = c.PartId,
                    .MinChunk = c.ChunkIndex,
                    .MaxChunk = c.ChunkIndex + 1
                });
            } else {
                auto& range = tableRanges[it->second];
                range.MinChunk = std::min(range.MinChunk, c.ChunkIndex);
                range.MaxChunk = std::max(range.MaxChunk, c.ChunkIndex + 1);
            }
            chunkIndexesByPart[c.PartId].push_back(c.ChunkIndex);
        }

        TFmrTableKeysBoundary leftKey = *taskRange.FirstKeysBound;
        bool leftInclusive = taskRange.IsFirstBoundInclusive;
        auto FilterBoundaryIt = perTableLeft.find(tableId);
        if (FilterBoundaryIt != perTableLeft.end()) {
            ChangeLeftKeyBoundaryIfNeeded(leftKey, leftInclusive, FilterBoundaryIt->second);
        }

        TFmrTableKeysBoundary rightKey = *chunks.back().KeyRange.LastKeysBound;
        bool rightInclusive = taskRange.IsLastBoundInclusive;
        ChangeRightKeyBoundaryIfNeeded(rightKey, taskLastKey);

        TFmrTableInputRef inputRef{
            .TableId = tableId,
            .TableRanges = std::move(tableRanges),
            .Columns = t.Columns,
            .SerializedColumnGroups = t.SerializedColumnGroups,
            .IsFirstRowInclusive = leftInclusive,
            .IsLastRowInclusive = rightInclusive,
            .FirstRowKeys = TString(leftKey.Row),
            .LastRowKeys = TString(rightKey.Row),
        };
        taskInput.Inputs.emplace_back(std::move(inputRef));
    }

    return taskInput;
}

TPartitionResult TSortedPartitionerBase::PartitionTablesIntoTasks(
    const std::vector<TOperationTableRef>& inputTables
) {
    if (inputTables.empty()) {
        return TPartitionResult{};
    }

    std::vector<TFmrTableRef> inputFmrTables;
    inputFmrTables.reserve(inputTables.size());
    for (const auto& table : inputTables) {
        if (auto fmrTable = std::get_if<TFmrTableRef>(&table)) {
            inputFmrTables.push_back(*fmrTable);
        } else {
            return TPartitionResult{.Error = TFmrError{
                .Component = EFmrComponent::Coordinator,
                .Reason = EFmrErrorReason::RestartQuery,
                .ErrorMessage = "Unsupported table type for partitioner: only FMR tables are supported"
            }};
        }
    }

    auto result = PartitionFmrTables(inputFmrTables);
    if (!result.Error) {
        YQL_CLOG(DEBUG, FastMapReduce) << "Successfully partitioned " << inputTables.size()
                                       << " input tables (sorted) into " << result.TaskInputs.size() << " tasks";
    }
    return result;
}

TPartitionResult TSortedPartitionerBase::PartitionFmrTables(const std::vector<TFmrTableRef>& inputTables) {
    Y_ENSURE(FmrPartitionSettings_.MaxDataWeightPerPart > 0, "MaxDataWeightPerPart must be > 0");
    Y_ENSURE(!KeyColumns_.Columns.empty(), "KeyColumns must be set");

    std::vector<TTaskTableInputRef> tasks;
    const ui64 maxParts = FmrPartitionSettings_.MaxParts;

    TFmrTablesChunkPool chunkPool(inputTables, PartIdsForTables_, PartIdStats_, KeyColumns_);
    if (auto error = chunkPool.GetError()) {
        return TPartitionResult{.Error = *error};
    }

    ui64 maxWeight = FmrPartitionSettings_.MaxDataWeightPerPart;
    if (FmrPartitionSettings_.AdjustDataWeightPerPartition) {
        ui64 totalWeight = CollectFmrTotalWeight(inputTables);
        ui64 estimatedParts = (totalWeight + maxWeight - 1) / maxWeight;
        if (estimatedParts > maxParts && maxParts > 0) {
            maxWeight = totalWeight / maxParts;
            YQL_CLOG(INFO, FastMapReduce) << "AdjustDataWeightPerPartition (sorted): adjusted MaxDataWeightPerPart from "
                << FmrPartitionSettings_.MaxDataWeightPerPart << " to " << maxWeight
                << " (totalWeight=" << totalWeight << ", maxParts=" << maxParts << ")";
        }
    }

    std::vector<TSlice> currentSlices;
    ui64 currentWeight = 0;

    try {
        while (chunkPool.IsNotEmpty()) {
            auto sliceResult = ReadSlice(chunkPool);
            if (sliceResult.Error) {
                return TPartitionResult{.Error = *sliceResult.Error};
            }
            if (!sliceResult.Slice.Defined()) {
                break;
            }
            auto& slice = *sliceResult.Slice;
            if (!currentSlices.empty() && currentWeight + slice.Weight > maxWeight) {
                tasks.emplace_back(CreateTaskInputFromSlices(currentSlices, inputTables, false));
                currentSlices.clear();
                currentWeight = 0;
            }
            currentWeight += slice.Weight;
            currentSlices.push_back(std::move(slice));
        }
        if (!currentSlices.empty()) {
            tasks.emplace_back(CreateTaskInputFromSlices(currentSlices, inputTables, true));
        }
    } catch (...) {
        return TPartitionResult{
            .Error = TFmrError {
                .Component = EFmrComponent::Coordinator,
                .Reason = EFmrErrorReason::FallbackOperation,
                .ErrorMessage = CurrentExceptionMessage()
            }
        };
    }

    if (maxParts < tasks.size()) {
        return TPartitionResult{.Error = TFmrError{
            .Component = EFmrComponent::Coordinator,
            .Reason = EFmrErrorReason::RestartQuery,
            .ErrorMessage = TStringBuilder() << "Partitioner produced " << tasks.size()
                            << " tasks which exceeds max_parts=" << maxParts
        }};
    }

    return TPartitionResult{.TaskInputs = tasks};
}

} // namespace NYql::NFmr

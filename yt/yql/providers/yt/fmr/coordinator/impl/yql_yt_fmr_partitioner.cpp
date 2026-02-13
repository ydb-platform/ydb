#include "yql_yt_fmr_partitioner.h"
#include <library/cpp/iterator/enumerate.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yt/cpp/mapreduce/common/helpers.h>

namespace NYql::NFmr {

TFmrPartitioner::TFmrPartitioner(
    const std::unordered_map<TFmrTableId, std::vector<TString>>& partIdsForTables,
    const std::unordered_map<TString, std::vector<TChunkStats>>& partIdStats,
    const TFmrPartitionerSettings& settings
)
    : PartIdsForTables_(partIdsForTables), PartIdStats_(partIdStats), Settings_(settings)
{
}

std::pair<std::vector<TTaskTableInputRef>, bool> TFmrPartitioner::PartitionFmrTablesIntoTasks(const std::vector<TFmrTableRef>& fmrTables) {
    // TODO - return matrix with ranges for all tables, in order to support table_index correctly.
    if (fmrTables.empty()) {
        return {{}, true};
    }
    const ui64 maxDataWeightPerPart = Settings_.MaxDataWeightPerPart;
    std::vector<TTaskTableInputRef> currentFmrTasks;
    std::vector<TLeftoverRange> leftoverRanges;
    // First try to create tasks in which all chunks have the same partId, then handle leftovers (end of chunks for each partId)
    for (const auto& fmrTable: fmrTables) {
        YQL_ENSURE(PartIdsForTables_.contains(fmrTable.FmrTableId));
        auto partIds = PartIdsForTables_.at(fmrTable.FmrTableId);
        for (auto& partId: partIds) {
            std::vector<TChunkStats> stats = PartIdStats_.at(partId);
            HandleFmrPartition(fmrTable, partId, stats, maxDataWeightPerPart, currentFmrTasks, leftoverRanges);
            if (!CheckMaxTasksSize(currentFmrTasks)) {
                return {{}, false};
            }
        }
    }
    HandleFmrLeftoverRanges(maxDataWeightPerPart, currentFmrTasks, leftoverRanges);
    if (!CheckMaxTasksSize(currentFmrTasks)) {
        return {{}, false};
    }
    return {currentFmrTasks, true};
}

void TFmrPartitioner::HandleFmrPartition(
    const TFmrTableRef& fmrTable,
    const TString& partId,
    const std::vector<TChunkStats> stats,
    ui64 maxDataWeightPerPart,
    std::vector<TTaskTableInputRef>& currentFmrTasks,
    std::vector<TLeftoverRange>& leftoverRanges
) {
    ui64 curDataWeight = 0;
    i64 curMinChunk = -1;

    for (ui64 i = 0; i < stats.size();) {
        if (curDataWeight + stats[i].DataWeight <= maxDataWeightPerPart) {
            // check if we can add this chunk to current task, or have to split
            curDataWeight += stats[i].DataWeight;
            if (curMinChunk == -1) {
                curMinChunk = i;
            }
            ++i;
        } else {
            if (curMinChunk != -1) {
                std::vector<TTableRange> tableRange{TTableRange{.PartId = partId, .MinChunk = static_cast<ui64>(curMinChunk), .MaxChunk = i}};
                TFmrTableInputRef fmrTableInput{.TableId = fmrTable.FmrTableId.Id, .TableRanges = tableRange, .Columns = fmrTable.Columns, .SerializedColumnGroups = fmrTable.SerializedColumnGroups};
                currentFmrTasks.emplace_back(TTaskTableInputRef{.Inputs = {fmrTableInput}});
            }
            curMinChunk = -1;
            curDataWeight = 0;
            ui64 j = i;
            while (j < stats.size()) {
                // iterate to create separate tasks for all chunks which are larger then maxDataWeight
                if (stats[j].DataWeight < maxDataWeightPerPart) {
                    break;
                }
                std::vector<TTableRange> tableRange{TTableRange{.PartId = partId, .MinChunk = j, .MaxChunk = j + 1}};
                TFmrTableInputRef fmrTableInput{.TableId = fmrTable.FmrTableId.Id, .TableRanges = tableRange, .Columns = fmrTable.Columns, .SerializedColumnGroups = fmrTable.SerializedColumnGroups};
                currentFmrTasks.emplace_back(TTaskTableInputRef{.Inputs = {fmrTableInput}});
                ++j;
            }
            i = j;
        }
    }

    if (curMinChunk != -1) {
        TTableRange leftoverTableRange{.PartId = partId, .MinChunk = static_cast<ui64>(curMinChunk), .MaxChunk = stats.size()};
        leftoverRanges.emplace_back(TLeftoverRange{.TableId = fmrTable.FmrTableId.Id, .TableRange = leftoverTableRange, .DataWeight = curDataWeight, .Columns = fmrTable.Columns, .SerializedColumnGroups = fmrTable.SerializedColumnGroups});
    }
}

void TFmrPartitioner::HandleFmrLeftoverRanges(
    ui64 maxDataWeightPerPart,
    std::vector<TTaskTableInputRef>& fmrTasks,
    std::vector<TLeftoverRange>& leftoverRanges
) {
    if (leftoverRanges.empty()) {
        return;
    }

    TTaskTableInputRef currentTask{};
    ui64 curDataWeight = 0;
    TFmrTableInputRef curFmrTable;
    TString curTableId;
    for (auto& range: leftoverRanges) {
        if (curDataWeight + range.DataWeight > maxDataWeightPerPart) {
            if (curFmrTable != TFmrTableInputRef()) {
                currentTask.Inputs.emplace_back(curFmrTable);
                curFmrTable = TFmrTableInputRef();
                curTableId = range.TableId;
            }
            fmrTasks.emplace_back(currentTask);
            currentTask = TTaskTableInputRef();
            curDataWeight = 0;
        }
        if (range.TableId != curTableId && curFmrTable != TFmrTableInputRef()) {
            currentTask.Inputs.emplace_back(curFmrTable);
            curFmrTable = TFmrTableInputRef();
        }
        curTableId = range.TableId;
        curFmrTable.TableId = curTableId;
        curFmrTable.TableRanges.emplace_back(range.TableRange);
        curFmrTable.Columns = range.Columns;
        curFmrTable.SerializedColumnGroups = range.SerializedColumnGroups;
        curDataWeight += range.DataWeight;
    }

    if (curFmrTable != TFmrTableInputRef()) {
        currentTask.Inputs.emplace_back(curFmrTable);
    }
    if (!currentTask.Inputs.empty()) {
        fmrTasks.emplace_back(currentTask);
    }
}

bool TFmrPartitioner::CheckMaxTasksSize(const std::vector<TTaskTableInputRef>& currentFmrTasks) {
    return currentFmrTasks.size() <= Settings_.MaxParts;
}

TPartitionResult PartitionInputTablesIntoTasks(
    const std::vector<TYtTableRef>& ytInputTables,
    const std::vector<TFmrTableRef> fmrInputTables,
    TFmrPartitioner& partitioner,
    IYtCoordinatorService::TPtr ytCoordinatorService,
    const std::unordered_map<TFmrTableId, TClusterConnection> &clusterConnections,
    const TYtPartitionerSettings& ytPartitionSettings
) {

    std::vector<TTaskTableRef> tasks;
    std::vector<TTaskTableInputRef> currentTasks;

    auto [gottenFmrTasks, fmrPartitionStatus] = partitioner.PartitionFmrTablesIntoTasks(fmrInputTables);
    if (!fmrPartitionStatus) {
        return TPartitionResult{.PartitionStatus = false};
    }
    YQL_CLOG(INFO, FastMapReduce) << "Successfully partitioned " << fmrInputTables.size() << " input fmr tables into " << gottenFmrTasks.size() << " tasks";
    for (auto& fmrTask: gottenFmrTasks) {
        YQL_CLOG(DEBUG, FastMapReduce) << fmrTask;
        currentTasks.emplace_back(fmrTask);
    }
    if (ytInputTables.empty()) {
        return TPartitionResult{.TaskInputs = currentTasks, .PartitionStatus = true};
    }
    auto settings = ytPartitionSettings;
    if (settings.MaxParts <= gottenFmrTasks.size()) {
        return TPartitionResult{.PartitionStatus = false};
    }
    settings.MaxParts = ytPartitionSettings.MaxParts - gottenFmrTasks.size();
    Y_ENSURE(settings.PartitionMode == NYT::ETablePartitionMode::Unordered);
    auto [gottenYtTasks, ytPartitionStatus] = ytCoordinatorService->PartitionYtTables(ytInputTables, clusterConnections, settings);
    for (auto& ytTask: gottenYtTasks) {
        currentTasks.emplace_back(TTaskTableInputRef{.Inputs = {ytTask}});
    }
    YQL_CLOG(INFO, FastMapReduce) << "Gotten " << currentTasks.size() << " yt and fmr tasks to run from operation input tables";
    return TPartitionResult{.TaskInputs = currentTasks, .PartitionStatus = ytPartitionStatus};
}

}

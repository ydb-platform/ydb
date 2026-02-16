#include "yql_yt_ordered_partitioner.h"
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NFmr {

TOrderedPartitioner::TOrderedPartitioner(
    const std::unordered_map<TFmrTableId, std::vector<TString>>& partIdsForTables,
    const std::unordered_map<TString, std::vector<TChunkStats>>& partIdStats,
    const TOrderedPartitionSettings& settings
)
    : PartIdsForTables_(partIdsForTables)
    , PartIdStats_(partIdStats)
    , Settings_(settings)
{
}

std::pair<std::vector<TTaskTableInputRef>, bool> TOrderedPartitioner::PartitionTablesIntoTasksOrdered(
    const std::vector<TOperationTableRef>& inputTables,
    IYtCoordinatorService::TPtr ytCoordinatorService,
    const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections
) {
    if (inputTables.empty()) {
        return {{}, true};
    }

    ui64 fmrTaskSize = 0;
    ui64 ytTaskSize = 0;

    std::vector<TFmrTableRef> fmrTablesScope;
    std::vector<TYtTableRef> ytTablesScope;
    std::vector<TTaskTableInputRef> tasks;

    for (ui64 tableIdx = 0; tableIdx < inputTables.size(); ++tableIdx) {
        const auto& tableRef = inputTables[tableIdx];

        std::vector<TTaskTableRef> chunks;

        if (auto fmrTable = std::get_if<TFmrTableRef>(&tableRef)) {
            if (!ytTablesScope.empty()) {
                auto ytTasksResult = PartitionYtTablesScope(ytTablesScope, ytCoordinatorService, clusterConnections);
                ytTaskSize += ytTasksResult.ChunksNum;
                ytTablesScope.clear();
                tasks.insert(tasks.end(), ytTasksResult.TaskInputs.begin(), ytTasksResult.TaskInputs.end());
            }
            fmrTablesScope.push_back(*fmrTable);
        } else {
            if (!fmrTablesScope.empty()) {
                auto fmrTasksResult = PartitionFmrTablesScope(fmrTablesScope);
                fmrTaskSize += fmrTasksResult.ChunksNum;
                fmrTablesScope.clear();
                tasks.insert(tasks.end(), fmrTasksResult.TaskInputs.begin(), fmrTasksResult.TaskInputs.end());
            }
            auto ytTable = std::get<TYtTableRef>(tableRef);
            ytTablesScope.push_back(ytTable);
        }

        if (fmrTaskSize > Settings_.FmrPartitionSettings.MaxParts) {
            YQL_CLOG(ERROR, FastMapReduce) << "The limit on the number of parts inside Ordered Partitioner for FMR tables has been exceeded";
            return {{}, false};
        }
        if (ytTaskSize > Settings_.YtPartitionSettings.MaxParts) {
            YQL_CLOG(ERROR, FastMapReduce) << "The limit on the number of parts inside Ordered Partitioner for Yt tables has been exceeded";
            return {{}, false};
        }
    }

    if (!fmrTablesScope.empty()) {
        auto fmrTasksResult = PartitionFmrTablesScope(fmrTablesScope);
        fmrTaskSize += fmrTasksResult.ChunksNum;
        tasks.insert(tasks.end(), fmrTasksResult.TaskInputs.begin(), fmrTasksResult.TaskInputs.end());
    }

    if (!ytTablesScope.empty()) {
        auto ytTasksResult = PartitionYtTablesScope(ytTablesScope, ytCoordinatorService, clusterConnections);
        ytTaskSize += ytTasksResult.ChunksNum;
        tasks.insert(tasks.end(), ytTasksResult.TaskInputs.begin(), ytTasksResult.TaskInputs.end());
    }

    if (fmrTaskSize > Settings_.FmrPartitionSettings.MaxParts) {
        YQL_CLOG(ERROR, FastMapReduce) << "The limit on the number of parts inside Ordered Partitioner for FMR tables has been exceeded";
        return {{}, false};
    }
    if (ytTaskSize > Settings_.YtPartitionSettings.MaxParts) {
        YQL_CLOG(ERROR, FastMapReduce) << "The limit on the number of parts inside Ordered Partitioner for Yt tables has been exceeded";
        return {{}, false};
    }
    return {tasks, true};
}

TOrderedPartitioner::TOrderedPartitionerOutput TOrderedPartitioner::PartitionFmrTablesScope(
        const std::vector<TFmrTableRef>& fmrTablesScope
) {
    TOrderedPartitionerOutput result;
    auto fmrWeightPerPart = Settings_.FmrPartitionSettings.MaxDataWeightPerPart;
    Y_ENSURE(fmrWeightPerPart > 0);

    ui64 chunkWeight = 0;
    TTaskTableInputRef task_input;
    for (const auto& fmrTable : fmrTablesScope) {
        auto partIds = PartIdsForTables_.at(fmrTable.FmrTableId);
        auto getOrAddCurrentInputRef = [] (TTaskTableInputRef& currentTaskInput, const TFmrTableRef& currentTableRef) -> TFmrTableInputRef& {
            for (auto& taskInputRef : currentTaskInput.Inputs) {
                auto* fmrInputRef = std::get_if<TFmrTableInputRef>(&taskInputRef);
                YQL_ENSURE(fmrInputRef, "Ordered partitioner expected only TFmrTableInputRef for FMR scope");
                if (fmrInputRef->TableId == currentTableRef.FmrTableId.Id) {
                    return *fmrInputRef;
                }
            }
            currentTaskInput.Inputs.emplace_back(TFmrTableInputRef{
                .TableId = currentTableRef.FmrTableId.Id,
                .Columns = currentTableRef.Columns,
                .SerializedColumnGroups = currentTableRef.SerializedColumnGroups
            });
            return std::get<TFmrTableInputRef>(currentTaskInput.Inputs.back());
        };

        for (const auto& partId : partIds) {
            const std::vector<TChunkStats>& stats = PartIdStats_.at(partId);

            ui64 chunkStart = 0;
            while (chunkStart < stats.size()) {
                ui64 chunkEnd = chunkStart;
                while (chunkEnd < stats.size() && chunkWeight < fmrWeightPerPart) {
                    chunkWeight += stats[chunkEnd].DataWeight;
                    chunkEnd++;
                }

                bool isFull =  chunkWeight >= fmrWeightPerPart;

                TTableRange range{
                    .PartId = partId,
                    .MinChunk = chunkStart,
                    .MaxChunk = chunkEnd
                };

                auto& currentInputRef = getOrAddCurrentInputRef(task_input, fmrTable);
                currentInputRef.TableRanges.push_back(range);
                chunkStart = chunkEnd;

                if (isFull) {
                    result.append(task_input);
                    task_input = TTaskTableInputRef();
                    chunkWeight = 0;
                }
            }
        }
    }
    if (!task_input.Inputs.empty()) {
        result.append(task_input);
    }

    return result;
}

TOrderedPartitioner::TOrderedPartitionerOutput TOrderedPartitioner::PartitionYtTablesScope(
        const std::vector<TYtTableRef>& ytTables,
        IYtCoordinatorService::TPtr ytCoordinatorService,
        const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections
) {
    TOrderedPartitionerOutput result;
    Y_ENSURE(Settings_.YtPartitionSettings.PartitionMode == NYT::ETablePartitionMode::Ordered, "Only ordered yt partition mode is supported");
    auto [ytTasks, status] = ytCoordinatorService->PartitionYtTables(ytTables, clusterConnections, Settings_.YtPartitionSettings);

    if (!status) {
        YQL_CLOG(ERROR, FastMapReduce) << "Failed to partition YT table";
        return result;
    }

    for (const auto& ytTask : ytTasks) {
        TTaskTableInputRef task_input;
        task_input.Inputs.push_back(ytTask);
        result.append(task_input);
    }

    return result;
}

TPartitionResult PartitionInputTablesIntoTasksOrdered(
    const std::vector<TOperationTableRef>& inputTables,
    TOrderedPartitioner& partitioner,
    IYtCoordinatorService::TPtr ytCoordinatorService,
    const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections
) {
    auto [tasks, status] = partitioner.PartitionTablesIntoTasksOrdered(inputTables, ytCoordinatorService, clusterConnections);

    YQL_CLOG(DEBUG, FastMapReduce) << "Successfully partitioned " << inputTables.size()
                                   << " input tables (ordered) into " << tasks.size() << " tasks";

    return TPartitionResult{.TaskInputs = tasks, .PartitionStatus = status};
}

}  // namespace NYql::NFmr

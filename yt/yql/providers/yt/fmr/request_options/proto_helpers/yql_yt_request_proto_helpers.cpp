#include "yql_yt_request_proto_helpers.h"

namespace NYql::NFmr {

NProto::TFmrError FmrErrorToProto(const TFmrError& error) {
    NProto::TFmrError protoError;
    protoError.SetComponent(static_cast<NProto::EFmrComponent>(error.Component));
    protoError.SetErrorMessage(error.ErrorMessage);
    if (error.WorkerId) {
        protoError.SetWorkerId(*error.WorkerId);
    }
    if (error.TaskId) {
        protoError.SetTaskId(*error.TaskId);
    }
    if (error.OperationId) {
        protoError.SetOperationId(*error.OperationId);
    }
    return protoError;
}

TFmrError FmrErrorFromProto(const NProto::TFmrError& protoError) {
    TFmrError fmrError;
    fmrError.Component = static_cast<EFmrComponent>(protoError.GetComponent());
    fmrError.ErrorMessage = protoError.GetErrorMessage();
    if (protoError.HasWorkerId()) {
        fmrError.WorkerId = protoError.GetWorkerId();
    }
    if (protoError.HasTaskId()) {
        fmrError.TaskId = protoError.GetTaskId();
    }
    if (protoError.HasOperationId()) {
        fmrError.OperationId = protoError.GetOperationId();
    }
    return fmrError;
}

NProto::TYtTableRef YtTableRefToProto(const TYtTableRef& ytTableRef) {
    NProto::TYtTableRef protoYtTableRef;
    protoYtTableRef.SetPath(ytTableRef.Path);
    protoYtTableRef.SetCluster(ytTableRef.Cluster);
    return protoYtTableRef;
}

TYtTableRef YtTableRefFromProto(const NProto::TYtTableRef protoYtTableRef) {
    TYtTableRef ytTableRef;
    ytTableRef.Path = protoYtTableRef.GetPath();
    ytTableRef.Cluster = protoYtTableRef.GetCluster();
    return ytTableRef;
}

NProto::TFmrTableRef FmrTableRefToProto(const TFmrTableRef& fmrTableRef) {
    NProto::TFmrTableRef protoFmrTableRef;
    protoFmrTableRef.SetTableId(fmrTableRef.TableId);
    return protoFmrTableRef;
}

TFmrTableRef FmrTableRefFromProto(const NProto::TFmrTableRef protoFmrTableRef) {
    TFmrTableRef fmrTableRef;
    fmrTableRef.TableId = protoFmrTableRef.GetTableId();
    return fmrTableRef;
}

NProto::TTableRange TableRangeToProto(const TTableRange& tableRange) {
    NProto::TTableRange protoTableRange;
    protoTableRange.SetPartId(tableRange.PartId);
    protoTableRange.SetMinChunk(tableRange.MinChunk);
    protoTableRange.SetMaxChunk(tableRange.MaxChunk);
    return protoTableRange;
}

TTableRange TableRangeFromProto(const NProto::TTableRange& protoTableRange) {
    return TTableRange {
        .PartId = protoTableRange.GetPartId(),
        .MinChunk = protoTableRange.GetMinChunk(),
        .MaxChunk = protoTableRange.GetMaxChunk()
    };
}

NProto::TFmrTableInputRef FmrTableInputRefToProto(const TFmrTableInputRef& fmrTableInputRef) {
    NProto::TFmrTableInputRef protoFmrTableInputRef;
    protoFmrTableInputRef.SetTableId(fmrTableInputRef.TableId);
    for (auto& tableRange: fmrTableInputRef.TableRanges) {
        NProto::TTableRange protoTableRange = TableRangeToProto(tableRange);
        auto* curTableRange = protoFmrTableInputRef.AddTableRanges();
        curTableRange->Swap(&protoTableRange);
    }
    return protoFmrTableInputRef;
}

TFmrTableInputRef FmrTableInputRefFromProto(const NProto::TFmrTableInputRef& protoFmrTableInputRef) {
    TFmrTableInputRef fmrTableInputRef;
    fmrTableInputRef.TableId = protoFmrTableInputRef.GetTableId();
    std::vector<TTableRange> tableRanges;
    for (size_t i = 0; i < protoFmrTableInputRef.TableRangesSize(); ++i) {
        TTableRange tableRange = TableRangeFromProto(protoFmrTableInputRef.GetTableRanges(i));
        tableRanges.emplace_back(tableRange);
    }
    fmrTableInputRef.TableRanges = tableRanges;
    return fmrTableInputRef;
}

NProto::TFmrTableOutputRef FmrTableOutputRefToProto(const TFmrTableOutputRef& fmrTableOutputRef) {
    NProto::TFmrTableOutputRef protoFmrTableOutputRef;
    protoFmrTableOutputRef.SetTableId(fmrTableOutputRef.TableId);
    protoFmrTableOutputRef.SetPartId(fmrTableOutputRef.PartId);
    return protoFmrTableOutputRef;
}

TFmrTableOutputRef FmrTableOutputRefFromProto(const NProto::TFmrTableOutputRef& protoFmrTableOutputRef) {
    return TFmrTableOutputRef{
        .TableId = protoFmrTableOutputRef.GetTableId(),
        .PartId = protoFmrTableOutputRef.GetPartId()
    };
}

NProto::TTableStats TableStatsToProto(const TTableStats& tableStats) {
    NProto::TTableStats protoTableStats;
    protoTableStats.SetChunks(tableStats.Chunks);
    protoTableStats.SetRows(tableStats.Rows);
    protoTableStats.SetDataWeight(tableStats.DataWeight);
    return protoTableStats;
}

TTableStats TableStatsFromProto(const NProto::TTableStats& protoTableStats) {
    return TTableStats {
        .Chunks = protoTableStats.GetChunks(),
        .Rows = protoTableStats.GetRows(),
        .DataWeight = protoTableStats.GetDataWeight()
    };
}

NProto::TStatistics StatisticsToProto(const TStatistics& stats) {
    NProto::TStatistics protoStatistics;
    for (auto& [fmrTableOutputRef, tableStat]: stats.OutputTables) {
        NProto::TFmrTableOutputRef protoFmrTableOutputref = FmrTableOutputRefToProto(fmrTableOutputRef);
        NProto::TTableStats protoStats = TableStatsToProto(tableStat);
        NProto::TFmrStatisticsObject statTableObject;
        statTableObject.MutableTable()->Swap(&protoFmrTableOutputref);
        statTableObject.MutableStatistic()->Swap(&protoStats);
        auto* curOutputTable = protoStatistics.AddOutputTables();
        curOutputTable->Swap(&statTableObject);
    }
    return protoStatistics;
}

TStatistics StatisticsFromProto(const NProto::TStatistics& protoStats) {
    std::unordered_map<TFmrTableOutputRef, TTableStats> outputTables;
    for (size_t i = 0; i < protoStats.OutputTablesSize(); ++i) {
        NProto::TFmrStatisticsObject protoStatTableObject = protoStats.GetOutputTables(i);
        TFmrTableOutputRef fmrTableOutputRef = FmrTableOutputRefFromProto(protoStatTableObject.GetTable());
        TTableStats tableStats = TableStatsFromProto(protoStatTableObject.GetStatistic());
        outputTables[fmrTableOutputRef] = tableStats;
    }
    return TStatistics{.OutputTables = outputTables};
}

NProto::TOperationTableRef OperationTableRefToProto(const TOperationTableRef& operationTableRef) {
    NProto::TOperationTableRef protoOperationTableRef;
    if (auto* ytTableRefPtr = std::get_if<TYtTableRef>(&operationTableRef)) {
        NProto::TYtTableRef protoYtTableRef = YtTableRefToProto(*ytTableRefPtr);
        protoOperationTableRef.MutableYtTableRef()->Swap(&protoYtTableRef);
    } else {
        auto* fmrTableRefPtr = std::get_if<TFmrTableRef>(&operationTableRef);
        NProto::TFmrTableRef protoFmrTableRef = FmrTableRefToProto(*fmrTableRefPtr);
        protoOperationTableRef.MutableFmrTableRef()->Swap(&protoFmrTableRef);
    }
    return protoOperationTableRef;
}

TOperationTableRef OperationTableRefFromProto(const NProto::TOperationTableRef& protoOperationTableRef) {
    std::variant<TYtTableRef, TFmrTableRef> tableRef;
    if (protoOperationTableRef.HasYtTableRef()) {
        tableRef = YtTableRefFromProto(protoOperationTableRef.GetYtTableRef());
    } else {
        tableRef = FmrTableRefFromProto(protoOperationTableRef.GetFmrTableRef());
    }
    return {tableRef};
}

NProto::TTaskTableRef TaskTableRefToProto(const TTaskTableRef& taskTableRef) {
    NProto::TTaskTableRef protoTaskTableRef;
    if (auto* ytTableRefPtr = std::get_if<TYtTableRef>(&taskTableRef)) {
        NProto::TYtTableRef protoYtTableRef = YtTableRefToProto(*ytTableRefPtr);
        protoTaskTableRef.MutableYtTableRef()->Swap(&protoYtTableRef);
    } else {
        auto* fmrTableInputRefPtr = std::get_if<TFmrTableInputRef>(&taskTableRef);
        NProto::TFmrTableInputRef protoFmrTableInputRef = FmrTableInputRefToProto(*fmrTableInputRefPtr);
        protoTaskTableRef.MutableFmrTableInputRef()->Swap(&protoFmrTableInputRef);
    }
    return protoTaskTableRef;

}

TTaskTableRef TaskTableRefFromProto(const NProto::TTaskTableRef& protoTaskTableRef) {
    std::variant<TYtTableRef, TFmrTableInputRef> tableRef;
    if (protoTaskTableRef.HasYtTableRef()) {
        tableRef = YtTableRefFromProto(protoTaskTableRef.GetYtTableRef());
    } else {
        tableRef = FmrTableInputRefFromProto(protoTaskTableRef.GetFmrTableInputRef());
    }
    return {tableRef};
}

NProto::TUploadOperationParams UploadOperationParamsToProto(const TUploadOperationParams& uploadOperationParams) {
    NProto::TUploadOperationParams protoUploadOperationParams;
    auto input = FmrTableRefToProto(uploadOperationParams.Input);
    auto output = YtTableRefToProto(uploadOperationParams.Output);
    protoUploadOperationParams.MutableInput()->Swap(&input);
    protoUploadOperationParams.MutableOutput()->Swap(&output);
    return protoUploadOperationParams;
}

NProto::TUploadTaskParams UploadTaskParamsToProto(const TUploadTaskParams& uploadTaskParams) {
    NProto::TUploadTaskParams protoUploadTaskParams;
    auto input = FmrTableInputRefToProto(uploadTaskParams.Input);
    auto output = YtTableRefToProto(uploadTaskParams.Output);
    protoUploadTaskParams.MutableInput()->Swap(&input);
    protoUploadTaskParams.MutableOutput()->Swap(&output);
    return protoUploadTaskParams;
}

TUploadOperationParams UploadOperationParamsFromProto(const NProto::TUploadOperationParams& protoUploadOperationParams) {
    TUploadOperationParams uploadOperationParams;
    uploadOperationParams.Input = FmrTableRefFromProto(protoUploadOperationParams.GetInput());
    uploadOperationParams.Output = YtTableRefFromProto(protoUploadOperationParams.GetOutput());
    return uploadOperationParams;
}

TUploadTaskParams UploadTaskParamsFromProto(const NProto::TUploadTaskParams& protoUploadTaskParams) {
    TUploadTaskParams uploadTaskParams;
    uploadTaskParams.Input = FmrTableInputRefFromProto(protoUploadTaskParams.GetInput());
    uploadTaskParams.Output = YtTableRefFromProto(protoUploadTaskParams.GetOutput());
    return uploadTaskParams;
}

NProto::TDownloadOperationParams DownloadOperationParamsToProto(const TDownloadOperationParams& downloadOperationParams) {
    NProto::TDownloadOperationParams protoDownloadOperationParams;
    auto input = YtTableRefToProto(downloadOperationParams.Input);
    auto output = FmrTableRefToProto(downloadOperationParams.Output);
    protoDownloadOperationParams.MutableInput()->Swap(&input);
    protoDownloadOperationParams.MutableOutput()->Swap(&output);
    return protoDownloadOperationParams;
}

NProto::TDownloadTaskParams DownloadTaskParamsToProto(const TDownloadTaskParams& downloadTaskParams) {
    NProto::TDownloadTaskParams protoDownloadTaskParams;
    auto input = YtTableRefToProto(downloadTaskParams.Input);
    auto output = FmrTableOutputRefToProto(downloadTaskParams.Output);
    protoDownloadTaskParams.MutableInput()->Swap(&input);
    protoDownloadTaskParams.MutableOutput()->Swap(&output);
    return protoDownloadTaskParams;
}

TDownloadOperationParams DownloadOperationParamsFromProto(const NProto::TDownloadOperationParams& protoDownloadOperationParams) {
    TDownloadOperationParams downloadOperationParams;
    downloadOperationParams.Input = YtTableRefFromProto(protoDownloadOperationParams.GetInput());
    downloadOperationParams.Output = FmrTableRefFromProto(protoDownloadOperationParams.GetOutput());
    return downloadOperationParams;
}

TDownloadTaskParams DownloadTaskParamsFromProto(const NProto::TDownloadTaskParams& protoDownloadTaskParams) {
    TDownloadTaskParams downloadTaskParams;
    downloadTaskParams.Input = YtTableRefFromProto(protoDownloadTaskParams.GetInput());
    downloadTaskParams.Output = FmrTableOutputRefFromProto(protoDownloadTaskParams.GetOutput());
    return downloadTaskParams;
}

NProto::TMergeOperationParams MergeOperationParamsToProto(const TMergeOperationParams& mergeOperationParams) {
    NProto::TMergeOperationParams protoMergeOperationParams;
    for (size_t i = 0; i < mergeOperationParams.Input.size(); ++i) {
        auto inputTable = OperationTableRefToProto(mergeOperationParams.Input[i]);
        auto* curInput = protoMergeOperationParams.AddInput();
        curInput->Swap(&inputTable);
    }
    auto outputTable = FmrTableRefToProto(mergeOperationParams.Output);
    protoMergeOperationParams.MutableOutput()->Swap(&outputTable);
    return protoMergeOperationParams;
}

NProto::TMergeTaskParams MergeTaskParamsToProto(const TMergeTaskParams& mergeTaskParams) {
    NProto::TMergeTaskParams protoMergeTaskParams;
    for (size_t i = 0; i < mergeTaskParams.Input.size(); ++i) {
        auto inputTable = TaskTableRefToProto(mergeTaskParams.Input[i]);
        auto* curInput = protoMergeTaskParams.AddInput();
        curInput->Swap(&inputTable);
    }
    auto outputTable = FmrTableOutputRefToProto(mergeTaskParams.Output);
    protoMergeTaskParams.MutableOutput()->Swap(&outputTable);
    return protoMergeTaskParams;
}

TMergeOperationParams MergeOperationParamsFromProto(const NProto::TMergeOperationParams& protoMergeOperationParams) {
    TMergeOperationParams mergeOperationParams;
    std::vector<TOperationTableRef> input;
    for (size_t i = 0; i < protoMergeOperationParams.InputSize(); ++i) {
        TOperationTableRef inputTable = OperationTableRefFromProto(protoMergeOperationParams.GetInput(i));
        input.emplace_back(inputTable);
    }
    mergeOperationParams.Input = input;
    mergeOperationParams.Output = FmrTableRefFromProto(protoMergeOperationParams.GetOutput());
    return mergeOperationParams;
}

TMergeTaskParams MergeTaskParamsFromProto(const NProto::TMergeTaskParams& protoMergeTaskParams) {
    TMergeTaskParams mergeTaskParams;
    std::vector<TTaskTableRef> input;
    for (size_t i = 0; i < protoMergeTaskParams.InputSize(); ++i) {
        TTaskTableRef inputTable = TaskTableRefFromProto(protoMergeTaskParams.GetInput(i));
        input.emplace_back(inputTable);
    }
    mergeTaskParams.Input = input;
    mergeTaskParams.Output = FmrTableOutputRefFromProto(protoMergeTaskParams.GetOutput());
    return mergeTaskParams;
}

NProto::TOperationParams OperationParamsToProto(const TOperationParams& operationParams) {
    NProto::TOperationParams protoOperationParams;
    if (auto* uploadOperationParamsPtr = std::get_if<TUploadOperationParams>(&operationParams)) {
        NProto::TUploadOperationParams protoUploadOperationParams = UploadOperationParamsToProto(*uploadOperationParamsPtr);
        protoOperationParams.MutableUploadOperationParams()->Swap(&protoUploadOperationParams);
    } else if (auto* downloadOperationParamsPtr = std::get_if<TDownloadOperationParams>(&operationParams)) {
        NProto::TDownloadOperationParams protoDownloadOperationParams = DownloadOperationParamsToProto(*downloadOperationParamsPtr);
        protoOperationParams.MutableDownloadOperationParams()->Swap(&protoDownloadOperationParams);
    } else {
        auto* mergeOperationParamsPtr = std::get_if<TMergeOperationParams>(&operationParams);
        NProto::TMergeOperationParams protoMergeOperationParams = MergeOperationParamsToProto(*mergeOperationParamsPtr);
        protoOperationParams.MutableMergeOperationParams()->Swap(&protoMergeOperationParams);
    }
    return protoOperationParams;
}

NProto::TTaskParams TaskParamsToProto(const TTaskParams& taskParams) {
    NProto::TTaskParams protoTaskParams;
    if (auto* uploadTaskParamsPtr = std::get_if<TUploadTaskParams>(&taskParams)) {
        NProto::TUploadTaskParams protoUploadTaskParams = UploadTaskParamsToProto(*uploadTaskParamsPtr);
        protoTaskParams.MutableUploadTaskParams()->Swap(&protoUploadTaskParams);
    } else if (auto* downloadTaskParamsPtr = std::get_if<TDownloadTaskParams>(&taskParams)) {
        NProto::TDownloadTaskParams protoDownloadTaskParams = DownloadTaskParamsToProto(*downloadTaskParamsPtr);
        protoTaskParams.MutableDownloadTaskParams()->Swap(&protoDownloadTaskParams);
    } else {
        auto* mergeTaskParamsPtr = std::get_if<TMergeTaskParams>(&taskParams);
        NProto::TMergeTaskParams protoMergeTaskParams = MergeTaskParamsToProto(*mergeTaskParamsPtr);
        protoTaskParams.MutableMergeTaskParams()->Swap(&protoMergeTaskParams);
    }
    return protoTaskParams;
}

TOperationParams OperationParamsFromProto(const NProto::TOperationParams& protoOperationParams) {
    TOperationParams operationParams;
    if (protoOperationParams.HasDownloadOperationParams()) {
        operationParams = DownloadOperationParamsFromProto(protoOperationParams.GetDownloadOperationParams());
    } else if (protoOperationParams.HasUploadOperationParams()) {
        operationParams = UploadOperationParamsFromProto(protoOperationParams.GetUploadOperationParams());
    } else {
        operationParams = MergeOperationParamsFromProto(protoOperationParams.GetMergeOperationParams());
    }
    return operationParams;
}

TTaskParams TaskParamsFromProto(const NProto::TTaskParams& protoTaskParams) {
    TTaskParams taskParams;
    if (protoTaskParams.HasDownloadTaskParams()) {
        taskParams = DownloadTaskParamsFromProto(protoTaskParams.GetDownloadTaskParams());
    } else if (protoTaskParams.HasUploadTaskParams()) {
        taskParams = UploadTaskParamsFromProto(protoTaskParams.GetUploadTaskParams());
    } else {
        taskParams = MergeTaskParamsFromProto(protoTaskParams.GetMergeTaskParams());
    }
    return taskParams;
}

NProto::TClusterConnection ClusterConnectionToProto(const TClusterConnection& clusterConnection) {
    NProto::TClusterConnection protoClusterConnection;
    protoClusterConnection.SetTransactionId(clusterConnection.TransactionId);
    protoClusterConnection.SetYtServerName(clusterConnection.YtServerName);
    if (clusterConnection.Token) {
        protoClusterConnection.SetToken(*clusterConnection.Token);
    }
    return protoClusterConnection;
}

TClusterConnection ClusterConnectionFromProto(const NProto::TClusterConnection& protoClusterConnection) {
    TClusterConnection clusterConnection{};
    clusterConnection.TransactionId = protoClusterConnection.GetTransactionId();
    clusterConnection.YtServerName = protoClusterConnection.GetYtServerName();
    if (protoClusterConnection.HasToken()) {
        clusterConnection.Token = protoClusterConnection.GetToken();
    }
    return clusterConnection;
}

NProto::TTask TaskToProto(const TTask& task) {
    NProto::TTask protoTask;
    protoTask.SetTaskType(static_cast<NProto::ETaskType>(task.TaskType));
    protoTask.SetTaskId(task.TaskId);
    auto taskParams = TaskParamsToProto(task.TaskParams);
    protoTask.MutableTaskParams()->Swap(&taskParams);
    protoTask.SetSessionId(task.SessionId);
    protoTask.SetNumRetries(task.NumRetries);
    auto clusterConnection = ClusterConnectionToProto(task.ClusterConnection);
    protoTask.MutableClusterConnection()->Swap(&clusterConnection);
    return protoTask;
}

TTask TaskFromProto(const NProto::TTask& protoTask) {
    TTask task;
    task.TaskType = static_cast<ETaskType>(protoTask.GetTaskType());
    task.TaskId = protoTask.GetTaskId();
    task.TaskParams = TaskParamsFromProto(protoTask.GetTaskParams());
    task.SessionId = protoTask.GetSessionId();
    task.NumRetries = protoTask.GetNumRetries();
    task.ClusterConnection = ClusterConnectionFromProto(protoTask.GetClusterConnection());
    return task;
}

NProto::TTaskState TaskStateToProto(const TTaskState& taskState) {
    NProto::TTaskState protoTaskState;
    protoTaskState.SetTaskStatus(static_cast<NProto::ETaskStatus>(taskState.TaskStatus));
    protoTaskState.SetTaskId(taskState.TaskId);
    if (taskState.TaskErrorMessage) {
        auto fmrError = FmrErrorToProto(*taskState.TaskErrorMessage);
        protoTaskState.MutableTaskErrorMessage()->Swap(&fmrError);
    }
    auto protoStatistics = StatisticsToProto(taskState.Stats);
    protoTaskState.MutableStats()->Swap(&protoStatistics);
    return protoTaskState;
}

TTaskState TaskStateFromProto(const NProto::TTaskState& protoTaskState) {
    TTaskState taskState;
    taskState.TaskStatus = static_cast<ETaskStatus>(protoTaskState.GetTaskStatus());
    taskState.TaskId = protoTaskState.GetTaskId();
    if (protoTaskState.HasTaskErrorMessage()) {
        taskState.TaskErrorMessage = FmrErrorFromProto(protoTaskState.GetTaskErrorMessage());
    }
    taskState.Stats = StatisticsFromProto(protoTaskState.GetStats());
    return taskState;
}

} // namespace NYql::NFmr

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
    protoYtTableRef.SetTransactionId(ytTableRef.TransactionId);
    return protoYtTableRef;
}

TYtTableRef YtTableRefFromProto(const NProto::TYtTableRef protoYtTableRef) {
    TYtTableRef ytTableRef;
    ytTableRef.Path = protoYtTableRef.GetPath();
    ytTableRef.Cluster = protoYtTableRef.GetCluster();
    ytTableRef.TransactionId = protoYtTableRef.GetTransactionId();
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

NProto::TTableRef TableRefToProto(const TTableRef& tableRef) {
    NProto::TTableRef protoTableRef;
    if (auto* ytTableRefPtr = std::get_if<TYtTableRef>(&tableRef.TableRef)) {
        NProto::TYtTableRef protoYtTableRef = YtTableRefToProto(*ytTableRefPtr);
        protoTableRef.MutableYtTableRef()->Swap(&protoYtTableRef);
    } else {
        auto* fmrTableRefPtr = std::get_if<TFmrTableRef>(&tableRef.TableRef);
        NProto::TFmrTableRef protoFmrTableRef = FmrTableRefToProto(*fmrTableRefPtr);
        protoTableRef.MutableFmrTableRef()->Swap(&protoFmrTableRef);
    }
    return protoTableRef;
}

TTableRef TableRefFromProto(const NProto::TTableRef& protoTableRef) {
    std::variant<TYtTableRef, TFmrTableRef> tableRef;
    if (protoTableRef.HasYtTableRef()) {
        tableRef = YtTableRefFromProto(protoTableRef.GetYtTableRef());
    } else {
        tableRef = FmrTableRefFromProto(protoTableRef.GetFmrTableRef());
    }
    return {tableRef};
}

NProto::TUploadTaskParams UploadTaskParamsToProto(const TUploadTaskParams& uploadTaskParams) {
    NProto::TUploadTaskParams protoUploadTaskParams;
    auto input = FmrTableRefToProto(uploadTaskParams.Input);
    auto output = YtTableRefToProto(uploadTaskParams.Output);
    protoUploadTaskParams.MutableInput()->Swap(&input);
    protoUploadTaskParams.MutableOutput()->Swap(&output);
    return protoUploadTaskParams;
}

TUploadTaskParams UploadTaskParamsFromProto(const NProto::TUploadTaskParams& protoUploadTaskParams) {
    TUploadTaskParams uploadTaskParams;
    uploadTaskParams.Input = FmrTableRefFromProto(protoUploadTaskParams.GetInput());
    uploadTaskParams.Output = YtTableRefFromProto(protoUploadTaskParams.GetOutput());
    return uploadTaskParams;
}

NProto::TDownloadTaskParams DownloadTaskParamsToProto(const TDownloadTaskParams& downloadTaskParams) {
    NProto::TDownloadTaskParams protoDownloadTaskParams;
    auto input = YtTableRefToProto(downloadTaskParams.Input);
    auto output = FmrTableRefToProto(downloadTaskParams.Output);
    protoDownloadTaskParams.MutableInput()->Swap(&input);
    protoDownloadTaskParams.MutableOutput()->Swap(&output);
    return protoDownloadTaskParams;
}

TDownloadTaskParams DownloadTaskParamsFromProto(const NProto::TDownloadTaskParams& protoDownloadTaskParams) {
    TDownloadTaskParams downloadTaskParams;
    downloadTaskParams.Input = YtTableRefFromProto(protoDownloadTaskParams.GetInput());
    downloadTaskParams.Output = FmrTableRefFromProto(protoDownloadTaskParams.GetOutput());
    return downloadTaskParams;
}

NProto::TMergeTaskParams MergeTaskParamsToProto(const TMergeTaskParams& mergeTaskParams) {
    NProto::TMergeTaskParams protoMergeTaskParams;
    std::vector<NProto::TTableRef> inputTables;
    for (size_t i = 0; i < mergeTaskParams.Input.size(); ++i) {
        auto inputTable = TableRefToProto(mergeTaskParams.Input[i]);
        auto* curInput = protoMergeTaskParams.AddInput();
        curInput->Swap(&inputTable);
    }
    auto outputTable = FmrTableRefToProto(mergeTaskParams.Output);
    protoMergeTaskParams.MutableOutput()->Swap(&outputTable);
    return protoMergeTaskParams;
}

TMergeTaskParams MergeTaskParamsFromProto(const NProto::TMergeTaskParams& protoMergeTaskParams) {
    TMergeTaskParams mergeTaskParams;
    std::vector<TTableRef> input;
    for (size_t i = 0; i < protoMergeTaskParams.InputSize(); ++i) {
        TTableRef inputTable = TableRefFromProto(protoMergeTaskParams.GetInput(i));
        input.emplace_back(inputTable);
    }
    mergeTaskParams.Input = input;
    mergeTaskParams.Output = FmrTableRefFromProto(protoMergeTaskParams.GetOutput());
    return mergeTaskParams;
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

NProto::TTask TaskToProto(const TTask& task) {
    NProto::TTask protoTask;
    protoTask.SetTaskType(static_cast<NProto::ETaskType>(task.TaskType));
    protoTask.SetTaskId(task.TaskId);
    auto taskParams = TaskParamsToProto(task.TaskParams);
    protoTask.MutableTaskParams()->Swap(&taskParams);
    protoTask.SetSessionId(task.SessionId);
    protoTask.SetNumRetries(task.NumRetries);
    return protoTask;
}

TTask TaskFromProto(const NProto::TTask& protoTask) {
    TTask task;
    task.TaskType = static_cast<ETaskType>(protoTask.GetTaskType());
    task.TaskId = protoTask.GetTaskId();
    task.TaskParams = TaskParamsFromProto(protoTask.GetTaskParams());
    task.SessionId = protoTask.GetSessionId();
    task.NumRetries = protoTask.GetNumRetries();
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
    return protoTaskState;
}

TTaskState TaskStateFromProto(const NProto::TTaskState& protoTaskState) {
    TTaskState taskState;
    taskState.TaskStatus = static_cast<ETaskStatus>(protoTaskState.GetTaskStatus());
    taskState.TaskId = protoTaskState.GetTaskId();
    if (protoTaskState.HasTaskErrorMessage()) {
        taskState.TaskErrorMessage = FmrErrorFromProto(protoTaskState.GetTaskErrorMessage());
    }
    return taskState;
}

} // namespace NYql::NFmr

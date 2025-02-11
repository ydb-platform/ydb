#pragma once

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <vector>

namespace NYql {

enum class EOperationStatus {
    Accepted,
    InProgress,
    Failed,
    Completed,
    Aborted,
    NotFound
};

enum class ETaskStatus {
    Accepted,
    InProgress,
    Failed,
    Completed,
    Aborted
};

enum class ETaskType {
    Download,
    Upload,
    Merge
};

enum class EFmrComponent {
    Coordinator,
    Worker,
    Job
};

struct TFmrError {
    EFmrComponent Component;
    TString ErrorMessage;
    TMaybe<ui32> WorkerId;
    TMaybe<TString> TaskId;
    TMaybe<TString> OperationId;
};

struct TStatistics {
};

struct TYtTableRef {
    TString Path;
    TString Cluster;
    TString TransactionId;
};

struct TFmrTableRef {
    TString TableId;
};

struct TTableRef {
    std::variant<TYtTableRef, TFmrTableRef> TableRef;
};

struct TUploadTaskParams {
    TFmrTableRef Input;
    TYtTableRef Output;
};

struct TDownloadTaskParams {
    TYtTableRef Input;
    TFmrTableRef Output;
};

struct TMergeTaskParams {
    std::vector<TTableRef> Input;
    TFmrTableRef Output;
};

using TTaskParams = std::variant<TUploadTaskParams, TDownloadTaskParams, TMergeTaskParams>;

struct TTask: public TThrRefBase {
    TTask(ETaskType taskType, const TString& taskId, const TTaskParams& taskParams, const TString& sessionId, ui32 numRetries = 1)
        : TaskType(taskType), TaskId(taskId), TaskParams(taskParams), SessionId(sessionId), NumRetries(numRetries)
    {
    }

    ETaskType TaskType;
    TString TaskId;
    TTaskParams TaskParams;
    TString SessionId;
    ui32 NumRetries; // Not supported yet

    using TPtr = TIntrusivePtr<TTask>;
};

struct TTaskState: public TThrRefBase {
    TTaskState(ETaskStatus taskStatus, const TString& taskId, const TMaybe<TFmrError>& errorMessage = Nothing())
        : TaskStatus(taskStatus), TaskId(taskId), TaskErrorMessage(errorMessage)
    {
    }

    ETaskStatus TaskStatus;
    TString TaskId;
    TMaybe<TFmrError> TaskErrorMessage;

    using TPtr = TIntrusivePtr<TTaskState>;
};

struct TTaskResult: public TThrRefBase {
    TTaskResult(ETaskStatus taskStatus, const TMaybe<TFmrError>& errorMessage = Nothing())
        : TaskStatus(taskStatus), TaskErrorMessage(errorMessage)
    {
    }

    ETaskStatus TaskStatus;
    TMaybe<TFmrError> TaskErrorMessage;

    using TPtr = TIntrusivePtr<TTaskResult>;
};

TTask::TPtr MakeTask(ETaskType taskType, const TString& taskId, const TTaskParams& taskParams, const TString& sessionId);

TTaskState::TPtr MakeTaskState(ETaskStatus taskStatus, const TString& taskId, const TMaybe<TFmrError>& taskErrorMessage = Nothing());

TTaskResult::TPtr MakeTaskResult(ETaskStatus taskStatus, const TMaybe<TFmrError>& taskErrorMessage = Nothing());

} // namespace NYql

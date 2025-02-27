#pragma once

#include <util/digest/numeric.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <vector>

namespace NYql::NFmr {

enum class EOperationStatus {
    Unknown,
    Accepted,
    InProgress,
    Failed,
    Completed,
    Aborted,
    NotFound
};

enum class ETaskStatus {
    Unknown,
    Accepted,
    InProgress,
    Failed,
    Completed
};

enum class ETaskType {
    Unknown,
    Download,
    Upload,
    Merge
};

enum class EFmrComponent {
    Unknown,
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

struct TError {
    TString ErrorMessage;
};

struct TYtTableRef {
    TString Path;
    TString Cluster;
    TString TransactionId;
};

struct TFmrTableRef {
    TString TableId;
};

struct TTableRange {
    TString PartId;
    ui64 MinChunk = 0;
    ui64 MaxChunk;
};

struct TFmrTableInputRef {
    TString TableId;
    std::vector<TTableRange> TableRanges;
};

struct TFmrTableOutputRef {
    TString TableId;
    TString PartId;

    bool operator==(const TFmrTableOutputRef&) const = default;
};

struct TTableStats {
    ui64 Chunks = 0;
    ui64 Rows;
    ui64 DataWeight;
};

} // namespace NYql::NFmr

namespace std {
    template<>
    struct hash<NYql::NFmr::TFmrTableOutputRef> {
        size_t operator()(const NYql::NFmr::TFmrTableOutputRef& ref) const {
            return CombineHashes(hash<TString>()(ref.TableId), hash<TString>()(ref.PartId));
        }
    };
}

namespace NYql::NFmr {

struct TStatistics {
    std::unordered_map<TFmrTableOutputRef, TTableStats> OutputTables;
};

//пока оставляем и со старым названием, чтобы тесты не падали, но после рефактора надо будет убрать
using TTableRef = std::variant<TYtTableRef, TFmrTableRef>;

using TOperationTableRef = std::variant<TYtTableRef, TFmrTableRef>;

using TTaskTableRef = std::variant<TYtTableRef, TFmrTableInputRef, TFmrTableOutputRef>;

struct TUploadOperationParams {
    TFmrTableRef Input;
    TYtTableRef Output;
};

struct TUploadTaskParams { // DEPRECATED TODO REMOVE
    TFmrTableRef Input;
    TYtTableRef Output;
};

struct TUploadTaskParamsNew {
    TFmrTableInputRef Input;
    TYtTableRef Output;
};

struct TDownloadOperationParams {
    TYtTableRef Input;
    TFmrTableRef Output;
};

struct TDownloadTaskParams { // DEPRECATED TODO REMOVE
    TYtTableRef Input;
    TFmrTableRef Output;
};

struct TDownloadTaskParamsNew {
    TYtTableRef Input;
    TFmrTableOutputRef Output;
};

struct TMergeOperationParams {
    std::vector<TOperationTableRef> Input;
    TFmrTableRef Output;
};

struct TMergeTaskParams { // DEPRECATED TODO REMOVE
    std::vector<TOperationTableRef> Input;
    TFmrTableRef Output;
};

struct TMergeTaskParamsNew {
    std::vector<TTaskTableRef> Input;
    TFmrTableOutputRef Output;
};

using TOperationParams = std::variant<TUploadOperationParams, TDownloadOperationParams, TMergeOperationParams>;

using TTaskParams = std::variant<TUploadTaskParams, TDownloadTaskParams, TMergeTaskParams>; // DEPRECATED TODO REMOVE

using TTaskParamsNew = std::variant<TUploadTaskParamsNew, TDownloadTaskParamsNew, TMergeTaskParamsNew>;

struct TTask: public TThrRefBase {
    TTask() = default;

    TTask(ETaskType taskType, const TString& taskId, const TTaskParams& taskParams, const TString& sessionId, ui32 numRetries = 1)
        : TaskType(taskType), TaskId(taskId), TaskParams(taskParams), SessionId(sessionId), NumRetries(numRetries)
    {
    }

    ETaskType TaskType;
    TString TaskId;
    TTaskParams TaskParams = {};
    TString SessionId;
    ui32 NumRetries; // Not supported yet

    using TPtr = TIntrusivePtr<TTask>;
};

struct TTaskState: public TThrRefBase {
    TTaskState() = default;

    TTaskState(ETaskStatus taskStatus, const TString& taskId, const TMaybe<TFmrError>& errorMessage = Nothing())
        : TaskStatus(taskStatus), TaskId(taskId), TaskErrorMessage(errorMessage)
    {
    }

    ETaskStatus TaskStatus;
    TString TaskId;
    TMaybe<TFmrError> TaskErrorMessage;
    TStatistics Stats;

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

} // namespace NYql::NFmr

#pragma once

#include <library/cpp/yson/node/node.h>
#include <util/digest/numeric.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/string/builder.h>
#include <vector>

#include <yt/cpp/mapreduce/interface/common.h>

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
    Merge,
    Map
};

enum class EFmrComponent {
    Unknown,
    Coordinator,
    Worker,
    Job
};

enum class EFmrErrorReason {
    ReasonUnknown,
    UserError
    // TODO - return FallbackQuery or FallbackOperation instead of UserError, pass info to gateway.
};

struct TFmrError {
    EFmrComponent Component;
    EFmrErrorReason Reason;
    TString ErrorMessage;
    TMaybe<ui32> WorkerId;
    TMaybe<TString> TaskId;
    TMaybe<TString> OperationId;
    TMaybe<TString> JobId;
};

struct TError {
    TString ErrorMessage;
};

struct TYtTableRef {
    TString Path;
    TString Cluster;
    TMaybe<TString> FilePath = Nothing();

    // TODO - maybe just use TRichYPath here also instead of Path and Cluster?

    bool operator == (const TYtTableRef&) const = default;
};

struct TYtTableTaskRef {
    std::vector<NYT::TRichYPath> RichPaths;
    std::vector<TString> FilePaths;

    bool operator == (const TYtTableTaskRef&) const = default;
}; // corresponds to a partition of several yt input tables.

struct TFmrTableId {
    TString Id;

    TFmrTableId() = default;

    TFmrTableId(const TString& id);

    TFmrTableId(const TString& cluster, const TString& path);

    bool operator == (const TFmrTableId&) const = default;
};

struct TFmrTableRef {
    TFmrTableId FmrTableId;
    bool operator == (const TFmrTableRef&) const = default;
};

struct TTableRange {
    TString PartId;
    ui64 MinChunk = 0;
    ui64 MaxChunk = 1;
    bool operator == (const TTableRange&) const = default;
}; // Corresnponds to range [MinChunk, MaxChunk)

struct TFmrTableInputRef {
    TString TableId;
    std::vector<TTableRange> TableRanges;
    bool operator == (const TFmrTableInputRef&) const = default;
}; // Corresponds to part of table with fixed TableId but several PartIds, Empty TablesRanges means that this table is not present in task.

struct TFmrTableOutputRef {
    TString TableId;
    TString PartId;

    bool operator == (const TFmrTableOutputRef&) const = default;
};

struct TTableStats {
    ui64 Chunks = 0;
    ui64 Rows = 0;
    ui64 DataWeight = 0;
    bool operator == (const TTableStats&) const = default;
};

struct TChunkStats {
    ui64 Rows = 0;
    ui64 DataWeight = 0;
    bool operator == (const TChunkStats&) const = default;
};

struct TTableChunkStats {
    TString PartId;
    std::vector<TChunkStats> PartIdChunkStats;
    bool operator == (const TTableChunkStats&) const = default;
}; // detailed statistics for all chunks in partition

} // namespace NYql::NFmr

namespace std {

    template<>
    struct hash<NYql::NFmr::TFmrTableId> {
        size_t operator()(const NYql::NFmr::TFmrTableId& tableId) const {
            return hash<TString>()(tableId.Id);
        }
    };

    template<>
    struct hash<NYql::NFmr::TFmrTableOutputRef> {
        size_t operator()(const NYql::NFmr::TFmrTableOutputRef& ref) const {
            return CombineHashes(hash<TString>()(ref.TableId), hash<TString>()(ref.PartId));
        }
    };

    template<>
    struct hash<NYql::NFmr::TYtTableRef> {
        size_t operator()(const NYql::NFmr::TYtTableRef& ref) const {
            return CombineHashes(hash<TString>()(ref.Cluster), hash<TString>()(ref.Path));
        }
    };
}

namespace NYql::NFmr {

struct TStatistics {
    std::unordered_map<TFmrTableOutputRef, TTableChunkStats> OutputTables;
};

using TOperationTableRef = std::variant<TYtTableRef, TFmrTableRef>;

using TTaskTableRef = std::variant<TYtTableTaskRef, TFmrTableInputRef>;

struct TUploadOperationParams {
    TFmrTableRef Input;
    TYtTableRef Output;
};

struct TUploadTaskParams {
    TFmrTableInputRef Input;
    TYtTableRef Output;
};

struct TDownloadOperationParams {
    TYtTableRef Input;
    TFmrTableRef Output;
};

struct TDownloadTaskParams {
    TYtTableTaskRef Input;
    TFmrTableOutputRef Output;
};

struct TMergeOperationParams {
    std::vector<TOperationTableRef> Input;
    TFmrTableRef Output;
};

struct TTaskTableInputRef {
    std::vector<TTaskTableRef> Inputs;
}; // Corresponds to task input tables, which can consist parts of either fmr or yt input tables.

struct TMergeTaskParams {
    TTaskTableInputRef Input;
    TFmrTableOutputRef Output;
};

struct TMapOperationParams {
    std::vector<TOperationTableRef> Input;
    std::vector<TFmrTableRef> Output;
    TString Executable;
};

struct TMapTaskParams {
    TTaskTableInputRef Input;
    std::vector<TFmrTableOutputRef> Output;
    TString Executable;
};

using TOperationParams = std::variant<TUploadOperationParams, TDownloadOperationParams, TMergeOperationParams, TMapOperationParams>;

using TTaskParams = std::variant<TUploadTaskParams, TDownloadTaskParams, TMergeTaskParams, TMapTaskParams>;

struct TClusterConnection {
    TString TransactionId;
    TString YtServerName;
    TMaybe<TString> Token;
};

struct TTask: public TThrRefBase {
    TTask() = default;

    TTask(ETaskType taskType, const TString& taskId, const TTaskParams& taskParams, const TString& sessionId, const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections, const TMaybe<NYT::TNode> & jobSettings = Nothing(), ui32 numRetries = 1)
        : TaskType(taskType), TaskId(taskId), TaskParams(taskParams), SessionId(sessionId), ClusterConnections(clusterConnections), JobSettings(jobSettings), NumRetries(numRetries)
    {
    }

    ETaskType TaskType;
    TString TaskId;
    TTaskParams TaskParams = {};
    TString SessionId;
    std::unordered_map<TFmrTableId, TClusterConnection> ClusterConnections = {};
    TMaybe<NYT::TNode> JobSettings = {};
    ui32 NumRetries; // Not supported yet

    using TPtr = TIntrusivePtr<TTask>;
};

struct TTaskState: public TThrRefBase {
    TTaskState() = default;

    TTaskState(ETaskStatus taskStatus, const TString& taskId, const TMaybe<TFmrError>& errorMessage = Nothing(), const TStatistics& stats = TStatistics())
        : TaskStatus(taskStatus), TaskId(taskId), TaskErrorMessage(errorMessage), Stats(stats)
    {
    }

    ETaskStatus TaskStatus;
    TString TaskId;
    TMaybe<TFmrError> TaskErrorMessage;
    TStatistics Stats;

    using TPtr = TIntrusivePtr<TTaskState>;
};
TTask::TPtr MakeTask(ETaskType taskType, const TString& taskId, const TTaskParams& taskParams, const TString& sessionId, const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections = {}, const TMaybe<NYT::TNode>& jobSettings = Nothing());

TTaskState::TPtr MakeTaskState(ETaskStatus taskStatus, const TString& taskId, const TMaybe<TFmrError>& taskErrorMessage = Nothing(), const TStatistics& stats = TStatistics());

} // namespace NYql::NFmr

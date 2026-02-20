#pragma once

#include <library/cpp/yson/node/node.h>
#include <util/digest/numeric.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/string/builder.h>
#include <vector>

#include <yt/cpp/mapreduce/interface/common.h>
#include <yt/cpp/mapreduce/interface/distributed_session.h>
#include <yt/yql/providers/yt/fmr/tvm/interface/yql_yt_fmr_tvm_interface.h>
#include <yt/yql/providers/yt/fmr/utils/comparator/yql_yt_binary_yson_compare_impl.h>

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
    Unknown = 0,
    Download = 1,
    Upload = 2,
    Merge = 3,
    Map = 4,
    SortedUpload = 5,
    SortedMerge = 6
};

enum class EFmrComponent {
    Unknown,
    Coordinator,
    Worker,
    Job
};

enum class EFmrErrorReason {
    Unknown,
    RestartOperation,
    RestartQuery,
    UdfTerminate
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

static constexpr TStringBuf FmrNonRetryableJobExceptionMarker = "[FmrNonRetryableJobException] ";

class TFmrNonRetryableJobException: public yexception {
public:
    TFmrNonRetryableJobException() : yexception()
{
    *this << ToString(FmrNonRetryableJobExceptionMarker);
}
};

EFmrErrorReason ParseFmrReasonFromErrorMessage(const TString& errorMessage);

struct TSortingColumns {
    std::vector<TString> Columns;
    std::vector<ESortOrder> SortOrders;
    bool operator==(const TSortingColumns&) const = default;
};

struct TFmrUserJobSettings {
    ui64 ThreadPoolSize = 3;
    ui64 QueueSizeLimit = 100;

    void Save(IOutputStream* buffer) const;
    void Load(IInputStream* buffer);
    bool operator==(const TFmrUserJobSettings&) const = default;
};

struct TFmrTvmJobSettings {
    TString WorkerTvmAlias;
    TTvmId TableDataServiceTvmId = 0;
    TMaybe<ui32> TvmPort;
    TMaybe<TString> TvmSecret;

    void Save(IOutputStream* buffer) const;
    void Load(IInputStream* buffer);
};

struct TFmrTvmGatewaySettings {
    TTvmId CoordinatorTvmId;
    TTvmId GatewayTvmId;
    TString GatewayTvmSecret;
    TString TvmDiskCacheDir;
};

struct TFmrTvmSpec {
    TString WorkerTvmAlias;
    TString CoordinatorTvmAlias;
    TString TableDataServiceTvmAlias;
    TTvmId WorkerTvmId;
    TTvmId CoordinatorTvmId;
    TTvmId TableDataServiceTvmId;
};

struct TYtTableRef {
    NYT::TRichYPath RichPath; // Path to yt table
    TMaybe<TString> FilePath; // Path to file corresponding to yt table, filled for file gateway

    TString GetPath() const;
    TString GetCluster() const;

    TYtTableRef();
    TYtTableRef(const NYT::TRichYPath& richPath, const TMaybe<TString>& filePath = Nothing());
    TYtTableRef(const TString& cluster, const TString& path, const TMaybe<TString>& filePath = Nothing());

    bool operator==(const TYtTableRef&) const = default;
};

struct TYtTableTaskRef {
    std::vector<NYT::TRichYPath> RichPaths;
    std::vector<TString> FilePaths;

    void Save(IOutputStream* buffer) const;
    void Load(IInputStream* buffer);

    bool operator==(const TYtTableTaskRef&) const = default;
}; // corresponds to a partition of several yt input tables.

void SaveRichPath(IOutputStream* buffer, const NYT::TRichYPath& path);
void LoadRichPath(IInputStream* buffer, NYT::TRichYPath& path);

TString SerializeRichPath(const NYT::TRichYPath& richPath);
NYT::TRichYPath DeserializeRichPath(const TString& serializedRichPath);

struct TFmrTableId {
    TString Id;

    TFmrTableId() = default;

    TFmrTableId(const TString& id);

    TFmrTableId(const TString& cluster, const TString& path);

    TFmrTableId(const NYT::TRichYPath& richPath);

    void Save(IOutputStream* buffer) const;
    void Load(IInputStream* buffer);

    bool operator==(const TFmrTableId&) const = default;
};

struct TFmrTableRef {
    TFmrTableId FmrTableId;
    std::vector<TString> Columns = {};
    TString SerializedColumnGroups = TString();
    std::vector<ESortOrder> SortOrder = {};
    std::vector<TString> SortColumns = {};

    bool operator==(const TFmrTableRef&) const = default;
};

struct TTableRange {
    TString PartId;
    ui64 MinChunk = 0;
    ui64 MaxChunk = 1;

    void Save(IOutputStream* buffer) const;
    void Load(IInputStream* buffer);

    bool operator==(const TTableRange&) const = default;
}; // Corresnponds to range [MinChunk, MaxChunk)

struct TFmrTableInputRef {
    TString TableId;
    std::vector<TTableRange> TableRanges;
    std::vector<TString> Columns = {};
    TString SerializedColumnGroups = TString();

    TMaybe<bool> IsFirstRowInclusive;
    TMaybe<TString> FirstRowKeys; // Binary YSON MAP
    TMaybe<TString> LastRowKeys;  // Binary YSON MAP

    void Save(IOutputStream* buffer) const;
    void Load(IInputStream* buffer);

    bool operator==(const TFmrTableInputRef&) const = default;
}; // Corresponds to part of table with fixed TableId but several PartIds, Empty TablesRanges means that this table is not present in task.

struct TFmrTableOutputRef {
    TString TableId;
    TString PartId;
    TString SerializedColumnGroups = TString(); // Serialized TNode of columnGroupSpec, empty if column groups is not set

    TSortingColumns SortingColumns;

    TFmrTableOutputRef() = default;

    TFmrTableOutputRef(const TString& tableId, const TMaybe<TString>& partId = Nothing());

    TFmrTableOutputRef(const TFmrTableRef& fmrTableRef);

    void Save(IOutputStream* buffer) const;
    void Load(IInputStream* buffer);

    bool operator==(const TFmrTableOutputRef&) const = default;
};

struct TTableStats {
    ui64 Chunks = 0;
    ui64 Rows = 0;
    ui64 DataWeight = 0;
    bool operator==(const TTableStats&) const = default;
};

struct TSortedChunkStats {
    bool IsSorted = false;
    NYT::TNode FirstRowKeys;
    NYT::TNode LastRowKeys;

    void Save(IOutputStream* buffer) const;
    void Load(IInputStream* buffer);

    bool operator==(const TSortedChunkStats&) const = default;
};

struct TChunkStats {
    ui64 Rows = 0;
    ui64 DataWeight = 0;
    TSortedChunkStats SortedChunkStats = TSortedChunkStats();
    bool operator==(const TChunkStats&) const = default;
};

struct TTableChunkStats {
    TString PartId;
    std::vector<TChunkStats> PartIdChunkStats;
    bool operator==(const TTableChunkStats&) const = default;
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
            return CombineHashes(hash<TString>()(ref.TableId),
                CombineHashes(hash<TString>()(ref.PartId), hash<TString>()(ref.SerializedColumnGroups)));
        }
    };
}

namespace NYql::NFmr {

struct TTaskUploadResult {};

struct TTaskDownloadResult {};

struct TTaskMergeResult {};

struct TTaskMapResult {};

struct TTaskSortedUploadResult {
    TString FragmentResultYson;
    ui64 FragmentOrder;
};

using TTaskResult = std::variant<TTaskUploadResult, TTaskDownloadResult, TTaskMergeResult, TTaskMapResult, TTaskSortedUploadResult>;

struct TStatistics {
    std::unordered_map<TFmrTableOutputRef, TTableChunkStats> OutputTables;
    TTaskResult TaskResult;
};

using TOperationTableRef = std::variant<TYtTableRef, TFmrTableRef>;

using TTaskTableRef = std::variant<TYtTableTaskRef, TFmrTableInputRef>;

// TODO - TYtTableTaskRef может быть из нескольких входных таблиц, но TFmrTableInputRef - часть одной таблицы, подумать как лучше

struct TClusterConnection {
    TString TransactionId;
    TString YtServerName;
    TMaybe<TString> Token;

    void Save(IOutputStream* buffer) const;
    void Load(IInputStream* buffer);
};

struct TYtReaderSettings {
    bool WithAttributes = false; // Enable RowIndex and RangeIndex, for now only mode = false is supported.
};

struct TYtWriterSettings {
    TMaybe<ui64> MaxRowWeight = Nothing();
};

struct TStartDistributedWriteOptions {
    TDuration Timeout = TDuration::Minutes(5);
    TDuration PingInterval = TDuration::Seconds(1);
};

struct TUploadOperationParams {
    TFmrTableRef Input;
    TYtTableRef Output;
};

struct TSortedUploadOperationParams {
    void UpdateAfterPreparation(std::vector<TString> cookies, TString PartitionId);

    TFmrTableRef Input;
    TYtTableRef Output;
    TString SessionId;
    std::vector<TString> Cookies;
    TString PartitionId;
    bool IsOrdered = true;
};

struct TUploadTaskParams {
    TFmrTableInputRef Input;
    TYtTableRef Output;
};

struct TSortedUploadTaskParams {
    TFmrTableInputRef Input;
    TYtTableRef Output;
    TString CookieYson;
    ui64 Order;
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

struct TSortedMergeOperationParams {
    void UpdateAfterPreparation(std::vector<TString> cookies, TString PartitionId);

    std::vector<TOperationTableRef> Input;
    TFmrTableRef Output;
};

struct TTaskTableInputRef {
    std::vector<TTaskTableRef> Inputs;

    void Save(IOutputStream* buffer) const;
    void Load(IInputStream* buffer);
}; // Corresponds to task input tables, which can consist parts of either fmr or yt input tables.

struct TMergeTaskParams {
    TTaskTableInputRef Input;
    TFmrTableOutputRef Output;
};

struct TSortedMergeTaskParams {
    TTaskTableInputRef Input;
    TFmrTableOutputRef Output;
};

struct TMapOperationParams {
    std::vector<TOperationTableRef> Input;
    std::vector<TFmrTableRef> Output;
    TString SerializedMapJobState;
    bool IsOrdered = false;
};

struct TMapTaskParams {
    TTaskTableInputRef Input;
    std::vector<TFmrTableOutputRef> Output;
    TString SerializedMapJobState;
    bool IsOrdered;
};


using TOperationParams = std::variant<TUploadOperationParams, TDownloadOperationParams, TMergeOperationParams, TSortedMergeOperationParams, TMapOperationParams, TSortedUploadOperationParams>;

using TTaskParams = std::variant<TUploadTaskParams, TDownloadTaskParams, TMergeTaskParams, TSortedMergeTaskParams, TMapTaskParams, TSortedUploadTaskParams>;

struct TFileInfo {
    TString LocalPath; // Path to local file, filled in worker.
    TString Md5Key; // hash of file content, used key in dist cache.
    TString Alias;
};

struct TYtResourceInfo {
    NYT::TRichYPath RichPath; // Path to resource in cypress, can be either file, or table which we need to download as file (MapJoin)
    TString YtServerName;
    TString Token;
    TString LocalPath; // Path to local file, filled in worker.
};

struct TFmrResourceOperationInfo {
    TFmrTableRef FmrTable;
    TString Alias;
};

struct TFmrResourceTaskInfo {
    std::vector<TFmrTableInputRef> FmrResourceTasks; // List of tasks corresponding to single fmr table which we want to download as files for MapJoin.
    TString LocalPath; // Path to local file, filled in worker.
    TString Alias;
};

struct TTask: public TThrRefBase {
    TTask() = default;

    TTask(ETaskType taskType, const TString& taskId, const TTaskParams& taskParams, const TString& sessionId, const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections, const std::vector<TFileInfo>& files, const std::vector<TYtResourceInfo>& ytResources, const std::vector<TFmrResourceTaskInfo>& fmrResources, const TMaybe<NYT::TNode> & jobSettings = Nothing(), ui32 numRetries = 1)
        : TaskType(taskType), TaskId(taskId), TaskParams(taskParams), SessionId(sessionId), ClusterConnections(clusterConnections), Files(files), YtResources(ytResources), FmrResources(fmrResources), JobSettings(jobSettings), NumRetries(numRetries)
    {
    }

    ETaskType TaskType;
    TString TaskId;
    TTaskParams TaskParams;
    TString SessionId;
    std::unordered_map<TFmrTableId, TClusterConnection> ClusterConnections;
    std::vector<TFileInfo> Files; // Udfs and user files from distributed cache.
    std::vector<TYtResourceInfo> YtResources; // Yt tables and files to download.
    std::vector<TFmrResourceTaskInfo> FmrResources; // Fmr tables (passed as tasks) which we want to download as files for MapJoin.
    TMaybe<TString> JobEnvironmentDir;
    TMaybe<NYT::TNode> JobSettings;
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
TTask::TPtr MakeTask(ETaskType taskType, const TString& taskId, const TTaskParams& taskParams, const TString& sessionId, const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections = {}, const std::vector<TFileInfo>& files = {}, const std::vector<TYtResourceInfo>& ytResources = {}, const std::vector<TFmrResourceTaskInfo>& fmrResources = {}, const TMaybe<NYT::TNode>& jobSettings = Nothing());

TTaskState::TPtr MakeTaskState(ETaskStatus taskStatus, const TString& taskId, const TMaybe<TFmrError>& taskErrorMessage = Nothing(), const TStatistics& stats = TStatistics());

} // namespace NYql::NFmr

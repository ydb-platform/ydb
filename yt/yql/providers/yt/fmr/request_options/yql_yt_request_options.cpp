#include "yql_yt_request_options.h"
#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/interface/serialize.h>

namespace NYql::NFmr {

EFmrErrorReason ParseFmrReasonFromErrorMessage(const TString& errorMessage) {
    TStringBuf message = errorMessage;
    if (TryParseTerminationMessage(message).Defined()) {
        return EFmrErrorReason::UdfTerminate;
    } else if (message.contains(FmrNonRetryableJobExceptionMarker)) {
        return EFmrErrorReason::RestartQuery;
    }
    return EFmrErrorReason::Unknown;
}

void TFmrUserJobSettings::Save(IOutputStream* buffer) const {
    ::SaveMany(
        buffer,
        ThreadPoolSize,
        QueueSizeLimit
    );
}

void TFmrUserJobSettings::Load(IInputStream* buffer) {
    ::LoadMany(
        buffer,
        ThreadPoolSize,
        QueueSizeLimit
    );
}

void TFmrTvmJobSettings::Save(IOutputStream* buffer) const {
    ::SaveMany(
        buffer,
        WorkerTvmAlias,
        TableDataServiceTvmId,
        TvmPort,
        TvmSecret
    );
}

void TFmrTvmJobSettings::Load(IInputStream* buffer) {
    ::LoadMany(
        buffer,
        WorkerTvmAlias,
        TableDataServiceTvmId,
        TvmPort,
        TvmSecret
    );
}

TYtTableRef::TYtTableRef()
{
}

TYtTableRef::TYtTableRef(const NYT::TRichYPath& richPath, const TMaybe<TString>& filePath)
    : RichPath(richPath), FilePath(filePath)
{
}

TYtTableRef::TYtTableRef(const TString& cluster, const TString& path, const TMaybe<TString>& filePath): FilePath(filePath) {
    RichPath = NYT::TRichYPath().Path(path).Cluster(cluster);
}

TString TYtTableRef::GetPath() const {
    return RichPath.Path_;
}

TString TYtTableRef::GetCluster() const {
    YQL_ENSURE(RichPath.Cluster_.Defined(), "YtTableRef cluster should be set");
    return *RichPath.Cluster_;
}

TFmrTableId::TFmrTableId(const TString& id): Id(id)
{
}

TFmrTableId::TFmrTableId(const NYT::TRichYPath& richPath) {
    YQL_ENSURE(richPath.Cluster_.Defined(), "YtTableRef cluster should be set");
    TString path = richPath.Path_;
    if (path.StartsWith("//")) {
        path = path.substr(2);
    }
    Id = *richPath.Cluster_+ "." + path;
}

TFmrTableId::TFmrTableId(const TString& cluster, const TString& path): Id(cluster + "." + path)
{
}

TTask::TPtr MakeTask(ETaskType taskType, const TString& taskId, const TTaskParams& taskParams, const TString& sessionId, const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections, const std::vector<TFileInfo>& files, const std::vector<TYtResourceInfo>& ytResources, const std::vector<TFmrResourceTaskInfo>& fmrResources, const TMaybe<NYT::TNode>& jobSettings) {
    return MakeIntrusive<TTask>(taskType, taskId, taskParams, sessionId, clusterConnections, files, ytResources, fmrResources, jobSettings);
}

TTaskState::TPtr MakeTaskState(ETaskStatus taskStatus, const TString& taskId, const TMaybe<TFmrError>& taskErrorMessage, const TStatistics& stats) {
    return MakeIntrusive<TTaskState>(taskStatus, taskId, taskErrorMessage, stats);
}

void TSortedUploadOperationParams::UpdateAfterPreparation(std::vector<TString> cookies, TString partitionId) {
    Cookies = std::move(cookies);
    PartitionId = std::move(partitionId);
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////

// Helper serialization functions

void SaveRichPath(IOutputStream* buffer, const NYT::TRichYPath& path) {
    TString serializedPath = SerializeRichPath(path);
    ::Save(buffer, serializedPath);
}
void LoadRichPath(IInputStream* buffer, NYT::TRichYPath& path) {
    TString serializedPath;
    ::Load(buffer, serializedPath);
    path = DeserializeRichPath(serializedPath);
}

void TYtTableTaskRef::Save(IOutputStream* buffer) const {
    ::Save(buffer, RichPaths.size());
    for (auto& path: RichPaths) {
        SaveRichPath(buffer, path);
    }
    ::Save(buffer, FilePaths);
}

void TYtTableTaskRef::Load(IInputStream* buffer) {
    ui64 richPathsSize;
    ::Load(buffer, richPathsSize);
    std::vector<NYT::TRichYPath> richPaths;

    for (ui64 i = 0; i < richPathsSize; ++i) {
        NYT::TRichYPath path;
        LoadRichPath(buffer, path);
        richPaths.emplace_back(path);
    }
    RichPaths = richPaths;
    ::Load(buffer, FilePaths);
}

void TTableRange::Save(IOutputStream* buffer) const {
    ::SaveMany(
        buffer,
        PartId,
        MinChunk,
        MaxChunk
    );
}

void TTableRange::Load(IInputStream* buffer) {
    ::LoadMany(
        buffer,
        PartId,
        MinChunk,
        MaxChunk
    );
}

void TFmrTableInputRef::Save(IOutputStream* buffer) const {
    ::SaveMany(
        buffer,
        TableId,
        TableRanges,
        Columns,
        SerializedColumnGroups,
        IsFirstRowInclusive,
        FirstRowKeys,
        LastRowKeys
    );
}

void TFmrTableInputRef::Load(IInputStream* buffer) {
    ::LoadMany(
        buffer,
        TableId,
        TableRanges,
        Columns,
        SerializedColumnGroups,
        IsFirstRowInclusive,
        FirstRowKeys,
        LastRowKeys
    );
}

void TTaskTableInputRef::Save(IOutputStream* buffer) const {
    ::Save(buffer, Inputs);
}

void TTaskTableInputRef::Load(IInputStream* buffer) {
    ::Load(buffer, Inputs);
}

TFmrTableOutputRef::TFmrTableOutputRef(const TString& tableId, const TMaybe<TString>& partId): TableId(tableId) {
    if (partId) {
        PartId = *partId;
    }
}

// Helper constructor which initializes all fields except PartId.
TFmrTableOutputRef::TFmrTableOutputRef(const TFmrTableRef& fmrTableRef)
    : TableId(fmrTableRef.FmrTableId.Id)
    , SerializedColumnGroups(fmrTableRef.SerializedColumnGroups)
    , SortingColumns(TSortingColumns{
        .Columns = fmrTableRef.SortColumns,
        .SortOrders = fmrTableRef.SortOrder
    })
{
}

void TFmrTableOutputRef::Save(IOutputStream* buffer) const {
    ::SaveMany(
        buffer,
        TableId,
        PartId,
        SerializedColumnGroups,
        SortingColumns.Columns,
        SortingColumns.SortOrders
    );
}

void TFmrTableOutputRef::Load(IInputStream* buffer) {
    ::LoadMany(
        buffer,
        TableId,
        PartId,
        SerializedColumnGroups,
        SortingColumns.Columns,
        SortingColumns.SortOrders
    );
}

void TClusterConnection::Save(IOutputStream* buffer) const {
    ::SaveMany(
        buffer,
        TransactionId,
        YtServerName,
        Token
    );
}

void TClusterConnection::Load(IInputStream* buffer) {
    ::LoadMany(
        buffer,
        TransactionId,
        YtServerName,
        Token
    );
}

void TFmrTableId::Save(IOutputStream* buffer) const {
    ::Save(buffer, Id);
}

void TFmrTableId::Load(IInputStream* buffer) {
    ::Load(buffer, Id);
}

void TSortedChunkStats::Save(IOutputStream* buffer) const {
    ::SaveMany(
        buffer,
        IsSorted,
        NYT::NodeToYsonString(FirstRowKeys),
        NYT::NodeToYsonString(LastRowKeys)
    );
}

void TSortedChunkStats::Load(IInputStream* buffer) {
    TString FirstRowKeysStr;
    TString LastRowKeysStr;
    ::LoadMany(buffer, IsSorted, FirstRowKeysStr, LastRowKeysStr);
    FirstRowKeys = NYT::NodeFromYsonString(FirstRowKeysStr);
    LastRowKeys = NYT::NodeFromYsonString(LastRowKeysStr);
}

// helper functions for rich path

TString SerializeRichPath(const NYT::TRichYPath& richPath) {
    return NYT::NodeToYsonString(NYT::PathToNode(richPath));
}

NYT::TRichYPath DeserializeRichPath(const TString& serializedRichPath) {
    auto node = NYT::NodeFromYsonString(serializedRichPath);
    NYT::TRichYPath richPath;
    NYT::Deserialize(richPath, node);
    return richPath;
}

} // namespace NYql::NFmr

//////////////////////////////////////////////////////////////////////////////////////////////////////////

// Helper output operators for structs


template<>
void Out<NYql::NFmr::TFmrTableId>(IOutputStream& out, const NYql::NFmr::TFmrTableId& tableId) {
    out << tableId.Id;
}

template<>
void Out<NYql::NFmr::TFmrError>(IOutputStream& out, const NYql::NFmr::TFmrError& error) {
    out << "FmrError[" << error.Component << "]";
    if (error.Component == NYql::NFmr::EFmrComponent::Worker) {
        out << "(TaskId: " << error.TaskId << " WorkerId: " << error.WorkerId << ") ";
    } else if (error.Component == NYql::NFmr::EFmrComponent::Coordinator) {
        out << "(OperationId: " << error.OperationId <<") ";
    }
    out << error.ErrorMessage;
}

template<>
void Out<NYql::NFmr::TTableStats>(IOutputStream& out, const NYql::NFmr::TTableStats& tableStats) {
    out << tableStats.Chunks << " chunks, " << tableStats.Rows << " rows, " << tableStats.DataWeight << " data weight";
}

template<>
void Out<NYql::NFmr::TTableRange>(IOutputStream& out, const NYql::NFmr::TTableRange& range) {
    out << "TableRange with part id: " << range.PartId << " , min chunk: " << range.MinChunk << " , max chunk: " << range.MaxChunk << "\n";
}

template<>
void Out<NYql::NFmr::TFmrTableInputRef>(IOutputStream& out, const NYql::NFmr::TFmrTableInputRef& inputRef) {
    out << "FmrTableInputRef consisting of " << inputRef.TableRanges.size() << " table ranges:\n";
    out << "TableId: " << inputRef.TableId << "\n";
    for (auto& range: inputRef.TableRanges) {
        out << range;
    }
}

template<>
void Out<NYql::NFmr::TYtTableTaskRef>(IOutputStream& out, const NYql::NFmr::TYtTableTaskRef& ytTableTaskRef) {
    if (!ytTableTaskRef.FilePaths.empty()) {
        out << "YtTableTaskRef consisting of " << ytTableTaskRef.FilePaths.size() << " file paths:\n";
        for (auto& filePath: ytTableTaskRef.FilePaths) {
            out << filePath << " ";
        }
    } else {
        out << "YtTableTaskRef consisting of " << ytTableTaskRef.RichPaths.size() << " rich yt paths:\n";
        for (auto& richPath: ytTableTaskRef.RichPaths) {
            out << NodeToYsonString(NYT::PathToNode(richPath)) << "\n";
        }
    }
}

template<>
void Out<NYql::NFmr::TTaskTableRef>(IOutputStream& out, const NYql::NFmr::TTaskTableRef& taskTableRef) {
    if (auto* ytTableTaskRef = std::get_if<NYql::NFmr::TYtTableTaskRef>(&taskTableRef)) {
        out << *ytTableTaskRef;
    } else {
        out << std::get<NYql::NFmr::TFmrTableInputRef>(taskTableRef);
    }
}

template<>
void Out<NYql::NFmr::TTaskTableInputRef>(IOutputStream& out, const NYql::NFmr::TTaskTableInputRef& taskTableInputRef) {
    for (auto& taskTableRef: taskTableInputRef.Inputs) {
        out << taskTableRef;
    }
}

template<>
void Out<NYql::NFmr::TChunkStats>(IOutputStream& out, const NYql::NFmr::TChunkStats& chunkStats) {
    out << chunkStats.Rows << " rows " << chunkStats.DataWeight << " dataWeight\n";
}

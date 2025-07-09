#include "yql_yt_request_options.h"
#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/interface/serialize.h>

namespace NYql::NFmr {

TFmrTableId::TFmrTableId(const TString& id): Id(id)
{
};

TFmrTableId::TFmrTableId(const TString& cluster, const TString& path): Id(cluster + "." + path)
{
};

TTask::TPtr MakeTask(ETaskType taskType, const TString& taskId, const TTaskParams& taskParams, const TString& sessionId, const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections, const TMaybe<NYT::TNode>& jobSettings) {
    return MakeIntrusive<TTask>(taskType, taskId, taskParams, sessionId, clusterConnections, jobSettings);
}

TTaskState::TPtr MakeTaskState(ETaskStatus taskStatus, const TString& taskId, const TMaybe<TFmrError>& taskErrorMessage, const TStatistics& stats) {
    return MakeIntrusive<TTaskState>(taskStatus, taskId, taskErrorMessage, stats);
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////

// Helper serialization functions

void SaveRichPath(IOutputStream* buffer, const NYT::TRichYPath& path) {
    TString serializedPath = NYT::NodeToYsonString(NYT::PathToNode(path));
    ::Save(buffer, serializedPath);
}
void LoadRichPath(IInputStream* buffer, NYT::TRichYPath& path) {
    TString serializedPath;
    ::Load(buffer, serializedPath);
    auto node = NYT::NodeFromYsonString(serializedPath);
    NYT::Deserialize(path, node);
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
        TableRanges
    );
}

void TFmrTableInputRef::Load(IInputStream* buffer) {
    ::LoadMany(
        buffer,
        TableId,
        TableRanges
    );
}

void TTaskTableInputRef::Save(IOutputStream* buffer) const {
    ::Save(buffer, Inputs);
}

void TTaskTableInputRef::Load(IInputStream* buffer) {
    ::Load(buffer, Inputs);
}

void TFmrTableOutputRef::Save(IOutputStream* buffer) const {
    ::SaveMany(
        buffer,
        TableId,
        PartId
    );
}

void TFmrTableOutputRef::Load(IInputStream* buffer) {
    ::LoadMany(
        buffer,
        TableId,
        PartId
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

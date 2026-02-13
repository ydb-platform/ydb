#pragma once

#include <library/cpp/threading/future/core/future.h>
#include <util/datetime/base.h>

#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>

namespace NYql::NFmr {

using TOperationPartitions = std::vector<TTaskParams>;

struct THeartbeatRequest {
    ui32 WorkerId;
    TString VolatileId;
    std::vector<TTaskState::TPtr> TaskStates;
    ui64 AvailableSlots = 0;
};
// Worker sends requests in loop or long polling

struct THeartbeatResponse {
    std::vector<TTask::TPtr> TasksToRun;
    std::unordered_set<TString> TaskToDeleteIds;
    bool NeedToRestart = false;
};

struct TStartOperationRequest {
    ETaskType TaskType;
    TOperationParams OperationParams;
    TString SessionId;
    TMaybe<TString> IdempotencyKey = Nothing();
    ui32 NumRetries = 1; // Not supported yet
    std::unordered_map<TFmrTableId, TClusterConnection> ClusterConnections = {};
    TMaybe<NYT::TNode> FmrOperationSpec = Nothing();
    std::vector<TFileInfo> Files = {};
    std::vector<TYtResourceInfo> YtResources = {};
    std::vector<TFmrResourceOperationInfo> FmrResources = {};
};

struct TStartOperationResponse {
    EOperationStatus Status;
    TString OperationId;
};

struct TPrepareOperationRequest {
    TOperationParams OperationParams;
    std::unordered_map<TFmrTableId, TClusterConnection> ClusterConnections;
    TMaybe<NYT::TNode> FmrOperationSpec;
};

struct TPrepareOperationResponse {
    TString PartitionId;
    ui64 TasksNum;
};

struct TGetOperationRequest {
    TString OperationId;
};

struct TGetOperationResponse {
    EOperationStatus Status;
    std::vector<TFmrError> ErrorMessages = {};
    std::vector<TTableStats> OutputTablesStats = {};
    std::vector<TString> OperationResultsYson = {};
};

struct TDeleteOperationRequest {
    TString OperationId;
};

struct TDeleteOperationResponse {
    EOperationStatus Status;
};

struct TDropTablesRequest {
    std::vector<TString> TableIds;
    TString SessionId;
};

struct TDropTablesResponse {
};

struct TGetFmrTableInfoRequest {
    TString TableId;
    TString SessionId;
};


struct TGetFmrTableInfoResponse {
    TTableStats TableStats;
    std::vector<TFmrError> ErrorMessages = {};
};

struct TClearSessionRequest {
    TString SessionId;
};

struct TOpenSessionRequest {
    TString SessionId;
};

struct TOpenSessionResponse {
};

struct TListSessionsRequest {
};

struct TListSessionsResponse {
    std::vector<TString> SessionIds;
};

struct TPingSessionRequest {
    TString SessionId;
};

struct TPingSessionResponse {
    bool Success;
};

enum class EPartitionType {
    UnorderedPartition,
    OrderedPartition,
    SortedPartition
};

class IFmrCoordinator: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IFmrCoordinator>;

    virtual ~IFmrCoordinator() = default;

    virtual NThreading::TFuture<TStartOperationResponse> StartOperation(const TStartOperationRequest& request) = 0;

    virtual NThreading::TFuture<TGetOperationResponse> GetOperation(const TGetOperationRequest& request) = 0;

    virtual NThreading::TFuture<TDeleteOperationResponse> DeleteOperation(const TDeleteOperationRequest& request) = 0;

    virtual NThreading::TFuture<TDropTablesResponse> DropTables(const TDropTablesRequest& request) = 0;

    virtual NThreading::TFuture<THeartbeatResponse> SendHeartbeatResponse(const THeartbeatRequest& request) = 0;

    virtual NThreading::TFuture<TGetFmrTableInfoResponse> GetFmrTableInfo(const TGetFmrTableInfoRequest& request) = 0;

    virtual NThreading::TFuture<void> ClearSession(const TClearSessionRequest& request) = 0;

    virtual NThreading::TFuture<TOpenSessionResponse> OpenSession(const TOpenSessionRequest& request) = 0;

    virtual NThreading::TFuture<TPingSessionResponse> PingSession(const TPingSessionRequest& request) = 0;

    virtual NThreading::TFuture<TListSessionsResponse> ListSessions(const TListSessionsRequest& request) = 0;

    virtual NThreading::TFuture<TPrepareOperationResponse> PrepareOperation(const TPrepareOperationRequest& request) = 0;
};

} // namespace NYql::NFmr

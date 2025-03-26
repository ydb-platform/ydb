#pragma once

#include <library/cpp/threading/future/core/future.h>
#include <util/datetime/base.h>

#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>

namespace NYql::NFmr {

struct THeartbeatRequest {
    ui32 WorkerId;
    TString VolatileId;
    std::vector<TTaskState::TPtr> TaskStates;
};
// Worker sends requests in loop or long polling

struct THeartbeatResponse {
    std::vector<TTask::TPtr> TasksToRun;
    std::unordered_set<TString> TaskToDeleteIds;
};

struct TStartOperationRequest {
    ETaskType TaskType;
    TOperationParams OperationParams;
    TString SessionId;
    TMaybe<TString> IdempotencyKey = Nothing();
    ui32 NumRetries = 1; // Not supported yet
    TClusterConnection ClusterConnection = {};
};

struct TStartOperationResponse {
    EOperationStatus Status;
    TString OperationId;
};

struct TGetOperationRequest {
    TString OperationId;
};

struct TGetOperationResponse {
    EOperationStatus Status;
    std::vector<TFmrError> ErrorMessages = {};
};

struct TDeleteOperationRequest {
    TString OperationId;
};

struct TDeleteOperationResponse {
    EOperationStatus Status;
};

struct TGetFmrTableInfoRequest {
    TString TableId;
};

struct TGetFmrTableInfoResponse {
    TTableStats TableStats; // for only one PartId
    std::vector<TFmrError> ErrorMessages = {};
};

class IFmrCoordinator: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IFmrCoordinator>;

    virtual ~IFmrCoordinator() = default;

    virtual NThreading::TFuture<TStartOperationResponse> StartOperation(const TStartOperationRequest& request) = 0;

    virtual NThreading::TFuture<TGetOperationResponse> GetOperation(const TGetOperationRequest& request) = 0;

    virtual NThreading::TFuture<TDeleteOperationResponse> DeleteOperation(const TDeleteOperationRequest& request) = 0;

    virtual NThreading::TFuture<THeartbeatResponse> SendHeartbeatResponse(const THeartbeatRequest& request) = 0;

    virtual NThreading::TFuture<TGetFmrTableInfoResponse> GetFmrTableInfo(const TGetFmrTableInfoRequest& request) = 0;
};

} // namespace NYql::NFmr

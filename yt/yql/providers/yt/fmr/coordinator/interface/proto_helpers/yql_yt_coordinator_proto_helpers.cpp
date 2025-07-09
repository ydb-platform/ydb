#include "yql_yt_coordinator_proto_helpers.h"
#include <library/cpp/yson/node/node_io.h>

namespace NYql::NFmr {

NProto::THeartbeatRequest HeartbeatRequestToProto(const THeartbeatRequest& heartbeatRequest) {
    NProto::THeartbeatRequest protoHeartbeatRequest;
    protoHeartbeatRequest.SetWorkerId(heartbeatRequest.WorkerId);
    protoHeartbeatRequest.SetVolatileId(heartbeatRequest.VolatileId);
    std::vector<NProto::TTaskState> taskStates;
    for (size_t i = 0; i < heartbeatRequest.TaskStates.size(); ++i) {
        auto protoTaskState = TaskStateToProto(*heartbeatRequest.TaskStates[i]);
        protoHeartbeatRequest.AddTaskStates();
        protoHeartbeatRequest.MutableTaskStates(i)->Swap(&protoTaskState);
    }
    protoHeartbeatRequest.SetAvailableSlots(heartbeatRequest.AvailableSlots);
    return protoHeartbeatRequest;
}

THeartbeatRequest HeartbeatRequestFromProto(const NProto::THeartbeatRequest protoHeartbeatRequest) {
    THeartbeatRequest heartbeatRequest;
    heartbeatRequest.WorkerId = protoHeartbeatRequest.GetWorkerId();
    heartbeatRequest.VolatileId = protoHeartbeatRequest.GetVolatileId();
    std::vector<TTaskState::TPtr> taskStates;
    for (size_t i = 0; i < protoHeartbeatRequest.TaskStatesSize(); ++i) {
        TTaskState curTaskState = TaskStateFromProto(protoHeartbeatRequest.GetTaskStates(i));
        taskStates.emplace_back(TIntrusivePtr<TTaskState>(new TTaskState(curTaskState)));
    }
    heartbeatRequest.TaskStates = taskStates;
    heartbeatRequest.AvailableSlots = protoHeartbeatRequest.GetAvailableSlots();
    return heartbeatRequest;
}

NProto::THeartbeatResponse HeartbeatResponseToProto(const THeartbeatResponse& heartbeatResponse) {
    NProto::THeartbeatResponse protoHeartbeatResponse;
    for (size_t i = 0; i < heartbeatResponse.TasksToRun.size(); ++i) {
        auto protoTask = TaskToProto(*heartbeatResponse.TasksToRun[i]);
        auto * taskToRun = protoHeartbeatResponse.AddTasksToRun();
        taskToRun->Swap(&protoTask);
    }
    for (auto& id: heartbeatResponse.TaskToDeleteIds) {
        protoHeartbeatResponse.AddTaskToDeleteIds(id);
    }
    protoHeartbeatResponse.SetNeedToRestart(heartbeatResponse.NeedToRestart);
    return protoHeartbeatResponse;
}

THeartbeatResponse HeartbeatResponseFromProto(const NProto::THeartbeatResponse& protoHeartbeatResponse) {
    THeartbeatResponse heartbeatResponse;
    std::vector<TTask::TPtr> tasksToRun;
    std::unordered_set<TString> taskToDeleteIds;
    for (size_t i = 0; i < protoHeartbeatResponse.TasksToRunSize(); ++i) {
        TTask curTask = TaskFromProto(protoHeartbeatResponse.GetTasksToRun(i));
        tasksToRun.emplace_back(TIntrusivePtr<TTask>(new TTask(curTask)));
    }
    for (size_t i = 0; i < protoHeartbeatResponse.TaskToDeleteIdsSize(); ++i) {
        taskToDeleteIds.emplace(protoHeartbeatResponse.GetTaskToDeleteIds(i));
    }

    heartbeatResponse.TasksToRun = tasksToRun;
    heartbeatResponse.TaskToDeleteIds = taskToDeleteIds;
    heartbeatResponse.NeedToRestart = protoHeartbeatResponse.GetNeedToRestart();
    return heartbeatResponse;
}

NProto::TStartOperationRequest StartOperationRequestToProto(const TStartOperationRequest& startOperationRequest) {
    NProto::TStartOperationRequest protoStartOperationRequest;
    protoStartOperationRequest.SetTaskType(static_cast<NProto::ETaskType>(startOperationRequest.TaskType));
    auto protoOperationParams = OperationParamsToProto(startOperationRequest.OperationParams);
    protoStartOperationRequest.MutableOperationParams()->Swap(&protoOperationParams);

    protoStartOperationRequest.SetSessionId(startOperationRequest.SessionId);
    if (startOperationRequest.IdempotencyKey) {
        protoStartOperationRequest.SetIdempotencyKey(*startOperationRequest.IdempotencyKey);
    }
    protoStartOperationRequest.SetNumRetries(startOperationRequest.NumRetries);
    auto& clusterConnections = *protoStartOperationRequest.MutableClusterConnections();
    for (auto& [tableName, conn]: startOperationRequest.ClusterConnections) {
        clusterConnections[tableName.Id] = ClusterConnectionToProto(conn);
    }
    if (startOperationRequest.FmrOperationSpec) {
        protoStartOperationRequest.SetFmrOperationSpec(NYT::NodeToYsonString(*startOperationRequest.FmrOperationSpec));
    }
    return protoStartOperationRequest;
}

TStartOperationRequest StartOperationRequestFromProto(const NProto::TStartOperationRequest& protoStartOperationRequest) {
    TStartOperationRequest startOperationRequest;
    startOperationRequest.TaskType = static_cast<ETaskType>(protoStartOperationRequest.GetTaskType());
    startOperationRequest.OperationParams = OperationParamsFromProto(protoStartOperationRequest.GetOperationParams());
    startOperationRequest.SessionId = protoStartOperationRequest.GetSessionId();
    if (protoStartOperationRequest.HasIdempotencyKey()) {
        startOperationRequest.IdempotencyKey = protoStartOperationRequest.GetIdempotencyKey();
    }
    startOperationRequest.NumRetries = protoStartOperationRequest.GetNumRetries();
    std::unordered_map<TFmrTableId, TClusterConnection> startOperationRequestClusterConnections;
    for (auto& [tableName, conn]: protoStartOperationRequest.GetClusterConnections()) {
        startOperationRequestClusterConnections[tableName] = ClusterConnectionFromProto(conn);
    }
    startOperationRequest.ClusterConnections = startOperationRequestClusterConnections;
    if (protoStartOperationRequest.HasFmrOperationSpec()) {
        startOperationRequest.FmrOperationSpec = NYT::NodeFromYsonString(protoStartOperationRequest.GetFmrOperationSpec());
    }
    return startOperationRequest;
}

NProto::TStartOperationResponse StartOperationResponseToProto(const TStartOperationResponse& startOperationResponse) {
    NProto::TStartOperationResponse protoStartOperationResponse;
    protoStartOperationResponse.SetOperationId(startOperationResponse.OperationId);
    protoStartOperationResponse.SetStatus(static_cast<NProto::EOperationStatus>(startOperationResponse.Status));
    return protoStartOperationResponse;
}

TStartOperationResponse StartOperationResponseFromProto(const NProto::TStartOperationResponse& protoStartOperationResponse) {
    return TStartOperationResponse{
        .Status = static_cast<EOperationStatus>(protoStartOperationResponse.GetStatus()),
        .OperationId = protoStartOperationResponse.GetOperationId()
    };
}

NProto::TGetOperationResponse GetOperationResponseToProto(const TGetOperationResponse& getOperationResponse) {
    NProto::TGetOperationResponse protoGetOperationResponse;
    protoGetOperationResponse.SetStatus(static_cast<NProto::EOperationStatus>(getOperationResponse.Status));
    for (auto& errorMessage: getOperationResponse.ErrorMessages) {
        auto* curError = protoGetOperationResponse.AddErrorMessages();
        auto protoError = FmrErrorToProto(errorMessage);
        curError->Swap(&protoError);
    }
    for (auto& tableStats: getOperationResponse.OutputTablesStats) {
        auto* curTableStats = protoGetOperationResponse.AddTableStats();
        auto protoTableStats = TableStatsToProto(tableStats);
        curTableStats->Swap(&protoTableStats);
    }
    return protoGetOperationResponse;
}

TGetOperationResponse GetOperationResponseFromProto(const NProto::TGetOperationResponse protoGetOperationReponse) {
    TGetOperationResponse getOperationResponse;
    getOperationResponse.Status = static_cast<EOperationStatus>(protoGetOperationReponse.GetStatus());
    std::vector<TFmrError> errorMessages;
    std::vector<TTableStats> outputTableStats;
    for (size_t i = 0; i < protoGetOperationReponse.ErrorMessagesSize(); ++i) {
        TFmrError errorMessage = FmrErrorFromProto(protoGetOperationReponse.GetErrorMessages(i));
        errorMessages.emplace_back(errorMessage);
    }
    for (size_t i = 0; i < protoGetOperationReponse.TableStatsSize(); ++i) {
        TTableStats tableStats = TableStatsFromProto(protoGetOperationReponse.GetTableStats(i));
        outputTableStats.emplace_back(tableStats);
    }
    getOperationResponse.ErrorMessages = errorMessages;
    getOperationResponse.OutputTablesStats = outputTableStats;
    return getOperationResponse;
}

NProto::TDeleteOperationResponse DeleteOperationResponseToProto(const TDeleteOperationResponse& deleteOperationResponse) {
    NProto::TDeleteOperationResponse protoDeleteOperationResponse;
    protoDeleteOperationResponse.SetStatus(static_cast<NProto::EOperationStatus>(deleteOperationResponse.Status));
    return protoDeleteOperationResponse;
}

TDeleteOperationResponse DeleteOperationResponseFromProto(const NProto::TDeleteOperationResponse& protoDeleteOperationResponse) {
    return TDeleteOperationResponse{.Status = static_cast<EOperationStatus>(protoDeleteOperationResponse.GetStatus())};
}

NProto::TGetFmrTableInfoRequest GetFmrTableInfoRequestToProto(const TGetFmrTableInfoRequest& getFmrTableInfoRequest) {
    NProto::TGetFmrTableInfoRequest protoRequest;
    protoRequest.SetTableId(getFmrTableInfoRequest.TableId);
    return protoRequest;
}

TGetFmrTableInfoRequest GetFmrTableInfoRequestFromProto(const NProto::TGetFmrTableInfoRequest& protoGetFmrTableInfoRequest) {
    return TGetFmrTableInfoRequest{.TableId = protoGetFmrTableInfoRequest.GetTableId()};
}

NProto::TGetFmrTableInfoResponse GetFmrTableInfoResponseToProto(const TGetFmrTableInfoResponse& getFmrTableInfoResponse) {
    NProto::TGetFmrTableInfoResponse protoGetFmrTableInfoResponse;
    NProto::TTableStats protoTableStats = TableStatsToProto(getFmrTableInfoResponse.TableStats);
    protoGetFmrTableInfoResponse.MutableTableStats()->Swap(&protoTableStats);
    for (auto& errorMessage: getFmrTableInfoResponse.ErrorMessages) {
        auto* curError = protoGetFmrTableInfoResponse.AddErrorMessages();
        auto protoError = FmrErrorToProto(errorMessage);
        curError->Swap(&protoError);
    }
    return protoGetFmrTableInfoResponse;
}

TGetFmrTableInfoResponse GetFmrTableInfoResponseFromProto(const NProto::TGetFmrTableInfoResponse& protoGetFmrTableInfoResponse) {
    TGetFmrTableInfoResponse getFmrTableInfoResponse;
    getFmrTableInfoResponse.TableStats = TableStatsFromProto(protoGetFmrTableInfoResponse.GetTableStats());
    std::vector<TFmrError> errorMessages;
    for (size_t i = 0; i < protoGetFmrTableInfoResponse.ErrorMessagesSize(); ++i) {
        TFmrError errorMessage = FmrErrorFromProto(protoGetFmrTableInfoResponse.GetErrorMessages(i));
        errorMessages.emplace_back(errorMessage);
    }
    getFmrTableInfoResponse.ErrorMessages = errorMessages;
    return getFmrTableInfoResponse;
}

NProto::TClearSessionRequest ClearSessionRequestToProto(const TClearSessionRequest& request) {
    NProto::TClearSessionRequest protoRequest;
    protoRequest.SetSessionId(request.SessionId);
    return protoRequest;
}

TClearSessionRequest ClearSessionRequestFromProto(const NProto::TClearSessionRequest& protoRequest) {
    return TClearSessionRequest{.SessionId = protoRequest.GetSessionId()};
}

} // namespace NYql::NFmr

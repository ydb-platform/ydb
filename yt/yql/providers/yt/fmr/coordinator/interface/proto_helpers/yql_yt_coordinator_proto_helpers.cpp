#include "yql_yt_coordinator_proto_helpers.h"

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
    return heartbeatResponse;
}

NProto::TStartOperationRequest StartOperationRequestToProto(const TStartOperationRequest& startOperationRequest) {
    NProto::TStartOperationRequest protoStartOperationRequest;
    protoStartOperationRequest.SetTaskType(static_cast<NProto::ETaskType>(startOperationRequest.TaskType));
    auto protoTaskParams = TaskParamsToProto(startOperationRequest.TaskParams);
    protoStartOperationRequest.MutableTaskParams()->Swap(&protoTaskParams);
    protoStartOperationRequest.SetSessionId(startOperationRequest.SessionId);
    if (startOperationRequest.IdempotencyKey) {
        protoStartOperationRequest.SetIdempotencyKey(*startOperationRequest.IdempotencyKey);
    }
    protoStartOperationRequest.SetNumRetries(startOperationRequest.NumRetries);
    auto protoClusterConnection = ClusterConnectionToProto(startOperationRequest.ClusterConnection);
    protoStartOperationRequest.MutableClusterConnection()->Swap(&protoClusterConnection);
    return protoStartOperationRequest;
}

TStartOperationRequest StartOperationRequestFromProto(const NProto::TStartOperationRequest& protoStartOperationRequest) {
    TStartOperationRequest startOperationRequest;
    startOperationRequest.TaskType = static_cast<ETaskType>(protoStartOperationRequest.GetTaskType());
    startOperationRequest.TaskParams = TaskParamsFromProto(protoStartOperationRequest.GetTaskParams());
    startOperationRequest.SessionId = protoStartOperationRequest.GetSessionId();
    if (protoStartOperationRequest.HasIdempotencyKey()) {
        startOperationRequest.IdempotencyKey = protoStartOperationRequest.GetIdempotencyKey();
    }
    startOperationRequest.NumRetries = protoStartOperationRequest.GetNumRetries();
    startOperationRequest.ClusterConnection = ClusterConnectionFromProto(protoStartOperationRequest.GetClusterConnection());
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
    return protoGetOperationResponse;
}

TGetOperationResponse GetOperationResponseFromProto(const NProto::TGetOperationResponse protoGetOperationReponse) {
    TGetOperationResponse getOperationResponse;
    getOperationResponse.Status = static_cast<EOperationStatus>(protoGetOperationReponse.GetStatus());
    std::vector<TFmrError> errorMessages;
    for (size_t i = 0; i < protoGetOperationReponse.ErrorMessagesSize(); ++i) {
        TFmrError errorMessage = FmrErrorFromProto(protoGetOperationReponse.GetErrorMessages(i));
        errorMessages.emplace_back(errorMessage);
    }
    getOperationResponse.ErrorMessages = errorMessages;
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

} // namespace NYql::NFmr

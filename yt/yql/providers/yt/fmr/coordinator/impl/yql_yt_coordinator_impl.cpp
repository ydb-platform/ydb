#include <thread>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>
#include "yql_yt_coordinator_impl.h"

namespace NYql::NFmr {

namespace {

struct TCoordinatorTaskInfo {
    TTask::TPtr Task;
    ETaskStatus TaskStatus;
    TString OperationId;
};

struct TOperationInfo {
    std::unordered_set<TString> TaskIds; // for now each operation consists only of one task, until paritioner is implemented
    EOperationStatus OperationStatus;
    std::vector<TFmrError> ErrorMessages;
    TString SessionId;
};

struct TIdempotencyKeyInfo {
    TString operationId;
    TInstant OperationCreationTime;
};

class TFmrCoordinator: public IFmrCoordinator {
public:
    TFmrCoordinator(const TFmrCoordinatorSettings& settings)
        : WorkersNum_(settings.WorkersNum),
        RandomProvider_(settings.RandomProvider),
        StopCoordinator_(false),
        TimeToSleepBetweenClearKeyRequests_(settings.TimeToSleepBetweenClearKeyRequests),
        IdempotencyKeyStoreTime_(settings.IdempotencyKeyStoreTime)
    {
        StartClearingIdempotencyKeys();
    }

    ~TFmrCoordinator() {
        StopCoordinator_ = true;
        ClearIdempotencyKeysThread_.join();
    }

    NThreading::TFuture<TStartOperationResponse> StartOperation(const TStartOperationRequest& request) override {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(request.SessionId);
        TGuard<TMutex> guard(Mutex_);
        TMaybe<TString> IdempotencyKey = request.IdempotencyKey;
        if (IdempotencyKey && IdempotencyKeys_.contains(*IdempotencyKey)) {
            auto operationId = IdempotencyKeys_[*IdempotencyKey].operationId;
            auto& operationInfo = Operations_[operationId];
            return NThreading::MakeFuture(TStartOperationResponse(operationInfo.OperationStatus, operationId));
        }
        auto operationId = GenerateId();
        if (IdempotencyKey) {
            IdempotencyKeys_[*IdempotencyKey] = TIdempotencyKeyInfo{.operationId = operationId, .OperationCreationTime=TInstant::Now()};
        }

        TString taskId = GenerateId();
        TTask::TPtr createdTask = MakeTask(request.TaskType, taskId, request.TaskParams, request.SessionId);

        Tasks_[taskId] = TCoordinatorTaskInfo{.Task = createdTask, .TaskStatus = ETaskStatus::Accepted, .OperationId = operationId};

        Operations_[operationId] = {.TaskIds = {taskId}, .OperationStatus = EOperationStatus::Accepted, .SessionId = request.SessionId};
        YQL_CLOG(DEBUG, FastMapReduce) << "Starting operation with id " << operationId;
        return NThreading::MakeFuture(TStartOperationResponse(EOperationStatus::Accepted, operationId));
    }

    NThreading::TFuture<TGetOperationResponse> GetOperation(const TGetOperationRequest& request) override {
        TGuard<TMutex> guard(Mutex_);
        auto operationId = request.OperationId;
        if (!Operations_.contains(operationId)) {
            return NThreading::MakeFuture(TGetOperationResponse(EOperationStatus::NotFound));
        }
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(Operations_[operationId].SessionId);
        YQL_CLOG(DEBUG, FastMapReduce) << "Getting operation status with id " << operationId;
        auto& operationInfo = Operations_[operationId];
        auto operationStatus =  operationInfo.OperationStatus;
        auto errorMessages = operationInfo.ErrorMessages;
        return NThreading::MakeFuture(TGetOperationResponse(operationStatus, errorMessages));
    }

    NThreading::TFuture<TDeleteOperationResponse> DeleteOperation(const TDeleteOperationRequest& request) override {
        TGuard<TMutex> guard(Mutex_);
        auto operationId = request.OperationId;
        if (!Operations_.contains(operationId)) {
            return NThreading::MakeFuture(TDeleteOperationResponse(EOperationStatus::NotFound));
        }
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(Operations_[operationId].SessionId);
        YQL_CLOG(DEBUG, FastMapReduce) << "Deleting operation with id " << operationId;
        auto taskIds = Operations_[operationId].TaskIds;
        YQL_ENSURE(taskIds.size() == 1);
        auto taskId = *taskIds.begin();
        YQL_ENSURE(Tasks_.contains(taskId));

        auto taskStatus = Tasks_[taskId].TaskStatus;
        if (taskStatus == ETaskStatus::InProgress) {
            TaskToDeleteIds_.insert(taskId); // Task is currently running, send signal to worker to cancel
        } else {
            ClearTask(taskId); // Task either hasn't begun running or finished, remove info
        }

        return NThreading::MakeFuture(TDeleteOperationResponse(EOperationStatus::Aborted));
    }

    NThreading::TFuture<THeartbeatResponse> SendHeartbeatResponse(const THeartbeatRequest& request) override {
        TGuard<TMutex> guard(Mutex_);

        ui32 workerId = request.WorkerId;
        YQL_ENSURE(workerId >= 1 && workerId <= WorkersNum_);
        if (! workerToVolatileId_.contains(workerId)) {
            workerToVolatileId_[workerId] = request.VolatileId;
        } else if (request.VolatileId != workerToVolatileId_[workerId]) {
            workerToVolatileId_[workerId] = request.VolatileId;
            for (auto& [taskId, taskInfo]: Tasks_) {
                auto taskStatus = Tasks_[taskId].TaskStatus;
                auto operationId = Tasks_[taskId].OperationId;
                if (taskStatus == ETaskStatus::InProgress) {
                    TaskToDeleteIds_.insert(taskId); // Task is currently running, send signal to worker to cancel
                    TString sessionId = Operations_[operationId].SessionId;
                    TFmrError error{
                        .Component = EFmrComponent::Coordinator, .ErrorMessage = "Max retries limit exceeded", .OperationId = operationId};
                    SetUnfinishedTaskStatus(taskId, ETaskStatus::Failed, error);
                }
            }
        }

        for (auto& requestTaskState: request.TaskStates) {
            auto taskId = requestTaskState->TaskId;
            YQL_ENSURE(Tasks_.contains(taskId));
            auto taskStatus = requestTaskState->TaskStatus;
            YQL_ENSURE(taskStatus != ETaskStatus::Accepted);
            SetUnfinishedTaskStatus(taskId, taskStatus, requestTaskState->TaskErrorMessage);
            if (TaskToDeleteIds_.contains(taskId) && Tasks_[taskId].TaskStatus != ETaskStatus::InProgress) {
                ClearTask(taskId); // Task finished, so we don't need to cancel it, just remove info
            }
        }

        std::vector<TTask::TPtr> tasksToRun;
        for (auto& taskToRunInfo: Tasks_) {
            if (taskToRunInfo.second.TaskStatus == ETaskStatus::Accepted) {
                SetUnfinishedTaskStatus(taskToRunInfo.first, ETaskStatus::InProgress);
                tasksToRun.emplace_back(taskToRunInfo.second.Task);
            }
        }

        for (auto& taskId: TaskToDeleteIds_) {
            SetUnfinishedTaskStatus(taskId, ETaskStatus::Aborted);
        }
        return NThreading::MakeFuture(THeartbeatResponse{.TasksToRun = tasksToRun, .TaskToDeleteIds = TaskToDeleteIds_});
    }

private:

    void StartClearingIdempotencyKeys() {
        auto ClearIdempotencyKeysFunc = [&] () {
            while (!StopCoordinator_) {
                with_lock(Mutex_) {
                    auto currentTime = TInstant::Now();
                    for (auto it = IdempotencyKeys_.begin(); it != IdempotencyKeys_.end();) {
                        auto operationCreationTime = it->second.OperationCreationTime;
                        auto operationId = it->second.operationId;
                        if (currentTime - operationCreationTime > IdempotencyKeyStoreTime_) {
                            it = IdempotencyKeys_.erase(it);
                            if (Operations_.contains(operationId)) {
                                auto& operationInfo = Operations_[operationId];
                                auto operationStatus = operationInfo.OperationStatus;
                                auto& taskIds = operationInfo.TaskIds;
                                YQL_ENSURE(taskIds.size() == 1);
                                auto taskId = *operationInfo.TaskIds.begin();
                                if (operationStatus != EOperationStatus::Accepted && operationStatus != EOperationStatus::InProgress) {
                                    ClearTask(taskId);
                                }
                            }
                        } else {
                            ++it;
                        }
                    }
                }
                Sleep(TimeToSleepBetweenClearKeyRequests_);
            }
        };
        ClearIdempotencyKeysThread_ = std::thread(ClearIdempotencyKeysFunc);
    }

    TString GenerateId() {
        return GetGuidAsString(RandomProvider_->GenGuid());
    }

    void ClearTask(const TString& taskId) {
        YQL_ENSURE(Tasks_.contains(taskId));
        auto& taskInfo = Tasks_[taskId];
        TaskToDeleteIds_.erase(taskId);
        Operations_.erase(taskInfo.OperationId);
        Tasks_.erase(taskId);
    }

    void SetUnfinishedTaskStatus(const TString& taskId, ETaskStatus newTaskStatus, const TMaybe<TFmrError>& taskErrorMessage = Nothing()) {
        auto& taskInfo = Tasks_[taskId];
        YQL_ENSURE(Operations_.contains(taskInfo.OperationId));
        auto& operationInfo = Operations_[taskInfo.OperationId];
        if (taskInfo.TaskStatus != ETaskStatus::Accepted && taskInfo.TaskStatus != ETaskStatus::InProgress) {
            return;
        }
        taskInfo.TaskStatus = newTaskStatus;
        operationInfo.OperationStatus = GetOperationStatus(taskInfo.OperationId);
        if (taskErrorMessage) {
            auto& errorMessages = operationInfo.ErrorMessages;
            errorMessages.emplace_back(*taskErrorMessage);
        }
    }

    EOperationStatus GetOperationStatus(const TString& operationId) {
        if (! Operations_.contains(operationId)) {
            return EOperationStatus::NotFound;
        }
        std::unordered_set<TString> taskIds = Operations_[operationId].TaskIds;
        YQL_ENSURE(taskIds.size() == 1);

        auto taskId = *taskIds.begin();
        ETaskStatus taskStatus = Tasks_[taskId].TaskStatus;
        return static_cast<EOperationStatus>(taskStatus);
    }

    std::unordered_map<TString, TCoordinatorTaskInfo> Tasks_; // TaskId -> current info about it
    std::unordered_set<TString> TaskToDeleteIds_; // TaskIds we want to pass to worker for deletion
    std::unordered_map<TString, TOperationInfo> Operations_; // OperationId -> current info about it
    std::unordered_map<TString, TIdempotencyKeyInfo> IdempotencyKeys_; // IdempotencyKey -> current info about it

    TMutex Mutex_;
    const ui32 WorkersNum_;
    std::unordered_map<ui32, TString> workerToVolatileId_; // worker id -> volatile id
    const TIntrusivePtr<IRandomProvider> RandomProvider_;
    std::thread ClearIdempotencyKeysThread_;
    std::atomic<bool> StopCoordinator_;
    TDuration TimeToSleepBetweenClearKeyRequests_;
    TDuration IdempotencyKeyStoreTime_;
};

} // namespace

IFmrCoordinator::TPtr MakeFmrCoordinator(const TFmrCoordinatorSettings& settings) {
    return MakeIntrusive<TFmrCoordinator>(settings);
}

} // namespace NYql::NFmr

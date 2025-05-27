#include <thread>
#include <library/cpp/resource/resource.h>
#include <yt/cpp/mapreduce/common/helpers.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>
#include "yql_yt_coordinator_impl.h"

namespace NYql::NFmr {

TFmrCoordinatorSettings::TFmrCoordinatorSettings() {
    DefaultFmrOperationSpec = NYT::NodeFromYsonString(NResource::Find("default_coordinator_settings.yson"));
    WorkersNum = 1;
    RandomProvider = CreateDefaultRandomProvider(),
    IdempotencyKeyStoreTime = TDuration::Seconds(10);
    TimeToSleepBetweenClearKeyRequests = TDuration::Seconds(1);
}

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
    std::vector<TString> OutputTableIds = {};
};

struct TIdempotencyKeyInfo {
    TString OperationId;
    TInstant OperationCreationTime;
};

struct TCoordinatorFmrTableStats {
    TTableStats Stats;
    TString PartId; // only one PartId for now
};

class TFmrCoordinator: public IFmrCoordinator {
public:
    TFmrCoordinator(const TFmrCoordinatorSettings& settings)
        : WorkersNum_(settings.WorkersNum),
        RandomProvider_(settings.RandomProvider),
        StopCoordinator_(false),
        TimeToSleepBetweenClearKeyRequests_(settings.TimeToSleepBetweenClearKeyRequests),
        IdempotencyKeyStoreTime_(settings.IdempotencyKeyStoreTime),
        DefaultFmrOperationSpec_(settings.DefaultFmrOperationSpec)
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
            auto operationId = IdempotencyKeys_[*IdempotencyKey].OperationId;
            auto& operationInfo = Operations_[operationId];
            return NThreading::MakeFuture(TStartOperationResponse(operationInfo.OperationStatus, operationId));
        }
        auto operationId = GenerateId();
        if (IdempotencyKey) {
            IdempotencyKeys_[*IdempotencyKey] = TIdempotencyKeyInfo{.OperationId = operationId, .OperationCreationTime=TInstant::Now()};
        }

        TString taskId = GenerateId();
        auto taskParams = MakeDefaultTaskParamsFromOperation(request.OperationParams);

        TTask::TPtr createdTask = MakeTask(request.TaskType, taskId, taskParams, request.SessionId, request.ClusterConnections, GetJobSettings(request.FmrOperationSpec));
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
        YQL_CLOG(TRACE, FastMapReduce) << "Getting operation status with id " << operationId;
        auto& operationInfo = Operations_[operationId];
        auto operationStatus =  operationInfo.OperationStatus;
        auto errorMessages = operationInfo.ErrorMessages;
        std::vector<TTableStats> outputTablesStats;
        for (auto& tableId : operationInfo.OutputTableIds) {
            outputTablesStats.emplace_back(FmrTableStatistics_[tableId].Stats);
        }
        return NThreading::MakeFuture(TGetOperationResponse(operationStatus, errorMessages, outputTablesStats));
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
        YQL_ENSURE(workerId >= 0 && workerId < WorkersNum_);
        if (!WorkerToVolatileId_.contains(workerId)) {
            WorkerToVolatileId_[workerId] = request.VolatileId;
        } else if (request.VolatileId != WorkerToVolatileId_[workerId]) {
            WorkerToVolatileId_[workerId] = request.VolatileId;
            for (auto& [taskId, taskInfo]: Tasks_) {
                auto taskStatus = Tasks_[taskId].TaskStatus;
                auto operationId = Tasks_[taskId].OperationId;
                if (taskStatus == ETaskStatus::InProgress) {
                    TaskToDeleteIds_.insert(taskId); // Task is currently running, send signal to worker to cancel
                    TFmrError error{
                        .Component = EFmrComponent::Coordinator, .ErrorMessage = "Max retries limit exceeded", .OperationId = operationId};
                    SetUnfinishedTaskStatus(taskId, ETaskStatus::Failed, error);
                }
            }
        }

        for (auto& requestTaskState: request.TaskStates) {
            auto taskId = requestTaskState->TaskId;
            auto operationId = Tasks_[taskId].OperationId;
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(Operations_[operationId].SessionId);
            YQL_ENSURE(Tasks_.contains(taskId));
            auto taskStatus = requestTaskState->TaskStatus;
            YQL_ENSURE(taskStatus != ETaskStatus::Accepted);
            SetUnfinishedTaskStatus(taskId, taskStatus, requestTaskState->TaskErrorMessage);
            if (TaskToDeleteIds_.contains(taskId) && Tasks_[taskId].TaskStatus != ETaskStatus::InProgress) {
                ClearTask(taskId); // Task finished, so we don't need to cancel it, just remove info
            }

            auto statistics = requestTaskState->Stats;
            for (auto& [fmrTableId, tableStats]: statistics.OutputTables) {
                if (FmrTableStatistics_.contains(fmrTableId.TableId)) {
                    auto curTableStats = FmrTableStatistics_[fmrTableId.TableId];
                    YQL_ENSURE(
                        tableStats.Chunks >= curTableStats.Stats.Chunks &&
                        tableStats.DataWeight >= curTableStats.Stats.DataWeight &&
                        tableStats.Rows >= curTableStats.Stats.Rows
                    );
                    YQL_ENSURE(fmrTableId.PartId == curTableStats.PartId);
                    if (taskStatus == ETaskStatus::Completed) {
                        YQL_CLOG(DEBUG, FastMapReduce) << "Current statistic from table with id" << fmrTableId.TableId << "_" << fmrTableId.PartId << ": " << tableStats;
                    }
                }
                Operations_[operationId].OutputTableIds.emplace_back(fmrTableId.TableId);
                FmrTableStatistics_[fmrTableId.TableId] = TCoordinatorFmrTableStats{
                    .Stats = tableStats,
                    .PartId = fmrTableId.PartId
                };
            }
        }

        std::vector<TTask::TPtr> tasksToRun;
        for (auto& taskToRunInfo: Tasks_) {
            if (taskToRunInfo.second.TaskStatus == ETaskStatus::Accepted) {
                SetUnfinishedTaskStatus(taskToRunInfo.first, ETaskStatus::InProgress);
                tasksToRun.emplace_back(taskToRunInfo.second.Task);
            }
        }
        return NThreading::MakeFuture(THeartbeatResponse{.TasksToRun = tasksToRun, .TaskToDeleteIds = TaskToDeleteIds_});
    }

    NThreading::TFuture<TGetFmrTableInfoResponse> GetFmrTableInfo(const TGetFmrTableInfoRequest& request) override {
        TGuard<TMutex> guard(Mutex_);
        TGetFmrTableInfoResponse response;
        auto tableId = request.TableId;
        if (!FmrTableStatistics_.contains(tableId)) {
            response.ErrorMessages = {TFmrError{
                .Component = EFmrComponent::Coordinator, .ErrorMessage = "Fmr table id " + tableId + " was not found"
            }};
            return NThreading::MakeFuture(response);
        }
        response.TableStats = FmrTableStatistics_[tableId].Stats;
        return NThreading::MakeFuture(response);
    }

private:

    void StartClearingIdempotencyKeys() {
        auto ClearIdempotencyKeysFunc = [&] () {
            while (!StopCoordinator_) {
                with_lock(Mutex_) {
                    auto currentTime = TInstant::Now();
                    for (auto it = IdempotencyKeys_.begin(); it != IdempotencyKeys_.end();) {
                        auto operationCreationTime = it->second.OperationCreationTime;
                        auto operationId = it->second.OperationId;
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

    TTableRange GetTableRangeFromId(const TString& tableId) {
        if (!FmrTableStatistics_.contains(tableId)) {
            TString partId = GenerateId();
            FmrTableStatistics_[tableId] = TCoordinatorFmrTableStats{.Stats=TTableStats{}, .PartId=partId};
            return TTableRange{.PartId = partId};
        }
        auto fmrTableStats = FmrTableStatistics_[tableId];
        return TTableRange{
            .PartId = fmrTableStats.PartId,
            .MinChunk = 0,
            .MaxChunk = fmrTableStats.Stats.Chunks
        };
    }

    std::vector<TTaskTableRef> TaskInputTablesFromOperationInputTables(const std::vector<TOperationTableRef>& operationTables) {
        std::vector<TTaskTableRef> taskInputTables;
        for (auto& elem: operationTables) {
            if (const TYtTableRef* ytTableRef = std::get_if<TYtTableRef>(&elem)) {
                taskInputTables.emplace_back(*ytTableRef);
            } else {
                TFmrTableRef fmrTableRef = std::get<TFmrTableRef>(elem);
                TString inputTableId = fmrTableRef.FmrTableId.Id;
                TFmrTableInputRef tableInput{
                    .TableId = inputTableId,
                    .TableRanges = {GetTableRangeFromId(inputTableId)}
                };
                taskInputTables.emplace_back(tableInput);
            }
        }
        return taskInputTables;
    }

    std::vector<TFmrTableOutputRef> TaskOutputTablesFromOperationOutputTables(const std::vector<TFmrTableRef>& operationTables) {
        std::vector<TFmrTableOutputRef> taskOutputTables;
        for (auto& fmrTableRef: operationTables) {
                TString outputTableId = fmrTableRef.FmrTableId.Id;
                TFmrTableOutputRef tableOutput{
                    .TableId = outputTableId,
                    .PartId = GetTableRangeFromId(outputTableId).PartId
                };
                taskOutputTables.emplace_back(tableOutput);
            }
        return taskOutputTables;
    }

    TTaskParams MakeDefaultTaskParamsFromOperation(const TOperationParams& operationParams) {
        if (const TUploadOperationParams* uploadOperationParams = std::get_if<TUploadOperationParams>(&operationParams)) {
            TUploadTaskParams uploadTaskParams{};
            uploadTaskParams.Output = uploadOperationParams->Output;
            TString inputTableId = uploadOperationParams->Input.FmrTableId.Id;
            TFmrTableInputRef fmrTableInput{
                .TableId = inputTableId,
                .TableRanges = {GetTableRangeFromId(inputTableId)}
            };
            uploadTaskParams.Input = fmrTableInput;
            return uploadTaskParams;
        } else if (const TDownloadOperationParams* downloadOperationParams = std::get_if<TDownloadOperationParams>(&operationParams)) {
            TDownloadTaskParams downloadTaskParams{};
            downloadTaskParams.Input = downloadOperationParams->Input;
            TString outputTableId = downloadOperationParams->Output.FmrTableId.Id;
            TFmrTableOutputRef fmrTableOutput{
                .TableId = outputTableId,
                .PartId = GetTableRangeFromId(outputTableId).PartId
            };
            downloadTaskParams.Output = fmrTableOutput;
            return downloadTaskParams;
        } else if (const TMergeOperationParams* mergeOperationParams = std::get_if<TMergeOperationParams>(&operationParams)) {
            TMergeTaskParams mergeTaskParams;
            mergeTaskParams.Input = TaskInputTablesFromOperationInputTables(mergeOperationParams->Input);
            TFmrTableOutputRef outputTable;
            mergeTaskParams.Output = TFmrTableOutputRef{.TableId = mergeOperationParams->Output.FmrTableId.Id};
            return mergeTaskParams;
        } else if (const TMapOperationParams* mapOperationParams = std::get_if<TMapOperationParams>(&operationParams)) {
            TMapTaskParams mapTaskParams;
            mapTaskParams.Input = TaskInputTablesFromOperationInputTables(mapOperationParams->Input);
            mapTaskParams.Output = TaskOutputTablesFromOperationOutputTables(mapOperationParams->Output);
            mapTaskParams.Executable = mapOperationParams->Executable;
            return mapTaskParams;
        } else {
            ythrow yexception() << "Unknown operation params";
        }
    }

    NYT::TNode GetJobSettings(const TMaybe<NYT::TNode>& currentFmrOperationSpec) {
        // For now fmr operation spec only consists of job settings
        if (!currentFmrOperationSpec) {
            return DefaultFmrOperationSpec_;
        }
        auto resultFmrOperationSpec = DefaultFmrOperationSpec_;
        NYT::MergeNodes(resultFmrOperationSpec, *currentFmrOperationSpec);
        return resultFmrOperationSpec;
    }

    std::unordered_map<TString, TCoordinatorTaskInfo> Tasks_; // TaskId -> current info about it
    std::unordered_set<TString> TaskToDeleteIds_; // TaskIds we want to pass to worker for deletion
    std::unordered_map<TString, TOperationInfo> Operations_; // OperationId -> current info about it
    std::unordered_map<TString, TIdempotencyKeyInfo> IdempotencyKeys_; // IdempotencyKey -> current info about it

    TMutex Mutex_;
    const ui32 WorkersNum_;
    std::unordered_map<ui32, TString> WorkerToVolatileId_; // worker id -> volatile id
    const TIntrusivePtr<IRandomProvider> RandomProvider_;
    std::thread ClearIdempotencyKeysThread_;
    std::atomic<bool> StopCoordinator_;
    TDuration TimeToSleepBetweenClearKeyRequests_;
    TDuration IdempotencyKeyStoreTime_;
    std::unordered_map<TFmrTableId, TCoordinatorFmrTableStats> FmrTableStatistics_; // TableId -> Statistics
    NYT::TNode DefaultFmrOperationSpec_;
};

} // namespace

IFmrCoordinator::TPtr MakeFmrCoordinator(const TFmrCoordinatorSettings& settings) {
    return MakeIntrusive<TFmrCoordinator>(settings);
}

} // namespace NYql::NFmr

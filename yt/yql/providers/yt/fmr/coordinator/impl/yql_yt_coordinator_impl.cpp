#include <algorithm>
#include <thread>
#include <library/cpp/resource/resource.h>
#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_fmr_partitioner.h>
#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_ordered_partitioner.h>
#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_sorted_partitioner.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_table_data_service_key.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/utils/failure_injector/failure_injector.h>
#include "yql_yt_coordinator_impl.h"

namespace NYql::NFmr {

TFmrCoordinatorSettings::TFmrCoordinatorSettings() {
    DefaultFmrOperationSpec = NYT::NodeFromYsonString(NResource::Find("default_operation_settings.yson"));
    WorkersNum = 1;
    RandomProvider = CreateDefaultRandomProvider();
    TimeProvider = CreateDefaultTimeProvider();
    IdempotencyKeyStoreTime = TDuration::Seconds(10);
    TimeToSleepBetweenClearKeyRequests = TDuration::Seconds(1);
    WorkerDeadlineLease = TDuration::Seconds(5);
    TimeToSleepBetweenCheckWorkerStatusRequests = TDuration::Seconds(1);
    SessionInactivityTimeout = TDuration::Minutes(5);
    HealthCheckInterval = TDuration::Seconds(1);
}

namespace {

class TFmrCoordinator: public IFmrCoordinator {
public:
    TFmrCoordinator(const TFmrCoordinatorSettings& settings, IYtCoordinatorService::TPtr ytCoordinatorService, IFmrGcService::TPtr gcService)
        : WorkersNum_(settings.WorkersNum),
        RandomProvider_(settings.RandomProvider),
        TimeProvider_(settings.TimeProvider),
        StopCoordinator_(false),
        TimeToSleepBetweenClearKeyRequests_(settings.TimeToSleepBetweenClearKeyRequests),
        IdempotencyKeyStoreTime_(settings.IdempotencyKeyStoreTime),
        WorkerDeadlineLease_(settings.WorkerDeadlineLease),
        TimeToSleepBetweenCheckWorkerStatusRequests_(settings.TimeToSleepBetweenCheckWorkerStatusRequests),
        SessionInactivityTimeout_(settings.SessionInactivityTimeout),
        HealthCheckInterval_(settings.HealthCheckInterval),
        DefaultFmrOperationSpec_(settings.DefaultFmrOperationSpec),
        YtCoordinatorService_(ytCoordinatorService),
        GcService_(gcService)
    {
        YQL_ENSURE(HealthCheckInterval_ > TDuration::Zero(),
            "HealthCheckInterval must be greater than 0");
        StartClearingIdempotencyKeys();
        CheckWorkersAliveStatus();
        CheckGatewaySessionsActivity();
        GcService_->ClearAll();
    }

    ~TFmrCoordinator() {
        StopCoordinator_ = true;
        ClearIdempotencyKeysThread_.join();
        CheckWorkersAliveStatusThread_.join();
        CheckGatewaySessionsActivityThread_.join();
    }

    NThreading::TFuture<TStartOperationResponse> StartOperation(const TStartOperationRequest& request) override {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(request.SessionId);
        TGuard<TMutex> guard(Mutex_);
        UpdateSessionActivity(request.SessionId);
        TMaybe<TString> IdempotencyKey = request.IdempotencyKey;
        if (IdempotencyKey && IdempotencyKeys_.contains(*IdempotencyKey)) {
            auto operationId = IdempotencyKeys_[*IdempotencyKey].OperationId;
            auto& operationInfo = Operations_[operationId];
            return NThreading::MakeFuture(TStartOperationResponse(operationInfo.OperationStatus, operationId));
        }
        auto operationId = GenerateId();
        YQL_ENSURE(Sessions_.contains(request.SessionId), "Session " << request.SessionId << " must be opened before starting operations");
        auto& sessionInfo = Sessions_[request.SessionId];
        sessionInfo.OperationIds.emplace_back(operationId);
        if (IdempotencyKey) {
            IdempotencyKeys_[*IdempotencyKey] = TIdempotencyKeyInfo{.OperationId = operationId, .OperationCreationTime=TimeProvider_->Now()};
        }

        auto fmrOperationSpec = GetMergedFmrOperationSpec(request.FmrOperationSpec);

        TString partitionId;

        try {
            if (auto distUploadParams = std::get_if<TSortedUploadOperationParams>(&request.OperationParams)) {
                partitionId = distUploadParams->PartitionId;
                YQL_ENSURE(OperationPartitions_.contains(partitionId), "Partition " << partitionId << " should to be prepered before starting operation");
            } else {
                partitionId = PartitionOperationIntoSeveralTasks(request.OperationParams, fmrOperationSpec, request.ClusterConnections);
            }
        } catch (const std::exception& e) {
            YQL_CLOG(ERROR, FastMapReduce) << "Failed to start operation with exception: " << e.what();
            return NThreading::MakeFuture(TStartOperationResponse(EOperationStatus::Failed, e.what()));
        }

        std::vector<TTaskParams> taskParams = GetOutputTaskParams(OperationPartitions_.at(partitionId), request.OperationParams);

        std::unordered_set<TString> taskIds;
        for (auto& currentTaskParams: taskParams) {
            TString taskId = GenerateId();
            auto fmrResourceTasks = PartitionFmrResourcesIntoTasks(request.FmrResources, fmrOperationSpec);
            SortedTasksPreprocess(currentTaskParams);

            TTask::TPtr createdTask = MakeTask(request.TaskType, taskId, currentTaskParams, request.SessionId, request.ClusterConnections, request.Files, request.YtResources, fmrResourceTasks, fmrOperationSpec);
            Tasks_[taskId] = TCoordinatorTaskInfo{.Task = createdTask, .TaskStatus = ETaskStatus::Accepted, .OperationId = operationId, .NumRetries = 0};
            TasksToRun_.emplace(createdTask, taskId);
            taskIds.emplace(taskId);
        }

        const auto initialStatus = taskIds.empty() ? EOperationStatus::Completed : EOperationStatus::Accepted;
        Operations_[operationId] = {.TaskIds = taskIds, .OperationStatus = initialStatus, .SessionId = request.SessionId};

        if (std::holds_alternative<TSortedUploadOperationParams>(request.OperationParams)) {
            InitializeSortedUploadOperation(operationId, taskIds.size(), request.ClusterConnections);
        }

        YQL_CLOG(DEBUG, FastMapReduce) << "Starting operation with id " << operationId;
        OperationPartitions_.erase(partitionId);
        return NThreading::MakeFuture(TStartOperationResponse(EOperationStatus::Accepted, operationId));
    }

    void SortedTasksPreprocess(TTaskParams& currentTaskParams) {
        if (auto* sortedMergeTaskParams = std::get_if<TSortedMergeTaskParams>(&currentTaskParams)) {
            if (sortedMergeTaskParams->Output.PartId.empty()) {
                TString newPartId = GenerateId();
                TString tableId = sortedMergeTaskParams->Output.TableId;
                sortedMergeTaskParams->Output.PartId = newPartId;
                PartIdsForTables_[tableId].emplace_back(newPartId);
            }
        }
    }

    NThreading::TFuture<TGetOperationResponse> GetOperation(const TGetOperationRequest& request) override {
        TGuard<TMutex> guard(Mutex_);
        auto operationId = request.OperationId;
        if (!Operations_.contains(operationId)) {
            return NThreading::MakeFuture(TGetOperationResponse(EOperationStatus::NotFound));
        }
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(Operations_[operationId].SessionId);
        UpdateSessionActivity(Operations_[operationId].SessionId);
        YQL_CLOG(TRACE, FastMapReduce) << "Getting operation status with id " << operationId;
        auto& operationInfo = Operations_[operationId];
        auto operationStatus =  operationInfo.OperationStatus;
        auto errorMessages = operationInfo.ErrorMessages;
        std::vector<TTableStats> outputTablesStats;
        std::vector<TString> result;
        if (operationStatus == EOperationStatus::Completed) {
            // Calculating output table stats only in case of successful completion of opereation
            for (auto& tableId : operationInfo.OutputTableIds) {
                outputTablesStats.emplace_back(CalculateTableStats(tableId));
            }
            result = GetOperationToProcessAfterResult(operationId);
        }
        return NThreading::MakeFuture(TGetOperationResponse(operationStatus, errorMessages, outputTablesStats, result));
    }

    NThreading::TFuture<TDeleteOperationResponse> DeleteOperation(const TDeleteOperationRequest& request) override {
        TGuard<TMutex> guard(Mutex_);
        auto operationId = request.OperationId;
        if (!Operations_.contains(operationId)) {
            return NThreading::MakeFuture(TDeleteOperationResponse(EOperationStatus::NotFound));
        }
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(Operations_[operationId].SessionId);
        UpdateSessionActivity(Operations_[operationId].SessionId);
        YQL_CLOG(DEBUG, FastMapReduce) << "Deleting operation with id " << operationId;
        auto taskIds = Operations_[operationId].TaskIds;
        for (auto& taskId: taskIds){
            YQL_ENSURE(Tasks_.contains(taskId));
            auto taskStatus = Tasks_[taskId].TaskStatus;
            if (taskStatus == ETaskStatus::InProgress) {
                TaskToDeleteIds_.insert(taskId); // Task is currently running, send signal to worker to cancel
            } else if (taskStatus == ETaskStatus::Accepted) {
                ClearTask(taskId); // Task hasn't begun running, remove info
            }
        }

        return NThreading::MakeFuture(TDeleteOperationResponse(EOperationStatus::Aborted));
    }

    NThreading::TFuture<TDropTablesResponse> DropTables(const TDropTablesRequest& request) override {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(request.SessionId);
        std::vector<TString> groupsToClear;

        with_lock(Mutex_) {
            UpdateSessionActivity(request.SessionId);
            std::vector<TString> tableIds = request.TableIds;

            YQL_ENSURE(!tableIds.empty(), "TableIds list is empty");

            for (const auto& tableId : tableIds) {
                if (!PartIdsForTables_.contains(tableId)) {
                    YQL_CLOG(TRACE, FastMapReduce) << "Table " << tableId  << " not found in PartIdsForTables_";
                    continue;
                }

                const auto& partIds = PartIdsForTables_[tableId];
                YQL_CLOG(TRACE, FastMapReduce) << "Dropping table " << tableId << " from TDS with " << partIds.size() << " partitions";

                for (const auto& partId : partIds) {
                    groupsToClear.emplace_back(GetTableDataServiceGroup(tableId, partId));
                }

                PartIdsForTables_.erase(tableId);
                OperationTableStats_.erase(tableId);
            }

            YQL_CLOG(TRACE, FastMapReduce) << "Dropping " << tableIds.size() << " FMR tables";
        }

        return GcService_->ClearGarbage(groupsToClear).Apply([](const auto& f) {
            f.GetValue();
            return NThreading::MakeFuture<TDropTablesResponse>();
        });
    }

    NThreading::TFuture<THeartbeatResponse> SendHeartbeatResponse(const THeartbeatRequest& request) override {
        YQL_LOG_CTX_ROOT_SCOPE("SendHeartbeatResponse");

        ui32 workerId = request.WorkerId;
        YQL_ENSURE(workerId >= 0 && workerId < WorkersNum_);

        with_lock(Mutex_) {
            if (!Workers_.contains(workerId)) {
                // first ever heartbeat from worker, initialize
                Workers_[workerId] = TWorkerInfo{
                    .VolatileId = request.VolatileId,
                    .LatestPing = TimeProvider_->Now(),
                    .NeedsToRestart = false
                };
            } else {
                auto& workerInfo = Workers_[workerId];
                workerInfo.LatestPing = TimeProvider_->Now();
                if (request.VolatileId != Workers_[workerId].VolatileId) {
                    // worker has restarted
                    YQL_ENSURE(workerInfo.NeedsToRestart = true);
                    YQL_ENSURE(request.TaskStates.empty());
                    workerInfo.NeedsToRestart = false; // Assume worker is alive again and can handle new tasks.
                    workerInfo.VolatileId = request.VolatileId;
                } else if (workerInfo.NeedsToRestart) {
                    // Worker has awoken after downtime, send signal to restart
                    return NThreading::MakeFuture(THeartbeatResponse{.NeedToRestart = true});
                }
            }
        }

        for (auto& requestTaskState: request.TaskStates) {
            bool isTaskToDelete = false;
            TString taskId;
            with_lock(Mutex_) {
                taskId = requestTaskState->TaskId;
                Workers_[request.WorkerId].TaskIds.emplace(taskId);
                YQL_ENSURE(Tasks_.contains(taskId));
                auto operationId = Tasks_[taskId].OperationId;
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(Operations_[operationId].SessionId);
                auto taskStatus = requestTaskState->TaskStatus;
                YQL_ENSURE(taskStatus != ETaskStatus::Accepted);
                if (taskStatus != ETaskStatus::InProgress) {
                    // TODO - refactor the whole function
                    Workers_[request.WorkerId].TaskIds.erase(taskId);
                    // Task finished in some status, removing info from worker
                }
                SetUnfinishedTaskStatus(taskId, taskStatus, requestTaskState->TaskErrorMessage);
                isTaskToDelete = (TaskToDeleteIds_.contains(taskId) && Tasks_[taskId].TaskStatus != ETaskStatus::InProgress);
                auto statistics = requestTaskState->Stats;
                YQL_CLOG(TRACE, FastMapReduce) << " Task with id " << taskId << " has current status " << taskStatus << Endl;
                bool isOperationCompleted = (GetOperationStatus(operationId) == EOperationStatus::Completed);
                for (auto& [fmrTableId, tableStats]: statistics.OutputTables) {
                    Operations_[operationId].OutputTableIds.emplace(fmrTableId.TableId);
                    PartIdStats_[fmrTableId.PartId] = tableStats.PartIdChunkStats;
                    if (isOperationCompleted) {
                        YQL_CLOG(INFO, FastMapReduce) << "Operation with id " << operationId << " has finished successfully";
                        CalculateTableStats(fmrTableId.TableId, true);
                    }
                    // TODO - проверка на валидность возвращаемой воркером статистики?
                }

                if (OperationsToProcessAfter_.contains(operationId) &&taskStatus == ETaskStatus::Completed) {
                    HandleOperationToProcessAfterTaskCompleted(operationId, requestTaskState);
                }
            }
            if (isTaskToDelete) {
                ClearTaskAndPartIds(taskId);
            }
        }

        std::vector<TTask::TPtr> currentTasksToRun;
        ui64 filledSlots = 0;

        while (filledSlots < request.AvailableSlots) {
            TTask::TPtr task;
            TString taskId;
            with_lock(Mutex_) {
                if (TasksToRun_.empty()) {
                    break;
                }
                auto currentTask = TasksToRun_.front();
                taskId = currentTask.second;
                task = currentTask.first;
                TasksToRun_.pop();
                if (!Tasks_.contains(taskId)) {
                    continue;
                }
                auto& taskInfo = Tasks_[taskId];
                YQL_ENSURE(taskInfo.TaskStatus == ETaskStatus::Accepted);
                SetUnfinishedTaskStatus(taskId, ETaskStatus::InProgress);
            }
            ClearPreviousPartIdsForTask(task);
            bool canRunTask = true;
            with_lock(Mutex_) {
                if (auto error = SetNewPartIdsForTask(task, taskId)) {
                    YQL_CLOG(ERROR, FastMapReduce) << error->ErrorMessage << ", taskId: " << taskId;
                    SetUnfinishedTaskStatus(taskId, ETaskStatus::Failed, *error);
                    canRunTask = false;
                }
            }
            if (canRunTask) {
                currentTasksToRun.emplace_back(task);
                ++filledSlots;
            }
        }

        auto heartbeatResponseFuture =  NThreading::MakeFuture(THeartbeatResponse{
            .TasksToRun = currentTasksToRun,
            .TaskToDeleteIds = TaskToDeleteIds_,
            .NeedToRestart = false
        });

        return heartbeatResponseFuture;
    }

    NThreading::TFuture<TGetFmrTableInfoResponse> GetFmrTableInfo(const TGetFmrTableInfoRequest& request) override {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(request.SessionId);
        TGuard<TMutex> guard(Mutex_);
        UpdateSessionActivity(request.SessionId);
        TGetFmrTableInfoResponse response;
        auto tableId = request.TableId;
        if (!PartIdsForTables_.contains(tableId)) {
            response.ErrorMessages = {TFmrError{
                .Component = EFmrComponent::Coordinator, .ErrorMessage = "Fmr table id " + tableId + " was not found"
            }};
            return NThreading::MakeFuture(response);
        }
        response.TableStats = CalculateTableStats(tableId);
        return NThreading::MakeFuture(response);
    }

    NThreading::TFuture<void> ClearSession(const TClearSessionRequest& request) override {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(request.SessionId);
        ClearSessionImpl(request.SessionId);
        return NThreading::MakeFuture();
    }

    NThreading::TFuture<TOpenSessionResponse> OpenSession(const TOpenSessionRequest& request) override {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(request.SessionId);

        // Failure injection point for testing session open failures
        TFailureInjector::Reach("coordinator.open_session", [] {
            ythrow yexception() << "Injected failure in OpenSession";
        });

        TString sessionId = request.SessionId;
        bool wasCreated = false;
        with_lock(Mutex_) {
            TInstant now = TimeProvider_->Now();
            auto [it, inserted] = Sessions_.try_emplace(sessionId);
            it->second.LastActivity = now;
            wasCreated = inserted;
        }
        if (wasCreated) {
            YQL_CLOG(INFO, FastMapReduce) << "Opening session " << sessionId;
        }
        return NThreading::MakeFuture(TOpenSessionResponse{});
    }

    NThreading::TFuture<TPingSessionResponse> PingSession(const TPingSessionRequest& request) override {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(request.SessionId);

        // Failure injection point for testing ping failures
        TFailureInjector::Reach("coordinator.ping_session", [] {
            ythrow yexception() << "Injected failure in PingSession";
        });

        TString sessionId = request.SessionId;
        bool found = false;
        with_lock(Mutex_) {
            if (Sessions_.contains(sessionId)) {
                UpdateSessionActivity(sessionId);
                found = true;
            }
        }
        if (!found) {
            YQL_CLOG(WARN, FastMapReduce) << "Session " << sessionId << " not found for ping";
            return NThreading::MakeFuture(TPingSessionResponse{.Success = false});
        }
        YQL_CLOG(TRACE, FastMapReduce) << "Pinged session " << sessionId;
        return NThreading::MakeFuture(TPingSessionResponse{.Success = true});
    }

    NThreading::TFuture<TListSessionsResponse> ListSessions(const TListSessionsRequest&) override {
        std::vector<TString> sessionIds;
        with_lock(Mutex_) {
            for (const auto& [sessionId, _] : Sessions_) {
                sessionIds.push_back(sessionId);
            }
        }
        return NThreading::MakeFuture(TListSessionsResponse{.SessionIds = sessionIds});
    }

    NThreading::TFuture<TPrepareOperationResponse> PrepareOperation(const TPrepareOperationRequest& request) override {
        TGuard<TMutex> guard(Mutex_);

        auto fmrOperationSpec = GetMergedFmrOperationSpec(request.FmrOperationSpec);

        try {
            auto partitionId = PartitionOperationIntoSeveralTasks(
                request.OperationParams,
                fmrOperationSpec,
                request.ClusterConnections
            );

            YQL_CLOG(DEBUG, FastMapReduce) << "Successfully prepared operation with partitionId=" << partitionId
                << ", tasksNum=" << OperationPartitions_[partitionId].TaskInputs.size();
            return NThreading::MakeFuture(TPrepareOperationResponse{.PartitionId = partitionId, .TasksNum = OperationPartitions_[partitionId].TaskInputs.size()});
        } catch (const std::exception& e) {
            YQL_CLOG(ERROR, FastMapReduce) << "Failed to prepare operation: " << e.what();
            return NThreading::MakeErrorFuture<TPrepareOperationResponse>(std::current_exception());
        }
    }

    std::vector<TString> GetOperationToProcessAfterResult(TString operationId) {
        std::vector<TString> result;

        if (!OperationsToProcessAfter_.contains(operationId)) {
            return result;
        }
        auto& distributedOpInfo = OperationsToProcessAfter_[operationId];

        if (auto* uploadMeta = std::get_if<TSortedUploadOperatonMeta>(&distributedOpInfo)) {
            result = std::move(uploadMeta->FragmentResultsYson);
            OperationsToProcessAfter_.erase(operationId);
        }
        return result;
    }

private:
    void StartClearingIdempotencyKeys() {
        auto ClearIdempotencyKeysFunc = [&] () {
            while (!StopCoordinator_) {
                with_lock(Mutex_) {
                    auto currentTime = TimeProvider_->Now();
                    for (auto it = IdempotencyKeys_.begin(); it != IdempotencyKeys_.end();) {
                        auto operationCreationTime = it->second.OperationCreationTime;
                        auto operationId = it->second.OperationId;
                        if (currentTime - operationCreationTime > IdempotencyKeyStoreTime_) {
                            it = IdempotencyKeys_.erase(it);
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

    void CheckWorkersAliveStatus() {
        auto checkWorkersAliveStatusFunc = [&] () {
            while (!StopCoordinator_) {
                with_lock(Mutex_) {
                    auto currentTime = TimeProvider_->Now();
                    for (auto& [workerId, workerInfo]: Workers_) {
                        auto currentWorkerLatestPing = workerInfo.LatestPing;
                        if (currentTime > currentWorkerLatestPing + WorkerDeadlineLease_) {
                            // assuming worker is dead and should restart, cancelling tasks and rescheduling
                            if (!workerInfo.NeedsToRestart) {
                                YQL_CLOG(INFO, FastMapReduce) << "Worker with id " << workerId << " is assumed dead, sending restart request to coordinator";
                            }
                            workerInfo.NeedsToRestart = true;
                            for (auto& taskId: workerInfo.TaskIds) {
                                // resetting task, TODO - add max retry
                                SetUnfinishedTaskStatus(taskId, ETaskStatus::Accepted);
                                YQL_ENSURE(Tasks_.contains(taskId));
                                TasksToRun_.emplace(Tasks_[taskId].Task, taskId);
                            }
                            workerInfo.TaskIds.clear();
                        }
                    }
                }
                Sleep(TimeToSleepBetweenCheckWorkerStatusRequests_);
            }
        };
        CheckWorkersAliveStatusThread_ = std::thread(checkWorkersAliveStatusFunc);
    }

    void CheckGatewaySessionsActivity() {
        YQL_LOG_CTX_ROOT_SCOPE("CheckGatewaySessionsActivityThread");
        auto checkFunc = [&] () {
            while (!StopCoordinator_) {
                TInstant now = TimeProvider_->Now();
                std::vector<TString> sessionsToCleanup;
                with_lock(Mutex_) {
                    for (const auto& [sessionId, sessionInfo] : Sessions_) {
                        TDuration inactivityDuration = now - sessionInfo.LastActivity;
                        if (inactivityDuration >= SessionInactivityTimeout_) {
                            YQL_CLOG(WARN, FastMapReduce)
                                << "Session " << sessionId
                                << " has been inactive for " << inactivityDuration
                                << ", scheduling cleanup (timeout: " << SessionInactivityTimeout_ << ")";
                            sessionsToCleanup.push_back(sessionId);
                        }
                    }
                }
                for (const auto& sessionId : sessionsToCleanup) {
                    YQL_CLOG(INFO, FastMapReduce) << "Cleaning up inactive session " << sessionId;
                    ClearSessionImpl(sessionId);
                }
                Sleep(HealthCheckInterval_);
            }
        };
        CheckGatewaySessionsActivityThread_ = std::thread(checkFunc);
    }

    void UpdateSessionActivity(const TString& sessionId) {
        if (Sessions_.contains(sessionId)) {
            Sessions_[sessionId].LastActivity = TimeProvider_->Now();
        }
    }

    void ClearSessionImpl(const TString& sessionId) {
        YQL_CLOG(INFO, FastMapReduce) << "Clearing fmr tables and coordinator state for session Id " << sessionId;
        std::vector<TString> tasks;
        with_lock(Mutex_) {
            if (!Sessions_.contains(sessionId)) {
                return;
            }
            auto& sessionInfo = Sessions_[sessionId];
            for (auto& operationId: sessionInfo.OperationIds) {
                auto& operationInfo = Operations_[operationId];
                std::unordered_set<TString> taskIdsToClear = operationInfo.TaskIds;
                for (auto& taskId: taskIdsToClear) {
                    tasks.push_back(taskId);
                }
            }
            Sessions_.erase(sessionId);
        }

        for (auto& taskId: tasks) {
            ClearTaskAndPartIds(taskId);
        }
    }

    TString GenerateId() {
        return GetGuidAsString(RandomProvider_->GenGuid());
    }

    void ClearTask(const TString& taskId) {
        YQL_ENSURE(Tasks_.contains(taskId));
        auto& taskInfo = Tasks_[taskId];
        TaskToDeleteIds_.erase(taskId);

        YQL_ENSURE(Operations_.contains(taskInfo.OperationId));
        auto& currentTaskIdsForOperation = Operations_[taskInfo.OperationId];
        currentTaskIdsForOperation.TaskIds.erase(taskId);
        if (currentTaskIdsForOperation.TaskIds.empty()) {
            // All task for operation are cleared, can clear it
            Operations_.erase(taskInfo.OperationId);
        }
        Tasks_.erase(taskId);
    }

    void ClearTaskAndPartIds(const TString& taskId) {
        TTask::TPtr task;
        with_lock(Mutex_) {
            task = Tasks_[taskId].Task;
        }
        ClearPreviousPartIdsForTask(task);
        with_lock(Mutex_) {
            ClearTask(taskId);
        }
    }

    void SetUnfinishedTaskStatus(const TString& taskId, ETaskStatus newTaskStatus, const TMaybe<TFmrError>& taskErrorMessage = Nothing()) {
        auto& taskInfo = Tasks_[taskId];
        YQL_ENSURE(Operations_.contains(taskInfo.OperationId));
        auto& operationInfo = Operations_[taskInfo.OperationId];
        if (taskInfo.TaskStatus != ETaskStatus::Accepted && taskInfo.TaskStatus != ETaskStatus::InProgress) {
            return;
        }
        YQL_CLOG(TRACE, FastMapReduce) << "Setting task status for task id" << taskId << " from " << taskInfo.TaskStatus << " to new Task status " << newTaskStatus;
        taskInfo.TaskStatus = newTaskStatus;
        operationInfo.OperationStatus = GetOperationStatus(taskInfo.OperationId);
        if (taskErrorMessage) {
            auto& errorMessages = operationInfo.ErrorMessages;
            errorMessages.emplace_back(*taskErrorMessage);
        }
    }

    EOperationStatus GetOperationStatus(const TString& operationId) {
        if (!Operations_.contains(operationId)) {
            return EOperationStatus::NotFound;
        }
        std::unordered_set<TString> taskIds = Operations_[operationId].TaskIds;
        std::unordered_set<ETaskStatus> taskStatuses;

        for (auto& taskId: taskIds) {
            taskStatuses.emplace(Tasks_[taskId].TaskStatus);
        }
        YQL_ENSURE(!taskStatuses.contains(ETaskStatus::Unknown));

        if (taskStatuses.contains(ETaskStatus::Failed)) {
            return EOperationStatus::Failed;
        }
        if (taskStatuses.contains(ETaskStatus::InProgress)) {
            return EOperationStatus::InProgress;
        }
        if (taskStatuses.contains(ETaskStatus::InProgress)) {
            return EOperationStatus::InProgress;
        }
        if (taskStatuses.contains(ETaskStatus::Accepted)) {
            return EOperationStatus::Accepted;
        }
        return EOperationStatus::Completed;
    }

    TFmrPartitionerSettings GetFmrPartitionerSettings(const NYT::TNode& fmrOperationSpec) {
        TFmrPartitionerSettings settings;
        auto& fmrPartitionSettings = fmrOperationSpec["partition"]["fmr_table"];
        settings.MaxDataWeightPerPart = fmrPartitionSettings["max_data_weight_per_part"].AsInt64();
        settings.MaxParts = fmrPartitionSettings["max_parts"].AsInt64();
        return settings;
    }

    TYtPartitionerSettings GetYtPartitionerSettings(const NYT::TNode& fmrOperationSpec) {
        TYtPartitionerSettings settings;
        auto& ytPartitionSettings = fmrOperationSpec["partition"]["yt_table"];
        settings.MaxDataWeightPerPart = ytPartitionSettings["max_data_weight_per_part"].AsInt64();
        settings.MaxParts = ytPartitionSettings["max_parts"].AsInt64();
        return settings;
    }

    TOrderedPartitionSettings GetOrderedPartitionerSettings(const NYT::TNode& fmrOperationSpec) {
        TOrderedPartitionSettings settings;
        settings.FmrPartitionSettings = GetFmrPartitionerSettings(fmrOperationSpec);
        settings.YtPartitionSettings = GetYtPartitionerSettings(fmrOperationSpec);
        settings.YtPartitionSettings.PartitionMode = NYT::ETablePartitionMode::Ordered;
        return settings;
    }

    TSortedPartitionSettings GetSortedPartitionerSettings(const NYT::TNode& fmrOperationSpec) {
        TSortedPartitionSettings settings;
        settings.FmrPartitionSettings = GetFmrPartitionerSettings(fmrOperationSpec);
        return settings;
    }

    EPartitionType CheckPartitionType(const TOperationParams& operationParams) {
        if (auto mapOptions = std::get_if<TMapOperationParams>(&operationParams)) {
            if (mapOptions->IsOrdered) {
                return EPartitionType::OrderedPartition;
            } else {
                return EPartitionType::UnorderedPartition;
            }
        } else if (std::holds_alternative<TSortedUploadOperationParams>(operationParams)) {
            return EPartitionType::OrderedPartition;
        } else if (std::holds_alternative<TSortedMergeOperationParams>(operationParams)) {
            return EPartitionType::SortedPartition;
        }
        return EPartitionType::UnorderedPartition;
    }

    TSortingColumns GetSortingColumns(const TOperationParams& operationParams) {
        TSortingColumns sortingColumns;
        if (auto sortedMerge = std::get_if<TSortedMergeOperationParams>(&operationParams)) {
            auto columns = sortedMerge->Output.SortColumns;
            auto sortOrder = sortedMerge->Output.SortOrder;
            sortingColumns.Columns = std::move(columns);
            sortingColumns.SortOrders = std::move(sortOrder);
        }
        return sortingColumns;
    }

    TString PartitionOperationIntoSeveralTasks(const TOperationParams& operationParams, const NYT::TNode& fmrOperationSpec, const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections) {
        EPartitionType partitionType = CheckPartitionType(operationParams);
        auto ytPartitionerSettings = GetYtPartitionerSettings(fmrOperationSpec);
        auto partitionId = GenerateId();
        YQL_CLOG(TRACE, FastMapReduce)
        << "PartitionOperationIntoSeveralTasks: partitionId=" << partitionId
        << ", isOrdered=" << (partitionType == EPartitionType::OrderedPartition)
        << ", opIndex=" << operationParams.index();

        TPartitionResult partitionResult;
        switch (partitionType) {
            case EPartitionType::OrderedPartition: {
                auto orderedPartitionerSettings = GetOrderedPartitionerSettings(fmrOperationSpec);
                auto orderedPartitioner = TOrderedPartitioner(PartIdsForTables_, PartIdStats_, orderedPartitionerSettings);

                TOperationInputTablesGetter tablesGetter{};
                std::visit(tablesGetter, operationParams);
                auto& inputTables = tablesGetter.OperationTableRef;
                partitionResult = PartitionInputTablesIntoTasksOrdered(inputTables, orderedPartitioner, YtCoordinatorService_, clusterConnections);
                break;
            } case EPartitionType::UnorderedPartition: {
                auto fmrPartitionerSettings = GetFmrPartitionerSettings(fmrOperationSpec);
                auto fmrPartitioner = TFmrPartitioner(PartIdsForTables_,PartIdStats_, fmrPartitionerSettings);
                std::vector<TYtTableRef> ytInputTables;
                std::vector<TFmrTableRef> fmrInputTables;
                GetOperationInputTables(ytInputTables, fmrInputTables, operationParams);
                partitionResult = PartitionInputTablesIntoTasks(ytInputTables, fmrInputTables, fmrPartitioner, YtCoordinatorService_, clusterConnections, ytPartitionerSettings);
                break;
            } case EPartitionType::SortedPartition: {
                auto sortedPartitionerSettings = GetSortedPartitionerSettings(fmrOperationSpec);
                TSortingColumns sortedColumns = GetSortingColumns(operationParams);
                auto sortedPartitioner = TSortedPartitioner(PartIdsForTables_, PartIdStats_, sortedColumns, sortedPartitionerSettings);

                TOperationInputTablesGetter tablesGetter{};
                std::visit(tablesGetter, operationParams);
                auto& inputTables = tablesGetter.OperationTableRef;
                partitionResult = PartitionInputTablesIntoTasksSorted(inputTables, sortedPartitioner);
                break;
            } default: {
                ythrow yexception() << "Unknown partition type";
            }
        }
        if (!partitionResult.PartitionStatus) {
            ythrow yexception() << "Failed to partition input tables into tasks";
            // TODO - return FAILED_PARTITIONING status instead.
        }
        OperationPartitions_[partitionId] = partitionResult;
        return partitionId;
    }

    std::vector<TFmrResourceTaskInfo> PartitionFmrResourcesIntoTasks(const std::vector<TFmrResourceOperationInfo>& fmrResources, const NYT::TNode& fmrOperationSpec) {
        // need to split fmrResources into tasks and pass to JobPreparer, for simpliclty split each fmr table separately.
        std::vector<TFmrResourceTaskInfo> fmrResourceTasks;

        auto fmrPartitionerSettings = GetFmrPartitionerSettings(fmrOperationSpec);
        auto fmrPartitioner = TFmrPartitioner(PartIdsForTables_,PartIdStats_, fmrPartitionerSettings);
        for (auto& fmrResource: fmrResources) {
            TFmrResourceTaskInfo curFmrResourceTaskInfo;
            auto [partition, partitionSuccess] = fmrPartitioner.PartitionFmrTablesIntoTasks({fmrResource.FmrTable});
            if (!partitionSuccess) {
                throw yexception() << "Failed to partition fmrResources into tasks";
            }

            for (auto& partitionTable: partition) {
                YQL_ENSURE(partitionTable.Inputs.size() == 1);
                TFmrTableInputRef fmrTableInputRef = std::get<TFmrTableInputRef>(partitionTable.Inputs[0]);
                curFmrResourceTaskInfo.FmrResourceTasks.emplace_back(fmrTableInputRef);
            }

            curFmrResourceTaskInfo.Alias = fmrResource.Alias;
            fmrResourceTasks.emplace_back(curFmrResourceTaskInfo);
        }
        return fmrResourceTasks;
    }

    void GetOperationInputTables(std::vector<TYtTableRef>& ytInputTables, std::vector<TFmrTableRef>& fmrInputTables, const TOperationParams& operationParams) {
        TOperationInputTablesGetter tablesGetter{};
        std::visit(tablesGetter, operationParams);

        auto& inputTables = tablesGetter.OperationTableRef;
        for (auto& table: inputTables) {
            auto ytTable = std::get_if<TYtTableRef>(&table);
            auto fmrTable = std::get_if<TFmrTableRef>(&table);
            if (ytTable) {
                ytInputTables.emplace_back(*ytTable);
            } else {
                fmrInputTables.emplace_back(*fmrTable);
            }
        }
    }

    TOperationPartitions GetOutputTaskParams(const TPartitionResult& partitionResult, const TOperationParams& operationParams) {
        TOutputTaskParamsGetter taskGetter{.PartitionResult = partitionResult};
        std::visit(taskGetter, operationParams);
        return taskGetter.TaskParams;
    }

    NYT::TNode GetMergedFmrOperationSpec(const TMaybe<NYT::TNode>& currentFmrOperationSpec) {
        // just pass whole merged operation spec for simplicity here
        if (!currentFmrOperationSpec) {
            return DefaultFmrOperationSpec_;
        }
        auto resultFmrOperationSpec = DefaultFmrOperationSpec_;
        NYT::MergeNodes(resultFmrOperationSpec, *currentFmrOperationSpec);
        return resultFmrOperationSpec;
    }

    TMaybe<TFmrError> SetNewPartIdsForTask(TTask::TPtr task, const TString& taskId) {
        // TODO - remove code duplication
        TString newPartId = GenerateId();

        auto* downloadTaskParams = std::get_if<TDownloadTaskParams>(&task->TaskParams);
        auto* mergeTaskParams = std::get_if<TMergeTaskParams>(&task->TaskParams);
        auto* sortedMergeTaskParams = std::get_if<TSortedMergeTaskParams>(&task->TaskParams);
        auto* mapTaskParams = std::get_if<TMapTaskParams>(&task->TaskParams);
        if (downloadTaskParams) {
            TString tableId = downloadTaskParams->Output.TableId;
            downloadTaskParams->Output.PartId = newPartId;
            PartIdsForTables_[tableId].emplace_back(newPartId);
        } else if (mergeTaskParams) {
            TString tableId = mergeTaskParams->Output.TableId;
            mergeTaskParams->Output.PartId = newPartId;
            PartIdsForTables_[tableId].emplace_back(newPartId);
        } else if (sortedMergeTaskParams) {
            if (sortedMergeTaskParams->Output.PartId.empty()) {
                return TFmrError{
                    .Component = EFmrComponent::Coordinator,
                    .Reason = EFmrErrorReason::RestartQuery,
                    .ErrorMessage = "SortedMerge task has empty output PartId, fallback to native gateway is required",
                    .TaskId = taskId,
                    .OperationId = Tasks_[taskId].OperationId
                };
            }
            TString tableId = sortedMergeTaskParams->Output.TableId;
            TString partId = sortedMergeTaskParams->Output.PartId;
            auto& partIds = PartIdsForTables_[tableId];
            if (std::find(partIds.begin(), partIds.end(), partId) == partIds.end()) {
                return TFmrError{
                    .Component = EFmrComponent::Coordinator,
                    .Reason = EFmrErrorReason::RestartQuery,
                    .ErrorMessage = "SortedMerge task output PartId is missing in coordinator part list, fallback to native gateway is required",
                    .TaskId = taskId,
                    .OperationId = Tasks_[taskId].OperationId
                };
            }
        } else if (mapTaskParams) {
            for (auto& fmrTableOutputRef: mapTaskParams->Output) {
                TString tableId = fmrTableOutputRef.TableId;
                fmrTableOutputRef.PartId = newPartId;
                PartIdsForTables_[tableId].emplace_back(newPartId);
            }
        }
        return Nothing();
    }

    std::vector<TPartIdInfo> CollectPreviousPartIdsForTask(TTask::TPtr task) {
        // TODO - remove code duplication, templates?
        std::vector<TPartIdInfo> groupsToClear; // (TableId, PartId)

        auto* downloadTaskParams = std::get_if<TDownloadTaskParams>(&task->TaskParams);
        auto* mergeTaskParams = std::get_if<TMergeTaskParams>(&task->TaskParams);
        auto* sortedMergeTaskParams = std::get_if<TSortedMergeTaskParams>(&task->TaskParams);
        auto* mapTaskParams = std::get_if<TMapTaskParams>(&task->TaskParams);

        if (downloadTaskParams) {
            TString tableId = downloadTaskParams->Output.TableId;
            if (!downloadTaskParams->Output.PartId.empty() && PartIdStats_.contains(downloadTaskParams->Output.PartId)) {
                auto prevPartId = downloadTaskParams->Output.PartId;
                groupsToClear.emplace_back(tableId, prevPartId);
            }
        } else if (mergeTaskParams) {
            TString tableId = mergeTaskParams->Output.TableId;
            if (!mergeTaskParams->Output.PartId.empty() && PartIdStats_.contains(mergeTaskParams->Output.PartId)) {
                auto prevPartId = mergeTaskParams->Output.PartId;
                groupsToClear.emplace_back(tableId, prevPartId);
            }
        } else if (sortedMergeTaskParams) {
            TString tableId = sortedMergeTaskParams->Output.TableId;
            if (!sortedMergeTaskParams->Output.PartId.empty() && PartIdStats_.contains(sortedMergeTaskParams->Output.PartId)) {
                auto prevPartId = sortedMergeTaskParams->Output.PartId;
                groupsToClear.emplace_back(tableId, prevPartId);
            }
        } else if (mapTaskParams) {
            for (auto& fmrTableOutputRef: mapTaskParams->Output) {
                TString tableId = fmrTableOutputRef.TableId;
                if (!fmrTableOutputRef.PartId.empty() && PartIdStats_.contains(fmrTableOutputRef.PartId)) {
                    auto prevPartId = fmrTableOutputRef.PartId;
                    groupsToClear.emplace_back(tableId, prevPartId);
                }
            }
        }
        return groupsToClear;
    }

    std::vector<TString> GetTableGroupsToClear(const std::vector<TPartIdInfo>& groupsToClear) {
        std::vector<TString> tableGroupsToClear;
        for (const auto& group : groupsToClear) {
            tableGroupsToClear.push_back(GetTableDataServiceGroup(group.TableId, group.PartId));
        }
        return tableGroupsToClear;
    }

    std::unordered_set<TString> GetPartIdsToKeepForTask(TTask::TPtr task) {
        std::unordered_set<TString> partIdsToKeep; // encoded as table data service group id
        if (auto* sortedMergeTaskParams = std::get_if<TSortedMergeTaskParams>(&task->TaskParams)) {
            if (!sortedMergeTaskParams->Output.PartId.empty()) {
                partIdsToKeep.emplace(GetTableDataServiceGroup(sortedMergeTaskParams->Output.TableId, sortedMergeTaskParams->Output.PartId));
            }
        }
        return partIdsToKeep;
    }

    void ClearPreviousPartIdsForTask(TTask::TPtr task) {
        std::vector<TString> tableGroupsToClear;
        std::vector<TPartIdInfo> groupsToClear;
        auto partIdsToKeep = GetPartIdsToKeepForTask(task);
        with_lock(Mutex_) {
            groupsToClear = CollectPreviousPartIdsForTask(task);
            tableGroupsToClear = GetTableGroupsToClear(groupsToClear);
        }

        GcService_->ClearGarbage(tableGroupsToClear).GetValueSync();

        with_lock(Mutex_) {
            for (const auto& group : groupsToClear) {
                if (partIdsToKeep.contains(GetTableDataServiceGroup(group.TableId, group.PartId))) {
                    continue;
                }
                auto tableIt = PartIdsForTables_.find(group.TableId);
                if (tableIt != PartIdsForTables_.end()) {
                    auto& partIds = tableIt->second;
                    auto partIdPosition = std::find(partIds.begin(), partIds.end(), group.PartId);
                    if (partIdPosition != partIds.end()) {
                        partIds.erase(partIdPosition);
                    }
                    if (partIds.empty()) {
                        PartIdsForTables_.erase(tableIt);
                    }
                }
            }
        }
    }

    TTableStats CalculateTableStats(const TString& tableId, bool isOperationFinished = false) {
        if (OperationTableStats_.contains(tableId)) {
            return OperationTableStats_[tableId];
        }
        TTableStats tableStats{};
        auto& partIds = PartIdsForTables_.at(tableId);
        YQL_CLOG(DEBUG, FastMapReduce) << "Calculating table stats for table with id " << tableId << " with " << partIds.size() << " part ids";
        for (auto& part: partIds) {
            auto& partStats = PartIdStats_[part];
            tableStats.Chunks += partStats.size();
            YQL_CLOG(DEBUG, FastMapReduce) << " Gotten " << partStats.size() << " chunks for part id " << part;
            for (auto& chunkStats: PartIdStats_[part]) {
                tableStats.DataWeight += chunkStats.DataWeight;
                tableStats.Rows += chunkStats.Rows;
            }
        }
        if (isOperationFinished) {
            // Stats for table won't change, inserting into map for caching
            OperationTableStats_[tableId] = tableStats;
        }
        return tableStats;
    }

    void InitializeSortedUploadOperation(
        const TString& operationId,
        ui64 tasksNum,
        const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections
    ) {
        YQL_ENSURE(clusterConnections.size() == 1, "SortedUpload should have exactly one cluster connection");

        TSortedUploadOperatonMeta uploadMeta{
            .FragmentResultsYson = std::vector<TString>(tasksNum)
        };

        OperationsToProcessAfter_[operationId] = uploadMeta;

        YQL_CLOG(DEBUG, FastMapReduce) << "Initialized SortedUpload operation " << operationId
            << " with " << tasksNum << " tasks";
    }

    void HandleOperationToProcessAfterTaskCompleted(const TString& operationId, TTaskState::TPtr taskState) {
        if (!OperationsToProcessAfter_.contains(operationId)) {
            return;
        }

        auto& distributedOpInfo = OperationsToProcessAfter_[operationId];

        if (auto* taskSortedUploadResult = std::get_if<TTaskSortedUploadResult>(&taskState->Stats.TaskResult)) {
            if (auto* uploadMeta = std::get_if<TSortedUploadOperatonMeta>(&distributedOpInfo)) {
                uploadMeta->FragmentResultsYson[taskSortedUploadResult->FragmentOrder] = taskSortedUploadResult->FragmentResultYson;
            }
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////

    struct TCoordinatorTaskInfo {
        TTask::TPtr Task;
        ETaskStatus TaskStatus;
        TString OperationId;
        ui64 NumRetries;
    };


    struct TSortedUploadOperatonMeta {
        std::vector<TString> FragmentResultsYson;  // order -> YSON-serialized fragment result
    };

    using TOperationsToProcessAfterMeta = std::variant<TSortedUploadOperatonMeta>;

    struct TOperationInfo {
        std::unordered_set<TString> TaskIds;
        EOperationStatus OperationStatus;
        std::vector<TFmrError> ErrorMessages;
        TString SessionId;
        std::unordered_set<TString> OutputTableIds = {};
    };

    struct TIdempotencyKeyInfo {
        TString OperationId;
        TInstant OperationCreationTime;
    };

    struct TWorkerInfo {
        TString VolatileId;
        TInstant LatestPing;
        std::unordered_set<TString> TaskIds;
        bool NeedsToRestart = false;
    };

    struct TSessionInfo {
        std::vector<TString> OperationIds;  // List of Operation Ids
        TInstant LastActivity;
    };

    std::unordered_map<TString, TCoordinatorTaskInfo> Tasks_; // TaskId -> current info about it
    std::queue<std::pair<TTask::TPtr, TString>> TasksToRun_; // Task, and TaskId
    std::unordered_set<TString> TaskToDeleteIds_; // TaskIds we want to pass to worker for deletion
    std::unordered_map<TString, TOperationInfo> Operations_; // OperationId -> current info about it
    std::unordered_map<TString, TOperationsToProcessAfterMeta> OperationsToProcessAfter_; // OperationId -> distributed operation tracking
    std::unordered_map<TString, TIdempotencyKeyInfo> IdempotencyKeys_; // IdempotencyKey -> current info about it
    std::unordered_map<TString, TSessionInfo> Sessions_; // SessionId -> Session info (operations and activity)
    std::unordered_map<TString, TPartitionResult> OperationPartitions_; // PartitionId -> TaskParamsPartition
    std::unordered_map<ui64, TWorkerInfo> Workers_; // WorkerId -> Info About It

    TMutex Mutex_;
    const ui32 WorkersNum_;
    std::unordered_map<ui32, TString> WorkerToVolatileId_; // worker id -> volatile id  // TODO - убрать это
    const TIntrusivePtr<IRandomProvider> RandomProvider_;
    const TIntrusivePtr<ITimeProvider> TimeProvider_;
    std::thread ClearIdempotencyKeysThread_, CheckWorkersAliveStatusThread_, CheckGatewaySessionsActivityThread_;
    std::atomic<bool> StopCoordinator_;
    TDuration TimeToSleepBetweenClearKeyRequests_;
    TDuration IdempotencyKeyStoreTime_;
    TDuration WorkerDeadlineLease_;
    TDuration TimeToSleepBetweenCheckWorkerStatusRequests_;
    TDuration SessionInactivityTimeout_;
    TDuration HealthCheckInterval_;

    std::unordered_map<TFmrTableId, std::vector<TString>> PartIdsForTables_; // TableId -> List of all corresponding partIds
    std::unordered_map<TString, std::vector<TChunkStats>> PartIdStats_; // PartId -> Detailed statistic for each chunk
    std::unordered_map<TString, TTableStats> OperationTableStats_; // TableId -> Statistic for fmr table, filled when operation completes

    NYT::TNode DefaultFmrOperationSpec_;
    IYtCoordinatorService::TPtr YtCoordinatorService_; // Needed for partitioning of yt tables
    IFmrGcService::TPtr GcService_;

    //////////////////////////////////////////////////////////////////////////////////////////////////////////

    struct TOperationInputTablesGetter {
        std::vector<TOperationTableRef> OperationTableRef; // will be filled when std::visit is called

        void operator()(const TUploadOperationParams& uploadOperationParams) {
            OperationTableRef.emplace_back(uploadOperationParams.Input);
        }
        void operator()(const TDownloadOperationParams& downloadOperationParams) {
            OperationTableRef.emplace_back(downloadOperationParams.Input);
        }
        void operator()(const TMergeOperationParams& mergeOperationParams) {
            OperationTableRef = mergeOperationParams.Input;
        }
        void operator()(const TSortedMergeOperationParams& sortedMergeOperationParams) {
            OperationTableRef = sortedMergeOperationParams.Input;
        }
        void operator()(const TMapOperationParams& mapOperationParams) {
            OperationTableRef = mapOperationParams.Input;
        }
        void operator()(const TSortedUploadOperationParams& SortedUploadOperationParams) {
            OperationTableRef.emplace_back(SortedUploadOperationParams.Input);
        }
    };

    struct TOutputTaskParamsGetter {
        std::vector<TTaskParams> TaskParams; // Will be filled when std::visit is called
        TPartitionResult PartitionResult;

        void operator()(const TUploadOperationParams& uploadOperationParams) {
            for (auto& task: PartitionResult.TaskInputs) {
                TUploadTaskParams uploadTaskParams;
                YQL_ENSURE(task.Inputs.size() == 1, "Upload task should have exactly one fmr table partition input");
                auto& fmrTablePart = task.Inputs[0];
                uploadTaskParams.Input = std::get<TFmrTableInputRef>(fmrTablePart);
                uploadTaskParams.Output = uploadOperationParams.Output;
                TaskParams.emplace_back(uploadTaskParams);
            }
        }
        void operator()(const TSortedUploadOperationParams& SortedUploadOperationParams) {
            ui64 taskOrder = 0;
            for (auto& task: PartitionResult.TaskInputs) {
                TSortedUploadTaskParams SortedUploadTaskParams;
                YQL_ENSURE(task.Inputs.size() == 1, "Distributed upload task should have exactly one fmr table partition input");
                auto& fmrTablePart = task.Inputs[0];
                SortedUploadTaskParams.Input = std::get<TFmrTableInputRef>(fmrTablePart);
                SortedUploadTaskParams.Output = SortedUploadOperationParams.Output;
                SortedUploadTaskParams.CookieYson = SortedUploadOperationParams.Cookies[taskOrder];
                SortedUploadTaskParams.Order = taskOrder;
                TaskParams.emplace_back(SortedUploadTaskParams);
                taskOrder++;
            }
        }
        void operator()(const TDownloadOperationParams& downloadOperationParams) {
            for (auto& task: PartitionResult.TaskInputs) {
                TDownloadTaskParams downloadTaskParams;
                YQL_ENSURE(task.Inputs.size() == 1, "Download task should have exactly one yt table partition input");
                auto& ytTablePart = task.Inputs[0];
                downloadTaskParams.Input = std::get<TYtTableTaskRef>(ytTablePart);
                downloadTaskParams.Output = TFmrTableOutputRef(downloadOperationParams.Output);
                // PartId for tasks which write to table data service will be set later
                TaskParams.emplace_back(downloadTaskParams);
            }
        }
        void operator()(const TMergeOperationParams& mergeOperationParams) {
            for (auto& task: PartitionResult.TaskInputs) {
                TMergeTaskParams mergeTaskParams;
                mergeTaskParams.Input = task;
                mergeTaskParams.Output = TFmrTableOutputRef(mergeOperationParams.Output);
                TaskParams.emplace_back(mergeTaskParams);
            }
        }
        void operator()(const TSortedMergeOperationParams& sortedMergeOperationParams) {
            for (auto& task: PartitionResult.TaskInputs) {
                TSortedMergeTaskParams sortedMergeTaskParams;
                sortedMergeTaskParams.Input = task;
                sortedMergeTaskParams.Output = TFmrTableOutputRef(sortedMergeOperationParams.Output);
                TaskParams.emplace_back(sortedMergeTaskParams);
            }
        }
        void operator()(const TMapOperationParams& mapOperationParams) {
            for (auto& task: PartitionResult.TaskInputs) {
                TMapTaskParams mapTaskParams;
                mapTaskParams.Input = task;
                std::vector<TFmrTableOutputRef> fmrTableOutputRefs;
                std::transform(mapOperationParams.Output.begin(), mapOperationParams.Output.end(), std::back_inserter(fmrTableOutputRefs), [] (const TFmrTableRef& fmrTableRef) {
                    return TFmrTableOutputRef(fmrTableRef);
                });

                mapTaskParams.Output = fmrTableOutputRefs;
                mapTaskParams.SerializedMapJobState = mapOperationParams.SerializedMapJobState;
                mapTaskParams.IsOrdered = mapOperationParams.IsOrdered;
                TaskParams.emplace_back(mapTaskParams);
            }
        }
    };
};

} // namespace

IFmrCoordinator::TPtr MakeFmrCoordinator(
    const TFmrCoordinatorSettings& settings,
    IYtCoordinatorService::TPtr ytCoordinatorService,
    IFmrGcService::TPtr gcService
) {
    return MakeIntrusive<TFmrCoordinator>(settings, ytCoordinatorService, gcService);
}

} // namespace NYql::NFmr

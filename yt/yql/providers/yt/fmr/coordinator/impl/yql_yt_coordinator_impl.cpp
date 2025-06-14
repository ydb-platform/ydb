#include <thread>
#include <library/cpp/resource/resource.h>
#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_partitioner.h>
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

class TFmrCoordinator: public IFmrCoordinator {
public:
    TFmrCoordinator(const TFmrCoordinatorSettings& settings, IYtCoordinatorService::TPtr ytCoordinatorService)
        : WorkersNum_(settings.WorkersNum),
        RandomProvider_(settings.RandomProvider),
        StopCoordinator_(false),
        TimeToSleepBetweenClearKeyRequests_(settings.TimeToSleepBetweenClearKeyRequests),
        IdempotencyKeyStoreTime_(settings.IdempotencyKeyStoreTime),
        DefaultFmrOperationSpec_(settings.DefaultFmrOperationSpec),
        YtCoordinatorService_(ytCoordinatorService)
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

        auto fmrOperationSpec = GetMergedFmrOperationSpec(request.FmrOperationSpec);
        auto taskParams = PartitionOperationIntoSeveralTasks(request.OperationParams, fmrOperationSpec, request.ClusterConnections);

        std::unordered_set<TString> taskIds;

        for (auto& currentTaskParams: taskParams) {
            TString taskId = GenerateId();
            TTask::TPtr createdTask = MakeTask(request.TaskType, taskId, currentTaskParams, request.SessionId, request.ClusterConnections, fmrOperationSpec);
            Tasks_[taskId] = TCoordinatorTaskInfo{.Task = createdTask, .TaskStatus = ETaskStatus::Accepted, .OperationId = operationId};
            TasksToRun_.emplace(createdTask, taskId);
            taskIds.emplace(taskId);
        }

        Operations_[operationId] = {.TaskIds = taskIds, .OperationStatus = EOperationStatus::Accepted, .SessionId = request.SessionId};
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
        if (operationStatus == EOperationStatus::Completed) {
            // Calculating output table stats only in case of successful completion of opereation
            for (auto& tableId : operationInfo.OutputTableIds) {
                outputTablesStats.emplace_back(CalculateTableStats(tableId));
            }
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
        for (auto& taskId: taskIds){
            YQL_ENSURE(Tasks_.contains(taskId));
            auto taskStatus = Tasks_[taskId].TaskStatus;
            if (taskStatus == ETaskStatus::InProgress) {
                TaskToDeleteIds_.insert(taskId); // Task is currently running, send signal to worker to cancel
            } else {
                ClearTask(taskId); // Task either hasn't begun running or finished, remove info
            }
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
            YQL_ENSURE(Tasks_.contains(taskId));
            auto operationId = Tasks_[taskId].OperationId;
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(Operations_[operationId].SessionId);
            auto taskStatus = requestTaskState->TaskStatus;
            YQL_ENSURE(taskStatus != ETaskStatus::Accepted);
            SetUnfinishedTaskStatus(taskId, taskStatus, requestTaskState->TaskErrorMessage);
            if (TaskToDeleteIds_.contains(taskId) && Tasks_[taskId].TaskStatus != ETaskStatus::InProgress) {
                ClearTask(taskId); // Task finished, so we don't need to cancel it, just remove info
            }

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
        }

        std::vector<TTask::TPtr> currentTasksToRun;
        ui64 filledSlots = 0;
        while (filledSlots < request.AvailableSlots) {
            if (TasksToRun_.empty()) {
                break;
            }
            auto [task, taskId] = TasksToRun_.front();
            TasksToRun_.pop();
            if (!Tasks_.contains(taskId)) {
                continue;
            }
            auto& taskInfo = Tasks_[taskId];
            YQL_ENSURE(taskInfo.TaskStatus == ETaskStatus::Accepted);
            SetUnfinishedTaskStatus(taskId, ETaskStatus::InProgress);
            SetPartIdsForTask(task);
            currentTasksToRun.emplace_back(task);
            ++filledSlots;
        }

        return NThreading::MakeFuture(THeartbeatResponse{.TasksToRun = currentTasksToRun, .TaskToDeleteIds = TaskToDeleteIds_});
    }

    NThreading::TFuture<TGetFmrTableInfoResponse> GetFmrTableInfo(const TGetFmrTableInfoRequest& request) override {
        TGuard<TMutex> guard(Mutex_);
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
                                if (operationStatus != EOperationStatus::Accepted && operationStatus != EOperationStatus::InProgress) {
                                    auto& taskIds = operationInfo.TaskIds;
                                    for (auto& taskId: taskIds) {
                                        ClearTask(taskId);
                                    }
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

        YQL_ENSURE(Operations_.contains(taskInfo.OperationId));
        auto& currentTaskIdsForOperation = Operations_[taskInfo.OperationId];
        currentTaskIdsForOperation.TaskIds.erase(taskId);
        if (currentTaskIdsForOperation.TaskIds.empty()) {
            // All task for operation are cleared, can clear it
            Operations_.erase(taskInfo.OperationId);
        }
        Tasks_.erase(taskId);
    }

    void SetUnfinishedTaskStatus(const TString& taskId, ETaskStatus newTaskStatus, const TMaybe<TFmrError>& taskErrorMessage = Nothing()) {
        auto& taskInfo = Tasks_[taskId];
        YQL_ENSURE(Operations_.contains(taskInfo.OperationId));
        auto& operationInfo = Operations_[taskInfo.OperationId];
        if (taskInfo.TaskStatus != ETaskStatus::Accepted && taskInfo.TaskStatus != ETaskStatus::InProgress) {
            return;
        }
        YQL_CLOG(TRACE, FastMapReduce) << "Setting task status for task id" << taskId << " from " << taskInfo.TaskStatus << " to new Task status " << newTaskStatus << "\n";
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

    std::vector<TTaskParams> PartitionOperationIntoSeveralTasks(const TOperationParams& operationParams, const NYT::TNode& fmrOperationSpec, const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections) {
        auto fmrPartitionerSettings = GetFmrPartitionerSettings(fmrOperationSpec);
        auto ytPartitionerSettings = GetYtPartitionerSettings(fmrOperationSpec);
        auto fmrPartitioner = TFmrPartitioner(PartIdsForTables_,PartIdStats_, fmrPartitionerSettings); // TODO - fix this

        std::vector<TYtTableRef> ytInputTables;
        std::vector<TFmrTableRef> fmrInputTables;
        GetOperationInputTables(ytInputTables, fmrInputTables, operationParams);

        TPartitionResult partitionResult = PartitionInputTablesIntoTasks(ytInputTables, fmrInputTables, fmrPartitioner, YtCoordinatorService_, clusterConnections, ytPartitionerSettings);
        if (!partitionResult.PartitionStatus) {
            ythrow yexception() << "Failed to partition input tables into tasks";
            // TODO - return FAILED_PARTITIONING status instead.
        }
        return GetOutputTaskParams(partitionResult, operationParams);
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

    std::vector<TTaskParams> GetOutputTaskParams(const TPartitionResult& partitionResult, const TOperationParams& operationParams) {
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

    void SetPartIdsForTask(TTask::TPtr task) {
        // TODO - add failover, clearing previous partId if exists
        TString partId = GenerateId();

        auto* downloadTaskParams = std::get_if<TDownloadTaskParams>(&task->TaskParams);
        auto* mergeTaskParams = std::get_if<TMergeTaskParams>(&task->TaskParams);
        auto* mapTaskParams = std::get_if<TMapTaskParams>(&task->TaskParams);
        if (downloadTaskParams) {
            TString tableId = downloadTaskParams->Output.TableId;
            downloadTaskParams->Output.PartId = partId;
            PartIdsForTables_[tableId].emplace_back(partId);
        } else if (mergeTaskParams) {
            TString tableId = mergeTaskParams->Output.TableId;
            mergeTaskParams->Output.PartId = partId;
            PartIdsForTables_[tableId].emplace_back(partId);
        } else if (mapTaskParams) {
            for (auto& fmrTableOutputRef: mapTaskParams->Output) {
                TString tableId = fmrTableOutputRef.TableId;
                fmrTableOutputRef.PartId = partId;
                PartIdsForTables_[tableId].emplace_back(partId);
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

    //////////////////////////////////////////////////////////////////////////////////////////////////////////

    struct TCoordinatorTaskInfo {
        TTask::TPtr Task;
        ETaskStatus TaskStatus;
        TString OperationId;
    };

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

    std::unordered_map<TString, TCoordinatorTaskInfo> Tasks_; // TaskId -> current info about it
    std::queue<std::pair<TTask::TPtr, TString>> TasksToRun_; // Task, and TaskId
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

    std::unordered_map<TFmrTableId, std::vector<TString>> PartIdsForTables_; // TableId -> List of all corresponding partIds
    std::unordered_map<TString, std::vector<TChunkStats>> PartIdStats_; // PartId -> Detailed statistic for each chunk
    std::unordered_map<TString, TTableStats> OperationTableStats_; // TableId -> Statistic for fmr table, filled when operation completes


    NYT::TNode DefaultFmrOperationSpec_;
    IYtCoordinatorService::TPtr YtCoordinatorService_; // Needed for partitioning of yt tables

    //////////////////////////////////////////////////////////////////////////////////////////////////////////

    // Helper structs for partitioning operation into tasks

    struct TOperationInputTablesGetter {
        std::vector<TOperationTableRef> OperationTableRef; // will be filled when std::visit is called

        void operator () (const TUploadOperationParams& uploadOperationParams) {
            OperationTableRef.emplace_back(uploadOperationParams.Input);
        }
        void operator () (const TDownloadOperationParams& downloadOperationParams) {
            OperationTableRef.emplace_back(downloadOperationParams.Input);
        }
        void operator () (const TMergeOperationParams& mergeOperationParams) {
            OperationTableRef = mergeOperationParams.Input;
        }
        void operator () (const TMapOperationParams& mapOperationParams) {
            OperationTableRef = mapOperationParams.Input;
        }
    };

    struct TOutputTaskParamsGetter {
        std::vector<TTaskParams> TaskParams; // Will be filled when std::visit is called
        TPartitionResult PartitionResult;

        void operator () (const TUploadOperationParams& uploadOperationParams) {
            for (auto& task: PartitionResult.TaskInputs) {
                TUploadTaskParams uploadTaskParams;
                YQL_ENSURE(task.Inputs.size() == 1, "Upload task should have exactly one fmr table partition input");
                auto& fmrTablePart = task.Inputs[0];
                uploadTaskParams.Input = std::get<TFmrTableInputRef>(fmrTablePart);
                uploadTaskParams.Output = uploadOperationParams.Output;
                TaskParams.emplace_back(uploadTaskParams);
            }
        }
        void operator () (const TDownloadOperationParams& downloadOperationParams) {
            for (auto& task: PartitionResult.TaskInputs) {
                TDownloadTaskParams downloadTaskParams;
                YQL_ENSURE(task.Inputs.size() == 1, "Download task should have exactly one yt table partition input");
                auto& ytTablePart = task.Inputs[0];
                downloadTaskParams.Input = std::get<TYtTableTaskRef>(ytTablePart);
                downloadTaskParams.Output = TFmrTableOutputRef{.TableId = downloadOperationParams.Output.FmrTableId.Id};
                // PartId for tasks which write to table data service will be set later
                TaskParams.emplace_back(downloadTaskParams);
            }
        }
        void operator () (const TMergeOperationParams& mergeOperationParams) {
            for (auto& task: PartitionResult.TaskInputs) {
                TMergeTaskParams mergeTaskParams;
                mergeTaskParams.Input = task;
                mergeTaskParams.Output = TFmrTableOutputRef{.TableId = mergeOperationParams.Output.FmrTableId.Id};
                TaskParams.emplace_back(mergeTaskParams);
            }
        }
        void operator () (const TMapOperationParams& mapOperationParams) {
            for (auto& task: PartitionResult.TaskInputs) {
                TMapTaskParams mapTaskParams;
                mapTaskParams.Input = task;
                std::vector<TFmrTableOutputRef> fmrTableOutputRefs;
                std::transform(mapOperationParams.Output.begin(), mapOperationParams.Output.end(), std::back_inserter(fmrTableOutputRefs), [] (const TFmrTableRef& fmrTableRef) {
                    return TFmrTableOutputRef{.TableId = fmrTableRef.FmrTableId.Id};
                });

                mapTaskParams.Output = fmrTableOutputRefs;
                mapTaskParams.Executable = mapOperationParams.Executable; // TODO - change Executable to mapper
                TaskParams.emplace_back(mapTaskParams);
            }
        }
    };
};

} // namespace

IFmrCoordinator::TPtr MakeFmrCoordinator(const TFmrCoordinatorSettings& settings, IYtCoordinatorService::TPtr ytCoordinatorService) {
    return MakeIntrusive<TFmrCoordinator>(settings, ytCoordinatorService);
}

} // namespace NYql::NFmr

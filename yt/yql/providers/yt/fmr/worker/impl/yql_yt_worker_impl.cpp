#include <library/cpp/threading/future/wait/wait.h>
#include <queue>
#include <thread>
#include <util/system/mutex.h>
#include <util/system/rusage.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>
#include "yql_yt_worker_impl.h"
#include <yt/yql/providers/yt/fmr/worker/interface/yql_yt_fmr_worker.h>

namespace NYql::NFmr {

namespace {

struct TResource {
    EFmrResourceType ResourceType;
    NThreading::TFuture<TFileLinkPtr> DownloadFuture;
};

struct TFmrWorkerState {
    TMutex Mutex;
    std::unordered_map<TString, TTaskState::TPtr> TaskStatuses;
    std::atomic<EFmrWorkerRuntimeState> State = EFmrWorkerRuntimeState::Stopped;

    std::unordered_map<TString, TTask::TPtr> TasksToRun; // taskId -> tasks with already downloaded files, waiting for job start
    std::unordered_map<TString, TTask::TPtr> TasksToDownload; // taskId -> tasks which are downloading files.
    std::unordered_map<TString, std::vector<TFileLinkPtr>> LocalFileCache; // taskId -> downloaded files
    std::unordered_set<TString> ForceFailedTaskIds; // taskIds force-failed, pending callback cleanup

    void HandleTaskException(const TString& taskId, const TString& exceptionMessage, EFmrErrorReason reason = EFmrErrorReason::Unknown) {
        with_lock(Mutex) {
            YQL_CLOG(ERROR, FastMapReduce) << "Task with id " << taskId << " failed with error message " << exceptionMessage;
            if (ForceFailedTaskIds.contains(taskId)) {
                ForceFailedTaskIds.erase(taskId);
                return;
            }
            if (!TaskStatuses.contains(taskId)) {
                YQL_CLOG(WARN, FastMapReduce) << "HandleTaskException for task " << taskId
                    << " which is no longer in TaskStatuses (likely due to worker restart). Error was: " << exceptionMessage;
                return;
            }
            auto taskError = TFmrError{
                .Component = EFmrComponent::Worker,
                .Reason = reason,
                .ErrorMessage = exceptionMessage,
                .TaskId = taskId
            };
            TaskStatuses[taskId] = MakeTaskState(ETaskStatus::Failed, taskId, taskError, TStatistics());

            TasksToRun.erase(taskId);
            TasksToDownload.erase(taskId);
        }
    }

    void HandleResourceDownload(TTask::TPtr task, std::vector<TResource>& downloadedResources) {
        TString taskId = task->TaskId;
        auto handleResourceDownload = [&] <class TTaskResourceInfo, class TResourceDescFunc> (
            std::vector<TTaskResourceInfo>& taskResourcesInfo,
            EFmrResourceType taskResourceType,
            TResourceDescFunc resourceDescFunc
        ) {
            ui64 pos = 0;

            for (auto& resource: downloadedResources) {
                if (taskResourceType != resource.ResourceType) {
                    continue;
                }
                TFileLinkPtr downloadedResource = resource.DownloadFuture.GetValue();
                LocalFileCache[taskId].emplace_back(downloadedResource);
                YQL_CLOG(DEBUG, FastMapReduce) << "Downloaded resource of type " << taskResourceType << " with desc " << resourceDescFunc(taskResourcesInfo[pos]) << " to task with id " << taskId;
                taskResourcesInfo[pos].LocalPath = downloadedResource->GetPath(); // Fill localPath information in tasks with downloaded resources.
                ++pos;
            }
        };

        auto fileInfoDescFunc = [] (const TFileInfo& file) {
            return file.Md5Key;
        };

        auto ytResourceDescFunc = [] (const TYtResourceInfo& ytResource) {
            return SerializeRichPath(ytResource.RichPath);
        };

        auto fmrResourceDescFunc = [] (const TFmrResourceTaskInfo& fmrResource) {
            return fmrResource.FmrResourceTasks[0].TableId;
        };

        handleResourceDownload(task->Files, EFmrResourceType::DistributedCacheFile, fileInfoDescFunc);
        handleResourceDownload(task->YtResources, EFmrResourceType::YtResource, ytResourceDescFunc);
        handleResourceDownload(task->FmrResources, EFmrResourceType::FmrResource, fmrResourceDescFunc);
    }
};


class TFmrWorker: public IFmrWorker {
public:
    TFmrWorker(
        IFmrCoordinator::TPtr coordinator,
        IFmrJobFactory::TPtr jobFactory,
        IFmrJobPreparer::TPtr jobPreparer,
        const TFmrWorkerSettings& settings
    )
        : Coordinator_(coordinator),
        JobFactory_(jobFactory),
        JobPreparer_(jobPreparer),
        WorkerState_(std::make_shared<TFmrWorkerState>(TMutex(), std::unordered_map<TString, TTaskState::TPtr>{}, EFmrWorkerRuntimeState::Stopped)),
        StopWorker_(false),
        NeedToRestart_(false),
        RandomProvider_(settings.RandomProvider),
        WorkerId_(settings.WorkerId),
        TimeToSleepBetweenRequests_(settings.TimeToSleepBetweenRequests),
        MemoryLimitBytes_(settings.MemoryLimitBytes)
{
    YQL_ENSURE(Coordinator_ && RandomProvider_ && JobPreparer_);
    GenerateVolatileId();
}

    ~TFmrWorker() {
        Stop();
    }

    void Start() override {
        WorkerState_->State = EFmrWorkerRuntimeState::Running;

        HeartbeatInFlight_ = std::make_shared<std::atomic<bool>>(false);

        auto heartbeatThreadFunc = [&] () {
            while (!StopWorker_) {
                try {
                    if (NeedToRestart_.load() || HeartbeatInFlight_->load()) {
                        Sleep(TimeToSleepBetweenRequests_);
                        continue;
                    }

                    if (MemoryLimitBytes_ > 0) {
                        ui64 currentRss = TRusage::GetCurrentRSS();
                        if (currentRss > MemoryLimitBytes_) {
                            with_lock(WorkerState_->Mutex) {
                                YQL_CLOG(ERROR, FastMapReduce) << "Worker OOM detected (RSS: " << currentRss
                                    << " bytes, limit: " << MemoryLimitBytes_ << " bytes), failing all tasks";
                                for (auto& [taskId, taskState] : WorkerState_->TaskStatuses) {
                                    if (taskState->TaskStatus == ETaskStatus::InProgress) {
                                        auto taskError = TFmrError{
                                            .Component = EFmrComponent::Worker,
                                            .Reason = EFmrErrorReason::WorkerOOM,
                                            .ErrorMessage = TStringBuilder()
                                                << "Worker out of memory (RSS: " << currentRss << " bytes, limit: " << MemoryLimitBytes_ << " bytes)",
                                            .WorkerId = WorkerId_,
                                            .TaskId = taskId
                                        };
                                        taskState = MakeTaskState(ETaskStatus::Failed, taskId, taskError, TStatistics());
                                        WorkerState_->ForceFailedTaskIds.insert(taskId);
                                    }
                                }
                                WorkerState_->TasksToRun.clear();
                                WorkerState_->TasksToDownload.clear();
                            }
                        }
                    }

                    std::vector<TTaskState::TPtr> taskStates;
                    std::vector<TString> taskIdsToErase;
                    ui64 availableSlots;
                    TString volatileId;
                    with_lock(WorkerState_->Mutex) {
                        for (auto& [taskId, taskState]: WorkerState_->TaskStatuses) {
                            auto taskStatus = taskState->TaskStatus;
                            if (taskStatus != ETaskStatus::InProgress) {
                                taskIdsToErase.emplace_back(taskId);
                            }
                            taskStates.emplace_back(taskState);
                        }
                        for (auto& taskId: taskIdsToErase) {
                            WorkerState_->TaskStatuses.erase(taskId);
                            TasksCancelStatus_.erase(taskId);
                        }

                        ui64 maxParallelJobCount = JobFactory_->GetMaxParallelJobCount();
                        YQL_ENSURE(maxParallelJobCount >= WorkerState_->TaskStatuses.size());
                        availableSlots = maxParallelJobCount - WorkerState_->TaskStatuses.size();
                        volatileId = VolatileId_;
                    }

                    auto heartbeatRequest = THeartbeatRequest(
                        WorkerId_,
                        volatileId,
                        taskStates,
                        availableSlots
                    );

                    YQL_CLOG(TRACE, FastMapReduce) << "Sending heartbeat request to coordinator";
                    HeartbeatInFlight_->store(true);
                    Coordinator_->SendHeartbeatResponse(heartbeatRequest).Subscribe(
                        [this, weakState = std::weak_ptr(WorkerState_), heartbeatInFlight = HeartbeatInFlight_](const auto& f) {
                            try {
                                auto heartbeatResponse = f.GetValue();

                                if (heartbeatResponse.NeedToRestart) {
                                    NeedToRestart_.store(true);
                                    heartbeatInFlight->store(false);
                                    return;
                                }

                                std::shared_ptr<TFmrWorkerState> state = weakState.lock();
                                if (!state) {
                                    heartbeatInFlight->store(false);
                                    return;
                                }

                                with_lock(state->Mutex) {
                                    for (auto& taskToDeleteId: heartbeatResponse.TaskToDeleteIds) {
                                        if (TasksCancelStatus_.contains(taskToDeleteId)) {
                                            TasksCancelStatus_[taskToDeleteId]->store(true);
                                        }
                                    }

                                    for (auto task: heartbeatResponse.TasksToRun) {
                                        auto taskId = task->TaskId;
                                        if (state->TaskStatuses.contains(taskId)) {
                                            YQL_CLOG(WARN, FastMapReduce) << "Task " << taskId << " already in TaskStatuses, skipping";
                                            continue;
                                        }
                                        state->TaskStatuses[taskId] = MakeTaskState(ETaskStatus::InProgress, taskId);
                                        TasksCancelStatus_[taskId] = std::make_shared<std::atomic<bool>>(false);
                                        PendingTasks_.push(task);
                                    }
                                }
                            } catch (...) {
                                YQL_CLOG(ERROR, FastMapReduce) << "Error processing heartbeat response: " << CurrentExceptionMessage();
                            }
                            heartbeatInFlight->store(false);
                        }
                    );
                } catch (...) {
                    YQL_CLOG(ERROR, FastMapReduce) << "Error in heartbeat thread: " << CurrentExceptionMessage();
                }
                Sleep(TimeToSleepBetweenRequests_);
            }
        };

        auto taskProcessingThreadFunc = [&] () {
            while (!StopWorker_) {
                try {
                    if (NeedToRestart_.exchange(false)) {
                        Restart();
                        continue;
                    }

                    std::vector<TTask::TPtr> tasksToProcess;
                    with_lock(WorkerState_->Mutex) {
                        while (!PendingTasks_.empty()) {
                            tasksToProcess.push_back(PendingTasks_.front());
                            PendingTasks_.pop();
                        }
                    }

                    std::unordered_map<TString, std::vector<TResource>> downloadResourcesQueue;
                    std::unordered_map<TString, NThreading::TFuture<TTaskState::TPtr>> runningJobFutures;

                    std::vector<std::pair<TString, TString>> failedTaskPreparations;

                    with_lock(WorkerState_->Mutex) {
                        for (auto task: tasksToProcess) {
                            auto taskId = task->TaskId;
                            if (!WorkerState_->TaskStatuses.contains(taskId)) {
                                YQL_CLOG(WARN, FastMapReduce) << "Task " << taskId << " disappeared from TaskStatuses before processing, skipping";
                                continue;
                            }

                            try {
                                auto tempDir = JobPreparer_->GenerateJobEnvironmentDir(taskId);
                                task->JobEnvironmentDir = tempDir;

                                bool needToDownloadResourcesForTask = !(task->Files.empty() && task->YtResources.empty() && task->FmrResources.empty());
                                if (!needToDownloadResourcesForTask) {
                                    WorkerState_->TasksToRun.emplace(taskId, task);
                                } else {
                                    WorkerState_->TasksToDownload.emplace(taskId, task);
                                    ScheduleResourcesDownload(task, downloadResourcesQueue);
                                }
                            } catch (...) {
                                auto msg = CurrentExceptionMessage();
                                YQL_CLOG(ERROR, FastMapReduce) << "Error preparing task " << taskId << ": " << msg;
                                failedTaskPreparations.emplace_back(taskId, msg);
                            }
                        }

                        for (auto& [taskId, task]: WorkerState_->TasksToRun) {
                            runningJobFutures.emplace(taskId, JobFactory_->StartJob(task, TasksCancelStatus_[taskId]));
                        }
                        WorkerState_->TasksToRun.clear();
                    }

                    for (auto& [taskId, msg]: failedTaskPreparations) {
                        WorkerState_->HandleTaskException(taskId, msg, ParseFmrReasonFromErrorMessage(msg));
                    }

                    for (auto& [taskId, resources]: downloadResourcesQueue) {
                        std::vector<NThreading::TFuture<void>> allDownloadResourceFutures;
                        for (auto& resource: resources) {
                            allDownloadResourceFutures.emplace_back(resource.DownloadFuture.IgnoreResult());
                        }

                        NThreading::WaitExceptionOrAll(allDownloadResourceFutures).Subscribe([weakState = std::weak_ptr(WorkerState_), taskId, downloadResourcesQueue] (const auto& f) {
                            std::shared_ptr<TFmrWorkerState> state = weakState.lock();
                            if (state) {
                                try {
                                    f.GetValue();
                                    with_lock(state->Mutex) {
                                        if (state->ForceFailedTaskIds.contains(taskId)) {
                                            YQL_CLOG(DEBUG, FastMapReduce) << "Skipping resource download callback for force-failed task " << taskId;
                                            state->ForceFailedTaskIds.erase(taskId);
                                            return;
                                        }
                                        if (!state->TasksToDownload.contains(taskId)) {
                                            YQL_CLOG(WARN, FastMapReduce) << "Resource download callback arrived for task " << taskId
                                                << " which is no longer in TasksToDownload (likely due to worker restart)";
                                            return;
                                        }
                                        auto task = state->TasksToDownload[taskId];

                                        YQL_ENSURE(downloadResourcesQueue.contains(taskId));
                                        std::vector<TResource> downloadedResources = downloadResourcesQueue.at(taskId);
                                        state->HandleResourceDownload(task, downloadedResources);

                                        state->TasksToRun.emplace(taskId, task);
                                        state->TasksToDownload.erase(taskId);
                                    }
                                } catch (...) {
                                    state->HandleTaskException(taskId, CurrentExceptionMessage(), EFmrErrorReason::RestartOperation);
                                }
                            }
                        });
                    }

                    for (auto& [taskId, future]: runningJobFutures) {
                        future.Subscribe([weakState = std::weak_ptr(WorkerState_), taskId](const auto& jobFuture) {
                            std::shared_ptr<TFmrWorkerState> state = weakState.lock();
                            if (state) {
                                try {
                                    auto finalTaskState = jobFuture.GetValue();
                                    with_lock(state->Mutex) {
                                        if (state->ForceFailedTaskIds.contains(taskId)) {
                                            state->ForceFailedTaskIds.erase(taskId);
                                            return;
                                        }
                                        if (!state->TaskStatuses.contains(taskId)) {
                                            YQL_CLOG(WARN, FastMapReduce) << "Job completion callback arrived for task " << taskId
                                                << " which is no longer in TaskStatuses (likely due to worker restart)";
                                            return;
                                        }
                                        state->TaskStatuses[taskId] = finalTaskState;
                                    }
                                } catch (...) {
                                    auto msg = CurrentExceptionMessage();
                                    state->HandleTaskException(taskId, msg, ParseFmrReasonFromErrorMessage(msg));
                                }
                            }
                        });
                    }
                } catch (...) {
                    YQL_CLOG(ERROR, FastMapReduce) << "Error in task processing thread: " << CurrentExceptionMessage();
                }
                Sleep(TDuration::MilliSeconds(100));
            }
        };

        HeartbeatThread_ = std::thread(heartbeatThreadFunc);
        TaskProcessingThread_ = std::thread(taskProcessingThreadFunc);
    }

    void Stop() override {
        StopWorker_ = true;

        if (HeartbeatThread_.joinable()) {
            HeartbeatThread_.join();
        }

        if (HeartbeatInFlight_) {
            while (HeartbeatInFlight_->load()) {
                Sleep(TDuration::MilliSeconds(10));
            }
        }

        with_lock(WorkerState_->Mutex) {
            StopJobFactoryTasks();
            WorkerState_->State = EFmrWorkerRuntimeState::Stopped;
        }

        if (TaskProcessingThread_.joinable()) {
            TaskProcessingThread_.join();
        }
        JobFactory_->Stop();
    }

    EFmrWorkerRuntimeState GetWorkerState() const override {
        return WorkerState_->State;
    }

private:
    void Restart() {
        YQL_CLOG(INFO, FastMapReduce) << "Worker with id " << WorkerId_ << " is assumed dead by coordinator, restarting";
        with_lock(WorkerState_->Mutex) {
            StopJobFactoryTasks();
        }
        JobFactory_->Stop();
        with_lock(WorkerState_->Mutex) {
            ClearState();
            GenerateVolatileId();
        }
        JobFactory_->Start();
    }

    void StopJobFactoryTasks() {
        for (auto& taskInfo: TasksCancelStatus_) {
            if (taskInfo.second) {
                taskInfo.second->store(true);
            }
        }
    }

    void ClearState() {
        TasksCancelStatus_.clear();
        WorkerState_->TaskStatuses.clear();
        WorkerState_->TasksToRun.clear();
        WorkerState_->TasksToDownload.clear();
        WorkerState_->ForceFailedTaskIds.clear();
        std::queue<TTask::TPtr> empty;
        PendingTasks_.swap(empty);
    }

    void GenerateVolatileId() {
        VolatileId_ = GetGuidAsString(RandomProvider_->GenGuid());
        YQL_CLOG(INFO, FastMapReduce) << "Setting volatile id " << VolatileId_ << " for worker with id " << WorkerId_;
    }

    void ScheduleResourcesDownload(TTask::TPtr task, std::unordered_map<TString, std::vector<TResource>>& downloadResourcesQueue) {
        TString taskId = task->TaskId;

        auto scheduleResourcesDownloadForFixedType = [&] <class Resource, class DownloadFunc> (
            const std::vector<Resource>& resources,
            DownloadFunc downloadFunc,
            EFmrResourceType resourceType
        ) {
            if (!resources.empty()) {
                std::vector<TResource> downloadResourceFutures;
                for (auto& resource: resources) {
                    downloadResourceFutures.emplace_back(TResource(resourceType, downloadFunc(resource)));
                }
                auto& queue = downloadResourcesQueue[taskId];
                queue.insert(queue.end(), downloadResourceFutures.begin(), downloadResourceFutures.end());
            }
        };

        scheduleResourcesDownloadForFixedType(
            task->Files,
            [this] (const TFileInfo& fileInfo) {
                return JobPreparer_->DownloadFileFromDistributedCache(fileInfo.Md5Key);
            },
            EFmrResourceType::DistributedCacheFile
        );

        scheduleResourcesDownloadForFixedType(
            task->YtResources,
            [this] (const TYtResourceInfo& ytResource) {
                return JobPreparer_->DownloadYtResource(ytResource.RichPath, ytResource.YtServerName, ytResource.Token);
            },
            EFmrResourceType::YtResource
        );

        scheduleResourcesDownloadForFixedType(
            task->FmrResources,
            [this] (const TFmrResourceTaskInfo& fmrResource) {
                return JobPreparer_->DownloadFmrResource(fmrResource);
            },
            EFmrResourceType::FmrResource
        );
    }

private:
    IFmrCoordinator::TPtr Coordinator_;
    IFmrJobFactory::TPtr JobFactory_;
    IFmrJobPreparer::TPtr JobPreparer_;
    std::unordered_map<TString, std::shared_ptr<std::atomic<bool>>> TasksCancelStatus_;
    std::shared_ptr<TFmrWorkerState> WorkerState_;
    std::atomic<bool> StopWorker_;
    std::atomic<bool> NeedToRestart_;
    const TIntrusivePtr<IRandomProvider> RandomProvider_;
    const ui32 WorkerId_;
    TString VolatileId_;
    std::thread HeartbeatThread_;
    std::thread TaskProcessingThread_;
    std::shared_ptr<std::atomic<bool>> HeartbeatInFlight_;
    std::queue<TTask::TPtr> PendingTasks_;
    const TDuration TimeToSleepBetweenRequests_;
    const ui64 MemoryLimitBytes_;
};

} // namespace

IFmrWorker::TPtr MakeFmrWorker(IFmrCoordinator::TPtr coordinator, IFmrJobFactory::TPtr jobFactory, IFmrJobPreparer::TPtr jobPreparer, const TFmrWorkerSettings& settings) {
    return MakeIntrusive<TFmrWorker>(coordinator, jobFactory, jobPreparer, settings);
}

} // namespace NYql::NFmr

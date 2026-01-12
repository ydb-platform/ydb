#include <library/cpp/threading/future/wait/wait.h>
#include <thread>
#include <util/system/mutex.h>
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

    void HandleTaskException(const TString& taskId, const TString& exceptionMessage) {
        // TODO (@cdzyura171) - Restart Fmr Operation in case download of files / resources failed in jobPreparer.
        with_lock(Mutex) {
            YQL_CLOG(ERROR, FastMapReduce) << "Task with id " << taskId << " failed with error message " << exceptionMessage;
            YQL_ENSURE(TaskStatuses.contains(taskId));
            auto taskStatus = ETaskStatus::Failed;
            auto taskError = TFmrError{.Component = EFmrComponent::Worker, .ErrorMessage = exceptionMessage, .TaskId = taskId};
            auto taskState = MakeTaskState(taskStatus, taskId, taskError, TStatistics());
            TaskStatuses[taskId] = taskState;

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
        RandomProvider_(settings.RandomProvider),
        WorkerId_(settings.WorkerId),
        TimeToSleepBetweenRequests_(settings.TimeToSleepBetweenRequests)
{
    YQL_ENSURE(Coordinator_ && RandomProvider_ && JobPreparer_);
    GenerateVolatileId();
}

    ~TFmrWorker() {
        Stop();
    }

    void Start() override {
        auto mainThreadFunc = [&] () {
            WorkerState_->State = EFmrWorkerRuntimeState::Running;

            while (!StopWorker_) {
                try {
                    std::vector<TTaskState::TPtr> taskStates;
                    std::vector<TString> taskIdsToErase;
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
                    }

                    ui64 maxParallelJobCount = JobFactory_->GetMaxParallelJobCount();
                    YQL_ENSURE(maxParallelJobCount >= WorkerState_->TaskStatuses.size());
                    ui64 availableSlots = maxParallelJobCount - WorkerState_->TaskStatuses.size();
                    auto heartbeatRequest = THeartbeatRequest(
                        WorkerId_,
                        VolatileId_,
                        taskStates,
                        availableSlots
                    );

                    YQL_CLOG(TRACE, FastMapReduce) << "Sending heartbeat request to coordiantor";
                    auto heartbeatResponseFuture = Coordinator_->SendHeartbeatResponse(heartbeatRequest);
                    auto heartbeatResponse = heartbeatResponseFuture.GetValueSync();

                    if (heartbeatResponse.NeedToRestart) {
                        Restart();
                        continue;
                    }

                    std::vector<TTask::TPtr> tasksToRun = heartbeatResponse.TasksToRun;
                    std::unordered_set<TString> taskToDeleteIds = heartbeatResponse.TaskToDeleteIds;
                    YQL_ENSURE(tasksToRun.size() <= availableSlots);

                    std::unordered_map<TString, std::vector<TResource>> downloadResourcesQueue;
                    std::unordered_map<TString, NThreading::TFuture<TTaskState::TPtr>> runningJobFutures;

                    with_lock(WorkerState_->Mutex) {
                        for (auto task: tasksToRun) {
                            auto taskId = task->TaskId;
                            YQL_ENSURE(!WorkerState_->TaskStatuses.contains(taskId));
                            WorkerState_->TaskStatuses[taskId] = MakeTaskState(ETaskStatus::InProgress, taskId);
                            TasksCancelStatus_[taskId] = std::make_shared<std::atomic<bool>>(false);

                            auto tempDir = JobPreparer_->GenerateJobEnvironmentDir(taskId);
                            task->JobEnvironmentDir = tempDir;

                            bool needToDownloadResourcesForTask = !(task->Files.empty() && task->YtResources.empty() && task->FmrResources.empty());
                            if (!needToDownloadResourcesForTask) {
                                // If we don't have any resources, immediately schedule task to run.
                                WorkerState_->TasksToRun.emplace(taskId, task);
                            } else {
                                WorkerState_->TasksToDownload.emplace(taskId, task);
                                ScheduleResourcesDownload(task, downloadResourcesQueue);
                            }
                        }

                        for (auto& taskToDeleteId: taskToDeleteIds) {
                            if (TasksCancelStatus_.contains(taskToDeleteId)) {
                                TasksCancelStatus_[taskToDeleteId]->store(true);
                            }
                        }

                        for (auto& [taskId, task]: WorkerState_->TasksToRun) {
                            runningJobFutures.emplace(taskId, JobFactory_->StartJob(task, TasksCancelStatus_[taskId]));
                        }
                        WorkerState_->TasksToRun.clear();
                    }

                    for (auto& [taskId, resources]: downloadResourcesQueue) {
                        std::vector<NThreading::TFuture<void>> allDownloadResourceFutures; // all download resource futures for fixed task
                        for (auto& resource: resources) {
                            allDownloadResourceFutures.emplace_back(resource.DownloadFuture.IgnoreResult());
                        }

                        NThreading::WaitExceptionOrAll(allDownloadResourceFutures).Subscribe([weakState = std::weak_ptr(WorkerState_), taskId, downloadResourcesQueue = std::move(downloadResourcesQueue)] (const auto& f) {
                            std::shared_ptr<TFmrWorkerState> state = weakState.lock();
                            if (state) {
                                try {
                                    f.GetValue();
                                    with_lock(state->Mutex) {
                                        YQL_ENSURE(state->TasksToDownload.contains(taskId));
                                        auto task = state->TasksToDownload[taskId];

                                        YQL_ENSURE(downloadResourcesQueue.contains(taskId));
                                        std::vector<TResource> downloadedResources = downloadResourcesQueue.at(taskId);
                                        state->HandleResourceDownload(task, downloadedResources);

                                        state->TasksToRun.emplace(taskId, task);
                                        state->TasksToDownload.erase(taskId);
                                    }
                                } catch (...) {
                                    state->HandleTaskException(taskId, CurrentExceptionMessage());
                                }
                            }
                        });
                    }

                    for (auto& [taskId, future]: runningJobFutures) {
                        auto runJobFuture = future.Subscribe([weakState = std::weak_ptr(WorkerState_), taskId](const auto& jobFuture) {
                            std::shared_ptr<TFmrWorkerState> state = weakState.lock();
                            if (state) {
                                auto finalTaskState = jobFuture.GetValue();
                                try {
                                    with_lock(state->Mutex) {
                                        YQL_ENSURE(state->TaskStatuses.contains(taskId));
                                        state->TaskStatuses[taskId] = finalTaskState;
                                    }
                                } catch (...) {
                                    state->HandleTaskException(taskId, CurrentExceptionMessage());
                                }
                            }
                        });
                    }
                } catch (...) {
                    YQL_CLOG(ERROR, FastMapReduce) << "Error while processing heartbeat request: " << CurrentExceptionMessage();
                }
                Sleep(TimeToSleepBetweenRequests_);
            }
        };
        MainThread_ = std::thread(mainThreadFunc);
    }

    void Stop() override {
        with_lock(WorkerState_->Mutex) {
            StopJobFactoryTasks();
            StopWorker_ = true;
            WorkerState_->State = EFmrWorkerRuntimeState::Stopped;
        }
        JobFactory_->Stop();
        if (MainThread_.joinable()) {
            MainThread_.join();
        }
    }

    EFmrWorkerRuntimeState GetWorkerState() const override {
        return WorkerState_->State;
    }

private:
    void Restart() {
        YQL_CLOG(INFO, FastMapReduce) << "Worker with id " << WorkerId_ << " is assumed dead by coordinator, restarting";
        JobFactory_->Stop();
        with_lock(WorkerState_->Mutex) {
            StopJobFactoryTasks();
            ClearState();
            GenerateVolatileId();
        }
        JobFactory_->Start();
    }

    void StopMainThread() {
        if (MainThread_.joinable()) {
            MainThread_.join();
        }
    }

    void StopJobFactoryTasks() {
        for (auto& taskInfo: TasksCancelStatus_) {
            taskInfo.second->store(true);
        }
    }

    void ClearState() {
        TasksCancelStatus_.clear();
        WorkerState_->TaskStatuses.clear();
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
                downloadResourcesQueue[taskId] = downloadResourceFutures;
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
    const TIntrusivePtr<IRandomProvider> RandomProvider_;
    const ui32 WorkerId_;
    TString VolatileId_;
    std::thread MainThread_;
    const TDuration TimeToSleepBetweenRequests_;
};

} // namespace

IFmrWorker::TPtr MakeFmrWorker(IFmrCoordinator::TPtr coordinator, IFmrJobFactory::TPtr jobFactory, IFmrJobPreparer::TPtr jobPreparer, const TFmrWorkerSettings& settings) {
    return MakeIntrusive<TFmrWorker>(coordinator, jobFactory, jobPreparer, settings);
}

} // namespace NYql::NFmr

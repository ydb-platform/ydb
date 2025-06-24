#include <library/cpp/threading/future/wait/wait.h>
#include <thread>
#include <util/system/mutex.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>
#include "yql_yt_worker_impl.h"

namespace NYql::NFmr {

namespace {

struct TFmrWorkerState {
    TMutex Mutex;
    std::unordered_map<TString, TTaskState::TPtr> TaskStatuses;
};

class TFmrWorker: public IFmrWorker {
public:
    TFmrWorker(IFmrCoordinator::TPtr coordinator, IFmrJobFactory::TPtr jobFactory, const TFmrWorkerSettings& settings)
        : Coordinator_(coordinator),
        JobFactory_(jobFactory),
        WorkerState_(std::make_shared<TFmrWorkerState>(TMutex(), std::unordered_map<TString, TTaskState::TPtr>{})),
        StopWorker_(false),
        RandomProvider_(settings.RandomProvider),
        WorkerId_(settings.WorkerId),
        TimeToSleepBetweenRequests_(settings.TimeToSleepBetweenRequests)
{
    GenerateVolatileId();
}

    ~TFmrWorker() {
        Stop();
    }

    void Start() override {
        auto mainThreadFunc = [&] () {
            while (!StopWorker_) {
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
                auto heartbeatResponseFuture = Coordinator_->SendHeartbeatResponse(heartbeatRequest);
                auto heartbeatResponse = heartbeatResponseFuture.GetValueSync();

                if (heartbeatResponse.NeedToRestart) {
                    Restart();
                    continue;
                }

                std::vector<TTask::TPtr> tasksToRun = heartbeatResponse.TasksToRun;
                std::unordered_set<TString> taskToDeleteIds = heartbeatResponse.TaskToDeleteIds;
                YQL_ENSURE(tasksToRun.size() <= availableSlots);

                with_lock(WorkerState_->Mutex) {
                    for (auto task: tasksToRun) {
                        auto taskId = task->TaskId;
                        YQL_ENSURE(!WorkerState_->TaskStatuses.contains(taskId));
                        WorkerState_->TaskStatuses[taskId] = MakeTaskState(ETaskStatus::InProgress, taskId);
                        TasksCancelStatus_[taskId] = std::make_shared<std::atomic<bool>>(false);
                    }
                    for (auto& taskToDeleteId: taskToDeleteIds) {
                        if (TasksCancelStatus_.contains(taskToDeleteId)) {
                            TasksCancelStatus_[taskToDeleteId]->store(true);
                        }
                    }

                    for (auto task: tasksToRun) {
                        auto taskId = task->TaskId;
                        auto future = JobFactory_->StartJob(task, TasksCancelStatus_[taskId]);
                        future.Subscribe([weakState = std::weak_ptr(WorkerState_), task](const auto& jobFuture) {
                            auto finalTaskState = jobFuture.GetValue();
                            std::shared_ptr<TFmrWorkerState> state = weakState.lock();
                            if (state) {
                                with_lock(state->Mutex) {
                                    YQL_ENSURE(state->TaskStatuses.contains(task->TaskId));
                                    state->TaskStatuses[task->TaskId] = finalTaskState;
                                }
                            }
                        });
                    }
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
        }
        JobFactory_->Stop();
        if (MainThread_.joinable()) {
            MainThread_.join();
        }
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

private:

    IFmrCoordinator::TPtr Coordinator_;
    IFmrJobFactory::TPtr JobFactory_;
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

IFmrWorker::TPtr MakeFmrWorker(IFmrCoordinator::TPtr coordinator, IFmrJobFactory::TPtr jobFactory, const TFmrWorkerSettings& settings) {
    return MakeHolder<TFmrWorker>(coordinator, jobFactory, settings);
}

} // namespace NYql::NFmr

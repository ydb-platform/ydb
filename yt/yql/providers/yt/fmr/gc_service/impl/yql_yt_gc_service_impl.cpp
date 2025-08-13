#include "yql_yt_gc_service_impl.h"
#include <queue>
#include <util/string/builder.h>
#include <util/system/condvar.h>
#include <util/system/mutex.h>
#include <util/thread/pool.h>

namespace NYql::NFmr {

namespace {

class TFmrGcService: public IFmrGcService {

public:
    TFmrGcService(ITableDataService::TPtr tableDataService, const TGcServiceSettings& settings)
        : TableDataService_(tableDataService),
        TimeToSleepBetweenGroupDeletionRequests_(settings.TimeToSleepBetweenGroupDeletionRequests),
        GroupDeletionRequestMaxBatchSize_(settings.GroupDeletionRequestMaxBatchSize),
        MaxInflightGroupDeletionRequests_(settings.MaxInflightGroupDeletionRequests)
    {
        ThreadPool_ = CreateThreadPool(settings.WorkersNum);
        ProcessGroupDeletionRequests();
    }

    ~TFmrGcService() {
        StopGcService_ = true;
        CondVar_.BroadCast();
        ThreadPool_->Stop();
    }

    NThreading::TFuture<void> ClearGarbage(const std::vector<TString>& groupsToDelete) override {
        if (!TableDataService_) {
            return NThreading::MakeFuture();
        }
        auto promise = NThreading::NewPromise<void>();
        auto future = promise.GetFuture();
        auto registerDeletionFunc = [&, groupsToDelete = std::move(groupsToDelete), promise = std::move(promise)] () mutable  {
            with_lock(Mutex_) {
                CondVar_.Wait(Mutex_, [&] {
                    return CurGroupDeletionRequestsNum_ + groupsToDelete.size() < MaxInflightGroupDeletionRequests_ || StopGcService_;
                });
                CurGroupDeletionRequestsNum_ += groupsToDelete.size();
                ui64 cnt = GroupsToDeleteBatches_.empty() ? 0 : GroupsToDeleteBatches_.back().size();
                for (auto& group: groupsToDelete) {
                    if (cnt % GroupDeletionRequestMaxBatchSize_ == 0) {
                        GroupsToDeleteBatches_.push({});
                    }
                    GroupsToDeleteBatches_.back().emplace_back(group);
                    ++cnt;
                }
            }
            promise.SetValue();
        };
        ThreadPool_->SafeAddFunc(registerDeletionFunc);
        return future;
    }

    NThreading::TFuture<void> ClearAll() override {
        return TableDataService_ ? TableDataService_->Clear() : NThreading::MakeFuture();
    }

private:
    void ProcessGroupDeletionRequests() {
        auto runExistingGroupDeleteRequestsFunc = [&] () {
            while (!StopGcService_) {
                std::vector<TString> groupsToDelete;
                with_lock(Mutex_) {
                    if (!GroupsToDeleteBatches_.empty()) {
                        groupsToDelete = GroupsToDeleteBatches_.front();
                        GroupsToDeleteBatches_.pop();
                        CurGroupDeletionRequestsNum_ -= groupsToDelete.size();
                        CondVar_.Signal();
                    }
                }
                if (groupsToDelete.empty()) {
                    Sleep(TimeToSleepBetweenGroupDeletionRequests_);
                    continue;
                }
                TableDataService_->RegisterDeletion(groupsToDelete).GetValueSync();
                Sleep(TimeToSleepBetweenGroupDeletionRequests_);
            }
        };
        ThreadPool_->SafeAddFunc(runExistingGroupDeleteRequestsFunc);
    }

private:
    ITableDataService::TPtr TableDataService_;

    const TDuration TimeToSleepBetweenGroupDeletionRequests_;
    const ui64 GroupDeletionRequestMaxBatchSize_;
    const ui64 MaxInflightGroupDeletionRequests_;
    ui64 CurGroupDeletionRequestsNum_ = 0;

    std::queue<std::vector<TString>> GroupsToDeleteBatches_;
    TMutex Mutex_ = TMutex();
    TCondVar CondVar_;
    THolder<IThreadPool> ThreadPool_;
    std::atomic<bool> StopGcService_ = false;
};

} // namespace

IFmrGcService::TPtr MakeGcService(ITableDataService::TPtr tableDataService, const TGcServiceSettings& settings) {
    return MakeIntrusive<TFmrGcService>(tableDataService, settings);
}

} // namespace NYql::NFmr

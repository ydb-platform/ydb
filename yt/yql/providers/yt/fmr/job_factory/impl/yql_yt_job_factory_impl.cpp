#include <util/system/mutex.h>
#include <yql/essentials/utils/log/log.h>
#include <yt/yql/providers/yt/fmr/job_factory/impl/yql_yt_job_factory_impl.h>

namespace NYql::NFmr {

class TFmrJobFactory: public IFmrJobFactory {
public:
    TFmrJobFactory(const TFmrJobFactorySettings& settings)
        : NumThreads_(settings.NumThreads), Function_(settings.Function), RandomProvider_(settings.RandomProvider)
    {
        Start();
    }

    ~TFmrJobFactory() {
        Stop();
    }

    NThreading::TFuture<TTaskState::TPtr> StartJob(TTask::TPtr task, std::shared_ptr<std::atomic<bool>> cancelFlag) override {
        auto promise = NThreading::NewPromise<TTaskState::TPtr>();
        auto future = promise.GetFuture();
        auto startJobFunc = [&, task, cancelFlag, promise = std::move(promise)] () mutable {
            ETaskStatus finalTaskStatus;
            TStatistics finalTaskStatistics;
            TMaybe<TFmrError> taskErrorMessage;
            TString jobId = GetGuidAsString(RandomProvider_->GenGuid());
            try {
                TString sessionId = task->SessionId;
                TString taskId = task->TaskId;
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId, jobId);
                YQL_CLOG(DEBUG, FastMapReduce) << "Starting job with taskId " << task->TaskId;
                auto taskResult = Function_(task, cancelFlag);
                finalTaskStatus = taskResult.TaskStatus;
                finalTaskStatistics = taskResult.Stats;
            } catch (...) {
                finalTaskStatus = ETaskStatus::Failed;
                taskErrorMessage = TFmrError{.Component = EFmrComponent::Job, .ErrorMessage = CurrentExceptionMessage(), .TaskId = task->TaskId, .JobId = jobId};
            }
            promise.SetValue(MakeTaskState(finalTaskStatus, task->TaskId, taskErrorMessage, finalTaskStatistics));
        };
        ThreadPool_->SafeAddFunc(startJobFunc);
        return future;
    }

    ui64 GetMaxParallelJobCount() const override {
        return NumThreads_;
    }

    void Start() override {
        ThreadPool_ = CreateThreadPool(NumThreads_);
    }

    void Stop() override {
        ThreadPool_->Stop();
    }

private:
    THolder<IThreadPool> ThreadPool_;
    ui64 NumThreads_;
    std::function<TJobResult(TTask::TPtr, std::shared_ptr<std::atomic<bool>>)> Function_;
    const TIntrusivePtr<IRandomProvider> RandomProvider_;
};

TFmrJobFactory::TPtr MakeFmrJobFactory(const TFmrJobFactorySettings& settings) {
    return MakeIntrusive<TFmrJobFactory>(settings);
}

} // namespace NYql::NFmr

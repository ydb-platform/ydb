#include <util/system/mutex.h>
#include <yql/essentials/utils/log/log.h>
#include <yt/yql/providers/yt/fmr/job_factory/impl/yql_yt_job_factory_impl.h>

namespace NYql::NFmr {

class TFmrJobFactory: public IFmrJobFactory {
public:
    TFmrJobFactory(const TFmrJobFactorySettings& settings)
        : NumThreads_(settings.NumThreads), Function_(settings.Function)
    {
        Start();
    }

    ~TFmrJobFactory() {
        Stop();
    }

    NThreading::TFuture<TTaskResult::TPtr> StartJob(TTask::TPtr task, std::shared_ptr<std::atomic<bool>> cancelFlag) override {
        auto promise = NThreading::NewPromise<TTaskResult::TPtr>();
        auto future = promise.GetFuture();
        auto startJobFunc = [&, task, cancelFlag, promise = std::move(promise)] () mutable {
            ETaskStatus finalTaskStatus;
            TMaybe<TFmrError> taskErrorMessage;
            try {
                TString sessionId;
                if (task) {
                    sessionId = task->SessionId;
                }
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);
                YQL_CLOG(DEBUG, FastMapReduce) << "Starting job with taskId " << task->TaskId;
                finalTaskStatus = Function_(task, cancelFlag);
            } catch (const std::exception& exc) {
                finalTaskStatus = ETaskStatus::Failed;
                taskErrorMessage = TFmrError{.Component = EFmrComponent::Job, .ErrorMessage = exc.what()};
            }
            promise.SetValue(MakeTaskResult(finalTaskStatus, taskErrorMessage));
        };
        ThreadPool_->SafeAddFunc(startJobFunc);
        return future;
    }

    void Start() override {
        ThreadPool_ = CreateThreadPool(NumThreads_);
    }

    void Stop() override {
        ThreadPool_->Stop();
    }

private:
    THolder<IThreadPool> ThreadPool_;
    i32 NumThreads_;
    std::function<ETaskStatus(TTask::TPtr, std::shared_ptr<std::atomic<bool>>)> Function_;
};

TFmrJobFactory::TPtr MakeFmrJobFactory(const TFmrJobFactorySettings& settings) {
    return MakeIntrusive<TFmrJobFactory>(settings);
}

} // namespace NYql::NFmr

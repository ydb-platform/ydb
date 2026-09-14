#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/runtime/runtime.h>

#include "runtime_impl.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/executor/executor_impl.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <grpcpp/alarm.h>

#include <thread>
#include <utility>

namespace NYdb::inline Dev {

namespace {

void PostTask(THolder<IObjectInQueue> task);

class TScheduledTask final : public TThrRefBase {
public:
    TScheduledTask(THolder<IObjectInQueue> task, std::function<void(std::exception_ptr)> onError)
        : Task_(std::move(task))
        , OnError_(std::move(onError))
    {
    }

    void Start(TDeadline deadline) {
        Alarm_.Set(NRuntime::GetNetwork().CompletionQueue(), deadline, OnAlarmTag_.Prepare());
    }

private:
    void OnAlarm(bool ok) {
        if (!ok) {
            if (OnError_) {
                OnError_(std::make_exception_ptr(NThreading::TFutureException() << "Scheduled runtime task was cancelled"));
            }
            return;
        }
        try {
            PostTask(std::move(Task_));
        } catch (...) {
            if (!OnError_) {
                throw;
            }
            OnError_(std::current_exception());
        }
    }

    grpc::Alarm Alarm_;
    THolder<IObjectInQueue> Task_;
    std::function<void(std::exception_ptr)> OnError_;
    NYdbGrpc::TQueueClientFixedEvent<TScheduledTask> OnAlarmTag_ = {
        this, &TScheduledTask::OnAlarm};
};

// Runtime workers cannot depend on util singletons destroyed during process exit.
class TThreadFactory final : public IThreadFactory {
private:
    class TThread final : public IThread {
    public:
        ~TThread() override {
            if (Thread_.joinable()) {
                Thread_.detach();
            }
        }

    private:
        void DoRun(IThreadAble* task) override {
            Thread_ = std::thread([task] { task->Execute(); });
        }

        void DoJoin() noexcept override {
            if (Thread_.joinable()) {
                Thread_.join();
            }
        }

        std::thread Thread_;
    };

    IThread* DoCreate() override {
        return new TThread;
    }
};

std::shared_ptr<IThreadPool> CreateThreadPool(std::size_t threadCount, std::size_t maxQueueSize) {
    static auto* factory = new TThreadFactory;
    TThreadPoolParams params(factory);
    std::shared_ptr<IThreadPool> pool;
    if (threadCount) {
        pool = std::make_shared<TThreadPool>(params.SetBlocking(true).SetCatching(false));
    } else {
        pool = std::make_shared<TAdaptiveThreadPool>(params);
    }
    pool->Start(threadCount, maxQueueSize);
    return pool;
}

void PostTask(THolder<IObjectInQueue> task) {
    static const auto* pool = new std::shared_ptr<IThreadPool>(CreateThreadPool(0, 0));
    // MakeThrFuncObj deletes itself after execution; transfer only on admission.
    (*pool)->SafeAdd(task.Get());
    Y_UNUSED(task.Release());
}

} // namespace

NYdbGrpc::TGRpcClientLow& NRuntime::GetNetwork(std::size_t threadCount) {
    static auto* network = new NYdbGrpc::TGRpcClientLow(
        threadCount ? threadCount : NYdbGrpc::DEFAULT_NUM_THREADS);
    return *network;
}

IExecutor::TPtr NRuntime::CreateExecutor(std::size_t threadCount, std::size_t maxQueueSize) {
    return std::make_shared<TThreadPoolExecutor>(CreateThreadPool(threadCount, maxQueueSize));
}

void TRuntime::Post(TTask task) const {
    PostTask(THolder<IObjectInQueue>(MakeThrFuncObj(std::move(task))));
}

void TRuntime::Schedule(TDeadline deadline, TTask task) const {
    ScheduleTask(deadline, THolder<IObjectInQueue>(MakeThrFuncObj(std::move(task))));
}

void TRuntime::ScheduleTask(TDeadline deadline, THolder<IObjectInQueue> task,
    std::function<void(std::exception_ptr)> onError) const
{
    if (deadline <= TDeadline::Now()) {
        PostTask(std::move(task));
        return;
    }
    MakeIntrusive<TScheduledTask>(std::move(task), std::move(onError))->Start(deadline);
}

void TRuntime::Schedule(TDuration delay, TTask task) const {
    Schedule(TDeadline::AfterDuration(delay), std::move(task));
}

TRuntime& GetRuntime() {
    static auto* runtime = new TRuntime();
    return *runtime;
}

} // namespace NYdb

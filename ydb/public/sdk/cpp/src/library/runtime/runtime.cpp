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

class TScheduledTask final : public TThrRefBase {
public:
    explicit TScheduledTask(TRuntime::TTask task)
        : Task_(std::move(task))
    {
    }

    void Start(TDeadline deadline) {
        Alarm_.Set(NRuntime::GetNetwork().CompletionQueue(), deadline, OnAlarmTag_.Prepare());
    }

private:
    void OnAlarm(bool ok) {
        if (ok) {
            GetRuntime().Post(std::move(Task_));
        }
    }

    grpc::Alarm Alarm_;
    TRuntime::TTask Task_;
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

} // namespace

NYdbGrpc::TGRpcClientLow& NRuntime::GetNetwork(std::size_t threadCount) {
    static auto* network = new NYdbGrpc::TGRpcClientLow(
        threadCount ? threadCount : NYdbGrpc::DEFAULT_NUM_THREADS);
    return *network;
}

IExecutor::TPtr NRuntime::CreateExecutor(std::size_t threadCount, std::size_t maxQueueSize) {
    static auto* factory = new TThreadFactory;
    TThreadPoolParams params(factory);
    std::shared_ptr<IThreadPool> pool;
    if (threadCount) {
        pool = std::make_shared<TThreadPool>(params.SetBlocking(true).SetCatching(false));
    } else {
        pool = std::make_shared<TAdaptiveThreadPool>(params);
    }
    pool->Start(threadCount, maxQueueSize);
    return std::make_shared<TThreadPoolExecutor>(std::move(pool));
}

void TRuntime::Post(TTask task) const {
    static const auto* executor = new IExecutor::TPtr(NRuntime::CreateExecutor(0));
    (*executor)->Post(std::move(task));
}

void TRuntime::Schedule(TDeadline deadline, TTask task) const {
    if (deadline <= TDeadline::Now()) {
        Post(std::move(task));
        return;
    }
    MakeIntrusive<TScheduledTask>(std::move(task))->Start(deadline);
}

void TRuntime::Schedule(TDuration delay, TTask task) const {
    Schedule(TDeadline::AfterDuration(delay), std::move(task));
}

TRuntime& GetRuntime() {
    static auto* runtime = new TRuntime();
    return *runtime;
}

} // namespace NYdb

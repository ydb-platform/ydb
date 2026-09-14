#define INCLUDE_YDB_INTERNAL_H
#include "runtime.h"
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/exceptions/exceptions.h>
#include <ydb/public/sdk/cpp/src/library/runtime/runtime_impl.h>

#include <grpcpp/alarm.h>

#include <utility>

namespace NYdb::inline Dev {

namespace {

class TScheduledCallback final : public TThrRefBase {
public:
    explicit TScheduledCallback(std::function<void(bool)> callback)
        : Callback_(std::move(callback))
    {
    }

    void Start(TDeadline deadline, NYdbGrpc::IQueueClientContextPtr context) {
        if (context) {
            context = context->CreateContext();
        }
        auto* cq = context ? context->CompletionQueue() : NRuntime::GetNetwork().CompletionQueue();
        // Publish the cancellation context before handing the tag to the CQ.
        Context_ = context;
        Alarm_.Set(cq, deadline, OnAlarmTag_.Prepare());
        if (context) {
            context->SubscribeCancel([self = TIntrusivePtr<TScheduledCallback>(this)] {
                self->Alarm_.Cancel();
            });
        }
    }

private:
    void OnAlarm(bool ok) {
        Context_.reset();
        auto callback = std::move(Callback_);
        callback(ok);
    }

    NYdbGrpc::IQueueClientContextPtr Context_;
    grpc::Alarm Alarm_;
    std::function<void(bool)> Callback_;
    NYdbGrpc::TQueueClientFixedEvent<TScheduledCallback> OnAlarmTag_ = {
        this, &TScheduledCallback::OnAlarm};
};

} // namespace

IExecutor::TPtr TSdkRuntime::GetExecutor(
    IExecutor::TPtr executor,
    std::size_t threadCount,
    std::size_t maxQueueSize)
{
    static const auto* sharedExecutor = [&] {
        auto selected = executor ? executor : NRuntime::CreateExecutor(threadCount, maxQueueSize);
        selected->Start();
        return new IExecutor::TPtr(std::move(selected));
    }();
    if (executor && executor != *sharedExecutor) {
        throw TContractViolation("The process-wide YDB SDK executor has already been configured with another instance");
    }
    return *sharedExecutor;
}

NYdbGrpc::TGRpcClientLow& TSdkRuntime::GetNetwork(std::size_t threadCount) {
    return NRuntime::GetNetwork(threadCount);
}

void TSdkRuntime::ScheduleCallback(
    TDuration timeout,
    std::function<void(bool)> callback,
    NYdbGrpc::IQueueClientContextPtr context)
{
    ScheduleCallback(TDeadline::AfterDuration(timeout), std::move(callback), std::move(context));
}

void TSdkRuntime::ScheduleCallback(
    TDeadline deadline,
    std::function<void(bool)> callback,
    NYdbGrpc::IQueueClientContextPtr context)
{
    MakeIntrusive<TScheduledCallback>(std::move(callback))->Start(deadline, std::move(context));
}

NThreading::TFuture<bool> TSdkRuntime::ScheduleFuture(
    TDuration timeout,
    NYdbGrpc::IQueueClientContextPtr context)
{
    auto promise = NThreading::NewPromise<bool>();
    ScheduleCallback(timeout, [promise](bool ok) mutable { promise.SetValue(ok); }, std::move(context));
    return promise.GetFuture();
}

TSdkRuntime& GetSdkRuntime() {
    static TSdkRuntime* runtime = new TSdkRuntime();
    return *runtime;
}

} // namespace NYdb

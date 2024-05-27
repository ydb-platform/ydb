#pragma once

#include <ydb/library/workload/tpcc/error.h>
#include <ydb/library/workload/tpcc/stats.h>
#include <ydb/library/workload/tpcc/config.h>

#include <ydb/library/yql/utils/threading/async_queue.h>

#include <library/cpp/retry/retry.h>
#include <library/cpp/threading/future/core/future.h>
#include <library/cpp/threading/task_scheduler/task_scheduler.h>

#include <util/random/fast.h>
#include <util/thread/pool.h>
#include <library/cpp/logger/log.h>

#include <deque>
#include <random>

using namespace NYdb;
using namespace NThreading;

namespace NYdbWorkload {
namespace NTPCC {

class IProcedure;

struct TProcedureResource;

template <typename F>
class TTerminalSubtask;


class TTerminal : public IObjectInQueue {
    friend class IProcedure;
    friend class TSchedulerTerminalTask;
    friend class TSchedulerTerminalSubtask;
    template <typename F> friend class TTerminalSubtask;
public:
    TTerminal(const TRunParams& params, i32 warehouseId, i32 terminalId, TLog& log,
              std::shared_ptr<NTable::TTableClient> tableClient,
              std::shared_ptr<TTaskScheduler> taskScheduler, 
              std::shared_ptr<TThreadPool> threadPool,
              std::shared_ptr<std::deque<TTerminal*>> readyTerminals,
              TMutex& readyQueueMutex,
              i32 seed);

    ~TTerminal() = default;

    void Process(void* threadSpecificResource) override;

    void Start();

    const std::vector<std::unique_ptr<TStatistics>>& GetStatistics();

    void NotifyAboutWorkloadStart();
    
    void NotifyAboutWorkloadEnd();

    i32 GetTerminalId();

    template <typename F>
    TFuture<TFutureType<TFunctionResult<F>>> ExecuteAndProcessingDataQuery(
        NTable::TDataQuery& dataQuery, TParams&& params, TMaybe<NTable::TTransaction>& transaction,
        F&& processing, TString funcName = ""
    ) {
        using resultType = TFutureType<TFunctionResult<F>>;

        if (transaction.Empty() || !transaction->IsActive()) {
            return NThreading::MakeErrorFuture<resultType>(std::make_exception_ptr(
                yexception() << funcName << ": Empty or not active transaction"
            ));
        }

        NThreading::TFuture<resultType> res = ApplyByThreadPool(
            dataQuery.Execute(
                NTable::TTxControl::Tx(*transaction),
                std::move(params)
            ),
            std::forward<F>(processing),
            funcName
        );
        
        return res;
    }

    // The processing function is executed in the thread pool.
    template <typename F, typename P>
    TFuture<TFutureType<std::invoke_result_t<F, TFuture<P>>>> ApplyByThreadPool(
        const TFuture<P>& future, F&& processing, TString funcName = ""
    ) {
        auto promise = NewPromise<TFutureType<std::invoke_result_t<F, TFuture<P>>>>();

        if (future.HasException()) {
            try {
                future.GetValue();
            } catch (...) {
                promise.SetException(std::current_exception());
            }
            return promise.GetFuture();
        }

        return future.Apply([=, this](const TFuture<P>& future) mutable {
            auto task = THolder(new TTerminalSubtask([=] () mutable { return processing(future); }, promise, funcName));

            AddSubtask(std::move(task));

            return promise.GetFuture();
        });
    }

private:
    enum ERetryType {
        NOT_RETRIABLE,
        INSTANT_RETRY,
        FAST_RETRY,
        SLOW_RETRY
    };

    TInstant GetPreExecutionWait();
    TInstant GetPostExecutionWait();

    void AddToScheduler(TInstant expire);
    void AddToThreadPool();

    void SuccessProcedure();
    void ProcedureFailed(EStatus status);
    void RetryProcedure(TInstant expire, EStatus status);
    void FailWithMessage(TString&& message, EStatus status);
    void NextProcedure();
    void FinishAndGetNextProcedure();

    ERetryType GetRetryType(EStatus status);

    TMaybe<TInstant> HandleException(EStatus ex);
    
    void GetProcedureResource();
    void GetNewOrderResource();
    void GetPaymentResource();
    void GetDeliveryResource();
    void GetOrderStatusResource();
    void GetStockLevelResource();


    template <typename F>
    void AddSubtask(THolder<TTerminalSubtask<F>>&& subtask) const {
        try {
            if (WorkloadEnded.load()) {
                return;
            }

            ThreadPool->SafeAddAndOwn(std::move(subtask));
        } catch (const std::exception& ex) {
            throw yexception() << "Couldn't add task to ThreadPool: " << ex.what();
        } catch (...) {
            throw yexception() << "Couldn't add task to ThreadPool: ";
        }
    }

    TRunParams Params;

    TLog& Log;

    std::atomic_bool WorkloadStarted = false;
    std::atomic_bool WorkloadEnded = false;

    TInstant StartTime;
    std::vector<std::unique_ptr<TStatistics>> Statistics;
    
    ui32 RetryCount = 0;
    TRetryOptions FastRetryOption;
    TRetryOptions SlowRetryOption;

    std::shared_ptr<IProcedure> Procedure;
    std::shared_ptr<TProcedureResource> Resource;
    std::shared_ptr<NTable::TTableClient> TableClient;

    i32 WarehouseId;
    i32 TerminalId;

    std::shared_ptr<TThreadPool> ThreadPool;
    std::shared_ptr<TTaskScheduler> Scheduler;
    std::shared_ptr<std::deque<TTerminal*>> ReadyTerminals;
    TMutex& ReadyQueueMutex;

    TFastRng32 Rng;

    std::uniform_int_distribution<i32> DistrictGen;
    std::uniform_int_distribution<i32> UniformGen;
    std::uniform_int_distribution<i32> CustomerGen;
};


template <typename F>
class TTerminalSubtask : public IObjectInQueue {
public:
    using ResultType = TFutureType<TFunctionResult<F>>;

    TTerminalSubtask(F&& func,
                     const NThreading::TPromise<ResultType>& promise,
                     const TString& message = "")
        : Func(std::forward<F>(func))
        , Message(message + (!message.empty() ? ": " : ""))
        , Promise(promise)
    {
    }

    void Process(void*) override {
        NImpl::SetValue(Promise, std::forward<F>(Func));
    }

private:
    F Func;
    TString Message;

    NThreading::TPromise<ResultType> Promise;
};


class TSchedulerTerminalTask : public TTaskScheduler::ITask {
public:
    TSchedulerTerminalTask(TTerminal& terminal) 
        : Terminal(terminal) {}

    ~TSchedulerTerminalTask() = default;

    TInstant Process() override {
        Terminal.AddToThreadPool();
        return TInstant::Max();
    }

private:
    TTerminal& Terminal;
};


class TSchedulerTerminalSubtask : public TTaskScheduler::ITask {
public:
    TSchedulerTerminalSubtask(TPromise<void>& promise)
        : Promise(promise) {}

    TInstant Process() override {
        Promise.SetValue();
        return TInstant::Max();
    }

private:
    TPromise<void> Promise;
};


}
}
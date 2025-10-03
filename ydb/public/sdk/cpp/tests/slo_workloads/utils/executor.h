#pragma once

#include "utils.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <util/datetime/base.h>
#include <util/string/builder.h>
#include <util/system/sem.h>
#include <util/system/spinlock.h>
#include <util/system/thread.h>
#include <util/thread/pool.h>

#include <list>

extern const TDuration WaitTimeout;

// Debug use only
extern std::atomic<std::uint64_t> ReadPromises;

template<typename T>
class TTracedPromise : public NThreading::TPromise<T> {
public:
    TTracedPromise(NThreading::TPromise<T> promise, std::atomic<std::uint64_t>* counter)
        : NThreading::TPromise<T>(promise)
        , Counter(counter)
    {
        Counter->fetch_add(1);
    }

    TTracedPromise(const TTracedPromise& other)
        : NThreading::TPromise<T>(other)
        , Counter(other.Counter)
    {
        other.Counter = nullptr;
    }

    TTracedPromise& operator=(const TTracedPromise&) = delete;

    ~TTracedPromise() {
        if (Counter) {
            Counter->fetch_sub(1);
        }
    }

private:
    mutable std::atomic<std::uint64_t>* Counter;
};

class TInsistentClient {
public:
    struct TDelayedCallback;

    struct TCheckedIterator {
        std::list<TDelayedCallback>::iterator RealIter;
        bool Valid = true;
    };

    struct TOperationContext : public TThrRefBase {
        bool Finished = false;
        TAdaptiveLock Lock;
        TCheckedIterator RetryIter;
        TCheckedIterator TimeoutIter;
    };

    struct TDelayedCallback {
        TInstant ExecucionTime;
        std::function<void()> Callback;
        std::shared_ptr<TOperationContext> context;
    };

    TInsistentClient(const TCommonOptions& opts);
    ~TInsistentClient();
    void Report(TStringBuilder& out) const;
    TAsyncFinalStatus ExecuteWithRetry(const NYdb::NTable::TTableClient::TOperationFunc& operation);
    std::uint64_t GetActiveSessions() const;

private:
    void ClearContext(std::shared_ptr<TOperationContext>& context);
    void RemoveRetryIter(std::shared_ptr<TOperationContext>& context);
    void RemoveTimeoutIter(std::shared_ptr<TOperationContext>& context);

    TThreadPool CallbackQueue;
    NYdb::NTable::TTableClient Client;
    std::uint32_t ClientMaxRetries;
    TDuration Timeout;
    TDuration RetryTimeout;
    TDuration SessionTimeout;
    TAdaptiveLock CallbacksLock;
    std::unique_ptr<IThreadFactory::IThread> WorkThread;
    TManualEvent ShouldStop;
    std::list<TDelayedCallback> RetryCallbacks;
    std::list<TDelayedCallback> TimeoutCallbacks;
    bool UseApplicationTimeout;
    bool SendPreventiveRequest;

    // Ok received on the First try
    std::atomic<std::uint64_t> CounterFOk = 0;
    // Ok received on the Second try
    std::atomic<std::uint64_t> CounterSOk = 0;
    // First try launches (= total)
    std::atomic<std::uint64_t> CounterFStart = 0;
    // Second try launches
    std::atomic<std::uint64_t> CounterSStart = 0;
};

class TExecutor {
public:
    enum EMode {
        ModeBlocking,
        ModeNonBlocking
    };

    struct TErrorData {
        std::string Message;
        std::uint64_t Counter;
    };

    TExecutor(const TCommonOptions& opts, TStat& stats, EMode mode = ModeNonBlocking);
    virtual ~TExecutor();
    virtual bool Execute(const NYdb::NTable::TTableClient::TOperationFunc& func);

    void Start(TInstant deadline);
    // Abort all waiting jobs and do not accept new ones
    void Stop();
    // Wait for all jobs to finish
    void Wait();
    // Signal that there will be no more new jobs
    void StopAndWait();
    std::uint32_t StopAndWait(TDuration waitTimeout);
    std::uint32_t Wait(TDuration waitTimeout);
    bool IsStopped();
    void Finish();
    std::uint32_t GetTotal() const;
    void Report(TStringBuilder& out) const;

protected:
    void DecrementInfly();
    void DecrementWaiting();
    // Checks if all jobs are done
    void CheckForFinish();
    void UpdateStats();
    void ReportStats();
    void CheckForError(const NYdb::TStatus& status);

    const TCommonOptions& Opts;
    TStat& Stats;
    TInsistentClient InsistentClient;
    TFastSemaphore Semaphore;
    EMode Mode;
    std::unique_ptr<TThreadPool> InputQueue;
    std::unique_ptr<IThreadFactory::IThread> MetricsPusherThread;
    std::atomic<bool> ShouldStop = false;
    std::atomic<std::uint64_t> Total = 0;
    std::atomic<std::uint64_t> Succeeded = 0;
    std::atomic<std::uint64_t> Failed = 0;
    TAdaptiveLock ErrorLock;
    std::unordered_map<NYdb::EStatus, TErrorData> Errors;
    TAdaptiveLock Lock;
    // Jobs put in queue but haven't started yet
    std::uint32_t Waiting = 0;
    // Jobs started executing
    std::uint32_t Infly = 0;
    std::uint32_t MaxInfly = 0;
    bool AllJobsLaunched = false;
    TManualEvent AllJobsFinished;
    TInstant Deadline;
    // Last second we reported Infly
    std::uint64_t LastReportSec = 0;
    // Max infly for current second
    std::uint64_t MaxSecInfly = 0;
    // Max Active sessions for current second
    std::uint64_t MaxSecSessions = 0;

    //(Debug usage) Monitoring the number of jobs waiting for rps limiter
    std::size_t InProgressCount = 0;
    std::size_t InProgressSum = 0;
    std::uint64_t MaxSecReadPromises = 0;
    std::uint64_t MaxSecExecutorPromises = 0;
};

class TExecutorWithRetry : public TExecutor {
public:
    struct TRetryContext {
        TRetryContext(TStat& stat)
            : LifeTimeStat(stat.CreateStatUnit())
            , PerRequestStat(stat.CreateStatUnit())
        {}

        TStatUnit LifeTimeStat;
        TStatUnit PerRequestStat;
        std::unique_ptr<std::function<void(const TAsyncFinalStatus& resultFuture)>> HandleStatusFunc;
        std::atomic<std::uint64_t> Retries = 0;
    };

    TExecutorWithRetry(const TCommonOptions& opts, TStat& stats);
    bool Execute(const NYdb::NTable::TTableClient::TOperationFunc& func) override;
};

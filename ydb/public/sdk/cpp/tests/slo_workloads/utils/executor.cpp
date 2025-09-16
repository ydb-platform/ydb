#include "executor.h"

const TDuration WaitTimeout = TDuration::Seconds(10);

// Debug use only:
std::atomic<ui64> ReadPromises = 0;
std::atomic<ui64> ExecutorPromises = 0;


TInsistentClient::TInsistentClient(const TCommonOptions& opts)
    : Client(
        opts.DatabaseOptions.Driver,
        NYdb::NTable::TClientSettings()
            .SessionPoolSettings(NYdb::NTable::TSessionPoolSettings().MaxActiveSessions(opts.MaxInfly))
            .MinSessionCV(8)
            .AllowRequestMigration(true)
    )
    , ClientMaxRetries(opts.MaxRetries)
    , Timeout(opts.ReactionTime)
    , RetryTimeout(Timeout / 2)
    , SessionTimeout(Timeout + ReactionTimeDelay)
    , UseApplicationTimeout(opts.UseApplicationTimeout)
    , SendPreventiveRequest(opts.SendPreventiveRequest)
{
    if (UseApplicationTimeout || SendPreventiveRequest) {
        CallbackQueue.Start(opts.MaxCallbackThreads);
        // Thread that executes timeout callbacks
        auto threadFunc = [this]() {
            TDuration timeToSleep;
            while (!ShouldStop.WaitT(timeToSleep)) {
                TInstant wakeupTime;
                TInstant now;
                with_lock(CallbacksLock) {
                    now = TInstant::Now();
                    while (!TimeoutCallbacks.empty() && now >= TimeoutCallbacks.front().ExecucionTime) {
                        Y_UNUSED(CallbackQueue.AddFunc(TimeoutCallbacks.front().Callback));
                        RemoveTimeoutIter(TimeoutCallbacks.front().context);
                    }
                    while (!RetryCallbacks.empty() && now >= RetryCallbacks.front().ExecucionTime) {
                        Y_UNUSED(CallbackQueue.AddFunc(RetryCallbacks.front().Callback));
                        RemoveRetryIter(RetryCallbacks.front().context);
                    }
                    if (RetryCallbacks.empty()) {
                        wakeupTime = now + RetryTimeout;
                    } else {
                        wakeupTime = RetryCallbacks.front().ExecucionTime;
                    }
                    if (!TimeoutCallbacks.empty()) {
                        wakeupTime = Min(wakeupTime, TimeoutCallbacks.front().ExecucionTime);
                    }
                }
                timeToSleep = wakeupTime - now;
            }
        };
        WorkThread.reset(SystemThreadFactory()->Run(threadFunc).Release());
    }
}

TInsistentClient::~TInsistentClient() {
    ShouldStop.Signal();
    if (UseApplicationTimeout || SendPreventiveRequest) {
        if (WorkThread) {
            WorkThread->Join();
        } else {
            Cerr << (TStringBuilder() << "TInsistentClient::~TINsistentClient Error: WorkThread is not running." << Endl);
        }
        CallbackQueue.Stop();
    }
    Client.Stop().Wait(WaitTimeout);
}

void TInsistentClient::Report(TStringBuilder& out) const {
    out << "Client retries sent: total " << CounterSStart.load()
        << ", successful " << CounterSOk.load() << Endl;
}

ui64 TInsistentClient::GetActiveSessions() const {
    i64 sessions = Client.GetActiveSessionCount();
    return static_cast<ui64>(sessions);
}

void TInsistentClient::ClearContext(std::shared_ptr<TOperationContext>& context) {
    if (SendPreventiveRequest) {
        RemoveRetryIter(context);
    }
    if (UseApplicationTimeout) {
        RemoveTimeoutIter(context);
    }
}

void TInsistentClient::RemoveRetryIter(std::shared_ptr<TOperationContext>& context) {
    if (context->RetryIter.Valid) {
        context->RetryIter.Valid = false;
        RetryCallbacks.erase(context->RetryIter.RealIter);
    }
}

void TInsistentClient::RemoveTimeoutIter(std::shared_ptr<TOperationContext>& context) {
    if (context->TimeoutIter.Valid) {
        context->TimeoutIter.Valid = false;
        TimeoutCallbacks.erase(context->TimeoutIter.RealIter);
    }
}

TAsyncFinalStatus TInsistentClient::ExecuteWithRetry(const NYdb::NTable::TTableClient::TOperationFunc& operation) {
    //auto promise = NThreading::NewPromise<NYdb::TStatus>();
    TTracedPromise<TFinalStatus> promise = TTracedPromise<TFinalStatus>(
        NThreading::NewPromise<TFinalStatus>(),
        &ExecutorPromises
    );
    std::shared_ptr<TOperationContext> context = std::make_shared<TOperationContext>();

    auto launchOperation = [this, operation, promise, context](bool firstTime) mutable {
        with_lock(context->Lock) {
            if (context->Finished) {
                return;
            }
        }
        auto callback = [promise, context, firstTime, this](const NYdb::TAsyncStatus& future) mutable {
            Y_ABORT_UNLESS(future.HasValue());
            // Not setting promise under lock to avoid deadlock
            bool firstCallback = false;
            with_lock(context->Lock) {
                if (!context->Finished) {
                    context->Finished = true;
                    firstCallback = true;
                }
            }
            if (firstCallback) {
                promise.SetValue(future.GetValue());
                with_lock(CallbacksLock) {
                    if (firstTime) {
                        CounterFOk.fetch_add(1);
                    } else {
                        CounterSOk.fetch_add(1);
                    }
                    ClearContext(context);
                }
            }
        };
        if (firstTime) {
            CounterFStart.fetch_add(1);
        } else {
            CounterSStart.fetch_add(1);
        }
        NYdb::NTable::TRetryOperationSettings settings;
        settings.MaxRetries(ClientMaxRetries);
        settings.GetSessionClientTimeout(SessionTimeout);
        auto future = Client.RetryOperation(operation, settings);
        future.Subscribe(std::move(callback));
    };

    with_lock(CallbacksLock) {
        TInstant now = TInstant::Now();

        if (SendPreventiveRequest) {
            auto onRetryTimeout = [launchOperation]() mutable {
                launchOperation(false);
            };

            RetryCallbacks.push_back({ now + RetryTimeout, onRetryTimeout, context });
            context->RetryIter = { --RetryCallbacks.end() };
        }

        if (UseApplicationTimeout) {
            auto onTimeout = [this, promise, context]() mutable {
                // Not setting promise under lock to avoid deadlock
                bool firstCallback = false;
                with_lock(context->Lock) {
                    if (!context->Finished) {
                        context->Finished = true;
                        firstCallback = true;
                    }
                }
                if (firstCallback) {
                    promise.SetValue(TFinalStatus());
                    with_lock(CallbacksLock) {
                        ClearContext(context);
                    }
                }
            };

            TimeoutCallbacks.push_back({ now + Timeout, onTimeout, context });
            context->TimeoutIter = { --TimeoutCallbacks.end() };
        }
    }

    launchOperation(true);

    return promise.GetFuture();
}

TExecutor::TExecutor(const TCommonOptions& opts, TStat& stats, EMode mode)
    : Opts(opts)
    , Stats(stats)
    , InsistentClient(opts)
    , Semaphore(opts.MaxInputThreads)
    , Mode(mode)
{
    InputQueue = std::make_unique<TThreadPool>();
    InputQueue->Start(opts.MaxInputThreads);

    auto threadFunc = [this]() {
        TInstant wakeupTime = TInstant::Now();
        while (!ShouldStop.load()) {
            with_lock(Lock) {
                UpdateStats();
            }
            wakeupTime += TDuration::Seconds(1);
            SleepUntil(wakeupTime);
        }
    };
    SolomonPusherThread.reset(SystemThreadFactory()->Run(threadFunc).Release());
}

TExecutor::~TExecutor() {
    ui32 infly = StopAndWait(WaitTimeout);
    if (SolomonPusherThread) {
        SolomonPusherThread->Join();
    } else {
        Cerr << (TStringBuilder() << "TExecutor::~TExecutor Error: SolomonPusherThread is not running." << Endl);
    }
    if (infly) {
        Cerr << "Warning: destroying TExecutor while having " << infly << " infly requests." << Endl;
    }
}

namespace {
    class TSemaphoreWrapper : public TThrRefBase {
    public:
        TSemaphoreWrapper(TFastSemaphore& semaphore)
            : Semaphore(semaphore)
        {}

        ~TSemaphoreWrapper() {
            if (Acquired) {
                Semaphore.Release();
            }
        }

        void Acquire() {
            Semaphore.Acquire();
            Acquired = true;
        }

    private:
        TFastSemaphore& Semaphore;
        bool Acquired = false;
    };
}

bool TExecutor::Execute(const NYdb::NTable::TTableClient::TOperationFunc& func) {
    TIntrusivePtr<TSemaphoreWrapper> SemaphoreWrapper;
    if (Mode == ModeBlocking) {
        SemaphoreWrapper = new TSemaphoreWrapper(Semaphore);
    }
    auto threadFunc = [this, func, SemaphoreWrapper]() {
        if (IsStopped()) {
            DecrementWaiting();
            return;
        }

        with_lock(Lock) {
            --Waiting;
            if (Infly < Opts.MaxInfly) {
                ++Infly;
                UpdateStats();
                if (Infly > MaxInfly) {
                    MaxInfly = Infly;
                }
            } else {
                Stats.ReportMaxInfly();
                UpdateStats();
                return;
            }
        }

        TStatUnit stat = Stats.CreateStatUnit();

        auto future = InsistentClient.ExecuteWithRetry(func);
        future.Subscribe([this, stat, SemaphoreWrapper](const TAsyncFinalStatus& future) mutable {
            Y_ABORT_UNLESS(future.HasValue());
            TFinalStatus resultStatus = future.GetValue();
            Stats.Report(stat, resultStatus);
            if (resultStatus) {
                CheckForError(*resultStatus);
            }
            DecrementInfly();
        });
    };

    if (IsStopped()) {
        return false;
    }

    bool CanLaunchJob = false;

    with_lock(Lock) {
        if (!AllJobsLaunched) {
            CanLaunchJob = true;
            ++Waiting;
        }
    }

    if (CanLaunchJob) {
        if (Mode == ModeBlocking) {
            SemaphoreWrapper->Acquire();
        }
        if (!InputQueue->AddFunc(threadFunc)) {
            DecrementWaiting();
        }
    }
    ++InProgressCount;
    InProgressSum += InputQueue->Size();
    return true;
}

void TExecutor::Start(TInstant deadline) {
    Deadline = deadline;
}

void TExecutor::Stop() {
    if (!IsStopped()) {
        ShouldStop.store(true);
        Finish();
    }
}

void TExecutor::Wait() {
    AllJobsFinished.WaitI();
    InputQueue->Stop();
}

ui32 TExecutor::Wait(TDuration waitTimeout) {
    AllJobsFinished.WaitT(waitTimeout);
    InputQueue->Stop();
    return Infly;
}

void TExecutor::StopAndWait() {
    Stop();
    Wait();
}

ui32 TExecutor::StopAndWait(TDuration waitTimeout) {
    Stop();
    return Wait(waitTimeout);
}

bool TExecutor::IsStopped() {
    return ShouldStop.load();
}

void TExecutor::Finish() {
    // Stats.UpdateSessionStats(InsistentClient.GetSessionStats());
    with_lock(Lock) {
        if (!AllJobsLaunched) {
            AllJobsLaunched = true;
            CheckForFinish();
        }
    }
}

void TExecutor::UpdateStats() {
    if (Infly > MaxSecInfly) {
        MaxSecInfly = Infly;
    }
    ui64 activeSessions = InsistentClient.GetActiveSessions();
    if (activeSessions > MaxSecSessions) {
        MaxSecSessions = activeSessions;
    }

    // Debug use only:
    ui64 readPromises = ReadPromises.load();
    if (readPromises > MaxSecReadPromises) {
        MaxSecReadPromises = readPromises;
    }
    ui64 executorPromises = ExecutorPromises.load();
    if (executorPromises > MaxSecExecutorPromises) {
        MaxSecExecutorPromises = executorPromises;
    }
    ReportStats();
}

void TExecutor::ReportStats() {
    TInstant now = TInstant::Now();
    if (now.Seconds() > LastReportSec) {
        Stats.ReportStats(MaxSecInfly, MaxSecSessions, MaxSecReadPromises, MaxSecExecutorPromises);
        MaxSecInfly = 0;
        MaxSecSessions = 0;
        MaxSecReadPromises = 0;
        MaxSecExecutorPromises = 0;
        LastReportSec = now.Seconds();
    }
}

void TExecutor::DecrementInfly() {
    with_lock(Lock) {
        --Infly;
        UpdateStats();
        if (!Infly) {
            CheckForFinish();
        }
    }
}

void TExecutor::DecrementWaiting() {
    with_lock(Lock) {
        --Waiting;
        CheckForFinish();
    }
}

void TExecutor::CheckForFinish() {
    if (AllJobsLaunched && !Infly && !Waiting) {
        AllJobsFinished.Signal();
    }
}

void TExecutor::CheckForError(const NYdb::TStatus& status) {
    if (!status.IsSuccess()) {
        with_lock(ErrorLock) {
            auto it = Errors.find(status.GetStatus());
            if (it == Errors.end()) {
                Errors.insert({ status.GetStatus() , { status.GetIssues().ToString() , 1 } });
            } else {
                ++it->second.Counter;
            }
        }
    }
}

void TExecutor::Report(TStringBuilder& out) const {
    out << MaxInfly << " maxInfly" << Endl
        << (InProgressCount ? InProgressSum / InProgressCount : 0) << " average Inprogress threads in input queue" << Endl;
    InsistentClient.Report(out);
    with_lock(ErrorLock) {
        if (Errors.size()) {
            out << "Errors:" << Endl;
            for (auto& error : Errors) {
                out << error.second.Counter << " errors with status " << error.first << ": " << error.second.Message << Endl;
            }
        }
    }
}


TExecutorWithRetry::TExecutorWithRetry(const TCommonOptions& opts, TStat& stats)
    : TExecutor(opts, stats)
{}

bool TExecutorWithRetry::Execute(const NYdb::NTable::TTableClient::TOperationFunc& func) {
    auto threadFunc = [this, func]() {
        if (IsStopped()) {
            DecrementWaiting();
            return;
        }

        with_lock(Lock) {
            --Waiting;
            if (Infly < Opts.MaxInfly) {
                ++Infly;
                if (Infly > MaxInfly) {
                    MaxInfly = Infly;
                }
                UpdateStats();
            } else {
                Stats.ReportMaxInfly();
                UpdateStats();
                return;
            }
        }

        std::shared_ptr<TRetryContext> context = std::make_shared<TRetryContext>(Stats);

        auto executeOperation = [this, func]() {
            return InsistentClient.ExecuteWithRetry(func);
        };

        context->HandleStatusFunc = std::make_unique<std::function<void(const TAsyncFinalStatus& resultFuture)>>(
            [this, executeOperation, context](const TAsyncFinalStatus& future) mutable {
            Y_ABORT_UNLESS(future.HasValue());
            TFinalStatus resultStatus = future.GetValue();
            if (resultStatus) {
                // Reply received
                CheckForError(*resultStatus);
                if (resultStatus->IsSuccess()) {
                    //Ok received
                    Stats.Report(context->LifeTimeStat, resultStatus->GetStatus()); 
                    DecrementInfly();
                    context->HandleStatusFunc.reset();
                    return;
                }
            }
            if (IsStopped() || TInstant::Now() - context->LifeTimeStat.Start > GlobalTimeout) {
                // Application stopped working or global timeout reached. Ok reply hasn't received yet
                Stats.Report(context->LifeTimeStat, TInnerStatus::StatusNotFinished);
                DecrementInfly();
                context->HandleStatusFunc.reset();
                return;
            }
            Stats.Report(context->PerRequestStat, resultStatus);
            context->PerRequestStat = Stats.CreateStatUnit();
            // Retrying:
            executeOperation().Subscribe(*context->HandleStatusFunc);
        });

        context->Retries.fetch_add(1);
        Y_ABORT_UNLESS(context->Retries.load() < 500, "Too much retries");

        executeOperation().Subscribe(*context->HandleStatusFunc);
    };

    if (IsStopped()) {
        return false;
    }

    bool CanLaunchJob = false;

    with_lock(Lock) {
        if (!AllJobsLaunched) {
            CanLaunchJob = true;
            ++Waiting;
        }
    }

    if (CanLaunchJob) {
        if (!InputQueue->AddFunc(threadFunc)) {
            DecrementWaiting();
        }
    }
    ++InProgressCount;
    InProgressSum += InputQueue->Size();
    return true;
}

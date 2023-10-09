#include "executor.h"

#include "thread_extra.h"
#include "what_thread_does.h"
#include "what_thread_does_guard.h"

#include <util/generic/utility.h>
#include <util/random/random.h>
#include <util/stream/str.h>
#include <util/system/tls.h>
#include <util/system/yassert.h>

#include <array>

using namespace NActor;
using namespace NActor::NPrivate;

namespace {
    struct THistoryInternal {
        struct TRecord {
            TAtomic MaxQueueSize;

            TRecord()
                : MaxQueueSize()
            {
            }

            TExecutorHistory::THistoryRecord Capture() {
                TExecutorHistory::THistoryRecord r;
                r.MaxQueueSize = AtomicGet(MaxQueueSize);
                return r;
            }
        };

        ui64 Start;
        ui64 LastTime;

        std::array<TRecord, 3600> Records;

        THistoryInternal() {
            Start = TInstant::Now().Seconds();
            LastTime = Start - 1;
        }

        TRecord& GetRecordForTime(ui64 time) {
            return Records[time % Records.size()];
        }

        TRecord& GetNowRecord(ui64 now) {
            for (ui64 t = LastTime + 1; t <= now; ++t) {
                GetRecordForTime(t) = TRecord();
            }
            LastTime = now;
            return GetRecordForTime(now);
        }

        TExecutorHistory Capture() {
            TExecutorHistory history;
            ui64 now = TInstant::Now().Seconds();
            ui64 lastHistoryRecord = now - 1;
            ui32 historySize = Min<ui32>(lastHistoryRecord - Start, Records.size() - 1);
            history.HistoryRecords.resize(historySize);
            for (ui32 i = 0; i < historySize; ++i) {
                history.HistoryRecords[i] = GetRecordForTime(lastHistoryRecord - historySize + i).Capture();
            }
            history.LastHistoryRecordSecond = lastHistoryRecord;
            return history;
        }
    };

}

Y_POD_STATIC_THREAD(TExecutor*)
ThreadCurrentExecutor;

static const char* NoLocation = "nowhere";

struct TExecutorWorkerThreadLocalData {
    ui32 MaxQueueSize;
};

static TExecutorWorkerThreadLocalData WorkerNoThreadLocalData;
Y_POD_STATIC_THREAD(TExecutorWorkerThreadLocalData)
WorkerThreadLocalData;

namespace NActor {
    struct TExecutorWorker {
        TExecutor* const Executor;
        TThread Thread;
        const char** WhatThreadDoesLocation;
        TExecutorWorkerThreadLocalData* ThreadLocalData;

        TExecutorWorker(TExecutor* executor)
            : Executor(executor)
            , Thread(RunThreadProc, this)
            , WhatThreadDoesLocation(&NoLocation)
            , ThreadLocalData(&::WorkerNoThreadLocalData)
        {
            Thread.Start();
        }

        void Run() {
            WhatThreadDoesLocation = ::WhatThreadDoesLocation();
            AtomicSet(ThreadLocalData, &::WorkerThreadLocalData);
            WHAT_THREAD_DOES_PUSH_POP_CURRENT_FUNC();
            Executor->RunWorker();
        }

        static void* RunThreadProc(void* thiz0) {
            TExecutorWorker* thiz = (TExecutorWorker*)thiz0;
            thiz->Run();
            return nullptr;
        }
    };

    struct TExecutor::TImpl {
        TExecutor* const Executor;
        THistoryInternal History;

        TSystemEvent HelperStopSignal;
        TThread HelperThread;

        TImpl(TExecutor* executor)
            : Executor(executor)
            , HelperThread(HelperThreadProc, this)
        {
        }

        void RunHelper() {
            ui64 nowSeconds = TInstant::Now().Seconds();
            for (;;) {
                TInstant nextStop = TInstant::Seconds(nowSeconds + 1) + TDuration::MilliSeconds(RandomNumber<ui32>(1000));

                if (HelperStopSignal.WaitD(nextStop)) {
                    return;
                }

                nowSeconds = nextStop.Seconds();

                THistoryInternal::TRecord& record = History.GetNowRecord(nowSeconds);

                ui32 maxQueueSize = Executor->GetMaxQueueSizeAndClear();
                if (maxQueueSize > record.MaxQueueSize) {
                    AtomicSet(record.MaxQueueSize, maxQueueSize);
                }
            }
        }

        static void* HelperThreadProc(void* impl0) {
            TImpl* impl = (TImpl*)impl0;
            impl->RunHelper();
            return nullptr;
        }
    };

}

static TExecutor::TConfig MakeConfig(unsigned workerCount) {
    TExecutor::TConfig config;
    config.WorkerCount = workerCount;
    return config;
}

TExecutor::TExecutor(size_t workerCount)
    : Config(MakeConfig(workerCount))
{
    Init();
}

TExecutor::TExecutor(const TExecutor::TConfig& config)
    : Config(config)
{
    Init();
}

void TExecutor::Init() {
    Impl.Reset(new TImpl(this));

    AtomicSet(ExitWorkers, 0);

    Y_ABORT_UNLESS(Config.WorkerCount > 0);

    for (size_t i = 0; i < Config.WorkerCount; i++) {
        WorkerThreads.push_back(new TExecutorWorker(this));
    }

    Impl->HelperThread.Start();
}

TExecutor::~TExecutor() {
    Stop();
}

void TExecutor::Stop() {
    AtomicSet(ExitWorkers, 1);

    Impl->HelperStopSignal.Signal();
    Impl->HelperThread.Join();

    {
        TWhatThreadDoesAcquireGuard<TMutex> guard(WorkMutex, "executor: acquiring lock for Stop");
        WorkAvailable.BroadCast();
    }

    for (size_t i = 0; i < WorkerThreads.size(); i++) {
        WorkerThreads[i]->Thread.Join();
    }

    // TODO: make queue empty at this point
    ProcessWorkQueueHere();
}

void TExecutor::EnqueueWork(TArrayRef<IWorkItem* const> wis) {
    if (wis.empty())
        return;

    if (Y_UNLIKELY(AtomicGet(ExitWorkers) != 0)) {
        Y_ABORT_UNLESS(WorkItems.Empty(), "executor %s: cannot add tasks after queue shutdown", Config.Name);
    }

    TWhatThreadDoesPushPop pp("executor: EnqueueWork");

    WorkItems.PushAll(wis);

    {
        if (wis.size() == 1) {
            TWhatThreadDoesAcquireGuard<TMutex> g(WorkMutex, "executor: acquiring lock for EnqueueWork");
            WorkAvailable.Signal();
        } else {
            TWhatThreadDoesAcquireGuard<TMutex> g(WorkMutex, "executor: acquiring lock for EnqueueWork");
            WorkAvailable.BroadCast();
        }
    }
}

size_t TExecutor::GetWorkQueueSize() const {
    return WorkItems.Size();
}

using namespace NTSAN;

ui32 TExecutor::GetMaxQueueSizeAndClear() const {
    ui32 max = 0;
    for (unsigned i = 0; i < WorkerThreads.size(); ++i) {
        TExecutorWorkerThreadLocalData* wtls = AtomicGet(WorkerThreads[i]->ThreadLocalData);
        max = Max<ui32>(max, RelaxedLoad(&wtls->MaxQueueSize));
        RelaxedStore<ui32>(&wtls->MaxQueueSize, 0);
    }
    return max;
}

TString TExecutor::GetStatus() const {
    return GetStatusRecordInternal().Status;
}

TString TExecutor::GetStatusSingleLine() const {
    TStringStream ss;
    ss << "work items: " << GetWorkQueueSize();
    return ss.Str();
}

TExecutorStatus TExecutor::GetStatusRecordInternal() const {
    TExecutorStatus r;

    r.WorkQueueSize = GetWorkQueueSize();

    {
        TStringStream ss;
        ss << "work items:     " << GetWorkQueueSize() << "\n";
        ss << "workers:\n";
        for (unsigned i = 0; i < WorkerThreads.size(); ++i) {
            ss << "-- " << AtomicGet(*AtomicGet(WorkerThreads[i]->WhatThreadDoesLocation)) << "\n";
        }
        r.Status = ss.Str();
    }

    r.History = Impl->History.Capture();

    return r;
}

bool TExecutor::IsInExecutorThread() const {
    return ThreadCurrentExecutor == this;
}

TAutoPtr<IWorkItem> TExecutor::DequeueWork() {
    IWorkItem* wi = reinterpret_cast<IWorkItem*>(1);
    size_t queueSize = Max<size_t>();
    if (!WorkItems.TryPop(&wi, &queueSize)) {
        TWhatThreadDoesAcquireGuard<TMutex> g(WorkMutex, "executor: acquiring lock for DequeueWork");
        while (!WorkItems.TryPop(&wi, &queueSize)) {
            if (AtomicGet(ExitWorkers) != 0)
                return nullptr;

            TWhatThreadDoesPushPop pp("waiting for work on condvar");
            WorkAvailable.Wait(WorkMutex);
        }
    }

    auto& wtls = TlsRef(WorkerThreadLocalData);

    if (queueSize > RelaxedLoad(&wtls.MaxQueueSize)) {
        RelaxedStore<ui32>(&wtls.MaxQueueSize, queueSize);
    }

    return wi;
}

void TExecutor::RunWorkItem(TAutoPtr<IWorkItem> wi) {
    WHAT_THREAD_DOES_PUSH_POP_CURRENT_FUNC();
    wi.Release()->DoWork();
}

void TExecutor::ProcessWorkQueueHere() {
    IWorkItem* wi;
    while (WorkItems.TryPop(&wi)) {
        RunWorkItem(wi);
    }
}

void TExecutor::RunWorker() {
    Y_ABORT_UNLESS(!ThreadCurrentExecutor, "state check");
    ThreadCurrentExecutor = this;

    SetCurrentThreadName("wrkr");

    for (;;) {
        TAutoPtr<IWorkItem> wi = DequeueWork();
        if (!wi) {
            break;
        }
        // Note for messagebus users: make sure program crashes
        // on uncaught exception in thread, otherewise messagebus may just hang on error.
        RunWorkItem(wi);
    }

    ThreadCurrentExecutor = (TExecutor*)nullptr;
}

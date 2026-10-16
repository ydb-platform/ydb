#include "executor.h"

#include "queue.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/thread.h>

#include <library/cpp/coroutine/engine/impl.h>

#include <util/system/event.h>
#include <util/system/thread.h>

#include <atomic>

namespace NYdb::NBS {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TWorker: public TAtomicRefCount<TWorker>
{
private:
    ITaskPtr Task;

public:
    TWorker(ITaskPtr task)
        : Task(std::move(task))
    {}

    void Start(TContExecutor* e)
    {
        e->Create<TWorker, &TWorker::Execute>(this, "worker");
    }

private:
    void Execute(TCont* c)
    {
        Y_UNUSED(c);

        TIntrusivePtr<TWorker> holder(this);
        try {
            Task->Execute();
        } catch (...) {
            Cerr << "Unhandled error in Execute: " << CurrentExceptionMessage();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDispatcher: public TAtomicRefCount<TDispatcher>
{
private:
    TContLockFreeQueue<ITask*> Queue;

public:
    TDispatcher(TContExecutor* e)
        : Queue(e)
    {}

    void Start(TContExecutor* e)
    {
        e->Create<TDispatcher, &TDispatcher::Dispatch>(this, "dispatch");
    }

    void Stop()
    {
        Queue.Enqueue(nullptr);
    }

    void Enqueue(ITaskPtr task)
    {
        Queue.Enqueue(task.release());
    }

    // Destroy tasks that were queued after the stop signal. Call this only
    // after the executor thread has left Execute: task destructors may drop
    // the last owner of the executor.
    void DropQueuedTasks()
    {
        ITask* task = nullptr;
        while (Queue.TryDequeue(&task)) {
            delete task;
        }
    }

private:
    void Dispatch(TCont* c)
    {
        TIntrusivePtr<TDispatcher> holder(this);
        try {
            DoDispatch(c);
        } catch (...) {
            Cerr << "Unhandled error in Dispatch: "
                 << CurrentExceptionMessage();
        }
    }

    void DoDispatch(TCont* c)
    {
        auto* executor = c->Executor();

        ITask* task;
        while (Queue.Dequeue(&task)) {
            if (!task) {
                // stop signal received
                break;
            }

            auto worker = MakeIntrusive<TWorker>(ITaskPtr(task));
            worker->Start(executor);

            // make sure worker started
            c->Yield();
        }

        executor->Abort();

        // Abort cancels coroutines that are already waiting. A resumed
        // coroutine may block in WaitFor again. Cancel those waits too, and
        // leave this coroutine only after the others have finished.
        // Otherwise a blocked coroutine keeps the executor's owner alive,
        // ~TExecutor never runs, and LeakSanitizer reports the TContExecutor
        // allocated in ThreadProc.
        while (executor->TotalConts() > 1) {
            // The running dispatcher is counted as waiting. More than one
            // waiter means some other coroutine is blocked.
            if (executor->TotalWaitingConts() > 1) {
                executor->Abort();
            }
            c->Yield();
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct TExecutor::TThread: public ISimpleThread
{
private:
    const TString Name;
    const TAffinity Affinity;
    const size_t ContStackSize;

    TManualEvent StartEvent;
    // Set when ~TExecutor runs on this thread. ThreadProc then deletes
    // itself after Execute returns; joining from here would deadlock.
    std::atomic<bool> DeleteSelf{false};

public:
    std::unique_ptr<TContExecutor> Executor;
    TIntrusivePtr<TDispatcher> Dispatcher;

    TThread(TString name, TAffinity affinity, size_t contStackSize)
        : Name(std::move(name))
        , Affinity(std::move(affinity))
        , ContStackSize(contStackSize)
    {}

    void Start()
    {
        ISimpleThread::Start();
        StartEvent.WaitI();
    }

    // The executor thread owns this object from here on and deletes it
    // after Execute returns.
    void Orphan()
    {
        DeleteSelf.store(true);
    }

    void* ThreadProc() override
    {
        TAffinityGuard affinityGuard(Affinity);

        ::NYdb::NBS::SetCurrentThreadName(Name);

        Executor = std::make_unique<TContExecutor>(ContStackSize);
        Dispatcher = MakeIntrusive<TDispatcher>(Executor.get());

        Dispatcher->Start(Executor.get());
        StartEvent.Signal();

        Executor->Execute();

        if (DeleteSelf.load()) {
            // Execute has returned, so the coroutine runtime is idle.
            // Detach before delete: ~TThread joins, and this is that thread.
            if (Dispatcher) {
                Dispatcher->DropQueuedTasks();
            }
            Detach();
            delete this;
            return nullptr;
        }
        return nullptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

TExecutor::TExecutor(TString name, TAffinity affinity, size_t contStackSize)
    : Thread(new TThread(std::move(name), std::move(affinity), contStackSize))
{}

TExecutor::~TExecutor()
{
    Stop();
}

void TExecutor::Start()
{
    Thread->Start();
}

void TExecutor::Stop()
{
    if (!Thread || !Thread->Dispatcher) {
        return;
    }

    auto dispatcher = Thread->Dispatcher;
    dispatcher->Stop();

    if (Thread->Id() == Thread->CurrentThreadId()) {
        // A task on this thread dropped the last reference. Join would
        // fail with EDEADLK. Let ThreadProc delete the thread after
        // Execute returns.
        Thread->Orphan();
        Thread.release();
        return;
    }

    Thread->Join();
    // The queue destructor drops raw ITask pointers. Delete them on this
    // thread, after the executor thread is gone, so owners are released
    // without joining the executor from itself.
    dispatcher->DropQueuedTasks();
}

void TExecutor::Enqueue(ITaskPtr task)
{
    Y_ABORT_UNLESS(Thread->Dispatcher);
    Thread->Dispatcher->Enqueue(std::move(task));
}

TContExecutor* TExecutor::GetContExecutor()
{
    Y_ABORT_UNLESS(Thread->Executor);
    return Thread->Executor.get();
}

TExecutorPtr
TExecutor::Create(TString name, TAffinity affinity, size_t contStackSize)
{
    return std::shared_ptr<TExecutor>(
        new TExecutor(std::move(name), std::move(affinity), contStackSize));
}

}   // namespace NYdb::NBS

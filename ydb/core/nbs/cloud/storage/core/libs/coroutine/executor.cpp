#include "executor.h"

#include "queue.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/thread.h>

#include <library/cpp/coroutine/engine/impl.h>

#include <util/system/event.h>
#include <util/system/thread.h>

namespace NYdb::NBS {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TWorker
    : public TAtomicRefCount<TWorker>
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

class TDispatcher
    : public TAtomicRefCount<TDispatcher>
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

private:
    void Dispatch(TCont* c)
    {
        TIntrusivePtr<TDispatcher> holder(this);
        try {
            DoDispatch(c);
        } catch (...) {
            Cerr << "Unhandled error in Dispatch: " << CurrentExceptionMessage();
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
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct TExecutor::TThread
    : public ISimpleThread
{
private:
    const TString Name;
    const TAffinity Affinity;
    const size_t ContStackSize;

    TManualEvent StartEvent;

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

    void* ThreadProc() override
    {
        TAffinityGuard affinityGuard(Affinity);

        ::NYdb::NBS::SetCurrentThreadName(Name);

        Executor = std::make_unique<TContExecutor>(ContStackSize);
        Dispatcher = MakeIntrusive<TDispatcher>(Executor.get());

        Dispatcher->Start(Executor.get());
        StartEvent.Signal();

        Executor->Execute();
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
    if (Thread->Dispatcher) {
        Thread->Dispatcher->Stop();
        Thread->Join();
    }
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

TExecutorPtr TExecutor::Create(
    TString name,
    TAffinity affinity,
    size_t contStackSize)
{
    return std::shared_ptr<TExecutor>(
        new TExecutor(std::move(name), std::move(affinity), contStackSize));
}

}   // namespace NYdb::NBS

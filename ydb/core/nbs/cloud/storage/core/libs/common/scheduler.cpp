#include "scheduler.h"

#include "task_queue.h"
#include "thread.h"
#include "timer.h"

#include <ydb/library/actors/prof/tag.h>

#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/system/atexit.h>
#include <util/system/event.h>
#include <util/system/progname.h>
#include <util/system/thread.h>

namespace NYdb::NBS {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration MaxExecutionTime = TDuration::MilliSeconds(50);

////////////////////////////////////////////////////////////////////////////////

struct TTask: public ITask
{
    TTask* Next = nullptr;

    ITaskQueue* const TaskQueue;
    const TInstant Deadline;
    const TCallback Callback;

    TTask(ITaskQueue* taskQueue, TInstant deadline, TCallback callback)
        : TaskQueue(taskQueue)
        , Deadline(deadline)
        , Callback(std::move(callback))
    {}

    void Execute() noexcept override
    {
        Callback();
    }
};

////////////////////////////////////////////////////////////////////////////////

void DeleteList(TTask* task)
{
    while (task) {
        TTask* next = task->Next;
        delete task;
        task = next;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TTaskList
{
private:
    TTask* Head;

public:
    TTaskList(TTask* head = nullptr)
        : Head(head)
    {}

    ~TTaskList()
    {
        DeleteList(Head);
    }

    void Enqueue(std::unique_ptr<TTask> task)
    {
        task->Next = Head;

        // ownership transferred
        Head = task.release();
    }

    std::unique_ptr<TTask> Dequeue()
    {
        std::unique_ptr<TTask> task;
        if (Head) {
            task.reset(Head);
            Head = Head->Next;
        }
        return task;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TInputTasks
{
private:
    TTask* Head = nullptr;

public:
    ~TInputTasks()
    {
        DeleteList(Head);
    }

    void Enqueue(std::unique_ptr<TTask> task)
    {
        do {
            task->Next = AtomicGet(Head);
        } while (!AtomicCas(&Head, task.get(), task->Next));

        // ownership transferred
        Y_UNUSED(task.release());
    }

    TTaskList DequeueAll()
    {
        return {AtomicSwap(&Head, nullptr)};
    }
};

////////////////////////////////////////////////////////////////////////////////

class TPendingTasks
{
    struct TComparer
    {
        bool operator()(const TTask* l, const TTask* r) const
        {
            return r->Deadline < l->Deadline;   // min-heap
        }
    };

private:
    TVector<TTask*> Heap;
    TComparer Comparer;

public:
    ~TPendingTasks()
    {
        for (TTask* task: Heap) {
            delete task;
        }
    }

    void Enqueue(std::unique_ptr<TTask> task)
    {
        Heap.push_back(task.get());
        PushHeap(Heap.begin(), Heap.end(), Comparer);

        // ownership transferred
        Y_UNUSED(task.release());
    }

    std::unique_ptr<TTask> Dequeue(TInstant now)
    {
        std::unique_ptr<TTask> task;
        if (Heap) {
            TTask* pending = Heap.front();
            if (pending->Deadline <= now) {
                task.reset(pending);
                PopHeap(Heap.begin(), Heap.end(), Comparer);
                Heap.pop_back();
            }
        }
        return task;
    }

    TInstant GetDeadLine() const
    {
        if (Heap) {
            TTask* pending = Heap.front();
            return pending->Deadline;
        }
        return TInstant::Max();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TScheduler final
    : public ISimpleThread
    , public IScheduler
{
private:
    ITimerPtr Timer;
    TInputTasks InputTasks;
    TAutoEvent ThreadPark;

    Y_DECLARE_UNUSED char Padding[64];

    TAtomic ShouldStop = 0;
    TPendingTasks PendingTasks;

public:
    TScheduler(ITimerPtr timer)
        : Timer(std::move(timer))
    {}

    ~TScheduler()
    {
        Stop();
    }

    void Start() override;
    void Stop() override;

    void Schedule(
        ITaskQueue* taskQueue,
        TInstant deadline,
        TCallback callback) override;

private:
    void* ThreadProc() override;

    void ExecuteTask(std::unique_ptr<TTask> task) noexcept;
};

////////////////////////////////////////////////////////////////////////////////

void TScheduler::Start()
{
    ISimpleThread::Start();
}

void TScheduler::Stop()
{
    AtomicSet(ShouldStop, 1);
    ThreadPark.Signal();

    ISimpleThread::Join();
}

void TScheduler::Schedule(
    ITaskQueue* taskQueue,
    TInstant deadline,
    TCallback callback)
{
    auto task =
        std::make_unique<TTask>(taskQueue, deadline, std::move(callback));

    InputTasks.Enqueue(std::move(task));
    ThreadPark.Signal();
}

void* TScheduler::ThreadProc()
{
    if (AtomicGet(ShouldStop) == 1 || ExitStarted()) {
        return nullptr;
    }

    ::NYdb::NBS::SetCurrentThreadName("Timer");
    NProfiling::TMemoryTagScope tagScope("STORAGE_THREAD_TIMER");

    while (AtomicGet(ShouldStop) == 0) {
        std::unique_ptr<TTask> task;

        auto inputTasks = InputTasks.DequeueAll();
        while (task = inputTasks.Dequeue()) {
            PendingTasks.Enqueue(std::move(task));
        }

        auto started = Timer->Now();
        while (task = PendingTasks.Dequeue(started)) {
            ExecuteTask(std::move(task));
        }

        auto elapsed = Timer->Now() - started;
        if (elapsed > MaxExecutionTime) {
            Cerr << (TStringBuilder()
                     << GetProgramName()
                     << " WARN: scheduler thread was blocked for too long: "
                     << elapsed)
                 << Endl;
        }

        ThreadPark.WaitD(PendingTasks.GetDeadLine());
    }

    return nullptr;
}

void TScheduler::ExecuteTask(std::unique_ptr<TTask> task) noexcept
{
    if (task->TaskQueue) {
        // enqueue task for execution
        task->TaskQueue->Enqueue(std::move(task));
    } else {
        // execute right now
        task->Execute();
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TSchedulerStub final: public IScheduler
{
    void Start() override
    {}

    void Stop() override
    {}

    void Schedule(
        ITaskQueue* taskQueue,
        TInstant deadline,
        TCallback callback) override
    {
        Y_UNUSED(taskQueue);
        Y_UNUSED(deadline);
        Y_UNUSED(callback);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TBackgroundScheduler final: public IScheduler
{
private:
    ISchedulerPtr Scheduler;
    ITaskQueuePtr TaskQueue;

public:
    TBackgroundScheduler(ISchedulerPtr scheduler, ITaskQueuePtr taskQueue)
        : Scheduler(std::move(scheduler))
        , TaskQueue(std::move(taskQueue))
    {}

    void Start() override
    {}

    void Stop() override
    {}

    void Schedule(
        ITaskQueue* taskQueue,
        TInstant deadline,
        TCallback callback) override
    {
        Scheduler->Schedule(
            taskQueue ? taskQueue : TaskQueue.get(),
            deadline,
            std::move(callback));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ISchedulerPtr CreateScheduler()
{
    return CreateScheduler(CreateWallClockTimer());
}

ISchedulerPtr CreateScheduler(ITimerPtr timer)
{
    return std::make_shared<TScheduler>(std::move(timer));
}

ISchedulerPtr CreateSchedulerStub()
{
    return std::make_shared<TSchedulerStub>();
}

ISchedulerPtr CreateBackgroundScheduler(
    ISchedulerPtr scheduler,
    ITaskQueuePtr taskQueue)
{
    return std::make_shared<TBackgroundScheduler>(
        std::move(scheduler),
        std::move(taskQueue));
}

}   // namespace NYdb::NBS

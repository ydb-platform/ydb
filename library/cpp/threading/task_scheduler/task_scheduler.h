#pragma once

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/generic/vector.h>
#include <util/generic/ptr.h>
#include <util/generic/map.h>

#include <util/datetime/base.h>

#include <util/system/condvar.h>
#include <util/system/mutex.h>

#include <functional>

class TTaskScheduler {
public:
    class ITask;
    using ITaskRef = TIntrusivePtr<ITask>;

    class IRepeatedTask;
    using IRepeatedTaskRef = TIntrusivePtr<IRepeatedTask>;

    class TTaskSchedulerTaskLimitReached: public yexception {};

public:
    explicit TTaskScheduler(size_t threadCount = 1, size_t maxTaskCount = Max<size_t>());
    ~TTaskScheduler();

    void Start();
    void Stop();

    bool Add(ITaskRef task, TInstant expire);
    bool Add(IRepeatedTaskRef task, TDuration period);
    [[nodiscard]] bool AddFunc(std::function<TInstant()> function, TInstant expire);
    [[nodiscard]] bool AddRepeatedFunc(std::function<bool()> function, TDuration period);

    // Safe versions that throw yexception when task limit is reached
    void SafeAdd(ITaskRef task, TInstant expire);
    void SafeAdd(IRepeatedTaskRef task, TDuration period);
    void SafeAddFunc(std::function<TInstant()> function, TInstant expire);
    void SafeAddRepeatedFunc(std::function<bool()> function, TDuration period);

    size_t GetTaskCount() const;

private:
    class TWorkerThread;

    struct TTaskHolder {
        explicit TTaskHolder(ITaskRef& task)
            : Task(task)
        {
        }
    public:
        ITaskRef Task;
        TWorkerThread* WaitingWorker = nullptr;
    };

    using TQueueType = TMultiMap<TInstant, TTaskHolder>;
    using TQueueIterator = TQueueType::iterator;

private:
    void ChangeDebugState(TWorkerThread* thread, const TString& state);
    void ChooseFromQueue(TQueueIterator& toWait);
    bool Wait(TWorkerThread* thread, TQueueIterator& toWait);

    void WorkerFunc(TWorkerThread* thread);

private:
    bool IsStopped_ = false;

    TAtomic TaskCounter_ = 0;
    TQueueType Queue_;

    TCondVar CondVar_;
    TMutex Lock_;

    TVector<TAutoPtr<TWorkerThread>> Workers_;

    const size_t MaxTaskCount_;
};

class TTaskScheduler::ITask
    : public TAtomicRefCount<ITask>
{
public:
    virtual ~ITask();

    virtual TInstant Process() {//returns time to repeat this task
        return TInstant::Max();
    }
};

class TTaskScheduler::IRepeatedTask
    : public TAtomicRefCount<IRepeatedTask>
{
public:
    virtual ~IRepeatedTask();

    virtual bool Process() {//returns if to repeat task again
        return false;
    }
};


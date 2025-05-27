#include "task_scheduler.h"

#include <util/system/thread.h>
#include <util/string/cast.h>
#include <util/stream/output.h>
#include <util/generic/yexception.h>

TTaskScheduler::ITask::~ITask() {}
TTaskScheduler::IRepeatedTask::~IRepeatedTask() {}

class TFunctionTask final : public TTaskScheduler::ITask {
public:
    explicit TFunctionTask(std::function<TInstant()> function)
        : Function_{std::move(function)}
    {}

    TInstant Process() override {
        return Function_();
    }

private:
    std::function<TInstant()> Function_;
};

class TRepeatedFunctionTask final : public TTaskScheduler::IRepeatedTask {
public:
    explicit TRepeatedFunctionTask(std::function<bool()> function)
        : Function_{std::move(function)}
    {}

    bool Process() override {
        return Function_();
    }

private:
    std::function<bool()> Function_;
};

class TTaskScheduler::TWorkerThread
    : public ISimpleThread
{
public:
    TWorkerThread(TTaskScheduler& state)
        : Scheduler_(state)
    {
    }

    TString DebugState = "?";
    TString DebugId = "";
private:
    void* ThreadProc() noexcept override {
        Scheduler_.WorkerFunc(this);
        return nullptr;
    }
private:
    TTaskScheduler& Scheduler_;
};



TTaskScheduler::TTaskScheduler(size_t threadCount, size_t maxTaskCount)
    : MaxTaskCount_(maxTaskCount)
{
    for (size_t i = 0; i < threadCount; ++i) {
        Workers_.push_back(new TWorkerThread(*this));
        Workers_.back()->DebugId = ToString(i);
    }
}

TTaskScheduler::~TTaskScheduler() {
    try {
        Stop();
    } catch (...) {
        Cdbg << "task scheduled destruction error: " << CurrentExceptionMessage();
    }
}

void TTaskScheduler::Start() {
    for (auto& w : Workers_) {
        w->Start();
    }
}

void TTaskScheduler::Stop() {
    with_lock (Lock_) {
        IsStopped_ = true;
        CondVar_.BroadCast();
    }

    for (auto& w: Workers_) {
        w->Join();
    }

    Workers_.clear();
    Queue_.clear();
}

size_t TTaskScheduler::GetTaskCount() const {
    return static_cast<size_t>(AtomicGet(TaskCounter_));
}

namespace {
    class TTaskWrapper
        : public TTaskScheduler::ITask
        , TNonCopyable
    {
    public:
        TTaskWrapper(TTaskScheduler::ITaskRef task, TAtomic& counter)
            : Task_(task)
            , Counter_(counter)
        {
            AtomicIncrement(Counter_);
        }

        ~TTaskWrapper() override {
            AtomicDecrement(Counter_);
        }
    private:
        TInstant Process() override {
            return Task_->Process();
        }
    private:
        TTaskScheduler::ITaskRef Task_;
        TAtomic& Counter_;
    };
}

bool TTaskScheduler::Add(ITaskRef task, TInstant expire) {
    with_lock (Lock_) {
        if (!IsStopped_ && Workers_.size() > 0 && GetTaskCount() + 1 <= MaxTaskCount_) {
            ITaskRef newTask = new TTaskWrapper(task, TaskCounter_);
            Queue_.insert(std::make_pair(expire, TTaskHolder(newTask)));

            if (!Queue_.begin()->second.WaitingWorker) {
                CondVar_.Signal();
            }
            return true;
        }
    }

    return false;
}

namespace {
    class TRepeatedTask
        : public TTaskScheduler::ITask
    {
    public:
        TRepeatedTask(TTaskScheduler::IRepeatedTaskRef task, TDuration period, TInstant deadline)
            : Task_(task)
            , Period_(period)
            , Deadline_(deadline)
        {
        }
    private:
        TInstant Process() final {
            Deadline_ += Period_;
            if (Task_->Process()) {
                return Deadline_;
            } else {
                return TInstant::Max();
            }
        }
    private:
        TTaskScheduler::IRepeatedTaskRef Task_;
        TDuration Period_;
        TInstant Deadline_;
    };
}

bool TTaskScheduler::Add(IRepeatedTaskRef task, TDuration period) {
    const TInstant deadline = Now() + period;
    ITaskRef t = new TRepeatedTask(task, period, deadline);
    return Add(t, deadline);
}

static void ThrowOnTaskLimitReached(bool sucessfullyScheduled) {
    if (!sucessfullyScheduled) {
        throw TTaskScheduler::TTaskSchedulerTaskLimitReached{} << "Failed to schedule task";
    }
}

bool TTaskScheduler::AddFunc(std::function<TInstant()> function, TInstant expire) {
    return Add(MakeIntrusive<TFunctionTask>(std::move(function)), expire);
}

bool TTaskScheduler::AddRepeatedFunc(std::function<bool()> function, TDuration period) {
    return Add(MakeIntrusive<TRepeatedFunctionTask>(std::move(function)), period);
}

void TTaskScheduler::SafeAdd(ITaskRef task, TInstant expire) {
    ThrowOnTaskLimitReached(Add(std::move(task), expire));
}

void TTaskScheduler::SafeAdd(IRepeatedTaskRef task, TDuration period) {
    ThrowOnTaskLimitReached(Add(std::move(task), period));
}

void TTaskScheduler::SafeAddFunc(std::function<TInstant()> function, TInstant expire) {
    ThrowOnTaskLimitReached(AddFunc(std::move(function), expire));
}

void TTaskScheduler::SafeAddRepeatedFunc(std::function<bool()> function, TDuration period) {
    ThrowOnTaskLimitReached(AddRepeatedFunc(std::move(function), period));
}

const bool debugOutput = false;

void TTaskScheduler::ChangeDebugState(TWorkerThread* thread, const TString& state) {
    if (!debugOutput) {
        Y_UNUSED(thread);
        Y_UNUSED(state);
        return;
    }

    thread->DebugState = state;

    TStringStream ss;
    ss << Now() << " " << thread->DebugId << ":\t";
    for (auto& w : Workers_) {
        ss << w->DebugState << " ";
    }
    ss << " [" << Queue_.size() << "] [" << TaskCounter_ << "]" <<  Endl;
    Cerr << ss.Str();
}

bool TTaskScheduler::Wait(TWorkerThread* thread, TQueueIterator& toWait) {
    ChangeDebugState(thread, "w");
    toWait->second.WaitingWorker = thread;
    return !CondVar_.WaitD(Lock_, toWait->first);
}

void TTaskScheduler::ChooseFromQueue(TQueueIterator& toWait) {
    for (TQueueIterator it = Queue_.begin(); it != Queue_.end(); ++it) {
        if (!it->second.WaitingWorker) {
            if (toWait == Queue_.end()) {
                toWait = it;
            } else if (it->first < toWait->first) {
                toWait->second.WaitingWorker = nullptr;
                toWait = it;
            }
            break;
        }
    }
}

void TTaskScheduler::WorkerFunc(TWorkerThread* thread) {
    TThread::SetCurrentThreadName("TaskSchedWorker");

    TQueueIterator toWait = Queue_.end();
    ITaskRef toDo;

    for (;;) {
        TInstant repeat = TInstant::Max();

        if (!!toDo) {
            try {
                repeat = toDo->Process();
            } catch (...) {
               Cdbg << "task scheduler error: " << CurrentExceptionMessage();
            }
        }


        with_lock (Lock_) {
            ChangeDebugState(thread, "f");

            if (IsStopped_) {
                ChangeDebugState(thread, "s");
                return ;
            }

            if (!!toDo) {
                if (repeat < TInstant::Max()) {
                    Queue_.insert(std::make_pair(repeat, TTaskHolder(toDo)));
                }
            }

            toDo = nullptr;

            ChooseFromQueue(toWait);

            if (toWait != Queue_.end()) {
                if (toWait->first <= Now() || Wait(thread, toWait)) {

                    toDo = toWait->second.Task;
                    Queue_.erase(toWait);
                    toWait = Queue_.end();

                    if (!Queue_.empty() && !Queue_.begin()->second.WaitingWorker && Workers_.size() > 1) {
                        CondVar_.Signal();
                    }

                    ChangeDebugState(thread, "p");
                }
            } else {
                ChangeDebugState(thread, "e");
                CondVar_.WaitI(Lock_);
            }
        }
    }
}

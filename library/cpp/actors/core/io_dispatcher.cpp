#include "io_dispatcher.h"
#include "actor_bootstrapped.h"
#include "hfunc.h"
#include <util/system/mutex.h>
#include <util/system/condvar.h>
#include <util/system/thread.h>
#include <map>
#include <list>

namespace NActors {

    class TIoDispatcherActor : public TActorBootstrapped<TIoDispatcherActor> {
        enum {
            EvNotifyThreadStopped = EventSpaceBegin(TEvents::ES_PRIVATE),
        };

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // IO task queue
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        class TTask {
            TInstant Timestamp;
            std::function<void()> Callback;

        public:
            TTask(TInstant timestamp, TEvInvokeQuery *ev)
                : Timestamp(timestamp)
                , Callback(std::move(ev->Callback))
            {}

            void Execute() {
                Callback();
            }

            TInstant GetTimestamp() const {
                return Timestamp;
            }
        };

        class TTaskQueue {
            std::list<TTask> Tasks;
            TMutex Mutex;
            TCondVar CondVar;
            size_t NumThreadsToStop = 0;

        public:
            void Enqueue(TInstant timestamp, TEvInvokeQuery *ev) {
                std::list<TTask> list;
                list.emplace_back(timestamp, ev);
                with_lock (Mutex) {
                    Tasks.splice(Tasks.end(), std::move(list));
                }
                CondVar.Signal();
            }

            bool Dequeue(std::list<TTask>& list, bool *sendNotify) {
                with_lock (Mutex) {
                    CondVar.Wait(Mutex, [&] { return NumThreadsToStop || !Tasks.empty(); });
                    if (NumThreadsToStop) {
                        *sendNotify = NumThreadsToStop != Max<size_t>();
                        if (*sendNotify) {
                            --NumThreadsToStop;
                        }
                        return false;
                    } else {
                        list.splice(list.end(), Tasks, Tasks.begin());
                        return true;
                    }
                }
            }

            void Stop() {
                with_lock (Mutex) {
                    NumThreadsToStop = Max<size_t>();
                }
                CondVar.BroadCast();
            }

            void StopOne() {
                with_lock (Mutex) {
                    ++NumThreadsToStop;
                    Y_ABORT_UNLESS(NumThreadsToStop);
                }
                CondVar.Signal();
            }

            std::optional<TInstant> GetEarliestTaskTimestamp() {
                with_lock (Mutex) {
                    return Tasks.empty() ? std::nullopt : std::make_optional(Tasks.front().GetTimestamp());
                }
            }
        };

        TTaskQueue TaskQueue;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // IO dispatcher threads
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        class TThread : public ISimpleThread {
            TIoDispatcherActor& Actor;
            TActorSystem* const ActorSystem;

        public:
            TThread(TIoDispatcherActor& actor, TActorSystem *actorSystem)
                : Actor(actor)
                , ActorSystem(actorSystem)
            {
                Start();
            }

            void *ThreadProc() override {
                SetCurrentThreadName("kikimr IO");
                for (;;) {
                    std::list<TTask> tasks;
                    bool sendNotify;
                    if (!Actor.TaskQueue.Dequeue(tasks, &sendNotify)) {
                        if (sendNotify) {
                            ActorSystem->Send(new IEventHandle(EvNotifyThreadStopped, 0, Actor.SelfId(), TActorId(),
                                nullptr, TThread::CurrentThreadId()));
                        }
                        break;
                    }
                    for (TTask& task : tasks) {
                        task.Execute();
                        ++*Actor.TasksCompleted;
                    }
                }
                return nullptr;
            }
        };

        static constexpr size_t MinThreadCount = 4;
        static constexpr size_t MaxThreadCount = 64;
        std::map<TThread::TId, std::unique_ptr<TThread>> Threads;
        size_t NumRunningThreads = 0;

        void StartThread() {
            auto thread = std::make_unique<TThread>(*this, TlsActivationContext->ExecutorThread.ActorSystem);
            const TThread::TId id = thread->Id();
            Threads.emplace(id, std::move(thread));
            *NumThreads = ++NumRunningThreads;
            ++*ThreadsStarted;
        }

        void StopThread() {
            Y_ABORT_UNLESS(Threads.size());
            TaskQueue.StopOne();
            *NumThreads = --NumRunningThreads;
            ++*ThreadsStopped;
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Counters
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        NMonitoring::TDynamicCounters::TCounterPtr NumThreads;
        NMonitoring::TDynamicCounters::TCounterPtr TasksAdded;
        NMonitoring::TDynamicCounters::TCounterPtr TasksCompleted;
        NMonitoring::TDynamicCounters::TCounterPtr ThreadsStarted;
        NMonitoring::TDynamicCounters::TCounterPtr ThreadsStopped;

    public:
        TIoDispatcherActor(const NMonitoring::TDynamicCounterPtr& counters)
            : NumThreads(counters->GetCounter("NumThreads"))
            , TasksAdded(counters->GetCounter("TasksAdded", true))
            , TasksCompleted(counters->GetCounter("TasksCompleted", true))
            , ThreadsStarted(counters->GetCounter("ThreadsStarted", true))
            , ThreadsStopped(counters->GetCounter("ThreadsStopped", true))
        {}

        ~TIoDispatcherActor() override {
            TaskQueue.Stop();
        }

        static constexpr char ActorName[] = "IO_DISPATCHER_ACTOR";

        void Bootstrap() {
            while (NumRunningThreads < MinThreadCount) {
                StartThread();
            }
            HandleWakeup();
            Become(&TThis::StateFunc);
        }

        void HandleThreadStopped(TAutoPtr<IEventHandle> ev) {
            auto it = Threads.find(ev->Cookie);
            Y_ABORT_UNLESS(it != Threads.end());
            it->second->Join();
            Threads.erase(it);
        }

        void Handle(TEvInvokeQuery::TPtr ev) {
            ++*TasksAdded;
            TaskQueue.Enqueue(TActivationContext::Now(), ev->Get());
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Thread usage counter logic
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        std::optional<TInstant> IdleTimestamp;
        static constexpr TDuration ThreadStartTime = TDuration::MilliSeconds(500);
        static constexpr TDuration ThreadStopTime = TDuration::MilliSeconds(500);

        void HandleWakeup() {
            const TInstant now = TActivationContext::Now();
            std::optional<TInstant> earliest = TaskQueue.GetEarliestTaskTimestamp();
            if (earliest) {
                if (now >= *earliest + ThreadStartTime && NumRunningThreads < MaxThreadCount) {
                    StartThread();
                }
                IdleTimestamp.reset();
            } else if (!IdleTimestamp) {
                IdleTimestamp = now;
            } else if (now >= *IdleTimestamp + ThreadStopTime) {
                IdleTimestamp.reset();
                if (NumRunningThreads > MinThreadCount) {
                    StopThread();
                }
            }
            Schedule(TDuration::MilliSeconds(100), new TEvents::TEvWakeup);
        }

        STRICT_STFUNC(StateFunc, {
            fFunc(EvNotifyThreadStopped, HandleThreadStopped);
            hFunc(TEvInvokeQuery, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            cFunc(TEvents::TSystem::Poison, PassAway);
        })
    };

    IActor *CreateIoDispatcherActor(const NMonitoring::TDynamicCounterPtr& counters) {
        return new TIoDispatcherActor(counters);
    }

} // NActors

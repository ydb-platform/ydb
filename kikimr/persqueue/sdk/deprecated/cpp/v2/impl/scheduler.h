#pragma once
#include <util/datetime/base.h>
#include <util/generic/ptr.h>
#include <util/system/event.h>
#include <util/system/spinlock.h>
#include <util/thread/factory.h>

#include <functional>
#include <queue>
#include <utility>

namespace NPersQueue {

class TPQLibPrivate;

class TScheduler {
    struct TCallbackHandlersCompare;
public:
    using TCallback = std::function<void()>;

    class TCallbackHandler: public TAtomicRefCount<TCallbackHandler> {
        friend class TScheduler;
        friend struct TCallbackHandlersCompare;

        template <class TFunc>
        TCallbackHandler(TInstant time, TFunc&& func, TPQLibPrivate* pqLib, const void* queueTag)
            : Time(time)
            , Callback(std::forward<TFunc>(func))
            , PQLib(pqLib)
            , QueueTag(queueTag)
        {
        }

        void Execute();

    public:
        // Cancels execution of callback.
        // If callback is already canceled or executes (executed), does nothing.
        // Posteffect: callback is guaranteed to be destroyed after this call.
        void TryCancel();

    private:
        TInstant Time;
        TCallback Callback;
        TAdaptiveLock Lock;
        TPQLibPrivate* PQLib;
        const void* QueueTag;
    };

public:
    // Starts a scheduler thread.
    explicit TScheduler(TPQLibPrivate* pqLib);

    // Stops a scheduler thread.
    ~TScheduler();

    // Schedules a new callback to be executed
    template <class TFunc>
    TIntrusivePtr<TCallbackHandler> Schedule(TInstant time, const void* queueTag, TFunc&& func) {
        TIntrusivePtr<TCallbackHandler> handler(new TCallbackHandler(time, std::forward<TFunc>(func), PQLib, queueTag));
        AddToSchedule(handler);
        return handler;
    }

    template <class TFunc>
    TIntrusivePtr<TCallbackHandler> Schedule(TDuration delta, const void* queueTag, TFunc&& func) {
        return Schedule(TInstant::Now() + delta, queueTag, std::forward<TFunc>(func));
    }

private:
    void AddToSchedule(TIntrusivePtr<TCallbackHandler> handler);
    void ShutdownAndWait();
    void SchedulerThread();

private:
    struct TCallbackHandlersCompare {
        bool operator()(const TIntrusivePtr<TCallbackHandler>& h1, const TIntrusivePtr<TCallbackHandler>& h2) const;
    };

    using TCallbackQueue = std::priority_queue<TIntrusivePtr<TCallbackHandler>, std::vector<TIntrusivePtr<TCallbackHandler>>, TCallbackHandlersCompare>;
    TAdaptiveLock Lock;
    TAutoEvent Event;
    TCallbackQueue Callbacks;
    TAtomic Shutdown;
    THolder<IThreadFactory::IThread> Thread;
    TPQLibPrivate* PQLib;
};
} // namespace NPersQueue

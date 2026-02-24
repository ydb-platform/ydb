#pragma once

#include "public.h"

#include <library/cpp/coroutine/engine/events.h>
#include <library/cpp/coroutine/engine/network.h>
#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/generic/deque.h>
#include <util/system/pipe.h>
#include <util/thread/lfqueue.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TSimpleQueue
{
private:
    TDeque<T> Items;

public:
    void Enqueue(T item)
    {
        Items.push_back(std::move(item));
    }

    bool Dequeue(T* item)
    {
        if (Items) {
            *item = std::move(Items.front());
            Items.pop_front();
            return true;
        }

        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TContPipeEvent
{
private:
    TContExecutor* Executor;

    TPipeHandle SignalRecvPipe;
    TPipeHandle SignalSendPipe;

    // optimization for exactly one waiter
    enum EState
    {
        STATE_EMPTY = 0,
        STATE_SIGNALED = 1,
        STATE_WAITING = 2
    };

    TAtomic State = STATE_EMPTY;

public:
    TContPipeEvent(TContExecutor* e)
        : Executor(e)
    {
        TPipeHandle::Pipe(SignalRecvPipe, SignalSendPipe);

        SetNonBlock(SignalRecvPipe);
        SetNonBlock(SignalSendPipe);
    }

    void Signal()
    {
        TAtomicBase state;
        do {
            state = AtomicGet(State);
            if (state & STATE_SIGNALED) {
                // nothing to do
                return;
            }
        } while (!AtomicCas(&State, STATE_SIGNALED, state));

        // use pipe only if somebody waiting
        if (state & STATE_WAITING) {
            char tmp = 1;
            SignalSendPipe.Write(&tmp, 1);
        }
    }

    int WaitI()
    {
        TAtomicBase state;
        do {
            state = AtomicGet(State);
        } while (!AtomicCas(&State, (state & STATE_SIGNALED) ? STATE_EMPTY : STATE_WAITING, state));

        // use pipe only if not signaled
        if (state & STATE_SIGNALED) {
            return 0;
        }

        char tmp;
        return NCoro::ReadI(Executor->Running(), SignalRecvPipe, &tmp, 1).Status();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename T, typename TQueue, typename TEvent>
class TContQueue
{
private:
    TQueue Queue;
    TEvent Event;

public:
    TContQueue(TContExecutor* e)
        : Event(e)
    {}

    void Enqueue(T item)
    {
        Queue.Enqueue(std::move(item));
        Event.Signal();
    }

    bool Dequeue(T* item)
    {
        do {
            if (Queue.Dequeue(item)) {
                return true;
            }
        } while (Event.WaitI() != ECANCELED);

        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
using TContSimpleQueue = TContQueue<T, TSimpleQueue<T>, TContSimpleEvent>;

template <typename T>
using TContLockFreeQueue = TContQueue<T, TLockFreeQueue<T>, TContPipeEvent>;

}   // namespace NYdb::NBS

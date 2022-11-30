#pragma once

#include <library/cpp/coroutine/engine/events.h>
#include <library/cpp/coroutine/engine/impl.h>
#include <library/cpp/coroutine/engine/network.h>

#include <util/network/socket.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/pipe.h>

// TPipeEvent and TPipeSemaphore try to minimize number of coroutines reading from same pipe
// because its actually quite expensive with >1000 coroutines waiting on same event/semaphore

// Using this class you can block in coroutine on waiting
// a signal from outer thread.
// Usual thread synchronization primitives are not appropriate
// because they will block all coroutines in single thread.
class TPipeEvent {
public:
    explicit TPipeEvent()
        : Signaled(0)
        , NumWaiting(0)
    {
        TPipeHandle::Pipe(SignalRecvPipe, SignalSendPipe);
        SetNonBlock(SignalRecvPipe);
        SetNonBlock(SignalSendPipe);
    }

    void Signal() {
        if (AtomicCas(&Signaled, 1, 0)) {
            char tmp = 1;
            SignalSendPipe.Write(&tmp, 1);
        }
    }

    void Wait(TCont* cont) {
        if (++NumWaiting > 1) {
            ToWake.WaitI(cont);
        }
        char tmp;
        NCoro::ReadI(cont, SignalRecvPipe, &tmp, 1).Checked();
        AtomicSet(Signaled, 0);
        if (--NumWaiting > 0) {
            ToWake.Signal();
        }
    }

private:
    TAtomic Signaled;
    size_t NumWaiting;
    TContWaitQueue ToWake;
    TPipeHandle SignalRecvPipe;
    TPipeHandle SignalSendPipe;
};

class TPipeSemaphore {
public:
    explicit TPipeSemaphore()
        : Value(0)
        , NumWaiting(0)
    {
        TPipeHandle::Pipe(SignalRecvPipe, SignalSendPipe);
        SetNonBlock(SignalRecvPipe);
        SetNonBlock(SignalSendPipe);
    }

    void Inc() {
        if (AtomicIncrement(Value) <= 0) {
            char tmp = 1;
            SignalSendPipe.Write(&tmp, 1);
        }
    }

    void Dec(TCont* cont) {
        if (AtomicDecrement(Value) < 0) {
            if (++NumWaiting > 1) {
                ToWake.WaitI(cont);
            }
            char tmp;
            NCoro::ReadI(cont, SignalRecvPipe, &tmp, 1).Checked();
            if (--NumWaiting > 0) {
                ToWake.Signal();
            }
        }
    }

private:
    TAtomic Value;
    size_t NumWaiting;
    TContWaitQueue ToWake;
    TPipeHandle SignalRecvPipe;
    TPipeHandle SignalSendPipe;
};

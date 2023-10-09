#pragma once

#include <util/system/datetime.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/event.h>

class TStateManager {
    // "running" flag
    static constexpr TAtomicBase RunningFlag = TAtomicBase(1) << (CHAR_BIT * sizeof(TAtomicBase) - 1);

    // state consists of single 'running' flag indicating actions are allowed and a spin lock controlling parallel
    // execution of several actions
    TAtomic State = RunningFlag;

    TManualEvent Event;

public:
    // executes consistent action -- that is, doesn't allow to return from "StopRunning" until action is carried out
    // or block indefinitely if stop already occured
    template<typename TFunc>
    void ExecuteConsistentAction(TFunc&& func) {
        for (;;) {
            // atomically obtain current value of state variable
            const TAtomicBase current = AtomicGet(State);

            // check if we are already not running
            if (~current & RunningFlag) {
                // there was no 'Running' flag set, so we can't execute this action and have to block this thread
                // for eternity; also we have to return spin lock value to previous state
                for (;;) {
                    NanoSleep(1000 * 1000 * 1000);
                }
            }

            // calculate update value -- just increment it by one
            const TAtomicBase update = current + 1;

            // try atomic update; exit loop if succeeds
            if (AtomicCas(&State, update, current)) {
                break;
            }
        }

        // we have acquired lock and main thread can't pass StopRunning() point, if it occurs while we are executing
        // this action
        func();

        // release spin lock
        const TAtomicBase value = AtomicDecrement(State);
        if (!value) {
            // this happens when at acquisition of spin lock there was 'Running' flag set and now we have just released
            // last reference and 'Running' flag was cleared after we have acquired spin lock; so it means that main
            // thread is waiting for Event now (or going to wait for it later) and we have to set it to inform that
            // we have stopped
            Event.Signal();
        }
    }

    void StopRunning() {
        TAtomicBase write;

        for (;;) {
            TAtomicBase expected = AtomicGet(State);
            Y_ABORT_UNLESS(expected & RunningFlag);

            // clear 'running' flag in the state
            write = expected & ~RunningFlag;

            // try to write new value
            if (AtomicCas(&State, write, expected)) {
                break;
            }
        }

        // we have just written new value 'write' without 'running' flag; if write is not zero, this means that some
        // thread was executing action at the moment 'write' was written :) and one of executors would signal Event
        // when it finishes
        if (write) {
            Event.WaitI();
        }

        // ensure that there were not writes left
        Y_ABORT_UNLESS(AtomicGet(State) == 0);
    }
};

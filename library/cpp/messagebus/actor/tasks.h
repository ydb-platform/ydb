#pragma once

#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/yassert.h>

namespace NActor {
    class TTasks {
        enum {
            // order of values is important
            E_WAITING,
            E_RUNNING_NO_TASKS,
            E_RUNNING_GOT_TASKS,
        };

    private:
        TAtomic State;

    public:
        TTasks()
            : State(E_WAITING)
        {
        }

        // @return true iff caller have to either schedule task or execute it
        bool AddTask() {
            // High contention case optimization: AtomicGet is cheaper than AtomicSwap.
            if (E_RUNNING_GOT_TASKS == AtomicGet(State)) {
                return false;
            }

            TAtomicBase oldState = AtomicSwap(&State, E_RUNNING_GOT_TASKS);
            return oldState == E_WAITING;
        }

        // called by executor
        // @return true iff we have to recheck queues
        bool FetchTask() {
            TAtomicBase newState = AtomicDecrement(State);
            if (newState == E_RUNNING_NO_TASKS) {
                return true;
            } else if (newState == E_WAITING) {
                return false;
            }
            Y_ABORT("unknown");
        }
    };

}

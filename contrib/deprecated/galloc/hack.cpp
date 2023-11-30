#include "hack.h"

#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/spin_wait.h>

#include "spinlock.h"

void SPIN_L(spinlock_t* l) {
    if (!AtomicTryLock(l)) {
        TSpinWait sw;

        while (!AtomicTryAndTryLock(l)) {
            sw.Sleep();
        }
    }
}

void SPIN_U(spinlock_t* l) {
    AtomicUnlock(l);
}

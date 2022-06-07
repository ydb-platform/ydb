#pragma once

#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/condvar.h>
#include <util/system/mutex.h>

namespace NBus {
    namespace NPrivate {
        class TBusSessionJobCount {
        private:
            TAtomic JobCount;

            TMutex Mutex;
            TCondVar CondVar;

        public:
            TBusSessionJobCount();
            ~TBusSessionJobCount();

            void Add(unsigned delta) {
                AtomicAdd(JobCount, delta);
            }

            void Increment() {
                Add(1);
            }

            void Decrement() {
                if (AtomicDecrement(JobCount) == 0) {
                    TGuard<TMutex> guard(Mutex);
                    CondVar.BroadCast();
                }
            }

            void WaitForZero();
        };

    }
}

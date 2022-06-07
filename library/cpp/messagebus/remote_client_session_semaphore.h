#pragma once

#include "cc_semaphore.h"

#include <util/generic/noncopyable.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/condvar.h>
#include <util/system/mutex.h>

namespace NBus {
    namespace NPrivate {
        class TRemoteClientSessionSemaphore: public TComplexConditionSemaphore<TRemoteClientSessionSemaphore> {
        private:
            const char* const Name;

            TAtomicBase const Limit;
            TAtomic Current;
            TAtomic StopSignal;

        public:
            TRemoteClientSessionSemaphore(TAtomicBase limit, const char* name = "unnamed");
            ~TRemoteClientSessionSemaphore();

            TAtomicBase GetCurrent() const {
                return AtomicGet(Current);
            }

            void Acquire();
            bool TryAcquire();
            void Increment();
            void IncrementMultiple(TAtomicBase count);
            bool TryWait();
            void Release();
            void ReleaseMultiple(TAtomicBase count);
            void Stop();

        private:
            void CheckNeedToUnlock();
        };

    }
}

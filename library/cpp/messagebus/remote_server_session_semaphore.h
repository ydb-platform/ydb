#pragma once

#include "cc_semaphore.h"

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/generic/noncopyable.h>

namespace NBus {
    namespace NPrivate {
        class TRemoteServerSessionSemaphore: public TComplexConditionSemaphore<TRemoteServerSessionSemaphore> {
        private:
            const char* const Name;

            TAtomicBase const LimitCount;
            TAtomicBase const LimitSize;
            TAtomic CurrentCount;
            TAtomic CurrentSize;
            TAtomic PausedByUser;
            TAtomic StopSignal;

        public:
            TRemoteServerSessionSemaphore(TAtomicBase limitCount, TAtomicBase limitSize, const char* name = "unnamed");
            ~TRemoteServerSessionSemaphore();

            TAtomicBase GetCurrentCount() const {
                return AtomicGet(CurrentCount);
            }
            TAtomicBase GetCurrentSize() const {
                return AtomicGet(CurrentSize);
            }

            void IncrementMultiple(TAtomicBase count, TAtomicBase size);
            bool TryWait();
            void ReleaseMultiple(TAtomicBase count, TAtomicBase size);
            void Stop();
            void PauseByUsed(bool pause);

        private:
            void CheckNeedToUnlock();
        };

    }
}

#pragma once
#include <ydb/core/util/queue_oneone_inplace.h>
#include <util/system/condvar.h>
#include <util/system/mutex.h>

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TCountedQueueOneOne
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
template<typename T, ui32 TSize>
class TCountedQueueOneOne {
    TOneOneQueueInplace<T, TSize> Queue;
    TAtomic SizeLowerEstimate;
    TMutex ProducedMutex;
    TCondVar ProducedCondVar;
public:
    TCountedQueueOneOne()
        : SizeLowerEstimate(0)
    {}

    virtual ~TCountedQueueOneOne() {
        Y_VERIFY_S(AtomicGet(SizeLowerEstimate) == 0, "Unexpected SizeLowerEstimate# " << AtomicGet(SizeLowerEstimate));
    }

    void Push(T x) noexcept {
        Queue.Push(x);
        AtomicIncrement(SizeLowerEstimate);
        WakeUp();
    }

    T Pop() {
        AtomicDecrement(SizeLowerEstimate);
        return Queue.Pop();
    }

    T Head() {
        return Queue.Head();
    }

    TAtomicBase GetWaitingSize() {
        return AtomicLoad(&SizeLowerEstimate);
    }

    void ProducedWaitI() {
        TGuard<TMutex> guard(ProducedMutex);
        if (AtomicGet(SizeLowerEstimate)) {
            return;
        }
        return ProducedCondVar.WaitI(ProducedMutex);
    }

    bool ProducedWait(TDuration duration) {
        TGuard<TMutex> guard(ProducedMutex);
        if (AtomicGet(SizeLowerEstimate)) {
            return true;
        }
        return ProducedCondVar.WaitT(ProducedMutex, duration);
    }

    void WakeUp() {
        TGuard<TMutex> guard(ProducedMutex);
        ProducedCondVar.Signal();
    }
};

} // NPDisk
} // NKikimr

#pragma once
#include "defs.h"
#include <ydb/core/util/queue_oneone_inplace.h>
#include <util/system/condvar.h>
#include <library/cpp/threading/queue/mpsc_vinfarr_obstructive.h>
#include <library/cpp/threading/queue/mpsc_read_as_filled.h>

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TCountedQueueManyOne
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
template<typename T, ui32 TSize>
class TCountedQueueManyOne {
    NThreading::TObstructiveConsumerAuxQueue<T> Queue;
    TAtomic SizeLowerEstimate;
    TMutex ProducedMutex;
    TCondVar ProducedCondVar;
public:
    TCountedQueueManyOne()
        : SizeLowerEstimate(0)
    {}

    virtual ~TCountedQueueManyOne() {
        Y_VERIFY_S(AtomicGet(SizeLowerEstimate) == 0, "Unexpected SizeLowerEstimate# " << AtomicGet(SizeLowerEstimate));
    }

    void Push(T *x) noexcept {
        Queue.Push(x);
        AtomicIncrement(SizeLowerEstimate);
        WakeUp();
    }

    T *Pop() {
        AtomicDecrement(SizeLowerEstimate);
        return Queue.Pop();
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

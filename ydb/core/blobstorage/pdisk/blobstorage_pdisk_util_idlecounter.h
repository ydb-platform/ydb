#pragma once

#include "defs.h"
#include "blobstorage_pdisk_util_atomicblockcounter.h"
#include "blobstorage_pdisk_mon.h"

namespace NKikimr {

namespace NPDisk {

class TIdleCounter {
    static constexpr ui32 InFlightThreshold = 1024;

    TAtomicBlockCounter ReversedInFlight;
    TLight &IdleLight;

public:

    TIdleCounter(TLight &light)
        : IdleLight(light)
    {
        ReversedInFlight.Add(InFlightThreshold + 1);
    }

    void Increment() {
        TAtomicBlockCounter::TResult res;
        ReversedInFlight.ThresholdSub(1, InFlightThreshold, res);
        IdleLight.Set(res.A, res.Seqno);
    }

    void Decrement() {
        TAtomicBlockCounter::TResult res;
        ReversedInFlight.ThresholdAdd(1, InFlightThreshold, res);
        IdleLight.Set(res.A, res.Seqno);
    }
};

} // namespace NKikimr

} // namespace NPDisk

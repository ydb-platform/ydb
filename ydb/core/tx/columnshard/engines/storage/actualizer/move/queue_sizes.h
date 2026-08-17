#pragma once

#include <util/system/types.h>

namespace NKikimr::NOlap::NActualizer {

// Per-queue breakdown of the portions MoveData still owes work for. The response
// gate only needs the total; the split tells a stalled move apart: stuck in
// Pending means accessors are not loading, in ConfirmedToMove means no rewrite
// task slot, in InFlight means tasks run but do not commit.
struct TMoveDataQueueSizes {
    ui64 Pending = 0;
    ui64 ConfirmedToMove = 0;
    ui64 InFlight = 0;

    ui64 GetTotal() const {
        return Pending + ConfirmedToMove + InFlight;
    }

    TMoveDataQueueSizes& operator+=(const TMoveDataQueueSizes& item) {
        Pending += item.Pending;
        ConfirmedToMove += item.ConfirmedToMove;
        InFlight += item.InFlight;
        return *this;
    }
};

}   // namespace NKikimr::NOlap::NActualizer

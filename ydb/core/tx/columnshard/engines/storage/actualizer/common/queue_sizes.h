#pragma once

#include <util/system/types.h>

namespace NKikimr::NOlap::NActualizer {

// The gate needs only the total; the split says where a stalled move is stuck.
struct TMoveDataQueueSizes {
    ui64 Pending = 0;
    ui64 ConfirmedToMove = 0;
    ui64 RewriteStarted = 0;
    ui64 Uncommitted = 0;
    ui64 GetTotal() const {
        return Pending + ConfirmedToMove + RewriteStarted + Uncommitted;
    }

    TMoveDataQueueSizes& operator+=(const TMoveDataQueueSizes& item) {
        Pending += item.Pending;
        ConfirmedToMove += item.ConfirmedToMove;
        RewriteStarted += item.RewriteStarted;
        Uncommitted += item.Uncommitted;
        return *this;
    }
};

}   // namespace NKikimr::NOlap::NActualizer

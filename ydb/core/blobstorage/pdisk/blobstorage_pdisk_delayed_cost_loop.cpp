#include "blobstorage_pdisk_delayed_cost_loop.h"

namespace NKikimr {
namespace NPDisk {

TDelayedCostLoop::TDelayedCostLoop(ui64 loopSize) {
    Data.resize(loopSize);
}

void TDelayedCostLoop::Clear() {
    Begin = 0;
    End = 0;
}

void TDelayedCostLoop::Push(ui64 cost) {
    if (Data.size()) {
        Data[End % Data.size()] = cost;
        End++;
        if (End > Begin + Data.size()) {
            Begin = End - Data.size();
        }
    }
}

ui64 TDelayedCostLoop::TryPop(ui64 cost) {
    while (Begin < End) {
        ui64 &item = Data[Begin % Data.size()];
        if (item >= cost) {
            item -= cost;
            return 0;
        } else { // item.cost < cost
            cost -= item;
            item = 0;
            ++Begin;
        }
    }
    return cost;
}

} // NPDisk
} // NKikimr


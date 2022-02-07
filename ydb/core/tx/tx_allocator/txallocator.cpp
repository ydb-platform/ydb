#include "txallocator.h"
#include "txallocator_impl.h"

namespace NKikimr {

IActor* CreateTxAllocator(const TActorId &tablet, TTabletStorageInfo *info) {
    return new NTxAllocator::TTxAllocator(tablet, info);
}

ui64 TEvTxAllocator::TEvAllocateResult::AllocationMask() {
    return (ui64)0xFFFF << (64 - 16);
}

ui64 TEvTxAllocator::TEvAllocateResult::ExtractPrivateMarker(ui64 item) {
    return item & AllocationMask();
}

}

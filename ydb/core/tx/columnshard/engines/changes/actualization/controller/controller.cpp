#include "controller.h"

namespace NKikimr::NOlap::NActualizer {

ui32 TController::GetLimitForAddress(const NActualizer::TRWAddress& address) const {
    if (address.WriteIs(NTiering::NCommon::DeleteTierName)) {
        return 16;
    } else if (address.ReadIs(IStoragesManager::DefaultStorageId) && address.WriteIs(IStoragesManager::DefaultStorageId)) {
        return 16;
    } else {
        // Eviction to external tier (e.g. S3): allow several concurrent tasks per (read, write) address
        // so that tiering can make progress while earlier eviction tasks are still in flight.
        return 8;
    }
}

}
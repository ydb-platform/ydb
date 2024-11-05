#include "controller.h"

namespace NKikimr::NOlap::NActualizer {

ui32 TController::GetLimitForAddress(const NActualizer::TRWAddress& address) const {
    if (address.WriteIs(NTiering::NCommon::DeleteTierName)) {
        return 16;
    } else if (address.ReadIs(IStoragesManager::DefaultStorageId) && address.WriteIs(IStoragesManager::DefaultStorageId)) {
        return 16;
    } else {
        return 1;
    }
}

}
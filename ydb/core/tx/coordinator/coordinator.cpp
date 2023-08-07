#include "coordinator.h"
#include "coordinator_impl.h"

namespace NKikimr {

IActor* CreateFlatTxCoordinator(const TActorId &tablet, TTabletStorageInfo *info) {
    return new NFlatTxCoordinator::TTxCoordinator(info, tablet);
}

} // namespace NKikimr

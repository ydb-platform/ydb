#include "coordinator.h"
#include "coordinator_impl.h"
#include <ydb/library/dbgtrace/debug_trace.h>

namespace NKikimr {

IActor* CreateFlatTxCoordinator(const TActorId &tablet, TTabletStorageInfo *info) {
    DBGTRACE("CreateFlatTxCoordinator");
    return new NFlatTxCoordinator::TTxCoordinator(info, tablet);
}

} // namespace NKikimr

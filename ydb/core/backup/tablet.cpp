#include "tablet.h"
#include "tablet_impl.h"
#include "tx_init_schema.h"
#include "tx_init.h"

namespace NKikimr::NBackup {

IActor* CreateBackupControllerTablet(const TActorId& tablet, TTabletStorageInfo* info) {
    return new TBackupControllerTablet(tablet, info);
}

} // namespace NKikimr::NBackup

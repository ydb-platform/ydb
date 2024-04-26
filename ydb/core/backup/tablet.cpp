#include "tablet.h"
#include "tablet_impl.h"

namespace NKikimr::NBackup {

IActor* CreateBackupControllerTablet(const TActorId& tablet, TTabletStorageInfo* info) {
    return new TBackupControllerTablet(tablet, info);
}

} // namespace NKikimr::NBackup

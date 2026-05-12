#include "boot_type.h"

namespace NKikimr {

ETabletBootType BootTypeFromProto(NKikimrConfig::TBootstrap::EBootType type) {
    switch (type) {
        case NKikimrConfig::TBootstrap_EBootType_NORMAL:
            return ETabletBootType::Normal;
        case NKikimrConfig::TBootstrap_EBootType_RECOVERY:
            return ETabletBootType::Recovery;
        default:
            return ETabletBootType::Normal;
    }
}

} // namespace NKikimr

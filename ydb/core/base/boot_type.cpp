#include "boot_type.h"

namespace NKikimr {

EBootType BootTypeFromProto(NKikimrConfig::TBootstrap::EBootType type) {
    switch (type) {
        case NKikimrConfig::TBootstrap_EBootType_NORMAL:
            return EBootType::Normal;
        case NKikimrConfig::TBootstrap_EBootType_RECOVERY:
            return EBootType::Recovery;
        default:
            return EBootType::Normal;
    }
}

} // namespace NKikimr

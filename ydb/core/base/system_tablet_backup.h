#pragma once

#include "tablet_types.h"

namespace NKikimr {

inline bool SupportsSystemTabletBackup(TTabletTypes::EType type) {
    switch (type) {
    case TTabletTypes::Mediator:
    case TTabletTypes::Coordinator:
    case TTabletTypes::Hive:
    case TTabletTypes::BSController:
    case TTabletTypes::SchemeShard:
    case TTabletTypes::Cms:
    case TTabletTypes::NodeBroker:
    case TTabletTypes::TxAllocator:
    case TTabletTypes::Console:
        return true;
    default:
        return false;
    }
}

} // namespace NKikimr

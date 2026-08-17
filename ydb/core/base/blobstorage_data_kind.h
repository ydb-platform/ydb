#pragma once

#include "tablet_types.h"

#include <ydb/core/protos/blobstorage_base.pb.h>

namespace NKikimr {

// Tablets whose data is required for the tenant to operate at all: without them the tenant can
// neither boot nor accept the schema operations that free the space up again. Everything not
// listed here -- including unknown and TTabletTypes::UserTypeStart types -- is user data.
inline bool IsSystemTabletType(TTabletTypes::EType type) {
    switch (type) {
        // Tenant core.
        case TTabletTypes::SchemeShard:
        case TTabletTypes::Hive:
        case TTabletTypes::Coordinator:
        case TTabletTypes::Mediator:
        case TTabletTypes::TxAllocator:
        // Cluster level.
        case TTabletTypes::BSController:
        case TTabletTypes::Console:
        case TTabletTypes::Cms:
        case TTabletTypes::NodeBroker:
        case TTabletTypes::TenantSlotBroker:
        // Sits on the data path of a whole group: the user data it relays stays USER, but its own
        // index is what records the barriers and the trash, so nothing in a group backed by it can
        // be deleted while it cannot write.
        case TTabletTypes::BlobDepot:
            return true;
        default:
            return false;
    }
}

inline NKikimrBlobStorage::TDataKind::E DataKindByTabletType(TTabletTypes::EType type) {
    return IsSystemTabletType(type)
        ? NKikimrBlobStorage::TDataKind::SYSTEM
        : NKikimrBlobStorage::TDataKind::USER;
}

// The status flag which tells a writer of the given data kind to stop writing. BlobStorage keeps
// accepting such writes for one more color past this point, which leaves room for the requests
// already in flight and for the writes a tablet must issue in order to shut down cleanly.
inline NKikimrBlobStorage::EStatusFlags StopWritingStatusFlag(NKikimrBlobStorage::TDataKind::E kind) {
    switch (kind) {
        case NKikimrBlobStorage::TDataKind::SYSTEM:
            return NKikimrBlobStorage::StatusDiskSpaceOrange;
        case NKikimrBlobStorage::TDataKind::USER:
            return NKikimrBlobStorage::StatusDiskSpaceYellowStop;
        default:
            return NKikimrBlobStorage::StatusDiskSpaceYellowStop;
    }
}

} // namespace NKikimr

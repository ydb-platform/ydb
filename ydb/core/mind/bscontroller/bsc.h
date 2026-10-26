#pragma once

#include <ydb/core/base/blobstorage.h>

namespace NKikimr {

    enum EBlobStorageControllerInterfaceVersion : ui32 {
        BSC_INTERFACE_VERSION = 5, // current interface version

        // features of BSC
        BSC_INTERFACE_REPLACE_CONFIG = 1, // version that supports TEvControllerReplaceConfigRequest
        BSC_INTERFACE_DISTCONF_CONTROL = 2, // TEvControllerDistconfRequest
        BSC_INTERFACE_POPULATE_PDISK = 3, // TPopulatePDisk command in TConfigRequest
        BSC_INTERFACE_BRIDGE_PILE_OPS = 4, // ValidateAddBridgePile and pending pile operations in ClusterStateDetails
        BSC_INTERFACE_REMOVE_BRIDGE_PILE = 5, // ValidateRemoveBridgePile and pending REMOVE pile operation
    };

IActor* CreateFlatBsController(const TActorId &tablet, TTabletStorageInfo *info);

} //NKikimr

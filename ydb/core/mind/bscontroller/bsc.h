#pragma once

#include <ydb/core/base/blobstorage.h>

namespace NKikimr {

    enum EBlobStorageControllerInterfaceVersion : ui32 {
        BSC_INTERFACE_VERSION = 3, // current interface version

        // features of BSC
        BSC_INTERFACE_REPLACE_CONFIG = 1, // version that supports TEvControllerReplaceConfigRequest
        BSC_INTERFACE_DISTCONF_CONTROL = 2, // TEvControllerDistconfRequest
        BSC_INTERFACE_POPULATE_PDISK = 3, // TPopulatePDisk command in TConfigRequest
    };

IActor* CreateFlatBsController(const TActorId &tablet, TTabletStorageInfo *info);

} //NKikimr

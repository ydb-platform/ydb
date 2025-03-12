#pragma once

#include <ydb/core/base/blobstorage.h>

namespace NKikimr {

    enum EBlobStorageControllerInterfaceVersion : ui32 {
        BSC_INTERFACE_VERSION = 2, // current interface version

        // features of BSC
        BSC_INTERFACE_REPLACE_CONFIG = 1, // version that supports TEvControllerReplaceConfigRequest
        BSC_INTERFACE_DISTCONF_CONTROL = 2, // TEvControllerDistconfRequest
    };

IActor* CreateFlatBsController(const TActorId &tablet, TTabletStorageInfo *info);

} //NKikimr

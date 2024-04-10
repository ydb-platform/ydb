#pragma once

#include "defs.h"
#include <ydb/core/protos/blobstorage.pb.h>

namespace NKikimr {

    ///////////////////////////////////////////////////////////////////////////////////
    // Settings for TEvVPut requests
    // How do we handle TEvVPut and with what priority
    ///////////////////////////////////////////////////////////////////////////////////
    namespace NPriPut {

        enum EHandleType {
            Log = 0,                // write record to log
            HugeForeground = 1,     // huge blob, write it with high priority
            HugeBackground = 2      // huge blog, write it with low priority
        };

        EHandleType HandleType(const ui32 minREALHugeBlobSize, NKikimrBlobStorage::EPutHandleClass handleClass,
                               ui32 originalBufSizeWithoutOverhead, bool addHeader);

    } // NPriPut

} // NKikimr

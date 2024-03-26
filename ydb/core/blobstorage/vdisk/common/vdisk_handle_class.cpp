#include "vdisk_handle_class.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_blob.h>

namespace NKikimr {

    ///////////////////////////////////////////////////////////////////////////////////
    // Settings for TEvVPut requests
    ///////////////////////////////////////////////////////////////////////////////////
    namespace NPriPut {

        EHandleType HandleType(const ui32 minREALHugeBlobSize, NKikimrBlobStorage::EPutHandleClass handleClass,
                               ui32 originalBufSizeWithoutOverhead, bool addHeader) {
            // what size of huge blob it would be, if it huge
            const ui64 hugeBlobSize = (addHeader ? TDiskBlob::HeaderSize : 0) + originalBufSizeWithoutOverhead;

            switch (handleClass) {
                case NKikimrBlobStorage::TabletLog:
                    return (hugeBlobSize >= minREALHugeBlobSize ? HugeForeground : Log);
                case NKikimrBlobStorage::AsyncBlob:
                    return (hugeBlobSize >= minREALHugeBlobSize ? HugeBackground : Log);
                case NKikimrBlobStorage::UserData:
                    return (hugeBlobSize >= minREALHugeBlobSize ? HugeForeground : Log);
                default:
                    Y_ABORT("Unexpected case");
            }
        }

    } // NPriPut
} // NKikimr

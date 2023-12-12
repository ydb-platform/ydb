#pragma once

#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>


namespace NKikimr {

    TVector<ui8> PartsToSendOnMain(
        const TBlobStorageGroupInfo::TTopology& top,
        const TVDiskIdShort &vdisk,
        const TLogoBlobID &key,
        const TIngress& ingress
    );
    TVector<ui8> PartsToDelete(
        const TBlobStorageGroupInfo::TTopology& top,
        const TVDiskIdShort &vdisk,
        const TLogoBlobID &key,
        const TIngress& ingress
    );
    TVDiskID GetVDiskId(const TBlobStorageGroupInfo& gInfo, const TLogoBlobID& key);

} // NKikimr

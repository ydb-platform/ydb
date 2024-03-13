#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>


namespace NKikimr {
namespace NBalancing {

    TVector<ui8> PartIdsToSendOnMain(
        const TBlobStorageGroupInfo::TTopology& top,
        const TVDiskIdShort &vdisk,
        const TLogoBlobID &key,
        const TIngress& ingress
    );
    TVector<ui8> PartIdsToDelete(
        const TBlobStorageGroupInfo::TTopology& top,
        const TVDiskIdShort &vdisk,
        const TLogoBlobID &key,
        const TIngress& ingress
    );
    TVDiskID GetMainReplicaVDiskId(const TBlobStorageGroupInfo& gInfo, const TLogoBlobID& key);

    struct TPartsCollectorMerger {
        const TBlobStorageGroupType GType;
        
        TIngress Ingress;
        TVector<std::optional<std::variant<TDiskPart, TRope>>> Parts;

        TPartsCollectorMerger(const TBlobStorageGroupType gType);

        static bool HaveToMergeData() { return true; }

        void AddFromSegment(const TMemRecLogoBlob& memRec, const TDiskPart *outbound, const TKeyLogoBlob&, ui64);
        void AddFromFresh(const TMemRecLogoBlob& memRec, const TRope* data, const TKeyLogoBlob& key, ui64 /*lsn*/);
        void Clear();
    };

} // NBalancing
} // NKikimr

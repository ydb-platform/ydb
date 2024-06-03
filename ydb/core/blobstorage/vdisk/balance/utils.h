#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>


namespace NKikimr {
namespace NBalancing {

    TVDiskID GetMainReplicaVDiskId(const TBlobStorageGroupInfo& gInfo, const TLogoBlobID& key);

    struct TPartsCollectorMerger {
        const TBlobStorageGroupType GType;

        TIngress Ingress;
        TVector<std::pair<NMatrix::TVectorType, std::variant<TDiskPart, TRope>>> Parts;

        TPartsCollectorMerger(const TBlobStorageGroupType gType);

        static bool HaveToMergeData() { return true; }

        void AddFromSegment(const TMemRecLogoBlob& memRec, const TDiskPart *outbound, const TKeyLogoBlob&, ui64);
        void AddFromFresh(const TMemRecLogoBlob& memRec, const TRope* data, const TKeyLogoBlob& key, ui64 /*lsn*/);
        void Clear();
    };


    struct TWaiter {
        enum EState {
            EInit,
            EStartJob,
            ECompleteJob,
            EPassAway
        };

        EState State = EInit;

        const TDuration SendTimeout = TDuration::Seconds(10);
        TInstant SendPartsStart;
        ui32 ToSendPartsCount = 0;
        ui32 SentPartsCount = 0;
        ui32 PartsLeft = 0;
        ui64 Epoch = 0;

        void Init();
        ui64 StartJob(TInstant now, ui32 count, ui32 partsLeft);
        void PartJobDone(ui64 epoch, ui32 cnt=1);
        NActors::TEvents::TEvCompleted* IsJobDone(ui32 epoch, TInstant now);
        bool IsPassAway();
    };

} // NBalancing
} // NKikimr

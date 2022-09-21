#pragma once
#include "defs.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/vdisk/common/blobstorage_vdisk_guids.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>


namespace NKikimr {

    struct TPDiskParams;
    class TBlobStorageGroupInfo;
    class THugeBlobCtx;

    ////////////////////////////////////////////////////////////////////////////
    // TEvFrontRecoveryStatus
    ////////////////////////////////////////////////////////////////////////////
    class TEvFrontRecoveryStatus : public TEventLocal<TEvFrontRecoveryStatus, TEvBlobStorage::EvFrontRecoveryStatus> {
    public:
        enum EPhase {
            LocalRecoveryDone,      // Local recovery finished
            SyncGuidRecoveryDone,   // Sync Guid recovery finished
        };

        const EPhase Phase;
        const NKikimrProto::EReplyStatus Status;
        const TIntrusivePtr<TPDiskParams> Dsk;
        const std::shared_ptr<THugeBlobCtx> HugeBlobCtx;
        const TVDiskIncarnationGuid VDiskIncarnationGuid;

        TEvFrontRecoveryStatus(EPhase phase,
                               NKikimrProto::EReplyStatus status,
                               const TIntrusivePtr<TPDiskParams> &dsk,
                               std::shared_ptr<THugeBlobCtx> hugeBlobCtx,
                               TVDiskIncarnationGuid vdiskIncarnationGuid);
        ~TEvFrontRecoveryStatus();
    };

    struct TVDiskConfig;
    IActor* CreateVDiskSkeletonFront(const TIntrusivePtr<TVDiskConfig> &cfg,
                                     const TIntrusivePtr<TBlobStorageGroupInfo> &info,
                                     const TIntrusivePtr<::NMonitoring::TDynamicCounters> &counters);

} // NKikimr

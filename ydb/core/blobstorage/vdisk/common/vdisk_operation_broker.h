#pragma once

#include <ydb/core/base/blobstorage.h>

namespace NKikimr {

    struct TEvAcquireVDiskOperationToken
        : public TEventLocal<TEvAcquireVDiskOperationToken, TEvBlobStorage::EvAcquireVDiskOperationToken>
    {
        TActorId VDiskServiceId;
        ui32 PDiskId = 0;

        TEvAcquireVDiskOperationToken(const TActorId& vdiskServiceId, ui32 pdiskId)
            : VDiskServiceId(vdiskServiceId)
            , PDiskId(pdiskId)
        {}
    };

    struct TEvVDiskOperationToken
        : public TEventLocal<TEvVDiskOperationToken, TEvBlobStorage::EvVDiskOperationToken>
    {};

    struct TEvReleaseVDiskOperationToken
        : public TEventLocal<TEvReleaseVDiskOperationToken, TEvBlobStorage::EvReleaseVDiskOperationToken>
    {
        TActorId VDiskServiceId;
        ui32 PDiskId = 0;

        TEvReleaseVDiskOperationToken(const TActorId& vdiskServiceId, ui32 pdiskId)
            : VDiskServiceId(vdiskServiceId)
            , PDiskId(pdiskId)
        {}
    };

    class TControlWrapper;

    IActor* CreateVDiskOperationBrokerActor(const TControlWrapper& maxInProgressCount,
        const TControlWrapper& maxInProgressPerPDiskCount);

} // NKikimr

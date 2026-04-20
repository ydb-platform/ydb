#pragma once

#include <ydb/core/base/blobstorage.h>

namespace NKikimr {

    struct TEvAcquireVDiskOperationToken
        : public TEventLocal<TEvAcquireVDiskOperationToken, TEvBlobStorage::EvAcquireVDiskOperationToken>
    {
        TActorId VDiskServiceId;

        explicit TEvAcquireVDiskOperationToken(const TActorId& vdiskServiceId)
            : VDiskServiceId(vdiskServiceId)
        {}
    };

    struct TEvVDiskOperationToken
        : public TEventLocal<TEvVDiskOperationToken, TEvBlobStorage::EvVDiskOperationToken>
    {};

    struct TEvReleaseVDiskOperationToken
        : public TEventLocal<TEvReleaseVDiskOperationToken, TEvBlobStorage::EvReleaseVDiskOperationToken>
    {
        TActorId VDiskServiceId;

        explicit TEvReleaseVDiskOperationToken(const TActorId& vdiskServiceId)
            : VDiskServiceId(vdiskServiceId)
        {}
    };

    class TControlWrapper;

    IActor* CreateVDiskOperationBrokerActor(const TControlWrapper& maxInProgressCount,
        const TControlWrapper& maxInProgressPerPDiskCount);

} // NKikimr

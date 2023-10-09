#pragma once

#include "defs.h"
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/base/blobstorage_vdiskid.h>


namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TEvAnubisVGet
    // Anubis want to read status for these blobs
    ////////////////////////////////////////////////////////////////////////////
    struct TEvAnubisVGet
        : public TEventLocal<TEvAnubisVGet, TEvBlobStorage::EvAnubisVGet>
    {
        TVector<TLogoBlobID> Candidates;

        TEvAnubisVGet(TVector<TLogoBlobID> &&c)
            : Candidates(std::move(c))
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // TEvAnubisVGetResult
    // Anubis want to read status for these blobs
    ////////////////////////////////////////////////////////////////////////////
    struct TEvAnubisVGetResult
        : public TEventLocal<TEvAnubisVGetResult, TEvBlobStorage::EvAnubisVGetResult>
    {
        TVDiskIdShort VDiskIdShort;
        TEvBlobStorage::TEvVGetResult::TPtr Ev;

        TEvAnubisVGetResult(TVDiskIdShort vd, TEvBlobStorage::TEvVGetResult::TPtr &ev)
            : VDiskIdShort(vd)
            , Ev(ev)
        {}
    };


    ////////////////////////////////////////////////////////////////////////////
    // CreateAnubisProxy
    ////////////////////////////////////////////////////////////////////////////
    IActor* CreateAnubisProxy(const TIntrusivePtr<TVDiskContext> &vctx,
                              const TIntrusivePtr<TBlobStorageGroupInfo> &ginfo,
                              const TVDiskIdShort &vd,
                              ui32 replInterconnectChannel);

} // NKikimr

#pragma once

#include "defs.h"
#include "blobstorage_synclogdata.h"
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/base/blobstorage.h>

namespace NKikimr {
    namespace NSyncLog {

        struct TEvSyncLogTrim
            : public TEventLocal<TEvSyncLogTrim, TEvBlobStorage::EvSyncLogTrim>
        {
            const ui64 Lsn; // all disks confirmed that they got a record with this Lsn
            TEvSyncLogTrim(ui64 lsn)
                : Lsn(lsn)
            {}
        };

        struct TEvSyncLogFreeChunk
            : public TEventLocal<TEvSyncLogFreeChunk, TEvBlobStorage::EvSyncLogFreeChunk>
        {
            const ui32 ChunkIdx;
            TEvSyncLogFreeChunk(ui32 chunkIdx)
                : ChunkIdx(chunkIdx)
            {}
        };

        struct TEvSyncLogSnapshot
            : public TEventLocal<TEvSyncLogSnapshot, TEvBlobStorage::EvSyncLogSnapshot>
        {
            const bool IntrospectionInfo;
            TEvSyncLogSnapshot(bool introspectionInfo = false)
                : IntrospectionInfo(introspectionInfo)
            {}
        };

        class TSyncLogSnapshot;
        struct TEvSyncLogSnapshotResult
            : public TEventLocal<TEvSyncLogSnapshotResult,
                                 TEvBlobStorage::EvSyncLogSnapshotResult>
        {
            TIntrusivePtr<TSyncLogSnapshot> SnapshotPtr;
            TString SublogContent = {};

            TEvSyncLogSnapshotResult(const TIntrusivePtr<TSyncLogSnapshot> &ptr, const TString &sublogContent);
            ~TEvSyncLogSnapshotResult();
        };

        struct TEvSyncLogReadFinished
            : TEventLocal<TEvSyncLogReadFinished, TEvBlobStorage::EvSyncLogReadFinished>
        {
            const TVDiskID VDiskID;
            TEvSyncLogReadFinished(const TVDiskID &vdisk)
                : VDiskID(vdisk)
            {}
        };

        struct TEvSyncLogLocalStatus
            : TEventLocal<TEvSyncLogLocalStatus, TEvBlobStorage::EvSyncLogLocalStatus>
        {
        };

        struct TEvSyncLogLocalStatusResult
            : TEventLocal<TEvSyncLogLocalStatusResult, TEvBlobStorage::EvSyncLogLocalStatusResult>
        {
            TLogEssence Essence;

            TEvSyncLogLocalStatusResult(const TLogEssence &e)
                : Essence(e)
            {}
        };

    } // NSyncLog
} // NKikimr

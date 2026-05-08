#pragma once

#include "defs.h"
#include "blobstorage_synclogdata.h"
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/vdisk/synclog/phantom_flag_storage/phantom_flag_storage_data.h>
#include <ydb/core/blobstorage/vdisk/synclog/phantom_flag_storage/phantom_flag_storage_snapshot.h>
#include <ydb/core/blobstorage/vdisk/synclog/phantom_flag_storage/phantom_flags.h>
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

        struct TEvPhantomFlagStorageFinishBuilder
                : public TEventLocal<TEvPhantomFlagStorageFinishBuilder,
                                     TEvBlobStorage::EvPhantomFlagStorageFinishBuilder>
        {
            TEvPhantomFlagStorageFinishBuilder(TPhantomFlags&& flags, TPhantomFlagThresholds&& thresholds)
                : Flags(std::move(flags))
                , Thresholds(std::move(thresholds))
            {}

            TPhantomFlags Flags;
            TPhantomFlagThresholds Thresholds;
        };

        struct TEvPhantomFlagStorageGetSnapshot
                : public TEventLocal<TEvPhantomFlagStorageGetSnapshot,
                                     TEvBlobStorage::EvPhantomFlagStorageGetSnapshot>
        {
            // Persistent PhantomFlagStorage  also includes flags from the main synclog
            TSyncLogSnapshotPtr SyncLogSnapshot;
        };

        struct TEvPhantomFlagStorageGetSnapshotResult
                : public TEventLocal<TEvPhantomFlagStorageGetSnapshotResult,
                                     TEvBlobStorage::EvPhantomFlagStorageGetSnapshotResult>
        {
            TEvPhantomFlagStorageGetSnapshotResult(TPhantomFlagStorageSnapshot&& snapshot);

            TPhantomFlagStorageSnapshot Snapshot;
        };

        struct TEvSyncLogUpdateNeighbourSyncedLsn
            : public TEventLocal<TEvSyncLogUpdateNeighbourSyncedLsn,
                                 TEvBlobStorage::EvSyncLogUpdateNeighbourSyncedLsn>
        {
            ui32 OrderNumber;
            ui64 SyncedLsn;

            TEvSyncLogUpdateNeighbourSyncedLsn(ui32 orderNumber, ui64 syncedLsn)
                : OrderNumber(orderNumber)
                , SyncedLsn(syncedLsn)
            {}
        };

        struct TEvPhantomFlagStorageWriteItems
                : public TEventLocal<TEvPhantomFlagStorageWriteItems,
                                     TEvBlobStorage::EvPhantomFlagStorageWriteItems>
        {
            TEvPhantomFlagStorageWriteItems(std::vector<TPhantomFlagStorageItem>&& items);

            std::vector<TPhantomFlagStorageItem> Items;
        };

        struct TEvPhantomFlagStorageCommitData
                : public TEventLocal<TEvPhantomFlagStorageCommitData,
                                     TEvBlobStorage::EvPhantomFlagStorageCommitData>
        {
            TEvPhantomFlagStorageCommitData(const std::optional<TPhantomFlagStorageData>& data,
                    std::vector<ui32> retiredChunks);

            std::optional<TPhantomFlagStorageData> Data;
            std::vector<ui32> RetiredChunks;
        };

        struct TEvPhantomFlagStorageDrop
                : public TEventLocal<TEvPhantomFlagStorageDrop,
                                     TEvBlobStorage::EvPhantomFlagStorageDrop>
        {};

        struct TEvPhantomFlagExtractedFromChunk
                : public TEventLocal<TEvPhantomFlagExtractedFromChunk,
                                     TEvBlobStorage::EvPhantomFlagExtractedFromChunk>
        {
            TEvPhantomFlagExtractedFromChunk(ui32 chunkIdx, TPhantomFlags&& flags);

            ui32 ChunkIdx;
            TPhantomFlags Flags;
        };

    } // NSyncLog
} // NKikimr

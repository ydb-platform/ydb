#include "blobstorage_synclog_private_events.h"
#include "blobstorage_synclogdata.h"

namespace NKikimr {
    namespace NSyncLog {

        TEvSyncLogSnapshotResult::TEvSyncLogSnapshotResult(
                const TIntrusivePtr<TSyncLogSnapshot> &ptr,
                const TString &sublogContent)
            : SnapshotPtr(ptr)
            , SublogContent(sublogContent)
        {}

        TEvSyncLogSnapshotResult::~TEvSyncLogSnapshotResult() = default;

        TEvPhantomFlagStorageGetSnapshotResult::TEvPhantomFlagStorageGetSnapshotResult(
                TPhantomFlagStorageSnapshot&& snapshot)
            : Snapshot(std::move(snapshot))
        {}

        TEvPhantomFlagStorageWriteItems::TEvPhantomFlagStorageWriteItems(
                std::vector<TPhantomFlagStorageItem>&& items)
            : Items(std::move(items))
        {}

        TEvPhantomFlagStorageCommitData::TEvPhantomFlagStorageCommitData(
                const std::optional<TPhantomFlagStorageData>& data,
                std::vector<ui32> retiredChunks)
            : Data(data)
            , RetiredChunks(std::move(retiredChunks))
        {}

        TEvPhantomFlagExtractedFromChunk::TEvPhantomFlagExtractedFromChunk(
                ui32 chunkIdx, TPhantomFlags&& flags)
            : ChunkIdx(chunkIdx)
            , Flags(std::move(flags))
        {}

    } // NSyncLog
} // NKikimr

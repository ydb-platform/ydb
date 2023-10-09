#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/common/disk_part.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/base/utility.h>

#include <util/generic/set.h>

namespace NKikimr {

    class TPDiskCtx;
    using TPDiskCtxPtr = std::shared_ptr<TPDiskCtx>;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TBulkFormedSstInfo
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    struct TBulkFormedSstInfo {
        // Bulk-formed SSTable info structure is created together with replicated SSTable and is used to store correct
        // LSN range of replicated blobs (as they are not known when SSTable is written) and to keep SSTable information
        // when it is deleted from index but still needed to recover SyncLog.
        //
        // Lifecycle:
        // 1. Replicated SSTable is created and added to index; matching bulk-formed segment is added to index too
        //
        // 2. First case: replicated SSTable is deleted from index during compaction, but SyncLog LSN is still less or
        //    equal to LastLsn of replicated SSTable -- in this case we move ChunkIds to bulk-formed segment info and
        //    it acquires ownership of these chunks. When SyncLog passes LastLsn point, during next compaction these
        //    chunks are freed and SSTable is destroyed. Also bulk-formed segment is removed.
        //
        // 3. Second case: SyncLog passes LastLsn point before SSTable is compacted. In this case nothing happens :)
        //    During compaction of replicated SSTable its chunk will be deleted as usual and matching bulk-formed segment
        //    too.
        //
        // This lifecycle leads to these possible situations:
        // 1. RemovedFromIndex=false ChunkIds empty -- normal situation, just created SSTable.
        // 2. RemovedFromIndex=true ChunkIds not empty -- SSTable compacted and deleted from index, but still needed to
        //    recover SyncLog correctly; ChunkIds are freed during next compaction when SyncLogFirstLsnToKeep > LastLsn.
        //    *** this is an obsolete case and we don't handle SyncLog through bulk formed segments now ***
        // 3. RemovedFromIndex=true ChunkIds empty -- SSTable compated and deleted and is not needed to recover SyncLog.
        //    In this case bulk-formed segment is marked obsolete and is collected during compaction.

        // LSN of first BLOB within this SSTable
        ui64 FirstBlobLsn;

        // LSN of last BLOB within this SSTable (used to determine whether this SSTable is needed by SyncLog)
        ui64 LastBlobLsn;

        // entry point to identify SSTable on disk (in order to read it on recovery)
        TDiskPart EntryPoint;

        // IDs of chunks composing this SSTable (stored only when SSTable is dropped from index, otherwise empty)
        TVector<ui32> ChunkIds;

        // is this record removed from index?
        bool RemovedFromIndex;

        TBulkFormedSstInfo(ui64 firstBlobLsn, ui64 lastBlobLsn, const TDiskPart& entryPoint)
            : FirstBlobLsn(firstBlobLsn)
            , LastBlobLsn(lastBlobLsn)
            , EntryPoint(entryPoint)
            , RemovedFromIndex(false)
        {
            Y_ABORT_UNLESS(firstBlobLsn && lastBlobLsn && !entryPoint.Empty(),
                    "firstBlobLsn# %" PRIu64 " lastBlobLsn# %" PRIu64 " entryPoint# %s",
                    firstBlobLsn, lastBlobLsn, entryPoint.ToString().data());
        }

        TBulkFormedSstInfo(const NKikimrVDiskData::TBulkFormedSstInfo& proto)
            : FirstBlobLsn(proto.GetFirstBlobLsn())
            , LastBlobLsn(proto.GetLastBlobLsn())
            , EntryPoint(proto.GetEntryPoint())
            , RemovedFromIndex(proto.GetRemovedFromIndex())
        {
            Y_ABORT_UNLESS(FirstBlobLsn && LastBlobLsn && !EntryPoint.Empty(),
                    "FirstBlobLsn# %" PRIu64 " LastBlobLsn# %" PRIu64 " EntryPoint# %s",
                    FirstBlobLsn, LastBlobLsn, EntryPoint.ToString().data());

            const auto& chunkIds = proto.GetChunkIds();
            ChunkIds.reserve(proto.ChunkIdsSize());
            ChunkIds.insert(ChunkIds.end(), chunkIds.begin(), chunkIds.end());
        }

        void SerializeToProto(NKikimrVDiskData::TBulkFormedSstInfo &pb) const {
            // ensure we are in one of correct states -- either we are not removed from index and ChunkIds are empty
            // (because they are owned by matching SSTable), or we are removed from index, but ChunkIds are not empty
            // as we own them, because they are needed to recover SyncLog; in case when RemovedFromIndex=true and
            // ChunkIds are empty this bulk-formed segment must be collected and not written to index
            Y_ABORT_UNLESS(RemovedFromIndex == !ChunkIds.empty());

            pb.SetFirstBlobLsn(FirstBlobLsn);
            pb.SetLastBlobLsn(LastBlobLsn);
            EntryPoint.SerializeToProto(*pb.MutableEntryPoint());
            for (ui32 chunkId : ChunkIds) {
                pb.AddChunkIds(chunkId);
            }
            if (RemovedFromIndex != pb.GetRemovedFromIndex()) {
                // set obsolete flag if it differs from its default value
                pb.SetRemovedFromIndex(RemovedFromIndex);
            }
        }

        void GetOwnedChunks(TSet<TChunkIdx>& chunks) const {
            for (TChunkIdx chunkIdx : ChunkIds) {
                const bool inserted = chunks.insert(chunkIdx).second;
                Y_ABORT_UNLESS(inserted);
            }
        }

        friend bool operator <(const TBulkFormedSstInfo& x, const TBulkFormedSstInfo& y) {
            return x.EntryPoint < y.EntryPoint;
        }

        friend bool operator <(const TBulkFormedSstInfo& x, const TDiskPart& y) {
            return x.EntryPoint < y;
        }
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TBulkFormedSstInfoSet
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    class TBulkFormedSstInfoSet {
    public:
        TBulkFormedSstInfoSet() = default;
        TBulkFormedSstInfoSet(const NKikimrVDiskData::TBulkFormedSstInfoSet& pb);

        // mark specific SSTable (generated by replication job) as deleted when compaction is done and this SSTable
        // is not stored in index anymore
        void RemoveSstFromIndex(const TDiskPart& entryPoint);

        // generate compacted bulk-formed SSTable info set
        void ApplyCompactionResult(TBulkFormedSstInfoSet& output, TVector<TChunkIdx>& deleteChunks);

        void AddBulkFormedSst(ui64 firstLsn, ui64 lastLsn, const TDiskPart& entryPoint);

        // create actor which loads bulk-formed segments necessary for SyncLog recovery
        IActor *CreateLoaderActor(TVDiskContextPtr vctx, TPDiskCtxPtr pdiskCtx, ui64 syncLogMaxLsnStored,
            const TActorId& localRecoveryActorId);

        void SerializeToProto(NKikimrVDiskData::TBulkFormedSstInfoSet &pb) const;
        static bool ConvertToProto(NKikimrVDiskData::TBulkFormedSstInfoSet &pb, const char *begin, const char *end);

        // finds bulk-formed segment that is still in index; it is identified by its entrypoint and it must exist
        const TBulkFormedSstInfo& FindIntactBulkFormedSst(const TDiskPart& entryPoint) const;
        TBulkFormedSstInfo& FindIntactBulkFormedSst(const TDiskPart& entryPoint);

        void GetOwnedChunks(TSet<TChunkIdx>& chunks) const;

    private:
        TVector<TBulkFormedSstInfo> BulkFormedSsts;
        const TBulkFormedSstInfo& FindIntactBulkFormedSstPrivate(const TDiskPart& entryPoint) const;
    };

} // NKikimr

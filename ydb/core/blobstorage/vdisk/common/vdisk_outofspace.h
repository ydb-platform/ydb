#pragma once
#include "defs.h"

#include <ydb/core/blobstorage/base/blobstorage_oos_defs.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_defs.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_util_space_color.h>
#include <ydb/core/protos/node_whiteboard.pb.h>

namespace NKikimr {

    using TSpaceColor = NKikimrBlobStorage::TPDiskSpaceColor;
    using ESpaceColor = TSpaceColor::E;

    ////////////////////////////////////////////////////////////////////////////
    // TOutOfSpaceState -- global state for disk space availability
    ////////////////////////////////////////////////////////////////////////////
    class TOutOfSpaceState {
    public:

        TOutOfSpaceState(ui32 totalVDisks, ui32 selfOrderNum);
        static NKikimrWhiteboard::EFlag ToWhiteboardFlag(const ESpaceColor color);
        // update flags for vdisk with vdiskOrderNum
        void Update(ui32 vdiskOrderNum, NPDisk::TStatusFlags flags);

        NKikimrWhiteboard::EFlag GlobalWhiteboardFlag() const {
            return ToWhiteboardFlag(GetGlobalColor());
        }

        NKikimrWhiteboard::EFlag LocalWhiteboardFlag() const {
            return ToWhiteboardFlag(GetLocalColor());
        }

        ESpaceColor GetGlobalColor() const {
            return StatusFlagToSpaceColor(static_cast<NPDisk::TStatusFlags>(AtomicGet(GlobalFlags)));
        }

        ESpaceColor GetLocalColor() const {
            return StatusFlagToSpaceColor(GetLocalStatusFlags());
        }

        // update state with flags received from local PDisk
        void UpdateLocalChunk(NPDisk::TStatusFlags flags) {
            if (flags & NKikimrBlobStorage::StatusIsValid && flags != AtomicGet(ChunkFlags)) {
                AtomicSet(ChunkFlags, flags);
                Update(SelfOrderNum, flags | AtomicGet(LogFlags));
            }
        }

        void UpdateLocalLog(NPDisk::TStatusFlags flags) {
            if (flags & NKikimrBlobStorage::StatusIsValid && flags != AtomicGet(LogFlags)) {
                AtomicSet(LogFlags, flags);
                Update(SelfOrderNum, flags | AtomicGet(ChunkFlags));
            }
        }

        void UpdateLocalFreeSpaceShare(ui64 freeSpaceShare24bit) {
            AtomicSet(ApproximateFreeSpaceShare24bit, freeSpaceShare24bit);
        }

        void UpdateLocalUsedChunks(ui32 usedChunks) {
            AtomicSet(LocalUsedChunks, static_cast<TAtomicBase>(usedChunks));
        }

        NPDisk::TStatusFlags GetLocalStatusFlags() const {
            return static_cast<NPDisk::TStatusFlags>(AtomicGet(AllVDiskFlags[SelfOrderNum]));
        }

        NPDisk::TStatusFlags GetLocalChunkStatusFlags() const {
            return static_cast<NPDisk::TStatusFlags>(AtomicGet(ChunkFlags));
        }

        NPDisk::TStatusFlags GetLocalLogStatusFlags() const {
            return static_cast<NPDisk::TStatusFlags>(AtomicGet(LogFlags));
        }

        TOutOfSpaceStatus GetGlobalStatusFlags() const {
            return TOutOfSpaceStatus(static_cast<NPDisk::TStatusFlags>(AtomicGet(GlobalFlags)), GetFreeSpaceShare());
        }

        // free space share as a fraction of 1 -- [0, 1)
        float GetFreeSpaceShare() const {
            return static_cast<float>(AtomicGet(ApproximateFreeSpaceShare24bit)) / 16'777'216.0f;
        }

        ui32 GetLocalUsedChunks() const {
            return static_cast<ui32>(AtomicGet(LocalUsedChunks));
        }

    private:
        // Log space flags.
        TAtomic LogFlags = 0;
        // Chunk space flags.
        TAtomic ChunkFlags = 0;
        // Flag for every VDisk in the BlobStorage group
        TAtomic AllVDiskFlags[MaxVDisksInGroup];
        // Cached global flags (obtained by merging AllVDiskFlags)
        TAtomic GlobalFlags = 0;
        // Approximate free space share (to calculate percentage of free/used space)
        TAtomic ApproximateFreeSpaceShare24bit = 0;
        // Total VDisks in the group
        const ui32 TotalVDisks;
        // VDisk order number for self
        const ui32 SelfOrderNum;
        // Chunks used locally by VDisk
        TAtomic LocalUsedChunks = 0;
    };

    ////////////////////////////////////////////////////////////////////////////
    // THugeHeapFragmentation - global stat about huge heap fragmentation
    ////////////////////////////////////////////////////////////////////////////
    class THugeHeapFragmentation {
    public:
        struct TStat {
            ui32 CurrentlyUsedChunks;
            ui32 CanBeFreedChunks;
        };

    private:
        union TStore {
            TStat Stat;
            TAtomicBase Data;
        };

        TAtomic Data;

    public:
        THugeHeapFragmentation() {
            AtomicSet(Data, static_cast<TAtomicBase>(0));
        }

        TStat Get() const {
            TStore store;
            store.Data = AtomicGet(Data);
            return store.Stat;
        }

        void Set(TStat stat) {
            Set(stat.CurrentlyUsedChunks, stat.CanBeFreedChunks);
        }

        void Set(ui32 currentlyUsedChunks, ui32 canBeFreedChunks) {
            TStore store;
            store.Stat.CurrentlyUsedChunks = currentlyUsedChunks;
            store.Stat.CanBeFreedChunks = canBeFreedChunks;
            AtomicSet(Data, store.Data);
        }
    };

} // NKikimr

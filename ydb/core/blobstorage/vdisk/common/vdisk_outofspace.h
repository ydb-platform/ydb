#pragma once
#include "defs.h"

#include <ydb/core/blobstorage/base/blobstorage_oos_defs.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_defs.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_util_space_color.h>
#include <ydb/core/protos/node_whiteboard.pb.h>
#include <util/system/spinlock.h>

#include <optional>

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

        // Authoritative update from the serialized TEvCheckSpace poll. It may
        // move the known state in either direction.
        void UpdateLocalChunk(NPDisk::TStatusFlags flags,
                std::optional<ui64> expectedObservationGeneration = std::nullopt) {
            UpdateLocalAuthoritative(flags, ChunkFlags, LogFlags, expectedObservationGeneration);
        }

        void UpdateLocalLog(NPDisk::TStatusFlags flags,
                std::optional<ui64> expectedObservationGeneration = std::nullopt) {
            UpdateLocalAuthoritative(flags, LogFlags, ChunkFlags, expectedObservationGeneration);
        }

        // Ordinary PDisk replies may be delivered out of order. Such a reply
        // is only an observation and must never overwrite a newer, worse
        // state with an older, better one. TEvCheckSpace is the sole source
        // allowed to authoritatively improve the state.
        void ObserveLocalChunk(NPDisk::TStatusFlags flags) {
            ObserveLocal(flags, ChunkFlags, LogFlags);
        }

        void ObserveLocalLog(NPDisk::TStatusFlags flags) {
            ObserveLocal(flags, LogFlags, ChunkFlags);
        }

        ui64 GetLocalSpaceObservationGeneration() const {
            return static_cast<ui64>(AtomicGet(LocalSpaceObservationGeneration));
        }

        void UpdateLocalFreeSpaceShare(ui64 freeSpaceShare24bit) {
            AtomicSet(ApproximateFreeSpaceShare24bit, freeSpaceShare24bit);
        }

        void UpdateLocalUsedChunks(ui32 usedChunks) {
            AtomicSet(LocalUsedChunks, static_cast<TAtomicBase>(usedChunks));
        }

        void UpdateLocalTotalChunks(ui32 totalChunks) {
            AtomicSet(LocalTotalChunks, static_cast<TAtomicBase>(totalChunks));
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

        ui32 GetLocalTotalChunks() const {
            return static_cast<ui32>(AtomicGet(LocalTotalChunks));
        }

    private:
        void UpdateLocalAuthoritative(NPDisk::TStatusFlags flags, TAtomic& observed, const TAtomic& other,
                std::optional<ui64> expectedObservationGeneration) {
            if (!(flags & NKikimrBlobStorage::StatusIsValid)) {
                return;
            }

            TGuard<TSpinLock> guard(LocalFlagsLock);
            const auto current = static_cast<NPDisk::TStatusFlags>(AtomicGet(observed));
            if (expectedObservationGeneration
                    && *expectedObservationGeneration
                        != static_cast<ui64>(AtomicGet(LocalSpaceObservationGeneration))
                    && (current & NKikimrBlobStorage::StatusIsValid)
                    && StatusFlagToSpaceColor(flags) <= StatusFlagToSpaceColor(current)) {
                // A regular PDisk reply was observed after this poll was sent.
                // A stale poll may still worsen the state, but must not improve
                // or rewrite an equally severe newer observation.
                return;
            }

            if (flags != current) {
                AtomicSet(observed, flags);
                Update(SelfOrderNum, flags | AtomicGet(other));
            }
        }

        void ObserveLocal(NPDisk::TStatusFlags flags, TAtomic& observed, const TAtomic& other) {
            if (!(flags & NKikimrBlobStorage::StatusIsValid)) {
                return;
            }

            TGuard<TSpinLock> guard(LocalFlagsLock);
            const auto current = static_cast<NPDisk::TStatusFlags>(AtomicGet(observed));
            const auto newColor = StatusFlagToSpaceColor(flags);
            const auto oldColor = StatusFlagToSpaceColor(current);
            if ((current & NKikimrBlobStorage::StatusIsValid) && newColor < oldColor) {
                // Improving observations are ignored. They must not invalidate an
                // in-flight CheckSpace poll, which is the sole source allowed to
                // improve the known state.
                return;
            }

            if (flags != current) {
                // Only an accepted worsening (or the first valid observation)
                // invalidates polls already in flight. No-op replies under load
                // must not pin the color at its worst value.
                if (!(current & NKikimrBlobStorage::StatusIsValid) || newColor > oldColor) {
                    AtomicIncrement(LocalSpaceObservationGeneration);
                }
                AtomicSet(observed, flags);
                Update(SelfOrderNum, flags | AtomicGet(other));
            }
        }

        mutable TSpinLock LocalFlagsLock;
        TAtomic LocalSpaceObservationGeneration = 0;

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
        // VDisk chunks limit in shared free space mode
        TAtomic LocalTotalChunks = 0;
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

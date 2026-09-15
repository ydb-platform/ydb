#pragma once

#include "defs.h"
#include "blobstorage_hullhugedefs.h"

#include <ydb/core/protos/blobstorage_vdisk_internal.pb.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <deque>

namespace NKikimr {
    namespace NHuge {

        ////////////////////////////////////////////////////////////////////////////
        // TStripeHeap
        // Variable-size extent allocator over PDisk chunks. Allocations are contiguous
        // stripes whose offset and length are multiples of AppendBlockSize.
        ////////////////////////////////////////////////////////////////////////////
        class TStripeHeap {
        public:
            TStripeHeap(TString vdiskLogPrefix, ui32 chunkSize, ui32 appendBlockSize);
            explicit TStripeHeap(TString vdiskLogPrefix, const NKikimrVDiskData::THugeKeeperStripeHeap& proto);

            ui32 GetChunkSize() const { return ChunkSize; }
            ui32 GetAppendBlockSize() const { return AppendBlockSize; }
            ui32 AlignSize(ui32 size) const;

            bool Empty() const;
            bool ContainsChunk(ui32 chunkId) const;

            // Returns false if a new (untyped) free chunk is required.
            bool Allocate(ui32 size, THugeSlot *hugeSlot);
            // Format chunkId as an empty stripe chunk and allocate from it.
            void Allocate(ui32 size, THugeSlot *hugeSlot, ui32 chunkId);

            TFreeRes Free(const TDiskPart &addr);
            void AddChunk(ui32 chunkId);
            ui32 RemoveChunk(); // force-free / shredded empty chunks only

            // Give up a chunk claimed during log replay, once a later record shows the chunk has left this heap.
            // Replay-only: it refuses to drop a chunk that holds anything, and nothing is occupied until derivation.
            // Returns whether the chunk was ours.
            bool ForgetChunk(ui32 chunkId);

            bool LockChunk(ui32 chunkId);
            THeapStat GetStat() const;
            void ShredNotify(const std::vector<ui32>& chunksToShred);
            void ListChunks(const THashSet<TChunkIdx>& chunksOfInterest, THashSet<TChunkIdx>& chunks) const;
            THashSet<TChunkIdx> GetForbiddenChunks() const;

            THugeSlot ConvertDiskPart(const TDiskPart &addr) const;

            TFreeRes RecoveryModeFree(const TDiskPart &addr);
            void RecoveryModeAllocate(const TDiskPart &addr);

            // Mark an extent live because the recovered hull references it. The chunk must already be owned by this
            // heap -- the entry point is allowed to name chunks that turn out to hold nothing, but never to omit one
            // that is still referenced, so a missing chunk means the two have genuinely diverged.
            void RecoveryOccupyDerived(const TDiskPart &addr);

            // Chunks that no reference landed in are empty, whatever the entry point claimed; hand them back to the
            // slot heap. Shredded ones go to the force-free queue instead. Call once, after derivation.
            std::vector<ui32> DropUnreferencedChunks();

            // Temporarily free an in-flight stripe so it is serialized as free.
            TFreeRes ReleaseStripe(THugeSlot slot);
            void OccupyStripe(THugeSlot slot, bool inLockedChunks);

            // Shrink an in-flight stripe to newSize, returning its tail to the free list. Used once an SST is written
            // and its real length is known, so that the stripe recorded by the commit is the one actually occupied.
            void ShrinkStripe(THugeSlot slot, ui32 newSize);

            void SaveToProto(NKikimrVDiskData::THugeKeeperStripeHeap& proto) const;
            void LoadFromProto(const NKikimrVDiskData::THugeKeeperStripeHeap& proto);

            void GetOwnedChunks(TSet<TChunkIdx>& chunks) const;
            void CollectChunkIds(THashSet<ui32>& chunks) const;
            void RenderHtml(IOutputStream &str) const;
            TString ToString() const;

        private:
            // Only free extents are tracked: everything else in the chunk is allocated. Extent boundaries are not
            // stored, because every caller that frees or looks up a stripe already knows its length.
            struct TChunkState {
                TMap<ui32, ui32> Free; // offset -> length, coalesced
                ui32 UsedBytes = 0;
                bool Locked = false;
                bool Forbidden = false;
            };

            using THole = std::pair<ui32, ui32>; // (chunkId, offset)

            ui32 AlignUpLocal(ui32 size) const;
            void IndexInsert(ui32 length, ui32 chunkId, ui32 offset);
            void IndexErase(ui32 length, ui32 chunkId, ui32 offset);
            void InsertFree(TChunkState& st, ui32 chunkId, ui32 offset, ui32 length);
            void EraseFree(TChunkState& st, ui32 chunkId, ui32 offset);
            bool TryAllocateInChunk(TChunkState& st, ui32 chunkId, ui32 offset, ui32 length, THugeSlot *hugeSlot);
            bool FindBestFit(ui32 length, ui32 *chunkId, ui32 *offset) const;
            void OccupyRange(TChunkState& st, ui32 offset, ui32 length);
            TFreeRes DoFree(ui32 chunkId, ui32 offset, ui32 length);
            void DropEmptyChunk(ui32 chunkId, TChunkState& st, TFreeRes *res);
            // true if [offset, offset + length) does not intersect any free extent; this is what makes a double free
            // or a free of never-allocated space detectable without tracking extent boundaries
            static bool RangeIsOccupied(const TChunkState& st, ui32 offset, ui32 length);

            const TString VDiskLogPrefix;
            ui32 ChunkSize = 0;
            ui32 AppendBlockSize = 0;
            THashMap<ui32, TChunkState> Chunks;
            TMap<ui32, TSet<THole>> FreeBySize; // length -> holes
            THashSet<ui32> ForbiddenChunks;
            std::deque<ui32> ForceFreeChunks;
        };

    } // NHuge
} // NKikimr

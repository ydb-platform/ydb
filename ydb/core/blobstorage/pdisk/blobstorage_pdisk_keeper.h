#pragma once
#include "defs.h"

#include "blobstorage_pdisk_defs.h"
#include "blobstorage_pdisk_config.h"
#include "blobstorage_pdisk_chunk_tracker.h"
#include "blobstorage_pdisk_free_chunks.h"
#include "blobstorage_pdisk_keeper_params.h"
#include "blobstorage_pdisk_mon.h"

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Chunk facade.
// Tracks both space quotas and free chunk lists
// Part of the in-memory state.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TKeeper {
protected:
    TPDiskMon &Mon;
    TIntrusivePtr<TPDiskConfig> Cfg;

    TFreeChunks UntrimmedFreeChunks; // Untrimmed free chunk list for fast deallocation
    TFreeChunks TrimmedFreeChunks; // Trimmed free chunk list for fast allocation

    TChunkTracker ChunkTracker;
public:

    TKeeper(TPDiskMon &mon, TIntrusivePtr<TPDiskConfig> cfg)
        : Mon(mon)
        , Cfg(cfg)
        , UntrimmedFreeChunks(Mon.UntrimmedFreeChunks, cfg->SortFreeChunksPerItems)
        , TrimmedFreeChunks(Mon.FreeChunks, cfg->SortFreeChunksPerItems)
        , ChunkTracker()
    {}

    //
    // Initialization
    //

    bool Reset(const TKeeperParams& params, const TColorLimits &limits, TString &outErrorReason) {
        return ChunkTracker.Reset(params, limits, outErrorReason);
    }

    void InitialPushFree(TChunkIdx chunkIdx) {
        UntrimmedFreeChunks.Push(chunkIdx);
    }

    void InitialPushTrimmed(TChunkIdx chunkIdx) {
        TrimmedFreeChunks.Push(chunkIdx);
    }

    //
    // Add/remove owner
    //

    void AddOwner(TOwner owner, TVDiskID vdiskId) {
        ChunkTracker.AddOwner(owner, vdiskId);
    }

    void RemoveOwner(TOwner owner) {
        ChunkTracker.RemoveOwner(owner);
    }

    //
    // Normal operation
    //
    ui32 GetFreeChunkCount() const {
        return TrimmedFreeChunks.Size() + UntrimmedFreeChunks.Size();
    }

    ui32 GetTrimmedFreeChunkCount() const {
        return TrimmedFreeChunks.Size();
    }

    i64 GetOwnerHardLimit(TOwner owner) const {
        return ChunkTracker.GetOwnerHardLimit(owner);
    }

    i64 GetOwnerFree(TOwner owner) const {
        return ChunkTracker.GetOwnerFree(owner);
    }

    i64 GetOwnerUsed(TOwner owner) const {
        return ChunkTracker.GetOwnerUsed(owner);
    }

    TChunkIdx PopOwnerFreeChunk(TOwner owner, TString &outErrorReason) {
        if (ChunkTracker.TryAllocate(owner, 1, outErrorReason)) {
            TChunkIdx idx = PopFree(outErrorReason);
            if (idx == 0) {
                ChunkTracker.Release(owner, 1);
            }
            return idx;
        } else {
            return 0;
        }
    }

    TVector<TChunkIdx> PopOwnerFreeChunks(TOwner owner, ui32 chunkCount, TString &outErrorReason) {
        TVector<TChunkIdx> chunks;
        if (ChunkTracker.TryAllocate(owner, chunkCount, outErrorReason)) {
            chunks.resize(chunkCount);
            for (ui32 i = 0; i < chunkCount; ++i) {
                TChunkIdx idx = PopFree(outErrorReason);
                if (idx == 0) {
                    for (ui32 f = 0; f < i; ++f) {
                        UntrimmedFreeChunks.Push(chunks[f]);
                    }
                    ChunkTracker.Release(owner, chunkCount);
                    return {};
                }
                chunks[i] = idx;
            }
        }
        return chunks;
    }

    void PushFreeOwnerChunk(TOwner owner, TChunkIdx chunkIdx) {
        Y_ABORT_UNLESS(chunkIdx != 0);
        UntrimmedFreeChunks.Push(chunkIdx);
        ChunkTracker.Release(owner, 1);
    }

    TStatusFlags GetSpaceStatusFlags(TOwner owner, double *occupancy) const {
        return ChunkTracker.GetSpaceStatusFlags(owner, occupancy);
    }

    NKikimrBlobStorage::TPDiskSpaceColor::E EstimateSpaceColor(TOwner owner, i64 allocationSize, double *occupancy) const {
        return ChunkTracker.EstimateSpaceColor(owner, allocationSize, occupancy);
    }

    //
    // Trimming
    //
    TChunkIdx PopUntrimmedFreeChunk() {
        return UntrimmedFreeChunks.Pop();
    }

    void PushTrimmedFreeChunk(TChunkIdx chunkIdx) {
        TrimmedFreeChunks.Push(chunkIdx);
    }

    //
    // Locking/Unlocking
    //
    TChunkIdx PopFreeChunkHack(TString &outErrorReason) {
        return PopFree(outErrorReason);
    }

    void PushFreeChunkHack(TChunkIdx chunkIdx) {
        UntrimmedFreeChunks.Push(chunkIdx);
    }

    ui32 ColorFlagLimit(TOwner owner, NKikimrBlobStorage::TPDiskSpaceColor::E color) {
        return ChunkTracker.ColorFlagLimit(owner, color);
    }

    //
    // GUI
    //
    void PrintHTML(IOutputStream &str) {
        ChunkTracker.PrintHTML(str);
    }

protected:
    //
    // Internals
    //
    TChunkIdx PopFree(TString &outErrorReason) {
        TChunkIdx chunkIdx = TrimmedFreeChunks.Pop();
        if (!chunkIdx) {
            chunkIdx = UntrimmedFreeChunks.Pop();
        }
        if (!chunkIdx) {
            outErrorReason = "Can't pop chunk neither from Trimmed nor from Untrimmed. Marker# BPK10";
        }
        return chunkIdx;
    }

};

} // NPDisk
} // NKikimr

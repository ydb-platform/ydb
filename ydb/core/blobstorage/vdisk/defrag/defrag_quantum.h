#pragma once

#include "defs.h"
#include "defrag_actor.h"
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>

namespace NKikimr {

    class TDefragQuantumChunkFinder;

    struct TEvDefragQuantumResult :
        public TEventLocal<TEvDefragQuantumResult, TEvBlobStorage::EvDefragQuantumResult>
    {
        struct TStat {
            // found chunks to defrag/free, can be larger than actual chunks freed
            size_t FoundChunksToDefrag = 0;
            // number of huge blob recs rewritten
            size_t RewrittenRecs = 0;
            // number of bytes rewritten
            size_t RewrittenBytes = 0;
            // have we got the best possible state of Huge Heap?
            bool Eof = false;
            // list of freed chunks; FoundChunksToDefrag can differ from FreedChunks.size(),
            // because we search ChunksToDefrag on some snapshot, real state can differ
            TDefragChunks FreedChunks;
        };

        TStat Stat;

        TEvDefragQuantumResult(TStat stat)
            : Stat(std::move(stat))
        {}
    };

    struct TChunksToDefrag;

    IActor *CreateDefragQuantumActor(const std::shared_ptr<TDefragCtx>& dctx, const TVDiskID& selfVDiskId,
        std::optional<TChunksToDefrag> chunksToDefrag, bool needCompaction);

} // NKikimr


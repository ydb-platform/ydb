#pragma once

#include "defs.h"
#include "incrhuge_keeper_common.h"
#include "incrhuge.h"

#include <util/generic/set.h>

namespace NKikimr {
    namespace NIncrHuge {

        class TDefragmenter
            : public TKeeperComponentBase
        {
            // the current chunk we are defragmenting
            TChunkIdx ChunkInProgress = 0;

            // identifier of current chunk to check if it is gone
            TChunkSerNum ChunkInProgressSerNum;

            // index of current chunk
            TVector<TBlobIndexRecord> Index;
            ui32 IndexPos = 0;
            ui32 OffsetInBlocks = 0;

            // number of in-flight reads
            ui32 InFlightReads = 0;
            const ui32 MaxInFlightReads = 3;
            ui32 InFlightReadBytes = 0;
            const ui32 MaxInFlightReadBytes = 32 << 20;
            ui32 InFlightWrites = 0;
            const ui32 MaxInFlightWrites = 3;

            // current threshold
            double Threshold = 0.0;

        public:
            TDefragmenter(TKeeper& keeper);
            ~TDefragmenter();

            void InFlightWritesChanged(const TActorContext& ctx);

            // called when chunk state (in terms of defragmentation) has changed -- chunk was partially/completely
            // cleaned
            void UpdateChunkState(TChunkIdx chunkIdx, const TActorContext& ctx);

            void ApplyScan(const TActorId& sender, TEvIncrHugeScanResult& msg, const TActorContext& ctx);

            void HandleControlDefrag(TEvIncrHugeControlDefrag::TPtr& ev, const TActorContext& ctx);

        private:
            void ProcessPendingChunksQueue(const TActorContext& ctx);

            void ProcessIndex(const TActorContext& ctx);

            void ApplyRead(const TBlobIndexRecord& record, TChunkIdx chunkIdx, TChunkSerNum chunkSerNum,
                    ui32 offsetInBlocks, ui32 index, NKikimrProto::EReplyStatus status,
                    NPDisk::TEvChunkReadResult& result, const TActorContext& ctx);

            void ApplyWrite(const TBlobDeleteLocator& deleteLocator, NKikimrProto::EReplyStatus status,
                    const TActorContext& ctx);

            void FinishChunkInProgress(const TActorContext& ctx);

            TChunkInfo *CheckCurrentChunk(const TActorContext& ctx);
        };

    } // NIncrHuge
} // NKikimr

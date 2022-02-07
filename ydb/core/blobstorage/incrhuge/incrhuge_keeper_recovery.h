#pragma once

#include "defs.h"
#include "incrhuge_keeper_common.h"
#include "incrhuge.h"

#include <util/generic/queue.h>

namespace NKikimr {
    namespace NIncrHuge {

        class TRecovery
            : public TKeeperComponentBase
        {
            struct TInitQueueItem {
                TVDiskID VDiskId;
                ui8 Owner;
                ui64 FirstLsn;
                TActorId Sender;
                ui64 Cookie;
                TQueue<std::pair<TChunkIdx, TChunkInfo *>> ScanQueue;
                THashMap<TChunkIdx, TDynBitMap> DeletedItemsMap;
                TVector<TEvIncrHugeInitResult::TItem> Items;
            };
            using TInitQueue = TList<TInitQueueItem>;
            TInitQueue InitQueue;

            struct TScanQueueItem {
                TChunkIdx ChunkIdx;
                bool IndexOnly;
                TChunkSerNum ChunkSerNum;

                friend bool operator <(const TScanQueueItem& left, const TScanQueueItem& right) {
                    return left.ChunkIdx < right.ChunkIdx;
                }
            };
            using TScanQueue = TQueue<TScanQueueItem>;
            TScanQueue ScanQueue;
            ui32 ScanBytesInFlight = 0;
            THashMap<TChunkIdx, TDynBitMap> IntentChunksDeletedItems;

            struct TScanResult {
                NKikimrProto::EReplyStatus Status;
                TChunkIdx ChunkIdx;
                bool IndexOnly;
                bool IndexValid;
                TVector<TBlobIndexRecord> Index;
            };
            TQueue<TChunkSerNum> ChunkSerNumQueue;
            THashMap<TChunkSerNum, TScanResult> PendingResults;

            struct TIncompleteChunk {
                TChunkIdx ChunkIdx;
                ui32 OffsetInBlocks;
                TVector<TBlobIndexRecord> Index;
            };
            TMap<TChunkSerNum, TIncompleteChunk> IncompleteChunks;

            struct TScanInfo {
                TInitQueue::iterator It;
                TChunkIdx ChunkIdx;
                TChunkInfo& Chunk;
            };
            THashMap<TActorId, TScanInfo> ScannerMap;

        public:
            TRecovery(TKeeper& keeper);
            ~TRecovery();

            // handle yard init message
            void ApplyYardInit(NKikimrProto::EReplyStatus status, NPDisk::TLogRecord *chunks, NPDisk::TLogRecord *deletes,
                    const TActorContext& ctx);

            // handle read log result
            void ApplyReadLog(const TActorId& sender, TEvIncrHugeReadLogResult& msg, const TActorContext& ctx);

            // handle scan result
            void ApplyScan(const TActorId& sender, TEvIncrHugeScanResult& msg, const TActorContext& ctx);

            // handle init message from client
            void HandleInit(TEvIncrHugeInit::TPtr& ev, const TActorContext& ctx);

        private:
            void EnqueueScan(TScanQueueItem&& item, const TActorContext& ctx);
            bool ProcessScanItem(TScanQueueItem& item, const TActorContext& ctx);
            void ProcessScanQueue(const TActorContext& ctx);
            void ProcessScanResult(TScanResult& scanResult, const TActorContext& ctx);

            void ProcessInitItem(TInitQueue::iterator it, const TActorContext& ctx);
            void SendInitResponse(TInitQueue::iterator it, NKikimrProto::EReplyStatus status, const TActorContext& ctx);
            void ProcessInitItemScanQueue(TInitQueue::iterator it, const TActorContext& ctx);
            void ApplyInitItemScan(TInitQueue::iterator it, TChunkIdx chunkIdx, TChunkInfo& chunk,
                    TEvIncrHugeScanResult& msg, const TActorContext& ctx);
        };

    } // NIncrHuge
} // NKikimr

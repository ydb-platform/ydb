#pragma once

#include "defs.h"
#include "blobstorage_readbatch.h"
#include "blobstorage_hullcompactdeferredqueue.h"
#include <ydb/core/blobstorage/vdisk/balance/handoff_map.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/blobstorage_hullwritesst.h>
#include <ydb/core/blobstorage/vdisk/hulldb/blobstorage_hullgcmap.h>
#include <ydb/core/blobstorage/vdisk/scrub/restore_corrupted_blob_actor.h>

namespace NKikimr {

    template<typename TKey, typename TMemRec, typename TIterator>
    class THullCompactionWorker {
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // COMMON TYPE ALIASES
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        // handoff map
        using THandoffMap = NKikimr::THandoffMap<TKey, TMemRec>;
        using TTransformedItem = typename THandoffMap::TTransformedItem;
        using THandoffMapPtr = TIntrusivePtr<THandoffMap>;

        // garbage collector map
        using TGcMap = NKikimr::TGcMap<TKey, TMemRec>;
        using TGcMapPtr = TIntrusivePtr<TGcMap>;
        using TGcMapIterator = typename TGcMap::TIterator;

        // compaction record merger
        using TCompactRecordMergerIndexPass = NKikimr::TCompactRecordMergerIndexPass<TKey, TMemRec>;
        using TCompactRecordMergerDataPass = NKikimr::TCompactRecordMergerDataPass<TKey, TMemRec>;

        // level segment
        using TLevelSegment = NKikimr::TLevelSegment<TKey, TMemRec>;
        using TWriter = typename TLevelSegment::TWriter;

        // level index
        using TLevelIndex = NKikimr::TLevelIndex<TKey, TMemRec>;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // DEFERRED ITEM QUEUE PROCESSOR
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        class TDeferredItemQueue : public TDeferredItemQueueBase<TDeferredItemQueue> {
            TWriter *Writer = nullptr;

            friend class TDeferredItemQueueBase<TDeferredItemQueue>;

            void StartImpl(TWriter *writer) {
                Y_ABORT_UNLESS(!Writer);
                Writer = writer;
                Y_ABORT_UNLESS(Writer);
            }

            void ProcessItemImpl(const TDiskPart& preallocatedLocation, TRope&& buffer) {
                TDiskPart writtenLocation = Writer->PushDataOnly(std::move(buffer));

                // ensure that item was written into preallocated position
                Y_ABORT_UNLESS(writtenLocation == preallocatedLocation);
            }

            void FinishImpl() {
                Y_ABORT_UNLESS(Writer);
                Writer = nullptr;
            }

        public:
            TDeferredItemQueue(TRopeArena& arena, TBlobStorageGroupType gtype, bool addHeader)
                : TDeferredItemQueueBase<TDeferredItemQueue>(arena, gtype, addHeader)
            {}
        };

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // LOCAL TYPE DEFINITIONS
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        // state of compaction worker automaton
        enum class EState {
            Invalid,                // invalid state; this state should never be reached
            GetNextItem,            // going to extract next item for processing or finish if there are no more items
            TryProcessItem,         // trying to write item into SST
            WaitingForDeferredItems,
            FlushingSST,            // flushing SST to disk
            WaitForPendingRequests, // waiting for all pending requests to finish
        };

        // status of try
        enum class ETryProcessItemStatus {
            Success,        // item was written to SST
            NeedMoreChunks, // we need more chunks to create new writer
            FinishSST,      // we need to flush current SST to start a new one as this is full
        };

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // MEMBER VARIABLES
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        // basic contexts
        THullCtxPtr HullCtx;
        TPDiskCtxPtr PDiskCtx;

        // Group Type
        const TBlobStorageGroupType GType;

        // pointer to level index
        const TIntrusivePtr<TLevelIndex> LevelIndex;

        // handoff map we use to transform items
        THandoffMapPtr Hmp;

        // garbage collector iterator
        TGcMapIterator GcmpIt;

        // LSN range
        const ui64 FirstLsn = 0;
        const ui64 LastLsn = 0;

        // level DB iterator
        TIterator It;

        // true if fresh segment is being compacted
        const bool IsFresh;

        // maximum number of chunks we use per SST
        ui32 ChunksToUse;

        // chunks currently reserved and not used
        TDeque<TChunkIdx> ReservedChunks;

        // all reserved chunks during the compaction
        TDeque<TChunkIdx> AllocatedChunks;

        // record merger for compaction
        TCompactRecordMergerIndexPass IndexMerger;

        // current handoff-transformed item
        const TTransformedItem *TransformedItem = nullptr;

        // SST writer
        std::unique_ptr<TWriter> WriterPtr;

        // number of chunks we have asked to reserve, but not yet confirmed
        ui32 ChunkReservePending = 0;

        // automaton state
        EState State = EState::Invalid;

        // number of currently unresponded write requests
        ui32 InFlightWrites = 0;

        // maximum number of such requests
        ui32 MaxInFlightWrites;

        // number of currently unresponded read requests
        ui32 InFlightReads = 0;

        // maximum number of such requests
        ui32 MaxInFlightReads;

        // vector of freed huge blobs
        TDiskPartVec FreedHugeBlobs;

        // generated level segments
        TVector<TIntrusivePtr<TLevelSegment>> LevelSegments;

        // generated chunks
        TVector<TChunkIdx> CommitChunks;

        // pointer to an atomic variable contaning number of in flight reads
        TAtomic *ReadsInFlight;

        // pointer to an atomic variable contaning number of in flight writes
        TAtomic *WritesInFlight;

        struct TBatcherPayload {
            ui64 Id = 0;
            NMatrix::TVectorType LocalParts; // a bit vector of local parts we are going to read from this disk blob
            TLogoBlobID BlobId;
            TDiskPart Location;

            TBatcherPayload() = default;
            TBatcherPayload(ui64 id, NMatrix::TVectorType localParts, TLogoBlobID blobId, TDiskPart location)
                : Id(id)
                , LocalParts(localParts)
                , BlobId(blobId)
                , Location(location)
            {}
        };
        TCompactReadBatcher<TBatcherPayload> ReadBatcher;

        // arena for different kinds of small-block allocations
        TRopeArena Arena;

        TDeferredItemQueue DeferredItems;
        ui64 NextDeferredItemId = 1;

        // previous key (in case of logoblobs)
        TKey PreviousKey;

        // is this the first key?
        bool IsFirstKey = true;

    public:
        struct TStatistics {
            THullCtxPtr HullCtx;
            // read/write stat
            ui64 BytesRead = 0;
            ui64 ReadIOPS = 0;
            ui64 BytesWritten = 0;
            ui64 WriteIOPS = 0;
            ui64 ItemsWritten = 0;
            // garbage collect stat
            ui64 KeepItemsWithData = 0;
            ui64 KeepItemsWOData = 0;
            ui64 DontKeepItems = 0;
            // time stat
            TInstant CreationTime;
            TInstant StartTime;
            TInstant FinishTime;

            TStatistics(THullCtxPtr hullCtx)
                : HullCtx(hullCtx)
                , CreationTime(TAppData::TimeProvider->Now())
                , StartTime()
                , FinishTime()
            {}

            TString ToString() const {
                TStringStream str;
                str << "{WaitTime# " << (StartTime - CreationTime).ToString()
                    << " GetNextItemTime# " << (FinishTime - StartTime).ToString()
                    << " BytesRead# " << BytesRead << " ReadIOPS# " << ReadIOPS
                    << " BytesWritten# " << BytesWritten << " WriteIOPS# " << WriteIOPS
                    << " ItemsWritten# " << ItemsWritten
                    << " KeepItemsWithData# " << KeepItemsWithData
                    << " KeepItemsWOData# " << KeepItemsWOData
                    << " DontKeepItems# " << DontKeepItems << "}";

                return str.Str();
            }

            void ItemAdded() {
                ItemsWritten++;
            }

            void Update(const NPDisk::TEvChunkRead *msg) {
                BytesRead += msg->Size;
                ReadIOPS++;
                ++HullCtx->LsmHullGroup.LsmCompactionReadRequests();
                HullCtx->LsmHullGroup.LsmCompactionBytesRead() += msg->Size;
            }

            void Update(const NPDisk::TEvChunkWrite *msg) {
                ui32 bytes = msg->PartsPtr ? msg->PartsPtr->ByteSize() : 0;
                BytesWritten += bytes;
                WriteIOPS++;
                ++HullCtx->LsmHullGroup.LsmCompactionWriteRequests();
                HullCtx->LsmHullGroup.LsmCompactionBytesWritten() += bytes;
            }
        };

        TStatistics Statistics;
        TDuration RestoreDeadline;
        // Partition key is used for splitting resulting SSTs by the PartitionKey if present. Partition key
        // is used for compaction policy implementation to limit number of intermediate chunks durint compaction.
        std::optional<TKey> PartitionKey;

    public:
        THullCompactionWorker(THullCtxPtr hullCtx,
                              TPDiskCtxPtr pdiskCtx,
                              TIntrusivePtr<TLevelIndex> levelIndex,
                              const TIterator& it,
                              bool isFresh,
                              ui64 firstLsn,
                              ui64 lastLsn,
                              TDuration restoreDeadline,
                              std::optional<TKey> partitionKey)
            : HullCtx(std::move(hullCtx))
            , PDiskCtx(std::move(pdiskCtx))
            , GType(HullCtx->VCtx->Top->GType)
            , LevelIndex(std::move(levelIndex))
            , FirstLsn(firstLsn)
            , LastLsn(lastLsn)
            , It(it)
            , IsFresh(isFresh)
            , IndexMerger(GType, HullCtx->AddHeader)
            , ReadBatcher(PDiskCtx->Dsk->ReadBlockSize,
                    PDiskCtx->Dsk->SeekTimeUs * PDiskCtx->Dsk->ReadSpeedBps / 1000000,
                    HullCtx->HullCompReadBatchEfficiencyThreshold)
            , Arena(&TRopeArenaBackend::Allocate)
            , DeferredItems(Arena, HullCtx->VCtx->Top->GType, HullCtx->AddHeader)
            , Statistics(HullCtx)
            , RestoreDeadline(restoreDeadline)
            , PartitionKey(partitionKey)
        {
            if (IsFresh) {
                ChunksToUse = HullCtx->HullSstSizeInChunksFresh;
                MaxInFlightWrites = HullCtx->FreshCompMaxInFlightWrites;
                MaxInFlightReads = 0;
                ReadsInFlight = nullptr;
                WritesInFlight = &LevelIndex->FreshCompWritesInFlight;
            } else {
                ChunksToUse = HullCtx->HullSstSizeInChunksLevel;
                MaxInFlightWrites = HullCtx->HullCompMaxInFlightWrites;
                MaxInFlightReads = HullCtx->HullCompMaxInFlightReads;
                ReadsInFlight = &LevelIndex->HullCompReadsInFlight;
                WritesInFlight = &LevelIndex->HullCompWritesInFlight;
            }
        }

        void Prepare(THandoffMapPtr hmp, TGcMapIterator gcmpIt) {
            Hmp = std::move(hmp);
            GcmpIt = gcmpIt;
            State = EState::GetNextItem;
        }

        // main cycle function; return true if compaction is finished and compaction actor can proceed to index load;
        // when there is more work to do, return false; MUST NOT return true unless all pending requests are finished
        bool MainCycle(TVector<std::unique_ptr<IEventBase>>& msgsForYard) {
            for (;;) {
                switch (State) {
                    case EState::Invalid:
                        Y_ABORT("unexpected state");

                    case EState::GetNextItem:
                        if (It.Valid()) {
                            const TKey& key = It.GetCurKey();
                            if (IsFirstKey) {
                                IsFirstKey = false;
                            } else {
                                Y_ABORT_UNLESS(!key.IsSameAs(PreviousKey), "duplicate keys: %s -> %s",
                                        PreviousKey.ToString().data(), key.ToString().data());
                            }
                            PreviousKey = key;

                            // iterator is valid and we have one more item to process; instruct merger whether we want
                            // data or not and proceed to TryProcessItem state
                            Y_ABORT_UNLESS(GcmpIt.Valid());
                            IndexMerger.SetLoadDataMode(GcmpIt.KeepData());
                            It.PutToMerger(&IndexMerger);

                            const bool haveToProcessItem = PreprocessItem();
                            if (haveToProcessItem) {
                                State = EState::TryProcessItem;
                            } else {
                                FinishItem();
                            }
                        }  else if (WriterPtr) {
                            // start processing deferred items
                            DeferredItems.Start(WriterPtr.get());
                            // start batcher
                            ReadBatcher.Start();
                            // iterator is not valid and we have writer with items (because we don't create writer
                            // unless there are actual items we want to write); proceed to WaitingForDeferredItems state
                            State = EState::WaitingForDeferredItems;
                        } else {
                            // iterator is not valid and we have no writer -- so just proceed to WaitForPendingRequests
                            // state and finish
                            State = EState::WaitForPendingRequests;
                        }
                        break;

                    case EState::TryProcessItem:
                        // ensure we have transformed item
                        Y_ABORT_UNLESS(TransformedItem);
                        // try to process it
                        ETryProcessItemStatus status;
                        status = TryProcessItem();
                        switch (status) {
                            case ETryProcessItemStatus::Success:
                                // try to send some messages if this is the fresh segment
                                if (IsFresh) {
                                    ProcessPendingMessages(msgsForYard);
                                }
                                // finalize item
                                FinishItem();
                                // continue with next item
                                State = EState::GetNextItem;
                                break;

                            case ETryProcessItemStatus::NeedMoreChunks:
                                // generate request for chunk reservation and try again
                                if (auto msg = CheckForReservation()) {
                                    msgsForYard.push_back(std::move(msg));
                                } else {
                                    Y_ABORT_UNLESS(ChunkReservePending);
                                }
                                return false;

                            case ETryProcessItemStatus::FinishSST:
                                // start processing deferred items
                                DeferredItems.Start(WriterPtr.get());
                                // start batcher
                                ReadBatcher.Start();
                                // wait
                                State = EState::WaitingForDeferredItems;
                                break;
                        }
                        break;

                    case EState::WaitingForDeferredItems:
                        ProcessPendingMessages(msgsForYard);
                        if (!DeferredItems.AllProcessed()) {
                            return false;
                        }
                        DeferredItems.Finish();
                        // we have finished will all of the deferred items, it's a good time to flush SST now
                        State = EState::FlushingSST;
                        ReadBatcher.Finish();
                        break;

                    case EState::FlushingSST:
                        // do not continue processing if there are too many writes in flight
                        if (InFlightWrites >= MaxInFlightWrites) {
                            return false;
                        }
                        // try to flush SST
                        if (FlushSST(msgsForYard)) {
                            // we return to state from which finalization was invoked
                            State = TransformedItem ? EState::TryProcessItem : EState::GetNextItem;
                        }
                        break;

                    case EState::WaitForPendingRequests:
                        // wait until all writes succeed
                        if (InFlightWrites) {
                            return false;
                        }

                        // wait until chunk reservation finishes (if any)
                        if (ChunkReservePending) {
                            return false;
                        }

                        // should never return to main cycle
                        State = EState::Invalid;

                        // return true indicating successful completion of this compaction job
                        return true;
                }
            }
        }

        TEvRestoreCorruptedBlob *Apply(NPDisk::TEvChunkReadResult *msg, TInstant now) {
            AtomicDecrement(*ReadsInFlight);
            Y_ABORT_UNLESS(InFlightReads > 0);
            --InFlightReads;

            // apply read result to batcher
            ReadBatcher.Apply(msg);
            return ProcessReadBatcher(now);
        }

        bool ExpectingBlobRestoration = false;

        TEvRestoreCorruptedBlob *Apply(TEvRestoreCorruptedBlobResult *msg, bool *isAborting, TInstant now) {
            Y_ABORT_UNLESS(msg->Items.size() == 1);
            Y_ABORT_UNLESS(ExpectingBlobRestoration);
            ExpectingBlobRestoration = false;
            auto& item = msg->Items.front();
            switch (item.Status) {
                case NKikimrProto::OK: {
                    std::array<TRope, 8> parts;
                    ui32 numParts = 0;
                    for (ui32 i = item.Needed.FirstPosition(); i != item.Needed.GetSize(); i = item.Needed.NextPosition(i)) {
                        parts[numParts++] = std::move(item.Parts[i]);
                    }
                    DeferredItems.AddReadDiskBlob(item.Cookie, TDiskBlob::CreateFromDistinctParts(&parts[0],
                        &parts[numParts], item.Needed, item.BlobId.BlobSize(), Arena, HullCtx->AddHeader), item.Needed);
                    return ProcessReadBatcher(now);
                }

                case NKikimrProto::DEADLINE:
                case NKikimrProto::ERROR:
                    *isAborting = true;
                    return nullptr;

                default:
                    Y_ABORT();
            }
        }

        TEvRestoreCorruptedBlob *ProcessReadBatcher(TInstant now) {
            if (ExpectingBlobRestoration) {
                return nullptr;
            }
            // try to extract as much as possible items from read batcher
            ui64 serial;
            TBatcherPayload payload;
            NKikimrProto::EReplyStatus status;
            TRcBuf buffer;
            while (ReadBatcher.GetResultItem(&serial, &payload, &status, &buffer)) {
                if (status == NKikimrProto::CORRUPTED) {
                    ExpectingBlobRestoration = true;
                    TEvRestoreCorruptedBlob::TItem item(payload.BlobId, payload.LocalParts, GType, payload.Location, payload.Id);
                    return new TEvRestoreCorruptedBlob(now + RestoreDeadline, {1u, item}, false, true);
                } else {
                    Y_ABORT_UNLESS(status == NKikimrProto::OK);
                    DeferredItems.AddReadDiskBlob(payload.Id, TRope(std::move(buffer)), payload.LocalParts);
                }
            }
            return nullptr;
        }

        void Apply(NPDisk::TEvChunkWriteResult * /*msg*/) {
            // adjust number of in flight messages
            Y_ABORT_UNLESS(InFlightWrites > 0);
            --InFlightWrites;
            AtomicDecrement(*WritesInFlight);
        }

        void Apply(NPDisk::TEvChunkReserveResult *msg) {
            // reset in flight allocation counter
            ChunkReservePending = 0;

            // add newly allocated chunks to reserved chunks set
            ReservedChunks.insert(ReservedChunks.end(), msg->ChunkIds.begin(), msg->ChunkIds.end());
            AllocatedChunks.insert(AllocatedChunks.end(), msg->ChunkIds.begin(), msg->ChunkIds.end());
        }

        const TVector<TIntrusivePtr<TLevelSegment>>& GetLevelSegments() { return LevelSegments; }
        const TVector<TChunkIdx>& GetCommitChunks() const { return CommitChunks; }
        const TDiskPartVec& GetFreedHugeBlobs() const { return FreedHugeBlobs; }
        const TDeque<TChunkIdx>& GetReservedChunks() const { return ReservedChunks; }
        const TDeque<TChunkIdx>& GetAllocatedChunks() const { return AllocatedChunks; }

    private:
        void CollectRemovedHugeBlobs(const TVector<TDiskPart> &hugeBlobs) {
            for (const TDiskPart& p : hugeBlobs) {
                if (!p.Empty()) {
                    FreedHugeBlobs.PushBack(p);
                }
            }
        }

        // start item processing; this function transforms item using handoff map and adds collected huge blobs, if any
        // it returns true if we should keep this item; otherwise it returns false
        bool PreprocessItem() {
            const TKey key = It.GetCurKey();

            // finish merging data for this item
            IndexMerger.Finish();

            // reset transformed item and try to create new one if we want to keep this item
            const bool keepData = GcmpIt.KeepData();
            const bool keepItem = GcmpIt.KeepItem();
            TransformedItem = Hmp->Transform(key, &IndexMerger.GetMemRec(), IndexMerger.GetDataMerger(), keepData, keepItem);
            if (keepItem) {
                ++(keepData ? Statistics.KeepItemsWithData : Statistics.KeepItemsWOData);
            } else {
                ++Statistics.DontKeepItems;
            }

            // collect huge blobs -- we unconditionally delete DeletedData and save SavedData only in case
            // when this blob is written -- that is, when TransformedItem is set.
            const TDataMerger *dataMerger = TransformedItem ? TransformedItem->DataMerger : IndexMerger.GetDataMerger();
            if (!TransformedItem) {
                CollectRemovedHugeBlobs(dataMerger->GetHugeBlobMerger().SavedData());
            }
            CollectRemovedHugeBlobs(dataMerger->GetHugeBlobMerger().DeletedData());

            return TransformedItem != nullptr;
        }

        ETryProcessItemStatus TryProcessItem() {
            // if there is no active writer, create one and start writing
            if (!WriterPtr) {
                // ensure we have enough reserved chunks to do operation; or else request for allocation and wait
                if (ReservedChunks.size() < ChunksToUse) {
                    return ETryProcessItemStatus::NeedMoreChunks;
                }

                // create new instance of writer
                WriterPtr = std::make_unique<TWriter>(HullCtx->VCtx, IsFresh ? EWriterDataType::Fresh : EWriterDataType::Comp,
                        ChunksToUse, PDiskCtx->Dsk->Owner, PDiskCtx->Dsk->OwnerRound,
                        (ui32)PDiskCtx->Dsk->ChunkSize, PDiskCtx->Dsk->AppendBlockSize,
                        (ui32)PDiskCtx->Dsk->BulkWriteBlockSize, LevelIndex->AllocSstId(), false, ReservedChunks, Arena,
                        HullCtx->AddHeader);
            }

            // if we have PartitionKey, check it is time to split partitions by PartitionKey
            if (PartitionKey && !IsFirstKey && PreviousKey < *PartitionKey && TransformedItem->Key >= *PartitionKey &&
                !WriterPtr->Empty()) {
                return ETryProcessItemStatus::FinishSST;
            }

            // special logic for fresh: we just put this item into data segment as usual, do not preallocate and then
            // write data
            if (IsFresh) {
                const bool itemWritten = WriterPtr->Push(TransformedItem->Key, *TransformedItem->MemRec,
                        TransformedItem->DataMerger);
                if (itemWritten) {
                    Statistics.ItemAdded();
                    return ETryProcessItemStatus::Success;
                } else {
                    return ETryProcessItemStatus::FinishSST;
                }
            }

            // calculate inplaced data size: extract TLogoBlobID from the key, calculate total blob size, then reduce
            // it to part size using layout information and finally calculate serialized blob size using number of local
            // parts stored in this record; if inplacedDataSize is zero, then we do not store any data inside SSTable,
            // otherwise we store DiskBlob and have to assemble it at data pass
            ui32 inplacedDataSize = 0;
            const NMatrix::TVectorType partsToStore = TransformedItem->MemRec->GetLocalParts(GType);
            if (TransformedItem->DataMerger->GetType() == TBlobType::DiskBlob && !partsToStore.Empty()) {
                inplacedDataSize = TDiskBlob::CalculateBlobSize(GType, TransformedItem->Key.LogoBlobID(), partsToStore,
                    HullCtx->AddHeader);
            }

            // try to push item into SST; in case of failure there is not enough space to fit this item
            TDiskPart preallocatedLocation;
            if (WriterPtr->PushIndexOnly(TransformedItem->Key, *TransformedItem->MemRec, TransformedItem->DataMerger,
                    inplacedDataSize, &preallocatedLocation)) {
                // count added item
                Statistics.ItemAdded();

                // if we do generate some small blob, we have to enqueue it in Deferred Items queue and then possibly
                // issue some reads
                if (inplacedDataSize != 0) {
                    ui32 numReads = 0;
                    auto lambda = [&](const TMemRec& memRec) {
                        Y_ABORT_UNLESS(memRec.GetType() == TBlobType::DiskBlob && memRec.HasData());

                        // find out where our disk blob resides
                        TDiskDataExtractor extr;
                        memRec.GetDiskData(&extr, nullptr);
                        TDiskPart location = extr.SwearOne();

                        // get its vector of local parts stored in that location
                        NMatrix::TVectorType parts = memRec.GetLocalParts(GType);

                        // enqueue read
                        ReadBatcher.AddReadItem(location.ChunkIdx, location.Offset, location.Size,
                                TBatcherPayload(NextDeferredItemId, parts, It.GetCurKey().LogoBlobID(), location));

                        ++numReads;
                    };
                    IndexMerger.ForEachSmallDiskBlob(std::move(lambda));

                    // either we read something or it is already in memory
                    const TDiskBlobMerger& diskBlobMerger = TransformedItem->DataMerger->GetDiskBlobMerger();
                    Y_ABORT_UNLESS(!diskBlobMerger.Empty() || numReads > 0, "Key# %s MemRec# %s LocalParts# %s KeepData# %s",
                            It.GetCurKey().ToString().data(),
                            TransformedItem->MemRec->ToString(HullCtx->IngressCache.Get(), nullptr).data(),
                            TransformedItem->MemRec->GetLocalParts(GType).ToString().data(),
                            GcmpIt.KeepData() ? "true" : "false");

                    // TODO(alexvru): maybe we should get rid of copying DiskBlobMerger?
                    DeferredItems.Put(NextDeferredItemId, numReads, preallocatedLocation, diskBlobMerger, partsToStore,
                        It.GetCurKey().LogoBlobID());

                    // advance deferred item identifier
                    ++NextDeferredItemId;
                }

                // return success indicating that this item required no further processing
                return ETryProcessItemStatus::Success;
            } else {
                // return failure meaning this item should be restarted after flushing current SST
                return ETryProcessItemStatus::FinishSST;
            }
        }

        void FinishItem() {
            // clear merger and on-disk record list and advance both iterators synchronously
            IndexMerger.Clear();
            It.Next();
            GcmpIt.Next();
            TransformedItem = nullptr;
        }

        bool FlushSST(TVector<std::unique_ptr<IEventBase>>& msgsForYard) {
            // try to flush some more data; if the flush fails, it means that we have reached in flight write limit and
            // there is nothing to do here now, so we return
            const bool flushDone = WriterPtr->FlushNext(FirstLsn, LastLsn, MaxInFlightWrites - InFlightWrites);
            ProcessPendingMessages(msgsForYard);
            if (!flushDone) {
                return false;
            }

            // get writer conclusion and fill in entrypoint and used chunks vector
            const auto& conclusion = WriterPtr->GetConclusion();
            LevelSegments.push_back(conclusion.LevelSegment);
            CommitChunks.insert(CommitChunks.end(), conclusion.UsedChunks.begin(), conclusion.UsedChunks.end());

            // ensure that all writes were executed and drop writer
            Y_ABORT_UNLESS(!WriterPtr->GetPendingMessage());
            WriterPtr.reset();

            return true;
        }

        void ProcessPendingMessages(TVector<std::unique_ptr<IEventBase>>& msgsForYard) {
            // ensure that we have writer
            Y_ABORT_UNLESS(WriterPtr);

            // send new messages until we reach in flight limit
            std::unique_ptr<NPDisk::TEvChunkWrite> msg;
            while (InFlightWrites < MaxInFlightWrites && (msg = WriterPtr->GetPendingMessage())) {
                HullCtx->VCtx->CountCompactionCost(*msg);
                Statistics.Update(msg.get());
                msgsForYard.push_back(std::move(msg));
                ++InFlightWrites;
                AtomicIncrement(*WritesInFlight);
            }

            std::unique_ptr<NPDisk::TEvChunkRead> readMsg;
            while (InFlightReads < MaxInFlightReads && (readMsg = ReadBatcher.GetPendingMessage(
                            PDiskCtx->Dsk->Owner, PDiskCtx->Dsk->OwnerRound, NPriRead::HullComp))) {
                HullCtx->VCtx->CountCompactionCost(*readMsg);
                Statistics.Update(readMsg.get());
                msgsForYard.push_back(std::move(readMsg));
                ++InFlightReads;
                AtomicIncrement(*ReadsInFlight);
            }
        }

        std::unique_ptr<NPDisk::TEvChunkReserve> CheckForReservation() {
            if (ReservedChunks.size() + ChunkReservePending >= ChunksToUse) {
                return nullptr;
            }
            const ui32 num = ChunksToUse - (ReservedChunks.size() + ChunkReservePending);
            ChunkReservePending += num;
            return std::make_unique<NPDisk::TEvChunkReserve>(PDiskCtx->Dsk->Owner, PDiskCtx->Dsk->OwnerRound, num);
        }
    };

} // NKikimr

#pragma once

#include "defs.h"
#include "blobstorage_readbatch.h"
#include "blobstorage_hullcompactdeferredqueue.h"
#include <ydb/core/blobstorage/vdisk/balance/handoff_map.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/blobstorage_hullwritesst.h>
#include <ydb/core/blobstorage/vdisk/hulldb/blobstorage_hullgcmap.h>
#include <ydb/core/blobstorage/vdisk/scrub/restore_corrupted_blob_actor.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_hugeblobctx.h>

namespace NKikimr {

    template<typename TKey, typename TMemRec, typename TIterator>
    class THullCompactionWorker {
        static constexpr bool LogoBlobs = std::is_same_v<TKey, TKeyLogoBlob>;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // COMMON TYPE ALIASES
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        // handoff map
        using THandoffMap = NKikimr::THandoffMap<TKey, TMemRec>;
        using THandoffMapPtr = TIntrusivePtr<THandoffMap>;

        // compaction record merger
        using TCompactRecordMerger = NKikimr::TCompactRecordMerger<TKey, TMemRec>;

        // level segment
        using TLevelSegment = NKikimr::TLevelSegment<TKey, TMemRec>;
        using TWriter = typename TLevelSegment::TWriter;

        // level index
        using TLevelIndex = NKikimr::TLevelIndex<TKey, TMemRec>;
        using TLevelIndexSnapshot = NKikimr::TLevelIndexSnapshot<TKey, TMemRec>;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // DEFERRED ITEM QUEUE PROCESSOR
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        class TDeferredItemQueue : public TDeferredItemQueueBase<TDeferredItemQueue> {
            THullCompactionWorker *Worker = nullptr;

            friend class TDeferredItemQueueBase<TDeferredItemQueue>;

            void StartImpl(THullCompactionWorker *worker) {
                Y_VERIFY_S(!Worker, this->VDiskLogPrefix);
                Worker = worker;
                Y_VERIFY_S(Worker, this->VDiskLogPrefix);
                Y_VERIFY_S(Worker->WriterPtr, this->VDiskLogPrefix);
            }

            void ProcessItemImpl(const TDiskPart& preallocatedLocation, TRope&& buffer, bool isInline) {
                Y_DEBUG_ABORT_UNLESS(Worker);

                if (isInline) {
                    const TDiskPart writtenLocation = Worker->WriterPtr->PushDataOnly(std::move(buffer));
                    Y_VERIFY_S(writtenLocation == preallocatedLocation, this->VDiskLogPrefix);
                } else {
                    Y_VERIFY_S(preallocatedLocation.Size == buffer.GetSize(), this->VDiskLogPrefix);
                    size_t fullSize = buffer.GetSize();
                    if (const size_t misalign = fullSize % Worker->PDiskCtx->Dsk->AppendBlockSize) {
                        fullSize += Worker->PDiskCtx->Dsk->AppendBlockSize - misalign;
                    }
                    auto partsPtr = MakeIntrusive<NPDisk::TEvChunkWrite::TRopeAlignedParts>(std::move(buffer), fullSize);
                    void *cookie = nullptr;
                    auto write = std::make_unique<NPDisk::TEvChunkWrite>(Worker->PDiskCtx->Dsk->Owner,
                        Worker->PDiskCtx->Dsk->OwnerRound, preallocatedLocation.ChunkIdx, preallocatedLocation.Offset,
                        partsPtr, cookie, true, NPriWrite::HullComp, false);
                    Worker->PendingWrites.push_back(std::move(write));
                }
            }

            void FinishImpl() {
                Y_VERIFY_S(Worker, this->VDiskLogPrefix);
                Worker = nullptr;
            }

        public:
            TDeferredItemQueue(const TString& prefix, TRopeArena& arena, TBlobStorageGroupType gtype,
                    EBlobHeaderMode blobHeaderMode)
                : TDeferredItemQueueBase<TDeferredItemQueue>(prefix, arena, gtype, blobHeaderMode)
            {}
        };

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // LOCAL TYPE DEFINITIONS
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        // state of compaction worker automaton
        enum class EState {
            Invalid,                // invalid state; this state should never be reached
            GetNextItem,            // going to extract next item for processing or finish if there are no more items
            WaitForSlotAllocation,  // waiting for slot allocation from huge keeper
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
        THugeBlobCtxPtr HugeBlobCtx;
        ui32 MinHugeBlobInBytes;

        // Group Type
        const TBlobStorageGroupType GType;

        // pointer to level index
        const TIntrusivePtr<TLevelIndex> LevelIndex;

        // handoff map we use to transform items
        THandoffMapPtr Hmp;

        // garbage collector iterator
        TIntrusivePtr<TBarriersSnapshot::TBarriersEssence> Barriers;

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
        TCompactRecordMerger IndexMerger;

        // current handoff-transformed MemRec
        std::optional<TMemRec> MemRec;

        // SST writer
        std::unique_ptr<TWriter> WriterPtr;
        bool WriterHasPendingOperations = false;

        // number of chunks we have asked to reserve, but not yet confirmed
        ui32 ChunkReservePending = 0;

        // automaton state
        EState State = EState::Invalid;

        // number of currently unresponded write requests
        ui32 InFlightWrites = 0;

        // number of currently unresponded read requests
        ui32 InFlightReads = 0;

        // vector of freed huge blobs
        TDiskPartVec FreedHugeBlobs;
        TDiskPartVec AllocatedHugeBlobs;

        // generated level segments
        TVector<TIntrusivePtr<TLevelSegment>> LevelSegments;

        // generated chunks
        TVector<TChunkIdx> CommitChunks;

        // pointer to an atomic variable contaning number of in flight reads
        TAtomic *ReadsInFlight;

        // pointer to an atomic variable contaning number of in flight writes
        TAtomic *WritesInFlight;

        // max inflight request to pdisk
        ui32 MaxInFlightWrites;
        ui32 MaxInFlightReads;

        struct TBatcherPayload {
            ui64 Id = 0;
            ui8 PartIdx;
            TLogoBlobID BlobId;
            TDiskPart Location;

            TBatcherPayload() = default;
            TBatcherPayload(ui64 id, ui8 partIdx, TLogoBlobID blobId, TDiskPart location)
                : Id(id)
                , PartIdx(partIdx)
                , BlobId(blobId)
                , Location(location)
            {}
        };
        TCompactReadBatcher<TBatcherPayload> ReadBatcher;

        // arena for different kinds of small-block allocations
        TRopeArena Arena;

        TDeferredItemQueue DeferredItems;
        ui64 NextDeferredItemId = 1;

        TKey Key; // current key
        std::optional<TKey> PreviousKey; // previous key (nullopt for the first iteration)

        TLevelIndexSnapshot *LevelSnap = nullptr;
        std::optional<typename TLevelIndexSnapshot::TForwardIterator> LevelSnapIt;

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

        const bool AllowGarbageCollection;

        std::deque<std::unique_ptr<NPDisk::TEvChunkWrite>> PendingWrites;

    public:
        THullCompactionWorker(THullCtxPtr hullCtx,
                              TPDiskCtxPtr pdiskCtx,
                              THugeBlobCtxPtr hugeBlobCtx,
                              ui32 minHugeBlobInBytes,
                              TIntrusivePtr<TLevelIndex> levelIndex,
                              const TIterator& it,
                              bool isFresh,
                              ui64 firstLsn,
                              ui64 lastLsn,
                              TDuration restoreDeadline,
                              std::optional<TKey> partitionKey,
                              bool allowGarbageCollection)
            : HullCtx(std::move(hullCtx))
            , PDiskCtx(std::move(pdiskCtx))
            , HugeBlobCtx(std::move(hugeBlobCtx))
            , MinHugeBlobInBytes(minHugeBlobInBytes)
            , GType(HullCtx->VCtx->Top->GType)
            , LevelIndex(std::move(levelIndex))
            , FirstLsn(firstLsn)
            , LastLsn(lastLsn)
            , It(it)
            , IsFresh(isFresh)
            , IndexMerger(GType, HullCtx->VCfg->BlobHeaderMode)
            , ReadBatcher(HullCtx->VCtx->VDiskLogPrefix,
                    PDiskCtx->Dsk->ReadBlockSize,
                    PDiskCtx->Dsk->SeekTimeUs * PDiskCtx->Dsk->ReadSpeedBps / 1000000,
                    HullCtx->HullCompReadBatchEfficiencyThreshold)
            , Arena(&TRopeArenaBackend::Allocate)
            , DeferredItems(HullCtx->VCtx->VDiskLogPrefix, Arena, HullCtx->VCtx->Top->GType, HullCtx->VCfg->BlobHeaderMode)
            , Statistics(HullCtx)
            , RestoreDeadline(restoreDeadline)
            , PartitionKey(partitionKey)
            , AllowGarbageCollection(allowGarbageCollection)
        {
            if (IsFresh) {
                ChunksToUse = HullCtx->HullSstSizeInChunksFresh;
                ReadsInFlight = &LevelIndex->FreshCompReadsInFlight;
                WritesInFlight = &LevelIndex->FreshCompWritesInFlight;
            } else {
                ChunksToUse = HullCtx->HullSstSizeInChunksLevel;
                ReadsInFlight = &LevelIndex->HullCompReadsInFlight;
                WritesInFlight = &LevelIndex->HullCompWritesInFlight;
            }

            MaxInFlightWrites = GetMaxInFlightWrites();
            MaxInFlightReads = GetMaxInFlightReads();
        }

        void Prepare(THandoffMapPtr hmp, TIntrusivePtr<TBarriersSnapshot::TBarriersEssence> barriers,
                TLevelIndexSnapshot *levelSnap) {
            Hmp = std::move(hmp);
            Barriers = std::move(barriers);
            LevelSnap = levelSnap;
            State = EState::GetNextItem;
        }

        // main cycle function; return true if compaction is finished and compaction actor can proceed to index load;
        // when there is more work to do, return false; MUST NOT return true unless all pending requests are finished
        bool MainCycle(TVector<std::unique_ptr<IEventBase>>& msgsForYard, std::vector<ui32> **slotAllocations) {
            for (;;) {
                switch (State) {
                    case EState::Invalid:
                        Y_ABORT("unexpected state");

                    case EState::GetNextItem:
                        if (It.Valid()) {
                            Key = It.GetCurKey();
                            Y_VERIFY_S(!PreviousKey || *PreviousKey < Key, HullCtx->VCtx->VDiskLogPrefix
                                << "duplicate keys: " << PreviousKey->ToString() << " -> " << Key.ToString());

                            // iterator is valid and we have one more item to process; instruct merger whether we want
                            // data or not and proceed to TryProcessItem state
                            It.PutToMerger(&IndexMerger);

                            const bool haveToProcessItem = PreprocessItem();
                            if (!haveToProcessItem) {
                                FinishItem();
                            } else if (TDataMerger& dataMerger = IndexMerger.GetDataMerger(); !LogoBlobs ||
                                    dataMerger.GetSlotsToAllocate().empty()) {
                                State = EState::TryProcessItem;
                            } else {
                                State = EState::WaitForSlotAllocation;
                                *slotAllocations = &dataMerger.GetSlotsToAllocate();
                                return false; // expect allocated slots to continue compacting
                            }
                        }  else if (WriterPtr) {
                            StartCollectingDeferredItems();
                        } else {
                            // iterator is not valid and we have no writer -- so just proceed to WaitForPendingRequests
                            // state and finish
                            State = EState::WaitForPendingRequests;
                        }
                        break;

                    case EState::WaitForSlotAllocation:
                        return false;

                    case EState::TryProcessItem:
                        // ensure we have transformed item
                        Y_VERIFY_S(MemRec, HullCtx->VCtx->VDiskLogPrefix);
                        // try to process it
                        switch (TryProcessItem()) {
                            case ETryProcessItemStatus::Success:
                                // try to send some messages if needed
                                ProcessPendingMessages(msgsForYard);
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
                                    Y_VERIFY_S(ChunkReservePending, HullCtx->VCtx->VDiskLogPrefix);
                                }
                                return false;

                            case ETryProcessItemStatus::FinishSST:
                                StartCollectingDeferredItems();
                                break;
                        }
                        break;

                    case EState::WaitingForDeferredItems:
                        ProcessPendingMessages(msgsForYard); // issue any messages generated by deferred items queue
                        if (PendingWrites.size()) {
                            return false;
                        }
                        if (!DeferredItems.AllProcessed()) {
                            return false;
                        }
                        DeferredItems.Finish();
                        // we have finished will all of the deferred items, it's a good time to flush SST now
                        State = EState::FlushingSST;
                        ReadBatcher.Finish();
                        break;

                    case EState::FlushingSST: {
                        // if MemRec is set, then this state was invoked from the TryProcessItem call
                        const bool finished = FlushSST();
                        State = !finished ? State : MemRec ? EState::TryProcessItem : EState::GetNextItem;
                        ProcessPendingMessages(msgsForYard); // issue any generated messages
                        if (finished) {
                            Y_VERIFY_S(!WriterPtr->GetPendingMessage(), HullCtx->VCtx->VDiskLogPrefix);
                            WriterPtr.reset();
                        } else {
                            Y_VERIFY_S(InFlightWrites == MaxInFlightWrites, HullCtx->VCtx->VDiskLogPrefix);
                            return false;
                        }
                        break;
                    }


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

        void StartCollectingDeferredItems() {
            DeferredItems.Start(this);
            ReadBatcher.Start();
            State = EState::WaitingForDeferredItems;
        }

        TEvRestoreCorruptedBlob *Apply(NPDisk::TEvChunkReadResult *msg, TInstant now) {
            AtomicDecrement(*ReadsInFlight);
            Y_VERIFY_S(InFlightReads > 0, HullCtx->VCtx->VDiskLogPrefix);
            --InFlightReads;

            // apply read result to batcher
            ReadBatcher.Apply(msg);
            return ProcessReadBatcher(now);
        }

        bool ExpectingBlobRestoration = false;

        TEvRestoreCorruptedBlob *Apply(TEvRestoreCorruptedBlobResult *msg, bool *isAborting, TInstant now) {
            Y_VERIFY_S(msg->Items.size() == 1, HullCtx->VCtx->VDiskLogPrefix);
            Y_VERIFY_S(ExpectingBlobRestoration, HullCtx->VCtx->VDiskLogPrefix);
            ExpectingBlobRestoration = false;
            auto& item = msg->Items.front();
            switch (item.Status) {
                case NKikimrProto::OK: {
                    Y_DEBUG_ABORT_UNLESS(item.Needed.CountBits() == 1);
                    const ui8 partIdx = item.Needed.FirstPosition();
                    DeferredItems.AddReadDiskBlob(item.Cookie, std::move(item.Parts[partIdx]), partIdx);
                    return ProcessReadBatcher(now);
                }

                case NKikimrProto::DEADLINE:
                case NKikimrProto::ERROR:
                    *isAborting = true;
                    return nullptr;

                default:
                    Y_ABORT_S(HullCtx->VCtx->VDiskLogPrefix);
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
                    const auto needed = NMatrix::TVectorType::MakeOneHot(payload.PartIdx, GType.TotalPartCount());
                    TEvRestoreCorruptedBlob::TItem item(payload.BlobId, needed, GType, payload.Location, payload.Id);
                    return new TEvRestoreCorruptedBlob(now + RestoreDeadline, {1u, item}, false, true);
                } else {
                    Y_VERIFY_S(status == NKikimrProto::OK, HullCtx->VCtx->VDiskLogPrefix);
                    DeferredItems.AddReadDiskBlob(payload.Id, TRope(std::move(buffer)), payload.PartIdx);
                }
            }
            return nullptr;
        }

        void Apply(NPDisk::TEvChunkWriteResult * /*msg*/) {
            // adjust number of in flight messages
            Y_VERIFY_S(InFlightWrites > 0, HullCtx->VCtx->VDiskLogPrefix);
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

        void Apply(TEvHugeAllocateSlotsResult *msg) {
            if constexpr (LogoBlobs) {
                Y_DEBUG_ABORT_UNLESS(State == EState::WaitForSlotAllocation);
                State = EState::TryProcessItem;
                for (const TDiskPart& p : msg->Locations) { // remember newly allocated slots for entrypoint
                    AllocatedHugeBlobs.PushBack(p);
                }
                IndexMerger.GetDataMerger().ApplyAllocatedSlots(msg->Locations);
            } else {
                Y_ABORT("impossible case");
            }
        }

        const TVector<TIntrusivePtr<TLevelSegment>>& GetLevelSegments() { return LevelSegments; }
        const TVector<TChunkIdx>& GetCommitChunks() const { return CommitChunks; }
        const TDiskPartVec& GetFreedHugeBlobs() const { return FreedHugeBlobs; }
        const TDiskPartVec& GetAllocatedHugeBlobs() const { return AllocatedHugeBlobs; }
        const TDeque<TChunkIdx>& GetReservedChunks() const { return ReservedChunks; }
        const TDeque<TChunkIdx>& GetAllocatedChunks() const { return AllocatedChunks; }

    private:
        void CollectRemovedHugeBlobs(const std::vector<TDiskPart>& hugeBlobs) {
            for (const TDiskPart& p : hugeBlobs) {
                if (!p.Empty()) {
                    FreedHugeBlobs.PushBack(p);
                }
            }
        }

        // start item processing; this function transforms item using handoff map and adds collected huge blobs, if any
        // it returns true if we should keep this item; otherwise it returns false
        bool PreprocessItem() {
            NGc::TKeepStatus keep(true);

            if constexpr (LogoBlobs) {
                if (!LevelSnapIt) {
                    LevelSnapIt.emplace(HullCtx, LevelSnap);
                    LevelSnapIt->Seek(Key);
                } else {
                    for (ui32 i = 0; i < 6 && LevelSnapIt->Valid() && LevelSnapIt->GetCurKey() < Key; ++i) {
                        LevelSnapIt->Next();
                    }
                    if (LevelSnapIt->Valid() && LevelSnapIt->GetCurKey() < Key) {
                        LevelSnapIt->Seek(Key);
                    }
                }
                Y_VERIFY_S(LevelSnapIt->Valid(), HullCtx->VCtx->VDiskLogPrefix);
                Y_VERIFY_S(LevelSnapIt->GetCurKey() == Key, HullCtx->VCtx->VDiskLogPrefix);

                const ui32 subsKeep = IndexMerger.GetNumKeepFlags();
                const ui32 subsDoNotKeep = IndexMerger.GetNumDoNotKeepFlags();

                IndexMerger.SetExternalDataStage();
                LevelSnapIt->PutToMerger(&IndexMerger);

                const ui32 wholeKeep = IndexMerger.GetNumKeepFlags() - subsKeep; // they are counted too
                const ui32 wholeDoNotKeep = IndexMerger.GetNumDoNotKeepFlags() - subsDoNotKeep; // so are they

                NGcOpt::TKeepFlagStat keepFlagStat;
                if (IsFresh) {
                    keepFlagStat.Needed = true;
                } else {
                    keepFlagStat = {subsKeep, subsDoNotKeep, wholeKeep, wholeDoNotKeep};
                }
                keep = Barriers->Keep(Key, IndexMerger.GetMemRecForBarriers(), keepFlagStat, HullCtx->AllowKeepFlags,
                    AllowGarbageCollection);

                const TLogoBlobID& id = Key.LogoBlobID();
                if (!TBlobStorageGroupType::IsCrcModeValid(id.CrcMode())) {
                    LOG_CRIT_S(*TlsActivationContext, NKikimrServices::BS_SKELETON, HullCtx->VCtx->VDiskLogPrefix
                        << "invalid CrcMode in BlobId found during compaction"
                        << " BlobId# " << id.ToString()
                        << " KeepIndex# " << keep.KeepIndex
                        << " KeepData# " << keep.KeepData
                        << " SubsKeep# " << subsKeep
                        << " SubsDoNotKeep# " << subsDoNotKeep
                        << " WholeKeep# " << wholeKeep
                        << " WholeDoNotKeep# " << wholeDoNotKeep);
                }

                IndexMerger.Finish(HugeBlobCtx->IsHugeBlob(GType, id, MinHugeBlobInBytes), keep.KeepData);
            } else {
                keep = Barriers->Keep(Key, IndexMerger.GetMemRecForBarriers(), {}, HullCtx->AllowKeepFlags,
                    AllowGarbageCollection);

                IndexMerger.Finish(false, false);
            }

            Y_VERIFY_S(keep.KeepIndex || !keep.KeepData, HullCtx->VCtx->VDiskLogPrefix); // either we keep the item, or we drop it along with data

            if (keep.KeepIndex) {
                ++(keep.KeepData ? Statistics.KeepItemsWithData : Statistics.KeepItemsWOData);
            } else {
                ++Statistics.DontKeepItems;
            }

            if (keep.KeepIndex) {
                Hmp->Transform(Key, MemRec.emplace(IndexMerger.GetMemRec()), IndexMerger.GetDataMerger());
            }

            if constexpr (LogoBlobs) {
                CollectRemovedHugeBlobs(IndexMerger.GetDataMerger().GetDeletedHugeBlobs());
            }

            return keep.KeepIndex;
        }

        ETryProcessItemStatus TryProcessItem() {
            // if we have PartitionKey, check it is time to split partitions by PartitionKey
            if (PartitionKey && PreviousKey && *PreviousKey < *PartitionKey && Key <= *PartitionKey && WriterPtr) {
                return ETryProcessItemStatus::FinishSST;
            }

            // if there is no active writer, create one and start writing
            if (!WriterPtr) {
                // ensure we have enough reserved chunks to do operation; or else request for allocation and wait
                if (ReservedChunks.size() < ChunksToUse) {
                    return ETryProcessItemStatus::NeedMoreChunks;
                }

                // create new instance of writer
                WriterPtr = std::make_unique<TWriter>(HullCtx->VCtx, IsFresh ? EWriterDataType::Fresh : EWriterDataType::Comp,
                    ChunksToUse, PDiskCtx->Dsk->Owner, PDiskCtx->Dsk->OwnerRound, (ui32)PDiskCtx->Dsk->ChunkSize,
                    PDiskCtx->Dsk->AppendBlockSize, (ui32)PDiskCtx->Dsk->BulkWriteBlockSize, LevelIndex->AllocSstId(),
                    false, ReservedChunks, Arena, HullCtx->VCfg->BlobHeaderMode);

                WriterHasPendingOperations = false;
            }

            // try to push blob to the index
            TDataMerger& dataMerger = IndexMerger.GetDataMerger();
            TDiskPart preallocatedLocation;
            if (!WriterPtr->PushIndexOnly(Key, *MemRec, LogoBlobs ? &dataMerger : nullptr, &preallocatedLocation)) {
                return ETryProcessItemStatus::FinishSST;
            }

            // count added item
            Statistics.ItemAdded();

            if constexpr (LogoBlobs) {
                const TLogoBlobID& blobId = Key.LogoBlobID();

                auto& collectTask = dataMerger.GetCollectTask();
                if (MemRec->GetType() == TBlobType::DiskBlob && MemRec->DataSize()) {
                    // ensure preallocated location has correct size
                    Y_DEBUG_ABORT_UNLESS(preallocatedLocation.ChunkIdx && preallocatedLocation.Size == MemRec->DataSize());
                    // producing inline blob with data here
                    for (const auto& [location, partIdx] : collectTask.Reads) {
                        ReadBatcher.AddReadItem(location, {NextDeferredItemId, partIdx, blobId, location});
                    }
                    if (!collectTask.Reads.empty() || WriterHasPendingOperations) { // defer this blob
                        DeferredItems.Put(NextDeferredItemId++, collectTask.Reads.size(), preallocatedLocation,
                            collectTask.BlobMerger, blobId, true);
                        WriterHasPendingOperations = true;
                    } else { // we can and will produce this inline blob now
                        const TDiskPart writtenLocation = WriterPtr->PushDataOnly(dataMerger.CreateDiskBlob(Arena));
                        Y_VERIFY_S(writtenLocation == preallocatedLocation, HullCtx->VCtx->VDiskLogPrefix);
                    }
                } else {
                    Y_VERIFY_S(collectTask.BlobMerger.ContainsMetadataPartsOnly() || collectTask.BlobMerger.Empty(),
                        HullCtx->VCtx->VDiskLogPrefix);
                    Y_VERIFY_S(collectTask.Reads.empty(), HullCtx->VCtx->VDiskLogPrefix);
                }

                for (const auto& [partIdx, from, to] : dataMerger.GetHugeBlobWrites()) {
                    const auto parts = NMatrix::TVectorType::MakeOneHot(partIdx, GType.TotalPartCount());
                    DeferredItems.Put(NextDeferredItemId++, 0, to, TDiskBlob(from, parts, GType, blobId), blobId, false);
                }

                for (const auto& [partIdx, from, to] : dataMerger.GetHugeBlobMoves()) {
                    ReadBatcher.AddReadItem(from, {NextDeferredItemId, partIdx, blobId, from});
                    DeferredItems.Put(NextDeferredItemId++, 1, to, TDiskBlobMerger(), blobId, false);
                }
            }

            // return success indicating that this item required no further processing
            return ETryProcessItemStatus::Success;
        }

        void FinishItem() {
            // adjust previous key
            PreviousKey.emplace(Key);
            // clear merger and on-disk record list and advance both iterators synchronously
            IndexMerger.Clear();
            It.Next();
            MemRec.reset();
        }

        bool FlushSST() {
            // try to flush some more data; if the flush fails, it means that we have reached in flight write limit and
            // there is nothing to do here now, so we return
            if (!WriterPtr->FlushNext(FirstLsn, LastLsn, MaxInFlightWrites - InFlightWrites)) {
                return false;
            }

            // get writer conclusion and fill in entrypoint and used chunks vector
            const auto& conclusion = WriterPtr->GetConclusion();
            LevelSegments.push_back(conclusion.LevelSegment);
            CommitChunks.insert(CommitChunks.end(), conclusion.UsedChunks.begin(), conclusion.UsedChunks.end());

            return true;
        }

        void ProcessPendingMessages(TVector<std::unique_ptr<IEventBase>>& msgsForYard) {
            // ensure that we have writer
            Y_VERIFY_S(WriterPtr, HullCtx->VCtx->VDiskLogPrefix);
            Y_VERIFY_S(MaxInFlightWrites, HullCtx->VCtx->VDiskLogPrefix);
            Y_VERIFY_S(MaxInFlightReads, HullCtx->VCtx->VDiskLogPrefix);

            // send new messages until we reach in flight limit
            std::unique_ptr<NPDisk::TEvChunkWrite> msg;
            while (InFlightWrites < MaxInFlightWrites && (msg = GetPendingWriteMessage())) {
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

        std::unique_ptr<NPDisk::TEvChunkWrite> GetPendingWriteMessage() {
            std::unique_ptr<NPDisk::TEvChunkWrite> res = WriterPtr->GetPendingMessage();
            if (!res && !PendingWrites.empty()) {
                res = std::move(PendingWrites.front());
                PendingWrites.pop_front();
            }
            return res;
        }

        std::unique_ptr<NPDisk::TEvChunkReserve> CheckForReservation() {
            if (ReservedChunks.size() + ChunkReservePending >= ChunksToUse) {
                return nullptr;
            }
            const ui32 num = ChunksToUse - (ReservedChunks.size() + ChunkReservePending);
            ChunkReservePending += num;
            return std::make_unique<NPDisk::TEvChunkReserve>(PDiskCtx->Dsk->Owner, PDiskCtx->Dsk->OwnerRound, num);
        }

        ui32 GetMaxInFlightWrites() {
            return IsFresh ? HullCtx->VCfg->FreshCompMaxInFlightWrites : HullCtx->VCfg->HullCompMaxInFlightWrites;
        }

        ui32 GetMaxInFlightReads() {
            return IsFresh ? (ui32) HullCtx->VCfg->FreshCompMaxInFlightReads : (ui32) HullCtx->VCfg->HullCompMaxInFlightReads;
        }
    };

} // NKikimr

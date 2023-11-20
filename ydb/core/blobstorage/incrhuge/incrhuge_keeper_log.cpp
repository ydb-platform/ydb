#include "incrhuge_keeper_log.h"
#include "incrhuge_keeper.h"
#include <ydb/core/protos/blobstorage_vdisk_internal.pb.h>

#include <google/protobuf/text_format.h>
#include <util/generic/variant.h>

namespace NKikimr {
    namespace NIncrHuge {

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // CHUNK LOG RECORD MERGER
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        void TChunkRecordMerger::operator ()(const NKikimrVDiskData::TIncrHugeChunks& record) {
            for (const auto& c : record.GetChunks()) {
                Y_ABORT_UNLESS(c.HasChunkIdx() && c.HasChunkSerNum() && c.HasState());
                const TChunkIdx chunkIdx = c.GetChunkIdx();
                const TChunkSerNum chunkSerNum(c.GetChunkSerNum());
                auto it = Chunks.find(chunkIdx);
                if (it != Chunks.end()) {
                    TChunkInfo& chunk = it->second;
                    Y_ABORT_UNLESS(chunk.ChunkSerNum == chunkSerNum);
                    chunk.State = c.GetState();
                } else {
                    Chunks.emplace(chunkIdx, TChunkInfo{chunkSerNum, c.GetState()});
                }
            }
            for (TChunkIdx chunkIdx : record.GetDeletedChunks()) {
                auto it = Chunks.find(chunkIdx);
                Y_ABORT_UNLESS(it != Chunks.end());
                Chunks.erase(it);
            }
            if (record.HasCurrentSerNum()) {
                CurrentSerNum = TChunkSerNum(record.GetCurrentSerNum());
            }
        }

        void TChunkRecordMerger::operator ()(const TChunkAllocation& record) {
            for (size_t i = 0; i < record.NewChunkIds.size(); ++i) {
                const TChunkIdx chunkIdx = record.NewChunkIds[i];
                Y_ABORT_UNLESS(!Chunks.count(chunkIdx));
                Chunks[chunkIdx] = TChunkInfo{
                    record.BaseSerNum.Add(i),
                    NKikimrVDiskData::TIncrHugeChunks::WriteIntent
                };
            }
            CurrentSerNum = record.CurrentSerNum;
        }

        void TChunkRecordMerger::operator ()(const TChunkDeletion& record) {
            auto it = Chunks.find(record.ChunkIdx);
            Y_ABORT_UNLESS(it != Chunks.end());
            Chunks.erase(it);
        }

        void TChunkRecordMerger::operator ()(const TCompleteChunk& record) {
            auto it = Chunks.find(record.ChunkIdx);
            Y_ABORT_UNLESS(it != Chunks.end());
            TChunkInfo& chunk = it->second;
            Y_ABORT_UNLESS(chunk.ChunkSerNum == record.ChunkSerNum);
            Y_ABORT_UNLESS(chunk.State == NKikimrVDiskData::TIncrHugeChunks::WriteIntent);
            chunk.State = NKikimrVDiskData::TIncrHugeChunks::Complete;
        }

        NKikimrVDiskData::TIncrHugeChunks TChunkRecordMerger::GetCurrentState() const {
            NKikimrVDiskData::TIncrHugeChunks record;
            for (const auto& pair : Chunks) {
                const TChunkIdx chunkIdx = pair.first;
                const TChunkInfo& chunk = pair.second;
                auto *c = record.AddChunks();
                c->SetChunkIdx(chunkIdx);
                c->SetChunkSerNum(static_cast<ui64>(chunk.ChunkSerNum));
                c->SetState(chunk.State);
            }
            if (CurrentSerNum) {
                record.SetCurrentSerNum(static_cast<ui64>(*CurrentSerNum));
            }
            return record;
        }

        TString TChunkRecordMerger::Serialize(const NKikimrVDiskData::TIncrHugeChunks& record) {
            TString data;
            bool status = record.SerializeToString(&data);
            Y_ABORT_UNLESS(status);
            return data;
        }

        TString TChunkRecordMerger::Serialize(const TChunkAllocation& record) {
            NKikimrVDiskData::TIncrHugeChunks protobuf;
            for (size_t i = 0; i < record.NewChunkIds.size(); ++i) {
                auto *c = protobuf.AddChunks();
                c->SetChunkIdx(record.NewChunkIds[i]);
                c->SetChunkSerNum(static_cast<ui64>(record.BaseSerNum.Add(i)));
                c->SetState(NKikimrVDiskData::TIncrHugeChunks::WriteIntent);
            }
            protobuf.SetCurrentSerNum(static_cast<ui64>(record.CurrentSerNum));
            return Serialize(protobuf);
        }

        TString TChunkRecordMerger::Serialize(const TChunkDeletion& record) {
            NKikimrVDiskData::TIncrHugeChunks protobuf;
            protobuf.AddDeletedChunks(record.ChunkIdx);
            return Serialize(protobuf);
        }

        TString TChunkRecordMerger::Serialize(const TCompleteChunk& record) {
            NKikimrVDiskData::TIncrHugeChunks protobuf;
            auto *c = protobuf.AddChunks();
            c->SetChunkIdx(record.ChunkIdx);
            c->SetChunkSerNum(static_cast<ui64>(record.ChunkSerNum));
            c->SetState(NKikimrVDiskData::TIncrHugeChunks::Complete);
            return Serialize(protobuf);
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // DELETE LOG RECORD MERGER
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        // store deletion bitmap into log record item
        void SerializeDeletes(const TDynBitMap& deletedItems, NKikimrVDiskData::TIncrHugeDelete::TChunkInfo *chunk) {
            // serialize into item list
            auto& items = *chunk->MutableDeletedItems();
            size_t first = 0, count = 0;
            auto add = [&] {
                if (count == 1) {
                    items.AddIndexes(first);
                } else if (count > 1) {
                    auto *range = items.AddRanges();
                    range->SetFirst(first);
                    range->SetCount(count);
                }
            };
            Y_FOR_EACH_BIT(index, deletedItems) {
                if (index == first + count) {
                    ++count;
                } else {
                    add();
                    first = index;
                    count = 1;
                }
            }
            add();

            // second, serialize into string
            TStringStream stream;
            deletedItems.Save(&stream);
            TString bits = stream.Str();

            // choose the best
            if (bits.size() < (size_t)items.ByteSize()) {
                chunk->SetBits(bits);
            }
        }

        void DeserializeDeletes(TDynBitMap& deletedItems, const NKikimrVDiskData::TIncrHugeDelete::TChunkInfo& chunk) {
            switch (chunk.DeletedData_case()) {
                case NKikimrVDiskData::TIncrHugeDelete::TChunkInfo::kDeletedItems: {
                    const auto& x = chunk.GetDeletedItems();
                    for (ui32 index : x.GetIndexes()) {
                        deletedItems.Set(index);
                    }
                    for (const auto& range : x.GetRanges()) {
                        Y_ABORT_UNLESS(range.HasFirst() && range.HasCount());
                        const ui32 first = range.GetFirst();
                        const ui32 count = range.GetCount();
                        deletedItems.Set(first, first + count);
                    }
                    break;
                }

                case NKikimrVDiskData::TIncrHugeDelete::TChunkInfo::kBits: {
                    TStringStream stream(chunk.GetBits());
                    deletedItems.Load(&stream);
                    break;
                }

                default:
                    Y_ABORT("unexpected case");
            }
        }

        void TDeleteRecordMerger::operator ()(const NKikimrVDiskData::TIncrHugeDelete& record) {
            for (const auto& c : record.GetChunks()) {
                Y_ABORT_UNLESS(c.HasChunkSerNum());
                const TChunkSerNum chunkSerNum(c.GetChunkSerNum());

                // find matching entry
                TDynBitMap& deletedItems = SerNumToChunk[chunkSerNum];

                // deserialize deletion bitmap
                TDynBitMap newDeletes;
                DeserializeDeletes(newDeletes, c);

                // ensure that there are no intersections
                Y_ABORT_UNLESS((newDeletes & deletedItems).Count() == 0);

                // combine items
                deletedItems |= newDeletes;
            }

            // merge owner to seq no map
            for (const auto& o : record.GetOwners()) {
                Y_ABORT_UNLESS(o.HasOwner() && o.HasSeqNo());

                // verify that owner's sequence numbers are logged in strictly increasing order
                auto it = OwnerToSeqNo.find(o.GetOwner());
                if (it != OwnerToSeqNo.end()) {
                    Y_ABORT_UNLESS(it->second < o.GetSeqNo(), "Delete record reordering OldSeqNo# %" PRIu64 " NewSeqNo# %"
                            PRIu64, it->second, o.GetSeqNo());
                }

                // update seq no
                OwnerToSeqNo[o.GetOwner()] = o.GetSeqNo();
            }
        }

        void TDeleteRecordMerger::operator ()(const TBlobDeletes& record) {
            for (auto it = record.DeleteLocators.begin(); it != record.DeleteLocators.end(); ) {
                const TChunkIdx chunkIdx = it->ChunkIdx;
                TDynBitMap& deletedItems = SerNumToChunk[it->ChunkSerNum];
                for (; it != record.DeleteLocators.end() && it->ChunkIdx == chunkIdx; ++it) {
                    Y_ABORT_UNLESS(!deletedItems.Get(it->IndexInsideChunk), "trying to delete already deleted item ChunkIdx# %"
                            PRIu32 " ChunkSerNum# %s Id# %016" PRIx64 " IndexInsideChunk# %" PRIu32 " SizeInBlocks# %"
                            PRIu32, it->ChunkIdx, it->ChunkSerNum.ToString().data(), it->Id, it->IndexInsideChunk,
                            it->SizeInBlocks);
                    deletedItems.Set(it->IndexInsideChunk);
                }
            }

            if (record.Owner) {
                auto it = OwnerToSeqNo.find(record.Owner);
                if (it != OwnerToSeqNo.end()) {
                    Y_ABORT_UNLESS(it->second < record.SeqNo, "Delete record reordering OldSeqNo# %" PRIu64 " NewSeqNo# %"
                            PRIu64, it->second, record.SeqNo);
                }

                OwnerToSeqNo[record.Owner] = record.SeqNo;
            }
        }

        void TDeleteRecordMerger::operator ()(const TDeleteChunk& record) {
            auto it = SerNumToChunk.find(record.ChunkSerNum);
            Y_ABORT_UNLESS(it != SerNumToChunk.end());
            TDynBitMap& deletedItems = it->second;
            Y_ABORT_UNLESS(record.NumItems == deletedItems.Count());
            for (ui32 i = 0; i < record.NumItems; ++i) {
                // ensure that there are no gaps in deleted items
                Y_ABORT_UNLESS(deletedItems.Get(i));
            }
            SerNumToChunk.erase(it);
        }

        NKikimrVDiskData::TIncrHugeDelete TDeleteRecordMerger::GetCurrentState() const {
            NKikimrVDiskData::TIncrHugeDelete record;

            for (const auto& chunk : SerNumToChunk) {
                auto *c = record.AddChunks();
                c->SetChunkSerNum(static_cast<ui64>(chunk.first));
                SerializeDeletes(chunk.second, c);
            }

            for (const auto& owner : OwnerToSeqNo) {
                auto *o = record.AddOwners();
                o->SetOwner(owner.first);
                o->SetSeqNo(owner.second);
            }

            return record;
        }

        TString TDeleteRecordMerger::Serialize(const NKikimrVDiskData::TIncrHugeDelete& record) {
            TString data;
            bool status = record.SerializeToString(&data);
            Y_ABORT_UNLESS(status);
            return data;
        }

        TString TDeleteRecordMerger::Serialize(const TBlobDeletes& record) {
            NKikimrVDiskData::TIncrHugeDelete protobuf;
            if (record.Owner) {
                auto *owner = protobuf.AddOwners();
                owner->SetOwner(record.Owner);
                owner->SetSeqNo(record.SeqNo);
            }
            for (auto it = record.DeleteLocators.begin(); it != record.DeleteLocators.end(); ) {
                const TChunkIdx chunkIdx = it->ChunkIdx;
                const TChunkSerNum chunkSerNum = it->ChunkSerNum;

                // create bitmap of deleted items
                TDynBitMap deletedItems;
                for (; it != record.DeleteLocators.end() && it->ChunkIdx == chunkIdx; ++it) {
                    deletedItems.Set(it->IndexInsideChunk);
                }

                // serialize it into record
                auto *chunk = protobuf.AddChunks();
                chunk->SetChunkSerNum(static_cast<ui64>(chunkSerNum));
                SerializeDeletes(deletedItems, chunk);
            }
            return Serialize(protobuf);
        }

        TString TDeleteRecordMerger::Serialize(const TDeleteChunk& /*record*/) {
            Y_ABORT("this function should never be called");
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // TLogger CLASS
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        TLogger::TLogger(TKeeper& keeper)
            : TKeeperComponentBase(keeper, "Logger")
        {
            memset(LastSeqNo.data(), 0, LastSeqNo.size() * sizeof(ui64));
        }

        TLogger::~TLogger()
        {}

        void TLogger::SetLsnOnRecovery(ui64 lsn, bool issueInitialStartingPoints) {
            Lsn = lsn;
            IssueChunksStartingPoint = issueInitialStartingPoints;
        }

        struct TLogger::TChunkQueueItem {
            // log entry content
            std::variant<TChunkRecordMerger::TChunkAllocation, TChunkRecordMerger::TChunkDeletion,
                TChunkRecordMerger::TCompleteChunk, NKikimrVDiskData::TIncrHugeChunks> Content;

            // commit chunks
            TVector<TChunkIdx> CommitChunks;

            // delete chunks
            TVector<TChunkIdx> DeleteChunks;

            // callback to invoke upon completion
            std::unique_ptr<IEventCallback> Callback;

            // is it an entrypoint?
            bool Entrypoint;

            // lsn
            ui64 Lsn;
        };

        struct TLogger::TDeleteQueueItem {
            // actual record content depending on its type; for blob deletes it contains TBlobDeletes item, for
            // entrypoint -- full record that; this record will be simply merged into confirmed state upon success
            std::variant<TDeleteRecordMerger::TBlobDeletes, TDeleteRecordMerger::TDeleteChunk,
                NKikimrVDiskData::TIncrHugeDelete> Content;

            // vector of callbacks that are invoked on success or failure of logging
            TVector<std::unique_ptr<IEventCallback>> Callbacks;

            // is this an entrypoint?
            bool Entrypoint;

            // LSN
            ui64 Lsn;

            // is this virtual entrypoint?
            bool Virtual;

            // failure expected? debug only; used to detect situations when PDisk logger for queries 1, 2, 3 responds
            // with success to 1 and 3, but fails 2 -- this should be incorrect behavior and can lead to data leak
            bool FailureExpected;
        };

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // CHUNKS HANDLING LOGIC
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        void TLogger::LogChunkAllocation(TVector<TChunkIdx>&& newChunkIds, TChunkSerNum baseSerNum,
                std::unique_ptr<IEventCallback>&& callback, const TActorContext& ctx) {
            TVector<TChunkIdx> commitChunks(newChunkIds);
            ProcessChunkQueueItem(TChunkQueueItem{
                    TChunkRecordMerger::TChunkAllocation{std::move(newChunkIds), baseSerNum, Keeper.State.CurrentSerNum},
                    std::move(commitChunks),
                    {},
                    std::move(callback),
                    IssueChunksStartingPoint,
                    {},
                }, ctx);
            IssueChunksStartingPoint = false;
        }

        void TLogger::LogChunkDeletion(TChunkIdx chunkIdx, TChunkSerNum chunkSerNum, ui32 numItems,
                std::unique_ptr<IEventCallback>&& callback, const TActorContext& ctx) {
            ProcessChunkQueueItem(TChunkQueueItem{
                    TChunkRecordMerger::TChunkDeletion{chunkIdx, chunkSerNum, numItems},
                    {},
                    TVector<TChunkIdx>{chunkIdx},
                    std::move(callback),
                    IssueChunksStartingPoint,
                    {},
                }, ctx);
            IssueChunksStartingPoint = false;
        }

        void TLogger::LogCompleteChunk(TChunkIdx chunkIdx, TChunkSerNum chunkSerNum, const TActorContext& ctx) {
            ProcessChunkQueueItem(TChunkQueueItem{
                    TChunkRecordMerger::TCompleteChunk{chunkIdx, chunkSerNum},
                    {},
                    {},
                    {},
                    IssueChunksStartingPoint,
                    {},
                }, ctx);
            IssueChunksStartingPoint = false;
        }

        void TLogger::ProcessChunkQueueItem(TChunkQueueItem&& newItem, const TActorContext& ctx) {
            // if there is an entrypoint log pending, we do not accept new items, we put 'em into waiting queue
            if (LogChunksEntrypointPending) {
                PendingChunkQueue.push_back(std::move(newItem));
                return;
            }

            // insert new item into end of queue and get a reference to it
            ChunkQueue.push_back(std::move(newItem));
            TChunkQueueItem& item = ChunkQueue.back();

            // serialize it into string
            TRcBuf data = TRcBuf(std::visit([](const auto& content) { return TChunkRecordMerger::Serialize(content); }, item.Content));

            // generate LSN
            item.Lsn = Lsn++;

            // create callback which will be invoked by yard
            auto callback = [this, p = &item](NKikimrProto::EReplyStatus status, IEventBase *msg, const TActorContext& ctx) {
                ApplyLogChunkItem(*p, status, msg, ctx);
            };

            // create commit record
            NPDisk::TCommitRecord commit;
            commit.IsStartingPoint = item.Entrypoint;
            commit.CommitChunks = std::move(item.CommitChunks);
            commit.DeleteChunks = std::move(item.DeleteChunks);
            commit.FirstLsnToKeep = GetFirstLsnToKeep(item.Entrypoint ? EEntrypointType::Chunks : EEntrypointType::None);

            // issue log record
            TLsnSeg seg(item.Lsn, item.Lsn);
            ctx.Send(Keeper.State.Settings.PDiskActorId, new NPDisk::TEvLog(Keeper.State.PDiskParams->Owner,
                    Keeper.State.PDiskParams->OwnerRound, TLogSignature::SignatureIncrHugeChunks, commit, data,
                    seg, Keeper.RegisterYardCallback(MakeCallback(std::move(callback)))));

            if (item.Entrypoint) {
                ProcessedChunksWithoutEntrypoint = 0;
            } else if (++ProcessedChunksWithoutEntrypoint >= 10) { // FIXME correct number
                LogChunksEntrypoint(ctx);
            }
        }

        void TLogger::ApplyLogChunkItem(TChunkQueueItem& item, NKikimrProto::EReplyStatus status, IEventBase *msg,
                const TActorContext& ctx) {
            Y_ABORT_UNLESS(ChunkQueue && &item == &ChunkQueue.front());

            IHLOG_DEBUG(ctx, "ApplyLogChunkItem Lsn# %" PRIu64 " Status# %s",
                    item.Lsn, NKikimrProto::EReplyStatus_Name(status).data());

            if (status == NKikimrProto::OK) {
                // if this was an entrypoint, reset merger and update LSN
                if (item.Entrypoint) {
                    Y_ABORT_UNLESS(item.Lsn > ChunksEntrypointLsn);
                    ChunksEntrypointLsn = item.Lsn;
                    ConfirmedChunkMerger = TChunkRecordMerger();
                }
                // update confirmed state
                std::visit([this](const auto& content) { ConfirmedChunkMerger(content); }, item.Content);
                // if it was chunk deletion, propagate this information to deleter state
                if (auto *record = std::get_if<TChunkRecordMerger::TChunkDeletion>(&item.Content)) {
                    IHLOG_DEBUG(ctx, "DeleteChunk ChunkIdx# %" PRIu32 " ChunkSerNum# %s",
                            record->ChunkIdx, record->ChunkSerNum.ToString().data());

                    TDeleteQueueItem deleteItem{
                        TDeleteRecordMerger::TDeleteChunk{record->ChunkSerNum, record->NumItems}, // Content
                        {},                                                                       // Callback
                        false,                                                                    // Entrypoint
                        Lsn,                                                                      // Lsn
                        true,                                                                     // Virtual
                        false,                                                                    // FailureExpected
                    };
                    DeleteQueue.push_back(std::move(deleteItem));
                    ProcessDeleteQueueVirtualItems(ctx);
                }
            }

            // invoke callback
            if (std::unique_ptr<IEventCallback> callback = std::move(item.Callback)) {
                callback->Apply(status, msg, ctx);
            }

            // delete item
            ChunkQueue.pop_front();

            // if it was the last item and we have pending entrypoint set, then put it now
            if (!ChunkQueue && LogChunksEntrypointPending) {
                GenerateChunkEntrypoint(ctx);
            }
        }

        void TLogger::LogChunksEntrypoint(const TActorContext& ctx) {
            LogChunksEntrypointPending = true;
            if (!ChunkQueue) {
                GenerateChunkEntrypoint(ctx);
            }
        }

        void TLogger::GenerateChunkEntrypoint(const TActorContext& ctx) {
            Y_ABORT_UNLESS(LogChunksEntrypointPending);
            LogChunksEntrypointPending = false;

            // create new queue of requests, starting from entrypoint and including all pending items
            TChunkQueue temp;
            temp.swap(PendingChunkQueue);
            temp.push_front(TChunkQueueItem{ConfirmedChunkMerger.GetCurrentState(), {}, {}, {}, true, {}});

            // process queue
            for (TChunkQueueItem& item : temp) {
                ProcessChunkQueueItem(std::move(item), ctx);
            }
        }

        void TLogger::SetInitialChunksState(const NKikimrVDiskData::TIncrHugeChunks& chunks) {
            ConfirmedChunkMerger(chunks);
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // DELETE RECORDS PROCESSING LOGIC
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        void TLogger::LogBlobDeletes(ui8 owner, ui64 seqNo, TVector<TBlobDeleteLocator>&& deleteLocators,
                std::unique_ptr<IEventCallback>&& callback, const TActorContext& ctx) {
            // check that locators are sorted and do not repeat
            Y_ABORT_UNLESS(std::is_sorted(deleteLocators.begin(), deleteLocators.end()));
            Y_ABORT_UNLESS(std::adjacent_find(deleteLocators.begin(), deleteLocators.end()) == deleteLocators.end());

            for (const TBlobDeleteLocator& deleteLocator : deleteLocators) {
                IHLOG_DEBUG(ctx, "LogBlobDeletes ChunkIdx# %" PRIu32 " ChunkSerNum# %s"
                        " Id# %016" PRIx64 " IndexInsideChunk# %" PRIu32 " SizeInBlocks# %" PRIu32 " Lsn# %" PRIu64
                        " Owner# %d SeqNo# %" PRIu64, deleteLocator.ChunkIdx, deleteLocator.ChunkSerNum.ToString().data(),
                        deleteLocator.Id, deleteLocator.IndexInsideChunk, deleteLocator.SizeInBlocks, Lsn, owner,
                        seqNo);
            }

            // format blob deletes record
            TDeleteRecordMerger::TBlobDeletes record{owner, seqNo, std::move(deleteLocators)};

            // ensure that user has provided us a callback
            Y_ABORT_UNLESS(callback);
            TVector<std::unique_ptr<IEventCallback>> callbacks;
            callbacks.push_back(std::move(callback));

            // create an item in the delete queue
            ProcessDeleteQueueItem(TDeleteQueueItem{
                    std::move(record),    // Content
                    std::move(callbacks), // Callbacks
                    false,                // Entrypoint
                    Lsn++,                // Lsn
                    false,                // Virtual
                    false,                // FailureExpected
                }, ctx);

            // create entrypoint if needed
            if (++ProcessedDeletesWithoutEntrypoint == 100) { // FIXME: correct number
                LogDeletesEntrypoint(ctx);
            }
        }

        void TLogger::LogVirtualBlobDeletes(TVector<TBlobDeleteLocator>&& deleteLocators, const TActorContext& ctx) {
            // check that locators are sorted and do not repeat
            Y_ABORT_UNLESS(std::is_sorted(deleteLocators.begin(), deleteLocators.end()));
            Y_ABORT_UNLESS(std::adjacent_find(deleteLocators.begin(), deleteLocators.end()) == deleteLocators.end());

            for (const TBlobDeleteLocator& deleteLocator : deleteLocators) {
                IHLOG_DEBUG(ctx, "LogVirtualBlobDeletes ChunkIdx# %" PRIu32 " ChunkSerNum# %s"
                        " Id# %016" PRIx64 " IndexInsideChunk# %" PRIu32 " SizeInBlocks# %" PRIu32 " Lsn# %" PRIu64,
                        deleteLocator.ChunkIdx, deleteLocator.ChunkSerNum.ToString().data(), deleteLocator.Id,
                        deleteLocator.IndexInsideChunk, deleteLocator.SizeInBlocks, Lsn);
            }

            DeleteQueue.push_back(TDeleteQueueItem{
                    TDeleteRecordMerger::TBlobDeletes{0, 0, std::move(deleteLocators)}, // Content
                    {},                                                                 // Callbacks
                    false,                                                              // Entrypoint
                    Lsn,                                                                // Lsn
                    true,                                                               // Virtual
                    false,                                                              // FailureExpected
                });

            ProcessDeleteQueueVirtualItems(ctx);
        }

        void TLogger::SetInitialDeletesState(const NKikimrVDiskData::TIncrHugeDelete& record) {
            // just add this record to merger
            ConfirmedDeletesMerger(record);
        }

        void TLogger::LogDeletesEntrypoint(const TActorContext& ctx) {
            // combine all in-flight records with current confirmed state (or find last entrypoint and merge all other
            // items after it)
            TDeleteRecordMerger merger;
            auto it = DeleteQueue.end();
            while (it != DeleteQueue.begin()) {
                --it;
                if (it->Entrypoint) {
                    break;
                }
            }
            if (it == DeleteQueue.end() || !it->Entrypoint) {
                merger = ConfirmedDeletesMerger;
            }
            for (; it != DeleteQueue.end(); ++it) {
                std::visit([&merger](const auto& content) { merger(content); }, it->Content);
            }

            // create new log queue item
            ProcessDeleteQueueItem(TDeleteQueueItem{
                    merger.GetCurrentState(), // Content
                    {},                       // Callbacks
                    true,                     // Entrypoint
                    Lsn++,                    // Lsn
                    false,                    // Virtual
                    false,                    // FailureExpected
                }, ctx);

            // clear number of processed deletes without entrypoint
            ProcessedDeletesWithoutEntrypoint = 0;
        }

        void TLogger::ProcessDeleteQueueItem(TDeleteQueueItem&& newItem, const TActorContext& ctx) {
            // insert this item into queue and get a reference
            DeleteQueue.emplace_back(std::move(newItem));
            TDeleteQueueItem& item = DeleteQueue.back();

            IHLOG_DEBUG(ctx, "ProcessDeleteQueueItem Lsn# %" PRIu64 " Entrypoint# %s"
                    " Virtual# %s", item.Lsn, item.Entrypoint ? "true" : "false", item.Virtual ? "true" : "false");

            if (item.Virtual) {
                ProcessDeleteQueueVirtualItems(ctx);
                return;
            }

            // create callback that will be invoked upon completion of log
            auto callback = [this, p = &item](NKikimrProto::EReplyStatus status, IEventBase *msg, const TActorContext& ctx) {
                IHLOG_DEBUG(ctx, "ProcessDeleteQueueItem Lsn# %" PRIu64 " Status# %s",
                        p->Lsn, NKikimrProto::EReplyStatus_Name(status).data());
                ApplyLogDeleteItem(*p, status, msg, ctx);
            };

            // serialize it into string, depending on its type
            TRcBuf data = TRcBuf(std::visit([](const auto& content) { return TDeleteRecordMerger::Serialize(content); }, item.Content));

            // format commit record
            NPDisk::TCommitRecord commit;
            commit.IsStartingPoint = item.Entrypoint;
            commit.FirstLsnToKeep = GetFirstLsnToKeep(item.Entrypoint ? EEntrypointType::Deletes : EEntrypointType::None);

            // send record to logger
            TLsnSeg seg(item.Lsn, item.Lsn);
            ctx.Send(Keeper.State.Settings.PDiskActorId, new NPDisk::TEvLog(Keeper.State.PDiskParams->Owner,
                    Keeper.State.PDiskParams->OwnerRound, TLogSignature::SignatureIncrHugeDeletes, commit, data,
                    seg, Keeper.RegisterYardCallback(MakeCallback(std::move(callback)))));
        }

        void TLogger::ApplyLogDeleteItem(TDeleteQueueItem& item, NKikimrProto::EReplyStatus status, IEventBase *msg,
                const TActorContext& ctx) {
            // ensure FIFO order of records processing
            Y_ABORT_UNLESS(DeleteQueue && &item == &DeleteQueue.front());

            // check if this item was virtual
            bool v = item.Virtual;

            if (status == NKikimrProto::OK) {
                // ensure that failure is not expected for this item (if it is not virtual)
                Y_ABORT_UNLESS(!item.FailureExpected || v);
                // if this was an entrypoint, reset merger, update LSN
                if (item.Entrypoint) {
                    Y_ABORT_UNLESS(item.Lsn > DeletesEntrypointLsn);
                    DeletesEntrypointLsn = item.Lsn;
                    ConfirmedDeletesMerger = TDeleteRecordMerger();
                }
                // update confirmed state
                IHLOG_DEBUG(ctx, "ApplyLogDeleteItem Entrypoint# %s Lsn# %" PRIu64
                        " Virtual# %s", item.Entrypoint ? "true" : "false", item.Lsn, item.Virtual ? "true" : "false");
                std::visit([this](const auto& content) { ConfirmedDeletesMerger(content); }, item.Content);
            } else {
                // set failure expected flag for all pending items -- they are already in flight and should also fail
                for (TDeleteQueueItem& item : DeleteQueue) {
                    item.FailureExpected = true;
                }
            }

            // invoke all pending callbacks; they must be non-null
            for (const auto& callback : item.Callbacks) {
                callback->Apply(status, msg, ctx);
            }

            // remove item from queue
            DeleteQueue.pop_front();

            // process virtual items if it was the real one
            if (!v) {
                ProcessDeleteQueueVirtualItems(ctx);
            }
        }

        void TLogger::ProcessDeleteQueueVirtualItems(const TActorContext& ctx) {
            while (DeleteQueue && DeleteQueue.front().Virtual) {
                TDeleteQueueItem& item = DeleteQueue.front();
                ApplyLogDeleteItem(item, NKikimrProto::OK, nullptr, ctx);
            }
        }

        ui64 TLogger::GetFirstLsnToKeep(EEntrypointType ep) const {
            ui64 lsn = Max<ui64>();
            if (ep != EEntrypointType::Chunks && ChunksEntrypointLsn < lsn) {
                lsn = ChunksEntrypointLsn;
            }
            if (ep != EEntrypointType::Deletes && DeletesEntrypointLsn < lsn) {
                lsn = DeletesEntrypointLsn;
            }
            return lsn;
        }

        void TLogger::HandleCutLog(NPDisk::TEvCutLog& msg, const TActorContext& ctx) {
            if (ChunksEntrypointLsn < msg.FreeUpToLsn) {
                LogChunksEntrypoint(ctx);
            }
            if (DeletesEntrypointLsn < msg.FreeUpToLsn) {
                LogDeletesEntrypoint(ctx);
            }
        }

    } // NIncrHuge
} // NKikimr

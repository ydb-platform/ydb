#pragma once

#include <ydb/core/blobstorage/vdisk/hullop/defs.h>

#include <ydb/core/blobstorage/vdisk/huge/blobstorage_hullhuge.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <ydb/core/base/blobstorage.h>

#include <util/generic/queue.h>

// Delayed huge blob deletion mechanism is needed to prevent races between huge blob reads and compactions when compacted
// items are being read at the moment of deletion. To ensure this we have LevelIndex shared state that holds actual
// list of taken snapshots stored as a map of LSN (of a snapshot) pointing to counter that covers all snapshots that were
// taken with this LSN:
//
//                             +--------------------------------------------------------------------+
//                             |                                                                    V
// compact                   snap1            snap2                 compact           snap3    release_snap1
//    |                        |                |                      |                |           |
// -----------------------------------------------------------------------------------------------------------> LSN axis
//
// Each snapshot, when taken, contains data with actual index information. This means that we should hold all deletes
// with "deletion LSN" > "LSN of the snapshot" until this snapshot is released. Also, there is no difference between
// snapshots with different LSNs taken between the same two compactions, so we use LSN of the last compaction as the key
// to store snapshots. This key is referred as 'cookie' below. Cookie is increased only when compaction is going to log
// new index and this index contains at least one of removed huge blobs.

namespace NKikimr {

    struct TEvHullReleaseSnapshot : public TEventLocal<TEvHullReleaseSnapshot, TEvBlobStorage::EvHullReleaseSnapshot> {
        const ui64 Cookie;

        TEvHullReleaseSnapshot(ui64 cookie)
            : Cookie(cookie)
        {}
    };

    // LevelIndex-wide state of taken snapshots; contains shared data between corresponding actor and LevelIndex; when
    // snapshot is taken, it is stored here; when it is released, a message is sent to deletion actor and the action is
    // taken
    class TDelayedCompactionDeleterInfo : public TThrRefBase {
        // map <LastDeletionLsn> -> <number of snapshots that were taken during the time LastDeletionLsn was equal
        // to key>; when snapshot counter reaches zero, the key is deleted from map
        TMap<ui64, ui32> CurrentSnapshots;

        // last deletion LSN is set every time to LSN of log record containing FreeHugeBlobs vector; it is used as key
        // to CurrentSnapshots map; every shapshot taken when LastDeletionLsn has the specific value must be freed before
        // further deletions (with their respective DeletionLsn > LastDeletionLsn when snapshot was taken); it is changed
        // stepwise no prevent unnecessary allocations in CurrentSnaphots
        ui64 LastDeletionLsn = 0;

        // delayed huge blob deleter actor id
        TActorId ActorId;

        // a queue of removed huge blobs per compaction
        struct TReleaseQueueItem {
            ui64 RecordLsn;
            TDiskPartVec RemovedHugeBlobs;
            TVector<TChunkIdx> ChunksToForget;
            TLogSignature Signature;
            ui64 WId;

            TReleaseQueueItem(ui64 recordLsn, TDiskPartVec&& removedHugeBlobs, TVector<TChunkIdx> chunksToForget,
                    TLogSignature signature, ui64 wId)
                : RecordLsn(recordLsn)
                , RemovedHugeBlobs(std::move(removedHugeBlobs))
                , ChunksToForget(std::move(chunksToForget))
                , Signature(signature)
                , WId(wId)
            {}
        };
        TDeque<TReleaseQueueItem> ReleaseQueue;

    public:
        void SetActorId(const TActorId& actorId) {
            Y_ABORT_UNLESS(!ActorId);
            ActorId = actorId;
        }

        const TActorId& GetActorId() const {
            return ActorId;
        }

        // this function is called when snapshot is taken; it returns a cookie that needs to be passed in a Release
        // message to delayed huge blob deleter actor
        ui64 TakeSnapshot() {
            ++CurrentSnapshots[LastDeletionLsn];
            return LastDeletionLsn;
        }

        // this function is called every time when compaction is about to commit new entrypoint containing at least
        // one removed huge blob; recordLsn is allocated LSN of this entrypoint
        void Update(ui64 recordLsn, TDiskPartVec&& removedHugeBlobs, TVector<TChunkIdx> chunksToForget, TLogSignature signature,
                ui64 wId, const TActorContext& ctx, const TActorId& hugeKeeperId, const TActorId& skeletonId,
                const TPDiskCtxPtr& pdiskCtx, const TVDiskContextPtr& vctx) {
            Y_ABORT_UNLESS(recordLsn > LastDeletionLsn);
            Y_ABORT_UNLESS(!removedHugeBlobs.Empty());
            LastDeletionLsn = recordLsn;
            LOG_DEBUG_S(ctx, NKikimrServices::BS_HULLCOMP, vctx->VDiskLogPrefix
                << "TDelayedCompactionDeleter: Update recordLsn# " << recordLsn << " removedHugeBlobs.size# "
                << removedHugeBlobs.Size() << " CurrentSnapshots.size# " << CurrentSnapshots.size()
                << " CurrentSnapshots.front# " << (CurrentSnapshots.empty() ? 0 : CurrentSnapshots.begin()->first)
                << " CurrentSnapshots.back# " << (CurrentSnapshots.empty() ? 0 : (--CurrentSnapshots.end())->first));
            ReleaseQueue.emplace_back(recordLsn, std::move(removedHugeBlobs), std::move(chunksToForget), signature, wId);
            ProcessReleaseQueue(ctx, hugeKeeperId, skeletonId, pdiskCtx, vctx);
        }

        void RenderState(IOutputStream &str) {
            HTML(str) {
                DIV_CLASS("panel panel-default") {
                    DIV_CLASS("panel-heading") {
                        str << "Delayed Compaction Deleter";
                    }
                    DIV_CLASS("panel-body") {
                         DIV_CLASS("panel panel-default") {
                            DIV_CLASS("panel-heading") {
                                str << "CurrentSnapshots";
                            }
                            DIV_CLASS("panel-body") {
                                TABLE_CLASS("table table-condensed") {
                                    TABLEHEAD() {
                                        TABLER() {
                                            TABLEH() { str << "LSN"; }
                                            TABLEH() { str << "Counter"; }
                                        }
                                    }
                                    TABLEBODY() {
                                        for (const auto &pair : CurrentSnapshots) {
                                            TABLER() {
                                                TABLED() { str << pair.first; }
                                                TABLED() { str << pair.second; }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        DIV_CLASS("panel panel-default") {
                            DIV_CLASS("panel-heading") {
                                str << "LastDeletionLsn";
                            }
                            DIV_CLASS("panel-body") {
                                STRONG() {
                                    str << LastDeletionLsn;
                                }
                            }
                        }

                        ui32 index = 1;
                        for (const auto &record : ReleaseQueue) {
                            TMap<TChunkIdx, ui32> slots;
                            for (const TDiskPart &part : record.RemovedHugeBlobs) {
                                ++slots[part.ChunkIdx];
                            }
                            for (const TChunkIdx chunkIdx : record.ChunksToForget) {
                                ui32& value = slots[chunkIdx];
                                Y_ABORT_UNLESS(!value);
                                value = Max<ui32>();
                            }

                            DIV_CLASS("panel panel-default") {
                                DIV_CLASS("panel-heading") {
                                    str << "ReleaseQueue[" << index << "]";
                                }
                                DIV_CLASS("panel-body") {
                                    DIV() {
                                        STRONG() {
                                            str << "Lsn# " << record.RecordLsn;
                                        }
                                    }

                                    TABLE_CLASS("table table-condensed") {
                                        TABLEHEAD() {
                                            TABLER() {
                                                TABLEH() { str << "Chunk"; }
                                                TABLEH() { str << "Number of freed slots"; }
                                            }
                                        }
                                        TABLEBODY() {
                                            for (const auto& [chunkIdx, value] : slots) {
                                                TABLER() {
                                                    TABLED() { str << chunkIdx; }
                                                    TABLED() {
                                                        if (value != Max<ui32>()) {
                                                            str << value;
                                                        } else {
                                                            str << "decommitted SST chunk";
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            ++index;
                        }
                    }
                }
            }
        }

    private:
        friend class TDelayedCompactionDeleterActor;

        void ReleaseSnapshot(ui64 cookie, const TActorContext& ctx, const TActorId& hugeKeeperId, const TActorId& skeletonId,
                const TPDiskCtxPtr& pdiskCtx, const TVDiskContextPtr& vctx) {
            auto it = CurrentSnapshots.find(cookie);
            Y_ABORT_UNLESS(it != CurrentSnapshots.end() && it->second > 0);
            if (!--it->second) {
                CurrentSnapshots.erase(it);
                ProcessReleaseQueue(ctx, hugeKeeperId, skeletonId, pdiskCtx, vctx);
            }
        }

        void ProcessReleaseQueue(const TActorContext& ctx, const TActorId& hugeKeeperId, const TActorId& skeletonId,
                const TPDiskCtxPtr& pdiskCtx, const TVDiskContextPtr& vctx) {
            // if we have no snapshots, we can safely process all messages; otherwise we can process only those messages
            // which do not have snapshots created before the point of compaction
            while (ReleaseQueue) {
                TReleaseQueueItem& item = ReleaseQueue.front();
                if (CurrentSnapshots.empty() || (item.RecordLsn <= CurrentSnapshots.begin()->first)) {
                    // matching record -- commit it to huge hull keeper and throw out of the queue
                    ctx.Send(hugeKeeperId, new TEvHullFreeHugeSlots(std::move(item.RemovedHugeBlobs),
                        item.RecordLsn, item.Signature, item.WId));
                    if (item.ChunksToForget) {
                        LOG_DEBUG(ctx, NKikimrServices::BS_VDISK_CHUNKS, VDISKP(vctx->VDiskLogPrefix,
                            "FORGET: PDiskId# %s ChunksToForget# %s", pdiskCtx->PDiskIdString.data(),
                            FormatList(item.ChunksToForget).data()));
                        TActivationContext::Send(new IEventHandle(pdiskCtx->PDiskId, skeletonId, new NPDisk::TEvChunkForget(
                            pdiskCtx->Dsk->Owner, pdiskCtx->Dsk->OwnerRound, std::move(item.ChunksToForget))));
                    }
                    ReleaseQueue.pop_front();
                } else {
                    // we have no matching record
                    break;
                }
            }
        }
    };

    struct TDelayedCompactionDeleterNotifier : public TThrRefBase {
        TActorSystem* const ActorSystem;
        const TIntrusivePtr<TDelayedCompactionDeleterInfo> Info;
        const ui64 Cookie;

        TDelayedCompactionDeleterNotifier(TActorSystem *actorSystem, TIntrusivePtr<TDelayedCompactionDeleterInfo> info)
            : ActorSystem(actorSystem)
            , Info(std::move(info))
            , Cookie(Info->TakeSnapshot())
        {}

        // implemented in blobstorage_hull.h
        ~TDelayedCompactionDeleterNotifier() {
            ActorSystem->Send(new IEventHandle(Info->GetActorId(), TActorId(), new TEvHullReleaseSnapshot(Cookie)));
        }
    };

    IActor *CreateDelayedCompactionDeleterActor(const TActorId hugeKeeperId, const TActorId skeletonId,
        TPDiskCtxPtr pdiskCtx, TVDiskContextPtr vctx, TIntrusivePtr<TDelayedCompactionDeleterInfo> info);

} // NKikimr

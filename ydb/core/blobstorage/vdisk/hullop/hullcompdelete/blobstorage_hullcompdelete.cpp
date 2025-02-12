#include "blobstorage_hullcompdelete.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>

namespace NKikimr {

    void TDelayedCompactionDeleterInfo::ProcessReleaseQueue(const TActorContext& ctx, const TActorId& hugeKeeperId,
            const TActorId& skeletonId, const TPDiskCtxPtr& pdiskCtx, const TVDiskContextPtr& vctx) {
        const ui64 lsn = CurrentSnapshots.empty() ? Max<ui64>() : CurrentSnapshots.begin()->first;

        // if we have no snapshots, we can safely process all messages; otherwise we can process only those messages
        // which do not have snapshots created before the point of compaction
        for (; !ReleaseQueue.empty(); ReleaseQueue.pop_front()) {
            if (TReleaseQueueItem& item = ReleaseQueue.front(); item.RecordLsn <= lsn) {
                // matching record -- commit it to huge hull keeper and throw out of the queue
                if (!item.RemovedHugeBlobs.Empty() || !item.AllocatedHugeBlobs.Empty()) {
                    ctx.Send(hugeKeeperId, new TEvHullFreeHugeSlots(std::move(item.RemovedHugeBlobs),
                        std::move(item.AllocatedHugeBlobs), item.RecordLsn, item.Signature, item.WId));
                }
                if (item.ChunksToForget) {
                    LOG_DEBUG(ctx, NKikimrServices::BS_VDISK_CHUNKS, VDISKP(vctx->VDiskLogPrefix,
                        "FORGET: PDiskId# %s ChunksToForget# %s", pdiskCtx->PDiskIdString.data(),
                        FormatList(item.ChunksToForget).data()));
                    ctx.Send(skeletonId, new TEvNotifyChunksDeleted(item.RecordLsn, item.ChunksToForget));
                    TActivationContext::Send(new IEventHandle(pdiskCtx->PDiskId, skeletonId, new NPDisk::TEvChunkForget(
                        pdiskCtx->Dsk->Owner, pdiskCtx->Dsk->OwnerRound, std::move(item.ChunksToForget))));
                }
            } else {
                // we have no matching record
                break;
            }
        }
    }

    class TDelayedCompactionDeleterActor : public TActor<TDelayedCompactionDeleterActor> {
        // pointer to database general data; actually we need only HugeKeeperID from that data
        const TActorId HugeKeeperId;
        const TActorId SkeletonId;
        const TPDiskCtxPtr PDiskCtx;
        const TVDiskContextPtr VCtx;

        // pointer to shared deleter state, it is primarily created in TLevelIndex
        const TIntrusivePtr<TDelayedCompactionDeleterInfo> Info;

    public:
        static constexpr auto ActorActivityType() {
            return NKikimrServices::TActivity::BS_DELAYED_HUGE_BLOB_DELETER;
        }

        TDelayedCompactionDeleterActor(const TActorId hugeKeeperId, const TActorId skeletonId, TPDiskCtxPtr pdiskCtx,
                TVDiskContextPtr vctx, TIntrusivePtr<TDelayedCompactionDeleterInfo> info)
            : TActor(&TDelayedCompactionDeleterActor::StateFunc)
            , HugeKeeperId(hugeKeeperId)
            , SkeletonId(skeletonId)
            , PDiskCtx(pdiskCtx)
            , VCtx(std::move(vctx))
            , Info(std::move(info))
        {}

        void Handle(TEvHullReleaseSnapshot::TPtr& ev, const TActorContext& ctx) {
            Info->ReleaseSnapshot(ev->Get()->Cookie, ctx, HugeKeeperId, SkeletonId, PDiskCtx, VCtx);
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvHullReleaseSnapshot, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
        )
    };

    IActor *CreateDelayedCompactionDeleterActor(const TActorId hugeKeeperId, const TActorId skeletonId,
            TPDiskCtxPtr pdiskCtx, TVDiskContextPtr vctx, TIntrusivePtr<TDelayedCompactionDeleterInfo> info) {
        return new TDelayedCompactionDeleterActor(hugeKeeperId, skeletonId, std::move(pdiskCtx), std::move(vctx),
            std::move(info));
    }

} // NKikimr

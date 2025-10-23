#include "defrag_rewriter.h"
#include <ydb/core/blobstorage/vdisk/scrub/restore_corrupted_blob_actor.h>
#include <ydb/core/blobstorage/vdisk/skeleton/blobstorage_takedbsnap.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TDefragRewriter
    ////////////////////////////////////////////////////////////////////////////
    class TDefragRewriter : public TActorBootstrapped<TDefragRewriter>
    {
        friend class TActorBootstrapped<TDefragRewriter>;

        std::shared_ptr<TDefragCtx> DCtx;
        const TVDiskID SelfVDiskId;
        const TActorId NotifyId;
        // we can rewrite data while we are holding snapshot for data being read
        std::optional<THullDsSnap> FullSnap;
        std::vector<TDefragRecord> Recs;
        size_t RecToReadIdx = 0;
        size_t RewrittenRecsCounter = 0;
        size_t RewrittenBytes = 0;

        struct TCheckLocationMerger {
            TDefragRecord& Rec;
            TBlobStorageGroupType GType;
            bool Found = false;

            TCheckLocationMerger(TDefragRecord& rec, TBlobStorageGroupType gtype)
                : Rec(rec)
                , GType(gtype)
            {}

            void AddFromFresh(const TMemRecLogoBlob& memRec, const TRope*, const TKeyLogoBlob&, ui64) {
                Process(memRec, nullptr);
            }

            void AddFromSegment(const TMemRecLogoBlob& memRec, const TDiskPart *outbound, const TKeyLogoBlob&, ui64, const void*) {
                Process(memRec, outbound);
            }

            static constexpr bool HaveToMergeData() { return false; }

            void Process(const TMemRecLogoBlob& memRec, const TDiskPart *outbound) {
                TDiskDataExtractor extr;
                if (memRec.GetType() == TBlobType::HugeBlob || memRec.GetType() == TBlobType::ManyHugeBlobs) {
                    memRec.GetDiskData(&extr, outbound);
                    const NMatrix::TVectorType local = memRec.GetIngress().LocalParts(GType);
                    ui8 partIdx = local.FirstPosition();
                    for (const TDiskPart *p = extr.Begin; p != extr.End; ++p, partIdx = local.NextPosition(partIdx)) {
                        if (*p == Rec.OldDiskPart && partIdx + 1 == Rec.LogoBlobId.PartId()) {
                            Found = true;
                        }
                    }
                }
            }
        };

        void Bootstrap(const TActorContext &ctx) {
            SendNextRead(ctx);
            Become(&TThis::StateFunc);
        }

        void SendNextRead(const TActorContext &ctx) {
            if (RecToReadIdx < Recs.size()) {
                ctx.Send(DCtx->SkeletonId, new TEvTakeHullSnapshot(false));
            } else {
                ctx.Send(NotifyId, new TEvDefragRewritten(RewrittenRecsCounter, RewrittenBytes));
                Die(ctx);
            }
        }

        void Handle(TEvTakeHullSnapshotResult::TPtr ev, const TActorContext& ctx) {
            FullSnap.emplace(std::move(ev->Get()->Snap));
            FullSnap->BlocksSnap.Destroy();
            FullSnap->BarriersSnap.Destroy();

            TLogoBlobsSnapshot::TForwardIterator iter(FullSnap->HullCtx, &FullSnap->LogoBlobsSnap);
            auto& rec = Recs[RecToReadIdx];
            const TLogoBlobID id = rec.LogoBlobId;
            iter.Seek(id.FullID());
            if (iter.Valid() && iter.GetCurKey().LogoBlobID() == id.FullID()) {
                TCheckLocationMerger merger(rec, DCtx->VCtx->Top->GType);
                iter.PutToMerger(&merger);
                if (merger.Found) {
                    const TDiskPart& p = rec.OldDiskPart;
                    auto msg = std::make_unique<NPDisk::TEvChunkRead>(DCtx->PDiskCtx->Dsk->Owner,
                        DCtx->PDiskCtx->Dsk->OwnerRound, p.ChunkIdx, p.Offset, p.Size, NPriRead::HullComp, nullptr);
                    DCtx->VCtx->CountDefragCost(*msg);
                    ctx.Send(DCtx->PDiskCtx->PDiskId, msg.release());
                    DCtx->DefragMonGroup.DefragBytesRewritten() += p.Size;
                    RewrittenBytes += p.Size;
                    return;
                }
            }

            ++RecToReadIdx;
            SendNextRead(ctx);
        }

        void Handle(NPDisk::TEvChunkReadResult::TPtr ev, const TActorContext& ctx) {
            FullSnap.reset();

            auto *msg = ev->Get();
            const TDefragRecord &rec = Recs[RecToReadIdx++];

            if (msg->Status == NKikimrProto::CORRUPTED || (msg->Status == NKikimrProto::OK && !msg->Data.IsReadable())) {
                LOG_WARN_S(ctx, NKikimrServices::BS_VDISK_DEFRAG,
                        "Defrag skipping corrupted blob #" << rec.LogoBlobId << " on " << rec.OldDiskPart.ToString());
                const TBlobStorageGroupType gtype = DCtx->VCtx->Top->GType;
                Send(DCtx->SkeletonId,
                     new TEvRestoreCorruptedBlob(
                        ctx.Now() + TDuration::Minutes(2),
                        {TEvRestoreCorruptedBlob::TItem(
                            rec.LogoBlobId.FullID(),
                            NMatrix::TVectorType::MakeOneHot(rec.LogoBlobId.PartId() - 1, gtype.TotalPartCount()),
                            gtype,
                            rec.OldDiskPart)},
                        true, false));

                SendNextRead(ctx);
                return;
            }
            CHECK_PDISK_RESPONSE_READABLE(DCtx->VCtx, ev, ctx);

            const auto &gtype = DCtx->VCtx->Top->GType;
            ui8 partId = rec.LogoBlobId.PartId();
            Y_VERIFY_S(partId, DCtx->VCtx->VDiskLogPrefix);

            TRcBuf data = msg->Data.ToString();
            ui32 offset = 0;
            const ui32 partSize = gtype.PartSize(rec.LogoBlobId);
            const EBlobHeaderMode blobHeaderMode = TDiskBlob::DeriveBlobHeaderMode(partSize, data.size(), &offset);

            switch (blobHeaderMode) {
                case EBlobHeaderMode::OLD_HEADER: {
                    const char *header = data.data();
                    ui32 fullDataSize;
                    memcpy(&fullDataSize, header, sizeof(fullDataSize));
                    header += sizeof(fullDataSize);
                    Y_VERIFY_S(fullDataSize == rec.LogoBlobId.BlobSize(), DCtx->VCtx->VDiskLogPrefix);
                    Y_VERIFY_S(NMatrix::TVectorType::MakeOneHot(partId - 1, gtype.TotalPartCount()).Raw() == static_cast<ui8>(*header),
                        DCtx->VCtx->VDiskLogPrefix);
                    break;
                }

                case EBlobHeaderMode::NO_HEADER:
                    break;

                case EBlobHeaderMode::XXH3_64BIT_HEADER:
                    Y_ABORT_UNLESS(!offset);
                    Y_ABORT_UNLESS(TDiskBlob::ValidateChecksum(data));
                    break;
            }

            TRope rope(std::move(data));
            if (offset) {
                rope.EraseFront(offset);
            }
            if (partSize < rope.size()) {
                rope.EraseBack(rope.size() - partSize);
            }
            Y_VERIFY_S(rope.size() == gtype.PartSize(rec.LogoBlobId), DCtx->VCtx->VDiskLogPrefix);

            LOG_DEBUG_S(ctx, NKikimrServices::BS_VDISK_DEFRAG, DCtx->VCtx->VDiskLogPrefix << "rewriting BlobId# "
                << rec.LogoBlobId << " from Location# " << rec.OldDiskPart);

            auto msgSize = rope.size();
            auto writeEvent = std::make_unique<TEvBlobStorage::TEvVPut>(rec.LogoBlobId, std::move(rope),
                SelfVDiskId, true, nullptr, TInstant::Max(), NKikimrBlobStorage::EPutHandleClass::AsyncBlob,
                DCtx->VCfg->BlobHeaderMode == EBlobHeaderMode::XXH3_64BIT_HEADER);
            writeEvent->RewriteBlob = true;
            TEventsQuoter::QuoteMessage(DCtx->Throttler, std::make_unique<IEventHandle>(DCtx->SkeletonId, SelfId(), writeEvent.release()),
                msgSize, DCtx->VCfg->DefragThrottlerBytesRate);
        }

        void Handle(TEvBlobStorage::TEvVPutResult::TPtr& ev, const TActorContext& ctx) {
            // this message is received when huge blob is written by Skeleton
            // FIXME: Handle NotOK, in case of RACE just cancel the job

            if (auto& record = ev->Get()->Record; record.GetStatus() != NKikimrProto::OK) {
                LOG_WARN_S(ctx, NKikimrServices::BS_VDISK_DEFRAG, DCtx->VCtx->VDiskLogPrefix << "rewrite failed BlobId# "
                    << LogoBlobIDFromLogoBlobID(record.GetBlobID()) << " Record# " << SingleLineProto(record));
            } else {
                LOG_DEBUG_S(ctx, NKikimrServices::BS_VDISK_DEFRAG, DCtx->VCtx->VDiskLogPrefix << "rewritten BlobId# "
                    << LogoBlobIDFromLogoBlobID(record.GetBlobID()) << " to Location# " << ev->Get()->WrittenLocation);

                ++RewrittenRecsCounter;
            }

            SendNextRead(ctx);
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            HFunc(TEvTakeHullSnapshotResult, Handle)
            HFunc(NPDisk::TEvChunkReadResult, Handle)
            HFunc(TEvBlobStorage::TEvVPutResult, Handle)
        )

        PDISK_TERMINATE_STATE_FUNC_DEF;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_DEFRAG_REWRITER;
        }

        TDefragRewriter(
                const std::shared_ptr<TDefragCtx> &dCtx,
                const TVDiskID &selfVDiskId,
                const TActorId &notifyId,
                std::vector<TDefragRecord> &&recs)
            : TActorBootstrapped<TDefragRewriter>()
            , DCtx(dCtx)
            , SelfVDiskId(selfVDiskId)
            , NotifyId(notifyId)
            , Recs(std::move(recs))
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // VDISK DEFRAG REWRITER
    // Rewrites selected huge blobs to free up some Huge Heap chunks
    ////////////////////////////////////////////////////////////////////////////
    IActor* CreateDefragRewriter(
            const std::shared_ptr<TDefragCtx> &dCtx,
            const TVDiskID &selfVDiskId,
            const TActorId &notifyId,
            std::vector<TDefragRecord> &&recs) {
        return new TDefragRewriter(dCtx, selfVDiskId, notifyId, std::move(recs));
    }

    bool IsDefragRewriter(const IActor* actor) {
        return dynamic_cast<const TDefragRewriter*>(actor) != nullptr;
    }

} // NKikimr

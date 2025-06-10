#include "blobstorage_status.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_response.h>
#include <ydb/core/blobstorage/vdisk/skeleton/blobstorage_skeletonerr.h>

using namespace NKikimrServices;

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TStatusRequestHandler
    ////////////////////////////////////////////////////////////////////////////
    class TStatusRequestHandler : public TActorBootstrapped<TStatusRequestHandler> {
        TIntrusivePtr<TVDiskContext> VCtx;
        const TActorId SkeletonId;
        const TActorId SyncerId;
        const TActorId SyncLogId;
        std::shared_ptr<NMonGroup::TVDiskIFaceGroup> IFaceMonGroup;
        const TVDiskID SelfVDiskId;
        const ui64 IncarnationGuid;
        const TIntrusivePtr<TBlobStorageGroupInfo> GroupInfo;
        TEvBlobStorage::TEvVStatus::TPtr Ev;
        const TActorId NotifyId;
        const TInstant Now;
        const bool ReplDone;
        const bool IsReadOnly;
        unsigned Counter;
        std::unique_ptr<TEvBlobStorage::TEvVStatusResult> Result;

        friend class TActorBootstrapped<TStatusRequestHandler>;

        void Bootstrap(const TActorContext &ctx) {
            // check request
            const NKikimrBlobStorage::TEvVStatus &record = Ev->Get()->Record;
            if (!SelfVDiskId.SameDisk(record.GetVDiskID())) {
                Result = std::make_unique<TEvBlobStorage::TEvVStatusResult>(NKikimrProto::RACE, SelfVDiskId, false,
                    false, false, IncarnationGuid);
                SetRacingGroupInfo(record, Result->Record, GroupInfo);
                LOG_DEBUG(ctx, BS_VDISK_OTHER, VDISKP(VCtx->VDiskLogPrefix, "TEvVStatusResult Request# {%s} Response# {%s}",
                    SingleLineProto(record).data(), SingleLineProto(Result->Record).data()));
                SendVDiskResponse(ctx, Ev->Sender, Result.release(), Ev->Cookie, Ev->GetChannel(), VCtx, {});
                Die(ctx);
                return;
            }

            Result = std::make_unique<TEvBlobStorage::TEvVStatusResult>(
                NKikimrProto::OK, SelfVDiskId, true, ReplDone, IsReadOnly, IncarnationGuid);

            const auto& oos = VCtx->GetOutOfSpaceState();
            Result->Record.SetStatusFlags(oos.GetGlobalStatusFlags().Flags);
            Result->Record.SetApproximateFreeSpaceShare(oos.GetFreeSpaceShare());

            // send requests to all actors
            SendLocalStatusRequest(ctx, SkeletonId);
            SendLocalStatusRequest(ctx, SyncerId);
            SendLocalStatusRequest(ctx, SyncLogId);
            Become(&TThis::StateFunc);
        }

        void SendLocalStatusRequest(const TActorContext &ctx, const TActorId &actor) {
            if (actor != TActorId()) {
                ctx.Send(actor, new TEvLocalStatus());
                Counter++;
            }
        }

        void Handle(TEvLocalStatusResult::TPtr &ev, const TActorContext &ctx) {
            Y_DEBUG_ABORT_UNLESS(Counter > 0);
            --Counter;

            Result->Record.MergeFrom(ev->Get()->Record);

            if (Counter == 0) {
                ctx.Send(NotifyId, new TEvents::TEvActorDied());
                LOG_DEBUG(ctx, BS_VDISK_GET,
                    VDISKP(VCtx->VDiskLogPrefix, "TEvVStatusResult"));
                SendVDiskResponse(ctx, Ev->Sender, Result.release(), Ev->Cookie, Ev->GetChannel(), VCtx, {});
                Die(ctx);
            }
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvLocalStatusResult, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_STATUS_REQUEST_HANDLER;
        }

        TStatusRequestHandler(
                const TIntrusivePtr<TVDiskContext> &vctx,
                const TActorId &skeletonId,
                const TActorId &syncerId,
                const TActorId &syncLogId,
                const std::shared_ptr<NMonGroup::TVDiskIFaceGroup> &ifaceMonGroup,
                const TVDiskID selfVDiskId,
                const ui64 incarnationGuid,
                const TIntrusivePtr<TBlobStorageGroupInfo>& groupInfo,
                TEvBlobStorage::TEvVStatus::TPtr &ev,
                const TActorId &notifyId,
                const TInstant &now,
                bool replDone,
                bool isReadOnly)
            : TActorBootstrapped<TStatusRequestHandler>()
            , VCtx(vctx)
            , SkeletonId(skeletonId)
            , SyncerId(syncerId)
            , SyncLogId(syncLogId)
            , IFaceMonGroup(ifaceMonGroup)
            , SelfVDiskId(selfVDiskId)
            , IncarnationGuid(incarnationGuid)
            , GroupInfo(groupInfo)
            , Ev(ev)
            , NotifyId(notifyId)
            , Now(now)
            , ReplDone(replDone)
            , IsReadOnly(isReadOnly)
            , Counter(0)
        {}
    };

    IActor *CreateStatusRequestHandler(
            const TIntrusivePtr<TVDiskContext> &vctx,
            const TActorId &skeletonId,
            const TActorId &syncerId,
            const TActorId &syncLogId,
            const std::shared_ptr<NMonGroup::TVDiskIFaceGroup> &ifaceMonGroup,
            const TVDiskID selfVDiskId,
            const ui64 incarnationGuid,
            const TIntrusivePtr<TBlobStorageGroupInfo>& groupInfo,
            TEvBlobStorage::TEvVStatus::TPtr &ev,
            const TActorId &notifyId,
            const TInstant &now,
            bool replDone,
            bool isReadOnly) {
        return new TStatusRequestHandler(vctx, skeletonId, syncerId, syncLogId, ifaceMonGroup, selfVDiskId,
            incarnationGuid, groupInfo, ev, notifyId, now, replDone, isReadOnly);
    }

} // NKikimr

#include "skeleton_vpatch_orchestrator.h"

#include "defs.h"
#include "blobstorage_skeletonerr.h"
#include "skeleton_vpatch_actor.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_queues.h>


namespace {
    using namespace NKikimr;

    class TSkeletonVPatchOrchestrator : public TActorBootstrapped<TSkeletonVPatchOrchestrator> {
    public:
        TSkeletonVPatchOrchestrator(
                const TIntrusivePtr<TBlobStorageGroupInfo> &gInfo,
                TActorIDPtr skeletonFrontIDPtr,
                std::shared_ptr<NMonGroup::TVDiskIFaceGroup> iFaceMonGroup,
                ui64 incarnationGuid,
                const TIntrusivePtr<TVDiskContext>& vCtx,
                TVDiskID selfVDiskId)
            : SelfVDiskId(selfVDiskId)
            , GInfo(gInfo)
            , SkeletonFrontIDPtr(skeletonFrontIDPtr)
            , IFaceMonGroup(iFaceMonGroup)
            , IncarnationGuid(incarnationGuid)
            , VCtx(vCtx)
        {}

        void Bootstrap(TActorId parentId) {
            SkeletonActorId = parentId; 
            Y_UNUSED(IncarnationGuid);
        }

        void UpdateVPatchCtx() {
            if (!VPatchCtx) {
                TIntrusivePtr<::NMonitoring::TDynamicCounters> patchGroup = VCtx->VDiskCounters->GetSubgroup("subsystem", "patch");
                VPatchCtx = MakeIntrusive<TVPatchCtx>();
                NBackpressure::TQueueClientId patchQueueClientId(NBackpressure::EQueueClientType::VPatch,
                            VCtx->Top->GetOrderNumber(VCtx->ShortSelfVDisk));
                CreateQueuesForVDisks(VPatchCtx->AsyncBlobQueues, SelfId(), GInfo, VCtx, GInfo->GetVDisks(), patchGroup,
                        patchQueueClientId, NKikimrBlobStorage::EVDiskQueueId::PutAsyncBlob,
                        "PeerVPatch",  TInterconnectChannels::IC_BLOBSTORAGE_ASYNC_DATA);
            }
        }

        template <class TOrigEv>
        void SendReply(std::unique_ptr<IEventBase> result, TOrigEv &orig) {
            SendVDiskResponse(TActivationContext::AsActorContext(), orig->Sender, result.release(), orig->Cookie, VCtx);
        }

        template <typename TEvPtr>
        void ReplyError(NKikimrProto::EReplyStatus status, const TString& errorReason, TEvPtr &ev,
                TInstant now) {
            using namespace NErrBuilder;
            std::unique_ptr<IEventBase> res = ErroneousResult(VCtx, status, errorReason, ev, now,
                    SkeletonFrontIDPtr, SelfVDiskId, IncarnationGuid, GInfo);
            SendReply(std::move(res), ev);
        }

        void Handle(TEvBlobStorage::TEvVPatchStart::TPtr &ev) {
            TInstant now = TActivationContext::Now();

            TLogoBlobID patchedBlobId = LogoBlobIDFromLogoBlobID(ev->Get()->Record.GetPatchedBlobId());

            if (VPatchActors.count(patchedBlobId)) {
                ReplyError(NKikimrProto::ERROR, "The patching request already is running", ev, TAppData::TimeProvider->Now());
                return;
            }

            LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::BS_VDISK_PATCH,
                    VCtx->VDiskLogPrefix << "TEvVPatch: register actor;"
                    << " Event# " << ev->Get()->ToString());
            IFaceMonGroup->PatchStartMsgs()++;
            UpdateVPatchCtx();
            std::unique_ptr<IActor> actor{CreateSkeletonVPatchActor(SelfId(), GInfo->Type, ev, now, SkeletonFrontIDPtr,
                    IFaceMonGroup->PatchFoundPartsMsgsPtr(), IFaceMonGroup->PatchResMsgsPtr(),
                    VCtx->Histograms.GetHistogram(NKikimrBlobStorage::FastRead), VCtx->Histograms.GetHistogram(NKikimrBlobStorage::AsyncBlob),
                    VPatchCtx, VCtx->VDiskLogPrefix, IncarnationGuid, VCtx)};
            TActorId vPatchActor = Register(actor.release());
            VPatchActors.emplace(patchedBlobId, vPatchActor);
        }

        template <typename TEvDiffPtr>
        void HandleVPatchDiffResending(TEvDiffPtr &ev) {
            if constexpr (std::is_same_v<TEvDiffPtr, TEvBlobStorage::TEvVPatchDiff::TPtr>) {
                LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::BS_VDISK_PATCH,
                        VCtx->VDiskLogPrefix << "TEvVPatch: recieve diff;"
                        << " Event# " << ev->Get()->ToString());
                IFaceMonGroup->PatchDiffMsgs()++;
            }
            if constexpr (std::is_same_v<TEvDiffPtr, TEvBlobStorage::TEvVPatchXorDiff::TPtr>) {
                LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::BS_VDISK_PATCH,
                        VCtx->VDiskLogPrefix << "TEvVPatch: recieve xor diff;"
                        << " Event# " << ev->Get()->ToString());
                IFaceMonGroup->PatchXorDiffMsgs()++;
            }
            TLogoBlobID patchedBlobId = LogoBlobIDFromLogoBlobID(ev->Get()->Record.GetPatchedPartBlobId()).FullID();
            auto it = VPatchActors.find(patchedBlobId);
            if (it != VPatchActors.end()) {
                TActivationContext::Send(ev->Forward(it->second));
            } else {
                ReplyError(NKikimrProto::ERROR, "VPatchActor doesn't exist", ev, TAppData::TimeProvider->Now());
            }
        }

        void Handle(TEvVPatchDyingRequest::TPtr &ev) {
            auto it = VPatchActors.find(ev->Get()->PatchedBlobId);
            if (it != VPatchActors.end()) {
                VPatchActors.erase(it);
            }
            Send(ev->Sender, new TEvVPatchDyingConfirm);
        }

        void Handle(TEvProxyQueueState::TPtr &/*ev*/) {
            // TODO(kruall): Make it better
        }

        void Handle(TEvVPatchUpdateSelfVDisk::TPtr &ev) {
            SelfVDiskId = ev->Get()->SelfVDiskId;
        }

        STFUNC(State) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvBlobStorage::TEvVPatchStart, Handle);
                hFunc(TEvBlobStorage::TEvVPatchDiff, HandleVPatchDiffResending);
                hFunc(TEvBlobStorage::TEvVPatchXorDiff, HandleVPatchDiffResending);

                hFunc(TEvVPatchDyingRequest, Handle);
                hFunc(TEvVPatchUpdateSelfVDisk, Handle);

                hFunc(TEvProxyQueueState, Handle);
            }
        }

    private:
        TActorId SkeletonActorId;

        TIntrusivePtr<TVPatchCtx> VPatchCtx;
        THashMap<TLogoBlobID, TActorId> VPatchActors;

        TVDiskID SelfVDiskId;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
        TActorIDPtr SkeletonFrontIDPtr;
        std::shared_ptr<NMonGroup::TVDiskIFaceGroup> IFaceMonGroup;
        ui64 IncarnationGuid;
        TIntrusivePtr<TVDiskContext> VCtx;
    };
}


namespace NKikimr {
    IActor* CreateSkeletonVPatchOrchestratorActor(
            const TIntrusivePtr<TBlobStorageGroupInfo> &gInfo,
            TActorIDPtr skeletonFrontIDPtr,
            std::shared_ptr<NMonGroup::TVDiskIFaceGroup> iFaceMonGroup,
            ui64 incarnationGuid,
            const TIntrusivePtr<TVDiskContext>& vCtx,
            TVDiskID selfVDiskId)
    {
        return new TSkeletonVPatchOrchestrator(gInfo, skeletonFrontIDPtr, iFaceMonGroup, incarnationGuid, vCtx, selfVDiskId);
    }
}

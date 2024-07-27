#include "skeleton_vmovedpatch_actor.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_response.h>


namespace NKikimr {
    namespace NPrivate {

        class TVMovedPatchActor : public TActorBootstrapped<TVMovedPatchActor> {
            friend TActorBootstrapped<TVMovedPatchActor>;

            ui32 OriginalGroupId;
            ui32 PatchedGroupId;
            TLogoBlobID OriginalId;
            TLogoBlobID PatchedId;

            TString Buffer;
            TString ErrorReason;

            ui32 DiffCount = 0;
            std::unique_ptr<TEvBlobStorage::TEvPatch::TDiff[]> Diffs;

            TActorIDPtr SkeletonFrontIDPtr;
            ::NMonitoring::TDynamicCounters::TCounterPtr MovedPatchResMsgsPtr;

            TEvBlobStorage::TEvVMovedPatch::TPtr Event;
            TActorId LeaderId;
            TOutOfSpaceStatus OOSStatus;

            TInstant Deadline = TInstant::Zero();

            NLWTrace::TOrbit Orbit;

            const ui64 IncarnationGuid;

            TVDiskContextPtr VCtx;

        public:
            TVMovedPatchActor(TActorId leaderId, TOutOfSpaceStatus oosStatus, TEvBlobStorage::TEvVMovedPatch::TPtr &ev,
                    TActorIDPtr skeletonFrontIDPtr, ::NMonitoring::TDynamicCounters::TCounterPtr movedPatchResMsgsPtr,
                    ui64 incarnationGuid, const TVDiskContextPtr &vCtx)
                : TActorBootstrapped()
                , SkeletonFrontIDPtr(skeletonFrontIDPtr)
                , MovedPatchResMsgsPtr(movedPatchResMsgsPtr)
                , Event(ev)
                , LeaderId(leaderId)
                , OOSStatus(oosStatus)
                , IncarnationGuid(incarnationGuid)
                , VCtx(vCtx)
            {
                NKikimrBlobStorage::TEvVMovedPatch &record = Event->Get()->Record;
                Y_ABORT_UNLESS(record.HasOriginalGroupId());
                OriginalGroupId = record.GetOriginalGroupId();
                Y_ABORT_UNLESS(record.HasPatchedGroupId());
                PatchedGroupId = record.GetPatchedGroupId();
                Y_ABORT_UNLESS(record.HasOriginalBlobId());
                OriginalId = LogoBlobIDFromLogoBlobID(record.GetOriginalBlobId());
                Y_ABORT_UNLESS(record.HasPatchedBlobId());
                PatchedId = LogoBlobIDFromLogoBlobID(record.GetPatchedBlobId());
                Deadline = TInstant::Seconds(record.GetMsgQoS().HasDeadlineSeconds());
                if (record.HasMsgQoS() && record.GetMsgQoS().HasDeadlineSeconds()) {
                    Deadline = TInstant::Seconds(record.GetMsgQoS().HasDeadlineSeconds());
                }

                DiffCount = record.DiffsSize();
                Diffs.reset(new TEvBlobStorage::TEvPatch::TDiff[DiffCount]);
                for (ui32 idx = 0; idx < DiffCount; ++idx) {
                    const NKikimrBlobStorage::TDiffBlock &diff = record.GetDiffs(idx);
                    Y_ABORT_UNLESS(diff.HasOffset());
                    Diffs[idx].Offset = diff.GetOffset();
                    Y_ABORT_UNLESS(diff.HasBuffer());
                    Diffs[idx].Buffer = TRcBuf(diff.GetBuffer());
                }
            }

        private:
            void SendResponseAndDie(const TActorContext &ctx, NKikimrProto::EReplyStatus status,
                    const TString &errorSubMsg = "")
            {
                NKikimrBlobStorage::TEvVMovedPatch &record = Event->Get()->Record;
                TVDiskID vdisk = VDiskIDFromVDiskID(record.GetVDiskID());

                TMaybe<ui64> cookie;
                if (record.HasCookie()) {
                    cookie = record.GetCookie();
                }

                TInstant now = TAppData::TimeProvider->Now();
                auto vMovedPatchResult = std::make_unique<TEvBlobStorage::TEvVMovedPatchResult>(status, OriginalId,
                        PatchedId, vdisk, cookie, OOSStatus, now, Event->Get()->GetCachedByteSize(), &record,
                        SkeletonFrontIDPtr, MovedPatchResMsgsPtr, nullptr, IncarnationGuid, ErrorReason);
                vMovedPatchResult->Orbit = std::move(Orbit);

                if (status == NKikimrProto::ERROR) {
                    LOG_ERROR_S(ctx, NKikimrServices::BS_VDISK_PATCH, VCtx->VDiskLogPrefix
                            << "TEvVMovedPatch: " << errorSubMsg << ';'
                            << " OriginalBlobId# " << OriginalId
                            << " PatchedBlobId# " << PatchedId
                            << " ErrorReason# " << ErrorReason
                            << " Marker# BSVSP01");
                }
                LOG_DEBUG_S(ctx, NKikimrServices::BS_VDISK_PATCH, VCtx->VDiskLogPrefix
                        << "Send result TEvVMovedPatch: " << errorSubMsg << ';'
                        << " OriginalBlobId# " << OriginalId
                        << " PatchedBlobId# " << PatchedId
                        << " ErrorReason# " << ErrorReason
                        << " Marker# BSVSP01");
                SendVDiskResponse(ctx, Event->Sender, vMovedPatchResult.release(), Event->Cookie);
                PassAway();
            }

            void ApplyDiffs() {
                for (ui32 idx = 0; idx < DiffCount; ++idx) {
                    const TEvBlobStorage::TEvPatch::TDiff &diff = Diffs[idx];
                    memcpy(Buffer.begin() + diff.Offset, diff.Buffer.begin(), diff.Buffer.size());
                }
            }

            void Handle(TEvBlobStorage::TEvGetResult::TPtr &ev, const TActorContext &ctx) {
                LOG_DEBUG_S(ctx, NKikimrServices::BS_VDISK_PATCH, VCtx->VDiskLogPrefix
                        << "Receive Get ub TEvVMovedPatch: "
                        << " OriginalBlobId# " << OriginalId
                        << " PatchedBlobId# " << PatchedId);
                TEvBlobStorage::TEvGetResult *result = ev->Get();
                Orbit = std::move(result->Orbit);

                ui32 patchedIdHash = PatchedId.Hash();

                constexpr auto errorSubMsg = "failed on VGet";
                if (ev->Cookie != patchedIdHash) {
                    ErrorReason = "Couldn't get the original blob; Received TEvGetResult with wrong cookie";
                    SendResponseAndDie(ctx, NKikimrProto::ERROR, errorSubMsg);
                    return;
                } else if (result->ResponseSz > 1) {
                    ErrorReason = "Couldn't get the original blob; Received TEvGetResult with more responses than needed";
                    SendResponseAndDie(ctx, NKikimrProto::ERROR, errorSubMsg);
                    return;
                } else if (result->Status != NKikimrProto::OK || result->ResponseSz != 1 || result->Responses[0].Status != NKikimrProto::OK) {
                    TString getResponseStatus;
                    if (result->ResponseSz == 1) {
                        getResponseStatus = TStringBuilder() << " GetResponseStatus# "
                                << NKikimrProto::EReplyStatus_Name(result->Responses[0].Status);
                    }
                    ErrorReason = TStringBuilder() << "Couldn't get the original blob;"
                            << " GetStatus# " << NKikimrProto::EReplyStatus_Name(result->Status)
                            << getResponseStatus
                            << " GetErrorReason# " << result->ErrorReason;
                    SendResponseAndDie(ctx, NKikimrProto::ERROR, errorSubMsg);
                    return;
                }

                Buffer = result->Responses[0].Buffer.ConvertToString();
                ApplyDiffs();

                // We have chosen UserData as PutHandleClass on purpose.
                // If VMovedPatch and Put were AsyncWrite, it would become a deadlock
                // because the put subrequest may not send and the moved patch request will end by timeout.
                std::unique_ptr<TEvBlobStorage::TEvPut> put = std::make_unique<TEvBlobStorage::TEvPut>(PatchedId, Buffer, Deadline,
                        NKikimrBlobStorage::UserData, TEvBlobStorage::TEvPut::TacticDefault);
                put->Orbit = std::move(Orbit);

                LOG_DEBUG_S(ctx, NKikimrServices::BS_VDISK_PATCH, VCtx->VDiskLogPrefix
                        << "Send Put ub TEvVMovedPatch: "
                        << " OriginalBlobId# " << OriginalId
                        << " PatchedBlobId# " << PatchedId);
                SendToBSProxy(SelfId(), PatchedGroupId, put.release(), OriginalId.Hash());
            }

            void Handle(TEvBlobStorage::TEvPutResult::TPtr &ev, const TActorContext &ctx) {
                TEvBlobStorage::TEvPutResult *result = ev->Get();
                Orbit = std::move(result->Orbit);

                ui32 originalIdHash = OriginalId.Hash();

                LOG_DEBUG_S(ctx, NKikimrServices::BS_VDISK_PATCH, VCtx->VDiskLogPrefix
                        << "Receive Put ub TEvVMovedPatch: "
                        << " OriginalBlobId# " << OriginalId
                        << " PatchedBlobId# " << PatchedId);

                constexpr auto errorSubMsg = "failed on VPut";
                if (ev->Cookie != originalIdHash) {
                    ErrorReason = "Couldn't put the patched blob; Received TEvPutResult with wrong cookie";
                    SendResponseAndDie(ctx, NKikimrProto::ERROR, errorSubMsg);
                    return;
                } else if (result->Status != NKikimrProto::OK) {
                    ErrorReason = TStringBuilder() << "Couldn't put the patched blob;"
                            << " PutStatus# " << NKikimrProto::EReplyStatus_Name(result->Status)
                            << " PutErrorReason# " << result->ErrorReason;
                    SendResponseAndDie(ctx, NKikimrProto::ERROR, errorSubMsg);
                    return;
                }

                SendResponseAndDie(ctx, NKikimrProto::OK);
            }

            void Bootstrap() {
                if (Deadline && Deadline < TActivationContext::Now()) {
                    SendResponseAndDie(TActivationContext::AsActorContext(), NKikimrProto::DEADLINE);
                    return;
                }

                std::unique_ptr<TEvBlobStorage::TEvGet> get = std::make_unique<TEvBlobStorage::TEvGet>(OriginalId, 0,
                        OriginalId.BlobSize(), Deadline, NKikimrBlobStorage::AsyncRead);
                get->Orbit = std::move(Event->Get()->Orbit);

                LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::BS_VDISK_PATCH, VCtx->VDiskLogPrefix
                        << "Send Get ub TEvVMovedPatch: "
                        << " OriginalBlobId# " << OriginalId
                        << " PatchedBlobId# " << PatchedId);

                SendToBSProxy(SelfId(), OriginalGroupId, get.release(), PatchedId.Hash());
                Become(&TThis::StateWait);
            }

            STFUNC(StateWait) {
                switch (ev->GetTypeRewrite()) {
                    HFunc(TEvBlobStorage::TEvGetResult, Handle);
                    HFunc(TEvBlobStorage::TEvPutResult, Handle);
                }
            }
        };

    } // NPrivate

    IActor* CreateSkeletonVMovedPatchActor(TActorId leaderId, TOutOfSpaceStatus oosStatus,
            TEvBlobStorage::TEvVMovedPatch::TPtr &ev, TActorIDPtr skeletonFrontIDPtr,
            ::NMonitoring::TDynamicCounters::TCounterPtr counterPtr, ui64 incarnationGuid,
            const TVDiskContextPtr &vCtx)
    {
        return new NPrivate::TVMovedPatchActor(leaderId, oosStatus, ev, skeletonFrontIDPtr,
                counterPtr, incarnationGuid, vCtx);
    }

} // NKikimr

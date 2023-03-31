#include "skeleton_vpatch_actor.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_response.h>
#include <ydb/core/util/stlog.h>

#include <util/generic/serialized_enum.h>


namespace NKikimr::NPrivate {

    class TSkeletonVPatchActor : public TActorBootstrapped<TSkeletonVPatchActor> {
        friend TActorBootstrapped<TSkeletonVPatchActor>;
        // When the actor is created with VPatchStart(request)
        // Send VGet for finding parts
        // Go to StartState

        // When the actor in StartState
        // Receive VGetResult
        // if it has error then send Error in VPatchFoundParts(response VPatchStart) and die
        // if it doesn't find parts send Ok in VPatchFoundParts(response VPatchStart) without parts and die
        // otherwise it send Ok in VPatchFoundParts(response VPatchStart) and fo to WaitState

        // When the actor in WaitState
        // Receive VPatchDiff(request)
        // if it has a force end flag then send Ok in VPatchResult(response for VPatchDiff) and die
        // Send VGet for pulling part
        // if it has expected xor diffs then part is parity and actor go to ParityState
        // otherwise part is data and actor go to DataState

        // When the actor in DataState
        // Receive VGetResult
        // if it has some troubles send Error in VPatchResult(response for VPatchDiff) and die
        // Send VPatchXorDiffs to vdisk with parity parts in future ignore their responses(VPatchResult)
        // Apply the patch
        // Send VPut with the patched part
        // Receive VPutResult
        // Send VPatchResult(response VPatchDiff) and die

        // When the actor in ParityState
        // Receive VGetResult
        // if it has some troubles send Error in VPatchResult(response for VPatchDiff) and die
        // Receive VPatchXorDiffs from vdisk with data parts, apply them and send responses(VPatchXorDiffs)
        // Send VPut with the patched part
        // Receive VPutResult
        // Send VPatchResult(response VPatchDiff) and die

        struct TXorReceiver {
            TVDiskID VDiskId;
            ui8 PartId;

            TXorReceiver(const TVDiskID &vDiskId, ui8 partId)
                : VDiskId(vDiskId)
                , PartId(partId)
            {
            }
        };

        struct TXorDiffs {
            TVector<TDiff> Diffs;
            ui8 PartId;
            TActorId Sender;
            ui64 Cookie;


            TXorDiffs(TVector<TDiff> &&diffs, ui8 partId, const TActorId &sender, ui64 cookie)
                : Diffs(std::move(diffs))
                , PartId(partId)
                , Sender(sender)
                , Cookie(cookie)
            {
            }
        };

        static constexpr TDuration CommonLiveTime = TDuration::Seconds(57);
        // 60s is timeout in backpressure queue, try to be nearer to it

        TActorId ProxyId;

        TLogoBlobID OriginalBlobId;
        TLogoBlobID PatchedBlobId;
        ui8 OriginalPartId = 0;
        ui8 PatchedPartId = 0;
        TVDiskID VDiskId;

        TInstant Deadline;

        TActorId Sender;
        ui64 Cookie;
        TActorIDPtr SkeletonFrontIDPtr;
        ::NMonitoring::TDynamicCounters::TCounterPtr VPatchFoundPartsMsgsPtr;
        ::NMonitoring::TDynamicCounters::TCounterPtr VPatchResMsgsPtr;
        const TIntrusivePtr<TVPatchCtx> VPatchCtx;
        TString VDiskLogPrefix;

        TActorId LeaderId;

        const ui64 IncarnationGuid;

        TStackVec<ui32, 1> FoundOriginalParts;

        TStackVec<TXorReceiver, 2> XorReceivers;
        TRope Buffer;
        TVector<TDiff> Diffs;
        TVector<TXorDiffs> ReceivedXorDiffs;

        TString ErrorReason;

        std::unique_ptr<TEvBlobStorage::TEvVPatchFoundParts> FoundPartsEvent;
        std::unique_ptr<TEvBlobStorage::TEvVPatchResult> ResultEvent;

        TBlobStorageGroupType GType;

        ui64 ReceivedXorDiffCount = 0;
        ui64 WaitedXorDiffCount = 0;

    public:
        TSkeletonVPatchActor(TActorId leaderId, const TBlobStorageGroupType &gType,
                TEvBlobStorage::TEvVPatchStart::TPtr &ev, TInstant now, TActorIDPtr skeletonFrontIDPtr,
                const ::NMonitoring::TDynamicCounters::TCounterPtr &vPatchFoundPartsMsgsPtr,
                const ::NMonitoring::TDynamicCounters::TCounterPtr &vPatchResMsgsPtr,
                const TIntrusivePtr<TVPatchCtx> &vPatchCtx, const TString &vDiskLogPrefix, ui64 incarnationGuid)
            : TActorBootstrapped()
            , Sender(ev->Sender)
            , Cookie(ev->Cookie)
            , SkeletonFrontIDPtr(skeletonFrontIDPtr)
            , VPatchFoundPartsMsgsPtr(vPatchFoundPartsMsgsPtr)
            , VPatchResMsgsPtr(vPatchResMsgsPtr)
            , VPatchCtx(vPatchCtx)
            , VDiskLogPrefix(vDiskLogPrefix)
            , LeaderId(leaderId)
            , IncarnationGuid(incarnationGuid)
            , GType(gType)
        {
            NKikimrBlobStorage::TEvVPatchStart &record = ev->Get()->Record;
            if (record.HasMsgQoS() && record.GetMsgQoS().HasDeadlineSeconds()) {
                Deadline = TInstant::Seconds(record.GetMsgQoS().HasDeadlineSeconds());
            }
            if (!Deadline) {
                Deadline = now + CommonLiveTime;
            }

            Y_VERIFY(record.HasOriginalBlobId());
            OriginalBlobId = LogoBlobIDFromLogoBlobID(record.GetOriginalBlobId());
            Y_VERIFY(record.HasPatchedBlobId());
            PatchedBlobId = LogoBlobIDFromLogoBlobID(record.GetPatchedBlobId());
            Y_VERIFY(record.HasVDiskID());
            VDiskId = VDiskIDFromVDiskID(record.GetVDiskID());
            Y_VERIFY(record.HasCookie());
            FoundPartsEvent = std::make_unique<TEvBlobStorage::TEvVPatchFoundParts>(
                    NKikimrProto::OK, OriginalBlobId, PatchedBlobId, VDiskId, record.GetCookie(), now, ErrorReason, &record,
                    SkeletonFrontIDPtr, VPatchFoundPartsMsgsPtr, nullptr, IncarnationGuid);
        }

        void Bootstrap() {
            STLOG(PRI_INFO, BS_VDISK_PATCH, BSVSP03,
                    VDiskLogPrefix << " TEvVPatch: bootstrapped;",
                    (OriginalBlobId, OriginalBlobId),
                    (Deadline, Deadline));
            ui32 cookie = 0;
            std::unique_ptr<TEvBlobStorage::TEvVGet> msg = TEvBlobStorage::TEvVGet::CreateRangeIndexQuery(VDiskId, Deadline,
                    NKikimrBlobStorage::EGetHandleClass::FastRead, TEvBlobStorage::TEvVGet::EFlags::None, cookie,
                    OriginalBlobId, TLogoBlobID(OriginalBlobId, TLogoBlobID::MaxPartId),
                    TLogoBlobID::MaxPartId, nullptr);
            Send(LeaderId, msg.release());

            TInstant now = TActivationContext::Now();
            if (Deadline != TInstant::Zero() && Deadline < now) {
                ErrorReason = "DEADLINE";
                SendVPatchFoundParts(NKikimrProto::ERROR);
                NotifySkeletonAboutDying();
                Become(&TThis::ErrorState);
                return;
            }

            Become(&TThis::StartState);

            TDuration liveDuration = Deadline - now;
            if (!Deadline || liveDuration > CommonLiveTime) {
                liveDuration = CommonLiveTime;
            }
            Schedule(liveDuration, new TKikimrEvents::TEvWakeup);
        }

        void SendVPatchFoundParts(NKikimrProto::EReplyStatus status)
        {
            STLOG(PRI_INFO, BS_VDISK_PATCH, BSVSP04,
                    VDiskLogPrefix << " TEvVPatch: sended found parts;",
                    (OriginalBlobId, OriginalBlobId),
                    (FoundParts, FormatList(FoundOriginalParts)),
                    (Status, status));
            FoundPartsEvent->Record.SetErrorReason(ErrorReason);
            for (ui8 part : FoundOriginalParts) {
                FoundPartsEvent->AddPart(part);
            }
            FoundPartsEvent->SetStatus(status);
            SendVDiskResponse(TActivationContext::AsActorContext(), Sender, FoundPartsEvent.release(), Cookie);
        }

        void PullOriginalPart(ui64 pullingPart) {
            STLOG(PRI_INFO, BS_VDISK_PATCH, BSVSP05,
                    VDiskLogPrefix << " TEvVPatch: send vGet for pulling part data;",
                    (OriginalBlobId, OriginalBlobId),
                    (PullingPart, pullingPart));
            ui32 cookie = 0;
            std::unique_ptr<TEvBlobStorage::TEvVGet> msg = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(VDiskId, Deadline,
                    NKikimrBlobStorage::EGetHandleClass::FastRead, TEvBlobStorage::TEvVGet::EFlags::None, cookie,
                    {});
            TLogoBlobID id(OriginalBlobId, pullingPart);
            msg->AddExtremeQuery(id, 0, 0, &pullingPart);
            Send(LeaderId, msg.release());
        }

        void HandleVGetRangeResult(TEvBlobStorage::TEvVGetResult::TPtr &ev) {
            Become(&TThis::WaitState);
            NKikimrBlobStorage::TEvVGetResult &record = ev->Get()->Record;
            Y_VERIFY(record.HasStatus());

            STLOG(PRI_INFO, BS_VDISK_PATCH, BSVSP06,
                    VDiskLogPrefix << " TEvVPatch: received parts index;",
                    (OriginalBlobId, OriginalBlobId),
                    (Status, record.GetStatus()),
                    (ResultSize, record.ResultSize()));
            if (record.GetStatus() != NKikimrProto::OK) {
                ErrorReason = TStringBuilder() << "Recieve not OK status from VGetRange,"
                        << " received status# " << NKikimrProto::EReplyStatus_Name(record.GetStatus());
                SendVPatchFoundParts(NKikimrProto::ERROR);
                NotifySkeletonAboutDying();
                Become(&TThis::ErrorState);
                return;
            }
            if (record.ResultSize() != 1) {
                ErrorReason = TStringBuilder() << "Expected only one result, but given " << record.ResultSize()
                        << " received status# " << NKikimrProto::EReplyStatus_Name(record.GetStatus());
                SendVPatchFoundParts(NKikimrProto::ERROR);
                NotifySkeletonAboutDying();
                Become(&TThis::ErrorState);
                return;
            }

            // it has to have only one result
            auto &item = record.GetResult(0);
            FoundOriginalParts.reserve(item.PartsSize());
            Y_VERIFY(item.HasStatus());
            if (item.GetStatus() == NKikimrProto::OK) {
                for (ui32 partId : item.GetParts()) {
                    FoundOriginalParts.push_back(partId);
                }
            }

            SendVPatchFoundParts(NKikimrProto::OK);
            if (FoundOriginalParts.empty()) {
                NotifySkeletonAboutDying();
                Become(&TThis::ErrorState);
                return;
            }
        }

        void SendVPatchResult(NKikimrProto::EReplyStatus status)
        {
            STLOG(PRI_INFO, BS_VDISK_PATCH, BSVSP07,
                    VDiskLogPrefix << " TEvVPatch: send patch result;",
                    (OriginalBlobId, OriginalBlobId),
                    (PatchedBlobId, PatchedBlobId),
                    (OriginalPartId, (ui32)OriginalPartId),
                    (PatchedPartId, (ui32)PatchedPartId),
                    (Status, status),
                    (ErrorReason, ErrorReason));
            Y_VERIFY(ResultEvent);
            ResultEvent->SetStatus(status, ErrorReason);
            SendVDiskResponse(TActivationContext::AsActorContext(), Sender, ResultEvent.release(), Cookie);
        }

        void HandleVGetResult(TEvBlobStorage::TEvVGetResult::TPtr &ev) {
            NKikimrBlobStorage::TEvVGetResult &record = ev->Get()->Record;
            Y_VERIFY(record.HasStatus());
            if (record.GetStatus() != NKikimrProto::OK) {
                ErrorReason = TStringBuilder() << "Recieve not OK status from VGetResult,"
                        << " received status# " << NKikimrProto::EReplyStatus_Name(record.GetStatus());
                SendVPatchResult(NKikimrProto::ERROR);
                NotifySkeletonAboutDying();
                Become(&TThis::ErrorState);
                return;
            }
            if (record.ResultSize() != 1) {
                ErrorReason = TStringBuilder() << "Recieve not correct result count from VGetResult,"
                        << " expetced 1 but given " << record.ResultSize();
                SendVPatchResult(NKikimrProto::ERROR);
                NotifySkeletonAboutDying();
                Become(&TThis::ErrorState);
                return;
            }

            auto &item = *record.MutableResult(0);
            Y_VERIFY(item.HasStatus());
            if (item.GetStatus() != NKikimrProto::OK) {
                ErrorReason = TStringBuilder() << "Recieve not OK status from VGetResult,"
                        << " received status# " << NKikimrProto::EReplyStatus_Name(record.GetStatus())
                        << " response status# " << NKikimrProto::EReplyStatus_Name(item.GetStatus());
                SendVPatchResult(NKikimrProto::ERROR);
                NotifySkeletonAboutDying();
                Become(&TThis::ErrorState);
                return;
            }

            Y_VERIFY(item.HasBlobID());
            TLogoBlobID blobId = LogoBlobIDFromLogoBlobID(item.GetBlobID());

            Y_VERIFY(item.HasBuffer());
            Buffer = TRope(item.GetBuffer());

            STLOG(PRI_INFO, BS_VDISK_PATCH, BSVSP08,
                    VDiskLogPrefix << " TEvVPatch: received part data;",
                    (OriginalBlobId, OriginalBlobId),
                    (PatchedBlobId, PatchedBlobId),
                    (OriginalPartId, (ui32)OriginalPartId),
                    (PatchedPartId, (ui32)PatchedPartId),
                    (DataParts, (ui32)GType.DataParts()),
                    (ReceivedBlobId, blobId),
                    (Status, record.GetStatus()),
                    (ResultSize, record.ResultSize()),
                    (ParityPart, (blobId.PartId() <= GType.DataParts() ? "no" : "yes")));

            ui8 *buffer = reinterpret_cast<ui8*>(Buffer.UnsafeGetContiguousSpanMut().data());
            if (blobId.PartId() <= GType.DataParts()) {
                if (GType.ErasureFamily() != TErasureType::ErasureMirror) {
                    SendXorDiff();
                }
                GType.ApplyDiff(TErasureType::CrcModeNone, buffer, Diffs);
                SendVPut();
            } else {
                ui8 toPart = blobId.PartId();
                ui32 dataSize = blobId.BlobSize();

                for (ui32 idx = ReceivedXorDiffs.size(); idx != 0; --idx) {
                    auto &[diffs, partId, sender, cookie] = ReceivedXorDiffs.back();
                    GType.ApplyXorDiff(TErasureType::CrcModeNone, dataSize, buffer, diffs, partId - 1, toPart - 1);
                    ReceivedXorDiffs.pop_back();
                }

                if (ReceivedXorDiffCount == WaitedXorDiffCount) {
                    SendVPut();
                }
            }
        }

        template <typename TDiffEvent>
        TVector<TDiff> PullDiff(const TDiffEvent &diffRecord, bool isXor) {
            TVector<TDiff> diffs;
            diffs.reserve(diffRecord.DiffsSize());
            for (auto &diff : diffRecord.GetDiffs()) {
                TString buffer = diff.GetBuffer();
                bool isAligned = (GType.ErasureFamily() != TErasureType::ErasureMirror);
                diffs.emplace_back(buffer, diff.GetOffset(), isXor, isAligned);
            }
            return std::move(diffs);
        }

        bool CheckDiff(const TVector<TDiff> &diffs, const TString &diffName) {
            for (ui32 diffIdx = 0; diffIdx < diffs.size(); ++diffIdx) {
                const TDiff &diff = diffs[diffIdx];
                bool ok = (diff.Offset < GType.PartSize(OriginalBlobId));
                ok &= (diff.Offset + diff.GetDiffLength() <= GType.PartSize(OriginalBlobId));
                if (!ok) {
                    ErrorReason = TStringBuilder() << "The diff at index " << diffIdx << " went beyound the blob part;"
                            << " DiffStart# " << diff.Offset
                            << " DiffEnd# " << diff.Offset + diff.GetDiffLength()
                            << " BlobPartSize# " << GType.PartSize(OriginalBlobId);
                    return false;
                }
            }
            for (ui32 diffIdx = 1; diffIdx < diffs.size(); ++diffIdx) {
                ui32 prevIdx = diffIdx - 1;
                bool ok = diffs[prevIdx].Offset < diffs[diffIdx].Offset;
                if (!ok) {
                    ErrorReason = TStringBuilder() << "[" << diffName << "]"
                            << " the start of the diff at index " << prevIdx << " righter than"
                            << " the start of the diff at index " << diffIdx << ';'
                            << " PrevDiffStart# " << diffs[prevIdx].Offset + diffs[prevIdx].GetDiffLength()
                            << " DiffStart# " << diffs[diffIdx].Offset;
                    return false;
                }
            }
            return true;
        }

        void SendXorDiff() {
            TVector<TDiff> xorDiffs;

            STLOG(PRI_INFO, BS_VDISK_PATCH, BSVSP14,
                    VDiskLogPrefix << " TEvVPatch: send xor diffs;",
                    (OriginalBlobId, OriginalBlobId),
                    (PatchedBlobId, PatchedBlobId),
                    (OriginalPartId, (ui32)OriginalPartId),
                    (PatchedPartId, (ui32)PatchedPartId),
                    (XorDiffCount, XorReceivers.size()));

            const ui8 *buffer = reinterpret_cast<const ui8*>(Buffer.GetContiguousSpan().data());
            GType.MakeXorDiff(TErasureType::CrcModeNone, OriginalBlobId.BlobSize(), buffer, Diffs, &xorDiffs);

            for (TXorReceiver &xorReceiver : XorReceivers) {
                std::unique_ptr<TEvBlobStorage::TEvVPatchXorDiff> xorDiff = std::make_unique<TEvBlobStorage::TEvVPatchXorDiff>(
                        TLogoBlobID(OriginalBlobId, xorReceiver.PartId),
                        TLogoBlobID(PatchedBlobId, xorReceiver.PartId),
                        xorReceiver.VDiskId, OriginalPartId, Deadline, 0);
                for (auto &diff : xorDiffs) {
                    Y_VERIFY(diff.Offset < GType.PartSize(PatchedBlobId));
                    Y_VERIFY(diff.Offset + diff.GetDiffLength() <= GType.PartSize(PatchedBlobId));
                    xorDiff->AddDiff(diff.Offset, diff.Buffer);
                }
                TVDiskIdShort shortId(xorReceiver.VDiskId);
                Y_VERIFY(VPatchCtx);
                Y_VERIFY(VPatchCtx->AsyncBlobQueues);
                auto it = VPatchCtx->AsyncBlobQueues.find(shortId);
                Y_VERIFY(it != VPatchCtx->AsyncBlobQueues.end());

                TInstant now = TActivationContext::Now();
                NKikimrBlobStorage::TEvVPatchXorDiff &record = xorDiff->Record;
                NKikimrBlobStorage::TMsgQoS &msgQoS = *record.MutableMsgQoS();
                NKikimrBlobStorage::TExecTimeStats &execTimeStats = *msgQoS.MutableExecTimeStats();
                execTimeStats.SetSubmitTimestamp(now.GetValue());

                Send(it->second, xorDiff.release());
            }
        }

        void SendVPut() {
            STLOG(PRI_INFO, BS_VDISK_PATCH, BSVSP15,
                    VDiskLogPrefix << " TEvVPatch: send vPut;",
                    (OriginalBlobId, OriginalBlobId),
                    (PatchedBlobId, PatchedBlobId),
                    (OriginalPartId, (ui32)OriginalPartId),
                    (PatchedPartId, (ui32)PatchedPartId),
                    (ReceivedXorDiffs, ReceivedXorDiffCount),
                    (ExpectedXorDiffs, WaitedXorDiffCount));
            ui64 cookie = OriginalBlobId.Hash();
            std::unique_ptr<IEventBase> put = std::make_unique<TEvBlobStorage::TEvVPut>(TLogoBlobID(PatchedBlobId, PatchedPartId),
                    Buffer, VDiskId, false, &cookie, Deadline, NKikimrBlobStorage::AsyncBlob);
            Send(LeaderId, put.release());
        }

        void HandleError(TEvBlobStorage::TEvVPatchDiff::TPtr &ev) {
            NKikimrBlobStorage::TEvVPatchDiff &record = ev->Get()->Record;
            TInstant now = TActivationContext::Now();
            ResultEvent = std::make_unique<TEvBlobStorage::TEvVPatchResult>(
                    NKikimrProto::OK, TLogoBlobID(OriginalBlobId, OriginalPartId),
                    TLogoBlobID(PatchedBlobId, PatchedPartId), VDiskId, record.GetCookie(), now,
                    &record, SkeletonFrontIDPtr, VPatchResMsgsPtr, nullptr, IncarnationGuid);
            Sender = ev->Sender;
            Cookie = ev->Cookie;
            SendVPatchResult(NKikimrProto::ERROR);
        }

        void HandleForceEnd(TEvBlobStorage::TEvVPatchDiff::TPtr &ev) {
            bool forceEnd = ev->Get()->IsForceEnd();
            SendVPatchFoundParts(NKikimrProto::ERROR);
            if (forceEnd) {
                SendVPatchResult(NKikimrProto::OK);
            } else {
                SendVPatchResult(NKikimrProto::ERROR);
            }
            NotifySkeletonAboutDying();
            Become(&TThis::ErrorState);
        }

        void Handle(TEvBlobStorage::TEvVPatchDiff::TPtr &ev) {
            NKikimrBlobStorage::TEvVPatchDiff &record = ev->Get()->Record;
            Y_VERIFY(record.HasCookie());

            TLogoBlobID originalPartBlobId = LogoBlobIDFromLogoBlobID(record.GetOriginalPartBlobId());
            TLogoBlobID patchedPartBlobId = LogoBlobIDFromLogoBlobID(record.GetPatchedPartBlobId());
            OriginalPartId = originalPartBlobId.PartId();
            PatchedPartId = patchedPartBlobId.PartId();

            bool forceEnd = ev->Get()->IsForceEnd();

            bool isXorReceiver = ev->Get()->IsXorReceiver();
            WaitedXorDiffCount = ev->Get()->GetExpectedXorDiffs();

            if (isXorReceiver) {
                Become(&TThis::ParityState);
            } else {
                Become(&TThis::DataState);
            }

            STLOG(PRI_INFO, BS_VDISK_PATCH, BSVSP09,
                    VDiskLogPrefix << " TEvVPatch: received diff;",
                    (OriginalBlobId, OriginalBlobId),
                    (PatchedBlobId, PatchedBlobId),
                    (OriginalPartId, (ui32)OriginalPartId),
                    (PatchedPartId, (ui32)PatchedPartId),
                    (XorReceiver, (isXorReceiver ? "yes" : "no")),
                    (ParityPart, (PatchedPartId <= GType.DataParts() ? "no" : "yes")),
                    (ForceEnd, (forceEnd ? "yes" : "no")));

            Y_VERIFY(!ResultEvent);
            TInstant now = TActivationContext::Now();

            ResultEvent = std::make_unique<TEvBlobStorage::TEvVPatchResult>(
                    NKikimrProto::OK, originalPartBlobId, patchedPartBlobId, VDiskId, record.GetCookie(), now,
                    &record, SkeletonFrontIDPtr, VPatchResMsgsPtr, nullptr, IncarnationGuid);
            Sender = ev->Sender;
            Cookie = ev->Cookie;

            if (forceEnd) {
                SendVPatchResult(NKikimrProto::OK);
                NotifySkeletonAboutDying();
                Become(&TThis::ErrorState);
                return;
            }

            for (auto &protoXorReceiver : record.GetXorReceivers()) {
                Y_VERIFY(protoXorReceiver.HasVDiskID());
                Y_VERIFY(protoXorReceiver.HasPartId());
                XorReceivers.emplace_back(
                        VDiskIDFromVDiskID(protoXorReceiver.GetVDiskID()),
                        protoXorReceiver.GetPartId());
            }

            Diffs = PullDiff(record, false);

            if (!CheckDiff(Diffs, "Diff from DSProxy")) {
                SendVPatchResult(NKikimrProto::ERROR);
                NotifySkeletonAboutDying();
                Become(&TThis::ErrorState);
                return;
            }

            PullOriginalPart(OriginalPartId);
        }

        void Handle(TEvBlobStorage::TEvVPutResult::TPtr &ev) {
            NKikimrBlobStorage::TEvVPutResult &record = ev->Get()->Record;
            Y_VERIFY(record.HasStatus());

            STLOG(PRI_INFO, BS_VDISK_PATCH, BSVSP10,
                    VDiskLogPrefix << " TEvVPatch: received put result;",
                    (OriginalBlobId, OriginalBlobId),
                    (PatchedBlobId, PatchedBlobId),
                    (OriginalPartId, (ui32)OriginalPartId),
                    (PatchedPartId, (ui32)PatchedPartId),
                    (Status, record.GetStatus()));

            NKikimrProto::EReplyStatus status = NKikimrProto::OK;
            if (record.GetStatus() != NKikimrProto::OK) {
                ErrorReason = TStringBuilder() << "Recieve not OK status from VPutResult,"
                        << " received status# " << NKikimrProto::EReplyStatus_Name(record.GetStatus());
                status = NKikimrProto::ERROR;
            }

            ResultEvent->SetStatusFlagsAndFreeSpace(record.GetStatusFlags(), record.GetApproximateFreeSpaceShare());

            SendVPatchResult(status);
            NotifySkeletonAboutDying();
            Become(&TThis::ErrorState);
        }

        void HandleError(TEvBlobStorage::TEvVPatchXorDiff::TPtr &ev) {
            NKikimrBlobStorage::TEvVPatchXorDiff &record = ev->Get()->Record;
            TInstant now = TActivationContext::Now();
            auto resultEvent = std::make_unique<TEvBlobStorage::TEvVPatchXorDiffResult>(
                    NKikimrProto::ERROR, now, &record, SkeletonFrontIDPtr, VPatchResMsgsPtr, nullptr);
            SendVDiskResponse(TActivationContext::AsActorContext(), ev->Sender, resultEvent.release(), ev->Cookie);
        }

        void Handle(TEvBlobStorage::TEvVPatchXorDiff::TPtr &ev) {
            NKikimrBlobStorage::TEvVPatchXorDiff &record = ev->Get()->Record;
            Y_VERIFY(record.HasFromPartId());
            ui8 fromPart = record.GetFromPartId();
            ui8 toPart = OriginalPartId;
            TVector<TDiff> xorDiffs = PullDiff(record, true);
            ReceivedXorDiffCount++;

            STLOG(PRI_INFO, BS_VDISK_PATCH, BSVSP13,
                    VDiskLogPrefix << " TEvVPatch: received xor diff;",
                    (OriginalBlobId, OriginalBlobId),
                    (PatchedBlobId, PatchedBlobId),
                    (FromPart, (ui32)fromPart),
                    (ToPart, (ui32)toPart),
                    (HasBuffer, (Buffer.GetSize() == 0 ? "no" : "yes")),
                    (ReceivedXorDiffCount, TStringBuilder() << ReceivedXorDiffCount << '/' << WaitedXorDiffCount));

            TInstant now = TActivationContext::Now();
            std::unique_ptr<TEvBlobStorage::TEvVPatchXorDiffResult> resultEvent = std::make_unique<TEvBlobStorage::TEvVPatchXorDiffResult>(
                    NKikimrProto::OK, now, &record, SkeletonFrontIDPtr, VPatchResMsgsPtr, nullptr);
            SendVDiskResponse(TActivationContext::AsActorContext(), ev->Sender, resultEvent.release(), ev->Cookie);

            if (!CheckDiff(xorDiffs, "XorDiff from datapart")) {
                if (ResultEvent) {
                    SendVPatchResult(NKikimrProto::ERROR);
                }
                NotifySkeletonAboutDying();
                Become(&TThis::ErrorState);
                return;
            }

            if (Buffer) {
                ui8 *buffer = reinterpret_cast<ui8*>(Buffer.UnsafeGetContiguousSpanMut().data());
                ui32 dataSize = OriginalBlobId.BlobSize();

                GType.ApplyXorDiff(TErasureType::CrcModeNone, dataSize, buffer, xorDiffs, fromPart - 1, toPart - 1);

                if (ReceivedXorDiffCount == WaitedXorDiffCount) {
                    SendVPut();
                }

                xorDiffs.clear();
            } else {
                ReceivedXorDiffs.emplace_back(std::move(xorDiffs), fromPart,
                        ev->Sender, ev->Cookie);
            }
        }

        void NotifySkeletonAboutDying() {
            STLOG(PRI_DEBUG, BS_VDISK_PATCH, BSVSP17, VDiskLogPrefix << " NotifySkeletonAboutDying;");
            Send(LeaderId, new TEvVPatchDyingRequest(PatchedBlobId));
        }

        void HandleInStartState(TKikimrEvents::TEvWakeup::TPtr &/*ev*/) {
            ErrorReason = "TEvVPatch: the vpatch actor died due to a deadline, before receiving diff";
            STLOG(PRI_ERROR, BS_VDISK_PATCH, BSVSP11, VDiskLogPrefix << " " << ErrorReason << ";");
            SendVPatchFoundParts(NKikimrProto::ERROR);
            NotifySkeletonAboutDying();
            Become(&TThis::ErrorState);
        }

        void HandleInWaitState(TKikimrEvents::TEvWakeup::TPtr &/*ev*/) {
            ErrorReason = "TEvVPatch: the vpatch actor died due to a deadline, before receiving diff";
            STLOG(PRI_ERROR, BS_VDISK_PATCH, BSVSP16, VDiskLogPrefix << " " << ErrorReason << ";");
            NotifySkeletonAboutDying();
            Become(&TThis::ErrorState);
        }

        void HandleInDataStates(TKikimrEvents::TEvWakeup::TPtr &/*ev*/) {
            ErrorReason = "TEvVPatch: the vpatch actor died due to a deadline, after receiving diff";
            STLOG(PRI_ERROR, BS_VDISK_PATCH, BSVSP12, VDiskLogPrefix << " " << ErrorReason << ";");
            SendVPatchResult(NKikimrProto::ERROR);
            NotifySkeletonAboutDying();
            Become(&TThis::ErrorState);
        }

        void HandleInParityStates(TKikimrEvents::TEvWakeup::TPtr &/*ev*/) {
            ErrorReason = "TEvVPatch: the vpatch actor died due to a deadline, after receiving diff";
            STLOG(PRI_ERROR, BS_VDISK_PATCH, BSVSP20, VDiskLogPrefix << " " << ErrorReason << ";");
            SendVPatchResult(NKikimrProto::ERROR);
            NotifySkeletonAboutDying();
            Become(&TThis::ErrorState);
        }

        STATEFN(StartState) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvBlobStorage::TEvVGetResult, HandleVGetRangeResult)
                hFunc(TEvBlobStorage::TEvVPatchXorDiff, Handle)
                hFunc(TEvBlobStorage::TEvVPatchDiff, HandleForceEnd)
                hFunc(TKikimrEvents::TEvWakeup, HandleInStartState)
                default: Y_FAIL_S(VDiskLogPrefix << " unexpected event " << TypeName(*ev->GetBase()));
            }
        }

        STATEFN(WaitState) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvBlobStorage::TEvVPatchDiff, Handle)
                hFunc(TEvBlobStorage::TEvVPatchXorDiff, Handle)
                hFunc(TKikimrEvents::TEvWakeup, HandleInWaitState)
                default: Y_FAIL_S(VDiskLogPrefix << " unexpected event " << TypeName(*ev->GetBase()));
            }
        }

        STATEFN(ErrorState) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvBlobStorage::TEvVPatchDiff, HandleError)
                hFunc(TEvBlobStorage::TEvVPatchXorDiff, HandleError)
                IgnoreFunc(TEvBlobStorage::TEvVPatchXorDiffResult)
                hFunc(TKikimrEvents::TEvWakeup, HandleInWaitState)
                sFunc(TEvVPatchDyingConfirm, PassAway)
                default: Y_FAIL_S(VDiskLogPrefix << " unexpected event " << TypeName(*ev->GetBase()));
            }
        }

        STATEFN(DataState) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvBlobStorage::TEvVGetResult, HandleVGetResult)
                hFunc(TEvBlobStorage::TEvVPutResult, Handle)
                IgnoreFunc(TEvBlobStorage::TEvVPatchXorDiffResult)
                hFunc(TKikimrEvents::TEvWakeup, HandleInDataStates)
                default: Y_FAIL_S(VDiskLogPrefix << " unexpected event " << TypeName(*ev->GetBase()));
            }
        }

        STATEFN(ParityState) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvBlobStorage::TEvVGetResult, HandleVGetResult)
                hFunc(TEvBlobStorage::TEvVPutResult, Handle)
                hFunc(TEvBlobStorage::TEvVPatchXorDiff, Handle)
                hFunc(TKikimrEvents::TEvWakeup, HandleInParityStates)
                default: Y_FAIL_S(VDiskLogPrefix << " unexpected event " << TypeName(*ev->GetBase()));
            }
        }
    };

} // NKikimr::NPrivate

namespace NKikimr {

    IActor* CreateSkeletonVPatchActor(TActorId leaderId, const TBlobStorageGroupType &gType,
            TEvBlobStorage::TEvVPatchStart::TPtr &ev, TInstant now, TActorIDPtr skeletonFrontIDPtr,
            const ::NMonitoring::TDynamicCounters::TCounterPtr &vPatchFoundPartsMsgsPtr,
            const ::NMonitoring::TDynamicCounters::TCounterPtr &vPatchResMsgsPtr,
            const TIntrusivePtr<TVPatchCtx> &vPatchCtx, const TString &vDiskLogPrefix, ui64 incarnationGuid)
    {
        return new NPrivate::TSkeletonVPatchActor(leaderId, gType, ev, now, skeletonFrontIDPtr,
                vPatchFoundPartsMsgsPtr, vPatchResMsgsPtr, vPatchCtx, vDiskLogPrefix, incarnationGuid);
    }

} // NKikimr

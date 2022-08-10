#include "dsproxy.h"
#include "dsproxy_mon.h"
#include "root_cause.h"
#include <ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.h>

#include <util/generic/ymath.h>
#include <util/system/datetime.h>
#include <util/system/hp_timer.h>

LWTRACE_USING(BLOBSTORAGE_PROVIDER);

namespace NKikimr {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PATCH request
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TBlobStorageGroupPatchRequest : public TBlobStorageGroupRequestActor<TBlobStorageGroupPatchRequest> {
    friend class TBlobStorageGroupRequestActor<TBlobStorageGroupPatchRequest>;

    struct TPartPlacement {
        ui8 VDiskIdxInSubgroup = 0;
        ui8 PartId = 0;

        TString ToString() const {
            return TStringBuilder() << "{VDiskIdxInSubgroup# " << (ui32)VDiskIdxInSubgroup << " PartId# " << (ui32)PartId << "}";
        }
    };

    static constexpr ui32 TypicalHandoffCount = 2;
    static constexpr ui32 TypicalPartPlacementCount = 1 + TypicalHandoffCount;
    static constexpr ui32 TypicalMaxPartsCount = TypicalPartPlacementCount * TypicalPartsInBlob;

    TString Buffer;

    ui32 OriginalGroupId;
    TLogoBlobID OriginalId;
    TLogoBlobID PatchedId;
    ui32 MaskForCookieBruteForcing;

    ui32 DiffCount = 0;
    TArrayHolder<TEvBlobStorage::TEvPatch::TDiff> Diffs;

    TStorageStatusFlags StatusFlags = 0;
    float ApproximateFreeSpaceShare = 0;

    TInstant StartTime;
    TInstant Deadline;

    NLWTrace::TOrbit Orbit;
    TString ErrorReason;

    ui32 SendedGetRequests = 0;
    ui32 ReceivedGetResponses = 0;
    ui32 SendedPutRequests = 0;
    ui32 ReceivedPutResponses = 0;

    TVector<ui32> OkVDisksWithParts;

    ui32 SendedStarts = 0;
    ui32 ReceivedFoundParts = 0;
    ui32 ErrorResponses = 0;
    ui32 ReceivedResults = 0;

    TStackVec<TPartPlacement, TypicalMaxPartsCount> FoundParts;
    TStackVec<bool, TypicalDisksInSubring> ReceivedResponseFlags;
    TStackVec<bool, TypicalDisksInSubring> EmptyResponseFlags;
    TStackVec<bool, TypicalDisksInSubring> ErrorResponseFlags;
    TBlobStorageGroupInfo::TVDiskIds VDisks;

    bool UseVPatch = false;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BS_PROXY_PATCH_ACTOR;
    }

    static const auto& ActiveCounter(const TIntrusivePtr<TBlobStorageGroupProxyMon>& mon) {
        return mon->ActivePatch;
    }

    static constexpr ERequestType RequestType() {
        return ERequestType::Patch;
    }

    TBlobStorageGroupPatchRequest(const TIntrusivePtr<TBlobStorageGroupInfo> &info,
            const TIntrusivePtr<TGroupQueues> &state, const TActorId &source,
            const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon, TEvBlobStorage::TEvPatch *ev,
            ui64 cookie, NWilson::TTraceId traceId, TInstant now,
            TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters,
            bool useVPatch = false)
        : TBlobStorageGroupRequestActor(info, state, mon, source, cookie, std::move(traceId),
                NKikimrServices::BS_PROXY_PATCH, false, {}, now, storagePoolCounters,
                ev->RestartCounter, "DSProxy.Patch")
        , OriginalGroupId(ev->OriginalGroupId)
        , OriginalId(ev->OriginalId)
        , PatchedId(ev->PatchedId)
        , MaskForCookieBruteForcing(ev->MaskForCookieBruteForcing)
        , DiffCount(ev->DiffCount)
        , Diffs(ev->Diffs.Release())
        , StartTime(now)
        , Deadline(ev->Deadline)
        , Orbit(std::move(ev->Orbit))
        , UseVPatch(useVPatch)
    {
    }

    void ReplyAndDie(NKikimrProto::EReplyStatus status) {
        std::unique_ptr<TEvBlobStorage::TEvPatchResult> result = std::make_unique<TEvBlobStorage::TEvPatchResult>(status, PatchedId,
                StatusFlags, Info->GroupID, ApproximateFreeSpaceShare);
        result->ErrorReason = ErrorReason;
        result->Orbit = std::move(Orbit);
        TDuration duration = TActivationContext::Now() - StartTime;
        Mon->CountPatchResponseTime(Info->GetDeviceType(), duration);
        SendResponseAndDie(std::move(result));
    }

    std::unique_ptr<IEventBase> RestartQuery(ui32 counter) {
        ++*Mon->NodeMon->RestartPatch;
        TEvBlobStorage::TEvPatch *patch;
        std::unique_ptr<IEventBase> ev(patch = new TEvBlobStorage::TEvPatch(OriginalGroupId, OriginalId, PatchedId,
                MaskForCookieBruteForcing, std::move(Diffs), DiffCount, Deadline));
        patch->RestartCounter = counter;
        patch->Orbit = std::move(Orbit);
        return std::move(ev);
    }

    void ApplyDiffs() {
        for (ui32 idx = 0; idx < DiffCount; ++idx) {
            const TEvBlobStorage::TEvPatch::TDiff &diff = Diffs[idx];
            Copy(diff.Buffer.begin(), diff.Buffer.end(), Buffer.begin() + diff.Offset);
        }
    }

    void Handle(TEvBlobStorage::TEvGetResult::TPtr &ev) {
        TEvBlobStorage::TEvGetResult *result = ev->Get();
        Orbit = std::move(result->Orbit);

        ui32 patchedIdHash = PatchedId.Hash();
        bool incorrectCookie = ev->Cookie != patchedIdHash;
        bool fail = incorrectCookie
                || result->Status != NKikimrProto::OK
                || result->ResponseSz != 1
                || result->Responses[0].Status != NKikimrProto::OK;
        if (fail) {
            if (ev->Cookie != patchedIdHash) {
                ErrorReason = "Couldn't get the original blob; Received TEvGetResult with wrong cookie";
            } else if (result->ResponseSz > 1) {
                ErrorReason = "Couldn't get the original blob; Received TEvGetResult with more responses than needed";
            } else {
                TString getResponseStatus;
                if (result->ResponseSz == 1) {
                    getResponseStatus = TStringBuilder() << " GetResponseStatus# "
                            << NKikimrProto::EReplyStatus_Name(result->Responses[0].Status);
                }
                ErrorReason = TStringBuilder() << "Couldn't get the original blob;"
                        << " GetStatus# " << NKikimrProto::EReplyStatus_Name(result->Status)
                        << getResponseStatus
                        << " GetErrorReason# " << result->ErrorReason;
            }
            R_LOG_ERROR_S("BPPA04", ErrorReason);
            ReplyAndDie(NKikimrProto::ERROR);
            return;
        }

        Buffer = result->Responses[0].Buffer;
        ApplyDiffs();

        std::unique_ptr<TEvBlobStorage::TEvPut> put = std::make_unique<TEvBlobStorage::TEvPut>(PatchedId, Buffer, Deadline,
                NKikimrBlobStorage::AsyncBlob, TEvBlobStorage::TEvPut::TacticDefault);
        put->Orbit = std::move(Orbit);
        SendToProxy(std::move(put), OriginalId.Hash(), Span.GetTraceId());
    }

    void Handle(TEvBlobStorage::TEvPutResult::TPtr &ev) {
        TEvBlobStorage::TEvPutResult *result = ev->Get();
        Orbit = std::move(result->Orbit);

        StatusFlags = result->StatusFlags;
        ApproximateFreeSpaceShare = result->ApproximateFreeSpaceShare;

        ui32 originalIdHash = OriginalId.Hash();
        bool incorrectCookie = ev->Cookie != originalIdHash;
        bool fail = incorrectCookie
            || result->Status != NKikimrProto::OK;
        if (fail) {
            if (incorrectCookie) {
                ErrorReason = "Couldn't put the patched blob; Received TEvPutResult with wrong cookie";
            } else {
                ErrorReason = TStringBuilder() << "Couldn't put the patched blob;"
                        << " PutStatus# " << NKikimrProto::EReplyStatus_Name(result->Status)
                        << " PutErrorReason# " << result->ErrorReason;
            }
            R_LOG_ERROR_S("BPPA03", ErrorReason);
            ReplyAndDie(NKikimrProto::ERROR);
            return;
        }

        ReplyAndDie(NKikimrProto::OK);
    }

    template <typename TEventResultRecord>
    void PullOutStatusFlagsAndFressSpace(const TEventResultRecord &record){
        if (record.HasStatusFlags()) {
            StatusFlags.Merge(record.GetStatusFlags());
        }
        if (record.HasApproximateFreeSpaceShare()) {
            float share = record.GetApproximateFreeSpaceShare();
            if (ApproximateFreeSpaceShare == 0.0 || share < ApproximateFreeSpaceShare) {
                ApproximateFreeSpaceShare = share;
            }
        }
    }

    void Handle(TEvBlobStorage::TEvVMovedPatchResult::TPtr &ev) {
        TEvBlobStorage::TEvVMovedPatchResult *result = ev->Get();
        A_LOG_DEBUG_S("BPPA02", "received " << ev->Get()->ToString()
            << " from# " << VDiskIDFromVDiskID(ev->Get()->Record.GetVDiskID()));
        NKikimrBlobStorage::TEvVMovedPatchResult &record = result->Record;
        PullOutStatusFlagsAndFressSpace(record);
        Orbit = std::move(result->Orbit);

        ui64 expectedCookie = ((ui64)OriginalId.Hash() << 32) | PatchedId.Hash();
        bool incorrectCookie = ev->Cookie != expectedCookie;
        Y_VERIFY(record.HasStatus());
        bool fail = incorrectCookie
            || record.GetStatus() != NKikimrProto::OK;
        if (fail) {
            if (incorrectCookie) {
                ErrorReason = "Couldn't put the patched blob; Received TEvVMovedPatchResult with wrong cookie";
            } else {
                TString subErrorReason;
                if (record.HasErrorReason()) {
                    subErrorReason = TStringBuilder() << " VMovedPatchErrorReason# " << record.GetErrorReason();
                }
                ErrorReason = TStringBuilder() << "Couldn't complete patch;"
                        << " VMovedPatchStatus# " << NKikimrProto::EReplyStatus_Name(record.GetStatus())
                        << subErrorReason;
            }
            A_LOG_INFO_S("BPPA05", "VMovedPatch failed, NaivePatch started;"
                    << " OriginalId# " << OriginalId
                    << " PatchedId# " << PatchedId
                    << " ErrorReason# " << ErrorReason);
            StartNaivePatch();
            return;
        }

        ReplyAndDie(NKikimrProto::OK);
    }

    void Handle(TEvBlobStorage::TEvVPatchFoundParts::TPtr &ev) {
        ReceivedFoundParts++;

        NKikimrBlobStorage::TEvVPatchFoundParts &record = ev->Get()->Record;

        Y_VERIFY(record.HasCookie());
        ui8 subgroupIdx = record.GetCookie();

        Y_VERIFY(record.HasStatus());
        NKikimrProto::EReplyStatus status = record.GetStatus();

        TString errorReason;
        if (record.HasErrorReason()) {
            errorReason = record.GetErrorReason();
        }

        bool wasReceived = std::exchange(ReceivedResponseFlags[subgroupIdx], true);
        Y_VERIFY(!wasReceived);

        if (status == NKikimrProto::ERROR) {
            ErrorResponses++;
            ErrorResponseFlags[subgroupIdx] = true;
        }

        EmptyResponseFlags[subgroupIdx] = !record.OriginalPartsSize();
        for (auto &partId : record.GetOriginalParts()) {
            FoundParts.push_back({subgroupIdx, (ui8)partId});
        }

        if (record.OriginalPartsSize()) {
            OkVDisksWithParts.push_back(subgroupIdx);
        }

        A_LOG_INFO_S("BPPA07", "received VPatchFoundParts"
                << " Status# " << status
                << " ActorId# " << SelfId()
                << " Group# " << Info->GroupID
                << " OriginalBlob# " << OriginalId
                << " PatchedBlob# " << PatchedId
                << " Deadline# " << Deadline
                << " SubgroupIdx# " << (ui32)subgroupIdx
                << " PartsCount# " << record.OriginalPartsSize()
                << " ReceivedFoundParts# " << ReceivedFoundParts << '/' << SendedStarts
                << " ErrorReason# " << errorReason);

        if (ReceivedFoundParts == SendedStarts) {
            bool continueVPatch = VerifyPartPlacement();
            if (continueVPatch) {
                continueVPatch = ContinueVPatch();
            } else {
                Mon->VPatchPartPlacementVerifyFailed->Inc();
            }
            if (!continueVPatch) {
                StopVPatch();
                StartFallback();
            }
        }
    }

    void Handle(TEvBlobStorage::TEvVPatchResult::TPtr &ev) {
        ReceivedResults++;
        NKikimrBlobStorage::TEvVPatchResult &record = ev->Get()->Record;
        PullOutStatusFlagsAndFressSpace(record);
        Y_VERIFY(record.HasStatus());
        NKikimrProto::EReplyStatus status = record.GetStatus();
        TString errorReason;
        if (record.HasErrorReason()) {
            errorReason = record.GetErrorReason();
        }

        A_LOG_INFO_S("BPPA06", "received VPatchResult"
                << " Status# " << status
                << " ActorId# " << SelfId()
                << " Group# " << Info->GroupID
                << " OriginalBlob# " << OriginalId
                << " PatchedBlob# " << PatchedId
                << " Deadline# " << Deadline
                << " ReceivedResults# " << ReceivedResults << '/' << Info->Type.TotalPartCount()
                << " ErrorReason# " << errorReason);

        Y_VERIFY(record.HasCookie());
        ui8 subgroupIdx = record.GetCookie();

        bool wasReceived = std::exchange(ReceivedResponseFlags[subgroupIdx], true);
        Y_VERIFY(!wasReceived);

        if (status != NKikimrProto::OK) {
            StartFallback();
            return;
        }

        if (ReceivedResults == Info->Type.TotalPartCount()) {
            ReplyAndDie(NKikimrProto::OK);
        }
    }

    bool VerifyPartPlacementForMirror3dc() const {
        constexpr ui32 DCCount = 3;
        constexpr ui32 VDiskByDC = 3;
        ui32 countByDC[DCCount] = {0, 0, 0};

        for (auto &[subgroupIdx, partId] : FoundParts) {
            countByDC[subgroupIdx / VDiskByDC]++;
        }

        if (countByDC[0] && countByDC[1] && countByDC[2]) {
            return true;
        }

        ui32 x2Count = 0;
        for (ui32 dcIdx = 0; dcIdx < DCCount; ++dcIdx) {
            if (countByDC[dcIdx] >= 2) {
                x2Count++;
            }
        }
        return x2Count >= 2;
    }

    bool VerifyPartPlacement() const {
        if (Info->Type.GetErasure() == TErasureType::ErasureMirror3dc) {
            return VerifyPartPlacementForMirror3dc();
        } else {
            TSubgroupPartLayout layout;

            for (auto &[subgroupIdx, partId] : FoundParts) {
                layout.AddItem(subgroupIdx, partId - 1, Info->Type);
            }
            return layout.CountEffectiveReplicas(Info->Type) == Info->Type.TotalPartCount();
        }
    }

    void SendStopDiffs() {
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVPatchDiff>> events;
        for (ui32 vdiskIdx = 0; vdiskIdx < VDisks.size(); ++vdiskIdx) {
            if (!ErrorResponseFlags[vdiskIdx] && !EmptyResponseFlags[vdiskIdx] && ReceivedResponseFlags[vdiskIdx]) {
                std::unique_ptr<TEvBlobStorage::TEvVPatchDiff> ev = std::make_unique<TEvBlobStorage::TEvVPatchDiff>(
                        OriginalId, PatchedId, VDisks[vdiskIdx], 0, Deadline, vdiskIdx);
                ev->SetForceEnd();
                events.emplace_back(std::move(ev));
            }
        }
        SendToQueues(events, false);
    }

    bool WithXorDiffs() const {
        return Info->Type.ErasureFamily() != TErasureType::ErasureMirror;
    }

    void SendDiffs(const TStackVec<TPartPlacement, TypicalPartsInBlob> &placement) {
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVPatchDiff>> events;

        TPartDiffSet diffSet;
        TVector<TDiff> diffs;
        diffs.reserve(DiffCount);
        for (ui32 diffIdx = 0; diffIdx < DiffCount; ++diffIdx) {
            diffs.emplace_back(Diffs[diffIdx].Buffer, Diffs[diffIdx].Offset, false, false);
        }
        Info->Type.SplitDiffs(TErasureType::CrcModeNone, OriginalId.BlobSize(), diffs, diffSet);

        ui32 dataParts = Info->Type.DataParts();
        ui32 dataPartCount = 0;
        TStackVec<TPartPlacement, TypicalPartsInBlob> parityPlacements;
        if (Info->Type.ErasureFamily() != TErasureType::ErasureMirror) {
            for (const TPartPlacement &partPlacement : placement) {
                if (partPlacement.PartId <= dataParts) {
                    dataPartCount++;
                } else {
                    parityPlacements.emplace_back(partPlacement);
                }
            }
        }

        for (const TPartPlacement &partPlacement : placement) {
            ui32 idxInSubgroup = partPlacement.VDiskIdxInSubgroup;
            // ui32 patchedPartId = partPlacement.PartId;
            Y_VERIFY_S(idxInSubgroup < VDisks.size(), "vdisidxInSubgroupkIdx# " << idxInSubgroup << "/" << VDisks.size());

            Y_VERIFY(Info->GetIdxInSubgroup(VDisks[idxInSubgroup], OriginalId.Hash()) == idxInSubgroup);
            ui32 patchedIdxInSubgroup = Info->GetIdxInSubgroup(VDisks[idxInSubgroup], PatchedId.Hash());
            if (patchedIdxInSubgroup != idxInSubgroup) {
                // now only mirror3dc has this case (has 9 vdisks instead of 4 or 8)
                Y_VERIFY(Info->Type.GetErasure() == TErasureType::ErasureMirror3dc);
                // patchedPartId = 1 + patchedIdxInSubgroup / 3;;
            }

            ReceivedResponseFlags[idxInSubgroup] = false;
            TLogoBlobID originalPartBlobId(OriginalId, partPlacement.PartId);
            TLogoBlobID partchedPartBlobId(PatchedId, partPlacement.PartId);

            ui32 waitedXorDiffs = (partPlacement.PartId > dataParts)  ? dataPartCount : 0;
            auto ev = std::make_unique<TEvBlobStorage::TEvVPatchDiff>(originalPartBlobId, partchedPartBlobId,
                VDisks[idxInSubgroup], waitedXorDiffs, Deadline, idxInSubgroup);

            ui32 diffForPartIdx = 0;
            if (Info->Type.ErasureFamily() != TErasureType::ErasureMirror) {
                diffForPartIdx = partPlacement.PartId - 1;
            }
            auto &diffsForPart = diffSet.PartDiffs[diffForPartIdx].Diffs;
            for (auto &diff : diffsForPart) {
                ev->AddDiff(diff.Offset, diff.Buffer);
            }

            for (const TPartPlacement &parity : parityPlacements) {
                ev->AddXorReceiver(VDisks[parity.VDiskIdxInSubgroup], parity.PartId);
            }

            events.push_back(std::move(ev));
        }
        SendToQueues(events, false);
        SendStopDiffs();
        ReceivedResponseFlags.assign(VDisks.size(), false);
    }

    void SendDiffs(const TStackVec<bool, TypicalPartsInBlob> &inPrimary,
            const TStackVec<ui32, TypicalHandoffCount> &choosenHandoffForParts)
    {
        ui32 handoffPartIdx = 0;
        TStackVec<ui32, TypicalPartsInBlob> vdiskIdxForParts(Info->Type.TotalPartCount());
        for (ui32 partIdx = 0; partIdx < Info->Type.TotalPartCount(); ++partIdx) {
            vdiskIdxForParts[partIdx] = partIdx;
            if (!inPrimary[partIdx]) {
                vdiskIdxForParts[partIdx] = choosenHandoffForParts[handoffPartIdx];
                handoffPartIdx++;
            }
        }
        TStackVec<TPartPlacement, TypicalPartsInBlob> placements;
        ui32 dataParts = Info->Type.DataParts();
        for (ui32 partIdx = 0; partIdx < Info->Type.TotalPartCount(); ++partIdx) {
            ui32 vdiskIdx = vdiskIdxForParts[partIdx];
            Y_VERIFY_S(vdiskIdx == partIdx || vdiskIdx >= dataParts, "vdiskIdx# " << vdiskIdx << " partIdx# " << partIdx);
            placements.push_back(TPartPlacement{static_cast<ui8>(vdiskIdx), static_cast<ui8>(partIdx + 1)});
        }
        SendDiffs(placements);
    }

    void StartMovedPatch() {
        A_LOG_DEBUG_S("BPPA12", "StartMovedPatch"
            << " ActorId# " << SelfId()
            << " Group# " << Info->GroupID
            << " OriginalBlob# " << OriginalId
            << " PatchedBlob# " << PatchedId
            << " Deadline# " << Deadline);
        Become(&TThis::MovedPatchState);

        ui32 subgroupIdx = 0;
        if (OkVDisksWithParts) {
            ui32 okVDiskIdx = RandomNumber<ui32>(OkVDisksWithParts.size());
            subgroupIdx = OkVDisksWithParts[okVDiskIdx];
        } else {
            subgroupIdx = RandomNumber<ui32>(Info->Type.TotalPartCount());
        }
        TVDiskID vDisk = Info->GetVDiskInSubgroup(subgroupIdx, OriginalId.Hash());
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVMovedPatch>> events;

        ui64 cookie = ((ui64)OriginalId.Hash() << 32) | PatchedId.Hash();
        events.emplace_back(new TEvBlobStorage::TEvVMovedPatch(OriginalGroupId, Info->GroupID,
                OriginalId, PatchedId, vDisk, false, cookie, Deadline));
        events.back()->Orbit = std::move(Orbit);
        for (ui64 diffIdx = 0; diffIdx < DiffCount; ++diffIdx) {
            auto &diff = Diffs[diffIdx];
            events.back()->AddDiff(diff.Offset, diff.Buffer);
        }
        SendToQueues(events, false);
    }

    void StartNaivePatch() {
        A_LOG_DEBUG_S("BPPA11", "StartNaivePatch"
            << " ActorId# " << SelfId()
            << " Group# " << Info->GroupID
            << " OriginalBlob# " << OriginalId
            << " PatchedBlob# " << PatchedId
            << " Deadline# " << Deadline);
        Become(&TThis::NaiveState);
        auto get = std::make_unique<TEvBlobStorage::TEvGet>(OriginalId, 0, OriginalId.BlobSize(), Deadline,
            NKikimrBlobStorage::AsyncRead);
        get->Orbit = std::move(Orbit);
        if (OriginalGroupId == Info->GroupID) {
            SendToProxy(std::move(get), PatchedId.Hash(), Span.GetTraceId());
        } else {
            SendToBSProxy(SelfId(), OriginalGroupId, get.release(), PatchedId.Hash(), Span.GetTraceId());
        }
    }

    void StartFallback() {
        Mon->PatchesWithFallback->Inc();
        if (WithMovingPatchRequestToStaticNode && UseVPatch) {
            StartMovedPatch();
        } else {
            StartNaivePatch();
        }
    }

    void StartVPatch() {
        Become(&TThis::VPatchState);

        Info->PickSubgroup(OriginalId.Hash(), &VDisks, nullptr);
        ReceivedResponseFlags.assign(VDisks.size(), false);
        ErrorResponseFlags.assign(VDisks.size(), false);
        EmptyResponseFlags.assign(VDisks.size(), false);

        TDeque<std::unique_ptr<TEvBlobStorage::TEvVPatchStart>> events;

        for (ui32 idx = 0; idx < VDisks.size(); ++idx) {
            std::unique_ptr<TEvBlobStorage::TEvVPatchStart> ev = std::make_unique<TEvBlobStorage::TEvVPatchStart>(
                    OriginalId, PatchedId, VDisks[idx], Deadline, idx, true);
            events.emplace_back(std::move(ev));
            SendedStarts++;
        }

        A_LOG_DEBUG_S("BPPA08", "StartVPatcn"
            << " ActorId# " << SelfId()
            << " Group# " << Info->GroupID
            << " OriginalBlob# " << OriginalId
            << " PatchedBlob# " << PatchedId
            << " Deadline# " << Deadline
            << " SendedStarts# " << SendedStarts);

        SendToQueues(events, false);
    }

    bool FindHandoffs(const TStackVec<TStackVec<ui32, TypicalHandoffCount>, TypicalPartsInBlob>& handoffForParts,
            const TStackVec<ui32, TypicalPartsInBlob> &handoffParts,
            TStackVec<ui32, TypicalHandoffCount> *choosenHandoffForParts, ui8 depth = 0)
    {
        auto &choosen = *choosenHandoffForParts;
        Y_VERIFY_DEBUG(choosen.size() == handoffParts.size());
        if (depth >= handoffParts.size()) {
            return true;
        }
        ui32 partIdx = handoffParts[depth];
        for (ui32 idx = 0; idx < handoffForParts[partIdx].size(); ++idx) {
            Y_VERIFY_DEBUG(depth < choosen.size());
            choosen[depth] = handoffForParts[partIdx][idx];
            bool isCorrect = true;
            for (ui32 prevDepth = 0; prevDepth < depth; ++prevDepth) {
                Y_VERIFY_DEBUG(prevDepth < choosen.size());
                isCorrect &= (choosen[depth] != choosen[prevDepth]);
            }
            bool hasAnswer = false;
            if (isCorrect) {
                hasAnswer = FindHandoffs(handoffForParts, handoffParts, choosenHandoffForParts, depth + 1);
            }
            if (hasAnswer) {
                return true;
            }
        }
        return false;
    }

    TString ConvertFoundPartsToString() const {
        TStringBuilder str;
        str << "[";
        for (auto &a : FoundParts) {
            str << a.ToString() << ' ';
        }
        str << ']';
        return str;
    }

    bool ContinueVPatchForMirror3dc() {
        constexpr ui32 DCCount = 3;
        constexpr ui32 VDiskByDC = 3;
        ui32 countByDC[DCCount] = {0, 0, 0};
        TPartPlacement diskByDC[DCCount][VDiskByDC];

        for (auto &[subgroupIdx, partId] : FoundParts) {
            ui32 dc = subgroupIdx / VDiskByDC;
            ui32 idx = countByDC[dc];
            diskByDC[dc][idx] = TPartPlacement{subgroupIdx, partId};
            countByDC[dc]++;
        }

        if (countByDC[0] && countByDC[1] && countByDC[2]) {
            SendDiffs({diskByDC[0][0], diskByDC[1][0], diskByDC[2][0]});
            return true;
        }

        ui32 x2Count = 0;
        for (ui32 dcIdx = 0; dcIdx < DCCount; ++dcIdx) {
            if (countByDC[dcIdx] >= 2) {
                x2Count++;
            }
        }
        if (x2Count < 2) {
            return false;
        }
        TStackVec<TPartPlacement, TypicalPartsInBlob> placements;
        for (ui32 dcIdx = 0; dcIdx < DCCount; ++dcIdx) {
            if (countByDC[dcIdx] >= 2) {
                placements.push_back(diskByDC[dcIdx][0]);
                placements.push_back(diskByDC[dcIdx][1]);
            }
        }
        SendDiffs(placements);
        return true;
    }

    bool ContinueVPatch() {
        A_LOG_DEBUG_S("BPPA09", "ContinueVPatch"
            << " ActorId# " << SelfId()
            << " Group# " << Info->GroupID
            << " OriginalBlob# " << OriginalId
            << " PatchedBlob# " << PatchedId
            << " FoundParts# " << ConvertFoundPartsToString()
            << " Deadline# " << Deadline);

        if (Info->Type.GetErasure() == TErasureType::ErasureMirror3dc) {
            return ContinueVPatchForMirror3dc();
        }

        TStackVec<bool, TypicalPartsInBlob> inPrimary;
        TStackVec<TStackVec<ui32, TypicalHandoffCount>, TypicalPartsInBlob> handoffForParts;

        inPrimary.resize(Info->Type.TotalPartCount());
        handoffForParts.resize(inPrimary.size());

        for (auto &[subgroupIdx, partId] : FoundParts) {
            if (subgroupIdx == partId) {
                inPrimary[subgroupIdx] = true;
            } else {
                handoffForParts[partId - 1].push_back(subgroupIdx);
            }
        }

        TStackVec<ui32, TypicalPartsInBlob> handoffParts;
        for (ui32 idx = 0; idx < inPrimary.size(); ++idx) {
            if (!inPrimary[idx]) {
                handoffParts.push_back(idx);
            }
        }

        TStackVec<ui32, TypicalHandoffCount> choosenHandoffForParts(handoffParts.size());
        if (handoffParts.size()) {
            bool find = FindHandoffs(handoffForParts, handoffParts, &choosenHandoffForParts);
            Y_VERIFY_DEBUG_S(find, "handoffParts# " << FormatList(handoffParts)
                    << " FoundParts# " << ConvertFoundPartsToString()
                    << " choosenHandoffForParts# " << FormatList(choosenHandoffForParts)
                    << " inPrimary# " << FormatList(inPrimary));
            if (!find) {
                Mon->VPatchContinueFailed->Inc();
                return false;
            }
        }

        SendDiffs(inPrimary, choosenHandoffForParts);
        return true;
    }

    void StopVPatch() {
        A_LOG_DEBUG_S("BPPA10", "StopVPatch"
            << " ActorId# " << SelfId()
            << " Group# " << Info->GroupID
            << " OriginalBlob# " << OriginalId
            << " PatchedBlob# " << PatchedId
            << " Deadline# " << Deadline);
        SendStopDiffs();
        ReceivedResponseFlags.assign(VDisks.size(), false);
    }

    bool CheckDiffs() {
        for (ui32 diffIdx = 0; diffIdx < DiffCount; ++diffIdx) {
            bool ok = Diffs[diffIdx].Offset < OriginalId.BlobSize();
            ok &= Diffs[diffIdx].Offset + Diffs[diffIdx].Buffer.size() <= OriginalId.BlobSize();
            if (!ok) {
                TStringBuilder str;
                str << "Diff at index " << diffIdx << " went beyound the blob;"
                    << " BlobSize# " << OriginalId.BlobSize()
                    << " DiffStart# " << Diffs[diffIdx].Offset
                    << " DiffEnd# " << Diffs[diffIdx].Offset + Diffs[diffIdx].Buffer.size() << Endl;
                ErrorReason = str;
                return false;
            }
        }
        for (ui32 diffIdx = 1; diffIdx < DiffCount; ++diffIdx) {
            ui32 prevIdx = diffIdx - 1;
            bool ok = Diffs[prevIdx].Offset + Diffs[prevIdx].Buffer.size() <= Diffs[diffIdx].Offset;
            if (!ok) {
                TStringBuilder str;
                str << "the end of the diff at index " << prevIdx << " righter than"
                    << " the start of the diff at index " << prevIdx << ';'
                    << " PrevDiffEnd# " << Diffs[prevIdx].Offset + Diffs[prevIdx].Buffer.size()
                    << " DiffStart# " << Diffs[diffIdx].Offset << Endl;
                ErrorReason = str;
                return false;
            }
        }
        return true;
    }

    void Bootstrap() {
        A_LOG_INFO_S("BPPA01", "bootstrap"
            << " ActorId# " << SelfId()
            << " Group# " << Info->GroupID
            << " DiffCount# " << DiffCount
            << " OriginalBlob# " << OriginalId
            << " PatchedBlob# " << PatchedId
            << " Deadline# " << Deadline
            << " RestartCounter# " << RestartCounter);
        TLogoBlobID truePatchedBlobId = PatchedId;
        bool result = true;
        if (Info->Type.ErasureFamily() == TErasureType::ErasureParityBlock) {
            result = TEvBlobStorage::TEvPatch::GetBlobIdWithSamePlacement(OriginalId, &truePatchedBlobId,
                    MaskForCookieBruteForcing, OriginalGroupId, Info->GroupID);
            if (result && PatchedId != truePatchedBlobId) {
                TStringBuilder str;
                str << "PatchedId wasn't from TEvBlobStorage::TEvPatch::GetBlobIdWithSamePlacement;";
                str << " OriginalId# " << OriginalId;
                str << " PatchedId# " << PatchedId;
                ErrorReason = str;
                ReplyAndDie(NKikimrProto::ERROR);
                return;
            }
        }

        if (!CheckDiffs()) {
            ReplyAndDie(NKikimrProto::ERROR);
            return;
        }

        bool isAllowedErasure = Info->Type.ErasureFamily() == TErasureType::ErasureParityBlock
                || Info->Type.GetErasure() == TErasureType::ErasureNone
                || Info->Type.GetErasure() == TErasureType::ErasureMirror3
                || Info->Type.GetErasure() == TErasureType::ErasureMirror3dc;
        if (result && isAllowedErasure && UseVPatch && OriginalGroupId == Info->GroupID) {
            StartVPatch();
        } else {
            StartFallback();
        }
    }

    STATEFN(NaiveState) {
        if (ProcessEvent(ev)) {
            return;
        }
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvGetResult, Handle);
            hFunc(TEvBlobStorage::TEvPutResult, Handle);
            IgnoreFunc(TEvBlobStorage::TEvVPatchResult);
        default:
            Y_FAIL("Received unknown event");
        };
    }

    STATEFN(MovedPatchState) {
        if (ProcessEvent(ev)) {
            return;
        }
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvVMovedPatchResult, Handle);
            IgnoreFunc(TEvBlobStorage::TEvVPatchResult);
        default:
            Y_FAIL("Received unknown event");
        };
    }

    STATEFN(VPatchState) {
        if (ProcessEvent(ev)) {
            return;
        }
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvVPatchFoundParts, Handle);
            hFunc(TEvBlobStorage::TEvVPatchResult, Handle);
        default:
            Y_FAIL("Received unknown event");
        };
    }
};

IActor* CreateBlobStorageGroupPatchRequest(const TIntrusivePtr<TBlobStorageGroupInfo> &info,
        const TIntrusivePtr<TGroupQueues> &state, const TActorId &source,
        const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon, TEvBlobStorage::TEvPatch *ev,
        ui64 cookie, NWilson::TTraceId traceId, TInstant now,
        TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters,
        bool useVPatch) {
    return new TBlobStorageGroupPatchRequest(info, state, source, mon, ev, cookie, std::move(traceId), now,
        storagePoolCounters, useVPatch);
}

}//NKikimr

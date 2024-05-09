#include "dsproxy.h"
#include "dsproxy_mon.h"
#include "root_cause.h"
#include <ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.h>
#include <ydb/core/util/stlog.h>

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

    ui32 SentStarts = 0;
    ui32 ReceivedFoundParts = 0;
    ui32 ErrorResponses = 0;
    ui32 ReceivedResults = 0;

    TStackVec<TPartPlacement, TypicalMaxPartsCount> FoundParts;
    TStackVec<bool, TypicalDisksInSubring> ReceivedResponseFlags;
    TStackVec<bool, TypicalDisksInSubring> EmptyResponseFlags;
    TStackVec<bool, TypicalDisksInSubring> ErrorResponseFlags;
    TStackVec<bool, TypicalDisksInSubring> ForceStopFlags;
    TBlobStorageGroupInfo::TVDiskIds VDisks;

    bool UseVPatch = false;
    bool IsGoodPatchedBlobId = false;
    bool IsAllowedErasure = false;
    bool IsSecured = false;

#define PATCH_LOG(priority, service, marker, msg, ...)                         \
        STLOG(priority, service, marker, msg,                                  \
                (ActorId, SelfId()),                                           \
                (Group, Info->GroupID),                                        \
                (DiffCount, DiffCount),                                        \
                (OriginalBlob, OriginalId),                                    \
                (PatchedBlob, PatchedId),                                     \
                (Deadline, Deadline),                                          \
                (RestartCounter, RestartCounter),                              \
                __VA_ARGS__)                                                   \
// PATCH_LOG

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
            ui64 cookie, NWilson::TSpan&& span, TInstant now,
            TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters,
            bool useVPatch = false)
        : TBlobStorageGroupRequestActor(info, state, mon, source, cookie,
                NKikimrServices::BS_PROXY_PATCH, false, {}, now, storagePoolCounters,
                ev->RestartCounter, std::move(span), std::move(ev->ExecutionRelay))
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
    {}

    void ReplyAndDie(NKikimrProto::EReplyStatus status) {
        PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA02, "ReplyAndDie",
                (Status, status),
                (ErrorReason, ErrorReason));

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

        PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA30, "Received TEvGetResult",
                (Status, result->Status),
                (ErrorReason, result->ErrorReason));

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
            ReplyAndDie(NKikimrProto::ERROR);
            return;
        }

        Buffer = result->Responses[0].Buffer.ConvertToString();
        ApplyDiffs();

        std::unique_ptr<TEvBlobStorage::TEvPut> put = std::make_unique<TEvBlobStorage::TEvPut>(PatchedId, Buffer, Deadline,
                NKikimrBlobStorage::AsyncBlob, TEvBlobStorage::TEvPut::TacticDefault);
        put->Orbit = std::move(Orbit);
        SendToProxy(std::move(put), OriginalId.Hash(), Span.GetTraceId());
    }

    void Handle(TEvBlobStorage::TEvPutResult::TPtr &ev) {
        TEvBlobStorage::TEvPutResult *result = ev->Get();
        Orbit = std::move(result->Orbit);

        PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA29, "Received TEvPutResult",
                (Status, result->Status),
                (ErrorReason, result->ErrorReason));

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
        NKikimrBlobStorage::TEvVMovedPatchResult &record = result->Record;
        PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA28, "Received TEvVMovedPatchResult",
                (Status, record.GetStatus()),
                (ErrorReason, record.GetErrorReason()),
                (VDiskId, VDiskIDFromVDiskID(record.GetVDiskID())));
        PullOutStatusFlagsAndFressSpace(record);
        Orbit = std::move(result->Orbit);

        ui64 expectedCookie = ((ui64)OriginalId.Hash() << 32) | PatchedId.Hash();
        bool incorrectCookie = ev->Cookie != expectedCookie;
        Y_ABORT_UNLESS(record.HasStatus());
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
            PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA27, "Start Naive strategy from hadling TEvVMovedPatchResult",
                    (Status, record.GetStatus()),
                    (ErrorReason, ErrorReason));
            StartNaivePatch();
            return;
        }

        ReplyAndDie(NKikimrProto::OK);
    }

    void Handle(TEvBlobStorage::TEvVPatchFoundParts::TPtr &ev) {
        ReceivedFoundParts++;

        NKikimrBlobStorage::TEvVPatchFoundParts &record = ev->Get()->Record;

        Y_ABORT_UNLESS(record.HasCookie());
        ui8 subgroupIdx = record.GetCookie();

        Y_ABORT_UNLESS(record.HasStatus());
        NKikimrProto::EReplyStatus status = record.GetStatus();

        TString errorReason;
        if (record.HasErrorReason()) {
            errorReason = record.GetErrorReason();
        }

        bool wasReceived = std::exchange(ReceivedResponseFlags[subgroupIdx], true);
        Y_ABORT_UNLESS(!wasReceived);

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

        PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA26, "Received VPatchFoundParts",
                (Status, status),
                (SubgroupIdx, (ui32)subgroupIdx),
                (ReceivedResults, static_cast<TString>(TStringBuilder() << ReceivedFoundParts << '/' << SentStarts)),
                (ErrorReason, errorReason));

        if (ReceivedFoundParts == SentStarts) {
            bool continueVPatch = VerifyPartPlacement();
            if (continueVPatch) {
                continueVPatch = ContinueVPatch();
            } else {
                PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA32, "Failed VerifyPartPlacement");
                Mon->VPatchPartPlacementVerifyFailed->Inc();
            }
            if (!continueVPatch) {
                PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA33, "Start Fallback strategy from hadling TEvVPatchFoundParts");
                StopVPatch();
                StartFallback();
            }
        }
    }

    void Handle(TEvBlobStorage::TEvVPatchResult::TPtr &ev) {
        NKikimrBlobStorage::TEvVPatchResult &record = ev->Get()->Record;

        Y_ABORT_UNLESS(record.HasCookie());
        ui8 subgroupIdx = record.GetCookie();
        if (ForceStopFlags[subgroupIdx]) {
            return; // ignore force stop response
        }
        ReceivedResults++;

        PullOutStatusFlagsAndFressSpace(record);
        Y_ABORT_UNLESS(record.HasStatus());
        NKikimrProto::EReplyStatus status = record.GetStatus();
        TString errorReason;
        if (record.HasErrorReason()) {
            errorReason = record.GetErrorReason();
        }

        PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA23, "Received VPatchResult",
                (Status, status),
                (SubgroupIdx, (ui32)subgroupIdx),
                (ReceivedResults, static_cast<TString>(TStringBuilder() << ReceivedResults << '/' << Info->Type.TotalPartCount())),
                (ErrorReason, errorReason));

        bool wasReceived = std::exchange(ReceivedResponseFlags[subgroupIdx], true);
        Y_ABORT_UNLESS(!wasReceived);

        if (status != NKikimrProto::OK) {
            PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA24, "Start Fallback strategy from handling VPatchResult",
                    (ReceivedResults, TStringBuilder() << ReceivedResults << '/' << Info->Type.TotalPartCount()));
            StartFallback();
            return;
        }

        if (ReceivedResults == Info->Type.TotalPartCount()) {
            PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA25, "Got all succesful responses, make own success response");
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
            PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA22, "VerifyPartPlacement {mirror-3-dc} found all 3 disks");
            return true;
        }

        ui32 x2Count = 0;
        for (ui32 dcIdx = 0; dcIdx < DCCount; ++dcIdx) {
            if (countByDC[dcIdx] >= 2) {
                x2Count++;
            }
        }
        PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA23, "VerifyPartPlacement {mirror-3-dc}",
                (X2Count, x2Count));
        return x2Count >= 2;
    }

    bool VerifyPartPlacement() const {
        if (Info->Type.GetErasure() == TErasureType::ErasureMirror3dc) {
            return VerifyPartPlacementForMirror3dc();
        } else {
            TSubgroupPartLayout layout;
            for (auto &placement : FoundParts) {
                PATCH_LOG(PRI_TRACE, BS_PROXY_PATCH, BPPA31, "Get part",
                        (SubgroupIdx, (ui32)placement.VDiskIdxInSubgroup),
                        (PartId, (ui32)placement.PartId));
                layout.AddItem(placement.VDiskIdxInSubgroup, placement.PartId - 1, Info->Type);
            }
            PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA21, "VerifyPartPlacement",
                    (EffectiveReplicas, layout.CountEffectiveReplicas(Info->Type)),
                    (TotalPartount, Info->Type.TotalPartCount()));
            return layout.CountEffectiveReplicas(Info->Type) == Info->Type.TotalPartCount();
        }
    }

    void SendStopDiffs() {
            PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA18, "Send stop diffs");
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVPatchDiff>> events;
        for (ui32 subgroupIdx = 0; subgroupIdx < VDisks.size(); ++subgroupIdx) {
            if (!ErrorResponseFlags[subgroupIdx] && !EmptyResponseFlags[subgroupIdx] && ReceivedResponseFlags[subgroupIdx]) {
                std::unique_ptr<TEvBlobStorage::TEvVPatchDiff> ev = std::make_unique<TEvBlobStorage::TEvVPatchDiff>(
                        OriginalId, PatchedId, VDisks[subgroupIdx], 0, Deadline, subgroupIdx);
                ev->SetForceEnd();
                ForceStopFlags[subgroupIdx] = true;
                events.emplace_back(std::move(ev));
                PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA19, "Send stop message",
                        (VDiskIdxInSubgroup, subgroupIdx),
                        (VDiskId, VDisks[subgroupIdx]));
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

            Y_ABORT_UNLESS(Info->GetIdxInSubgroup(VDisks[idxInSubgroup], OriginalId.Hash()) == idxInSubgroup);
            ui32 patchedIdxInSubgroup = Info->GetIdxInSubgroup(VDisks[idxInSubgroup], PatchedId.Hash());
            if (patchedIdxInSubgroup != idxInSubgroup) {
                // now only mirror3dc has this case (has 9 vdisks instead of 4 or 8)
                Y_ABORT_UNLESS(Info->Type.GetErasure() == TErasureType::ErasureMirror3dc);
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

                PATCH_LOG(PRI_TRACE, BS_PROXY_PATCH, BPPA35, "Add Diff",
                        (Offset, diff.Offset),
                        (BufferSize, diff.Buffer.Size()));
            }

            for (const TPartPlacement &parity : parityPlacements) {
                ev->AddXorReceiver(VDisks[parity.VDiskIdxInSubgroup], parity.PartId);
            }
            PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA20, "Send TEvVPatchDiff",
                    (VDiskIdxInSubgroup, idxInSubgroup),
                    (PatchedVDiskIdxInSubgroup, patchedIdxInSubgroup),
                    (PartId, (ui64)partPlacement.PartId),
                    (DiffsForPart, diffsForPart.size()),
                    (ParityPlacements, parityPlacements.size()),
                    (WaitedXorDiffs, waitedXorDiffs));
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
        PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA09, "Start Moved strategy",
                (SentStarts, SentStarts));
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
        PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA07, "Start Naive strategy");
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
        if (WithMovingPatchRequestToStaticNode && UseVPatch && !IsSecured) {
            PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA05, "Start Moved strategy from fallback");
            StartMovedPatch();
        } else {
            PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA06, "Start Naive strategy from fallback",
                    (WithMovingPatchRequestToStaticNode, WithMovingPatchRequestToStaticNode),
                    (UseVPatch, UseVPatch));
            StartNaivePatch();
        }
    }

    void StartVPatch() {
        Become(&TThis::VPatchState);

        Info->PickSubgroup(OriginalId.Hash(), &VDisks, nullptr);
        ReceivedResponseFlags.assign(VDisks.size(), false);
        ErrorResponseFlags.assign(VDisks.size(), false);
        EmptyResponseFlags.assign(VDisks.size(), false);
        ForceStopFlags.assign(VDisks.size(), false);

        TDeque<std::unique_ptr<TEvBlobStorage::TEvVPatchStart>> events;

        for (ui32 idx = 0; idx < VDisks.size(); ++idx) {
            std::unique_ptr<TEvBlobStorage::TEvVPatchStart> ev = std::make_unique<TEvBlobStorage::TEvVPatchStart>(
                    OriginalId, PatchedId, VDisks[idx], Deadline, idx, true);
            events.emplace_back(std::move(ev));
            SentStarts++;
        }

        PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA08, "Start VPatch strategy",
                (SentStarts, SentStarts));

        SendToQueues(events, false);
    }

    bool FindHandoffs(const TStackVec<TStackVec<ui32, TypicalHandoffCount>, TypicalPartsInBlob>& handoffForParts,
            const TStackVec<ui32, TypicalPartsInBlob> &handoffParts,
            TStackVec<ui32, TypicalHandoffCount> *choosenHandoffForParts, ui8 depth = 0)
    {
        auto &choosen = *choosenHandoffForParts;
        Y_DEBUG_ABORT_UNLESS(choosen.size() == handoffParts.size());
        if (depth >= handoffParts.size()) {
            return true;
        }
        ui32 partIdx = handoffParts[depth];
        for (ui32 idx = 0; idx < handoffForParts[partIdx].size(); ++idx) {
            Y_DEBUG_ABORT_UNLESS(depth < choosen.size());
            choosen[depth] = handoffForParts[partIdx][idx];
            bool isCorrect = true;
            for (ui32 prevDepth = 0; prevDepth < depth; ++prevDepth) {
                Y_DEBUG_ABORT_UNLESS(prevDepth < choosen.size());
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
        PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA10, "Continue VPatch {mirror-3-dc}");
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
            PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA11, "Found disks {mirror-3-dc} on each dc",
                    (DiskFromFirstDC, diskByDC[0][0].ToString()),
                    (DiskFromSecondDC, diskByDC[0][0].ToString()),
                    (DiskFromThirdDC, diskByDC[2][0].ToString()));
            SendDiffs({diskByDC[0][0], diskByDC[1][0], diskByDC[2][0]});
            return true;
        }
        PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA12, "Didn't find disks {mirror-3-dc} on each dc");

        ui32 x2Count = 0;
        for (ui32 dcIdx = 0; dcIdx < DCCount; ++dcIdx) {
            if (countByDC[dcIdx] >= 2) {
                x2Count++;
            }
        }
        if (x2Count < 2) {
            PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA13, "Didn't find disks {mirror-3-dc}");
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
        PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA14, "Found disks {mirror-3-dc} x2 mode",
                (FirstDiskFromFirstDC, placements[0]),
                (SecondDiskFromFirstDC, placements[1]),
                (FirstDiskFromSecondDC, placements[2]),
                (SecondDiskFromSecondDC, placements[3]));
        return true;
    }

    bool ContinueVPatch() {
        PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA15, "Continue VPatch strategy",
                (FoundParts, ConvertFoundPartsToString()));

        if (Info->Type.GetErasure() == TErasureType::ErasureMirror3dc) {
            return ContinueVPatchForMirror3dc();
        }

        TStackVec<bool, TypicalPartsInBlob> inPrimary;
        TStackVec<TStackVec<ui32, TypicalHandoffCount>, TypicalPartsInBlob> handoffForParts;

        inPrimary.resize(Info->Type.TotalPartCount());
        handoffForParts.resize(inPrimary.size());

        for (auto &[subgroupIdx, partId] : FoundParts) {
            if (subgroupIdx == partId - 1) {
                inPrimary[partId - 1] = true;
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
            PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA16, "Find handoff parts",
                    (HandoffParts, FormatList(handoffParts)),
                    (FoundParts, ConvertFoundPartsToString()),
                    (choosenHandoffForParts, FormatList(choosenHandoffForParts)),
                    (IsPrimary, FormatList(inPrimary)));
            if (!find) {
                Mon->VPatchContinueFailed->Inc();
                return false;
            }
        }

        SendDiffs(inPrimary, choosenHandoffForParts);
        return true;
    }

    void StopVPatch() {
        PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA17, "Stop VPatch strategy");
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
        PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA01, "Actor bootstrapped");

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

        IsSecured = (Info->GetEncryptionMode() != TBlobStorageGroupInfo::EEM_NONE);

        IsGoodPatchedBlobId = result;
        IsAllowedErasure = Info->Type.ErasureFamily() == TErasureType::ErasureParityBlock
                || Info->Type.GetErasure() == TErasureType::ErasureNone
                || Info->Type.GetErasure() == TErasureType::ErasureMirror3dc;
        if (IsGoodPatchedBlobId && IsAllowedErasure && UseVPatch && OriginalGroupId == Info->GroupID && !IsSecured) {
            PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA03, "Start VPatch strategy from bootstrap");
            StartVPatch();
        } else {
            PATCH_LOG(PRI_DEBUG, BS_PROXY_PATCH, BPPA04, "Start Fallback strategy from bootstrap",
                    (IsGoodPatchedBlobId, IsGoodPatchedBlobId),
                    (IsAllowedErasure, IsAllowedErasure),
                    (UseVPatch, UseVPatch),
                    (IsSameGroup, OriginalGroupId == Info->GroupID),
                    (IsSecured, IsSecured));
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
            Y_ABORT("Received unknown event");
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
            Y_ABORT("Received unknown event");
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
            Y_ABORT("Received unknown event");
        };
    }
};

IActor* CreateBlobStorageGroupPatchRequest(const TIntrusivePtr<TBlobStorageGroupInfo> &info,
        const TIntrusivePtr<TGroupQueues> &state, const TActorId &source,
        const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon, TEvBlobStorage::TEvPatch *ev,
        ui64 cookie, NWilson::TTraceId traceId, TInstant now,
        TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters,
        bool useVPatch) {
    NWilson::TSpan span(TWilson::BlobStorage, std::move(traceId), "DSProxy.Patch");
    if (span) {
        span.Attribute("event", ev->ToString());
    }

    return new TBlobStorageGroupPatchRequest(info, state, source, mon, ev, cookie, std::move(span), now,
        storagePoolCounters, useVPatch);
}

}//NKikimr

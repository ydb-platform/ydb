#pragma once

#include "dsproxy.h"
#include "dsproxy_blackboard.h"
#include "dsproxy_cookies.h"
#include "dsproxy_mon.h"
#include "dsproxy_strategy_accelerate_put.h"
#include "dsproxy_strategy_accelerate_put_m3dc.h"
#include "dsproxy_strategy_restore.h"
#include "dsproxy_strategy_put_m3dc.h"
#include "dsproxy_strategy_put_m3of4.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <util/generic/set.h>

namespace NKikimr {

class TStrategyBase;

class TPutImpl {
public:
    using TPutResultVec = TBatchedVec<std::pair<ui64, std::unique_ptr<TEvBlobStorage::TEvPutResult>>>;

private:
    TBlobStorageGroupInfo::TServiceIds VDisksSvc;
    TBlobStorageGroupInfo::TVDiskIds VDisksId;

    TInstant Deadline;
    const TIntrusivePtr<TBlobStorageGroupInfo> Info;

    TBlackboard Blackboard;
    TBatchedVec<bool> IsDone;
    TBatchedVec<bool> WrittenBeyondBarrier;
    TStorageStatusFlags StatusFlags;
    float ApproximateFreeSpaceShare;
    TIntrusivePtr<TBlobStorageGroupProxyMon> Mon;
    ui32 HandoffPartsSent = 0;
    ui32 VPutRequests = 0;
    ui32 VPutResponses = 0;
    ui32 VMultiPutRequests = 0;
    ui32 VMultiPutResponses = 0;
    bool AtLeastOneResponseWasNotOk = false;
    bool EnableRequestMod3x3ForMinLatecy = false;

    ui64 DoneBlobs = 0;

    const TEvBlobStorage::TEvPut::ETactic Tactic;

    struct TBlobInfo {
        TLogoBlobID BlobId;
        TRope Buffer;
        ui64 BufferSize;
        TActorId Recipient;
        ui64 Cookie;
        NLWTrace::TOrbit Orbit;
        bool Replied = false;
        std::vector<std::pair<ui64, ui32>> ExtraBlockChecks;
        NWilson::TSpan Span;
        std::shared_ptr<TEvBlobStorage::TExecutionRelay> ExecutionRelay;

        TBlobInfo(TLogoBlobID id, TRope&& buffer, TActorId recipient, ui64 cookie, NWilson::TTraceId traceId,
                NLWTrace::TOrbit&& orbit, std::vector<std::pair<ui64, ui32>> extraBlockChecks, bool single,
                std::shared_ptr<TEvBlobStorage::TExecutionRelay> executionRelay)
            : BlobId(id)
            , Buffer(std::move(buffer))
            , BufferSize(Buffer.size())
            , Recipient(recipient)
            , Cookie(cookie)
            , Orbit(std::move(orbit))
            , ExtraBlockChecks(std::move(extraBlockChecks))
            , Span(single ? NWilson::TSpan() : NWilson::TSpan(TWilson::BlobStorage, std::move(traceId), "DSProxy.Put.Blob"))
            , ExecutionRelay(std::move(executionRelay))
        {}

        void Output(IOutputStream& s) const {
            s << BlobId;
            if (!ExtraBlockChecks.empty()) {
                s << "{";
                for (auto it = ExtraBlockChecks.begin(); it != ExtraBlockChecks.end(); ++it) {
                    if (it != ExtraBlockChecks.begin()) {
                        s << ", ";
                    }
                    s << it->first << ":" << it->second;
                }
                s << "}";
            }
        }

        TString ToString() const {
            TStringStream s;
            Output(s);
            return s.Str();
        }
    };

    TBatchedVec<TBlobInfo> Blobs;

    friend class TBlobStorageGroupPutRequest;

    TStackVec<bool, MaxBatchedPutRequests * TypicalDisksInSubring> ReceivedVPutResponses;
    TStackVec<bool, MaxBatchedPutRequests * TypicalDisksInSubring> ReceivedVMultiPutResponses;

    bool IsInitialized = false;

    TString ErrorDescription;

    friend void ::Out<TBlobInfo>(IOutputStream&, const TBlobInfo&);

public:
    TPutImpl(const TIntrusivePtr<TBlobStorageGroupInfo> &info, const TIntrusivePtr<TGroupQueues> &state,
            TEvBlobStorage::TEvPut *ev, const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon,
            bool enableRequestMod3x3ForMinLatecy, TActorId recipient, ui64 cookie, NWilson::TTraceId traceId)
        : Deadline(ev->Deadline)
        , Info(info)
        , Blackboard(info, state, ev->HandleClass, NKikimrBlobStorage::EGetHandleClass::AsyncRead, false)
        , IsDone(1)
        , WrittenBeyondBarrier(1)
        , StatusFlags(0)
        , ApproximateFreeSpaceShare(0.f)
        , Mon(mon)
        , EnableRequestMod3x3ForMinLatecy(enableRequestMod3x3ForMinLatecy)
        , Tactic(ev->Tactic)
    {
        Blobs.emplace_back(ev->Id, TRope(ev->Buffer), recipient, cookie, std::move(traceId), std::move(ev->Orbit),
            std::move(ev->ExtraBlockChecks), true, std::move(ev->ExecutionRelay));

        auto& blob = Blobs.back();
        LWPROBE(DSProxyBlobPutTactics, blob.BlobId.TabletID(), Info->GroupID, blob.BlobId.ToString(), Tactic,
            NKikimrBlobStorage::EPutHandleClass_Name(GetPutHandleClass()));
    }

    TPutImpl(const TIntrusivePtr<TBlobStorageGroupInfo> &info, const TIntrusivePtr<TGroupQueues> &state,
            TBatchedVec<TEvBlobStorage::TEvPut::TPtr> &events, const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon,
            NKikimrBlobStorage::EPutHandleClass putHandleClass, TEvBlobStorage::TEvPut::ETactic tactic,
            bool enableRequestMod3x3ForMinLatecy)
        : Deadline(TInstant::Zero())
        , Info(info)
        , Blackboard(info, state, putHandleClass, NKikimrBlobStorage::EGetHandleClass::AsyncRead, false)
        , IsDone(events.size())
        , WrittenBeyondBarrier(events.size())
        , StatusFlags(0)
        , ApproximateFreeSpaceShare(0.f)
        , Mon(mon)
        , EnableRequestMod3x3ForMinLatecy(enableRequestMod3x3ForMinLatecy)
        , Tactic(tactic)
    {
        Y_VERIFY(events.size(), "TEvPut vector is empty");

        for (auto &ev : events) {
            auto& msg = *ev->Get();
            Y_VERIFY(msg.HandleClass == putHandleClass);
            Y_VERIFY(msg.Tactic == tactic);
            Blobs.emplace_back(msg.Id, TRope(msg.Buffer), ev->Sender, ev->Cookie, std::move(ev->TraceId),
                std::move(msg.Orbit), std::move(msg.ExtraBlockChecks), false, std::move(msg.ExecutionRelay));
            Deadline = Max(Deadline, msg.Deadline);

            auto& blob = Blobs.back();
            LWPROBE(DSProxyBlobPutTactics, blob.BlobId.TabletID(), Info->GroupID, blob.BlobId.ToString(), Tactic,
                NKikimrBlobStorage::EPutHandleClass_Name(GetPutHandleClass()));
        }

        Y_VERIFY(Blobs.size());
        Y_VERIFY(Blobs.size() <= MaxBatchedPutRequests);
    }

    NKikimrBlobStorage::EPutHandleClass GetPutHandleClass() const {
        return Blackboard.PutHandleClass;
    }

    ui32 GetHandoffPartsSent() const {
        return HandoffPartsSent;
    }

    template <typename TVPutEvent>
    void GenerateInitialRequests(TLogContext &logCtx, TBatchedVec<TStackVec<TRope, 8>>& partSets,
            TDeque<std::unique_ptr<TVPutEvent>> &outVPuts) {
        Y_UNUSED(logCtx);
        Y_VERIFY_S(partSets.size() == Blobs.size(), "partSets.size# " << partSets.size()
                << " Blobs.size# " << Blobs.size());
        const ui32 totalParts = Info->Type.TotalPartCount();
        for (ui64 blobIdx = 0; blobIdx < Blobs.size(); ++blobIdx) {
            TBlobInfo& blob = Blobs[blobIdx];
            Blackboard.RegisterBlobForPut(blob.BlobId, &blob.ExtraBlockChecks, &blob.Span);
            for (ui32 i = 0; i < totalParts; ++i) {
                if (Info->Type.PartSize(TLogoBlobID(blob.BlobId, i + 1))) {
                    Blackboard.AddPartToPut(blob.BlobId, i, TRope(partSets[blobIdx][i]));
                }
            }
            Blackboard.MarkBlobReadyToPut(blob.BlobId, blobIdx);
        }

        TPutResultVec putResults;
        bool workDone = Step(logCtx, outVPuts, putResults);
        IsInitialized = true;
        Y_VERIFY(!outVPuts.empty());
        Y_VERIFY(putResults.empty());
        Y_VERIFY(workDone);
    }

    template <typename TEvent>
    bool MarkRequest(const TEvent &event, ui32 orderNumber) {
        constexpr bool isVPut = std::is_same_v<TEvent, TEvBlobStorage::TEvVPutResult>;
        constexpr bool isVMultiPut = std::is_same_v<TEvent, TEvBlobStorage::TEvVMultiPutResult>;
        static_assert(isVPut || isVMultiPut);

        using TCookie = std::conditional_t<isVPut, TBlobCookie, TVMultiPutCookie>;

        auto responses = isVPut ? ReceivedVPutResponses : ReceivedVMultiPutResponses;
        auto putType = std::is_same_v<TCookie, TBlobCookie> ? "TEvVPut" : "TEvVMultiPut";

        const auto &record = event.Record;
        if (!record.HasCookie()) {
            ErrorDescription = TStringBuilder() << putType << " doesn't have cookie";
            return true;
        }
        TCookie cookie(record.GetCookie());
        if (cookie.GetVDiskOrderNumber() != orderNumber) {
            ErrorDescription = TStringBuilder() << putType << " has wrong cookie; unexpected orderNumber;"
                    << " expected# " << orderNumber
                    << " received# " << cookie.GetVDiskOrderNumber()
                    << " cookie# " << ui64(cookie);
            return true;
        }

        ui64 requestIdx = cookie.GetRequestIdx();
        if (responses[requestIdx]) {
            ErrorDescription =  TStringBuilder() << putType << "is recieved twice"
                    << " Event# " << event.ToString()
                    << " State# " << DumpFullState();
            return true;
        }
        responses[requestIdx] = true;
        return false;
    }

    void PackToStringImpl(TStringBuilder &) {
    }

    template <typename TArg, typename... TArgs>
    void PackToStringImpl(TStringBuilder &builder, TArg arg, TArgs... args) {
        builder << arg;
        PackToStringImpl(builder, args...);
    }

    template <typename... TArgs>
    TString PackToString(TArgs... args) {
        TStringBuilder builder;
        PackToStringImpl(builder, args...);
        return builder;
    }

    template <typename TPutRecord, typename ...TArgs>
    bool ProcessOneVPutResult(TLogContext &logCtx, const TPutRecord &record, TVDiskID vDiskId, ui32 orderNumber,
            TArgs ...resultTypeArgs)
    {
        Y_VERIFY(record.HasStatus());
        Y_VERIFY(record.HasBlobID());
        Y_VERIFY(record.HasCookie());
        NKikimrProto::EReplyStatus status = record.GetStatus();
        TLogoBlobID blobId = LogoBlobIDFromLogoBlobID(record.GetBlobID());
        ui32 diskIdx = Info->GetTopology().GetIdxInSubgroup(vDiskId, blobId.Hash());

        TBlobCookie cookie(record.GetCookie());
        if (cookie.GetPartId() != blobId.PartId()) {
            ErrorDescription = TStringBuilder()
                    << PackToString(resultTypeArgs...) << " has wrong cookie; unexpected PartId;"
                    << " expected# " << blobId.PartId()
                    << " received# " << cookie.GetPartId()
                    << " cookie# " << ui64(cookie);
            return true;
        }
        if (cookie.GetVDiskOrderNumber() != orderNumber) {
            ErrorDescription = TStringBuilder()
                    << PackToString(resultTypeArgs...) << " has wrong cookie; unexpected orderNumber;"
                    << " expected# " << orderNumber
                    << " received# " << cookie.GetVDiskOrderNumber()
                    << " cookie# " << ui64(cookie);
            return true;
        }

        ui64 blobIdx = cookie.GetBlobIdx();
        ui32 partIdx = blobId.PartId() - 1;

        A_LOG_LOG_SX(logCtx, false, PriorityForStatusInbound(status), "BPP11",
            "Got " << PackToString(resultTypeArgs...)
            << " part# " << partIdx
            << " diskIdx# " << diskIdx
            << " vDiskId# " << vDiskId
            << " blob# " << blobId
            << " status# " << status);

        if (IsDone.size() <= blobIdx) {
            ErrorDescription = TStringBuilder()
                    << PackToString(resultTypeArgs...) << " has wrong cookie; unexpected blobIdx;"
                    << " received# " << cookie.GetVDiskOrderNumber()
                    << " blobCount# " << IsDone.size()
                    << " cookie# " << ui64(cookie);
            return true;
        }

        if (IsDone[blobIdx]) {
            return false;
        }
        switch (status) {
            case NKikimrProto::ERROR:
            case NKikimrProto::VDISK_ERROR_STATE:
            case NKikimrProto::OUT_OF_SPACE:
                Blackboard.AddErrorResponse(blobId, orderNumber);
                AtLeastOneResponseWasNotOk = true;
                break;
            case NKikimrProto::OK:
            case NKikimrProto::ALREADY:
                Blackboard.AddPutOkResponse(blobId, orderNumber);
                WrittenBeyondBarrier[blobIdx] = record.GetWrittenBeyondBarrier();
                break;
            default:
                ErrorDescription = TStringBuilder() << "Unexpected status# " << status;
                return true;
        }
        return false;
    }

    template <typename TVPutEvent, typename TVPutEventResult>
    void OnVPutEventResult(TLogContext &logCtx, TActorId sender, TVPutEventResult &ev,
            TDeque<std::unique_ptr<TVPutEvent>> &outVPutEvents, TPutResultVec &outPutResults)
    {
        constexpr bool isVPut = std::is_same_v<TVPutEvent, TEvBlobStorage::TEvVPut>;
        constexpr bool isVMultiPut = std::is_same_v<TVPutEvent, TEvBlobStorage::TEvVMultiPut>;
        static_assert(isVPut || isVMultiPut);
        auto putType = isVPut ? "TEvVPut" : "TEvVMultiPut";

        auto &record = ev.Record;
        Y_VERIFY(record.HasStatus());
        NKikimrProto::EReplyStatus status = record.GetStatus();
        Y_VERIFY(status != NKikimrProto::BLOCKED && status != NKikimrProto::RACE && status != NKikimrProto::DEADLINE);
        if (record.HasStatusFlags()) {
            StatusFlags.Merge(record.GetStatusFlags());
        }
        if (record.HasApproximateFreeSpaceShare()) {
            float share = record.GetApproximateFreeSpaceShare();
            if (ApproximateFreeSpaceShare == 0.f || share < ApproximateFreeSpaceShare) {
                ApproximateFreeSpaceShare = share;
            }
        }

        Y_VERIFY(record.HasVDiskID());
        TVDiskID vDiskId = VDiskIDFromVDiskID(record.GetVDiskID());
        TVDiskIdShort shortId(vDiskId);
        ui32 orderNumber = Info->GetOrderNumber(shortId);

        Y_VERIFY_S(!MarkRequest(ev, orderNumber), ErrorDescription);

        if constexpr (isVPut) {
            VPutResponses++;
            bool error = ProcessOneVPutResult(logCtx, ev.Record, vDiskId, orderNumber, "TEvVPutResult");
            Y_VERIFY_S(!error, ErrorDescription);
        } else {
            TVMultiPutCookie cookie(record.GetCookie());
            if (cookie.GetItemCount() != record.ItemsSize()) {
                ErrorDescription = TStringBuilder() << putType << " has wrong cookie; unexpected ItemCount;"
                        << " expected# " << record.ItemsSize()
                        << " received# " << cookie.GetItemCount()
                        << " cookie# " << ui64(cookie);
                Y_VERIFY_S(false, ErrorDescription);
                return;
            }

            VMultiPutResponses++;
            for (ui32 itemIdx = 0; itemIdx < record.ItemsSize(); ++itemIdx) {
                bool error = ProcessOneVPutResult(logCtx, ev.Record.GetItems(itemIdx), vDiskId, orderNumber,
                        "TEvVMultiPutResult item# ", itemIdx);
                Y_VERIFY_S(!error, ErrorDescription);
            }
            A_LOG_LOG_SX(logCtx, false, PriorityForStatusInbound(status), "BPP23", "Got VMultiPutResult "
                << " vDiskId# " << vDiskId
                << " from# " << sender
                << " status# " << status
                << " vMultiPutResult# " << ev.ToString());
        }

        const auto &requests = isVPut ? VPutRequests : VMultiPutRequests;
        const auto &responses = isVPut ? VPutResponses : VMultiPutResponses;

        if (Info->Type.GetErasure() == TBlobStorageGroupType::Erasure4Plus2Block
                && !AtLeastOneResponseWasNotOk
                && requests >= 6 && responses <= 4) {
            // 6 == Info->Type.TotalPartCount()
            // There is no need to run strategy since no new information is recieved
            return;
        }

        Step(logCtx, outVPutEvents, outPutResults);
        Y_VERIFY_S(DoneBlobs == Blobs.size() || requests > responses,
                "No put result while"
                << " Type# " << putType
                << " DoneBlobs# " << DoneBlobs
                << " requests# " << requests
                << " responses# " << responses
                << " Blackboard# " << Blackboard.ToString());
    }

    void PrepareReply(NKikimrProto::EReplyStatus status, TLogContext &logCtx, TString errorReason,
            TPutResultVec &outPutResults);
    void PrepareReply(TLogContext &logCtx, TString errorReason, TBatchedVec<TBlackboard::TBlobStates::value_type*>& finished,
            TPutResultVec &outPutResults);
    void PrepareOneReply(NKikimrProto::EReplyStatus status, TLogoBlobID blobId, ui64 blobIdx, TLogContext &logCtx,
            TString errorReason, TPutResultVec &outPutResults);

    ui64 GetTimeToAccelerateNs(TLogContext &logCtx);

    template <typename TVPutEvent>
    void Accelerate(TLogContext &logCtx, TDeque<std::unique_ptr<TVPutEvent>> &outVPuts) {
        Blackboard.ChangeAll();
        switch (Info->Type.GetErasure()) {
            case TBlobStorageGroupType::ErasureMirror3dc:
                Blackboard.RunStrategy(logCtx, TAcceleratePut3dcStrategy(Tactic, EnableRequestMod3x3ForMinLatecy));
                break;
            case TBlobStorageGroupType::ErasureMirror3of4:
                Blackboard.RunStrategy(logCtx, TPut3of4Strategy(Tactic, true));
                break;
            default:
                Blackboard.RunStrategy(logCtx, TAcceleratePutStrategy());
                break;
        }
        PrepareVPuts(logCtx, outVPuts);
    }

    TString DumpFullState() const;

    bool MarkBlobAsSent(ui64 blobIdx);
    bool MarkBlobAsSent(TMap<TLogoBlobID, TBlobState>::iterator it);

    TString ToString() const;

protected:
    bool RunStrategies(TLogContext &logCtx, TPutResultVec &outPutResults);
    bool RunStrategy(TLogContext &logCtx, const IStrategy& strategy, TPutResultVec &outPutResults);

    // Returns true if there are additional requests to send
    template <typename TVPutEvent>
    bool Step(TLogContext &logCtx, TDeque<std::unique_ptr<TVPutEvent>> &outVPuts,
            TPutResultVec &outPutResults) {
        if (!RunStrategies(logCtx, outPutResults)) {
            const ui32 numRequests = outVPuts.size();
            PrepareVPuts(logCtx, outVPuts);
            return outVPuts.size() > numRequests;
        } else {
            Y_VERIFY(outPutResults.size());
            PrepareVPuts(logCtx, outVPuts);
            return false;
        }
    }


    template <typename TVPutEvent>
    void PrepareVPuts(TLogContext &logCtx, TDeque<std::unique_ptr<TVPutEvent>> &outVPutEvents) {
        constexpr bool isVPut = std::is_same_v<TEvBlobStorage::TEvVPut, TVPutEvent>;
        constexpr bool isVMultiPut = std::is_same_v<TEvBlobStorage::TEvVMultiPut, TVPutEvent>;
        static_assert(isVPut || isVMultiPut);

        const ui32 diskCount = Blackboard.GroupDiskRequests.DiskRequestsForOrderNumber.size();
        for (ui32 diskOrderNumber = 0; diskOrderNumber < diskCount; ++diskOrderNumber) {
            const TDiskRequests &requests = Blackboard.GroupDiskRequests.DiskRequestsForOrderNumber[diskOrderNumber];
            ui32 endIdx = requests.PutsToSend.size();
            ui32 beginIdx = requests.FirstUnsentPutIdx;

            if (beginIdx >= endIdx) {
                continue;
            }

            TVDiskID vDiskId = Info->GetVDiskId(diskOrderNumber);

            if constexpr (isVMultiPut) {
                ui64 cookie = TVMultiPutCookie(diskOrderNumber, endIdx - beginIdx, VMultiPutRequests);
                auto vMultiPut = std::make_unique<TEvBlobStorage::TEvVMultiPut>(vDiskId, Deadline, Blackboard.PutHandleClass,
                    false, &cookie);
                vMultiPut->ReservePayload(endIdx - beginIdx);
                outVPutEvents.push_back(std::move(vMultiPut));
                ++VMultiPutRequests;
                ReceivedVMultiPutResponses.push_back(false);
            }

            for (ui32 idx = beginIdx; idx < endIdx; ++idx) {
                const TDiskPutRequest &put = requests.PutsToSend[idx];
                ui32 counter = isVPut ? VPutRequests : VMultiPutRequests;
                ui64 cookie = TBlobCookie(diskOrderNumber, put.BlobIdx, put.Id.PartId(), counter);

                Y_VERIFY_DEBUG(Info->Type.GetErasure() != TBlobStorageGroupType::ErasureMirror3of4 ||
                    put.Id.PartId() != 3 || put.Buffer.IsEmpty());

                if constexpr (isVPut) {
                    auto vPut = std::make_unique<TEvBlobStorage::TEvVPut>(put.Id, put.Buffer, vDiskId, false, &cookie,
                            Deadline, Blackboard.PutHandleClass);
                    auto& record = vPut->Record;
                    if (put.ExtraBlockChecks) {
                        for (const auto& [tabletId, generation] : *put.ExtraBlockChecks) {
                            auto *p = record.AddExtraBlockChecks();
                            p->SetTabletId(tabletId);
                            p->SetGeneration(generation);
                        }
                    }
                    R_LOG_DEBUG_SX(logCtx, "BPP20", "Send put to orderNumber# " << diskOrderNumber << " idx# " << idx
                            << " vPut# " << vPut->ToString());
                    outVPutEvents.push_back(std::move(vPut));
                    ++VPutRequests;
                    ReceivedVPutResponses.push_back(false);
                } else if constexpr (isVMultiPut) {
                    // this request MUST originate from the TEvPut, so the Span field must be filled in
                    Y_VERIFY(put.Span);
                    outVPutEvents.back()->AddVPut(put.Id, TRcBuf(TRope(put.Buffer)), &cookie, put.ExtraBlockChecks, put.Span->GetTraceId());
                }

                if (put.IsHandoff) {
                    ++HandoffPartsSent;
                }

                LWPROBE(DSProxyPutVPut, put.Id.TabletID(), Info->GroupID, put.Id.Channel(), put.Id.PartId(),
                        put.Id.ToString(), Tactic,
                        NKikimrBlobStorage::EPutHandleClass_Name(Blackboard.PutHandleClass),
                        put.Id.BlobSize(), put.Buffer.size(), Info->GetFailDomainOrderNumber(vDiskId),
                        NKikimrBlobStorage::EVDiskQueueId_Name(TGroupQueues::TVDisk::TQueues::VDiskQueueId(*outVPutEvents.back())),
                        Blackboard.GroupQueues->GetPredictedDelayNsForEvent(*outVPutEvents.back(), Info->GetTopology()) * 0.000001);
            }

            if constexpr (isVMultiPut) {
                R_LOG_DEBUG_SX(logCtx, "BPP39", "Send put to orderNumber# " << diskOrderNumber
                        << " count# " << outVPutEvents.back()->Record.ItemsSize()
                        << " vMultiPut# " << outVPutEvents.back()->ToString());
            }

            Blackboard.GroupDiskRequests.DiskRequestsForOrderNumber[diskOrderNumber].FirstUnsentPutIdx = endIdx;
        }
    }
}; //TPutImpl

}//NKikimr

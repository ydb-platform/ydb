#include "dsproxy.h"
#include "dsproxy_mon.h"
#include "root_cause.h"
#include "dsproxy_put_impl.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>

#include <ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>

#include <util/generic/ymath.h>
#include <util/system/datetime.h>
#include <util/system/hp_timer.h>

LWTRACE_USING(BLOBSTORAGE_PROVIDER);

namespace NKikimr {

struct TEvAccelerate : public TEventLocal<TEvAccelerate, TEvBlobStorage::EvAccelerate> {
    ui64 CauseIdx;

    TEvAccelerate(ui64 causeIdx)
        : CauseIdx(causeIdx)
    {}
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PUT request
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TBlobStorageGroupPutRequest : public TBlobStorageGroupRequestActor<TBlobStorageGroupPutRequest> {
    TPutImpl PutImpl;
    TRootCause RootCauseTrack;

    const TDuration MaxQuantumDuration = TDuration::MicroSeconds(500);
    const ui32 MaxBytesToEncryptAtOnce = 256_KB;
    const ui32 MaxBytesToSplitAtOnce = 256_KB;

    ui32 BlobsEncrypted = 0;
    ui32 CurrentEncryptionOffset = 0;
    ui32 BlobsSplit = 0;
    TErasureSplitContext ErasureSplitContext = TErasureSplitContext::Init(MaxBytesToSplitAtOnce);
    TBatchedVec<TStackVec<TRope, TypicalPartsInBlob>> PartSets;

    TStackVec<ui64, TypicalDisksInGroup> WaitingVDiskResponseCount;
    ui64 WaitingVDiskCount = 0;

    bool IsManyPuts = false;

    TInstant Deadline;
    ui64 RequestsSent = 0;
    ui64 ResponsesReceived = 0;
    ui64 MaxSaneRequests = 0;
    ui64 ResponsesSent = 0;

    TInstant StartTime;
    NKikimrBlobStorage::EPutHandleClass HandleClass;

    i64 ReportedBytes;

    TBlobStorageGroupProxyTimeStats TimeStats;
    bool TimeStatsEnabled;

    const TEvBlobStorage::TEvPut::ETactic Tactic;

    TDiskResponsivenessTracker::TPerDiskStatsPtr Stats;

    bool IsAccelerated;
    bool IsAccelerateScheduled;

    const bool IsMultiPutMode;

    bool RequireExtraBlockChecks = false;

    void SanityCheck() {
        if (RequestsSent <= MaxSaneRequests) {
            return;
        }
        TStringStream err;
        err << "Group# " << Info->GroupID
            << " sent over MaxSaneRequests# " << MaxSaneRequests
            << " requests, internal state# " << PutImpl.DumpFullState();
        ErrorReason = err.Str();
        R_LOG_CRIT_S("BPG21", ErrorReason);
        ReplyAndDie(NKikimrProto::ERROR);
    }

    void Handle(TEvAccelerate::TPtr &ev) {
        RootCauseTrack.OnAccelerate(ev->Get()->CauseIdx);
        Accelerate();
        SanityCheck(); // May Die
    }

    void Accelerate() {
        if (IsAccelerated) {
            return;
        }
        IsAccelerated = true;

        if (IsMultiPutMode) {
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVMultiPut>> vMultiPuts;
            PutImpl.Accelerate(LogCtx, vMultiPuts);
            UpdatePengingVDiskResponseCount<TEvBlobStorage::TEvVMultiPut, TVMultiPutCookie>(vMultiPuts);
            RequestsSent += vMultiPuts.size();
            *Mon->NodeMon->AccelerateEvVMultiPutCount += vMultiPuts.size();
            CountPuts(vMultiPuts);
            SendToQueues(vMultiPuts, TimeStatsEnabled);
        } else {
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> vPuts;
            PutImpl.Accelerate(LogCtx, vPuts);
            UpdatePengingVDiskResponseCount<TEvBlobStorage::TEvVPut, TBlobCookie>(vPuts);
            RequestsSent += vPuts.size();
            *Mon->NodeMon->AccelerateEvVPutCount += vPuts.size();
            CountPuts(vPuts);
            SendToQueues(vPuts, TimeStatsEnabled);
        }
    }

    void Handle(TEvBlobStorage::TEvVPutResult::TPtr &ev) {
        A_LOG_LOG_S(false, ev->Get()->Record.GetStatus() == NKikimrProto::OK ? NLog::PRI_DEBUG : NLog::PRI_NOTICE,
            "BPP01", "received " << ev->Get()->ToString() << " from# " << VDiskIDFromVDiskID(ev->Get()->Record.GetVDiskID()));

        ProcessReplyFromQueue(ev);
        ResponsesReceived++;

        const ui64 cyclesPerUs = NHPTimer::GetCyclesPerSecond() / 1000000;
        ev->Get()->Record.MutableTimestamps()->SetReceivedByDSProxyUs(GetCycleCountFast() / cyclesPerUs);
        const NKikimrBlobStorage::TEvVPutResult &record = ev->Get()->Record;
        const TLogoBlobID blob = LogoBlobIDFromLogoBlobID(record.GetBlobID());
        const TLogoBlobID origBlobId = TLogoBlobID(blob, 0);
        Y_VERIFY(record.HasCookie());
        TBlobCookie cookie(record.GetCookie());
        const ui64 idx = cookie.GetBlobIdx();
        const ui64 vdisk = cookie.GetVDiskOrderNumber();
        const NKikimrProto::EReplyStatus status = record.GetStatus();

        Y_VERIFY(vdisk < WaitingVDiskResponseCount.size(), "blobIdx# %" PRIu64 " vdisk# %" PRIu64, idx, vdisk);
        if (WaitingVDiskResponseCount[vdisk] == 1) {
            WaitingVDiskCount--;
        }
        WaitingVDiskResponseCount[vdisk]--;

        Y_VERIFY(idx < PutImpl.Blobs.size());
        Y_VERIFY(origBlobId == PutImpl.Blobs[idx].BlobId);
        if (TimeStatsEnabled && record.GetMsgQoS().HasExecTimeStats()) {
            TimeStats.ApplyPut(PutImpl.Blobs[idx].BufferSize, record.GetMsgQoS().GetExecTimeStats());
        }

        Y_VERIFY(record.HasVDiskID());
        TVDiskID vDiskId = VDiskIDFromVDiskID(record.GetVDiskID());
        const TVDiskIdShort shortId(vDiskId);

        LWPROBE(DSProxyVDiskRequestDuration, TEvBlobStorage::EvVPut, blob.BlobSize(), blob.TabletID(),
                Info->GroupID, blob.Channel(), Info->GetFailDomainOrderNumber(shortId),
                GetStartTime(record.GetTimestamps()),
                GetTotalTimeMs(record.GetTimestamps()),
                GetVDiskTimeMs(record.GetTimestamps()),
                GetTotalTimeMs(record.GetTimestamps()) - GetVDiskTimeMs(record.GetTimestamps()),
                NKikimrBlobStorage::EPutHandleClass_Name(PutImpl.GetPutHandleClass()),
                NKikimrProto::EReplyStatus_Name(status));
        if (RootCauseTrack.IsOn) {
            RootCauseTrack.OnReply(cookie.GetCauseIdx(),
                GetTotalTimeMs(record.GetTimestamps()) - GetVDiskTimeMs(record.GetTimestamps()),
                GetVDiskTimeMs(record.GetTimestamps()));
        }

        TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> vPuts;
        TPutImpl::TPutResultVec putResults;
        PutImpl.OnVPutEventResult(LogCtx, ev->Sender, *ev->Get(), vPuts, putResults);
        UpdatePengingVDiskResponseCount<TEvBlobStorage::TEvVPut, TBlobCookie>(vPuts);
        RequestsSent += vPuts.size();
        CountPuts(vPuts);
        SendToQueues(vPuts, TimeStatsEnabled);
        if (ReplyAndDieWithLastResponse(putResults)) {
            return;
        }
        Y_VERIFY(RequestsSent > ResponsesReceived, "RequestsSent# %" PRIu64 " ResponsesReceived# %" PRIu64,
                ui64(RequestsSent), ui64(ResponsesReceived));

        if (!IsAccelerateScheduled && !IsAccelerated) {
            if (WaitingVDiskCount == 1 && RequestsSent > 1) {
                ui64 timeToAccelerateUs = PutImpl.GetTimeToAccelerateNs(LogCtx) / 1000;
                TDuration timeSinceStart = TActivationContext::Now() - StartTime;
                if (timeSinceStart.MicroSeconds() < timeToAccelerateUs) {
                    ui64 causeIdx = RootCauseTrack.RegisterAccelerate();
                    Schedule(TDuration::MicroSeconds(timeToAccelerateUs - timeSinceStart.MicroSeconds()),
                            new TEvAccelerate(causeIdx));
                    IsAccelerateScheduled = true;
                } else {
                    Accelerate();
                }
            }
        }
        SanityCheck(); // May Die
    }


    void Handle(TEvBlobStorage::TEvVMultiPutResult::TPtr &ev) {
        ProcessReplyFromQueue(ev);
        ResponsesReceived++;

        const ui64 cyclesPerUs = NHPTimer::GetCyclesPerSecond() / 1000000;
        ev->Get()->Record.MutableTimestamps()->SetReceivedByDSProxyUs(GetCycleCountFast() / cyclesPerUs);
        const NKikimrBlobStorage::TEvVMultiPutResult &record = ev->Get()->Record;

        if (TimeStatsEnabled && record.GetMsgQoS().HasExecTimeStats()) {
            TimeStats.ApplyPut(RequestBytes, record.GetMsgQoS().GetExecTimeStats());
        }

        Y_VERIFY(record.HasVDiskID());
        TVDiskID vDiskId = VDiskIDFromVDiskID(record.GetVDiskID());
        const TVDiskIdShort shortId(vDiskId);

        Y_VERIFY(record.HasCookie());
        TVMultiPutCookie cookie(record.GetCookie());
        const ui64 vdisk = cookie.GetVDiskOrderNumber();
        const NKikimrProto::EReplyStatus status = record.GetStatus();

        auto prio = NLog::PRI_DEBUG;
        if (status != NKikimrProto::OK) {
            prio = NLog::PRI_INFO;
        }
        for (const auto& item : record.GetItems()) {
            if (item.GetStatus() != NKikimrProto::OK) {
                prio = NLog::PRI_INFO;
            }
        }
        A_LOG_LOG_S(false, prio, "BPP02", "received " << ev->Get()->ToString()
                << " from# " << VDiskIDFromVDiskID(ev->Get()->Record.GetVDiskID()));

        Y_VERIFY(vdisk < WaitingVDiskResponseCount.size(), " vdisk# %" PRIu64, vdisk);
        if (WaitingVDiskResponseCount[vdisk] == 1) {
            WaitingVDiskCount--;
        }
        WaitingVDiskResponseCount[vdisk]--;

        // Trace put request duration
        if (LWPROBE_ENABLED(DSProxyVDiskRequestDuration)) {
            for (auto &item : record.GetItems()) {
                TLogoBlobID blobId = LogoBlobIDFromLogoBlobID(item.GetBlobID());
                NKikimrProto::EReplyStatus itemStatus = item.GetStatus();
                LWPROBE(DSProxyVDiskRequestDuration, TEvBlobStorage::EvVMultiPut, blobId.BlobSize(), blobId.TabletID(),
                        Info->GroupID, blobId.Channel(), Info->GetFailDomainOrderNumber(shortId),
                        GetStartTime(record.GetTimestamps()),
                        GetTotalTimeMs(record.GetTimestamps()),
                        GetVDiskTimeMs(record.GetTimestamps()),
                        GetTotalTimeMs(record.GetTimestamps()) - GetVDiskTimeMs(record.GetTimestamps()),
                        NKikimrBlobStorage::EPutHandleClass_Name(PutImpl.GetPutHandleClass()),
                        NKikimrProto::EReplyStatus_Name(itemStatus));
            }
        }

        // Handle put results
        bool isCauseRegistered = !RootCauseTrack.IsOn;
        TPutImpl::TPutResultVec putResults;
        for (auto &item : record.GetItems()) {
            if (!isCauseRegistered) {
                isCauseRegistered = RootCauseTrack.OnReply(cookie.GetCauseIdx(),
                    GetTotalTimeMs(record.GetTimestamps()) - GetVDiskTimeMs(record.GetTimestamps()),
                    GetVDiskTimeMs(record.GetTimestamps()));
            }

            Y_VERIFY(item.HasStatus());
            Y_VERIFY(item.HasBlobID());
            Y_VERIFY(item.HasCookie());
            ui64 blobIdx = TBlobCookie(item.GetCookie()).GetBlobIdx();
            NKikimrProto::EReplyStatus itemStatus = item.GetStatus();
            TLogoBlobID blobId = LogoBlobIDFromLogoBlobID(item.GetBlobID());
            Y_VERIFY(itemStatus != NKikimrProto::RACE); // we should get RACE for the whole request and handle it in CheckForTermErrors
            if (itemStatus == NKikimrProto::BLOCKED || itemStatus == NKikimrProto::DEADLINE) {
                TStringStream errorReason;
                errorReason << "Got VMultiPutResult itemStatus# " << itemStatus << " from VDiskId# " << vDiskId;
                ErrorReason = errorReason.Str();
                PutImpl.PrepareOneReply(itemStatus, TLogoBlobID(blobId, 0), blobIdx, LogCtx, ErrorReason, putResults);
            }
        }
        if (ReplyAndDieWithLastResponse(putResults)) {
            return;
        }
        putResults.clear();

        TDeque<std::unique_ptr<TEvBlobStorage::TEvVMultiPut>> vMultiPuts;
        PutImpl.OnVPutEventResult(LogCtx, ev->Sender, *ev->Get(), vMultiPuts, putResults);
        UpdatePengingVDiskResponseCount<TEvBlobStorage::TEvVMultiPut, TVMultiPutCookie>(vMultiPuts);
        RequestsSent += vMultiPuts.size();
        CountPuts(vMultiPuts);
        SendToQueues(vMultiPuts, TimeStatsEnabled);
        if (ReplyAndDieWithLastResponse(putResults)) {
            return;
        }
        Y_VERIFY(RequestsSent > ResponsesReceived, "RequestsSent# %" PRIu64 " ResponsesReceived# %" PRIu64
                " ResponsesSent# %" PRIu64 " BlobsCount# %" PRIu64 " TPutImpl# %s", ui64(RequestsSent), ui64(ResponsesReceived),
                (ui64)ResponsesSent, (ui64)PutImpl.Blobs.size(), PutImpl.DumpFullState().data());

        if (!IsAccelerateScheduled && !IsAccelerated) {
            if (WaitingVDiskCount == 1 && RequestsSent > 1) {
                ui64 timeToAccelerateUs = PutImpl.GetTimeToAccelerateNs(LogCtx) / 1000;
                TDuration timeSinceStart = TActivationContext::Now() - StartTime;
                if (timeSinceStart.MicroSeconds() < timeToAccelerateUs) {
                    ui64 causeIdx = RootCauseTrack.RegisterAccelerate();
                    Schedule(TDuration::MicroSeconds(timeToAccelerateUs - timeSinceStart.MicroSeconds()),
                            new TEvAccelerate(causeIdx));
                    IsAccelerateScheduled = true;
                } else {
                    Accelerate();
                }
            }
        }
        SanityCheck(); // May Die
    }

    friend class TBlobStorageGroupRequestActor<TBlobStorageGroupPutRequest>;

    void ReplyAndDie(NKikimrProto::EReplyStatus status) {
        TPutImpl::TPutResultVec putResults;
        PutImpl.PrepareReply(status, LogCtx, ErrorReason, putResults);
        Y_VERIFY(ReplyAndDieWithLastResponse(putResults));
    }

    bool ReplyAndDieWithLastResponse(TPutImpl::TPutResultVec &putResults) {
        for (auto& [blobIdx, result] : putResults) {
            Y_VERIFY(ResponsesSent != PutImpl.Blobs.size());
            SendReply(result, blobIdx);
        }
        if (ResponsesSent == PutImpl.Blobs.size()) {
            PassAway();
            return true;
        }
        return false;
    }

    void SendReply(std::unique_ptr<TEvBlobStorage::TEvPutResult> &putResult, ui64 blobIdx) {
        NKikimrProto::EReplyStatus status = putResult->Status;
        A_LOG_LOG_S(false, status == NKikimrProto::OK ? NLog::PRI_INFO : NLog::PRI_NOTICE, "BPP21",
            "SendReply putResult# " << putResult->ToString() << " ResponsesSent# " << ResponsesSent
            << " PutImpl.Blobs.size# " << PutImpl.Blobs.size()
            << " Last# " << (ResponsesSent + 1 == PutImpl.Blobs.size() ? "true" : "false"));
        const TDuration duration = TActivationContext::Now() - StartTime;
        TLogoBlobID blobId = putResult->Id;
        TLogoBlobID origBlobId = TLogoBlobID(blobId, 0);
        Mon->CountPutPesponseTime(Info->GetDeviceType(), HandleClass, PutImpl.Blobs[blobIdx].BufferSize, duration);
        *Mon->ActivePutCapacity -= ReportedBytes;
        Y_VERIFY(PutImpl.GetHandoffPartsSent() <= Info->Type.TotalPartCount() * MaxHandoffNodes * PutImpl.Blobs.size());
        ++*Mon->HandoffPartsSent[Min(Mon->HandoffPartsSent.size() - 1, (size_t)PutImpl.GetHandoffPartsSent())];
        ReportedBytes = 0;
        const bool success = (status == NKikimrProto::OK);
        LWPROBE(DSProxyRequestDuration, TEvBlobStorage::EvPut,
                blobId.BlobSize(),
                duration.SecondsFloat() * 1000.0,
                blobId.TabletID(), Info->GroupID, blobId.Channel(),
                NKikimrBlobStorage::EPutHandleClass_Name(HandleClass), success);
        ResponsesSent++;
        Y_VERIFY(ResponsesSent <= PutImpl.Blobs.size());
        RootCauseTrack.RenderTrack(PutImpl.Blobs[blobIdx].Orbit);
        LWTRACK(DSProxyPutReply, PutImpl.Blobs[blobIdx].Orbit);
        putResult->Orbit = std::move(PutImpl.Blobs[blobIdx].Orbit);
        putResult->WrittenBeyondBarrier = PutImpl.WrittenBeyondBarrier[blobIdx];
        putResult->ExecutionRelay = std::move(PutImpl.Blobs[blobIdx].ExecutionRelay);
        if (!IsManyPuts) {
            SendResponse(std::move(putResult), TimeStatsEnabled ? &TimeStats : nullptr);
        } else {
            if (putResult->Status == NKikimrProto::OK) {
                PutImpl.Blobs[blobIdx].Span.EndOk();
            } else {
                PutImpl.Blobs[blobIdx].Span.EndError(putResult->ErrorReason);
            }
            SendResponse(std::move(putResult), TimeStatsEnabled ? &TimeStats : nullptr,
                PutImpl.Blobs[blobIdx].Recipient, PutImpl.Blobs[blobIdx].Cookie, false);
            PutImpl.Blobs[blobIdx].Replied = true;
        }
    }

    TString BlobIdSequenceToString() const {
        TStringBuilder blobIdsStr;
        blobIdsStr << '[';
        for (ui64 blobIdx = 0; blobIdx < PutImpl.Blobs.size(); ++blobIdx) {
            if (blobIdx) {
                blobIdsStr << ' ';
            }
            blobIdsStr << PutImpl.Blobs[blobIdx].BlobId.ToString();
        }
        return blobIdsStr << ']';
    }

    std::unique_ptr<IEventBase> RestartQuery(ui32 counter) {
        ++*Mon->NodeMon->RestartPut;
        auto ev = std::make_unique<TEvBlobStorage::TEvBunchOfEvents>();
        for (auto& item : PutImpl.Blobs) {
            if (item.Replied) {
                continue;
            }
            TEvBlobStorage::TEvPut *put;

            DecryptInplace(item.Buffer, 0, 0, item.Buffer.size(), item.BlobId, *Info);

            ev->Bunch.emplace_back(new IEventHandle(
                TActorId() /*recipient*/,
                item.Recipient,
                put = new TEvBlobStorage::TEvPut(item.BlobId, TRcBuf(item.Buffer), Deadline, HandleClass, Tactic),
                0 /*flags*/,
                item.Cookie,
                nullptr /*forwardOnNondelivery*/,
                item.Span.GetTraceId()
            ));
            put->RestartCounter = counter;
            put->ExecutionRelay = std::move(item.ExecutionRelay);
        }
        return ev;
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BS_PROXY_PUT_ACTOR;
    }

    static const auto& ActiveCounter(const TIntrusivePtr<TBlobStorageGroupProxyMon>& mon) {
        return mon->ActivePut;
    }

    static constexpr ERequestType RequestType() {
        return ERequestType::Put;
    }

    TBlobStorageGroupPutRequest(const TIntrusivePtr<TBlobStorageGroupInfo> &info,
            const TIntrusivePtr<TGroupQueues> &state, const TActorId &source,
            const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon, TEvBlobStorage::TEvPut *ev,
            ui64 cookie, NWilson::TTraceId traceId, bool timeStatsEnabled,
            TDiskResponsivenessTracker::TPerDiskStatsPtr stats,
            TMaybe<TGroupStat::EKind> latencyQueueKind, TInstant now,
            TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters,
            bool enableRequestMod3x3ForMinLatecy)
        : TBlobStorageGroupRequestActor(info, state, mon, source, cookie, std::move(traceId),
                NKikimrServices::BS_PROXY_PUT, false, latencyQueueKind, now, storagePoolCounters,
                ev->RestartCounter, "DSProxy.Put", nullptr)
        , PutImpl(info, state, ev, mon, enableRequestMod3x3ForMinLatecy, source, cookie, Span.GetTraceId())
        , WaitingVDiskResponseCount(info->GetTotalVDisksNum())
        , Deadline(ev->Deadline)
        , StartTime(now)
        , HandleClass(ev->HandleClass)
        , ReportedBytes(0)
        , TimeStatsEnabled(timeStatsEnabled)
        , Tactic(ev->Tactic)
        , Stats(std::move(stats))
        , IsAccelerated(false)
        , IsAccelerateScheduled(false)
        , IsMultiPutMode(false)
        , RequireExtraBlockChecks(!ev->ExtraBlockChecks.empty())
    {
        if (ev->Orbit.HasShuttles()) {
            RootCauseTrack.IsOn = true;
        }
        ReportBytes(PutImpl.Blobs[0].Buffer.capacity() + sizeof(*this));

        RequestBytes = ev->Buffer.size();
        RequestHandleClass = HandleClassToHandleClass(HandleClass);
        MaxSaneRequests = info->Type.TotalPartCount() * (1ull + info->Type.Handoff()) * 2;
    }

    ui32 MaxRestartCounter(const TBatchedVec<TEvBlobStorage::TEvPut::TPtr>& events) {
        ui32 res = 0;
        for (const auto& ev : events) {
            res = Max(res, ev->Get()->RestartCounter);
        }
        return res;
    }

    TBlobStorageGroupPutRequest(const TIntrusivePtr<TBlobStorageGroupInfo> &info,
            const TIntrusivePtr<TGroupQueues> &state,
            const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon, TBatchedVec<TEvBlobStorage::TEvPut::TPtr> &events,
            bool timeStatsEnabled, TDiskResponsivenessTracker::TPerDiskStatsPtr stats,
            TMaybe<TGroupStat::EKind> latencyQueueKind, TInstant now,
            TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters,
            NKikimrBlobStorage::EPutHandleClass handleClass, TEvBlobStorage::TEvPut::ETactic tactic,
            bool enableRequestMod3x3ForMinLatecy)
        : TBlobStorageGroupRequestActor(info, state, mon, TActorId(), 0, NWilson::TTraceId(),
                NKikimrServices::BS_PROXY_PUT, false, latencyQueueKind, now, storagePoolCounters,
                MaxRestartCounter(events), "DSProxy.Put", nullptr)
        , PutImpl(info, state, events, mon, handleClass, tactic, enableRequestMod3x3ForMinLatecy)
        , WaitingVDiskResponseCount(info->GetTotalVDisksNum())
        , IsManyPuts(true)
        , Deadline(TInstant::Zero())
        , StartTime(now)
        , HandleClass(handleClass)
        , ReportedBytes(0)
        , TimeStatsEnabled(timeStatsEnabled)
        , Tactic(tactic)
        , Stats(std::move(stats))
        , IsAccelerated(false)
        , IsAccelerateScheduled(false)
        , IsMultiPutMode(true)
    {
        Y_VERIFY_DEBUG(events.size() <= MaxBatchedPutRequests);
        for (auto &ev : events) {
            auto& msg = *ev->Get();
            Deadline = Max(Deadline, msg.Deadline);
            if (msg.Orbit.HasShuttles()) {
                RootCauseTrack.IsOn = true;
            }
            if (!msg.ExtraBlockChecks.empty()) {
                RequireExtraBlockChecks = true;
            }
        }

        RequestBytes = 0;
        for (auto &item: PutImpl.Blobs) {
            ReportBytes(item.Buffer.capacity());
            RequestBytes += item.BufferSize;
        }
        ReportBytes(sizeof(*this));
        RequestHandleClass = HandleClassToHandleClass(HandleClass);
        MaxSaneRequests = info->Type.TotalPartCount() * (1ull + info->Type.Handoff()) * 2;
    }

    void ReportBytes(i64 bytes) {
        *Mon->ActivePutCapacity += bytes;
        ReportedBytes += bytes;
    }

    void Bootstrap() {
        A_LOG_INFO_S("BPP13", "bootstrap"
            << " ActorId# " << SelfId()
            << " Group# " << Info->GroupID
            << " BlobCount# " << PutImpl.Blobs.size()
            << " BlobIDs# " << BlobIdSequenceToString()
            << " HandleClass# " << NKikimrBlobStorage::EPutHandleClass_Name(HandleClass)
            << " Tactic# " << TEvBlobStorage::TEvPut::TacticName(Tactic)
            << " Deadline# " << Deadline
            << " RestartCounter# " << RestartCounter);

        for (ui64 blobIdx = 0; blobIdx < PutImpl.Blobs.size(); ++blobIdx) {
            LWTRACK(DSProxyPutBootstrapStart, PutImpl.Blobs[blobIdx].Orbit);
        }

        Become(&TThis::StateWait, TDuration::MilliSeconds(DsPutWakeupMs), new TKikimrEvents::TEvWakeup);

        PartSets.resize(PutImpl.Blobs.size());
        for (auto& partSet : PartSets) {
            partSet.resize(Info->Type.TotalPartCount());
        }
        if (Info->GetEncryptionMode() == TBlobStorageGroupInfo::EEM_NONE) {
            BlobsEncrypted = PartSets.size();
        }
        ResumeBootstrap();
    }

    bool EncodeQuantum() {
        const ui64 endTime = GetCycleCountFast() + DurationToCycles(MaxQuantumDuration);
        bool firstIteration = true;
        while (Min(BlobsEncrypted, BlobsSplit) < PutImpl.Blobs.size()) {
            if (!firstIteration && endTime <= GetCycleCountFast()) {
                return false;
            }
            firstIteration = false;

            if (BlobsEncrypted <= BlobsSplit) { // first we encrypt the blob (if encryption is enabled)
                auto& blob = PutImpl.Blobs[BlobsEncrypted];
                const ui32 size = Min<ui32>(blob.Buffer.size() - CurrentEncryptionOffset, MaxBytesToEncryptAtOnce);
                EncryptInplace(blob.Buffer, CurrentEncryptionOffset, size, blob.BlobId, *Info);
                CurrentEncryptionOffset += size;
                if (CurrentEncryptionOffset == blob.Buffer.size()) {
                    ++BlobsEncrypted;
                    CurrentEncryptionOffset = 0;
                }
            } else { // BlobsSplit < BlobsEncrypted -- then we split it
                auto& blob = PutImpl.Blobs[BlobsSplit];
                const auto crcMode = static_cast<TErasureType::ECrcMode>(blob.BlobId.CrcMode());
                if (ErasureSplit(crcMode, Info->Type, blob.Buffer, PartSets[BlobsSplit], &ErasureSplitContext)) {
                    ++BlobsSplit;
                    ErasureSplitContext = TErasureSplitContext::Init(MaxBytesToSplitAtOnce);
                }
            }
        }

        return true;
    }

    void Handle(TKikimrEvents::TEvWakeup::TPtr &ev) {
        Y_UNUSED(ev);
        A_LOG_WARN_S("BPP14", "Wakeup "
            << " ActorId# " << SelfId()
            << " Group# " << Info->GroupID
            << " BlobIDs# " << BlobIdSequenceToString()
            << " Not answered in "
            << (TActivationContext::Now() - StartTime).Seconds() << " seconds");
        if (TInstant::Now() > Deadline) {
            ErrorReason = "Deadline exceeded";
            ReplyAndDie(NKikimrProto::DEADLINE);
            return;
        }
        Schedule(TDuration::MilliSeconds(DsPutWakeupMs), new TKikimrEvents::TEvWakeup);
    }

    template <typename TPutEvent, typename TCookie>
    void UpdatePengingVDiskResponseCount(const TDeque<std::unique_ptr<TPutEvent>> &putEvents) {
        for (auto &event : putEvents) {
            Y_VERIFY(event->Record.HasCookie());
            TCookie cookie(event->Record.GetCookie());
            if (RootCauseTrack.IsOn) {
                cookie.SetCauseIdx(RootCauseTrack.RegisterCause());
                event->Record.SetCookie(cookie);
            }
            ui64 vdisk = cookie.GetVDiskOrderNumber();
            Y_VERIFY(vdisk < WaitingVDiskResponseCount.size());
            if (!WaitingVDiskResponseCount[vdisk]) {
                WaitingVDiskCount++;
            }
            WaitingVDiskResponseCount[vdisk]++;
        }
    }

    template<typename TEvV, typename TCookie>
    struct TIssue {
        void operator ()(TThis& self) {
            TDeque<std::unique_ptr<TEvV>> events;
            self.PutImpl.GenerateInitialRequests(self.LogCtx, self.PartSets, events);
            self.UpdatePengingVDiskResponseCount<TEvV, TCookie>(events);
            self.RequestsSent += events.size();
            self.CountPuts(events);
            self.SendToQueues(events, self.TimeStatsEnabled);
        }
    };

    void ResumeBootstrap() {
        if (EncodeQuantum()) {
            if (IsMultiPutMode) {
                TIssue<TEvBlobStorage::TEvVMultiPut, TVMultiPutCookie>()(*this);
            } else {
                TIssue<TEvBlobStorage::TEvVPut, TBlobCookie>()(*this);
            }
        } else {
            TActivationContext::Send(new IEventHandle(TEvBlobStorage::EvResume, 0, SelfId(), {}, nullptr, 0));
        }
        SanityCheck();
    }

    STATEFN(StateWait) {
        if (ProcessEvent(ev)) {
            return;
        }
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvVPutResult, Handle);
            hFunc(TEvBlobStorage::TEvVMultiPutResult, Handle);
            hFunc(TEvAccelerate, Handle);
            cFunc(TEvBlobStorage::EvResume, ResumeBootstrap);
            hFunc(TKikimrEvents::TEvWakeup, Handle);
        }
    }
};

IActor* CreateBlobStorageGroupPutRequest(const TIntrusivePtr<TBlobStorageGroupInfo> &info,
        const TIntrusivePtr<TGroupQueues> &state, const TActorId &source,
        const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon, TEvBlobStorage::TEvPut *ev,
        ui64 cookie, NWilson::TTraceId traceId, bool timeStatsEnabled,
        TDiskResponsivenessTracker::TPerDiskStatsPtr stats,
        TMaybe<TGroupStat::EKind> latencyQueueKind, TInstant now,
        TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters,
        bool enableRequestMod3x3ForMinLatecy) {
    return new TBlobStorageGroupPutRequest(info, state, source, mon, ev, cookie, std::move(traceId), timeStatsEnabled,
            std::move(stats), latencyQueueKind, now, storagePoolCounters, enableRequestMod3x3ForMinLatecy);
}

IActor* CreateBlobStorageGroupPutRequest(const TIntrusivePtr<TBlobStorageGroupInfo> &info,
        const TIntrusivePtr<TGroupQueues> &state,
        const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon, TBatchedVec<TEvBlobStorage::TEvPut::TPtr> &ev,
        bool timeStatsEnabled,
        TDiskResponsivenessTracker::TPerDiskStatsPtr stats,
        TMaybe<TGroupStat::EKind> latencyQueueKind, TInstant now,
        TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters,
        NKikimrBlobStorage::EPutHandleClass handleClass, TEvBlobStorage::TEvPut::ETactic tactic,
        bool enableRequestMod3x3ForMinLatecy) {
    return new TBlobStorageGroupPutRequest(info, state, mon, ev, timeStatsEnabled,
            std::move(stats), latencyQueueKind, now, storagePoolCounters, handleClass, tactic,
            enableRequestMod3x3ForMinLatecy);
}

}//NKikimr

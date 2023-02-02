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

struct TEvResume : public TEventLocal<TEvResume, TEvBlobStorage::EvResume> {
    double WilsonSec;
    double AllocateSec;
    double WaitSec;
    double SplitSec;
    size_t Count;
    TBatchedVec<TDataPartSet> PartSets;
};

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

    THPTimer Timer;
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

            TRcBuf buffer = TRcBuf(item.Buffer); //TODO(innokentii) prevent compaction
            char* data = buffer.GetContiguousSpanMut().data();
            Decrypt(data, data, 0, buffer.size(), item.BlobId, *Info);

            ev->Bunch.emplace_back(new IEventHandle(
                TActorId() /*recipient*/,
                item.Recipient,
                put = new TEvBlobStorage::TEvPut(item.BlobId, std::move(buffer), Deadline, HandleClass, Tactic),
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

        Become(&TThis::StateWait);

        Timer.Reset();

        double wilsonSec = Timer.PassedReset();

        const ui32 totalParts = Info->Type.TotalPartCount();

        TAutoPtr<TEvResume> resume(new TEvResume());
        resume->PartSets.resize(PutImpl.Blobs.size());

        for (ui64 idx = 0; idx < PutImpl.Blobs.size(); ++idx) {
            TLogoBlobID blobId = PutImpl.Blobs[idx].BlobId;

            const ui64 partSize = Info->Type.PartSize(blobId);
            ui64 bufferSize = PutImpl.Blobs[idx].BufferSize;

            char *data = PutImpl.Blobs[idx].Buffer.GetContiguousSpanMut().data();
            Encrypt(data, data, 0, bufferSize, blobId, *Info);
            TDataPartSet &partSet = resume->PartSets[idx];

            partSet.Parts.resize(totalParts);
            if (Info->Type.ErasureFamily() != TErasureType::ErasureMirror) {
                for (ui32 i = 0; i < totalParts; ++i) {
                    partSet.Parts[i].UninitializedOwnedWhole(partSize, 24);
                }
            }
        }
        double allocateSec = Timer.PassedReset();

        resume->WilsonSec = wilsonSec;
        resume->AllocateSec = allocateSec;
        resume->WaitSec = 0.0;
        resume->SplitSec = 0.0;
        resume->Count = 0;
        if (RequestBytes < BufferSizeThreshold) {
            ResumeBootstrap(resume);
        } else {
            Send(SelfId(), resume.Release());
            for (ui64 blobIdx = 0; blobIdx < PutImpl.Blobs.size(); ++blobIdx) {
                LWTRACK(DSProxyPutPauseBootstrap, PutImpl.Blobs[blobIdx].Orbit);
            }
        }

        Schedule(TDuration::MilliSeconds(DsPutWakeupMs), new TKikimrEvents::TEvWakeup);
        SanityCheck(); // May Die
    }

    void Handle(TEvResume::TPtr &ev) {
        if (ev->Get()->Count == 0) {
            // Record only the first resume to keep tracks structure simple
            for (ui64 blobIdx = 0; blobIdx < PutImpl.Blobs.size(); ++blobIdx) {
                LWTRACK(DSProxyPutResumeBootstrap, PutImpl.Blobs[blobIdx].Orbit);
            }
        }
        ResumeBootstrap(ev->Release());
        SanityCheck(); // May Die
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

    void ResumeBootstrap(TAutoPtr<TEvResume> resume) {
        double waitSec = Timer.PassedReset();
        resume->WaitSec += waitSec;

        Y_VERIFY(PutImpl.Blobs.size() == resume->PartSets.size());
        bool splitDone = true;
        for (ui64 idx = 0; idx < PutImpl.Blobs.size(); ++idx) {
            TDataPartSet &partSet = resume->PartSets[idx];
            TLogoBlobID blobId = PutImpl.Blobs[idx].BlobId;
            Info->Type.IncrementalSplitData((TErasureType::ECrcMode)blobId.CrcMode(), PutImpl.Blobs[idx].Buffer, partSet);
            if (partSet.IsSplitDone()) {
                ReportBytes(partSet.MemoryConsumed - PutImpl.Blobs[idx].BufferSize);
            } else {
                splitDone = false;
            }
        }
        double splitSec = Timer.PassedReset();
        resume->SplitSec += splitSec;
        resume->Count++;
        LWPROBE(ProxyPutBootstrapPart, RequestBytes, waitSec * 1000.0, splitSec * 1000.0, resume->Count, resume->SplitSec * 1000.0);

        if (splitDone) {
            for (ui64 blobIdx = 0; blobIdx < PutImpl.Blobs.size(); ++blobIdx) {
                LWTRACK(DSProxyPutBootstrapDone, PutImpl.Blobs[blobIdx].Orbit,
                        RequestBytes, resume->WilsonSec * 1000.0, resume->AllocateSec * 1000.0,
                        resume->WaitSec * 1000.0, resume->SplitSec * 1000.0, resume->Count, blobIdx);
            }
            if (IsMultiPutMode) {
                TDeque<std::unique_ptr<TEvBlobStorage::TEvVMultiPut>> vMultiPuts;
                PutImpl.GenerateInitialRequests(LogCtx, resume->PartSets, vMultiPuts);
                UpdatePengingVDiskResponseCount<TEvBlobStorage::TEvVMultiPut, TVMultiPutCookie>(vMultiPuts);
                RequestsSent += vMultiPuts.size();
                CountPuts(vMultiPuts);
                SendToQueues(vMultiPuts, TimeStatsEnabled);
            } else {
                TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> vPuts;
                PutImpl.GenerateInitialRequests(LogCtx, resume->PartSets, vPuts);
                UpdatePengingVDiskResponseCount<TEvBlobStorage::TEvVPut, TBlobCookie>(vPuts);
                RequestsSent += vPuts.size();
                CountPuts(vPuts);
                SendToQueues(vPuts, TimeStatsEnabled);
            }
        } else {
            Send(SelfId(), resume.Release());
        }
    }

    STATEFN(StateWait) {
        if (ProcessEvent(ev)) {
            return;
        }
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvVPutResult, Handle);
            hFunc(TEvBlobStorage::TEvVMultiPutResult, Handle);
            hFunc(TEvAccelerate, Handle);
            hFunc(TEvResume, Handle);
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

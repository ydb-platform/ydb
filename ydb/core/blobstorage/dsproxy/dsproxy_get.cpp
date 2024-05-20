#include "dsproxy.h"
#include "dsproxy_mon.h"
#include "root_cause.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/digest/crc32c/crc32c.h>
#include <util/generic/set.h>
#include <util/system/datetime.h>
#include "dsproxy_get_impl.h"

namespace NKikimr {

LWTRACE_USING(BLOBSTORAGE_PROVIDER);

struct TEvAccelerateGet : public TEventLocal<TEvAccelerateGet, TEvBlobStorage::EvAccelerateGet> {
    ui64 CauseIdx;
    TEvAccelerateGet(ui64 causeIdx)
        : CauseIdx(causeIdx)
    {}
};

struct TEvAcceleratePut : public TEventLocal<TEvAcceleratePut, TEvBlobStorage::EvAcceleratePut> {
    ui64 CauseIdx;
    TEvAcceleratePut(ui64 causeIdx)
        : CauseIdx(causeIdx)
    {}
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// GET request
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TBlobStorageGroupGetRequest : public TBlobStorageGroupRequestActor<TBlobStorageGroupGetRequest> {
    TGetImpl GetImpl;
    TRootCause RootCauseTrack;
    NLWTrace::TOrbit Orbit;
    const TInstant Deadline;
    TInstant StartTime;
    TInstant StartTimePut;
    ui32 RequestsSent;
    ui32 ResponsesReceived;
    i64 ReportedBytes;
    ui32 MaxSaneRequests = 0;
    bool IsPutStarted = false;

    struct TDiskCounters {
        ui32 Sent = 0;
        ui32 Received = 0;
    };

    TStackVec<TDiskCounters, TypicalDisksInGroup> DiskCounters;

    bool IsGetAccelerated = false;
    bool IsGetAccelerateScheduled = false;
    bool IsPutAccelerated = false;
    bool IsPutAccelerateScheduled = false;

    void Handle(TEvAccelerateGet::TPtr &ev) {
        RootCauseTrack.OnAccelerate(ev->Get()->CauseIdx);
        AccelerateGet();
    }

    void Handle(TEvAcceleratePut::TPtr &ev) {
        RootCauseTrack.OnAccelerate(ev->Get()->CauseIdx);
        AcceleratePut();
    }

    void AccelerateGet() {
        if (IsGetAccelerated) {
            return;
        }
        IsGetAccelerated = true;

        TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> vGets;
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> vPuts;
        GetImpl.AccelerateGet(LogCtx, GetUnresponsiveDiskOrderNumber(), vGets, vPuts);
        *Mon->NodeMon->AccelerateEvVPutCount += vPuts.size();
        *Mon->NodeMon->AccelerateEvVGetCount += vGets.size();
        SendVGetsAndVPuts(vGets, vPuts);
    }

    void AcceleratePut() {
        if (IsPutAccelerated) {
            return;
        }
        IsPutAccelerated = true;

        TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> vGets;
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> vPuts;
        GetImpl.AcceleratePut(LogCtx, GetUnresponsiveDiskOrderNumber(), vGets, vPuts);
        *Mon->NodeMon->AccelerateEvVPutCount += vPuts.size();
        *Mon->NodeMon->AccelerateEvVGetCount += vGets.size();
        SendVGetsAndVPuts(vGets, vPuts);
    }

    void SendVGetsAndVPuts(TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> &vGets,
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> &vPuts) {
        ReportBytes(GetImpl.GrabBytesToReport());
        RequestsSent += vGets.size();
        RequestsSent += vPuts.size();
        CountPuts(vPuts);
        if (vPuts.size()) {
            if (!IsPutStarted) {
                IsPutStarted = true;
                StartTimePut = TActivationContext::Now();
            }
        }
        for (size_t i = 0; i < vGets.size(); ++i) {
            if (RootCauseTrack.IsOn) {
                vGets[i]->Record.SetCookie(RootCauseTrack.RegisterCause());
            }
            Y_ABORT_UNLESS(vGets[i]->Record.HasVDiskID());
            TVDiskID vDiskId = VDiskIDFromVDiskID(vGets[i]->Record.GetVDiskID());
            const TVDiskIdShort shortId(vDiskId);
            ui32 orderNumber = Info->GetOrderNumber(shortId);
            if (DiskCounters.size() <= orderNumber) {
                DiskCounters.resize(orderNumber + 1);
            }
            DiskCounters[orderNumber].Sent++;
        }
        for (size_t i = 0; i < vPuts.size(); ++i) {
            if (RootCauseTrack.IsOn) {
                vPuts[i]->Record.SetCookie(RootCauseTrack.RegisterCause());
            }
            Y_ABORT_UNLESS(vPuts[i]->Record.HasVDiskID());
            TVDiskID vDiskId = VDiskIDFromVDiskID(vPuts[i]->Record.GetVDiskID());
            const TVDiskIdShort shortId(vDiskId);
            ui32 orderNumber = Info->GetOrderNumber(shortId);
            if (DiskCounters.size() <= orderNumber) {
                DiskCounters.resize(orderNumber + 1);
            }
            DiskCounters[orderNumber].Sent++;
        }
        SendToQueues(vGets, false);
        SendToQueues(vPuts, false);
    }

    ui32 CountDisksWithActiveRequests() {
        ui32 activeCount = 0;
        for (size_t i = 0; i < DiskCounters.size(); ++i) {
            if (DiskCounters[i].Sent != DiskCounters[i].Received) {
                ++activeCount;
            }
        }
        return activeCount;
    }

    i32 GetUnresponsiveDiskOrderNumber() {
        i32 unresponsiveDiskOrderNumber = -1;
        for (size_t i = 0; i < DiskCounters.size(); ++i) {
            if (DiskCounters[i].Sent != DiskCounters[i].Received) {
                unresponsiveDiskOrderNumber = i;
            }
        }
        return unresponsiveDiskOrderNumber;
    }

    void Handle(TEvBlobStorage::TEvVGetResult::TPtr &ev) {
        ProcessReplyFromQueue(ev);
        CountEvent(*ev->Get());

        const ui64 cyclesPerUs = NHPTimer::GetCyclesPerSecond() / 1000000;
        ev->Get()->Record.MutableTimestamps()->SetReceivedByDSProxyUs(GetCycleCountFast() / cyclesPerUs);
        const NKikimrBlobStorage::TEvVGetResult &record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.HasStatus());

        ui64 totalSize = 0;
        ui64 tabletId = 0;
        ui32 channel = 0;
        for (ui32 i = 0; i < record.ResultSize(); ++i) {
            const NKikimrBlobStorage::TQueryResult &queryResult = record.GetResult(i);
            if (record.GetStatus() == NKikimrProto::OK) {
                totalSize += ev->Get()->GetBlobSize(queryResult);
            }
            const TLogoBlobID blob = LogoBlobIDFromLogoBlobID(queryResult.GetBlobID());
            tabletId = blob.TabletID();
            channel = blob.Channel();
        }
        ++GeneratedSubrequests;
        GeneratedSubrequestBytes += totalSize;

        Y_ABORT_UNLESS(record.HasVDiskID());
        const TVDiskID vdisk = VDiskIDFromVDiskID(record.GetVDiskID());
        const TVDiskIdShort shortId(vdisk);

        LWPROBE(DSProxyVDiskRequestDuration, TEvBlobStorage::EvVGet, totalSize, tabletId, vdisk.GroupID.GetRawId(), channel,
                Info->GetFailDomainOrderNumber(shortId),
                GetStartTime(record.GetTimestamps()),
                GetTotalTimeMs(record.GetTimestamps()),
                GetVDiskTimeMs(record.GetTimestamps()),
                GetTotalTimeMs(record.GetTimestamps()) - GetVDiskTimeMs(record.GetTimestamps()),
                NKikimrBlobStorage::EGetHandleClass_Name(GetImpl.GetHandleClass()),
                NKikimrProto::EReplyStatus_Name(record.GetStatus()));
        if (RootCauseTrack.IsOn && record.HasCookie()) {
            RootCauseTrack.OnReply(record.GetCookie(),
                    GetTotalTimeMs(record.GetTimestamps()) - GetVDiskTimeMs(record.GetTimestamps()),
                    GetVDiskTimeMs(record.GetTimestamps()));
        }

        ui32 orderNumber = Info->GetOrderNumber(shortId);
        if (DiskCounters.size() <= orderNumber) {
            DiskCounters.resize(orderNumber + 1);
        }
        DiskCounters[orderNumber].Received++;

        TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> vGets;
        TAutoPtr<TEvBlobStorage::TEvGetResult> getResult;
        ResponsesReceived++;
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> vPuts;
        GetImpl.OnVGetResult(LogCtx, *ev->Get(), vGets, vPuts, getResult);
        SendVGetsAndVPuts(vGets, vPuts);

        if (getResult) {
            SendReplyAndDie(getResult);
            return;
        }
        Y_ABORT_UNLESS(RequestsSent > ResponsesReceived, "RequestsSent# %" PRIu32 " ResponsesReceived# %" PRIu32
                " GetImpl.DumpFullState# %s", RequestsSent, ResponsesReceived, GetImpl.DumpFullState().c_str());

        TryScheduleGetAcceleration();
        if (IsPutStarted) {
            TrySchedulePutAcceleration();
        }
        SanityCheck(); // May Die
    }

    void SanityCheck() {
        if (RequestsSent <= MaxSaneRequests) {
            return;
        }
        TStringStream err;
        err << "Group# " << Info->GroupID
            << " sent over MaxSaneRequests# " << MaxSaneRequests
            << " requests, internal state# " << GetImpl.DumpFullState();
        ErrorReason = err.Str();
        R_LOG_CRIT_S("BPG70", ErrorReason);
        ReplyAndDie(NKikimrProto::ERROR);
    }

    TLogoBlobID GetFirstBlobId(TEvBlobStorage::TEvVPutResult::TPtr &ev) {
        return LogoBlobIDFromLogoBlobID(ev->Get()->Record.GetBlobID());
    }

    ui64 SumBlobSize(TEvBlobStorage::TEvVPutResult::TPtr &ev) {
        return GetFirstBlobId(ev).BlobSize();
    }

    void Handle(TEvBlobStorage::TEvVPutResult::TPtr &ev) {
        ProcessReplyFromQueue(ev);
        HandleVPutResult(ev);
    }

    void HandleVPutResult(TEvBlobStorage::TEvVPutResult::TPtr &ev) {
        Y_ABORT_UNLESS(ev->Get()->Record.HasStatus());

        const ui64 cyclesPerUs = NHPTimer::GetCyclesPerSecond() / 1000000;
        ev->Get()->Record.MutableTimestamps()->SetReceivedByDSProxyUs(GetCycleCountFast() / cyclesPerUs);
        const auto &record = ev->Get()->Record;
        const TVDiskID vDiskId = VDiskIDFromVDiskID(record.GetVDiskID());
        TVDiskIdShort shortId(vDiskId);
        const NKikimrProto::EReplyStatus status = record.GetStatus();
        NActors::NLog::EPriority priority = PriorityForStatusInbound(status);
        A_LOG_LOG_S(priority != NActors::NLog::PRI_DEBUG, priority, "BPG30", "Handle VPuEventResult"
            << " status# " << NKikimrProto::EReplyStatus_Name(status).data()
            << " node# " << GetVDiskActorId(shortId).NodeId());

        const TLogoBlobID blob = GetFirstBlobId(ev);
        ui64 sumBlobSize = SumBlobSize(ev);
        LWPROBE(DSProxyVDiskRequestDuration, TEvBlobStorage::EvVPut, sumBlobSize, blob.TabletID(),
                Info->GroupID.GetRawId(), blob.Channel(), Info->GetFailDomainOrderNumber(shortId),
                GetStartTime(record.GetTimestamps()),
                GetTotalTimeMs(record.GetTimestamps()),
                GetVDiskTimeMs(record.GetTimestamps()),
                GetTotalTimeMs(record.GetTimestamps()) - GetVDiskTimeMs(record.GetTimestamps()),
                NKikimrBlobStorage::EPutHandleClass_Name(GetImpl.GetPutHandleClass()),
                NKikimrProto::EReplyStatus_Name(status));

        if (RootCauseTrack.IsOn && record.HasCookie()) {
            RootCauseTrack.OnReply(record.GetCookie(),
                    GetTotalTimeMs(record.GetTimestamps()) - GetVDiskTimeMs(record.GetTimestamps()),
                    GetVDiskTimeMs(record.GetTimestamps()));
        }

        ui32 orderNumber = Info->GetOrderNumber(shortId);
        if (DiskCounters.size() <= orderNumber) {
            DiskCounters.resize(orderNumber + 1);
        }
        DiskCounters[orderNumber].Received++;

        TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> vGets;
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> vPuts;
        TAutoPtr<TEvBlobStorage::TEvGetResult> getResult;
        ResponsesReceived++;

        GetImpl.OnVPutResult(LogCtx, *ev->Get(), vGets, vPuts, getResult);
        SendVGetsAndVPuts(vGets, vPuts);
        if (getResult) {
            SendReplyAndDie(getResult);
            return;
        }
        Y_ABORT_UNLESS(RequestsSent > ResponsesReceived, "RequestsSent# %" PRIu64 " ResponsesReceived# %" PRIu64,
                ui64(RequestsSent), ui64(ResponsesReceived));

        TrySchedulePutAcceleration();
        SanityCheck(); // May Die
    }

    void TryScheduleGetAcceleration() {
        if (!IsGetAccelerateScheduled && !IsGetAccelerated) {
            // Count VDisks that have requests in flight, if there is exactly one such VDisk, Accelerate
            if (CountDisksWithActiveRequests() <= 1) {
                ui64 timeToAccelerateUs = GetImpl.GetTimeToAccelerateGetNs(LogCtx) / 1000;
                TInstant now = TActivationContext::Now();
                TDuration timeSinceStart = (now > StartTime) ? (now - StartTime) : TDuration::MilliSeconds(0);
                if (timeSinceStart.MicroSeconds() < timeToAccelerateUs) {
                    ui64 causeIdx = RootCauseTrack.RegisterAccelerate();
                    Schedule(TDuration::MicroSeconds(timeToAccelerateUs - timeSinceStart.MicroSeconds()),
                            new TEvAccelerateGet(causeIdx));
                    IsGetAccelerateScheduled = true;
                } else {
                    AccelerateGet();
                }
            }
        }
    }

    void TrySchedulePutAcceleration() {
        if (!IsPutAccelerateScheduled && !IsPutAccelerated) {
            // Count VDisks that have requests in flight, if there is exactly one such VDisk, Accelerate
            if (CountDisksWithActiveRequests() <= 1) {
                ui64 timeToAccelerateUs = GetImpl.GetTimeToAcceleratePutNs(LogCtx) / 1000;
                TInstant now = TActivationContext::Now();
                TDuration timeSinceStart = (now > StartTimePut) ? (now - StartTimePut) : TDuration::MilliSeconds(0);
                if (timeSinceStart.MicroSeconds() < timeToAccelerateUs) {
                    ui64 causeIdx = RootCauseTrack.RegisterAccelerate();
                    Schedule(TDuration::MicroSeconds(timeToAccelerateUs - timeSinceStart.MicroSeconds()),
                            new TEvAcceleratePut(causeIdx));
                    IsPutAccelerateScheduled = true;
                } else {
                    AcceleratePut();
                }
            }
        }
    }

    void SendReplyAndDie(TAutoPtr<TEvBlobStorage::TEvGetResult> &evResult) {
        const TInstant now = TActivationContext::Now();
        const TDuration duration = (now > StartTime) ? (now - StartTime) : TDuration::MilliSeconds(0);
        Mon->CountGetResponseTime(Info->GetDeviceType(), GetImpl.GetHandleClass(), evResult->PayloadSizeBytes(), duration);
        *Mon->ActiveGetCapacity -= ReportedBytes;
        ReportedBytes = 0;
        bool success = evResult->Status == NKikimrProto::OK;
        ui64 requestSize = 0;
        ui64 tabletId = 0;
        ui32 channel = 0;
        for (ui32 i = 0; i < evResult->ResponseSz; ++i) {
            tabletId = evResult->Responses[i].Id.TabletID();
            channel = evResult->Responses[i].Id.Channel();
            requestSize += evResult->Responses[i].RequestedSize;
        }
        RootCauseTrack.RenderTrack(Orbit);
        LWTRACK(DSProxyGetReply, Orbit);
        evResult->Orbit = std::move(Orbit);
        LWPROBE(DSProxyRequestDuration, TEvBlobStorage::EvGet, requestSize, duration.SecondsFloat() * 1000.0, tabletId,
                evResult->GroupId.GetRawId(), channel, NKikimrBlobStorage::EGetHandleClass_Name(GetImpl.GetHandleClass()),
                success);
        A_LOG_LOG_S(true, success ? NLog::PRI_INFO : NLog::PRI_NOTICE, "BPG68", "Result# " << evResult->Print(false));
        return SendResponseAndDie(std::unique_ptr<TEvBlobStorage::TEvGetResult>(evResult.Release()));
    }

    std::unique_ptr<IEventBase> RestartQuery(ui32 counter) {
        ++*Mon->NodeMon->RestartIndexRestoreGet;
        return GetImpl.RestartQuery(counter);
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BS_PROXY_GET_ACTOR;
    }

    static constexpr ERequestType RequestType() {
        return ERequestType::Get;
    }

    static const auto& ActiveCounter(const TIntrusivePtr<TBlobStorageGroupProxyMon>& mon) {
        return mon->ActiveGet;
    }

    TBlobStorageGroupGetRequest(const TIntrusivePtr<TBlobStorageGroupInfo> &info,
            const TIntrusivePtr<TGroupQueues> &state, const TActorId &source,
            const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon, TEvBlobStorage::TEvGet *ev, ui64 cookie,
            NWilson::TSpan&& span, TNodeLayoutInfoPtr&& nodeLayout, TMaybe<TGroupStat::EKind> latencyQueueKind,
            TInstant now, TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters)
        : TBlobStorageGroupRequestActor(info, state, mon, source, cookie,
                NKikimrServices::BS_PROXY_GET, ev->IsVerboseNoDataEnabled || ev->CollectDebugInfo,
                latencyQueueKind, now, storagePoolCounters, ev->RestartCounter, std::move(span),
                std::move(ev->ExecutionRelay))
        , GetImpl(info, state, ev, std::move(nodeLayout), LogCtx.RequestPrefix)
        , Orbit(std::move(ev->Orbit))
        , Deadline(ev->Deadline)
        , StartTime(now)
        , StartTimePut(StartTime)
        , RequestsSent(0)
        , ResponsesReceived(0)
        , ReportedBytes(0)
    {
        ReportBytes(sizeof(*this));
        MaxSaneRequests = ev->QuerySize * info->Type.TotalPartCount() * (1 + info->Type.Handoff()) * 3;

        RequestBytes = GetImpl.CountRequestBytes();
        RequestHandleClass = HandleClassToHandleClass(ev->GetHandleClass);
        if (Orbit.HasShuttles()) {
            RootCauseTrack.IsOn = true;
        }
    }

    void ReportBytes(i64 bytes) {
        ReportedBytes += bytes;
        *Mon->ActiveGetCapacity += bytes;
    }

    void Bootstrap() {
        A_LOG_INFO_S("BPG01", "bootstrap"
            << " ActorId# " << SelfId()
            << " Group# " << Info->GroupID
            << " Query# " << GetImpl.DumpQuery()
            << " Deadline# " << Deadline
            << " RestartCounter# " << RestartCounter);

        LWTRACK(DSProxyGetBootstrap, Orbit);

        TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> vGets;
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> vPuts;
        GetImpl.GenerateInitialRequests(LogCtx, vGets);
        SendVGetsAndVPuts(vGets, vPuts);
        TryScheduleGetAcceleration();

        Y_ABORT_UNLESS(RequestsSent > ResponsesReceived);
        Become(&TThis::StateWait);
        SanityCheck(); // May Die
    }

    friend class TBlobStorageGroupRequestActor<TBlobStorageGroupGetRequest>;
    void ReplyAndDie(NKikimrProto::EReplyStatus status) {
        TAutoPtr<TEvBlobStorage::TEvGetResult> getResult;
        GetImpl.PrepareReply(status, ErrorReason, LogCtx, getResult);
        SendReplyAndDie(getResult);
        return;
    }

    STATEFN(StateWait) {
        if (ProcessEvent(ev)) {
            return;
        }
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvVGetResult, Handle);
            hFunc(TEvBlobStorage::TEvVPutResult, Handle);
            hFunc(TEvAccelerateGet, Handle);
            hFunc(TEvAcceleratePut, Handle);
        }
    }
};

IActor* CreateBlobStorageGroupGetRequest(const TIntrusivePtr<TBlobStorageGroupInfo> &info,
        const TIntrusivePtr<TGroupQueues> &state, const TActorId &source,
        const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon, TEvBlobStorage::TEvGet *ev,
        ui64 cookie, NWilson::TTraceId traceId, TNodeLayoutInfoPtr&& nodeLayout,
        TMaybe<TGroupStat::EKind> latencyQueueKind, TInstant now,
        TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters) {
    NWilson::TSpan span(TWilson::BlobStorage, std::move(traceId), "DSProxy.Get");
    if (span) {
        span.Attribute("event", ev->ToString());
    }

    return new TBlobStorageGroupGetRequest(info, state, source, mon, ev, cookie, std::move(span),
            std::move(nodeLayout), latencyQueueKind, now, storagePoolCounters);
}

}//NKikimr

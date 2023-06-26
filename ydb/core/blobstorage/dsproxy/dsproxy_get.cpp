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

    const bool IsVMultiPutMode = false;

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
        if (IsVMultiPutMode) {
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVMultiPut>> vMultiPuts;
            GetImpl.AccelerateGet(LogCtx, GetUnresponsiveDiskOrderNumber(), vGets, vMultiPuts);
            *Mon->NodeMon->AccelerateEvVMultiPutCount += vMultiPuts.size();
            *Mon->NodeMon->AccelerateEvVGetCount += vGets.size();
            SendVGetsAndVPuts(vGets, vMultiPuts);
        } else {
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> vPuts;
            GetImpl.AccelerateGet(LogCtx, GetUnresponsiveDiskOrderNumber(), vGets, vPuts);
            *Mon->NodeMon->AccelerateEvVPutCount += vPuts.size();
            *Mon->NodeMon->AccelerateEvVGetCount += vGets.size();
            SendVGetsAndVPuts(vGets, vPuts);
        }
    }

    void AcceleratePut() {
        if (IsPutAccelerated) {
            return;
        }
        IsPutAccelerated = true;

        TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> vGets;
        if (IsVMultiPutMode) {
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVMultiPut>> vMultiPuts;
            GetImpl.AcceleratePut(LogCtx, GetUnresponsiveDiskOrderNumber(), vGets, vMultiPuts);
            *Mon->NodeMon->AccelerateEvVMultiPutCount += vMultiPuts.size();
            *Mon->NodeMon->AccelerateEvVGetCount += vGets.size();
            SendVGetsAndVPuts(vGets, vMultiPuts);
        } else {
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> vPuts;
            GetImpl.AcceleratePut(LogCtx, GetUnresponsiveDiskOrderNumber(), vGets, vPuts);
            *Mon->NodeMon->AccelerateEvVPutCount += vPuts.size();
            *Mon->NodeMon->AccelerateEvVGetCount += vGets.size();
            SendVGetsAndVPuts(vGets, vPuts);
        }
    }

    template <typename TPutEvent>
    void SendVGetsAndVPuts(TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> &vGets, TDeque<std::unique_ptr<TPutEvent>> &vPuts) {
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
            Y_VERIFY(vGets[i]->Record.HasCookie());
            TVGetCookie cookie(vGets[i]->Record.GetCookie());
            if (RootCauseTrack.IsOn) {
                cookie.SetCauseIdx(RootCauseTrack.RegisterCause());
                vGets[i]->Record.SetCookie(cookie);
            }
            Y_VERIFY(vGets[i]->Record.HasVDiskID());
            TVDiskID vDiskId = VDiskIDFromVDiskID(vGets[i]->Record.GetVDiskID());
            const TVDiskIdShort shortId(vDiskId);
            ui32 orderNumber = Info->GetOrderNumber(shortId);
            if (DiskCounters.size() <= orderNumber) {
                DiskCounters.resize(orderNumber + 1);
            }
            DiskCounters[orderNumber].Sent++;
        }
        for (size_t i = 0; i < vPuts.size(); ++i) {
            Y_VERIFY(vPuts[i]->Record.HasCookie());
            TBlobCookie cookie(vPuts[i]->Record.GetCookie());
            if (RootCauseTrack.IsOn) {
                cookie.SetCauseIdx(RootCauseTrack.RegisterCause());
                vPuts[i]->Record.SetCookie(cookie);
            }
            Y_VERIFY(vPuts[i]->Record.HasVDiskID());
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
        Y_VERIFY(record.HasStatus());

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

        Y_VERIFY(record.HasVDiskID());
        const TVDiskID vdisk = VDiskIDFromVDiskID(record.GetVDiskID());
        const TVDiskIdShort shortId(vdisk);

        LWPROBE(DSProxyVDiskRequestDuration, TEvBlobStorage::EvVGet, totalSize, tabletId, vdisk.GroupID, channel,
                Info->GetFailDomainOrderNumber(shortId),
                GetStartTime(record.GetTimestamps()),
                GetTotalTimeMs(record.GetTimestamps()),
                GetVDiskTimeMs(record.GetTimestamps()),
                GetTotalTimeMs(record.GetTimestamps()) - GetVDiskTimeMs(record.GetTimestamps()),
                NKikimrBlobStorage::EGetHandleClass_Name(GetImpl.GetHandleClass()),
                NKikimrProto::EReplyStatus_Name(record.GetStatus()));
        Y_VERIFY(record.HasCookie());
        TVGetCookie cookie(record.GetCookie());
        if (RootCauseTrack.IsOn) {
            RootCauseTrack.OnReply(cookie.GetCauseIdx(),
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
        if (IsVMultiPutMode) {
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVMultiPut>> vMultiPuts;
            GetImpl.OnVGetResult(LogCtx, *ev->Get(), vGets, vMultiPuts, getResult);
            SendVGetsAndVPuts(vGets, vMultiPuts);
        } else {
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> vPuts;
            GetImpl.OnVGetResult(LogCtx, *ev->Get(), vGets, vPuts, getResult);
            SendVGetsAndVPuts(vGets, vPuts);
        }

        if (getResult) {
            SendReplyAndDie(getResult);
            return;
        }
        Y_VERIFY(RequestsSent > ResponsesReceived, "RequestsSent# %" PRIu32 " ResponsesReceived# %" PRIu32
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

    TLogoBlobID GetFirstBlobId(TEvBlobStorage::TEvVMultiPutResult::TPtr &ev) {
        Y_VERIFY(ev->Get()->Record.ItemsSize());
        return LogoBlobIDFromLogoBlobID(ev->Get()->Record.GetItems(0).GetBlobID());
    }

    ui64 SumBlobSize(TEvBlobStorage::TEvVPutResult::TPtr &ev) {
        return GetFirstBlobId(ev).BlobSize();
    }

    ui64 SumBlobSize(TEvBlobStorage::TEvVMultiPutResult::TPtr &ev) {
        ui64 sum = 0;
        for (auto &item : ev->Get()->Record.GetItems()) {
            TLogoBlobID blobId = LogoBlobIDFromLogoBlobID(item.GetBlobID());
            sum += blobId.BlobSize();
        }
        return sum;
    }

    void Handle(TEvBlobStorage::TEvVPutResult::TPtr &ev) {
        ProcessReplyFromQueue(ev);
        HandleVPutResult<TEvBlobStorage::TEvVPut, TEvBlobStorage::TEvVPutResult>(ev);
    }

    void Handle(TEvBlobStorage::TEvVMultiPutResult::TPtr &ev) {
        ProcessReplyFromQueue(ev);
        HandleVPutResult<TEvBlobStorage::TEvVMultiPut, TEvBlobStorage::TEvVMultiPutResult>(ev);
    }

    bool HandleVPutInnerErrorStatuses(TEvBlobStorage::TEvVPutResult::TPtr &ev,
            TAutoPtr<TEvBlobStorage::TEvGetResult> &outGetResult)
    {
        Y_UNUSED(ev, outGetResult);
        return false;
    }

    bool HandleVPutInnerErrorStatuses(TEvBlobStorage::TEvVMultiPutResult::TPtr &ev,
            TAutoPtr<TEvBlobStorage::TEvGetResult> &outGetResult)
    {
        const auto &record = ev->Get()->Record;
        const NKikimrProto::EReplyStatus status = record.GetStatus();
        const TVDiskID vDiskId = VDiskIDFromVDiskID(record.GetVDiskID());

        for (auto &item : record.GetItems()) {
            Y_VERIFY(item.HasStatus());
            Y_VERIFY(item.HasBlobID());
            Y_VERIFY(item.HasCookie());
            NKikimrProto::EReplyStatus itemStatus = item.GetStatus();
            TLogoBlobID blobId = LogoBlobIDFromLogoBlobID(item.GetBlobID());
            Y_VERIFY(itemStatus != NKikimrProto::RACE); // we should get RACE for the whole request and handle it in CheckForTermErrors
            if (itemStatus == NKikimrProto::BLOCKED || itemStatus == NKikimrProto::DEADLINE) {
                R_LOG_ERROR_S("BPG26", "Handle TEvVMultiPutResultItem"
                    << " blobId# " << blobId.ToString()
                    << " status# " << NKikimrProto::EReplyStatus_Name(status)
                    << " itemStatus# " << NKikimrProto::EReplyStatus_Name(itemStatus));
                TStringStream errorReason;
                errorReason << "Got VMultiPutResult itemStatus# " << itemStatus << " from VDiskId# " << vDiskId;
                ErrorReason = errorReason.Str();
                GetImpl.PrepareReply(itemStatus, ErrorReason, LogCtx, outGetResult);
                return true;
            } else {
                R_LOG_DEBUG_S("BPG27", "Handle TEvVMultiPutResultItem"
                    << " blobId# " << blobId.ToString()
                    << " status# " << NKikimrProto::EReplyStatus_Name(status)
                    << " itemStatus# " << NKikimrProto::EReplyStatus_Name(itemStatus));
            }
        }
        return false;
    }

    template <typename TPutEvent, typename TPutEventResult>
    void HandleVPutResult(typename TPutEventResult::TPtr &ev) {
        Y_VERIFY(ev->Get()->Record.HasStatus());

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
                Info->GroupID, blob.Channel(), Info->GetFailDomainOrderNumber(shortId),
                GetStartTime(record.GetTimestamps()),
                GetTotalTimeMs(record.GetTimestamps()),
                GetVDiskTimeMs(record.GetTimestamps()),
                GetTotalTimeMs(record.GetTimestamps()) - GetVDiskTimeMs(record.GetTimestamps()),
                NKikimrBlobStorage::EPutHandleClass_Name(GetImpl.GetPutHandleClass()),
                NKikimrProto::EReplyStatus_Name(status));

        Y_VERIFY(record.HasCookie());
        TBlobCookie cookie(record.GetCookie());
        if (RootCauseTrack.IsOn) {
            RootCauseTrack.OnReply(cookie.GetCauseIdx(),
                    GetTotalTimeMs(record.GetTimestamps()) - GetVDiskTimeMs(record.GetTimestamps()),
                    GetVDiskTimeMs(record.GetTimestamps()));
        }

        ui32 orderNumber = Info->GetOrderNumber(shortId);
        if (DiskCounters.size() <= orderNumber) {
            DiskCounters.resize(orderNumber + 1);
        }
        DiskCounters[orderNumber].Received++;

        TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> vGets;
        TDeque<std::unique_ptr<TPutEvent>> vPuts;
        TAutoPtr<TEvBlobStorage::TEvGetResult> getResult;
        ResponsesReceived++;

        if (HandleVPutInnerErrorStatuses(ev, getResult)) {
            Y_VERIFY(getResult);
            SendReplyAndDie(getResult);
            return;
        }
        GetImpl.OnVPutResult(LogCtx, *ev->Get(), vGets, vPuts, getResult);
        SendVGetsAndVPuts(vGets, vPuts);
        if (getResult) {
            SendReplyAndDie(getResult);
            return;
        }
        Y_VERIFY(RequestsSent > ResponsesReceived, "RequestsSent# %" PRIu64 " ResponsesReceived# %" PRIu64,
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
                evResult->GroupId, channel, NKikimrBlobStorage::EGetHandleClass_Name(GetImpl.GetHandleClass()),
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
            NWilson::TTraceId traceId, TNodeLayoutInfoPtr&& nodeLayout, TMaybe<TGroupStat::EKind> latencyQueueKind,
            TInstant now, TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters, bool isVMultiPutMode)
        : TBlobStorageGroupRequestActor(info, state, mon, source, cookie, std::move(traceId),
                NKikimrServices::BS_PROXY_GET, ev->IsVerboseNoDataEnabled || ev->CollectDebugInfo,
                latencyQueueKind, now, storagePoolCounters, ev->RestartCounter, "DSProxy.Get",
                std::move(ev->ExecutionRelay))
        , GetImpl(info, state, ev, std::move(nodeLayout), LogCtx.RequestPrefix)
        , Orbit(std::move(ev->Orbit))
        , Deadline(ev->Deadline)
        , StartTime(now)
        , StartTimePut(StartTime)
        , RequestsSent(0)
        , ResponsesReceived(0)
        , ReportedBytes(0)
        , IsVMultiPutMode(isVMultiPutMode)
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

        Y_VERIFY(RequestsSent > ResponsesReceived);
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
            hFunc(TEvBlobStorage::TEvVMultiPutResult, Handle);
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
        TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters, bool isVMultiPutMode) {
    return new TBlobStorageGroupGetRequest(info, state, source, mon, ev, cookie, std::move(traceId),
            std::move(nodeLayout), latencyQueueKind, now, storagePoolCounters, isVMultiPutMode);
}

}//NKikimr

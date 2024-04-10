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

    ui32 StatusMsgsSent = 0;
    ui32 StatusResultMsgsReceived = 0;

    bool BootstrapInProgress = true;

    TMonotonic StartTime;
    NKikimrBlobStorage::EPutHandleClass HandleClass;

    i64 ReportedBytes;

    TBlobStorageGroupProxyTimeStats TimeStats;
    bool TimeStatsEnabled;

    const TEvBlobStorage::TEvPut::ETactic Tactic;

    TDiskResponsivenessTracker::TPerDiskStatsPtr Stats;

    bool IsAccelerated;
    bool IsAccelerateScheduled;

    const bool IsMultiPutMode;

    bool Done = false;

    struct TIncarnationRecord {
        ui64 IncarnationGuid = 0;
        TMonotonic ExpirationTimestamp = TMonotonic::Max();
        TMonotonic StatusIssueTimestamp = TMonotonic::Zero(); // zero means no message in flight
    };
    std::vector<TIncarnationRecord> IncarnationRecords;

    TBlobStorageGroupInfo::TGroupVDisks ExpiredVDiskSet;

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

    bool Action(bool accelerate = false) {
        UpdateExpiredVDiskSet();

        TPutImpl::TPutResultVec putResults;
        PutImpl.Step(LogCtx, putResults, ExpiredVDiskSet, accelerate);
        if (ReplyAndDieWithLastResponse(putResults)) {
            return true;
        }

        // Generate new VPut/VMultiPut events to disks.
        auto events = PutImpl.GeneratePutRequests();

        // Count them properly.
        UpdatePengingVDiskResponseCount(events);
        CountPuts(events);

        // Send to VDisks.
        for (auto& ev : events) {
            std::visit([&](auto& ev) { SendToQueue(std::move(ev), 0, TimeStatsEnabled); }, ev);
            ++RequestsSent;
        }

        return false;
    }

    void Accelerate() {
        if (IsAccelerated) {
            return;
        }
        IsAccelerated = true;
        Action(true);
//        *(IsMultiPutMode ? Mon->NodeMon->AccelerateEvVMultiPutCount : Mon->NodeMon->AccelerateEvVPutCount) += v.size();
    }

    void HandleIncarnation(TMonotonic timestamp, ui32 orderNumber, ui64 incarnationGuid) {
        timestamp += TDuration::Seconds(15); // TODO: cooldown timeout

        Y_ABORT_UNLESS(orderNumber < IncarnationRecords.size());
        auto& record = IncarnationRecords[orderNumber];

        if (record.IncarnationGuid == 0 && record.ExpirationTimestamp == TMonotonic::Max()) { // empty record
            record.ExpirationTimestamp = TMonotonic::Zero();
        } else if (record.IncarnationGuid != incarnationGuid) {
            PutImpl.InvalidatePartStates(orderNumber);
            ++*Mon->NodeMon->IncarnationChanges;
        }

        record.IncarnationGuid = incarnationGuid;
        record.ExpirationTimestamp = Max(timestamp, record.ExpirationTimestamp);
    }

    void UpdateExpiredVDiskSet() {
        const TMonotonic now = TActivationContext::Monotonic();

        TBlobStorageGroupInfo::TGroupVDisks expired(&Info->GetTopology());
        for (ui32 i = 0; i < IncarnationRecords.size(); ++i) {
            if (IncarnationRecords[i].ExpirationTimestamp < now) {
                expired |= {&Info->GetTopology(), Info->GetVDiskId(i)};
            }
        }
        if (expired != ExpiredVDiskSet) { // expired set has changed
            PutImpl.ChangeAll();
        }
        ExpiredVDiskSet = expired;
    }

    void IssueStatusForExpiredDisks() {
        const TMonotonic now = TActivationContext::Monotonic();
        for (ui32 i = 0; i < IncarnationRecords.size(); ++i) {
            auto& record = IncarnationRecords[i];
            if (now + TDuration::Seconds(5) < record.ExpirationTimestamp) {
                continue; // not expired yet
            }
            if (record.StatusIssueTimestamp != TMonotonic()) {
                continue; // TEvVStatus is already in flight
            }

            const TVDiskID vdiskId = Info->GetVDiskId(i);
            A_LOG_INFO_S("BPP15", "sending TEvVStatus VDiskId# " << vdiskId);
            SendToQueue(std::make_unique<TEvBlobStorage::TEvVStatus>(vdiskId), i);
            ++StatusMsgsSent;
            record.StatusIssueTimestamp = now;

            ++*Mon->NodeMon->PutStatusQueries;
        }
    }

    void Handle(TEvBlobStorage::TEvVStatusResult::TPtr& ev) {
        A_LOG_INFO_S("BPP16", "received TEvVStatusResult " << ev->Get()->ToString());

        ProcessReplyFromQueue(ev);
        ++StatusResultMsgsReceived;

        auto& record = ev->Get()->Record;
        const ui32 orderNumber = ev->Cookie;
        auto& incarnationRecord = IncarnationRecords[orderNumber];
        Y_ABORT_UNLESS(incarnationRecord.IncarnationGuid);
        Y_ABORT_UNLESS(incarnationRecord.ExpirationTimestamp != TMonotonic::Max());
        const TMonotonic issue = std::exchange(incarnationRecord.StatusIssueTimestamp, TMonotonic());
        Y_ABORT_UNLESS(issue != TMonotonic());

        if (record.HasIncarnationGuid()) {
            HandleIncarnation(issue, orderNumber, record.GetIncarnationGuid());
        }

        Action();
    }

    void Handle(TEvBlobStorage::TEvVPutResult::TPtr &ev) {
        A_LOG_LOG_S(false, ev->Get()->Record.GetStatus() == NKikimrProto::OK ? NLog::PRI_DEBUG : NLog::PRI_INFO,
            "BPP01", "received " << ev->Get()->ToString() << " from# " << VDiskIDFromVDiskID(ev->Get()->Record.GetVDiskID()));

        ProcessReplyFromQueue(ev);
        ResponsesReceived++;

        const ui64 cyclesPerUs = NHPTimer::GetCyclesPerSecond() / 1000000;
        ev->Get()->Record.MutableTimestamps()->SetReceivedByDSProxyUs(GetCycleCountFast() / cyclesPerUs);
        const NKikimrBlobStorage::TEvVPutResult &record = ev->Get()->Record;
        const TLogoBlobID blobId = LogoBlobIDFromLogoBlobID(record.GetBlobID());
        Y_ABORT_UNLESS(record.HasVDiskID());
        TVDiskID vdiskId = VDiskIDFromVDiskID(record.GetVDiskID());
        const TVDiskIdShort shortId(vdiskId);
        const ui32 vdisk = Info->GetOrderNumber(shortId);
        const NKikimrProto::EReplyStatus status = record.GetStatus();
        const size_t blobIdx = PutImpl.GetBlobIdx(blobId);

        Y_ABORT_UNLESS(vdisk < WaitingVDiskResponseCount.size(), "blobIdx# %zu vdisk# %" PRIu32, blobIdx, vdisk);
        if (WaitingVDiskResponseCount[vdisk] == 1) {
            WaitingVDiskCount--;
        }
        WaitingVDiskResponseCount[vdisk]--;

        if (TimeStatsEnabled && record.GetMsgQoS().HasExecTimeStats()) {
            TimeStats.ApplyPut(PutImpl.Blobs[blobIdx].BufferSize, record.GetMsgQoS().GetExecTimeStats());
        }

        if (record.HasIncarnationGuid()) {
            // TODO: correct timestamp
            HandleIncarnation(TActivationContext::Monotonic(), Info->GetOrderNumber(shortId), record.GetIncarnationGuid());
        }

        LWPROBE(DSProxyVDiskRequestDuration, TEvBlobStorage::EvVPut, blobId.BlobSize(), blobId.TabletID(),
                Info->GroupID, blobId.Channel(), Info->GetFailDomainOrderNumber(shortId),
                GetStartTime(record.GetTimestamps()),
                GetTotalTimeMs(record.GetTimestamps()),
                GetVDiskTimeMs(record.GetTimestamps()),
                GetTotalTimeMs(record.GetTimestamps()) - GetVDiskTimeMs(record.GetTimestamps()),
                NKikimrBlobStorage::EPutHandleClass_Name(PutImpl.GetPutHandleClass()),
                NKikimrProto::EReplyStatus_Name(status));
        //if (RootCauseTrack.IsOn) {
        //    RootCauseTrack.OnReply(cookie.GetCauseIdx(),
        //        GetTotalTimeMs(record.GetTimestamps()) - GetVDiskTimeMs(record.GetTimestamps()),
        //        GetVDiskTimeMs(record.GetTimestamps()));
        //}

        if (status == NKikimrProto::BLOCKED || status == NKikimrProto::DEADLINE) {
            TString error = TStringBuilder() << "Got VPutResult status# " << status << " from VDiskId# " << vdiskId;
            TPutImpl::TPutResultVec putResults;
            PutImpl.PrepareOneReply(status, blobIdx, LogCtx, std::move(error), putResults);
            ReplyAndDieWithLastResponse(putResults);
        } else {
            PutImpl.ProcessResponse(*ev->Get());
            if (Action()) {
                return;
            }
            AccelerateIfNeeded();
            SanityCheck(); // May Die
        }
    }

    void Handle(TEvBlobStorage::TEvVMultiPutResult::TPtr &ev) {
        ProcessReplyFromQueue(ev);
        ResponsesReceived++;

        const ui64 cyclesPerUs = NHPTimer::GetCyclesPerSecond() / 1000000;
        ev->Get()->Record.MutableTimestamps()->SetReceivedByDSProxyUs(GetCycleCountFast() / cyclesPerUs);
        const NKikimrBlobStorage::TEvVMultiPutResult &record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.HasVDiskID());
        const TVDiskID vdiskId = VDiskIDFromVDiskID(record.GetVDiskID());
        const TVDiskIdShort shortId(vdiskId);
        const ui32 vdisk = Info->GetOrderNumber(shortId);
        const NKikimrProto::EReplyStatus status = record.GetStatus();

        if (TimeStatsEnabled && record.GetMsgQoS().HasExecTimeStats()) {
            TimeStats.ApplyPut(RequestBytes, record.GetMsgQoS().GetExecTimeStats());
        }

        if (record.HasIncarnationGuid()) {
            // TODO: correct timestamp
            HandleIncarnation(TActivationContext::Monotonic(), vdisk, record.GetIncarnationGuid());
        }

        auto prio = NLog::PRI_DEBUG;
        if (status != NKikimrProto::OK) {
            prio = NLog::PRI_INFO;
        } else {
            for (const auto& item : record.GetItems()) {
                if (item.GetStatus() != NKikimrProto::OK) {
                    prio = NLog::PRI_INFO;
                }
            }
        }
        A_LOG_LOG_S(false, prio, "BPP02", "received " << ev->Get()->ToString() << " from# " << vdiskId);

        Y_ABORT_UNLESS(vdisk < WaitingVDiskResponseCount.size(), " vdisk# %" PRIu32, vdisk);
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
        //bool isCauseRegistered = !RootCauseTrack.IsOn;
        TPutImpl::TPutResultVec putResults;
        for (auto &item : record.GetItems()) {
            //if (!isCauseRegistered) {
            //    isCauseRegistered = RootCauseTrack.OnReply(cookie.GetCauseIdx(),
            //        GetTotalTimeMs(record.GetTimestamps()) - GetVDiskTimeMs(record.GetTimestamps()),
            //        GetVDiskTimeMs(record.GetTimestamps()));
            //}

            Y_ABORT_UNLESS(item.HasStatus());
            Y_ABORT_UNLESS(item.HasBlobID());
            const TLogoBlobID blobId = LogoBlobIDFromLogoBlobID(item.GetBlobID());
            size_t blobIdx = PutImpl.GetBlobIdx(blobId);
            const NKikimrProto::EReplyStatus itemStatus = item.GetStatus();
            Y_ABORT_UNLESS(itemStatus != NKikimrProto::RACE); // we should get RACE for the whole request and handle it in CheckForTermErrors
            if (itemStatus == NKikimrProto::BLOCKED || itemStatus == NKikimrProto::DEADLINE) {
                ErrorReason = TStringBuilder() << "Got VMultiPutResult itemStatus# " << itemStatus << " from VDiskId# " << vdiskId;
                PutImpl.PrepareOneReply(itemStatus, blobIdx, LogCtx, ErrorReason, putResults);
            }
        }
        if (ReplyAndDieWithLastResponse(putResults)) {
            return;
        }

        PutImpl.ProcessResponse(*ev->Get());
        if (Action()) {
            return;
        }
        AccelerateIfNeeded();
        SanityCheck(); // May Die
    }

    void AccelerateIfNeeded() {
        if (!IsAccelerateScheduled && !IsAccelerated) {
            if (WaitingVDiskCount == 1 && RequestsSent > 1) {
                ui64 timeToAccelerateUs = Max<ui64>(1, PutImpl.GetTimeToAccelerateNs(LogCtx) / 1000);
                TDuration timeSinceStart = TActivationContext::Monotonic() - StartTime;
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
    }

    friend class TBlobStorageGroupRequestActor<TBlobStorageGroupPutRequest>;

    void ReplyAndDie(NKikimrProto::EReplyStatus status) {
        TPutImpl::TPutResultVec putResults;
        PutImpl.PrepareReply(status, LogCtx, ErrorReason, putResults);
        const bool done = ReplyAndDieWithLastResponse(putResults);
        Y_ABORT_UNLESS(done);
    }

    bool ReplyAndDieWithLastResponse(TPutImpl::TPutResultVec& putResults) {
        for (auto& [blobIdx, result] : putResults) {
            Y_ABORT_UNLESS(ResponsesSent != PutImpl.Blobs.size());
            SendReply(std::move(result), blobIdx);
        }
        if (ResponsesSent == PutImpl.Blobs.size()) {
            PassAway();
            Done = true;
            return true;
        }
        return false;
    }

    void SendReply(std::unique_ptr<TEvBlobStorage::TEvPutResult> putResult, size_t blobIdx) {
        NKikimrProto::EReplyStatus status = putResult->Status;
        A_LOG_LOG_S(false, status == NKikimrProto::OK ? NLog::PRI_INFO : NLog::PRI_NOTICE, "BPP21",
            "SendReply putResult# " << putResult->ToString() << " ResponsesSent# " << ResponsesSent
            << " PutImpl.Blobs.size# " << PutImpl.Blobs.size()
            << " Last# " << (ResponsesSent + 1 == PutImpl.Blobs.size() ? "true" : "false"));
        const TDuration duration = TActivationContext::Monotonic() - StartTime;
        TLogoBlobID blobId = putResult->Id;
        TLogoBlobID origBlobId = TLogoBlobID(blobId, 0);
        Mon->CountPutPesponseTime(Info->GetDeviceType(), HandleClass, PutImpl.Blobs[blobIdx].BufferSize, duration);
        *Mon->ActivePutCapacity -= ReportedBytes;
        Y_ABORT_UNLESS(PutImpl.GetHandoffPartsSent() <= Info->Type.TotalPartCount() * MaxHandoffNodes * PutImpl.Blobs.size());
        ++*Mon->HandoffPartsSent[Min(Mon->HandoffPartsSent.size() - 1, (size_t)PutImpl.GetHandoffPartsSent())];
        ReportedBytes = 0;
        const bool success = (status == NKikimrProto::OK);
        LWPROBE(DSProxyRequestDuration, TEvBlobStorage::EvPut,
                blobId.BlobSize(),
                duration.SecondsFloat() * 1000.0,
                blobId.TabletID(), Info->GroupID, blobId.Channel(),
                NKikimrBlobStorage::EPutHandleClass_Name(HandleClass), success);
        ResponsesSent++;
        Y_ABORT_UNLESS(ResponsesSent <= PutImpl.Blobs.size());
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
        for (size_t blobIdx = 0; blobIdx < PutImpl.Blobs.size(); ++blobIdx) {
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
            ui64 cookie, NWilson::TSpan&& span, bool timeStatsEnabled,
            TDiskResponsivenessTracker::TPerDiskStatsPtr stats,
            TMaybe<TGroupStat::EKind> latencyQueueKind, TInstant now,
            TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters,
            bool enableRequestMod3x3ForMinLatecy)
        : TBlobStorageGroupRequestActor(info, state, mon, source, cookie,
                NKikimrServices::BS_PROXY_PUT, false, latencyQueueKind, now, storagePoolCounters,
                ev->RestartCounter, std::move(span), nullptr)
        , PutImpl(info, state, ev, mon, enableRequestMod3x3ForMinLatecy, source, cookie, Span.GetTraceId())
        , WaitingVDiskResponseCount(info->GetTotalVDisksNum())
        , Deadline(ev->Deadline)
        , HandleClass(ev->HandleClass)
        , ReportedBytes(0)
        , TimeStatsEnabled(timeStatsEnabled)
        , Tactic(ev->Tactic)
        , Stats(std::move(stats))
        , IsAccelerated(false)
        , IsAccelerateScheduled(false)
        , IsMultiPutMode(false)
        , IncarnationRecords(info->GetTotalVDisksNum())
        , ExpiredVDiskSet(&info->GetTopology())
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
        : TBlobStorageGroupRequestActor(info, state, mon, TActorId(), 0,
                NKikimrServices::BS_PROXY_PUT, false, latencyQueueKind, now, storagePoolCounters,
                MaxRestartCounter(events), NWilson::TSpan(), nullptr)
        , PutImpl(info, state, events, mon, handleClass, tactic, enableRequestMod3x3ForMinLatecy)
        , WaitingVDiskResponseCount(info->GetTotalVDisksNum())
        , IsManyPuts(true)
        , Deadline(TInstant::Zero())
        , HandleClass(handleClass)
        , ReportedBytes(0)
        , TimeStatsEnabled(timeStatsEnabled)
        , Tactic(tactic)
        , Stats(std::move(stats))
        , IsAccelerated(false)
        , IsAccelerateScheduled(false)
        , IsMultiPutMode(true)
        , IncarnationRecords(info->GetTotalVDisksNum())
        , ExpiredVDiskSet(&info->GetTopology())
    {
        Y_DEBUG_ABORT_UNLESS(events.size() <= MaxBatchedPutRequests);
        for (auto &ev : events) {
            auto& msg = *ev->Get();
            Deadline = Max(Deadline, msg.Deadline);
            if (msg.Orbit.HasShuttles()) {
                RootCauseTrack.IsOn = true;
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

        StartTime = TActivationContext::Monotonic();

        for (size_t blobIdx = 0; blobIdx < PutImpl.Blobs.size(); ++blobIdx) {
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
        CheckRequests(TEvents::TSystem::Bootstrap);
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
            << (TActivationContext::Monotonic() - StartTime) << " seconds");
        if (TInstant::Now() > Deadline) {
            ErrorReason = "Deadline exceeded";
            ReplyAndDie(NKikimrProto::DEADLINE);
            return;
        }
        Schedule(TDuration::MilliSeconds(DsPutWakeupMs), new TKikimrEvents::TEvWakeup);
    }

    void UpdatePengingVDiskResponseCount(const TDeque<TPutImpl::TPutEvent>& putEvents) {
        for (auto& event : putEvents) {
            std::visit([&](auto& event) {
                //Y_ABORT_UNLESS(event->Record.HasCookie());
                //TCookie cookie(event->Record.GetCookie());
                //if (RootCauseTrack.IsOn) {
                //    cookie.SetCauseIdx(RootCauseTrack.RegisterCause());
                //    event->Record.SetCookie(cookie);
                //}
                const ui32 orderNumber = Info->GetOrderNumber(VDiskIDFromVDiskID(event->Record.GetVDiskID()));
                Y_ABORT_UNLESS(orderNumber < WaitingVDiskResponseCount.size());
                WaitingVDiskCount += !WaitingVDiskResponseCount[orderNumber]++;
            }, event);
        }
    }

    void ResumeBootstrap() {
        if (EncodeQuantum()) {
            PutImpl.GenerateInitialRequests(LogCtx, PartSets);
            Action();
            BootstrapInProgress = false;
        } else {
            TActivationContext::Send(new IEventHandle(TEvBlobStorage::EvResume, 0, SelfId(), {}, nullptr, 0));
        }
        SanityCheck();
    }

    void CheckRequests(ui32 type) {
        auto dumpIncarnationRecords = [&] {
            TStringStream s;
            s << '{';
            for (ui32 i = 0; i < IncarnationRecords.size(); ++i) {
                if (i) {
                    s << ' ';
                }
                s << i;
                auto& r = IncarnationRecords[i];
                s << '{' << r.IncarnationGuid
                    << ' ' << (r.ExpirationTimestamp != TMonotonic::Max() ? TStringBuilder() << r.ExpirationTimestamp : "-"_sb)
                    << ' ' << (r.StatusIssueTimestamp != TMonotonic::Zero() ? TStringBuilder() << r.StatusIssueTimestamp : "-"_sb)
                    << '}';
            }
            s << '}';
            return s.Str();
        };
        const TMonotonic now = TActivationContext::Monotonic();
        Y_VERIFY_S(ResponsesSent == PutImpl.Blobs.size()
            || BootstrapInProgress
            || RequestsSent > ResponsesReceived
            || StatusMsgsSent > StatusResultMsgsReceived,
            "query stuck"
            << " Type# 0x" << Sprintf("%08" PRIx32, type)
            << " ResponsesSent# " << ResponsesSent
            << " Blobs.size# " << PutImpl.Blobs.size()
            << " BootstrapInProgress# " << BootstrapInProgress
            << " RequestsSent# " << RequestsSent
            << " ResponsesReceived# " << ResponsesReceived
            << " StatusMsgsSent# " << StatusMsgsSent
            << " StatusResultMsgsReceived# " << StatusResultMsgsReceived
            << " Now# " << now
            << " Passed# " << (now - StartTime)
            << " ExpiredVDiskSet# " << ExpiredVDiskSet.ToString()
            << " IncarnationRecords# " << dumpIncarnationRecords()
            << " State# " << PutImpl.DumpFullState());
    }

    STATEFN(StateWait) {
        if (ProcessEvent(ev, true)) {
            return;
        }
        const ui32 type = ev->GetTypeRewrite();
        switch (type) {
            hFunc(TEvBlobStorage::TEvVStatusResult, Handle);
            hFunc(TEvBlobStorage::TEvVPutResult, Handle);
            hFunc(TEvBlobStorage::TEvVMultiPutResult, Handle);
            hFunc(TEvAccelerate, Handle);
            cFunc(TEvBlobStorage::EvResume, ResumeBootstrap);
            hFunc(TKikimrEvents::TEvWakeup, Handle);

            default:
                Y_DEBUG_ABORT_UNLESS(false, "unexpected event Type# 0x%08" PRIx32, type);
        }
        if (!Done) {
            IssueStatusForExpiredDisks();
        }
        CheckRequests(type);
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
    NWilson::TSpan span(TWilson::BlobStorage, std::move(traceId), "DSProxy.Put");
    if (span) {
        span.Attribute("event", ev->ToString());
    }

    return new TBlobStorageGroupPutRequest(info, state, source, mon, ev, cookie, std::move(span), timeStatsEnabled,
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

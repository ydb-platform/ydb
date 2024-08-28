#include "dsproxy.h"
#include "dsproxy_mon.h"
#include "root_cause.h"
#include "dsproxy_put_impl.h"
#include <ydb/core/blobstorage/dsproxy/dsproxy_request_reporting.h>
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

class TBlobStorageGroupPutRequest : public TBlobStorageGroupRequestActor {
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

    ui64 RequestsSent = 0;
    ui64 ResponsesReceived = 0;
    ui32 RequestsPendingBeforeAcceleration = 0;
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

    TAccelerationParams AccelerationParams;
    ui32 AccelerateRequestsSent = 0;
    bool IsAccelerateScheduled = false;

    bool Done = false;

    NLWTrace::TOrbit Orbit;

    struct TIncarnationRecord {
        ui64 IncarnationGuid = 0;
        TMonotonic ExpirationTimestamp = TMonotonic::Max();
        TMonotonic StatusIssueTimestamp = TMonotonic::Zero(); // zero means no message in flight
    };
    std::vector<TIncarnationRecord> IncarnationRecords;

    TBlobStorageGroupInfo::TGroupVDisks ExpiredVDiskSet;

    TDuration LongRequestThreshold;

    void SanityCheck() {
        if (RequestsSent <= MaxSaneRequests) {
            return;
        }
        TStringStream err;
        err << "Group# " << Info->GroupID
            << " sent over MaxSaneRequests# " << MaxSaneRequests
            << " requests, internal state# " << PutImpl.DumpFullState();
        ErrorReason = err.Str();
        DSP_LOG_CRIT_S("BPG21", ErrorReason);
        ReplyAndDie(NKikimrProto::ERROR);
    }

    void Handle(TEvAccelerate::TPtr &ev) {
        IsAccelerateScheduled = false;
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
        for (const auto& ev : events) {
            CountPut(std::visit([](const auto& item) { return item->GetBufferBytes(); }, ev));
        }

        // Update metrics for acceleration.
        if (accelerate) {
            for (const auto& ev : events) {
                auto getCounter = TOverloaded{
                    [&](const std::unique_ptr<TEvBlobStorage::TEvVPut>&) { return Mon->NodeMon->AccelerateEvVPutCount; },
                    [&](const std::unique_ptr<TEvBlobStorage::TEvVMultiPut>&) { return Mon->NodeMon->AccelerateEvVMultiPutCount; }
                };
                std::visit(getCounter, ev)->Inc();
            }
        }

        // Send to VDisks.
        for (auto& ev : events) {
            if (LWPROBE_ENABLED(DSProxyVPutSent) || Orbit.HasShuttles()) {
                auto vDiskId = std::visit([](const auto& item) { return VDiskIDFromVDiskID(item->Record.GetVDiskID()); }, ev);
                auto itemsCount = std::visit(overloaded{
                    [](const std::unique_ptr<TEvBlobStorage::TEvVPut>&) { return 1; },
                    [](const std::unique_ptr<TEvBlobStorage::TEvVMultiPut>& item) { return item->Record.GetItems().size(); }
                }, ev);
                LWTRACK(
                    DSProxyVPutSent, Orbit,
                    std::visit([](const auto& item) { return item->Type(); }, ev),
                    vDiskId.ToStringWOGeneration(),
                    Info->GetFailDomainOrderNumber(vDiskId),
                    itemsCount,
                    std::visit([](const auto& item) { return item->GetBufferBytes(); }, ev),
                    accelerate
                );
            }
            std::visit([&](auto& ev) { SendToQueue(std::move(ev), 0, TimeStatsEnabled); }, ev);
            ++RequestsSent;
            if (AccelerateRequestsSent == 0) {
                RequestsPendingBeforeAcceleration++;
            }
        }

        return false;
    }

    void Accelerate() {
        if (AccelerateRequestsSent == 2) {
            return;
        }
        PutImpl.RegisterAcceleration();
        ++AccelerateRequestsSent;
        Action(true);
        TryToScheduleNextAcceleration();
    }

    void HandleIncarnation(TMonotonic timestamp, ui32 orderNumber, ui64 incarnationGuid) {
        timestamp += VDiskCooldownTimeoutOnProxy;

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
            DSP_LOG_INFO_S("BPP15", "sending TEvVStatus VDiskId# " << vdiskId);
            SendToQueue(std::make_unique<TEvBlobStorage::TEvVStatus>(vdiskId), i);
            ++StatusMsgsSent;
            record.StatusIssueTimestamp = now;

            ++*Mon->NodeMon->PutStatusQueries;
        }
    }

    void Handle(TEvBlobStorage::TEvVStatusResult::TPtr& ev) {
        DSP_LOG_INFO_S("BPP16", "received TEvVStatusResult " << ev->Get()->ToString());

        ProcessReplyFromQueue(ev->Get());
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
        } else if (record.GetStatus() != NKikimrProto::OK) { // we can't obtain status from the vdisk; assume it has not been written
            Y_ABORT_UNLESS(orderNumber < IncarnationRecords.size());
            IncarnationRecords[orderNumber] = {};
            PutImpl.InvalidatePartStates(orderNumber);
            ++*Mon->NodeMon->IncarnationChanges;
        }

        Action();
    }

    void Handle(TEvBlobStorage::TEvVPutResult::TPtr &ev) {
        DSP_LOG_LOG_S(ev->Get()->Record.GetStatus() == NKikimrProto::OK ? NLog::PRI_DEBUG : NLog::PRI_INFO,
            "BPP01", "received " << ev->Get()->ToString() << " from# " << VDiskIDFromVDiskID(ev->Get()->Record.GetVDiskID()));

        ProcessReplyFromQueue(ev->Get());
        ResponsesReceived++;
        if (!AccelerateRequestsSent) {
            Y_DEBUG_ABORT_UNLESS(RequestsPendingBeforeAcceleration > 0);
            RequestsPendingBeforeAcceleration--;
        }

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

        LWTRACK(DSProxyVDiskRequestDuration, Orbit, TEvBlobStorage::EvVPut, blobId.BlobSize(), blobId.TabletID(),
                Info->GroupID.GetRawId(), blobId.Channel(), Info->GetFailDomainOrderNumber(shortId),
                GetStartTime(record.GetTimestamps()),
                GetTotalTimeMs(record.GetTimestamps()),
                GetVDiskTimeMs(record.GetTimestamps()),
                GetTotalTimeMs(record.GetTimestamps()) - GetVDiskTimeMs(record.GetTimestamps()),
                NKikimrBlobStorage::EPutHandleClass_Name(PutImpl.GetPutHandleClass()),
                NKikimrProto::EReplyStatus_Name(status));
        if (RootCauseTrack.IsOn) {
            RootCauseTrack.OnReply(record.GetCookie(),
                GetTotalTimeMs(record.GetTimestamps()) - GetVDiskTimeMs(record.GetTimestamps()),
                GetVDiskTimeMs(record.GetTimestamps()));
        }

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
            TryToScheduleNextAcceleration();
            SanityCheck(); // May Die
        }
    }

    void Handle(TEvBlobStorage::TEvVMultiPutResult::TPtr &ev) {
        ProcessReplyFromQueue(ev->Get());
        ResponsesReceived++;
        if (!AccelerateRequestsSent) {
            Y_DEBUG_ABORT_UNLESS(RequestsPendingBeforeAcceleration > 0);
            RequestsPendingBeforeAcceleration--;
        }

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
        DSP_LOG_LOG_S(prio, "BPP02", "received " << ev->Get()->ToString() << " from# " << vdiskId);

        Y_ABORT_UNLESS(vdisk < WaitingVDiskResponseCount.size(), " vdisk# %" PRIu32, vdisk);
        if (WaitingVDiskResponseCount[vdisk] == 1) {
            WaitingVDiskCount--;
        }
        WaitingVDiskResponseCount[vdisk]--;

        // Trace put request duration
        if (LWPROBE_ENABLED(DSProxyVDiskRequestDuration) || Orbit.HasShuttles()) {
            for (auto &item : record.GetItems()) {
                TLogoBlobID blobId = LogoBlobIDFromLogoBlobID(item.GetBlobID());
                NKikimrProto::EReplyStatus itemStatus = item.GetStatus();
                LWTRACK(DSProxyVDiskRequestDuration, Orbit,
                        TEvBlobStorage::EvVMultiPut, blobId.BlobSize(), blobId.TabletID(),
                        Info->GroupID.GetRawId(), blobId.Channel(), Info->GetFailDomainOrderNumber(shortId),
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
                isCauseRegistered = RootCauseTrack.OnReply(record.GetCookie(),
                    GetTotalTimeMs(record.GetTimestamps()) - GetVDiskTimeMs(record.GetTimestamps()),
                    GetVDiskTimeMs(record.GetTimestamps()));
            }

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
        TryToScheduleNextAcceleration();
        SanityCheck(); // May Die
    }

    void TryToScheduleNextAcceleration() {
        if (!IsAccelerateScheduled && AccelerateRequestsSent < 2) {
            if (WaitingVDiskCount > 0 && WaitingVDiskCount <= 2 && RequestsSent > 1) {
                ui64 timeToAccelerateUs = Max<ui64>(1, PutImpl.GetTimeToAccelerateNs(LogCtx) / 1000);
                if (RequestsPendingBeforeAcceleration == 1 && AccelerateRequestsSent == 1) {
                    // if there is only one request pending, but first accelerate is unsuccessful, make a pause
                    timeToAccelerateUs *= 2;
                }
                TDuration timeToAccelerate = TDuration::MicroSeconds(timeToAccelerateUs);
                TMonotonic now = TActivationContext::Monotonic();
                TMonotonic nextAcceleration = StartTime + timeToAccelerate;
                LWTRACK(DSProxyScheduleAccelerate, Orbit, nextAcceleration > now ? (nextAcceleration - now).MicroSeconds() / 1000.0 : 0.0);
                if (nextAcceleration > now) {
                    ui64 causeIdx = RootCauseTrack.RegisterAccelerate();
                    Schedule(nextAcceleration - now, new TEvAccelerate(causeIdx));
                    IsAccelerateScheduled = true;
                } else {
                    Accelerate();
                }
            }
        }
    }

    void ReplyAndDie(NKikimrProto::EReplyStatus status) override {
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
        DSP_LOG_LOG_S(status == NKikimrProto::OK ? NLog::PRI_INFO : NLog::PRI_NOTICE, "BPP21",
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
                blobId.TabletID(), Info->GroupID.GetRawId(), blobId.Channel(),
                NKikimrBlobStorage::EPutHandleClass_Name(HandleClass), success);
        ResponsesSent++;
        Y_ABORT_UNLESS(ResponsesSent <= PutImpl.Blobs.size());
        RootCauseTrack.RenderTrack(PutImpl.Blobs[blobIdx].Orbit);
        if (PutImpl.Blobs[blobIdx].Orbit.HasShuttles()) {
            LWTRACK(DSProxyPutReply, PutImpl.Blobs[blobIdx].Orbit, blobId.ToString(), NKikimrProto::EReplyStatus_Name(status), putResult->ErrorReason);
        }
        LWTRACK(DSProxyPutReply, Orbit, blobId.ToString(), NKikimrProto::EReplyStatus_Name(status), putResult->ErrorReason);
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

        if (TActivationContext::Monotonic() - StartTime >= LongRequestThreshold) {
            bool allowToReport = AllowToReport(HandleClass);
            if (allowToReport) {
                DSP_LOG_WARN_S("BPP71", "TEvPut Request was being processed for more than " << LongRequestThreshold
                        << " GroupId# " << Info->GroupID
                        << " HandleClass# " << NKikimrBlobStorage::EPutHandleClass_Name(HandleClass)
                        << " Tactic# " << TEvBlobStorage::TEvPut::TacticName(Tactic)
                        << " RestartCounter# " << RestartCounter
                        << " History# " << PutImpl.PrintHistory());
            }
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

    std::unique_ptr<IEventBase> RestartQuery(ui32 counter) override {
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
                put = new TEvBlobStorage::TEvPut(item.BlobId, TRcBuf(item.Buffer), item.Deadline, HandleClass, Tactic),
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
    ::NMonitoring::TDynamicCounters::TCounterPtr& GetActiveCounter() const override {
        return Mon->ActivePut;
    }

    ERequestType GetRequestType() const override {
        return ERequestType::Put;
    }

    TBlobStorageGroupPutRequest(TBlobStorageGroupPutParameters& params)
        : TBlobStorageGroupRequestActor(params)
        , PutImpl(Info, GroupQueues, params.Common.Event, Mon,
                params.EnableRequestMod3x3ForMinLatency, params.Common.Source,
                params.Common.Cookie, Span.GetTraceId(), params.AccelerationParams)
        , WaitingVDiskResponseCount(Info->GetTotalVDisksNum())
        , HandleClass(params.Common.Event->HandleClass)
        , ReportedBytes(0)
        , TimeStatsEnabled(params.TimeStatsEnabled)
        , Tactic(params.Common.Event->Tactic)
        , Stats(std::move(params.Stats))
        , AccelerationParams(params.AccelerationParams)
        , IncarnationRecords(Info->GetTotalVDisksNum())
        , ExpiredVDiskSet(&Info->GetTopology())
        , LongRequestThreshold(params.LongRequestThreshold)
    {
        if (params.Common.Event->Orbit.HasShuttles()) {
            RootCauseTrack.IsOn = true;
        }
        ReportBytes(PutImpl.Blobs[0].Buffer.capacity() + sizeof(*this));

        RequestBytes = params.Common.Event->Buffer.size();
        RequestHandleClass = HandleClassToHandleClass(HandleClass);
        MaxSaneRequests = Info->Type.TotalPartCount() * (1ull + Info->Type.Handoff()) * 2;
    }

    TBlobStorageGroupPutRequest(TBlobStorageGroupMultiPutParameters& params)
        : TBlobStorageGroupRequestActor(params)
        , PutImpl(Info, GroupQueues, params.Events, Mon, params.HandleClass, params.Tactic,
                params.EnableRequestMod3x3ForMinLatency, params.AccelerationParams)
        , WaitingVDiskResponseCount(Info->GetTotalVDisksNum())
        , IsManyPuts(true)
        , HandleClass(params.HandleClass)
        , ReportedBytes(0)
        , TimeStatsEnabled(params.TimeStatsEnabled)
        , Tactic(params.Tactic)
        , Stats(std::move(params.Stats))
        , AccelerationParams(params.AccelerationParams)
        , IncarnationRecords(Info->GetTotalVDisksNum())
        , ExpiredVDiskSet(&Info->GetTopology())
        , LongRequestThreshold(params.LongRequestThreshold)
    {
        Y_DEBUG_ABORT_UNLESS(params.Events.size() <= MaxBatchedPutRequests);
        for (auto &ev : params.Events) {
            auto& msg = *ev->Get();
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
        MaxSaneRequests = Info->Type.TotalPartCount() * (1ull + Info->Type.Handoff()) * 2;
    }

    void ReportBytes(i64 bytes) {
        *Mon->ActivePutCapacity += bytes;
        ReportedBytes += bytes;
    }

    void Bootstrap() override {
        DSP_LOG_INFO_S("BPP13", "bootstrap"
            << " ActorId# " << SelfId()
            << " Group# " << Info->GroupID
            << " BlobCount# " << PutImpl.Blobs.size()
            << " BlobIDs# " << BlobIdSequenceToString()
            << " HandleClass# " << NKikimrBlobStorage::EPutHandleClass_Name(HandleClass)
            << " Tactic# " << TEvBlobStorage::TEvPut::TacticName(Tactic)
            << " RestartCounter# " << RestartCounter);

        StartTime = TActivationContext::Monotonic();

        for (size_t blobIdx = 0; blobIdx < PutImpl.Blobs.size(); ++blobIdx) {
            LWTRACK(DSProxyPutBootstrapStart, PutImpl.Blobs[blobIdx].Orbit);
        }

        auto getTotalSize = [&]() {
            ui64 totalSize = 0;
            for (auto& blob : PutImpl.Blobs) {
                totalSize += blob.BufferSize;
            }
            return totalSize;
        };
        LWTRACK(
            DSProxyPutRequest, Orbit,
            Info->GroupID.GetRawId(),
            DeviceTypeStr(Info->GetDeviceType(), true),
            NKikimrBlobStorage::EPutHandleClass_Name(HandleClass),
            TEvBlobStorage::TEvPut::TacticName(Tactic),
            PutImpl.Blobs.size(),
            getTotalSize()
        );

        Become(&TBlobStorageGroupPutRequest::StateWait, TDuration::MilliSeconds(DsPutWakeupMs), new TKikimrEvents::TEvWakeup);

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

    void HandleWakeup() {
        DSP_LOG_WARN_S("BPP14", "Wakeup "
            << " ActorId# " << SelfId()
            << " Group# " << Info->GroupID
            << " BlobIDs# " << BlobIdSequenceToString()
            << " Not answered in "
            << (TActivationContext::Monotonic() - StartTime) << " seconds");
        const TInstant now = TActivationContext::Now();
        TPutImpl::TPutResultVec putResults;
        for (size_t blobIdx = 0; blobIdx < PutImpl.Blobs.size(); ++blobIdx) {
            if (!PutImpl.Blobs[blobIdx].Replied && now > PutImpl.Blobs[blobIdx].Deadline) {
                PutImpl.PrepareOneReply(NKikimrProto::DEADLINE, blobIdx, LogCtx, "Deadline timer hit", putResults);
            }
        }
        ReplyAndDieWithLastResponse(putResults);
        Schedule(TDuration::MilliSeconds(DsPutWakeupMs), new TKikimrEvents::TEvWakeup);
    }

    void UpdatePengingVDiskResponseCount(const TDeque<TPutImpl::TPutEvent>& putEvents) {
        for (auto& event : putEvents) {
            std::visit([&](auto& event) {
                ui64 causeIdx = RootCauseTrack.RegisterCause();
                if (event->Record.HasCookie() && RootCauseTrack.IsOn) {
                    event->Record.SetCookie(causeIdx);
                }
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
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);

            default:
                Y_DEBUG_ABORT("unexpected event Type# 0x%08" PRIx32, type);
        }
        if (!Done) {
            IssueStatusForExpiredDisks();
        }
        CheckRequests(type);
    }
};

IActor* CreateBlobStorageGroupPutRequest(TBlobStorageGroupPutParameters params) {
    return new TBlobStorageGroupPutRequest(params);
}

IActor* CreateBlobStorageGroupPutRequest(TBlobStorageGroupMultiPutParameters params) {
    return new TBlobStorageGroupPutRequest(params);
}

}//NKikimr

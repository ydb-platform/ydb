#include "dsproxy.h"
#include "dsproxy_mon.h"
#include "dsproxy_quorum_tracker.h"
#include <ydb/core/blobstorage/base/utility.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>

namespace NKikimr {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// COLLECT request
// Blobs with generation < CollectGeneration, or generation == CollectGeneration and step <= CollectStep are collected.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TBlobStorageGroupCollectGarbageRequest : public TBlobStorageGroupRequestActor {
    const ui64 TabletId;
    const ui32 RecordGeneration;
    const ui32 PerGenerationCounter;
    const ui32 Channel;
    const TInstant Deadline;
    std::unique_ptr<TVector<TLogoBlobID> > Keep;
    std::unique_ptr<TVector<TLogoBlobID> > DoNotKeep;
    const ui32 CollectGeneration;
    const ui32 CollectStep;
    const bool Hard;
    const bool Collect;
    const bool Decommission;

    TGroupQuorumTracker QuorumTracker;
    TInstant StartTime;

    ui32 RequestsSent = 0;
    ui32 ResponsesReceived = 0;

    void Handle(TEvBlobStorage::TEvVCollectGarbageResult::TPtr &ev) {
        ProcessReplyFromQueue(ev->Get());
        ResponsesReceived++;
        const NKikimrBlobStorage::TEvVCollectGarbageResult &record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.HasStatus());
        const NKikimrProto::EReplyStatus status = record.GetStatus();
        Y_ABORT_UNLESS(record.HasVDiskID());
        const TVDiskID vdisk = VDiskIDFromVDiskID(record.GetVDiskID());

        A_LOG_LOG_S(false, PriorityForStatusInbound(status), "DSPC01", "received"
               << " TEvVCollectGarbageResult# " << ev->Get()->ToString());

        Process(status, vdisk, record.HasIncarnationGuid() ? std::make_optional(record.GetIncarnationGuid()) : std::nullopt);
        CheckProgress();
    }

    void Handle(TEvBlobStorage::TEvVStatusResult::TPtr &ev) {
        ProcessReplyFromQueue(ev->Get());
        ResponsesReceived++;
        const auto& record = ev->Get()->Record;
        if (record.HasStatus() && record.HasVDiskID()) {
            Process(record.GetStatus(), VDiskIDFromVDiskID(record.GetVDiskID()), record.HasIncarnationGuid()
                ? std::make_optional(record.GetIncarnationGuid()) : std::nullopt);
        }
        CheckProgress();
    }

    void Process(NKikimrProto::EReplyStatus status, const TVDiskID& vdisk, std::optional<ui64> incarnationGuid) {
        std::vector<TVDiskID> queryStatus, resend;
        // replace already status to be treated as non-terminating OK for this kind of request
        status = status != NKikimrProto::ALREADY ? status : NKikimrProto::OK;
        switch (NKikimrProto::EReplyStatus newStatus = incarnationGuid
                ? QuorumTracker.ProcessReplyWithCooldown(vdisk, status, TActivationContext::Now(), *incarnationGuid, queryStatus, resend)
                : QuorumTracker.ProcessReply(vdisk, status)) {
            case NKikimrProto::OK:
                return ReplyAndDie(newStatus);

            case NKikimrProto::UNKNOWN:
                break;

            case NKikimrProto::ERROR:
            case NKikimrProto::VDISK_ERROR_STATE:
            case NKikimrProto::OUT_OF_SPACE:
                {
                    TStringStream str;
                    str << "Processed status# " << status << " from VDisk# " << vdisk;
                    if (incarnationGuid) {
                        str << " incarnationGuid# " << *incarnationGuid;
                    } else {
                        str << " incarnationGuid# empty";
                    }
                    str << " QuorumTracker status# " << newStatus;
                    ErrorReason = str.Str();
                }
                return ReplyAndDie(NKikimrProto::ERROR);

            default:
                Y_ABORT("unexpected newStatus# %s", NKikimrProto::EReplyStatus_Name(newStatus).data());
        }
        for (const TVDiskID& vdiskId : queryStatus) {
            SendToQueue(std::make_unique<TEvBlobStorage::TEvVStatus>(vdiskId), 0);
            RequestsSent++;
        }
        for (const TVDiskID& vdiskId : resend) {
            SendCollectGarbageRequest(vdiskId);
        }
    }

    void CheckProgress() {
        Y_ABORT_UNLESS(Dead || ResponsesReceived < RequestsSent, "No more unreplied vdisk requests!"
            " QuorumTracker# %s RequestsSent# %" PRIu32 " ResponsesReceived# %" PRIu32,
            QuorumTracker.ToString().c_str(), RequestsSent, ResponsesReceived);
    }

    void ReplyAndDie(NKikimrProto::EReplyStatus status) override {
        auto result = std::make_unique<TEvBlobStorage::TEvCollectGarbageResult>(status, TabletId, RecordGeneration,
            PerGenerationCounter, Channel);
        result->ErrorReason = ErrorReason;
        A_LOG_LOG_S(true, status == NKikimrProto::OK ? NLog::PRI_INFO : NLog::PRI_NOTICE, "DSPC02", "Result# " << result->Print(false));
        SendResponseAndDie(std::move(result));
    }

    void SendCollectGarbageRequest(const TVDiskID& vdiskId) {
        const ui64 cookie = TVDiskIdShort(vdiskId).GetRaw();
        auto msg = std::make_unique<TEvBlobStorage::TEvVCollectGarbage>(TabletId, RecordGeneration, PerGenerationCounter,
            Channel, Collect, CollectGeneration, CollectStep, Hard, Keep.get(), DoNotKeep.get(), vdiskId, Deadline);
        SendToQueue(std::move(msg), cookie);
        RequestsSent++;
    }

    std::unique_ptr<IEventBase> RestartQuery(ui32 counter) override {
        ++*Mon->NodeMon->RestartCollectGarbage;
        auto ev = std::make_unique<TEvBlobStorage::TEvCollectGarbage>(TabletId, RecordGeneration, PerGenerationCounter,
            Channel, Collect, CollectGeneration, CollectStep, Keep.release(), DoNotKeep.release(), Deadline, false, Hard);
        ev->RestartCounter = counter;
        ev->Decommission = Decommission;
        return ev;
    }

public:
    ::NMonitoring::TDynamicCounters::TCounterPtr& GetActiveCounter() const override {
        return Mon->ActiveCollectGarbage;
    }

    ERequestType GetRequestType() const override {
        return ERequestType::CollectGarbage;
    }

    TBlobStorageGroupCollectGarbageRequest(const TIntrusivePtr<TBlobStorageGroupInfo> &info,
            const TIntrusivePtr<TGroupQueues> &state, const TActorId &source,
            const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon, TEvBlobStorage::TEvCollectGarbage *ev, ui64 cookie,
            NWilson::TTraceId traceId, TInstant now, TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters)
        : TBlobStorageGroupRequestActor(info, state, mon, source, cookie,
                NKikimrServices::BS_PROXY_COLLECT, false, {}, now, storagePoolCounters, ev->RestartCounter,
                std::move(traceId), "DSProxy.CollectGarbage", ev, std::move(ev->ExecutionRelay),
                NKikimrServices::TActivity::BS_GROUP_COLLECT_GARBAGE)
        , TabletId(ev->TabletId)
        , RecordGeneration(ev->RecordGeneration)
        , PerGenerationCounter(ev->PerGenerationCounter)
        , Channel(ev->Channel)
        , Deadline(ev->Deadline)
        , Keep(ev->Keep.Release())
        , DoNotKeep(ev->DoNotKeep.Release())
        , CollectGeneration(ev->CollectGeneration)
        , CollectStep(ev->CollectStep)
        , Hard(ev->Hard)
        , Collect(ev->Collect)
        , Decommission(ev->Decommission)
        , QuorumTracker(Info.Get())
        , StartTime(now)
    {}

    void Bootstrap() override {
        A_LOG_INFO_S("DSPC03", "bootstrap"
            << " ActorId# " << SelfId()
            << " Group# " << Info->GroupID
            << " TabletId# " << TabletId
            << " Channel# " << Channel
            << " RecordGeneration# " << RecordGeneration
            << " PerGenerationCounter# " << PerGenerationCounter
            << " Deadline# " << Deadline
            << " CollectGeneration# " << CollectGeneration
            << " CollectStep# " << CollectStep
            << " Collect# " << (Collect ? "true" : "false")
            << " Hard# " << (Hard ? "true" : "false")
            << " RestartCounter# " << RestartCounter);

        for (const auto& item : Keep ? *Keep : TVector<TLogoBlobID>()) {
            A_LOG_INFO_S("DSPC04", "Keep# " << item);
        }

        for (const auto& item : DoNotKeep ? *DoNotKeep : TVector<TLogoBlobID>()) {
            A_LOG_INFO_S("DSPC05", "DoNotKeep# " << item);
        }

        for (const auto& vdisk : Info->GetVDisks()) {
            SendCollectGarbageRequest(Info->GetVDiskId(vdisk.OrderNumber));
        }

        Become(&TBlobStorageGroupCollectGarbageRequest::StateWait);
    }

    STATEFN(StateWait) {
        if (ProcessEvent(ev)) {
            return;
        }
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvVCollectGarbageResult, Handle);
            hFunc(TEvBlobStorage::TEvVStatusResult, Handle);
        }
    }
};

IActor* CreateBlobStorageGroupCollectGarbageRequest(const TIntrusivePtr<TBlobStorageGroupInfo> &info,
        const TIntrusivePtr<TGroupQueues> &state, const TActorId &source,
        const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon, TEvBlobStorage::TEvCollectGarbage *ev,
        ui64 cookie, NWilson::TTraceId traceId, TInstant now, TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters) {
    return new TBlobStorageGroupCollectGarbageRequest(info, state, source, mon, ev, cookie, std::move(traceId), now,
        storagePoolCounters);
}

} // NKikimr

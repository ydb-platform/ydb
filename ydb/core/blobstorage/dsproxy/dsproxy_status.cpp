#include "dsproxy.h"
#include "dsproxy_mon.h"
#include "dsproxy_quorum_tracker.h"
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>

namespace NKikimr {
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// STATUS request
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TBlobStorageGroupStatusRequest : public TBlobStorageGroupRequestActor {
    const TInstant Deadline;

    TStorageStatusFlags StatusFlags;
    ui64 Requests;
    ui64 Responses;
    TGroupQuorumTracker QuorumTracker;
    std::optional<float> ApproximateFreeSpaceShare;

    void Handle(TEvBlobStorage::TEvVStatusResult::TPtr &ev) {
        ProcessReplyFromQueue(ev->Get());
        const NKikimrBlobStorage::TEvVStatusResult& record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.HasStatus());
        const NKikimrProto::EReplyStatus status = record.GetStatus();
        Y_ABORT_UNLESS(record.HasVDiskID());
        const TVDiskID vdisk = VDiskIDFromVDiskID(record.GetVDiskID());

        A_LOG_LOG_S(false, PriorityForStatusInbound(status), "DSPS01", "Handle TEvVStatusResult"
            << " status# " << NKikimrProto::EReplyStatus_Name(status).data()
            << " From# " << vdisk.ToString()
            << " StatusFlags# " << (record.HasStatusFlags() ? Sprintf("%" PRIx32, record.GetStatusFlags()).data() : "NA")
            << " NodeId# " << Info->GetActorId(vdisk).NodeId());

        if (record.HasStatusFlags()) {
            StatusFlags.Merge(record.GetStatusFlags());
        }
        if (record.HasApproximateFreeSpaceShare()) {
            const float value = record.GetApproximateFreeSpaceShare();
            ApproximateFreeSpaceShare = Min(ApproximateFreeSpaceShare.value_or(value), value);
        }
        ++Responses;

        switch (const NKikimrProto::EReplyStatus overallStatus = QuorumTracker.ProcessReply(vdisk, status)) {
            case NKikimrProto::OK:
                if (Responses == Requests) {
                    ReplyAndDie(NKikimrProto::OK);
                }
                break;

            case NKikimrProto::ERROR:
                ReplyAndDie(NKikimrProto::ERROR);
                break;

            default:
                break;
        }
    }

    void ReplyAndDie(NKikimrProto::EReplyStatus status) override {
        auto result = std::make_unique<TEvBlobStorage::TEvStatusResult>(status, StatusFlags.Raw);
        if (ApproximateFreeSpaceShare) {
            result->ApproximateFreeSpaceShare = *ApproximateFreeSpaceShare;
        }
        result->ErrorReason = ErrorReason;
        A_LOG_DEBUG_S("DSPS03", "ReplyAndDie Result# " << result->Print(false));
        SendResponseAndDie(std::move(result));
    }

    std::unique_ptr<IEventBase> RestartQuery(ui32 counter) override {
        ++*Mon->NodeMon->RestartStatus;
        auto ev = std::make_unique<TEvBlobStorage::TEvStatus>(Deadline);
        ev->RestartCounter = counter;
        return ev;
    }

public:
    ::NMonitoring::TDynamicCounters::TCounterPtr& GetActiveCounter() const override {
        return Mon->ActiveStatus;
    }

    ERequestType GetRequestType() const override {
        return ERequestType::Status;
    }

    TBlobStorageGroupStatusRequest(const TIntrusivePtr<TBlobStorageGroupInfo> &info,
            const TIntrusivePtr<TGroupQueues> &state, const TActorId &source,
            const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon, TEvBlobStorage::TEvStatus *ev,
            ui64 cookie, NWilson::TTraceId traceId, TInstant now, TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters)
        : TBlobStorageGroupRequestActor(info, state, mon, source, cookie,
                NKikimrServices::BS_PROXY_STATUS, false, {}, now, storagePoolCounters, ev->RestartCounter,
                std::move(traceId), "DSProxy.Status", ev, std::move(ev->ExecutionRelay),
                NKikimrServices::TActivity::BS_PROXY_STATUS_ACTOR)
        , Deadline(ev->Deadline)
        , Requests(0)
        , Responses(0)
        , QuorumTracker(Info.Get())
    {}

    void Bootstrap() override {
        A_LOG_INFO_S("DSPS05", "bootstrap"
            << " ActorId# " << SelfId()
            << " Group# " << Info->GroupID
            << " Deadline# " << Deadline
            << " RestartCounter# " << RestartCounter);

        for (const auto& vdisk : Info->GetVDisks()) {
            const ui64 cookie = TVDiskIdShort(Info->GetVDiskId(vdisk.OrderNumber)).GetRaw();

            auto vd = Info->GetVDiskId(vdisk.OrderNumber);
            A_LOG_DEBUG_S("DSPS04", "Sending TEvVStatus"
                << " vDiskId# " << vd
                << " node# " << Info->GetActorId(vd).NodeId());

            auto msg = std::make_unique<TEvBlobStorage::TEvVStatus>(vd);
            SendToQueue(std::move(msg), cookie);
            ++Requests;
        }

        Become(&TBlobStorageGroupStatusRequest::StateWait);

        if (Requests == 0) {
            ReplyAndDie(NKikimrProto::OK);
        }
    }

    STATEFN(StateWait) {
        if (ProcessEvent(ev)) {
            return;
        }
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvVStatusResult, Handle);
        }
    }
};

IActor* CreateBlobStorageGroupStatusRequest(const TIntrusivePtr<TBlobStorageGroupInfo> &info,
        const TIntrusivePtr<TGroupQueues> &state, const TActorId &source,
        const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon, TEvBlobStorage::TEvStatus *ev,
        ui64 cookie, NWilson::TTraceId traceId, TInstant now, TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters) {
    return new TBlobStorageGroupStatusRequest(info, state, source, mon, ev, cookie, std::move(traceId), now, storagePoolCounters);
}

} // NKikimr

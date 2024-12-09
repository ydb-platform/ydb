#include "dsproxy.h"
#include "dsproxy_mon.h"
#include "dsproxy_quorum_tracker.h"
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>

namespace NKikimr {
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// STATUS request
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TBlobStorageGroupStatusRequest : public TBlobStorageGroupRequestActor<TBlobStorageGroupStatusRequest> {
    const TInstant Deadline;

    TStorageStatusFlags StatusFlags;
    ui64 Requests;
    ui64 Responses;
    TGroupQuorumTracker QuorumTracker;
    std::optional<float> ApproximateFreeSpaceShare;

    void Handle(TEvBlobStorage::TEvVStatusResult::TPtr &ev) {
        ProcessReplyFromQueue(ev);
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

    friend class TBlobStorageGroupRequestActor<TBlobStorageGroupStatusRequest>;
    void ReplyAndDie(NKikimrProto::EReplyStatus status) {
        auto result = std::make_unique<TEvBlobStorage::TEvStatusResult>(status, StatusFlags.Raw);
        if (ApproximateFreeSpaceShare) {
            result->ApproximateFreeSpaceShare = *ApproximateFreeSpaceShare;
        }
        result->ErrorReason = ErrorReason;
        A_LOG_DEBUG_S("DSPS03", "ReplyAndDie Result# " << result->Print(false));
        SendResponseAndDie(std::move(result));
    }

    std::unique_ptr<IEventBase> RestartQuery(ui32 counter) {
        ++*Mon->NodeMon->RestartStatus;
        auto ev = std::make_unique<TEvBlobStorage::TEvStatus>(Deadline);
        ev->RestartCounter = counter;
        return ev;
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BS_PROXY_STATUS_ACTOR;;
    }

    static const auto& ActiveCounter(const TIntrusivePtr<TBlobStorageGroupProxyMon>& mon) {
        return mon->ActiveStatus;
    }

    TBlobStorageGroupStatusRequest(TBlobStorageGroupStatusParameters& params)
        : TBlobStorageGroupRequestActor(params)
        , Deadline(params.Common.Event->Deadline)
        , Requests(0)
        , Responses(0)
        , QuorumTracker(Info.Get())
    {}

    void Bootstrap() {
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

        Become(&TThis::StateWait);

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

IActor* CreateBlobStorageGroupStatusRequest(TBlobStorageGroupStatusParameters params, NWilson::TTraceId traceId) {
    params.Common.Span = NWilson::TSpan(TWilson::BlobStorage, std::move(traceId), "DSProxy.Status");
    return new TBlobStorageGroupStatusRequest(params);
}

} // NKikimr

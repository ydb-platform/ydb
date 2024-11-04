#include "dsproxy.h"
#include "dsproxy_mon.h"
#include "dsproxy_quorum_tracker.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>

namespace NKikimr {
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// BLOCK request
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Both block and get block must operate in terms of FailDomains, not VDisks
// TODO: Get response should wait for 2 copies on mirror, not 1
class TBlobStorageGroupBlockRequest : public TBlobStorageGroupRequestActor<TBlobStorageGroupBlockRequest> {
    const ui64 TabletId;
    const ui32 Generation;
    const TInstant Deadline;
    const ui64 IssuerGuid;
    bool SeenAlready = false;

    TGroupQuorumTracker QuorumTracker;

    void Handle(TEvBlobStorage::TEvVBlockResult::TPtr &ev) {
        ProcessReplyFromQueue(ev);
        const NKikimrBlobStorage::TEvVBlockResult &record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.HasStatus());
        const NKikimrProto::EReplyStatus status = record.GetStatus();
        Y_ABORT_UNLESS(record.HasVDiskID());
        const TVDiskID vdisk = VDiskIDFromVDiskID(record.GetVDiskID());
        const TVDiskIdShort shortId(ev->Cookie);

        Y_ABORT_UNLESS(shortId.FailRealm == vdisk.FailRealm &&
                shortId.FailDomain == vdisk.FailDomain &&
                shortId.VDisk == vdisk.VDisk,
                "VDiskId does not match the cookie, cookie# %s VDiskId# %s",
                shortId.ToString().c_str(), vdisk.ToString().c_str());
        // You can't call GetActorId before calling IsValidId
        Y_ABORT_UNLESS(Info->IsValidId(shortId), "Invalid VDiskId VDiskId# %s", shortId.ToString().c_str());

        A_LOG_LOG_S(false, PriorityForStatusInbound(status), "DSPB01", "Handle TEvVBlockResult"
            << " status# " << NKikimrProto::EReplyStatus_Name(status).data()
            << " From# " << vdisk.ToString()
            << " NodeId# " << Info->GetActorId(vdisk).NodeId());

        Process(status, vdisk, record.HasIncarnationGuid() ? std::make_optional(record.GetIncarnationGuid()) : std::nullopt);
    }

    void Handle(TEvBlobStorage::TEvVStatusResult::TPtr &ev) {
        ProcessReplyFromQueue(ev);
        const auto& record = ev->Get()->Record;
        if (record.HasStatus() && record.HasVDiskID()) {
            Process(record.GetStatus(), VDiskIDFromVDiskID(record.GetVDiskID()), record.HasIncarnationGuid()
                ? std::make_optional(record.GetIncarnationGuid()) : std::nullopt);
        }
    }

    void Process(NKikimrProto::EReplyStatus status, const TVDiskID& vdisk, std::optional<ui64> incarnationGuid) {
        std::vector<TVDiskID> queryStatus, resend;
        if (status == NKikimrProto::ALREADY) {
            // ALREADY means that newly arrived Block is the same or older than existing one; we treat it as ERROR here
            // and reply with ALREADY only when no quorum could be obtained during the whole operation
            SeenAlready = true;
            status = NKikimrProto::ERROR;
        }
        switch (NKikimrProto::EReplyStatus newStatus = incarnationGuid
                ? QuorumTracker.ProcessReplyWithCooldown(vdisk, status, TActivationContext::Now(), *incarnationGuid, queryStatus, resend)
                : QuorumTracker.ProcessReply(vdisk, status)) {
            case NKikimrProto::OK:
                return ReplyAndDie(newStatus);

            case NKikimrProto::UNKNOWN:
                break;

            case NKikimrProto::ERROR: {
                TStringStream err;
                newStatus = SeenAlready ? NKikimrProto::ALREADY : NKikimrProto::ERROR;
                err << "Status# " << NKikimrProto::EReplyStatus_Name(newStatus)
                    << " From# " << vdisk.ToString()
                    << " NodeId# " << Info->GetActorId(vdisk).NodeId()
                    << " QuorumTracker# ";
                QuorumTracker.Output(err);
                ErrorReason = err.Str();
                return ReplyAndDie(newStatus);
            }

            default:
                Y_ABORT("unexpected newStatus# %s", NKikimrProto::EReplyStatus_Name(newStatus).data());
        }
        for (const TVDiskID& vdiskId : queryStatus) {
            SendToQueue(std::make_unique<TEvBlobStorage::TEvVStatus>(vdiskId), 0);
        }
        for (const TVDiskID& vdiskId : resend) {
            SendBlockRequest(vdiskId);
        }
    }

    friend class TBlobStorageGroupRequestActor<TBlobStorageGroupBlockRequest>;
    void ReplyAndDie(NKikimrProto::EReplyStatus status) {
        std::unique_ptr<TEvBlobStorage::TEvBlockResult> result(new TEvBlobStorage::TEvBlockResult(status));
        result->ErrorReason = ErrorReason;
        A_LOG_LOG_S(true, PriorityForStatusResult(status), "DSPB04", "Result# " << result->Print(false));
        Mon->CountBlockResponseTime(TActivationContext::Monotonic() - RequestStartTime);
        return SendResponseAndDie(std::move(result));
    }

    void SendBlockRequest(const TVDiskID& vdiskId) {
        const ui64 cookie = TVDiskIdShort(vdiskId).GetRaw();

        A_LOG_DEBUG_S("DSPB03", "Sending TEvVBlock Tablet# " << TabletId
            << " Generation# " << Generation
            << " vdiskId# " << vdiskId
            << " node# " << Info->GetActorId(vdiskId).NodeId());

        auto msg = std::make_unique<TEvBlobStorage::TEvVBlock>(TabletId, Generation, vdiskId, Deadline, IssuerGuid);
        SendToQueue(std::move(msg), cookie);
    }

    std::unique_ptr<IEventBase> RestartQuery(ui32 counter) {
        ++*Mon->NodeMon->RestartBlock;
        auto ev = std::make_unique<TEvBlobStorage::TEvBlock>(TabletId, Generation, Deadline, IssuerGuid);
        ev->RestartCounter = counter;
        return ev;
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BS_GROUP_BLOCK;
    }

    static const auto& ActiveCounter(const TIntrusivePtr<TBlobStorageGroupProxyMon>& mon) {
        return mon->ActiveBlock;
    }

    TBlobStorageGroupBlockRequest(TBlobStorageGroupBlockParameters& params)
        : TBlobStorageGroupRequestActor(params)
        , TabletId(params.Common.Event->TabletId)
        , Generation(params.Common.Event->Generation)
        , Deadline(params.Common.Event->Deadline)
        , IssuerGuid(params.Common.Event->IssuerGuid)
        , QuorumTracker(Info.Get())
    {}

    void Bootstrap() {
        A_LOG_DEBUG_S("DSPB05", "bootstrap"
            << " ActorId# " << SelfId()
            << " Group# " << Info->GroupID
            << " TabletId# " << TabletId
            << " Generation# " << Generation
            << " Deadline# " << Deadline
            << " RestartCounter# " << RestartCounter);

        for (const auto& vdisk : Info->GetVDisks()) {
            SendBlockRequest(Info->GetVDiskId(vdisk.OrderNumber));
        }

        Become(&TThis::StateWait);
    }

    STATEFN(StateWait) {
        if (ProcessEvent(ev)) {
            return;
        }
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvVBlockResult, Handle);
            hFunc(TEvBlobStorage::TEvVStatusResult, Handle);
        }
    }
};

IActor* CreateBlobStorageGroupBlockRequest(TBlobStorageGroupBlockParameters params, NWilson::TTraceId traceId) {
    NWilson::TSpan span(TWilson::BlobStorage, std::move(traceId), "DSProxy.Block");
    if (span) {
        span.Attribute("event", params.Common.Event->ToString());
    }
    params.Common.Span = std::move(span);
    return new TBlobStorageGroupBlockRequest(params);
}

} // NKikimr

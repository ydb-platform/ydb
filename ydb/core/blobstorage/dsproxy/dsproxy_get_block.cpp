#include "dsproxy.h"
#include "dsproxy_mon.h"
#include "dsproxy_quorum_tracker.h"
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>

namespace NKikimr {
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// GET BLOCK request
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TBlobStorageGroupGetBlockRequest : public TBlobStorageGroupRequestActor {
    const ui64 TabletId;
    ui64 Generation;
    const TInstant Deadline;
    ui64 Requests = 0;
    ui64 Responses = 0;
    TGroupQuorumTracker QuorumTracker;

    void Handle(TEvBlobStorage::TEvVGetBlockResult::TPtr &ev) {
        ProcessReplyFromQueue(ev->Get());
        const NKikimrBlobStorage::TEvVGetBlockResult &record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.HasStatus());
        NKikimrProto::EReplyStatus status = record.GetStatus();
        Y_ABORT_UNLESS(record.HasVDiskID());
        const TVDiskID vdisk = VDiskIDFromVDiskID(record.GetVDiskID());

        DSP_LOG_LOG_S(PriorityForStatusInbound(status), "DSPGB01", "Handle TEvVGetBlockResult"
            << " status# " << NKikimrProto::EReplyStatus_Name(status)
            << " From# " << vdisk.ToString()
            << " NodeId# " << Info->GetActorId(vdisk).NodeId());

        if (record.HasGeneration()) {
            Generation = Max<ui32>(Generation, record.GetGeneration());
        }
        if (status == NKikimrProto::NODATA) {
            status = NKikimrProto::OK;  // assume OK for quorum tracker
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
        auto result = std::make_unique<TEvBlobStorage::TEvGetBlockResult>(status, TabletId, Generation);
        result->ErrorReason = ErrorReason;
        DSP_LOG_DEBUG_S("DSPGB02", "ReplyAndDie Result# " << result->Print(false));
        SendResponseAndDie(std::move(result));
    }

    void SendGetBlockRequest(const TVDiskID& vdiskId) {
        DSP_LOG_DEBUG_S("DSPB03", "Sending TEvVBlock Tablet# " << TabletId
            << " Generation# " << Generation
            << " vdiskId# " << vdiskId
            << " node# " << Info->GetActorId(vdiskId).NodeId());

        auto msg = std::make_unique<TEvBlobStorage::TEvVGetBlock>(TabletId, vdiskId, Deadline);
        SendToQueue(std::move(msg), 0);
    }

    std::unique_ptr<IEventBase> RestartQuery(ui32 counter) override {
        ++*Mon->NodeMon->RestartGetBlock;
        auto ev = std::make_unique<TEvBlobStorage::TEvGetBlock>(TabletId, Deadline);
        ev->RestartCounter = counter;
        return ev;
    }
public:
    ::NMonitoring::TDynamicCounters::TCounterPtr& GetActiveCounter() const override {
        return Mon->ActiveGetBlock;
    }

    ERequestType GetRequestType() const override {
        return ERequestType::GetBlock;
    }

    TBlobStorageGroupGetBlockRequest(TBlobStorageGroupGetBlockParameters& params)
        : TBlobStorageGroupRequestActor(params)
        , TabletId(params.Common.Event->TabletId)
        , Deadline(params.Common.Event->Deadline)
        , QuorumTracker(Info.Get())
    {}

    void Bootstrap() override {
        DSP_LOG_INFO_S("DSPGB04", "bootstrap"
            << " ActorId# " << SelfId()
            << " Group# " << Info->GroupID
            << " Deadline# " << Deadline
            << " RestartCounter# " << RestartCounter);
        for (const auto& vdisk : Info->GetVDisks()) {
            SendGetBlockRequest(Info->GetVDiskId(vdisk.OrderNumber));
            ++Requests;
        }

        Become(&TBlobStorageGroupGetBlockRequest::StateWait);

        if (Requests == 0) {
            ReplyAndDie(NKikimrProto::OK);
        }
    }

    STATEFN(StateWait) {
        if (ProcessEvent(ev)) {
            return;
        }
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvVGetBlockResult, Handle);
        }
    }
};

IActor* CreateBlobStorageGroupGetBlockRequest(TBlobStorageGroupGetBlockParameters params) {
    return new TBlobStorageGroupGetBlockRequest(params);
}

} // NKikimr

#include "dsproxy.h"
#include "dsproxy_mon.h"
#include "dsproxy_quorum_tracker.h"
#include "dsproxy_blob_tracker.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>

namespace NKikimr {

class TBlobStorageGroupCheckIntegrityGetRequest : public TBlobStorageGroupRequestActor {
    const ui32 QuerySize;
    TArrayHolder<TEvBlobStorage::TEvGet::TQuery> Queries;
    const TInstant Deadline;
    NKikimrBlobStorage::EGetHandleClass GetHandleClass;

    TGroupQuorumTracker QuorumTracker;
    std::unique_ptr<TBlobStatusTracker> BlobStatusTracker;
    ui32 VGetsInFlight = 0;

    std::unique_ptr<TEvBlobStorage::TEvGetResult> PendingResult;

    void ReplyAndDie(NKikimrProto::EReplyStatus status) override {
        Mon->CountCheckIntegrityGetResponseTime(TActivationContext::Monotonic() - RequestStartTime);

        if (status != NKikimrProto::OK) {
            PendingResult.reset(new TEvBlobStorage::TEvGetResult(status, 1, Info->GroupID));
            PendingResult->ErrorReason = ErrorReason;

            auto& response = PendingResult->Responses[0];
            response.Status = NKikimrProto::UNKNOWN;
            response.Id = Queries[0].Id;
            response.Shift = Queries[0].Shift;
            response.RequestedSize = Queries[0].Size;
        }

        SendResponseAndDie(std::move(PendingResult));
    }

    TString DumpBlobStatus() const {
        TStringStream str;
        BlobStatusTracker->Output(str, Info.Get());
        return str.Str();
    }

    void Handle(TEvBlobStorage::TEvVGetResult::TPtr &ev) {
        ProcessReplyFromQueue(ev->Get());

        const NKikimrBlobStorage::TEvVGetResult &record = ev->Get()->Record;

        Y_ABORT_UNLESS(record.HasStatus());
        NKikimrProto::EReplyStatus status = record.GetStatus();

        Y_ABORT_UNLESS(record.HasVDiskID());
        const TVDiskID vDiskId = VDiskIDFromVDiskID(record.GetVDiskID());

        Y_ABORT_UNLESS(VGetsInFlight > 0);
        --VGetsInFlight;

        switch (NKikimrProto::EReplyStatus newStatus = QuorumTracker.ProcessReply(vDiskId, status)) {
            case NKikimrProto::ERROR:
                ErrorReason = "Group is disintegrated";
                ReplyAndDie(NKikimrProto::ERROR);
                return;

            case NKikimrProto::OK:
            case NKikimrProto::UNKNOWN:
                break;

            default:
                Y_ABORT("unexpected newStatus# %s", NKikimrProto::EReplyStatus_Name(newStatus).data());
        }

        for (size_t i = 0; i < record.ResultSize(); ++i) {
            const auto& result = record.GetResult(i);
            BlobStatusTracker->UpdateFromResponseData(result, vDiskId, Info.Get());
        }

        if (!VGetsInFlight) {
            Analyze();
        }
    }

    void Analyze() {
        PendingResult.reset(new TEvBlobStorage::TEvGetResult(NKikimrProto::OK, 1, Info->GroupID));

        auto& response = PendingResult->Responses[0];
        response.Id = Queries[0].Id;
        response.Shift = Queries[0].Shift;
        response.RequestedSize = Queries[0].Size;

        TBlobStorageGroupInfo::EBlobState blobState = BlobStatusTracker->GetBlobState(Info.Get(), nullptr);

        switch (blobState) {
            case TBlobStorageGroupInfo::EBS_DISINTEGRATED:
                ErrorReason = "Group is disintegrated";
                ReplyAndDie(NKikimrProto::ERROR);
                return;

            case TBlobStorageGroupInfo::EBS_FULL:
                response.Status = NKikimrProto::OK;
                break;

            case TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY:
            case TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY:
                response.Status = NKikimrProto::ERROR;
                break;

            case TBlobStorageGroupInfo::EBS_RECOVERABLE_DOUBTED:
                response.Status = NKikimrProto::UNKNOWN;
                break;
        }

        ReplyAndDie(NKikimrProto::OK);
    }

    std::unique_ptr<IEventBase> RestartQuery(ui32 counter) override {
        ++*Mon->NodeMon->RestartCheckIntegrityGet;

        auto ev = std::make_unique<TEvBlobStorage::TEvGet>(Queries, 1, Deadline, GetHandleClass, false, false);
        ev->RestartCounter = counter;
        ev->CheckIntegrity = true;
        return ev;
    }

public:
    ::NMonitoring::TDynamicCounters::TCounterPtr& GetActiveCounter() const override {
        return Mon->ActiveCheckIntegrityGet;
    }

    ERequestType GetRequestType() const override {
        return ERequestType::Get;
    }

    TBlobStorageGroupCheckIntegrityGetRequest(TBlobStorageGroupCheckIntegrityGetParameters& params)
        : TBlobStorageGroupRequestActor(params)
        , QuerySize(params.Common.Event->QuerySize)
        , Queries(params.Common.Event->Queries.Release())
        , Deadline(params.Common.Event->Deadline)
        , GetHandleClass(params.Common.Event->GetHandleClass)
        , QuorumTracker(Info.Get())
    {}

    void Bootstrap() override {
        if (QuerySize != 1) {
            ErrorReason = "CheckIntegrityGet can only be executed with one query";
            ReplyAndDie(NKikimrProto::ERROR);
            return;
        }

        const TLogoBlobID& id = Queries[0].Id;
        if (id.FullID() != id) {
            ErrorReason = "CheckIntegrityGet can only be executed with full blob id";
            ReplyAndDie(NKikimrProto::ERROR);
            return;
        }

        BlobStatusTracker.reset(new TBlobStatusTracker(id, Info.Get()));

        for (const auto& vdisk : Info->GetVDisks()) {
            auto vDiskId = Info->GetVDiskId(vdisk.OrderNumber);

            if (!Info->BelongsToSubgroup(vDiskId, id.Hash())) {
                continue;
            }

            auto vGet = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(
                vDiskId,
                Deadline,
                NKikimrBlobStorage::EGetHandleClass::FastRead,
                TEvBlobStorage::TEvVGet::EFlags::ShowInternals);

            vGet->AddExtremeQuery(id, 0, 0);

            SendToQueue(std::move(vGet), 0);
            ++VGetsInFlight;
        }

        Become(&TBlobStorageGroupCheckIntegrityGetRequest::StateWait);
    }

    STATEFN(StateWait) {
        if (ProcessEvent(ev)) {
            return;
        }
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvVGetResult, Handle);
        }
    }
};

IActor* CreateBlobStorageGroupCheckIntegrityGetRequest(TBlobStorageGroupCheckIntegrityGetParameters params) {
    return new TBlobStorageGroupCheckIntegrityGetRequest(params);
}

} //NKikimr

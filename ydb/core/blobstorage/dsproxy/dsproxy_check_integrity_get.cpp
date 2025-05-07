#include "dsproxy.h"
#include "dsproxy_mon.h"
#include "dsproxy_quorum_tracker.h"
#include "dsproxy_blob_tracker.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>

namespace NKikimr {


class TBlobStorageGroupCheckIntegrityRequest : public TBlobStorageGroupRequestActor {
    const TLogoBlobID Id;
    const TInstant Deadline;
    const NKikimrBlobStorage::EGetHandleClass GetHandleClass;

    TGroupQuorumTracker QuorumTracker;
    std::unique_ptr<TBlobStatusTracker> BlobStatus; // treats NOT_YET as ERROR
    std::unique_ptr<TBlobStatusTracker> BlobStatusOptimistic; // treats NOT_YET as currently replicating -> OK in the future

    ui32 VGetsInFlight = 0;

    using TEvCheckIntegrityResult = TEvBlobStorage::TEvCheckIntegrityResult;
    std::unique_ptr<TEvCheckIntegrityResult> PendingResult;

    void ReplyAndDie(NKikimrProto::EReplyStatus status) override {
        if (status != NKikimrProto::OK) {
            PendingResult.reset(new TEvCheckIntegrityResult(status));
            PendingResult->ErrorReason = ErrorReason;
            PendingResult->PlacementStatus = TEvCheckIntegrityResult::PS_UNKNOWN;
            PendingResult->DataStatus = TEvCheckIntegrityResult::DS_UNKNOWN;
            PendingResult->Id = Id;
        }

        SendResponseAndDie(std::move(PendingResult));
    }

    TString DumpBlobStatus() const {
        TStringStream str;
        BlobStatus->Output(str, Info.Get());
        return str.Str();
    }

    void Handle(TEvBlobStorage::TEvVGetResult::TPtr &ev) {
        ProcessReplyFromQueue(ev->Get());

        const NKikimrBlobStorage::TEvVGetResult& record = ev->Get()->Record;

        if (!record.HasStatus()) {
            ErrorReason = "erron in TEvVGetResult - no status";
            ReplyAndDie(NKikimrProto::ERROR);
            return;
        }
        NKikimrProto::EReplyStatus status = record.GetStatus();

        if (!record.HasVDiskID()) {
            ErrorReason = "erron in TEvVGetResult - no VDisk id";
            ReplyAndDie(NKikimrProto::ERROR);
            return;
        }
        const TVDiskID vDiskId = VDiskIDFromVDiskID(record.GetVDiskID());

        Y_ABORT_UNLESS(VGetsInFlight > 0);
        --VGetsInFlight;

        switch (NKikimrProto::EReplyStatus newStatus = QuorumTracker.ProcessReply(vDiskId, status)) {
            case NKikimrProto::ERROR:
                ErrorReason = "Group is disintegrated or has network problems";
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
            BlobStatus->UpdateFromResponseData(result, vDiskId, Info.Get());

            if (result.GetStatus() == NKikimrProto::NOT_YET) {
                NKikimrBlobStorage::TQueryResult okResult = result;
                okResult.SetStatus(NKikimrProto::OK);
                BlobStatusOptimistic->UpdateFromResponseData(okResult, vDiskId, Info.Get());
            } else {
                BlobStatusOptimistic->UpdateFromResponseData(result, vDiskId, Info.Get());
            }
        }

        if (!VGetsInFlight) {
            Analyze();
        }
    }

    void Analyze() {
        PendingResult.reset(new TEvBlobStorage::TEvCheckIntegrityResult(NKikimrProto::OK));
        PendingResult->Id = Id;
        PendingResult->DataStatus = TEvCheckIntegrityResult::DS_UNKNOWN; // TODO

        TBlobStorageGroupInfo::EBlobState state = BlobStatus->GetBlobState(Info.Get(), nullptr);

        switch (state) {
            case TBlobStorageGroupInfo::EBS_DISINTEGRATED:
                ErrorReason = "Group is disintegrated or has network problems";
                ReplyAndDie(NKikimrProto::ERROR);
                return;

            case TBlobStorageGroupInfo::EBS_FULL:
                PendingResult->PlacementStatus = TEvCheckIntegrityResult::PS_OK;
                break;

            case TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY:
                PendingResult->PlacementStatus = TEvCheckIntegrityResult::PS_ERROR;
                break;

            case TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY: {
                TBlobStorageGroupInfo::EBlobState stateOptimistic =
                    BlobStatusOptimistic->GetBlobState(Info.Get(), nullptr);

                if (stateOptimistic == TBlobStorageGroupInfo::EBS_FULL) {
                    PendingResult->PlacementStatus = TEvCheckIntegrityResult::PS_NOT_YET;
                } else {
                    PendingResult->PlacementStatus = TEvCheckIntegrityResult::PS_ERROR;
                }
                break;
            }
            case TBlobStorageGroupInfo::EBS_RECOVERABLE_DOUBTED:
                PendingResult->PlacementStatus = TEvCheckIntegrityResult::PS_UNKNOWN;
                break;
        }

        ReplyAndDie(NKikimrProto::OK);
    }

    std::unique_ptr<IEventBase> RestartQuery(ui32 counter) override {
        ++*Mon->NodeMon->RestartCheckIntegrity;

        auto ev = std::make_unique<TEvBlobStorage::TEvCheckIntegrity>(
            Id, Deadline, GetHandleClass);
        ev->RestartCounter = counter;
        return ev;
    }

public:
    ::NMonitoring::TDynamicCounters::TCounterPtr& GetActiveCounter() const override {
        return Mon->ActiveCheckIntegrity;
    }

    ERequestType GetRequestType() const override {
        return ERequestType::CheckIntegrity;
    }

    TBlobStorageGroupCheckIntegrityRequest(TBlobStorageGroupCheckIntegrityParameters& params)
        : TBlobStorageGroupRequestActor(params)
        , Id(params.Common.Event->Id)
        , Deadline(params.Common.Event->Deadline)
        , GetHandleClass(params.Common.Event->GetHandleClass)
        , QuorumTracker(Info.Get())
    {}

    void Bootstrap() override {
        BlobStatus.reset(new TBlobStatusTracker(Id, Info.Get()));
        BlobStatusOptimistic.reset(new TBlobStatusTracker(Id, Info.Get()));

        for (const auto& vdisk : Info->GetVDisks()) {
            auto vDiskId = Info->GetVDiskId(vdisk.OrderNumber);

            if (!Info->BelongsToSubgroup(vDiskId, Id.Hash())) {
                continue;
            }

            auto vGet = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(
                vDiskId, Deadline, GetHandleClass, TEvBlobStorage::TEvVGet::EFlags::ShowInternals);
            vGet->AddExtremeQuery(Id, 0, 0);

            SendToQueue(std::move(vGet), 0);
            ++VGetsInFlight;
        }

        Become(&TBlobStorageGroupCheckIntegrityRequest::StateWait);
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

IActor* CreateBlobStorageGroupCheckIntegrityRequest(TBlobStorageGroupCheckIntegrityParameters params) {
    return new TBlobStorageGroupCheckIntegrityRequest(params);
}

} //NKikimr

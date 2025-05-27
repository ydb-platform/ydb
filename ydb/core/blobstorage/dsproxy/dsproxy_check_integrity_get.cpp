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

    TSubgroupPartLayout PartLayout;
    TSubgroupPartLayout PartLayoutWithNotYet;

    bool HasErrorDisks = false;

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

    TString DumpLayout() const {
        TStringStream str;
        PartLayout.Output(str, Info->Type);
        return str.Str();
    }

    void UpdateFromResponseData(const NKikimrBlobStorage::TQueryResult& result, const TVDiskID& vDiskId) {
        if (!result.HasBlobID()) {
            return;
        }
        const TLogoBlobID id = LogoBlobIDFromLogoBlobID(result.GetBlobID());
        if (id.FullID() != Id) {
            return;
        }
        if (!result.HasStatus()) {
            return;
        }
        const NKikimrProto::EReplyStatus status = result.GetStatus();

        ui32 nodeId = Info->GetTopology().GetIdxInSubgroup(vDiskId, Id.Hash());
        const ui32 partId = id.PartId();

        if (!partId) {
            return;
        }

        switch (status) {
            case NKikimrProto::OK:
                PartLayout.AddItem(nodeId, partId - 1, Info->Type);
                PartLayoutWithNotYet.AddItem(nodeId, partId - 1, Info->Type);
                break;

            case NKikimrProto::NOT_YET:
                PartLayoutWithNotYet.AddItem(nodeId, partId - 1, Info->Type);
                break;

            default:
                break;
        }
    }

    void Handle(TEvBlobStorage::TEvVGetResult::TPtr &ev) {
        ProcessReplyFromQueue(ev->Get());

        const NKikimrBlobStorage::TEvVGetResult& record = ev->Get()->Record;

        if (!record.HasStatus()) {
            ErrorReason = "error in TEvVGetResult - no status";
            ReplyAndDie(NKikimrProto::ERROR);
            return;
        }
        NKikimrProto::EReplyStatus status = record.GetStatus();

        if (!record.HasVDiskID()) {
            ErrorReason = "error in TEvVGetResult - no VDisk id";
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

        if (status == NKikimrProto::OK) {
            for (size_t i = 0; i < record.ResultSize(); ++i) {
                UpdateFromResponseData(record.GetResult(i), vDiskId);
            }
        } else {
            HasErrorDisks = true;
        }

        if (!VGetsInFlight) {
            Analyze();
        }
    }

    void Analyze() {
        PendingResult.reset(new TEvCheckIntegrityResult(NKikimrProto::OK));
        PendingResult->Id = Id;
        PendingResult->DataStatus = TEvCheckIntegrityResult::DS_UNKNOWN; // TODO

        TBlobStorageGroupInfo::TSubgroupVDisks faultyDisks(&Info->GetTopology()); // empty set

        const auto& checker = Info->GetQuorumChecker();
        TBlobStorageGroupInfo::EBlobState state = checker.GetBlobStateWithoutLayoutCheck(
            PartLayout, faultyDisks);

        switch (state) {
            case TBlobStorageGroupInfo::EBS_DISINTEGRATED:
                ErrorReason = "Group is disintegrated or has network problems";
                ReplyAndDie(NKikimrProto::ERROR);
                return;

            case TBlobStorageGroupInfo::EBS_FULL:
                PendingResult->PlacementStatus = TEvCheckIntegrityResult::PS_OK;
                break;

            case TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY:
            case TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY:
            case TBlobStorageGroupInfo::EBS_RECOVERABLE_DOUBTED: {
                TBlobStorageGroupInfo::EBlobState stateNotYet = checker.GetBlobStateWithoutLayoutCheck(
                    PartLayoutWithNotYet, faultyDisks);

                if (stateNotYet == TBlobStorageGroupInfo::EBS_FULL) {
                    PendingResult->PlacementStatus = TEvCheckIntegrityResult::PS_NOT_YET;
                } else if (state == TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY) {
                    PendingResult->PlacementStatus = TEvCheckIntegrityResult::PS_RECOVERABLE;
                } else if (HasErrorDisks) {
                    PendingResult->PlacementStatus = TEvCheckIntegrityResult::PS_UNKNOWN;
                } else {
                    PendingResult->PlacementStatus = TEvCheckIntegrityResult::PS_ERROR;
                }
                break;
            }
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

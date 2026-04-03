#include "dsproxy.h"
#include "dsproxy_mon.h"
#include "dsproxy_quorum_tracker.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_data_check.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>

namespace NKikimr {

class TBlobStorageGroupCheckIntegrityRequest
    : public TBlobStorageGroupRequestActor<TBlobStorageGroupCheckIntegrityRequest>
{
    const TLogoBlobID Id;
    const TInstant Deadline;
    const NKikimrBlobStorage::EGetHandleClass GetHandleClass;
    const bool SingleLine;

    TGroupQuorumTracker QuorumTracker;

    TSubgroupPartLayout PartLayout;
    TSubgroupPartLayout PartLayoutWithNotYet;

    TBlobStorageGroupInfo::IDataIntegrityChecker::TPartsData PartsData;

    bool HasErrorDisks = false;

    ui32 VGetsInFlight = 0;

    using TEvCheckIntegrityResult = TEvBlobStorage::TEvCheckIntegrityResult;
    std::unique_ptr<TEvCheckIntegrityResult> PendingResult;


    TString DumpLayout() const {
        TStringStream str;
        PartLayout.Output(str, Info->Type);
        return str.Str();
    }

    void UpdateFromResponseData(TEvBlobStorage::TEvVGetResult* ev,
            const NKikimrBlobStorage::TQueryResult& result, const TVDiskID& vDiskId) {
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

        ui32 diskIdx = Info->GetTopology().GetIdxInSubgroup(vDiskId, Id.Hash());
        const ui32 partId = id.PartId();

        if (!partId) {
            return;
        }

        switch (status) {
            case NKikimrProto::OK:
                PartLayout.AddItem(diskIdx, partId - 1, Info->Type);
                PartLayoutWithNotYet.AddItem(diskIdx, partId - 1, Info->Type);
                PartsData.Parts[partId - 1].push_back(std::make_pair(diskIdx, ev->GetBlobData(result)));
                break;

            case NKikimrProto::NOT_YET:
                PartLayoutWithNotYet.AddItem(diskIdx, partId - 1, Info->Type);
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
                UpdateFromResponseData(ev->Get(), record.GetResult(i), vDiskId);
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
        PendingResult->DataStatus = TEvCheckIntegrityResult::DS_UNKNOWN;

        TBlobStorageGroupInfo::TSubgroupVDisks faultyDisks(&Info->GetTopology()); // empty set

        const auto& quorumChecker = Info->GetQuorumChecker();
        TBlobStorageGroupInfo::EBlobState state = quorumChecker.GetBlobStateWithoutLayoutCheck(
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
                TBlobStorageGroupInfo::EBlobState stateNotYet = quorumChecker.GetBlobStateWithoutLayoutCheck(
                    PartLayoutWithNotYet, faultyDisks);

                if (stateNotYet == TBlobStorageGroupInfo::EBS_FULL) {
                    PendingResult->PlacementStatus = TEvCheckIntegrityResult::PS_REPLICATION_IN_PROGRESS;
                } else if (state == TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY) {
                    PendingResult->PlacementStatus = TEvCheckIntegrityResult::PS_BLOB_IS_RECOVERABLE;
                } else if (HasErrorDisks) {
                    PendingResult->PlacementStatus = TEvCheckIntegrityResult::PS_UNKNOWN;
                } else {
                    PendingResult->PlacementStatus = TEvCheckIntegrityResult::PS_BLOB_IS_LOST;
                }
                break;
            }
        }

        char separator = SingleLine ? ' ' : '\n';

        const auto& dataChecker = Info->GetTopology().GetDataIntegrityChecker();
        auto partsState = dataChecker.GetDataState(Id, PartsData, separator);

        if (partsState.IsOk) {
            PendingResult->DataStatus = (PendingResult->PlacementStatus == TEvCheckIntegrityResult::PS_UNKNOWN) ?
                TEvCheckIntegrityResult::DS_UNKNOWN : TEvCheckIntegrityResult::DS_OK;
        } else {
            PendingResult->DataStatus = TEvCheckIntegrityResult::DS_ERROR;
        }

        TStringStream str;
        str << "Disks:" << separator;
        for (ui32 diskIdx = 0; diskIdx < Info->Type.BlobSubgroupSize(); ++diskIdx) {
            auto vDiskIdShort = Info->GetTopology().GetVDiskInSubgroup(diskIdx, Id.Hash());
            str << diskIdx << ": " << Info->CreateVDiskID(vDiskIdShort) << separator;
        }

        PendingResult->DataInfo = str.Str();
        PendingResult->DataInfo += partsState.DataInfo;

        ReplyAndDie(NKikimrProto::OK);
    }

public:
    static const auto& ActiveCounter(const TIntrusivePtr<TBlobStorageGroupProxyMon>& mon) {
        return mon->ActiveCheckIntegrity;
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BS_PROXY_CHECKINTEGRITY_ACTOR;
    }

    TBlobStorageGroupCheckIntegrityRequest(TBlobStorageGroupCheckIntegrityParameters& params)
        : TBlobStorageGroupRequestActor(params)
        , Id(params.Common.Event->Id)
        , Deadline(params.Common.Event->Deadline)
        , GetHandleClass(params.Common.Event->GetHandleClass)
        , SingleLine(params.Common.Event->SingleLine)
        , QuorumTracker(Info.Get())
    {}

    void Bootstrap() {
        PartsData.Parts.resize(Info->Type.TotalPartCount());

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

    void ReplyAndDie(NKikimrProto::EReplyStatus status) {
        if (status != NKikimrProto::OK) {
            PendingResult.reset(new TEvCheckIntegrityResult(status));
            PendingResult->ErrorReason = ErrorReason;
            PendingResult->PlacementStatus = TEvCheckIntegrityResult::PS_UNKNOWN;
            PendingResult->DataStatus = TEvCheckIntegrityResult::DS_UNKNOWN;
            PendingResult->Id = Id;
        }
        SendResponseAndDie(std::move(PendingResult));
    }

    std::unique_ptr<IEventBase> RestartQuery(ui32 counter) {
        ++*Mon->NodeMon->RestartCheckIntegrity;
        auto ev = std::make_unique<TEvBlobStorage::TEvCheckIntegrity>(Id, Deadline, GetHandleClass);
        ev->RestartCounter = counter;
        return ev;
    }
};

IActor* CreateBlobStorageGroupCheckIntegrityRequest(
        TBlobStorageGroupCheckIntegrityParameters params, NWilson::TTraceId traceId) {
    NWilson::TSpan span(TWilson::BlobStorage, std::move(traceId), "DSProxy.CheckIntegrity");
    if (span) {
        span.Attribute("event", params.Common.Event->ToString());
    }
    params.Common.Span = std::move(span);
    return new TBlobStorageGroupCheckIntegrityRequest(params);
}

} //NKikimr

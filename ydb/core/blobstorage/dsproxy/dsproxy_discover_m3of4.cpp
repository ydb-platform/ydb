#include "dsproxy.h"
#include "dsproxy_mon.h"
#include "dsproxy_quorum_tracker.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.h>

namespace NKikimr {

class TBlobStorageGroupMirror3of4DiscoverRequest : public TBlobStorageGroupRequestActor {
    const ui64 TabletId;
    const ui32 MinGeneration;
    const TInstant StartTime;
    const TInstant Deadline;
    const bool ReadBody;
    const bool DiscoverBlockedGeneration;
    const ui32 ForceBlockedGeneration;
    const bool FromLeader;

public:
    ::NMonitoring::TDynamicCounters::TCounterPtr& GetActiveCounter() const override {
        return Mon->ActiveDiscover;
    }

    ERequestType GetRequestType() const override {
        return ERequestType::Discover;
    }

    TBlobStorageGroupMirror3of4DiscoverRequest(TBlobStorageGroupDiscoverParameters& params)
        : TBlobStorageGroupRequestActor(params)
        , TabletId(params.Common.Event->TabletId)
        , MinGeneration(params.Common.Event->MinGeneration)
        , StartTime(params.Common.Now)
        , Deadline(params.Common.Event->Deadline)
        , ReadBody(params.Common.Event->ReadBody)
        , DiscoverBlockedGeneration(params.Common.Event->DiscoverBlockedGeneration)
        , ForceBlockedGeneration(params.Common.Event->ForceBlockedGeneration)
        , FromLeader(params.Common.Event->FromLeader)
    {
        for (size_t i = 0; i < DiskState.size(); ++i) {
            TDiskState& disk = DiskState[i];
            disk.VDiskId = Info->GetVDiskId(i);
            disk.From = MaxBlobId;
        }
    }

    std::unique_ptr<IEventBase> RestartQuery(ui32 counter) override {
        ++*Mon->NodeMon->RestartDiscover;
        auto ev = std::make_unique<TEvBlobStorage::TEvDiscover>(TabletId, MinGeneration, ReadBody,
            DiscoverBlockedGeneration, Deadline, ForceBlockedGeneration, FromLeader);
        ev->RestartCounter = counter;
        return ev;
    }

    void Bootstrap() override {
        DSP_LOG_INFO_S("DSPDX01", "bootstrap"
            << " TabletId# " << TabletId
            << " MinGeneration# " << MinGeneration
            << " Deadline# " << Deadline
            << " ReadBody# " << (ReadBody ? "true" : "false")
            << " DiscoverBlockedGeneration# " << (DiscoverBlockedGeneration ? "true" : "false")
            << " ForceBlockedGeneration# " << ForceBlockedGeneration
            << " FromLeader# " << (FromLeader ? "true" : "false")
            << " RestartCounter# " << RestartCounter);

        Become(&TBlobStorageGroupMirror3of4DiscoverRequest::StateFunc);

        if (Deadline != TInstant::Max()) {
            Schedule(Deadline - TActivationContext::Now(), new TEvents::TEvWakeup);
        }

        // start discovery by issuing range-read queries to all disks of a group
        IssueRangeReadQueries();
    }

    void HandleWakeup() {
        ReplyAndDie(NKikimrProto::DEADLINE);
    }

    void ReplyAndDie(NKikimrProto::EReplyStatus status) override {
        Y_ABORT_UNLESS(status != NKikimrProto::OK);
        auto formatFailedGroupDisks = [&] {
            TStringBuilder s;
            s << "[";
            bool first = true;
            for (TDiskState& disk : DiskState) {
                if (FailedGroupDisks & TBlobStorageGroupInfo::TGroupVDisks(&Info->GetTopology(), disk.VDiskId)) {
                    s << (std::exchange(first, false) ? "" : " ") << disk.VDiskId;
                }
            }
            s << "]";
            return s;
        };
        DSP_LOG_ERROR_S("DSPDX02", "request failed"
            << " Status# " << NKikimrProto::EReplyStatus_Name(status)
            << " ErrorReason# " << (ErrorReason ? ErrorReason : "<none>")
            << " FailedGroupDisks# " << formatFailedGroupDisks());
        std::unique_ptr<TEvBlobStorage::TEvDiscoverResult> response(new TEvBlobStorage::TEvDiscoverResult(status, MinGeneration,
            0U));
        response->ErrorReason = ErrorReason;
        SendResponseAndDie(std::move(response));
    }

    void ReplyAndDie(NKikimrProto::EReplyStatus status, std::optional<TString> errorReason) {
        if (errorReason) {
            ErrorReason = std::move(*errorReason);
        }
        ReplyAndDie(status);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Range-read scans
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    enum class EDiskState {
        IDLE,
        READ_PENDING,
        FINISHED,
    };

    struct TDiskState {
        TVDiskID VDiskId;
        EDiskState State = EDiskState::IDLE;
        TLogoBlobID From;
        bool Replied = false;

        // check if replies from this disk should cover provided blob id
        bool Covered(const TLogoBlobID& id) const {
            return State == EDiskState::FINISHED || From < id;
        }
    };

    static constexpr ui32 MaxBlobsAtOnce = 32;
    static constexpr auto HandleClass = NKikimrBlobStorage::EGetHandleClass::Discover;
    const TLogoBlobID MaxBlobId{TabletId, Max<ui32>(), Max<ui32>(), 0, TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie};
    std::vector<TDiskState> DiskState{Info->GetTotalVDisksNum()};
    std::map<TLogoBlobID, TSubgroupPartLayout> BlobMetadata; // obtained through range queries
    ui32 NumUnrepliedDisks = DiskState.size();
    TBlobStorageGroupInfo::TGroupVDisks FailedGroupDisks{&Info->GetTopology()};
    ui32 BlockedGeneration = 0;
    std::optional<TLogoBlobID> GetIssuedFor;
    bool Doubted = false;

    void IssueRangeReadQueries() {
        for (TDiskState& disk : DiskState) {
            IssueRangeReadQuery(disk);
        }
    }

    void IssueRangeReadQuery(TDiskState& state) {
        const TLogoBlobID to = TLogoBlobID(TabletId, MinGeneration, 0, 0, 0, 0);
        SendToQueue(TEvBlobStorage::TEvVGet::CreateRangeIndexQuery(state.VDiskId, Deadline, HandleClass,
            TEvBlobStorage::TEvVGet::EFlags::None, Nothing(), state.From, to, MaxBlobsAtOnce, nullptr,
            TEvBlobStorage::TEvVGet::TForceBlockTabletData(TabletId, ForceBlockedGeneration)), 0);
        const EDiskState prev = std::exchange(state.State, EDiskState::READ_PENDING);
        Y_ABORT_UNLESS(prev == EDiskState::IDLE);
    }

    void Handle(TEvBlobStorage::TEvVGetResult::TPtr ev) {
        ProcessReplyFromQueue(ev->Get());
        auto& record = ev->Get()->Record;
        if (!record.HasStatus() || !record.HasVDiskID()) {
            return ReplyAndDie(NKikimrProto::ERROR, "incorrect TEvVGetResult from VDisk");
        }
        if (record.HasBlockedGeneration() && record.GetBlockedGeneration() > BlockedGeneration) {
            BlockedGeneration = record.GetBlockedGeneration();
        }
        const TVDiskID& vdiskId = VDiskIDFromVDiskID(record.GetVDiskID());
        TDiskState& disk = DiskState[Info->GetOrderNumber(vdiskId)];
        Y_DEBUG_ABORT_UNLESS(disk.VDiskId == vdiskId);
        const EDiskState prev = std::exchange(disk.State, EDiskState::IDLE);
        Y_ABORT_UNLESS(prev == EDiskState::READ_PENDING);
        NumUnrepliedDisks -= !std::exchange(disk.Replied, true);
        switch (record.GetStatus()) {
            case NKikimrProto::OK: {
                std::optional<TLogoBlobID> lastId;
                for (const auto& blob : record.GetResult()) {
                    if (!blob.HasStatus() || !blob.HasBlobID()) {
                        return ReplyAndDie(NKikimrProto::ERROR, "incorrect TEvVGetResult::TQueryResult from VDisk");
                    } else if (blob.GetStatus() != NKikimrProto::OK) {
                        return ReplyAndDie(NKikimrProto::ERROR, "incorrect blob status in returned index query");
                    }
                    const TLogoBlobID& id = LogoBlobIDFromLogoBlobID(blob.GetBlobID());
                    if (id.PartId()) {
                        return ReplyAndDie(NKikimrProto::ERROR, "non-zero part id in returned index query");
                    } else if (lastId && !(id < *lastId)) {
                        return ReplyAndDie(NKikimrProto::ERROR, "incorrect sorting order in index query");
                    }
                    auto& layout = BlobMetadata[id];
                    for (const ui32 partId : blob.GetParts()) {
                        const ui32 nodeId = Info->GetIdxInSubgroup(vdiskId, id.Hash());
                        if (nodeId == Info->Type.BlobSubgroupSize()) {
                            return ReplyAndDie(NKikimrProto::ERROR, "blob does not match subgroup in index query");
                        }
                        layout.AddItem(nodeId, partId - 1, Info->Type);
                    }
                    lastId = id;
                }
                if (lastId) {
                    // resume reading from this blob id
                    disk.From = TLogoBlobID::PrevFull(*lastId, TLogoBlobID::MaxBlobSize);
                } else if (record.GetIsRangeOverflow()) {
                    // not very sane answer
                    return ReplyAndDie(NKikimrProto::ERROR, "empty index query result with overflow flag set");
                } else {
                    // we got empty response and no overflow flag set -- this disk has definitely finished its stream
                    disk.State = EDiskState::FINISHED;
                }
                break;
            }

            case NKikimrProto::ERROR:
            case NKikimrProto::VDISK_ERROR_STATE:
                FailedGroupDisks += {&Info->GetTopology(), vdiskId};
                if (!Info->GetTopology().GetQuorumChecker().CheckFailModelForGroup(FailedGroupDisks)) {
                    return ReplyAndDie(NKikimrProto::ERROR, "failure model exceeded");
                } else {
                    disk.State = EDiskState::FINISHED;
                }
                break;

            default:
                return ReplyAndDie(NKikimrProto::ERROR, "unexpected status code in TEvVGetResult");
        }
        // now we may have had some progress in reading blobs
        Process();
    }

    void Process() {
        if (GetIssuedFor || NumUnrepliedDisks) {
            return; // we have already issued Get-request, so wait for it to finish; otherwise wait for all disks to answer
        }
        while (!BlobMetadata.empty()) {
            const auto it = --BlobMetadata.end();
            const TLogoBlobID& id = it->first;
            const TSubgroupPartLayout& layout = it->second;

            // check the answer quorum for this blob
            TBlobStorageGroupInfo::TVDiskIds subgroup;
            ui32 numPendingDisks = Info->Type.BlobSubgroupSize();
            Info->PickSubgroup(id.Hash(), &subgroup, nullptr);
            for (const TVDiskID& vdiskId : subgroup) {
                TDiskState& disk = DiskState[Info->GetOrderNumber(vdiskId)];
                if (disk.Covered(id)) {
                    --numPendingDisks; // this disk has already covered this blob
                } else if (disk.State == EDiskState::IDLE) {
                    Y_ABORT_UNLESS(disk.Replied);
                    Y_ABORT_UNLESS(disk.From != MaxBlobId);
                    IssueRangeReadQuery(disk);
                } else {
                    Y_ABORT_UNLESS(disk.State == EDiskState::READ_PENDING);
                }
            }
            if (numPendingDisks) {
                return;
            }
            // this blob is fully answered, so we can make a decision whether it was written, can it be recovered,
            // can it be read, etc.
            TBlobStorageGroupInfo::TSubgroupVDisks failedSubgroupDisks(&Info->GetTopology());
            for (ui32 nodeId = 0; nodeId < subgroup.size(); ++nodeId) {
                if (FailedGroupDisks & TBlobStorageGroupInfo::TGroupVDisks(&Info->GetTopology(), subgroup[nodeId])) {
                    failedSubgroupDisks |= {&Info->GetTopology(), nodeId};
                }
            }
            Doubted = false;
            switch (Info->GetTopology().GetQuorumChecker().GetBlobState(layout, failedSubgroupDisks)) {
                case TBlobStorageGroupInfo::EBS_DISINTEGRATED:
                    Y_ABORT("incorrect state"); // we should not reach this point as we check failure model a bit earlier

                case TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY:
                    // this blob is not recoverable, so, possibly, it must have never been written; we may skip it
                    BlobMetadata.erase(it);
                    break;

                case TBlobStorageGroupInfo::EBS_RECOVERABLE_DOUBTED:
                    Doubted = true;
                    [[fallthrough]];
                case TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY:
                case TBlobStorageGroupInfo::EBS_FULL: {
                    // we have to process this blob
                    auto query = std::make_unique<TEvBlobStorage::TEvGet>(id, 0, 0, Deadline, HandleClass, true, !ReadBody, TEvBlobStorage::TEvGet::TForceBlockTabletData(TabletId, ForceBlockedGeneration));
                    query->IsInternal = true;
                    SendToProxy(std::move(query));
                    GetIssuedFor = id;
                    return;
                }

                default:
                    Y_ABORT("unexpected blob state");
            }
        }

        // ensure there are no remaining blobs to handle
        Y_ABORT_UNLESS(BlobMetadata.empty());

        // handle NODATA situation when all the disks are finished
        bool issuedMoreQueries = false;
        for (TDiskState& disk : DiskState) {
            if (disk.State == EDiskState::IDLE) {
                Y_ABORT_UNLESS(disk.Replied);
                Y_ABORT_UNLESS(disk.From != MaxBlobId);
                IssueRangeReadQuery(disk);
                issuedMoreQueries = true;
            }
        }
        if (!issuedMoreQueries) {
            SendResponseAndDie(std::make_unique<TEvBlobStorage::TEvDiscoverResult>(NKikimrProto::NODATA, MinGeneration,
                BlockedGeneration));
        }
    }

    void Handle(TEvBlobStorage::TEvGetResult::TPtr ev) {
        Y_ABORT_UNLESS(GetIssuedFor);
        auto *msg = ev->Get();
        Y_ABORT_UNLESS(msg->ResponseSz == 1);
        auto& resp = msg->Responses[0];
        Y_ABORT_UNLESS(resp.Id == *GetIssuedFor);
        GetIssuedFor.reset();
        switch (resp.Status) {
            case NKikimrProto::NODATA:
                if (Doubted) {
                    // we could not read correct (as we thought) blob
                    ReplyAndDie(NKikimrProto::ERROR, "lost the blob");
                } else {
                    // this blob recoverability is doubted, so we skip it and continue with the next blob
                    BlobMetadata.erase(resp.Id);
                    Process();
                }
                break;

            case NKikimrProto::ERROR:
                ReplyAndDie(NKikimrProto::ERROR, "TEvGet failed: " + msg->ErrorReason);
                break;

            case NKikimrProto::OK:
                SendResponseAndDie(std::make_unique<TEvBlobStorage::TEvDiscoverResult>(resp.Id, MinGeneration,
                    resp.Buffer.ConvertToString(), BlockedGeneration));
                break;

            default:
                ReplyAndDie(NKikimrProto::ERROR, "unexpected TEvGetResult status");
                break;
        }
    }

    STATEFN(StateFunc) {
        if (ProcessEvent(ev)) {
            return;
        }
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            hFunc(TEvBlobStorage::TEvVGetResult, Handle);
            hFunc(TEvBlobStorage::TEvGetResult, Handle);
        }
    }
};

IActor* CreateBlobStorageGroupMirror3of4DiscoverRequest(TBlobStorageGroupDiscoverParameters params) {
    return new TBlobStorageGroupMirror3of4DiscoverRequest(params);
}

}//NKikimr

#include "blobstorage_events.h"
#include <ydb/core/blobstorage/pdisk/drivedata_serializer.h>
#include <ydb/core/protos/blobstorage_distributed_config.pb.h>

namespace NKikimr {

    TEvNodeWardenStorageConfig::TEvNodeWardenStorageConfig(std::shared_ptr<const NKikimrBlobStorage::TStorageConfig> config,
            bool selfManagementEnabled, TBridgeInfo::TPtr bridgeInfo,
            std::shared_ptr<const NKikimrBlobStorage::TStorageConfig> committedConfig)
        : Config(std::move(config))
        , SelfManagementEnabled(selfManagementEnabled)
        , BridgeInfo(std::move(bridgeInfo))
        , CommittedConfig(std::move(committedConfig))
    {}

    TEvNodeWardenStorageConfig::~TEvNodeWardenStorageConfig()
    {}

} // NKikimr

namespace NKikimr {

TEvBlobStorage::TEvControllerUpdateDiskStatus::TEvControllerUpdateDiskStatus(const TVDiskID& vDiskId, ui32 nodeId, ui32 pdiskId, ui32 vslotId,
        std::optional<ui32> satisfactionRankPercent, NKikimrWhiteboard::EVDiskState state, bool replicated,
        NKikimrWhiteboard::EFlag diskSpace,
        std::optional<bool> isThrottling, std::optional<ui32> throttlingRate) {
    NKikimrBlobStorage::TVDiskMetrics* metric = Record.AddVDisksMetrics();
    VDiskIDFromVDiskID(vDiskId, metric->MutableVDiskId());
    if (satisfactionRankPercent) {
        metric->SetSatisfactionRank(*satisfactionRankPercent);
    }
    metric->SetState(state);
    metric->SetReplicated(replicated);
    metric->SetDiskSpace(diskSpace);
    if (isThrottling) {
        metric->SetIsThrottling(*isThrottling);
    }
    if (throttlingRate) {
        metric->SetThrottlingRate(*throttlingRate);
    }
    auto *p = metric->MutableVSlotId();
    p->SetNodeId(nodeId);
    p->SetPDiskId(pdiskId);
    p->SetVSlotId(vslotId);
}

TEvBlobStorage::TEvControllerUpdateDiskStatus::TEvControllerUpdateDiskStatus(const TVDiskID& vDiskId, ui32 nodeId, ui32 pdiskId, ui32 vslotId, ui32 statusFlags,
        ui64 vdiskSpaceSequence) {
    Record.SetVDiskSpaceSequence(vdiskSpaceSequence);
    NKikimrBlobStorage::TVDiskMetrics* metric = Record.AddVDisksMetrics();
    VDiskIDFromVDiskID(vDiskId, metric->MutableVDiskId());
    metric->MutableVDiskId()->ClearGroupGeneration();
    metric->SetStatusFlags(statusFlags);
    auto *p = metric->MutableVSlotId();
    p->SetNodeId(nodeId);
    p->SetPDiskId(pdiskId);
    p->SetVSlotId(vslotId);
}

TEvBlobStorage::TEvControllerRegisterNode::TEvControllerRegisterNode(ui32 nodeID, const TVector<ui32>& startedDynamicGroups,
        const TVector<ui32>& groupGenerations, const TVector<NPDisk::TDriveData>& drivesData) {
    Record.SetNodeID(nodeID);
    for (auto groupId: startedDynamicGroups) {
        Record.AddGroups(groupId);
    }
    for (auto generation : groupGenerations) {
        Record.AddGroupGenerations(generation);
    }
    for (const auto& data : drivesData) {
        DriveDataToDriveData(data, Record.AddDrivesData());
    }
}

TString TEvBlobStorage::TEvControllerRegisterNode::ToString() const {
    TStringStream str;
    str << "{TEvRegisterNode Record# " << Record.DebugString();
    str << "}";
    return str.Str();
}

TEvBlobStorage::TEvControllerUpdateNodeDrives::TEvControllerUpdateNodeDrives(ui32 nodeId, const TVector<NPDisk::TDriveData>& drivesData) {
    Record.SetNodeId(nodeId);
    for (const auto& data : drivesData) {
        DriveDataToDriveData(data, Record.AddDrivesData());
    }
}

TString TEvBlobStorage::TEvControllerUpdateNodeDrives::ToString() const {
    TStringStream str;
    str << "{TEvControllerUpdateNodeDrives Record# " << Record.DebugString();
    str << "}";
    return str.Str();
}

TEvBlobStorage::TEvControllerNodeServiceSetUpdate::TEvControllerNodeServiceSetUpdate(NKikimrProto::EReplyStatus status, ui32 nodeID) {
    Record.SetStatus(status);
    Record.SetNodeID(nodeID);
}

TString TEvBlobStorage::TEvControllerNodeServiceSetUpdate::ToString() const {
    TStringStream str;
    str << "{TEvControllerNodeServiceSetUpdate";
    str << " Record# " << Record.DebugString();
    str << "}";
    return str.Str();
}

TEvBlobStorage::TEvControllerGetGroup::TEvControllerGetGroup(ui32 nodeID, ui32 groupID) {
    Record.SetNodeID(nodeID);
    Record.AddGroupIDs(groupID);
}

TString TEvBlobStorage::TEvControllerGetGroup::ToString() const {
    TStringStream str;
    str << "{TEvControllerGetGroup NodeID# " << Record.GetNodeID();
    str << " GroupIDs.size()# " << Record.GroupIDsSize();
    if (Record.GroupIDsSize()) {
        str << "{";
        for (size_t i = 0; i < Record.GroupIDsSize(); ++i) {
            str << "GroupID# " << Record.GetGroupIDs((int)i);
        }
        str << "}";
    }
    str << "}";
    return str.Str();
}

TString TEvBlobStorage::TEvControllerSelectGroups::ToString() const {
    TStringStream str;
    str << "{TEvControllerSelectGroups}";
    return str.Str();
}

TString TEvBlobStorage::TEvControllerSelectGroupsResult::ToString() const {
    TStringStream str;
    str << "{TEvControllerSelectGroupsResult Status# " << NKikimrProto::EReplyStatus_Name(Record.GetStatus()).data();
    str << "}";
    return str.Str();
}

TEvBlobStorage::TEvControllerProposeGroupKey::TEvControllerProposeGroupKey(ui32 nodeId, ui32 groupId, ui32 lifeCyclePhase,
        const TString& mainKeyId, const TString& encryptedGroupKey, ui64 mainKeyVersion, ui64 groupKeyNonce) {
    Record.SetNodeId(nodeId);
    Record.SetGroupId(groupId);
    Record.SetLifeCyclePhase(lifeCyclePhase);
    Record.SetMainKeyId(mainKeyId);
    Record.SetEncryptedGroupKey(encryptedGroupKey);
    Record.SetMainKeyVersion(mainKeyVersion);
    Record.SetGroupKeyNonce(groupKeyNonce);
}

TString TEvBlobStorage::TEvControllerProposeGroupKey::ToString() const {
    TStringStream str;
    str << "{TEvControllerProposeGroupKey Record# " << Record.DebugString();
    str << "}";
    return str.Str();
}

TEvBlobStorage::TEvVStatusResult::TEvVStatusResult(NKikimrProto::EReplyStatus status, const TVDiskID &vdisk, bool joinedGroup, bool replicated, bool isReadOnly,
        ui64 incarnationGuid)
{
    Record.SetStatus(status);
    Record.SetJoinedGroup(joinedGroup);
    Record.SetReplicated(replicated);
    Record.SetIsReadOnly(isReadOnly);
    VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
    if (status == NKikimrProto::OK) {
        Record.SetIncarnationGuid(incarnationGuid);
    }
}

TEvBlobStorage::TEvVStatusResult::TEvVStatusResult(NKikimrProto::EReplyStatus status, const TVDiskID &vdisk) {
    Y_ABORT_UNLESS(status != NKikimrProto::OK);
    Record.SetStatus(status);
    Record.SetJoinedGroup(false);
    Record.SetReplicated(false);
    VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
}

TString TEvRegisterPDiskLoadActor::ToString() const {
    TStringStream str;
    str << "{TEvRegisterPDiskLoadActor";
    str << "}";
    return str.Str();
}

TString TEvRegisterPDiskLoadActorResult::ToString() const {
    TStringStream str;
    str << "{TEvRegisterPDiskLoadActorResult";
    str << " OwnerRound# " << OwnerRound;
    str << "}";
    return str.Str();
}

void TEvBlobStorage::TEvBunchOfEvents::Process(IActor *actor) {
    for (auto& ev : Bunch) {
        TAutoPtr<IEventHandle> handle(ev.release());
        actor->Receive(handle);
    }
}

TEvBlobStorage::TEvControllerScrubQueryStartQuantum::TEvControllerScrubQueryStartQuantum(ui32 nodeId, ui32 pdiskId, ui32 vslotId) {
    auto *x = Record.MutableVSlotId();
    x->SetNodeId(nodeId);
    x->SetPDiskId(pdiskId);
    x->SetVSlotId(vslotId);
}

TEvBlobStorage::TEvControllerScrubStartQuantum::TEvControllerScrubStartQuantum(ui32 nodeId, ui32 pdiskId, ui32 vslotId, std::optional<TString> state) {
    auto *x = Record.MutableVSlotId();
    x->SetNodeId(nodeId);
    x->SetPDiskId(pdiskId);
    x->SetVSlotId(vslotId);
    if (state) {
        Record.SetState(*state);
    }
}

TEvBlobStorage::TEvControllerScrubQuantumFinished::TEvControllerScrubQuantumFinished(ui32 nodeId, ui32 pdiskId, ui32 vslotId) {
    auto *x = Record.MutableVSlotId();
    x->SetNodeId(nodeId);
    x->SetPDiskId(pdiskId);
    x->SetVSlotId(vslotId);
}

TEvBlobStorage::TEvControllerScrubReportQuantumInProgress::TEvControllerScrubReportQuantumInProgress(ui32 nodeId, ui32 pdiskId, ui32 vslotId) {
    auto *x = Record.MutableVSlotId();
    x->SetNodeId(nodeId);
    x->SetPDiskId(pdiskId);
    x->SetVSlotId(vslotId);
}

} // namespace NKikimr

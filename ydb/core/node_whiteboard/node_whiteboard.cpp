#include "node_whiteboard.h"
#include <ydb/core/base/blobstorage_tablet_types.h>
#include <ydb/core/base/subdomain.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>

namespace NKikimr::NNodeWhiteboard {

TEvWhiteboard::TEvTabletStateUpdate::TEvTabletStateUpdate(TTabletId tabletId, TFollowerId followerId, NKikimrWhiteboard::TTabletStateInfo::ETabletState state, const TIntrusivePtr<TTabletStorageInfo>& storageInfo, ui32 generation, bool leader) {
    Record.SetTabletId(tabletId);
    Record.SetFollowerId(followerId);
    Record.SetType(storageInfo->TabletType);
    Record.SetState(state);
    Record.SetGeneration(generation);
    Record.SetLeader(leader);
    Record.MutableChannelGroupIDs()->Resize(storageInfo->Channels.size(), 0);
    for (const auto& channel : storageInfo->Channels) {
        const auto* latestEntry = channel.LatestEntry();
        ui32 groupId = latestEntry != nullptr ? latestEntry->GroupID : 0;
        Record.SetChannelGroupIDs(channel.Channel, groupId);
    }
    if (storageInfo->HiveId) {
        Record.SetHiveId(storageInfo->HiveId);
    }
    if (storageInfo->TenantPathId) {
        Record.MutableTenantId()->CopyFrom(TSubDomainKey(storageInfo->TenantPathId.OwnerId, storageInfo->TenantPathId.LocalPathId));
    }
}

TEvWhiteboard::TEvTabletStateUpdate::TEvTabletStateUpdate(TTabletId tabletId, TFollowerId followerId, NKikimrWhiteboard::TTabletStateInfo::ETabletState state, ui32 generation) {
    Record.SetTabletId(tabletId);
    Record.SetFollowerId(followerId);
    Record.SetState(state);
    Record.SetGeneration(generation);
}

TEvWhiteboard::TEvTabletStateUpdate::TEvTabletStateUpdate(TTabletId tabletId, TFollowerId followerId, NKikimrWhiteboard::TTabletStateInfo::ETabletState state, ui32 generation, bool leader) {
    Record.SetTabletId(tabletId);
    Record.SetFollowerId(followerId);
    Record.SetState(state);
    Record.SetGeneration(generation);
    Record.SetLeader(leader);
}

TEvWhiteboard::TEvTabletStateUpdate::TEvTabletStateUpdate(TTabletId tabletId, ui32 userState) {
    Record.SetTabletId(tabletId);
    Record.SetUserState(userState);
}

void TEvWhiteboard::TEvTabletStateResponsePacked5::Fill(NKikimrWhiteboard::TTabletStateInfo& result) const {
    result.SetTabletId(TabletId);
    result.SetFollowerId(FollowerId);
    result.SetGeneration(Generation);
    result.SetType(Type);
    result.SetState(State);
}

TEvWhiteboard::TEvTabletStateResponsePacked5* TEvWhiteboard::TEvTabletStateResponse::AllocatePackedResponse(size_t count) {
    auto& packed5 = *Record.MutablePacked5();
    packed5.resize(count * sizeof(TEvTabletStateResponsePacked5));
    return reinterpret_cast<TEvTabletStateResponsePacked5*>(packed5.Detach());
}

TEvWhiteboard::TEvPDiskStateUpdate::TEvPDiskStateUpdate(ui32 pDiskId, const TString& path, ui64 guid, ui64 category) {
    Record.SetPDiskId(pDiskId);
    Record.SetPath(path);
    Record.SetGuid(guid);
    Record.SetCategory(category);
}

TEvWhiteboard::TEvPDiskStateUpdate::TEvPDiskStateUpdate(ui32 pDiskId, ui64 availableSize, ui64 totalSize, ui32 state) {
    Record.SetPDiskId(pDiskId);
    Record.SetAvailableSize(availableSize);
    Record.SetTotalSize(totalSize);
    Record.SetState(static_cast<NKikimrBlobStorage::TPDiskState::E>(state));
}

TEvWhiteboard::TEvPDiskStateUpdate::TEvPDiskStateUpdate(ui32 pDiskId, NKikimrWhiteboard::EFlag realtime, NKikimrWhiteboard::EFlag device) {
    Record.SetPDiskId(pDiskId);
    Record.SetRealtime(realtime);
    Record.SetDevice(device);
}

TEvWhiteboard::TEvVDiskStateUpdate::TEvVDiskStateUpdate(const TVDiskID& vDiskId, const TMaybe<TString>& storagePoolName, ui32 pDiskId,
        ui32 vDiskSlotId, ui64 guid, NKikimrBlobStorage::TVDiskKind::EVDiskKind kind, bool donorMode,
        ui64 instanceGuid, std::vector<NKikimrBlobStorage::TVSlotId>&& donors, ui32 groupSizeInUnits)
    : Initial(true)
{
    VDiskIDFromVDiskID(vDiskId, Record.MutableVDiskId());
    if (storagePoolName) {
        Record.SetStoragePoolName(*storagePoolName);
    }
    Record.SetPDiskId(pDiskId);
    Record.SetVDiskSlotId(vDiskSlotId);
    Record.SetGuid(guid);
    Record.SetKind(kind);
    if (donorMode) {
        Record.SetDonorMode(true);
    }
    Record.SetInstanceGuid(instanceGuid);
    for (auto&& donor : donors) {
        *Record.AddDonors() = std::move(donor);
    }
    Record.SetGroupSizeInUnits(groupSizeInUnits);
}

TEvWhiteboard::TEvVDiskStateUpdate::TEvVDiskStateUpdate(NKikimrWhiteboard::EVDiskState state,
                             NKikimrWhiteboard::EFlag diskSpace,
                             bool replicated,
                             bool unreplicatedPhantoms,
                             bool unreplicatedNonPhantoms,
                             NKikimrWhiteboard::TVDiskDetailedReplicationStatus::E detailedReplicationStatus,
                             ui64 unsyncedVDisks,
                             NKikimrWhiteboard::EFlag frontQueuesLigth,
                             bool hasUnreadableBlobs) {
    Record.SetVDiskState(state);
    Record.SetDiskSpace(diskSpace);
    Record.SetReplicated(replicated);
    Record.SetUnreplicatedPhantoms(unreplicatedPhantoms);
    Record.SetUnreplicatedNonPhantoms(unreplicatedNonPhantoms);
    Record.SetDetailedReplicationStatus(detailedReplicationStatus);
    Record.SetUnsyncedVDisks(unsyncedVDisks);
    Record.SetFrontQueues(frontQueuesLigth);
    Record.SetHasUnreadableBlobs(hasUnreadableBlobs);
}

TEvWhiteboard::TEvBSGroupStateUpdate::TEvBSGroupStateUpdate(const TIntrusivePtr<TBlobStorageGroupInfo>& groupInfo) {
    Record.SetGroupID(groupInfo->GroupID.GetRawId());
    Record.SetGroupGeneration(groupInfo->GroupGeneration);
    Record.SetErasureSpecies(groupInfo->Type.ErasureSpeciesName(groupInfo->Type.GetErasure()));
    if (ui32 numVDisks = groupInfo->GetTotalVDisksNum()) {
        for (ui32 i = 0; i < numVDisks; ++i) {
            VDiskIDFromVDiskID(groupInfo->GetVDiskId(i), Record.AddVDiskIds());
            const TActorId& actorId = groupInfo->GetActorId(i);
            Record.AddVDiskNodeIds(actorId.NodeId());
        }
    } else {
        Record.SetNoVDisksInGroup(true);
    }
    Record.SetStoragePoolName(groupInfo->GetStoragePoolName());
    if (groupInfo->GetEncryptionMode() != TBlobStorageGroupInfo::EEM_NONE) {
        Record.SetEncryption(true);
    }
    if (groupInfo->BlobDepotId) {
        Record.SetBlobDepotId(*groupInfo->BlobDepotId);
    }
    Record.SetGroupSizeInUnits(groupInfo->GroupSizeInUnits);
    if (const auto& bridgeProxyGroupId = groupInfo->GetBridgeProxyGroupId()) {
        bridgeProxyGroupId->CopyToProto(&Record, &decltype(Record)::SetBridgeProxyGroupId);
    }
    groupInfo->GetBridgePileId().CopyToProto(&Record, &decltype(Record)::SetBridgePileId);
}

TEvWhiteboard::TEvSystemStateUpdate::TEvSystemStateUpdate(
        TInstant startTime,
        ui32 numberOfCpus,
        const TString& version) {
    Record.SetStartTime(startTime.MilliSeconds());
    Record.SetNumberOfCpus(numberOfCpus);
    Record.SetVersion(version);
}

TEvWhiteboard::TEvSystemStateUpdate::TEvSystemStateUpdate(
        TInstant startTime,
        ui32 numberOfCpus) {
    Record.SetStartTime(startTime.MilliSeconds());
    Record.SetNumberOfCpus(numberOfCpus);
}

TEvWhiteboard::TEvSystemStateUpdate::TEvSystemStateUpdate(const TVector<double>& loadAverage) {
    for (double d : loadAverage) {
        Record.AddLoadAverage(d);
    }
}

TEvWhiteboard::TEvSystemStateUpdate::TEvSystemStateUpdate(const TVector<std::tuple<TString, double, ui32, ui32>>& poolStats) {
    for (const auto& row : poolStats) {
        auto& pb = *Record.AddPoolStats();
        pb.SetName(std::get<0>(row));
        pb.SetUsage(std::get<1>(row));
        pb.SetThreads(std::get<2>(row));
        pb.SetLimit(std::get<3>(row));
    }
}

TEvWhiteboard::TEvSystemStateUpdate *TEvWhiteboard::CreateTotalSessionsUpdateRequest(ui32 totalSessions) {
    TEvSystemStateUpdate *request = new TEvSystemStateUpdate();
    request->Record.SetTotalSessions(totalSessions);
    return request;
}

} // namespace NKikimr::NNodeWhiteboard

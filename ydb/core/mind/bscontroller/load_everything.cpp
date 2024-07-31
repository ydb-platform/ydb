#include "impl.h"

namespace NKikimr {
namespace NBsController {

class TBlobStorageController::TTxLoadEverything : public TTransactionBase<TBlobStorageController> {
public:
    TTxLoadEverything(TBlobStorageController *controller)
        : TBase(controller)
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_LOAD_EVERYTHING; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXLE01, "TTxLoadEverything Execute");

        NIceDb::TNiceDb db(txc.DB);

        {
            // precharge
            auto state = db.Table<Schema::State>().Range().Select();
            auto nodes = db.Table<Schema::Node>().Range().Select();
            auto disk = db.Table<Schema::PDisk>().Range().Select();
            auto slot = db.Table<Schema::VSlot>().Range().Select();
            auto group = db.Table<Schema::Group>().Range().Select();
            auto pdiskMetrics = db.Table<Schema::PDiskMetrics>().Range().Select();
            auto vdiskMetrics = db.Table<Schema::VDiskMetrics>().Range().Select();
            auto hostConfig = db.Table<Schema::HostConfig>().Range().Select();
            auto hostConfigDrive = db.Table<Schema::HostConfigDrive>().Range().Select();
            auto box = db.Table<Schema::Box>().Range().Select();
            auto boxUser = db.Table<Schema::BoxUser>().Range().Select();
            auto boxHost = db.Table<Schema::BoxHostV2>().Range().Select();
            auto boxStoragePool = db.Table<Schema::BoxStoragePool>().Range().Select();
            auto boxStoragePoolUser = db.Table<Schema::BoxStoragePoolUser>().Range().Select();
            auto boxStoragePoolPDiskFilter = db.Table<Schema::BoxStoragePoolPDiskFilter>().Range().Select();
            auto groupStoragePool = db.Table<Schema::GroupStoragePool>().Range().Select();
            auto groupLatencies = db.Table<Schema::GroupLatencies>().Select();
            auto scrubState = db.Table<Schema::ScrubState>().Select();
            auto pdiskSerial = db.Table<Schema::DriveSerial>().Select();
            auto blobDepotDeleteQueue = db.Table<Schema::BlobDepotDeleteQueue>().Select();
            if (!state.IsReady()
                    || !nodes.IsReady()
                    || !disk.IsReady()
                    || !slot.IsReady()
                    || !group.IsReady()
                    || !pdiskMetrics.IsReady()
                    || !vdiskMetrics.IsReady()
                    || !hostConfig.IsReady()
                    || !hostConfigDrive.IsReady()
                    || !box.IsReady()
                    || !boxUser.IsReady()
                    || !boxHost.IsReady()
                    || !boxStoragePool.IsReady()
                    || !boxStoragePoolUser.IsReady()
                    || !boxStoragePoolPDiskFilter.IsReady()
                    || !groupStoragePool.IsReady()
                    || !groupLatencies.IsReady()
                    || !scrubState.IsReady()
                    || !pdiskSerial.IsReady()
                    || !blobDepotDeleteQueue.IsReady()) {
                return false;
            }
        }

        // State
        {
            using T = Schema::State;
            auto state = db.Table<T>().Select();
            if (!state.IsReady())
                return false;
            if (state.IsValid()) {
                Self->NextGroupID = TGroupId::FromValue(state.GetValue<T::NextGroupID>());
                Self->NextVirtualGroupId = TGroupId::FromValue(state.GetValueOrDefault<T::NextVirtualGroupId>());
                Self->NextStoragePoolId = state.GetValue<T::NextStoragePoolId>();
                Self->NextOperationLogIndex = state.GetValueOrDefault<T::NextOperationLogIndex>(1);
                Self->DefaultMaxSlots = state.GetValue<T::DefaultMaxSlots>();
                if (state.HaveValue<T::InstanceId>()) {
                    Self->InstanceId = state.GetValue<T::InstanceId>();
                }
                Self->SelfHealEnable = state.GetValue<T::SelfHealEnable>();
                Self->DonorMode = state.GetValue<T::DonorModeEnable>();
                Self->ScrubPeriodicity = TDuration::Seconds(state.GetValue<T::ScrubPeriodicity>());
                Self->SerialManagementStage = state.GetValue<T::SerialManagementStage>();
                Self->PDiskSpaceMarginPromille = state.GetValue<T::PDiskSpaceMarginPromille>();
                Self->GroupReserveMin = state.GetValue<T::GroupReserveMin>();
                Self->GroupReservePart = state.GetValue<T::GroupReservePart>();
                Self->MaxScrubbedDisksAtOnce = state.GetValue<T::MaxScrubbedDisksAtOnce>();
                Self->PDiskSpaceColorBorder = state.GetValue<T::PDiskSpaceColorBorder>();
                Self->GroupLayoutSanitizerEnabled = state.GetValue<T::GroupLayoutSanitizer>();
                Self->AllowMultipleRealmsOccupation = state.GetValueOrDefault<T::AllowMultipleRealmsOccupation>();
                Self->SysViewChangedSettings = true;
                Self->UseSelfHealLocalPolicy = state.GetValue<T::UseSelfHealLocalPolicy>();
                Self->TryToRelocateBrokenDisksLocallyFirst = state.GetValue<T::TryToRelocateBrokenDisksLocallyFirst>();
            }
        }

        // Node
        Self->Nodes.clear();
        {
            auto nodes = db.Table<Schema::Node>().Range().Select();
            if (!nodes.IsReady())
                return false;
            while (!nodes.EndOfSet()) {
                Self->AddNode(nodes.GetKey(),
                    {nodes.GetValue<Schema::Node::NextPDiskID>()});
                if (!nodes.Next())
                    return false;
            }
        }

        // GroupStoragePool
        std::unordered_map<TGroupId, TBoxStoragePoolId> groupToStoragePool;
        {
            using Table = Schema::GroupStoragePool;
            auto groupStoragePool = db.Table<Table>().Range().Select();
            if (!groupStoragePool.IsReady()) {
                return false;
            }
            while (groupStoragePool.IsValid()) {
                const auto groupId = groupStoragePool.GetValue<Table::GroupId>();
                const auto boxId = groupStoragePool.GetValue<Table::BoxId>();
                const auto storagePoolId = groupStoragePool.GetValue<Table::StoragePoolId>();
                const bool inserted = groupToStoragePool.try_emplace(TGroupId::FromValue(groupId), boxId, storagePoolId).second;
                Y_ABORT_UNLESS(inserted);
                Self->StoragePoolGroups.emplace(TBoxStoragePoolId(boxId, storagePoolId), TGroupId::FromValue(groupId));
                if (!groupStoragePool.Next()) {
                    return false;
                }
            }
        }

        // parse group geometry -- it is not known prior to VSlot processing, but we need it to construct correct groups
        std::unordered_map<TGroupId, std::tuple<ui32, ui32, ui32>> geometry;
        {
            using Table = Schema::VSlot;
            auto vslot = db.Table<Table>().Select();
            if (!vslot.IsReady()) {
                return false;
            }
            while (vslot.IsValid()) {
                const auto groupId = vslot.GetValue<Table::GroupID>();
                auto& record = geometry[groupId];

                {
                    auto& numFailRealms = std::get<0>(record);
                    const auto failRealmIdx = vslot.GetValue<Table::RingIdx>();
                    numFailRealms = Max(numFailRealms, failRealmIdx + 1);
                }

                {
                    auto& numFailDomainsPerFailRealm = std::get<1>(record);
                    const auto failDomainIdx = vslot.GetValue<Table::FailDomainIdx>();
                    numFailDomainsPerFailRealm = Max(numFailDomainsPerFailRealm, failDomainIdx + 1);
                }

                {
                    auto& numVDisksPerFailDomain = std::get<2>(record);
                    const auto vdiskIdx = vslot.GetValue<Table::VDiskIdx>();
                    numVDisksPerFailDomain = Max(numVDisksPerFailDomain, vdiskIdx + 1);
                }

                if (!vslot.Next()) {
                    return false;
                }
            }
        }

        // Group
        Self->GroupMap.clear();
        Self->GroupLookup.clear();
        Self->OwnerIdIdxToGroup.clear();
        Self->IndexGroupSpeciesToGroup.clear();
        {
            using T = Schema::Group;
            auto groups = db.Table<T>().Range().Select();
            if (!groups.IsReady())
                return false;
            while (!groups.EndOfSet()) {
                const auto it = groupToStoragePool.find(groups.GetKey());
                Y_ABORT_UNLESS(it != groupToStoragePool.end());
                const TBoxStoragePoolId storagePoolId = it->second;
                groupToStoragePool.erase(it);

                // geometry may be absent for virtual or finally decommitted group
                const auto geomIt = geometry.find(groups.GetKey());
                const auto geom = geomIt != geometry.end() ? geomIt->second : std::make_tuple(0u, 0u, 0u);

                TGroupInfo& group = Self->AddGroup(groups.GetKey(),
                                                   groups.GetValue<T::Generation>(),
                                                   groups.GetValue<T::Owner>(),
                                                   groups.GetValue<T::ErasureSpecies>(),
                                                   groups.GetValue<T::DesiredPDiskCategory>(),
                                                   groups.GetValueOrDefault<T::DesiredVDiskCategory>(NKikimrBlobStorage::TVDiskKind::Default),
                                                   groups.GetValueOrDefault<T::EncryptionMode>(),
                                                   groups.GetValueOrDefault<T::LifeCyclePhase>(),
                                                   groups.GetValueOrDefault<T::MainKeyId>(nullptr),
                                                   groups.GetValueOrDefault<T::EncryptedGroupKey>(nullptr),
                                                   groups.GetValueOrDefault<T::GroupKeyNonce>(),
                                                   groups.GetValueOrDefault<T::MainKeyVersion>(),
                                                   groups.GetValueOrDefault<T::Down>(),
                                                   groups.GetValueOrDefault<T::SeenOperational>(),
                                                   storagePoolId,
                                                   std::get<0>(geom),
                                                   std::get<1>(geom),
                                                   std::get<2>(geom));

                group.DecommitStatus = groups.GetValueOrDefault<T::DecommitStatus>();
                if (group.DecommitStatus == NKikimrBlobStorage::TGroupDecommitStatus::DONE) {
                    group.VDisksInGroup.clear();
                }

#define OPTIONAL(NAME) \
                if (groups.HaveValue<T::NAME>()) { \
                    group.NAME = groups.GetValue<T::NAME>(); \
                }

                OPTIONAL(VirtualGroupName)
                OPTIONAL(VirtualGroupState)
                OPTIONAL(HiveId)
                OPTIONAL(Database)
                OPTIONAL(BlobDepotConfig)
                OPTIONAL(BlobDepotId)
                OPTIONAL(ErrorReason)

                if (groups.HaveValue<T::Metrics>()) {
                    const bool success = group.GroupMetrics.emplace().ParseFromString(groups.GetValue<T::Metrics>());
                    Y_DEBUG_ABORT_UNLESS(success);
                }

#undef OPTIONAL

                Self->OwnerIdIdxToGroup.emplace(groups.GetValue<T::Owner>(), groups.GetKey());
                Self->IndexGroupSpeciesToGroup[group.GetGroupSpecies()].push_back(group.ID);
                if (!groups.Next())
                    return false;
            }
        }
        Y_ABORT_UNLESS(groupToStoragePool.empty());

        // HostConfig, Box, BoxStoragePool
        if (!NTableAdapter::FetchTable<Schema::HostConfig>(db, Self, Self->HostConfigs)
                || !NTableAdapter::FetchTable<Schema::Box>(db, Self, Self->Boxes)
                || !NTableAdapter::FetchTable<Schema::BoxStoragePool>(db, Self, Self->StoragePools)
                || !NTableAdapter::FetchTable<Schema::DriveSerial>(db, Self, Self->DrivesSerials)
                || !NTableAdapter::FetchTable<Schema::BlobDepotDeleteQueue>(db, Self, Self->BlobDepotDeleteQueue)) {
            return false;
        }
        for (const auto& [storagePoolId, storagePool] : Self->StoragePools) {
            Self->SysViewChangedStoragePools.insert(storagePoolId);
        }

        // create revmap
        std::map<std::tuple<TNodeId, TString>, TBoxId> driveToBox;
        for (const auto& [boxId, box] : Self->Boxes) {
            for (const auto& [host, value] : box.Hosts) {
                const auto& nodeId = Self->HostRecords->ResolveNodeId(host, value);
                Y_VERIFY_S(nodeId, "HostKey# " << host.Fqdn << ":" << host.IcPort << " does not resolve to a node");
                if (const auto it = Self->HostConfigs.find(value.HostConfigId); it != Self->HostConfigs.end()) {
                    for (const auto& [drive, info] : it->second.Drives) {
                        const bool inserted = driveToBox.emplace(std::make_pair(*nodeId, drive.Path), boxId).second;
                        Y_ABORT_UNLESS(inserted, "duplicate Box-generated drive BoxId# %" PRIu64 " FQDN# %s IcPort# %d Path# '%s'",
                            host.BoxId, host.Fqdn.data(), host.IcPort, drive.Path.data());
                    }
                } else {
                    Y_ABORT("HostConfigId# %" PRIu64 " not found in BoxId# %" PRIu64 " FQDN# %s IcPort# %d",
                        value.HostConfigId, host.BoxId, host.Fqdn.data(), host.IcPort);
                }
            }
        }

        for (const auto& [_, info] : Self->DrivesSerials) {
            if (info->LifeStage == NKikimrBlobStorage::TDriveLifeStage::ADDED_BY_DSTOOL) {
                Y_ABORT_UNLESS(info->NodeId);
                Y_ABORT_UNLESS(info->Path);
                driveToBox.emplace(std::make_tuple(*info->NodeId, *info->Path), info->BoxId);
            }
        }

        // PDisks
        Self->PDisks.clear();
        {
            using T = Schema::PDisk;
            auto disks = db.Table<T>().Range().Select();
            if (!disks.IsReady())
                return false;
            while (!disks.EndOfSet()) {
                auto getOpt = [&](auto col) {
                    using TCol = decltype(col);
                    TMaybe<typename TCol::Type> res;
                    if (disks.HaveValue<TCol>()) {
                        res = disks.GetValue<TCol>();
                    }
                    return res;
                };

                THostId hostId;
                TBoxId boxId;
                TString path = disks.GetValue<T::Path>();
                Y_VERIFY_S(path, "Couldn't find path for pdiskId# " << disks.GetValue<T::PDiskID>());

                if (const auto& x = Self->HostRecords->GetHostId(disks.GetValue<T::NodeID>())) {
                    hostId = *x;
                } else {
                    Y_ABORT("unknown node NodeId# %" PRIu32, disks.GetValue<T::NodeID>());
                }

                // find the owning box
                if (const auto it = driveToBox.find(std::make_tuple(disks.GetValue<T::NodeID>(), path)); it != driveToBox.end()) {
                    boxId = it->second;
                    driveToBox.erase(it);
                } else {
                    Y_ABORT("PDisk NodeId# %" PRIu32 " PDiskId# %" PRIu32 " not belonging to a box",
                        disks.GetValue<T::NodeID>(), disks.GetValue<T::PDiskID>());
                }

                const auto it = Self->StaticPDisks.find(disks.GetKey());
                const ui32 staticSlotUsage = it != Self->StaticPDisks.end() ? it->second.StaticSlotUsage : 0;

                // construct PDisk item
                Self->AddPDisk(disks.GetKey(), hostId, disks.GetValue<T::Path>(), disks.GetValue<T::Category>(),
                    disks.GetValue<T::Guid>(), getOpt(T::SharedWithOs()), getOpt(T::ReadCentric()),
                    disks.GetValueOrDefault<T::NextVSlotId>(), disks.GetValue<T::PDiskConfig>(), boxId,
                    Self->DefaultMaxSlots, disks.GetValue<T::Status>(), disks.GetValue<T::Timestamp>(),
                    disks.GetValue<T::DecommitStatus>(), disks.GetValue<T::Mood>(), disks.GetValue<T::ExpectedSerial>(),
                    disks.GetValue<T::LastSeenSerial>(), disks.GetValue<T::LastSeenPath>(), staticSlotUsage);

                if (!disks.Next())
                    return false;
            }
        }
        Y_ABORT_UNLESS(driveToBox.empty(), "missing PDisks defined by the box exist");

        // PDiskMetrics
        TVector<Schema::PDiskMetrics::TKey::Type> pdiskMetricsToDelete;
        {
            using Table = Schema::PDiskMetrics;
            auto table = db.Table<Table>().Range().Select();
            if (!table.IsReady()) {
                return false;
            }
            while (!table.EndOfSet()) {
                const TPDiskId pdiskId(table.GetValue<Table::NodeID>(), table.GetValue<Table::PDiskID>());
                if (TPDiskInfo *pdisk = Self->FindPDisk(pdiskId)) {
                    pdisk->Metrics = table.GetValueOrDefault<Table::Metrics>();
                } else {
                    pdiskMetricsToDelete.push_back(table.GetKey());
                }
                if (!table.Next()) {
                    return false;
                }
            }
        }

        // VSlots
        Self->VSlots.clear();
        {
            using T = Schema::VSlot;
            auto slot = db.Table<T>().Range().Select();
            if (!slot.IsReady())
                return false;
            while (!slot.EndOfSet()) {
                const TVSlotId& vslotId(slot.GetKey());
                TPDiskInfo *pdisk = Self->FindPDisk(vslotId.ComprisingPDiskId());
                Y_ABORT_UNLESS(pdisk);

                const TGroupId groupId = slot.GetValue<T::GroupID>();
                Y_ABORT_UNLESS(groupId.GetRawId());

                auto& x = Self->AddVSlot(vslotId, pdisk, groupId, slot.GetValueOrDefault<T::GroupPrevGeneration>(),
                    slot.GetValue<T::GroupGeneration>(), slot.GetValue<T::Category>(), slot.GetValue<T::RingIdx>(),
                    slot.GetValue<T::FailDomainIdx>(), slot.GetValue<T::VDiskIdx>(), slot.GetValueOrDefault<T::Mood>(),
                    Self->FindGroup(groupId), &Self->VSlotReadyTimestampQ, slot.GetValue<T::LastSeenReady>(),
                    slot.GetValue<T::ReplicationTime>());
                if (x.LastSeenReady != TInstant::Zero()) {
                    Self->NotReadyVSlotIds.insert(x.VSlotId);
                }

                if (!slot.Next()) {
                    return false;
                }
            }
        }
        for (const auto& [id, group] : Self->GroupMap) {
            group->FinishVDisksInGroup();
        }
        for (const auto& [vslotId, vslot] : Self->VSlots) {
            if (vslot->IsBeingDeleted()) {
                if (TGroupInfo *group = Self->FindGroup(vslot->GroupId)) {
                    group->VSlotsBeingDeleted.insert(vslotId);
                }
            }
        }

        // tie donors and acceptors
        for (const auto& [vslotId, vslot] : Self->VSlots) {
            if (vslot->Mood == TMood::Donor) {
                const TVSlotInfo *acceptor = Self->FindAcceptor(*vslot);
                const_cast<TVSlotInfo&>(*acceptor).Donors.insert(vslotId);
            }
        }

        // VDiskMetrics
        TVector<Schema::VDiskMetrics::TKey::Type> vdiskMetricsToDelete;
        {
            using Table = Schema::VDiskMetrics;
            auto table = db.Table<Table>().Range().Select();
            if (!table.IsReady()) {
                return false;
            }
            while (!table.EndOfSet()) {
                const TVDiskID key(TGroupId::FromValue(table.GetValue<Table::GroupID>()), table.GetValue<Table::GroupGeneration>(),
                    table.GetValue<Table::Ring>(), table.GetValue<Table::FailDomain>(), table.GetValue<Table::VDisk>());
                if (TVSlotInfo *slot = Self->FindVSlot(key)) {
                    slot->Metrics = table.GetValueOrDefault<Table::Metrics>();
                    slot->UpdateVDiskMetrics();
                } else {
                    vdiskMetricsToDelete.push_back(table.GetKey());
                }
                if (!table.Next()) {
                    return false;
                }
            }
        }

        // GroupLatencies
        {
            using Table = Schema::GroupLatencies;
            auto groupLatencies = db.Table<Table>().Select();
            if (!groupLatencies.IsReady()) {
                return false;
            }
            while (groupLatencies.IsValid()) {
                const TGroupId groupId = TGroupId::FromValue(groupLatencies.GetValue<Table::GroupId>());
                if (TGroupInfo *groupInfo = Self->FindGroup(groupId)) {
                    if (groupLatencies.HaveValue<Table::PutTabletLogLatencyUs>()) {
                        groupInfo->LatencyStats.PutTabletLog = TDuration::MicroSeconds(groupLatencies.GetValue<Table::PutTabletLogLatencyUs>());
                    }
                    if (groupLatencies.HaveValue<Table::PutUserDataLatencyUs>()) {
                        groupInfo->LatencyStats.PutUserData = TDuration::MicroSeconds(groupLatencies.GetValue<Table::PutUserDataLatencyUs>());
                    }
                    if (groupLatencies.HaveValue<Table::GetFastLatencyUs>()) {
                        groupInfo->LatencyStats.GetFast = TDuration::MicroSeconds(groupLatencies.GetValue<Table::GetFastLatencyUs>());
                    }
                } else {
                    STLOG(PRI_ERROR, BS_CONTROLLER, BSCTXLE02, "Nonexistent group in GroupLatencies", (GroupId, groupId));
                }

                if (!groupLatencies.Next()) {
                    return false;
                }
            }
        }

        // primitive garbage collection for obsolete metrics
        for (const auto& key : pdiskMetricsToDelete) {
            db.Table<Schema::PDiskMetrics>().Key(key).Delete();
        }
        for (const auto& key : vdiskMetricsToDelete) {
            db.Table<Schema::VDiskMetrics>().Key(key).Delete();
        }

        // apply storage pool stats
        std::unordered_map<TBoxStoragePoolId, ui64> allocatedSizeMap;
        for (const auto& [vslotId, slot] : Self->VSlots) {
            if (!slot->IsBeingDeleted()) {
                TGroupInfo *group = Self->FindGroup(slot->GroupId);
                Y_ABORT_UNLESS(group);
                allocatedSizeMap[group->StoragePoolId] += slot->Metrics.GetAllocatedSize();
            }
        }
        for (const auto& [id, info] : Self->StoragePools) {
            Self->StoragePoolStat->AddStoragePool(TStoragePoolStat::ConvertId(id), info.Name, allocatedSizeMap[id]);
        }
        for (const auto& [groupId, group] : Self->GroupMap) {
            group->StatusFlags = group->GetStorageStatusFlags();
            Self->StoragePoolStat->Update(TStoragePoolStat::ConvertId(group->StoragePoolId), std::nullopt, group->StatusFlags);
        }

        // scrub state
        Self->ScrubState.Clear();
        {
            using Table = Schema::ScrubState;
            auto scrubState = db.Table<Table>().Select();
            if (!scrubState.IsReady()) {
                return false;
            }
            while (scrubState.IsValid()) {
                Self->ScrubState.AddItem(
                    scrubState.GetKey(),
                    scrubState.HaveValue<Table::State>() ? std::make_optional(scrubState.GetValue<Table::State>()) : std::nullopt,
                    scrubState.GetValue<Table::ScrubCycleStartTime>(),
                    scrubState.GetValue<Table::ScrubCycleFinishTime>(),
                    scrubState.GetValue<Table::Success>());
                if (!scrubState.Next()) {
                    return false;
                }
            }
        }

        // calculate group status for all groups
        for (auto& [id, group] : Self->GroupMap) {
            group->CalculateGroupStatus();
        }

        return true;
    }

    void Complete(const TActorContext&) override {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXLE03, "TTxLoadEverything Complete");
        Self->LoadFinished();
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXLE04, "TTxLoadEverything InitQueue processed");
    }
};

ITransaction* TBlobStorageController::CreateTxLoadEverything() {
    return new TTxLoadEverything(this);
}

} // NBlobStorageController
} // NKikimr

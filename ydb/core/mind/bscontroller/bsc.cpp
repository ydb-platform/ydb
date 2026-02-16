#include "impl.h"
#include "config.h"
#include "self_heal.h"
#include "sys_view.h"
#include "cluster_balancing.h"
#include "console_interaction.h"
#include "group_geometry_info.h"
#include "group_layout_checker.h"
#include "util.h"

#include <ydb/core/blobstorage/nodewarden/distconf.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_impl.h>
#include <ydb/library/yaml_config/public/yaml_config.h>

#include <library/cpp/streams/zstd/zstd.h>

namespace NKikimr {

TString TGroupID::ToString() const {
    TStringStream str;
    str << "{TGroupID ConfigurationType# ";
    switch (ConfigurationType()) {
        case EGroupConfigurationType::Static: str << "Static"; break;
        case EGroupConfigurationType::Dynamic: str << "Dynamic"; break;
        case EGroupConfigurationType::Virtual: str << "Virtual"; break;
    }
    str << " GroupLocalID# " << GroupLocalID();
    str << "}";
    return str.Str();
}

IActor* CreateFlatBsController(const TActorId &tablet, TTabletStorageInfo *info) {
    return new NBsController::TBlobStorageController(tablet, info);
}

namespace NBsController {

TBlobStorageController::TVSlotInfo::TVSlotInfo(TVSlotId vSlotId, TPDiskInfo *pdisk, TGroupId groupId,
        Table::GroupGeneration::Type groupPrevGeneration, Table::GroupGeneration::Type groupGeneration,
        Table::Category::Type kind, Table::RingIdx::Type ringIdx, Table::FailDomainIdx::Type failDomainIdx,
        Table::VDiskIdx::Type vDiskIdx, Table::Mood::Type mood, TGroupInfo *group,
        TVSlotReadyTimestampQ *vslotReadyTimestampQ, TInstant lastSeenReady, TDuration replicationTime,
        Table::DDiskNumVChunksClaimed::Type ddiskNumVChunksClaimed)
    : VSlotId(vSlotId)
    , PDisk(pdisk)
    , GroupId(groupId)
    , GroupPrevGeneration(groupPrevGeneration)
    , GroupGeneration(groupGeneration)
    , Kind(kind)
    , RingIdx(ringIdx)
    , FailDomainIdx(failDomainIdx)
    , VDiskIdx(vDiskIdx)
    , Mood(mood)
    , LastSeenReady(lastSeenReady)
    , ReplicationTime(replicationTime)
    , DDiskNumVChunksClaimed(ddiskNumVChunksClaimed)
    , VSlotReadyTimestampQ(*vslotReadyTimestampQ)
{
    Y_ABORT_UNLESS(pdisk);
    const auto& [it, inserted] = pdisk->VSlotsOnPDisk.emplace(VSlotId.VSlotId, this);
    Y_ABORT_UNLESS(inserted);

    if (!IsBeingDeleted()) {
        if (Mood != TMood::Donor) {
            Y_ABORT_UNLESS(group);
            Group = group;
            group->AddVSlot(this);
        }
        pdisk->NumActiveSlots += TPDiskConfig::GetOwnerWeight(
            group->GroupSizeInUnits,
            pdisk->SlotSizeInUnits);
    }
}

bool TBlobStorageController::TGroupInfo::CalculateGroupStatus() {
    const TGroupStatus prev = Status;
    Status = {NKikimrBlobStorage::TGroupStatus::FULL, NKikimrBlobStorage::TGroupStatus::FULL};

    if ((VirtualGroupState == NKikimrBlobStorage::EVirtualGroupState::CREATE_FAILED ||
            VirtualGroupState == NKikimrBlobStorage::EVirtualGroupState::NEW) && VDisksInGroup.empty()) {
        Status.MakeWorst(NKikimrBlobStorage::TGroupStatus::DISINTEGRATED, NKikimrBlobStorage::TGroupStatus::DISINTEGRATED);
    }

    if (VDisksInGroup) {
        TBlobStorageGroupInfo::TGroupVDisks failed(Topology.get());
        TBlobStorageGroupInfo::TGroupVDisks failedByPDisk(Topology.get());
        for (const TVSlotInfo *slot : VDisksInGroup) {
            if (!slot->IsReady) {
                failed |= {Topology.get(), slot->GetShortVDiskId()};
            } else if (!slot->PDisk->HasGoodExpectedStatus()) {
                failedByPDisk |= {Topology.get(), slot->GetShortVDiskId()};
            }
        }
        Status.MakeWorst(DeriveStatus(Topology.get(), failed), DeriveStatus(Topology.get(), failed | failedByPDisk));
    }

    return Status != prev;
}

void TBlobStorageController::TGroupInfo::CalculateLayoutStatus(TBlobStorageController *self,
        TBlobStorageGroupInfo::TTopology *topology, const std::function<TGroupGeometryInfo()>& getGeom) {
    LayoutCorrect = true;
    if (VDisksInGroup) {
        NLayoutChecker::TGroupLayout layout(*topology);
        NLayoutChecker::TDomainMapper mapper;
        auto geom = getGeom();

        for (size_t index = 0; index < VDisksInGroup.size(); ++index) {
            const TVSlotInfo *slot = VDisksInGroup[index];
            TPDiskId pdiskId = slot->VSlotId.ComprisingPDiskId();
            const auto& location = self->HostRecords->GetLocation(pdiskId.NodeId);
            layout.AddDisk({mapper, location, pdiskId, geom}, index);
        }

        LayoutCorrect = layout.IsCorrect();
    }
}

bool TBlobStorageController::TGroupInfo::FillInGroupParameters(
        NKikimrBlobStorage::TEvControllerSelectGroupsResult::TGroupParameters *params,
        TBlobStorageController *self) const {
    if (GroupMetrics) {
        params->MergeFrom(GroupMetrics->GetGroupParameters());
        return true;
    } else if (BridgeGroupInfo) {
        Y_ABORT_UNLESS(self->BridgeInfo);
        bool res = true;
        for (const auto& pile : BridgeGroupInfo->GetBridgeGroupState().GetPile()) {
            const auto groupId = TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId);
            if (TGroupInfo *groupInfo = self->FindGroup(groupId)) {
                Y_ABORT_UNLESS(groupInfo->BridgePileId);
                if (!NBridge::PileStateTraits(self->BridgeInfo->GetPile(groupInfo->BridgePileId)->State).AllowsConnection) {
                    continue; // ignore groups from disconnected piles
                }
                res &= groupInfo->FillInGroupParameters(params, self);
            } else {
                Y_DEBUG_ABORT();
            }
        }
        return res;
    } else {
        bool res = true;
        res &= FillInResources(params->MutableAssuredResources(), true);
        res &= FillInResources(params->MutableCurrentResources(), false);
        res &= FillInVDiskResources(params);
        params->SetGroupSizeInUnits(GroupSizeInUnits);
        return res;
    }
}

bool TBlobStorageController::TGroupInfo::FillInResources(
        NKikimrBlobStorage::TEvControllerSelectGroupsResult::TGroupParameters::TResources *pb, bool countMaxSlots) const {
    // count minimum params for each of slots assuming they are shared fairly between all the slots (expected or currently created)
    std::optional<ui64> size;
    std::optional<double> iops;
    std::optional<ui64> readThroughput;
    std::optional<ui64> writeThroughput;
    std::optional<double> occupancy;

    Y_ABORT_UNLESS(Topology);
    TBlobStorageGroupInfo::TGroupVDisks vdisksWithAllMetrics(Topology.get());

    for (const TVSlotInfo *vslot : VDisksInGroup) {
        const TPDiskInfo *pdisk = vslot->PDisk;
        const auto& metrics = pdisk->Metrics;

        ui32 maxSlots = 0;
        ui32 slotSizeInUnits = 0;
        pdisk->ExtractInferredPDiskSettings(maxSlots, slotSizeInUnits);

        ui64 vdiskSlotSize = 0;
        const ui32 weight = TPDiskConfig::GetOwnerWeight(GroupSizeInUnits, slotSizeInUnits);
        if (metrics.HasEnforcedDynamicSlotSize()) {
            vdiskSlotSize = metrics.GetEnforcedDynamicSlotSize() * weight;
        } else if (metrics.GetTotalSize()) {
            const ui32 shareFactor = (countMaxSlots && maxSlots) ? maxSlots : pdisk->NumActiveSlots;
            vdiskSlotSize = metrics.GetTotalSize() / shareFactor * weight;
        }
        if (vdiskSlotSize) {
            size = Min(size.value_or(Max<ui64>()), vdiskSlotSize);
        }

        const ui32 shareFactor = (countMaxSlots && maxSlots) ? maxSlots : pdisk->VSlotsOnPDisk.size();
        if (metrics.HasMaxIOPS()) {
            iops = Min(iops.value_or(Max<double>()), metrics.GetMaxIOPS() * 100 / shareFactor * 0.01);
        }
        if (metrics.HasMaxReadThroughput()) {
            readThroughput = Min(readThroughput.value_or(Max<ui64>()), metrics.GetMaxReadThroughput() / shareFactor);
        }
        if (metrics.HasMaxWriteThroughput()) {
            writeThroughput = Min(writeThroughput.value_or(Max<ui64>()), metrics.GetMaxWriteThroughput() / shareFactor);
        }
        if (const auto& vm = vslot->Metrics; vm.HasNormalizedOccupancy()) {
            occupancy = Max(occupancy.value_or(0), vm.GetNormalizedOccupancy());
        }

        const bool hasAllMetrics = metrics.HasMaxIOPS()
            && metrics.HasMaxReadThroughput()
            && metrics.HasMaxWriteThroughput()
            && vslot->Metrics.HasNormalizedOccupancy();
        if (hasAllMetrics) {
            vdisksWithAllMetrics |= {Topology.get(), vslot->GetShortVDiskId()};
        }
    }

    // and recalculate it to the total size of the group according to the erasure
    TBlobStorageGroupType type(ErasureSpecies);
    const double factor = (double)VDisksInGroup.size() * type.DataParts() / type.TotalPartCount();
    if (size) {
        pb->SetSpace(Min<ui64>(pb->HasSpace() ? pb->GetSpace() : Max<ui64>(), *size * factor));
    }
    if (iops) {
        pb->SetIOPS(Min<double>(pb->HasIOPS() ? pb->GetIOPS() : Max<double>(), *iops * VDisksInGroup.size() / type.TotalPartCount()));
    }
    if (readThroughput) {
        pb->SetReadThroughput(Min<ui64>(pb->HasReadThroughput() ? pb->GetReadThroughput() : Max<ui64>(), *readThroughput * factor));
    }
    if (writeThroughput) {
        pb->SetWriteThroughput(Min<ui64>(pb->HasWriteThroughput() ? pb->GetReadThroughput() : Max<ui64>(), *writeThroughput * factor));
    }
    if (occupancy) {
        pb->SetOccupancy(Max<double>(pb->HasOccupancy() ? pb->GetOccupancy() : Min<double>(), *occupancy));
    }

    return Topology->GetQuorumChecker().CheckQuorumForGroup(vdisksWithAllMetrics);
}

bool TBlobStorageController::TGroupInfo::FillInVDiskResources(
        NKikimrBlobStorage::TEvControllerSelectGroupsResult::TGroupParameters *pb) const {
    Y_ABORT_UNLESS(Topology);
    TBlobStorageGroupInfo::TGroupVDisks vdisksWithAllMetrics(Topology.get());

    TBlobStorageGroupType type(ErasureSpecies);
    const double f = (double)VDisksInGroup.size() * type.DataParts() / type.TotalPartCount();
    for (const TVSlotInfo *vslot : VDisksInGroup) {
        const auto& m = vslot->Metrics;
        if (m.HasAvailableSize()) {
            pb->SetAvailableSize(Min<ui64>(pb->HasAvailableSize() ? pb->GetAvailableSize() : Max<ui64>(), f * m.GetAvailableSize()));
        }
        if (m.HasAllocatedSize()) {
            pb->SetAllocatedSize(Max<ui64>(pb->HasAllocatedSize() ? pb->GetAllocatedSize() : 0, f * m.GetAllocatedSize()));
        }
        if (m.HasCapacityAlert()) {
            pb->SetSpaceColor(pb->HasSpaceColor() ? Max(pb->GetSpaceColor(), m.GetCapacityAlert()) : m.GetCapacityAlert());
        }

        const bool hasAllMetrics = m.HasAvailableSize() && m.HasAllocatedSize();
        if (hasAllMetrics) {
            vdisksWithAllMetrics |= {Topology.get(), vslot->GetShortVDiskId()};
        }
    }

    return Topology->GetQuorumChecker().CheckQuorumForGroup(vdisksWithAllMetrics);
}

NKikimrBlobStorage::TGroupStatus::E TBlobStorageController::DeriveStatus(const TBlobStorageGroupInfo::TTopology *topology,
        const TBlobStorageGroupInfo::TGroupVDisks& failed) {
    auto& checker = *topology->QuorumChecker;
    if (!failed.GetNumSetItems()) { // all disks of group are operational
        return NKikimrBlobStorage::TGroupStatus::FULL;
    } else if (!checker.CheckFailModelForGroup(failed)) { // fail model exceeded
        return NKikimrBlobStorage::TGroupStatus::DISINTEGRATED;
    } else if (checker.IsDegraded(failed)) { // group degraded
        return NKikimrBlobStorage::TGroupStatus::DEGRADED;
    } else if (failed.GetNumSetItems()) { // group partially available, but not degraded
        return NKikimrBlobStorage::TGroupStatus::PARTIAL;
    } else {
        Y_ABORT("unexpected case");
    }
}

void TBlobStorageController::OnActivateExecutor(const TActorContext&) {
    StartConsoleInteraction();

    // create stat processor
    StatProcessorActorId = Register(CreateStatProcessorActor());

    // create system views collector
    SystemViewsCollectorId = Register(CreateSystemViewsCollector());

    Executor()->RegisterExternalTabletCounters(TabletCountersPtr);
    if (!ResponsivenessPinger) {
        ResponsivenessPinger = new TTabletResponsivenessPinger(TabletCounters->Simple()[NBlobStorageController::COUNTER_RESPONSE_TIME_USEC], TDuration::Seconds(1));
        ResponsivenessActorID = RegisterWithSameMailbox(ResponsivenessPinger);
    }

    // create storage pool stats monitor
    StoragePoolStat = std::make_unique<TStoragePoolStat>(GetServiceCounters(AppData()->Counters, "storage_pool_stat"));

    // initiate timer
    ScrubState.HandleTimer();

    // initialize not-ready histograms
    VSlotNotReadyHistogramUpdate();

    // request static group configuration
    Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvNodeWardenQueryStorageConfig(true), IEventHandle::FlagTrackDelivery);
}

void TBlobStorageController::Handle(TEvNodeWardenStorageConfig::TPtr ev) {
    const bool isFirstStorageConfig = !StorageConfig;
    Y_DEBUG_ABORT_UNLESS(!isFirstStorageConfig || CurrentStateFunc() == &TThis::StateInit);

    auto prevStorageConfig = std::exchange(StorageConfig, std::move(ev->Get()->Config));
    auto prevBridgeInfo = std::exchange(BridgeInfo, std::move(ev->Get()->BridgeInfo));
    SelfManagementEnabled = ev->Get()->SelfManagementEnabled;

    auto prevStaticPDisks = std::exchange(StaticPDisks, {});
    auto prevStaticVSlots = std::exchange(StaticVSlots, {});
    auto prevStaticGroups = std::exchange(StaticGroups, {});
    StaticVDiskMap.clear();

    const TMonotonic mono = TActivationContext::Monotonic();

    if (StorageConfig->HasBlobStorageConfig()) {
        const auto& bsConfig = StorageConfig->GetBlobStorageConfig();

        if (bsConfig.HasServiceSet()) {
            const auto& ss = bsConfig.GetServiceSet();
            for (const auto& pdisk : ss.GetPDisks()) {
                const TPDiskId pdiskId(pdisk.GetNodeID(), pdisk.GetPDiskID());
                StaticPDisks.try_emplace(pdiskId, pdisk, prevStaticPDisks);
                SysViewChangedPDisks.insert(pdiskId);
            }
            for (const auto& vslot : ss.GetVDisks()) {
                const auto& location = vslot.GetVDiskLocation();
                const TPDiskId pdiskId(location.GetNodeID(), location.GetPDiskID());
                const TVSlotId vslotId(pdiskId, location.GetVDiskSlotID());
                StaticVSlots.try_emplace(vslotId, vslot, prevStaticVSlots, mono);
                const TVDiskID& vdiskId = VDiskIDFromVDiskID(vslot.GetVDiskID());
                StaticVDiskMap.emplace(vdiskId, vslotId);
                StaticVDiskMap.emplace(TVDiskID(vdiskId.GroupID, 0, vdiskId), vslotId);
                ++StaticPDisks.at(pdiskId).StaticSlotUsage;
                SysViewChangedVSlots.insert(vslotId);
            }
            for (const auto& group : ss.GetGroups()) {
                const auto groupId = TGroupId::FromProto(&group, &NKikimrBlobStorage::TGroupInfo::GetGroupID);
                StaticGroups.try_emplace(groupId, group, prevStaticGroups);
                SysViewChangedGroups.insert(groupId);
            }
        } else {
            Y_FAIL("no storage configuration provided");
        }
    }

    // if bridge info has changed, then add any dynamic groups to sys view changed list
    THashSet<TGroupId> groupsToCheck;
    if (BridgeInfo) {
        bool changed = false;
        if (!prevBridgeInfo) {
            changed = true;
        } else {
            Y_ABORT_UNLESS(std::size(prevBridgeInfo->Piles) == std::size(BridgeInfo->Piles));
            for (size_t k = 0; k < std::size(BridgeInfo->Piles); ++k) {
                if (prevBridgeInfo->Piles[k].State != BridgeInfo->Piles[k].State) {
                    changed = true;
                    break;
                }
            }
        }
        if (changed) {
            for (const auto& [groupId, group] : GroupMap) {
                SysViewChangedGroups.insert(groupId);
            }
            for (const auto& [groupId, iter] : GroupToWaitingSelectGroupsItem) {
                groupsToCheck.insert(groupId);
            }
        }
    }

    if (SelfManagementEnabled) {
        // assuming that in autoconfig mode HostRecords are managed by the distconf; we need to apply it here to
        // avoid race with box autoconfiguration and node list change
        SetHostRecords(std::make_shared<THostRecordMap::element_type>(*StorageConfig));
    }

    // switch console interaction state based on new information
    if (Loaded) {
        if (SelfManagementEnabled) {
            ConsoleInteraction->Stop(); // distconf manages the Console
        } else {
            ConsoleInteraction->Start(); // we manage the Console now
        }
    }

    if (!isFirstStorageConfig) {
        // this isn't the first configuration change, we just update static groups information in self heal actor now
        PushStaticGroupsToSelfHeal();
    } else if (!SelfManagementEnabled) {
        // this is the first time we get StorageConfig in this instance of BSC; tablet is not yet initialized, and
        // self management is disabled, so we need to query nodes explicitly in the old way: through nameservice and
        // Console tablet, which is the source of truth for this case
        Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes(true));
    }

    if (Loaded) {
        ApplyStorageConfig();
    }

    ApplyStaticGroupUpdateForSyncers(prevStaticGroups);
    UpdateWaitingGroups(groupsToCheck);

    if (Loaded && BridgeInfo && (!prevStorageConfig || prevStorageConfig->GetClusterState().GetGeneration() <
            StorageConfig->GetClusterState().GetGeneration())) {
        // cluster state has changed -- we need to recheck groups
        RecheckUnsyncedBridgePiles = true;
        CheckUnsyncedBridgePiles();
    }
}

void TBlobStorageController::Handle(TEvents::TEvUndelivered::TPtr ev) {
    if (ev->Get()->SourceType == TEvNodeWardenQueryStorageConfig::EventType) {
        Y_DEBUG_ABORT_UNLESS(false);
    }
}

bool TBlobStorageController::HostConfigEquals(const THostConfigInfo& left, const NKikimrBlobStorage::TDefineHostConfig& right) const {
    if (left.Name != right.GetName()) {
        return false;
    }

    THashMap<TStringBuf, const THostConfigInfo::TDriveInfo*> driveMap;
    for (const auto& [key, info] : left.Drives) {
        driveMap.emplace(key.Path, &info);
    }

    TMaybe<TString> defaultPDiskConfig;
    if (right.HasDefaultHostPDiskConfig()) {
        const bool success = right.GetDefaultHostPDiskConfig().SerializeToString(&defaultPDiskConfig.ConstructInPlace());
        Y_ABORT_UNLESS(success);
    }

    auto checkDrive = [&](const auto& drive) {
        const auto it = driveMap.find(drive.GetPath());
        if (it == driveMap.end()) {
            return false;
        }

        if (drive.GetType() != it->second->Type ||
                drive.GetSharedWithOs() != it->second->SharedWithOs ||
                drive.GetReadCentric() != it->second->ReadCentric ||
                drive.GetKind() != it->second->Kind) {
            return false;
        }

        TMaybe<TString> pdiskConfig;
        if (drive.HasPDiskConfig()) {
            const bool success = drive.GetPDiskConfig().SerializeToString(&pdiskConfig.ConstructInPlace());
            Y_ABORT_UNLESS(success);
        } else {
            pdiskConfig = defaultPDiskConfig;
        }

        if (pdiskConfig != it->second->PDiskConfig) {
            return false;
        }

        driveMap.erase(it);
        return true;
    };

    for (const auto& drive : right.GetDrive()) {
        if (!checkDrive(drive)) {
            return false;
        }
    }

    auto addDrives = [&](const auto& field, NKikimrBlobStorage::EPDiskType type) {
        NKikimrBlobStorage::THostConfigDrive drive;
        drive.SetType(type);
        for (const auto& path : field) {
            if (drive.SetPath(path); !checkDrive(drive)) {
                return false;
            }
        }
        return true;
    };
    if (!addDrives(right.GetRot(), NKikimrBlobStorage::EPDiskType::ROT) ||
            !addDrives(right.GetSsd(), NKikimrBlobStorage::EPDiskType::SSD) ||
            !addDrives(right.GetNvme(), NKikimrBlobStorage::EPDiskType::NVME)) {
        return false;
    }

    return driveMap.empty();
}

void TBlobStorageController::ApplyBscSettings(const NKikimrConfig::TBlobStorageConfig& bsConfig) {
    if (!bsConfig.HasBscSettings()) {
        return;
    }

    auto ev = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
    auto& r = ev->Record;
    auto *request = r.MutableRequest();
    auto* command = request->AddCommand();

    command->MutableUpdateSettings()->CopyFrom(FromBscConfig(bsConfig.GetBscSettings()));

    STLOG(PRI_DEBUG, BS_CONTROLLER, BSC39, "ApplyBSCSettings", (Request, r));
    Send(SelfId(), ev.release());
}

std::unique_ptr<TEvBlobStorage::TEvControllerConfigRequest> TBlobStorageController::BuildConfigRequestFromStorageConfig(
        const NKikimrBlobStorage::TStorageConfig& storageConfig, const THostRecordMap& hostRecords, bool validationMode) {
    if (Boxes.size() > 1 || !storageConfig.HasBlobStorageConfig()) {
        return nullptr;
    }

    const auto& bsConfig = storageConfig.GetBlobStorageConfig();

    ui64 expectedBoxId = 1;
    std::optional<ui64> generation;
    bool needToDefineBox = true;
    if (!Boxes.empty()) {
        const auto& [boxId, box] = *Boxes.begin();

        expectedBoxId = boxId;
        generation = box.Generation.GetOrElse(1);
        needToDefineBox = false;

        // put all existing hosts of a singular box into the set
        THashSet<std::tuple<TString, ui32, ui64, TMaybe<ui32>>> hosts;
        for (const auto& [key, value] : box.Hosts) {
            hosts.emplace(key.Fqdn, key.IcPort, value.HostConfigId, value.EnforcedNodeId);
        }

        // drop matching entries from the new set
        for (const auto& host : bsConfig.GetDefineBox().GetHost()) {
            const auto& resolved = hostRecords->GetHostId(host.GetEnforcedNodeId());
            Y_ABORT_UNLESS(resolved);
            const auto& [fqdn, port] = *resolved;

            if (!hosts.erase(std::make_tuple(fqdn, port, host.GetHostConfigId(), Nothing()))) {
                needToDefineBox = true;
                break;
            }
        }

        if (!hosts.empty()) {
            needToDefineBox = true;
        }
    }

    auto ev = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
    auto& r = ev->Record;
    auto *request = r.MutableRequest();

    if (validationMode) {
        ev->EnforceHostRecords.emplace(hostRecords);
    }

    for (const auto& hostConfig : bsConfig.GetDefineHostConfig()) {
        const auto it = HostConfigs.find(hostConfig.GetHostConfigId());
        if (it != HostConfigs.end() && HostConfigEquals(it->second, hostConfig)) {
            continue;
        }

        auto *cmd = request->AddCommand();
        auto *defineHostConfig = cmd->MutableDefineHostConfig();
        defineHostConfig->CopyFrom(hostConfig);
        if (it != HostConfigs.end()) {
            defineHostConfig->SetItemConfigGeneration(it->second.Generation.GetOrElse(1));
        }
    }

    if (needToDefineBox) {
        auto *cmd = request->AddCommand();
        auto *defineBox = cmd->MutableDefineBox();
        defineBox->CopyFrom(bsConfig.GetDefineBox());
        defineBox->SetBoxId(expectedBoxId);
        for (auto& host : *defineBox->MutableHost()) {
            const ui32 nodeId = host.GetEnforcedNodeId();
            host.ClearEnforcedNodeId();
            auto *key = host.MutableKey();
            const auto& resolved = hostRecords->GetHostId(nodeId);
            Y_ABORT_UNLESS(resolved);
            const auto& [fqdn, port] = *resolved;
            key->SetFqdn(fqdn);
            key->SetIcPort(port);
        }
        if (generation) {
            defineBox->SetItemConfigGeneration(*generation);
        }
    }

    THashMap<THostConfigId, ui64> unusedHostConfigs;
    for (const auto& [hostConfigId, value] : HostConfigs) {
        unusedHostConfigs.emplace(hostConfigId, value.Generation.GetOrElse(1));
    }
    for (const auto& hostConfig : bsConfig.GetDefineHostConfig()) {
        unusedHostConfigs.erase(hostConfig.GetHostConfigId());
    }
    for (const auto& [hostConfigId, generation] : unusedHostConfigs) {
        auto *cmd = request->AddCommand();
        auto *del = cmd->MutableDeleteHostConfig();
        del->SetHostConfigId(hostConfigId);
        del->SetItemConfigGeneration(generation);
    }

    if (validationMode) {
        request->SetRollback(true);
    }

    request->MutableStorageConfig()->PackFrom(storageConfig);

    if (request->CommandSize()) {
        return ev;
    }

    return nullptr;
}

void TBlobStorageController::ApplyStorageConfig(bool ignoreDistconf) {
    if (!StorageConfig->HasBlobStorageConfig()) {
        return;
    }
    const auto& bsConfig = StorageConfig->GetBlobStorageConfig();

    ApplyBscSettings(bsConfig);

    if (!ignoreDistconf && (!SelfManagementEnabled || !StorageConfig->GetSelfManagementConfig().GetAutomaticBoxManagement())) {
        return; // not expected to be managed by BSC
    }

    if (auto ev = BuildConfigRequestFromStorageConfig(*StorageConfig, HostRecords, false)) {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC14, "ApplyStorageConfig", (Request, ev->Record));
        Send(SelfId(), ev.release());
    }
}

void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr ev) {
    if (const auto it = PendingValidationRequests.find(ev->Cookie); it != PendingValidationRequests.end()) {
        auto req = std::move(it->second);
        PendingValidationRequests.erase(it);

        const auto& resp = ev->Get()->Record.GetResponse();
        const bool rollbackSuccess = resp.GetRollbackSuccess();
        TString errorReason;
        if (!rollbackSuccess) {
            TStringStream s;
            s << resp.GetErrorDescription();
            for (const auto& group : resp.GetGroupsGetDegraded()) {
                s << " GroupGetDegraded# " << group;
            }
            for (const auto& group : resp.GetGroupsGetDisintegrated()) {
                s << " GroupGetDisintegrated# " << group;
            }
            for (const auto& group : resp.GetGroupsGetDisintegratedByExpectedStatus()) {
                s << " GroupGetDisintegratedByExpectedStatus# " << group;
            }
            errorReason = s.Str();
        }

        switch (req.Source) {
            case TConfigValidationInfo::ESource::Distconf: {
                auto response = std::make_unique<TEvBlobStorage::TEvControllerDistconfResponse>();
                auto& record = response->Record;

                if (rollbackSuccess) {
                    record.SetStatus(NKikimrBlobStorage::TEvControllerDistconfResponse::OK);
                } else {
                    record.SetStatus(NKikimrBlobStorage::TEvControllerDistconfResponse::Error);
                    record.SetErrorReason(errorReason);
                }

                auto h = std::make_unique<IEventHandle>(req.Sender, SelfId(), response.release(), 0, req.Cookie);
                if (req.InterconnectSession) {
                    h->Rewrite(TEvInterconnect::EvForward, req.InterconnectSession);
                }
                TActivationContext::Send(h.release());
                break;
            }

            case TConfigValidationInfo::ESource::ConsoleInteraction:
                if (!ConsoleInteraction) {
                    STLOG(PRI_ERROR, BS_CONTROLLER, BSC38, "Received console interaction validation response, but ConsoleInteraction is not set");
                    return;
                }
                ConsoleInteraction->ProcessDryRunResponse(rollbackSuccess, std::move(errorReason));
                break;
        }

        return;
    }

    auto& record = ev->Get()->Record;
    auto& response = record.GetResponse();
    STLOG(response.GetSuccess() ? PRI_DEBUG : PRI_ERROR, BS_CONTROLLER, BSC15, "TEvControllerConfigResponse",
        (Response, response));
}

void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerDistconfRequest::TPtr ev) {
    const auto& record = ev->Get()->Record;
    STLOG(PRI_DEBUG, BS_CONTROLLER, BSC52, "received TEvControllerDistconfRequest", (Operation, record.GetOperation()));

    // prepare the response
    auto response = std::make_unique<TEvBlobStorage::TEvControllerDistconfResponse>();
    auto& rr = response->Record;
    auto h = std::make_unique<IEventHandle>(ev->Sender, SelfId(), response.release(), 0, ev->Cookie);
    if (ev->InterconnectSession) {
        h->Rewrite(TEvInterconnect::EvForward, ev->InterconnectSession);
    }

    rr.SetStatus(NKikimrBlobStorage::TEvControllerDistconfResponse::OK);

    bool putConfigs = false;
    bool lock = false;

    std::optional<TString> mainYaml = record.HasCompressedMainConfig()
        ? std::make_optional(NYamlConfig::DecompressYamlString(record.GetCompressedMainConfig()))
        : std::nullopt;

    std::optional<TString> storageYaml = record.HasCompressedStorageConfig()
        ? std::make_optional(NYamlConfig::DecompressYamlString(record.GetCompressedStorageConfig()))
        : std::nullopt;

    switch (record.GetOperation()) {
        case NKikimrBlobStorage::TEvControllerDistconfRequest::EnableDistconf:
            if (ConsoleInteraction->RequestIsBeingProcessed()) {
                rr.SetStatus(NKikimrBlobStorage::TEvControllerDistconfResponse::Error);
                rr.SetErrorReason("a request is being processed right now");
            } else if (record.GetDedicatedConfigMode() != StorageYamlConfig.has_value()) {
                rr.SetStatus(NKikimrBlobStorage::TEvControllerDistconfResponse::Error);
                rr.SetErrorReason("can't switch dedicated storage config section along with enabling distconf");
            } else {
                putConfigs = lock = true;
            }
            break;

        case NKikimrBlobStorage::TEvControllerDistconfRequest::DisableDistconf: {
            // commit new configuration to the local database and then reply
            if (!mainYaml) {
                rr.SetStatus(NKikimrBlobStorage::TEvControllerDistconfResponse::Error);
                rr.SetErrorReason("missing main config yaml while disabling distconf");
                break;
            } else if (storageYaml.has_value() != record.GetDedicatedConfigMode()) {
                rr.SetStatus(NKikimrBlobStorage::TEvControllerDistconfResponse::Error);
                rr.SetErrorReason("storage yaml setting does not match desired dedicated storage config section mode");
                break;
            }

            // create full yaml config
            auto metadata = NYamlConfig::GetMainMetadata(*mainYaml);
            const ui64 mainYamlVersion = metadata.Version.value_or(0);
            auto updatedMetadata = metadata;
            updatedMetadata.Version.emplace(mainYamlVersion + 1);
            TYamlConfig yamlConfig(*mainYaml, mainYamlVersion, NYamlConfig::ReplaceMetadata(*mainYaml, updatedMetadata));

            // check if we have storage yaml expected version
            std::optional<ui64> expectedStorageYamlConfigVersion;
            if (record.HasExpectedStorageConfigVersion()) {
                expectedStorageYamlConfigVersion.emplace(record.GetExpectedStorageConfigVersion());
            }

            // commit it
            Execute(CreateTxCommitConfig(std::move(yamlConfig), std::make_optional(std::move(storageYaml)), std::nullopt,
                expectedStorageYamlConfigVersion, std::exchange(h, {}), std::nullopt,
                TAuditLogInfo{record.GetPeerName(), NACLib::TUserToken{record.GetUserToken()}}));
            break;
        }

        case NKikimrBlobStorage::TEvControllerDistconfRequest::ValidateConfig: {
            if (!mainYaml) {
                rr.SetStatus(NKikimrBlobStorage::TEvControllerDistconfResponse::Error);
                rr.SetErrorReason("missing main config yaml while validating distconf");
                break;
            }

            NKikimrBlobStorage::TStorageConfig storageConfig;
            if (record.HasStorageConfig()) {
                record.GetStorageConfig().UnpackTo(&storageConfig);
            } else {
                const TString& effectiveConfig = storageYaml ? *storageYaml : *mainYaml;
                try {
                    NKikimrConfig::TAppConfig appConfig = NYaml::Parse(effectiveConfig);
                    TString errorReason;
                    if (!NKikimr::NStorage::DeriveStorageConfig(appConfig, &storageConfig, &errorReason)) {
                        rr.SetStatus(NKikimrBlobStorage::TEvControllerDistconfResponse::Error);
                        rr.SetErrorReason("failed to derive storage config: " + errorReason);
                        break;
                    }
                } catch (const std::exception& ex) {
                    rr.SetStatus(NKikimrBlobStorage::TEvControllerDistconfResponse::Error);
                    rr.SetErrorReason(TStringBuilder() << "failed to parse YAML: " << ex.what());
                    break;
                }
            }

            const ui64 cookie = NextValidationCookie++;
            PendingValidationRequests.emplace(cookie, TConfigValidationInfo{
                .Sender = ev->Sender,
                .Cookie = ev->Cookie,
                .InterconnectSession = ev->InterconnectSession,
                .Source = TConfigValidationInfo::ESource::Distconf,
            });

            auto tempHostRecords = std::make_shared<THostRecordMap::element_type>(storageConfig);
            if (auto ev = BuildConfigRequestFromStorageConfig(storageConfig, tempHostRecords, true)) {
                Send(SelfId(), ev.release(), 0, cookie);
                return;
            } else {
                rr.SetStatus(NKikimrBlobStorage::TEvControllerDistconfResponse::OK);
                PendingValidationRequests.erase(cookie);
            }
            break;
        }
    }

    if (putConfigs) {
        if (YamlConfig) {
            rr.SetCompressedMainConfig(CompressSingleConfig(*YamlConfig));
            rr.SetExpectedMainConfigVersion(GetVersion(*YamlConfig) + 1);
        }
        if (StorageYamlConfig) {
            rr.SetCompressedStorageConfig(CompressStorageYamlConfig(*StorageYamlConfig));
        }
        rr.SetExpectedStorageConfigVersion(ExpectedStorageYamlConfigVersion);
    }

    if (lock) {
        ConfigLock.insert(ev->Recipient);
    }

    TActivationContext::Send(h.release());
}

void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerUpdateGroupStat::TPtr& ev) {
    TActivationContext::Send(ev->Forward(StatProcessorActorId));
}

void TBlobStorageController::Handle(TEvInterconnect::TEvNodesInfo::TPtr &ev) {
    STLOG(PRI_DEBUG, BS_CONTROLLER, BSC01, "Handle TEvInterconnect::TEvNodesInfo");
    SetHostRecords(std::make_shared<THostRecordMap::element_type>(ev->Get()));
}

void TBlobStorageController::SetHostRecords(THostRecordMap hostRecords) {
    if (std::exchange(HostRecords, std::move(hostRecords))) {
        // we already had some host records, so this isn't the first time this function called
        Send(SelfHealId, new TEvPrivate::TEvUpdateHostRecords(HostRecords));
        return;
    }

    // there were no host records, this is the first call, so we must initialize SelfHeal now and start booting tablet
    if (auto *appData = AppData()) {
        if (appData->Icb) {
            EnableSelfHealWithDegraded = std::make_shared<TControlWrapper>(0, 0, 1);
            TControlBoard::RegisterSharedControl(*EnableSelfHealWithDegraded,
                appData->Icb->BlobStorageControllerControls.EnableSelfHealWithDegraded);
        }
    }
    Y_ABORT_UNLESS(!SelfHealId);

    SelfHealSettings = ParseSelfHealSettings(StorageConfig);
    SelfHealId = Register(CreateSelfHealActor());

    ClusterBalancingSettings = ParseClusterBalancingSettings(StorageConfig);
    if (ClusterBalancingSettings.Enable) {
        ClusterBalanceActorId = Register(CreateClusterBalancingActor(SelfId(), ClusterBalancingSettings));
    }

    PushStaticGroupsToSelfHeal();
    Execute(CreateTxInitScheme());
}

void TBlobStorageController::IssueInitialGroupContent() {
    auto ev = MakeHolder<TEvControllerNotifyGroupChange>();
    for (const auto& kv : GroupMap) {
        ev->Created.push_back(kv.first);
    }
    Send(StatProcessorActorId, ev.Release());
}

void TBlobStorageController::ValidateInternalState() {
    // here we compare different structures to ensure that the memory state is sane
#ifndef NDEBUG
    for (const auto& [pdiskId, pdisk] : PDisks) {
        ui32 numActiveSlots = 0;
        for (const auto& [vslotId, vslot] : pdisk->VSlotsOnPDisk) {
            Y_ABORT_UNLESS(vslot == FindVSlot(TVSlotId(pdiskId, vslotId)));
            Y_ABORT_UNLESS(vslot->PDisk == pdisk.Get());
            if (!vslot->IsBeingDeleted()) {
                const TGroupInfo* group = FindGroup(vslot->GroupId);
                Y_ABORT_UNLESS(group);
                numActiveSlots += TPDiskConfig::GetOwnerWeight(group->GroupSizeInUnits, pdisk->SlotSizeInUnits);
            }
        }
        Y_ABORT_UNLESS(pdisk->NumActiveSlots == numActiveSlots);
    }
    for (const auto& [vslotId, vslot] : VSlots) {
        Y_ABORT_UNLESS(vslot->VSlotId == vslotId);
        Y_ABORT_UNLESS(vslot->PDisk == FindPDisk(vslot->VSlotId.ComprisingPDiskId()));
        const auto it = vslot->PDisk->VSlotsOnPDisk.find(vslotId.VSlotId);
        Y_ABORT_UNLESS(it != vslot->PDisk->VSlotsOnPDisk.end());
        Y_ABORT_UNLESS(it->second == vslot.Get());
        const TGroupInfo *group = FindGroup(vslot->GroupId);
        if (!vslot->IsBeingDeleted() && vslot->Mood != TMood::Donor) {
            Y_ABORT_UNLESS(group);
            Y_ABORT_UNLESS(vslot->Group == group);
        } else {
            Y_ABORT_UNLESS(!vslot->Group);
        }
        if (vslot->Mood == TMood::Donor) {
            const TVSlotInfo *acceptor = FindAcceptor(*vslot);
            Y_ABORT_UNLESS(!acceptor->IsBeingDeleted());
            Y_ABORT_UNLESS(acceptor->Mood != TMood::Donor);
            Y_ABORT_UNLESS(acceptor->Donors.contains(vslotId));
        }
        for (const TVSlotId& donorVSlotId : vslot->Donors) {
            const TVSlotInfo *donor = FindVSlot(donorVSlotId);
            Y_ABORT_UNLESS(donor);
            Y_ABORT_UNLESS(donor->Mood == TMood::Donor);
            Y_ABORT_UNLESS(donor->GroupId == vslot->GroupId);
            Y_ABORT_UNLESS(donor->GroupGeneration < vslot->GroupGeneration);
            Y_ABORT_UNLESS(donor->GetShortVDiskId() == vslot->GetShortVDiskId());
        }
        if (vslot->Group) {
            if (vslot->GetStatus() == NKikimrBlobStorage::EVDiskStatus::READY) {
                Y_DEBUG_ABORT_UNLESS(vslot->IsReady || vslot->IsInVSlotReadyTimestampQ());
            } else {
                Y_DEBUG_ABORT_UNLESS(!vslot->IsReady && !vslot->IsInVSlotReadyTimestampQ());
            }
        } else {
            Y_DEBUG_ABORT_UNLESS(!vslot->IsInVSlotReadyTimestampQ());
        }
    }
    for (const auto& [groupId, group] : GroupMap) {
        Y_ABORT_UNLESS(groupId == group->ID);
        Y_ABORT_UNLESS(FindGroup(groupId) == group.Get());
        for (const TVSlotInfo *vslot : group->VDisksInGroup) {
            Y_ABORT_UNLESS(FindVSlot(vslot->VSlotId) == vslot);
            Y_ABORT_UNLESS(vslot->Group == group.Get());
            Y_ABORT_UNLESS(vslot->GroupId == groupId);
            Y_ABORT_UNLESS(vslot->GroupGeneration == group->Generation);
        }
    }
    for (const auto& [key, value] : GroupLookup) {
        const auto it = GroupMap.find(key);
        Y_ABORT_UNLESS(it != GroupMap.end());
        Y_ABORT_UNLESS(value == it->second.Get());
    }
    Y_ABORT_UNLESS(GroupLookup.size() == GroupMap.size());
#endif
}

STFUNC(TBlobStorageController::StateWork) {
    const ui32 type = ev->GetTypeRewrite();
    THPTimer timer;

    switch (type) {
        fFunc(TEvBlobStorage::EvControllerRegisterNode, EnqueueIncomingEvent);
        fFunc(TEvBlobStorage::EvControllerGetGroup, EnqueueIncomingEvent);
        fFunc(TEvBlobStorage::EvControllerSelectGroups, EnqueueIncomingEvent);
        fFunc(TEvBlobStorage::EvControllerUpdateDiskStatus, EnqueueIncomingEvent);
        fFunc(TEvBlobStorage::EvControllerUpdateGroupStat, EnqueueIncomingEvent);
        fFunc(TEvBlobStorage::EvControllerGroupMetricsExchange, EnqueueIncomingEvent);
        fFunc(TEvBlobStorage::EvControllerUpdateNodeDrives, EnqueueIncomingEvent);
        fFunc(TEvControllerCommitGroupLatencies::EventType, EnqueueIncomingEvent);
        fFunc(TEvBlobStorage::EvRequestControllerInfo, EnqueueIncomingEvent);
        fFunc(TEvBlobStorage::EvControllerNodeReport, EnqueueIncomingEvent);
        fFunc(TEvBlobStorage::EvControllerConfigRequest, EnqueueIncomingEvent);
        fFunc(TEvBlobStorage::EvControllerProposeGroupKey, EnqueueIncomingEvent);
        fFunc(NSysView::TEvSysView::EvGetPDisksRequest, ForwardToSystemViewsCollector);
        fFunc(NSysView::TEvSysView::EvGetVSlotsRequest, ForwardToSystemViewsCollector);
        fFunc(NSysView::TEvSysView::EvGetGroupsRequest, ForwardToSystemViewsCollector);
        fFunc(NSysView::TEvSysView::EvGetStoragePoolsRequest, ForwardToSystemViewsCollector);
        fFunc(NSysView::TEvSysView::EvGetStorageStatsRequest, ForwardToSystemViewsCollector);
        fFunc(TEvPrivate::EvUpdateSystemViews, EnqueueIncomingEvent);
        hFunc(TEvInterconnect::TEvNodesInfo, Handle);
        hFunc(TEvTabletPipe::TEvServerConnected, Handle);
        hFunc(TEvTabletPipe::TEvServerDisconnected, Handle);
        fFunc(TEvPrivate::EvUpdateSelfHealCounters, EnqueueIncomingEvent);
        fFunc(TEvPrivate::EvDropDonor, EnqueueIncomingEvent);
        fFunc(TEvBlobStorage::EvControllerScrubQueryStartQuantum, EnqueueIncomingEvent);
        fFunc(TEvBlobStorage::EvControllerScrubQuantumFinished, EnqueueIncomingEvent);
        fFunc(TEvBlobStorage::EvControllerScrubReportQuantumInProgress, EnqueueIncomingEvent);
        fFunc(TEvBlobStorage::EvControllerGroupDecommittedNotify, EnqueueIncomingEvent);
        fFunc(TEvPrivate::EvScrub, EnqueueIncomingEvent);
        fFunc(TEvPrivate::EvVSlotReadyUpdate, EnqueueIncomingEvent);
        cFunc(TEvPrivate::EvVSlotNotReadyHistogramUpdate, VSlotNotReadyHistogramUpdate);
        cFunc(TEvPrivate::EvProcessIncomingEvent, ProcessIncomingEvent);
        hFunc(TEvNodeWardenStorageConfig, Handle);
        hFunc(TEvBlobStorage::TEvControllerConfigResponse, Handle);
        hFunc(TEvBlobStorage::TEvControllerProposeConfigResponse, ConsoleInteraction->Handle);
        hFunc(TEvBlobStorage::TEvControllerConsoleCommitResponse, ConsoleInteraction->Handle);
        fFunc(TConsoleInteraction::TEvPrivate::EvValidationTimeout, ConsoleInteraction->HandleValidationTimeout);
        hFunc(TEvBlobStorage::TEvControllerReplaceConfigRequest, ConsoleInteraction->Handle);
        hFunc(TEvBlobStorage::TEvControllerFetchConfigRequest, ConsoleInteraction->Handle);
        hFunc(TEvBlobStorage::TEvControllerValidateConfigResponse, ConsoleInteraction->Handle);
        hFunc(TEvTabletPipe::TEvClientConnected, ConsoleInteraction->Handle);
        hFunc(TEvTabletPipe::TEvClientDestroyed, ConsoleInteraction->Handle);
        hFunc(TEvBlobStorage::TEvGetBlockResult, ConsoleInteraction->Handle);
        hFunc(TEvBlobStorage::TEvControllerDistconfRequest, Handle);
        fFunc(TEvBlobStorage::EvControllerShredRequest, EnqueueIncomingEvent);
        cFunc(TEvPrivate::EvUpdateShredState, ShredState.HandleUpdateShredState);
        cFunc(TEvPrivate::EvCommitMetrics, CommitMetrics);
        hFunc(NStorage::TEvNodeConfigInvokeOnRootResult, Handle);
        cFunc(TEvPrivate::EvCheckSyncerDisconnectedNodes, CheckSyncerDisconnectedNodes);
        hFunc(TEvBlobStorage::TEvControllerUpdateSyncerState, Handle);
        hFunc(TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup, Handle);
        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                STLOG(PRI_ERROR, BS_CONTROLLER, BSC06, "StateWork unexpected event", (Type, type),
                    (Event, ev->ToString()));
            }
        break;
    }

    // start any pending bridge syncers
    ProcessSyncers();

    if (const TDuration time = TDuration::Seconds(timer.Passed()); time >= TDuration::MilliSeconds(100)) {
        STLOG(PRI_ERROR, BS_CONTROLLER, BSC00, "StateWork event processing took too much time", (Type, type),
            (Duration, time));
    }
}

void TBlobStorageController::PassAway() {
    if (ResponsivenessPinger) {
        ResponsivenessPinger->Detach(TActivationContext::ActorContextFor(ResponsivenessActorID));
        ResponsivenessPinger = nullptr;
    }
    for (TActorId *ptr : {&SelfHealId, &StatProcessorActorId, &SystemViewsCollectorId, &ClusterBalanceActorId}) {
        if (const TActorId actorId = std::exchange(*ptr, {})) {
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, SelfId(), nullptr, 0));
        }
    }
    for (const auto& [id, info] : GroupMap) {
        if (const auto& actorId = info->VirtualGroupSetupMachineId) {
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, SelfId(), nullptr, 0));
        }
    }
    for (const auto& [groupId, info] : BlobDepotDeleteQueue) {
        if (const auto& actorId = info.VirtualGroupSetupMachineId) {
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, SelfId(), nullptr, 0));
        }
    }
    TActivationContext::Send(new IEventHandle(TEvents::TSystem::Unsubscribe, 0, GetNameserviceActorId(), SelfId(),
        nullptr, 0));
    TActivationContext::Send(new IEventHandle(TEvents::TSystem::Unsubscribe, 0, MakeBlobStorageNodeWardenID(SelfId().NodeId()),
        SelfId(), nullptr, 0));
    if (ConsoleInteraction) {
        ConsoleInteraction->Stop();
    }
    TActor::PassAway();
}

TBlobStorageController::TBlobStorageController(const TActorId &tablet, TTabletStorageInfo *info)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
        , ResponsivenessPinger(nullptr)
        , ScrubState(this)
    {
        using namespace NBlobStorageController;
        TabletCountersPtr.Reset(new TProtobufTabletCounters<
            ESimpleCounters_descriptor,
            ECumulativeCounters_descriptor,
            EPercentileCounters_descriptor,
            ETxTypes_descriptor
        >());
        TabletCounters = TabletCountersPtr.Get();
    }

TBlobStorageController::~TBlobStorageController() = default;

ui32 TBlobStorageController::GetEventPriority(IEventHandle *ev) {
    switch (ev->GetTypeRewrite()) {
        // essential NodeWarden messages (also includes SelfHeal-generated commands and UpdateDiskStatus when status gets worse than last reported)
        case TEvBlobStorage::EvControllerRegisterNode:                 return 1;
        case TEvBlobStorage::EvControllerNodeReport:                   return 1;
        case TEvBlobStorage::EvControllerProposeGroupKey:              return 1;
        case TEvBlobStorage::EvControllerGetGroup:                     return 1;
        case TEvBlobStorage::EvControllerGroupDecommittedNotify:       return 1;

        // auxiliary messages that are not usually urgent (also includes RW transactions in TConfigRequest and UpdateDiskStatus)
        case TEvPrivate::EvDropDonor:                                  return 2;
        case TEvBlobStorage::EvControllerScrubQueryStartQuantum:       return 2;
        case TEvBlobStorage::EvControllerScrubQuantumFinished:         return 2;
        case TEvBlobStorage::EvControllerScrubReportQuantumInProgress: return 2;
        case TEvBlobStorage::EvControllerUpdateNodeDrives:             return 2;
        case TEvBlobStorage::EvControllerShredRequest:                 return 2;

        // hive-related commands
        case TEvBlobStorage::EvControllerSelectGroups:                 return 4;
        case TEvBlobStorage::EvControllerGroupMetricsExchange:         return 4;

        // timers and different observation (also includes RO transactions in TConfigRequest)
        case TEvPrivate::EvScrub:                                      return 5;
        case TEvPrivate::EvVSlotReadyUpdate:                           return 5;

        // external observation and non-latency-bound activities
        case TEvPrivate::EvUpdateSystemViews:                          return 10;
        case TEvBlobStorage::EvControllerUpdateGroupStat:              return 10;
        case TEvControllerCommitGroupLatencies::EventType:             return 10;
        case TEvBlobStorage::EvRequestControllerInfo:                  return 10;
        case TEvPrivate::EvUpdateSelfHealCounters:                     return 10;

        case TEvBlobStorage::EvControllerUpdateDiskStatus: {
            auto *msg = ev->Get<TEvBlobStorage::TEvControllerUpdateDiskStatus>();
            const auto& record = msg->Record;
            for (const auto& item : record.GetVDiskStatus()) {
                const TVSlotId vslotId(item.GetNodeId(), item.GetPDiskId(), item.GetVSlotId());
                if (TVSlotInfo *slot = FindVSlot(vslotId); slot && slot->GetStatus() > item.GetStatus()) {
                    return 1;
                } else if (const auto it = StaticVSlots.find(vslotId); it != StaticVSlots.end() && it->second.VDiskStatus > item.GetStatus()) {
                    return 1;
                }
            }
            return 2;
        }

        case TEvBlobStorage::EvControllerConfigRequest: {
            auto *msg = ev->Get<TEvBlobStorage::TEvControllerConfigRequest>();
            if (msg->SelfHeal) {
                return 2; // locally-generated self-heal commands
            }
            const auto& record = msg->Record;
            const auto& request = record.GetRequest();
            if (request.GetCito()) {
                return 0; // user-generated commands explicitly marked as OOB
            }
            for (const auto& cmd : request.GetCommand()) {
                switch (cmd.GetCommandCase()) {
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kDefineHostConfig:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kDeleteHostConfig:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kDefineBox:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kDeleteBox:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kDefineStoragePool:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kDeleteStoragePool:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kUpdateDriveStatus:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kProposeStoragePools:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kMergeBoxes:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kMoveGroups:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kChangeGroupSizeInUnits:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kAddMigrationPlan:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kDeleteMigrationPlan:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kEnableSelfHeal:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kDeclareIntent:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kEnableDonorMode:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kDropDonorDisk:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kSetScrubPeriodicity:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kAddDriveSerial:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kRemoveDriveSerial:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kForgetDriveSerial:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kMigrateToSerial:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kSetPDiskSpaceMarginPromille:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kUpdateSettings:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kReassignGroupDisk:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kAllocateVirtualGroup:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kDecommitGroups:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kWipeVDisk:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kSanitizeGroup:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kCancelVirtualGroup:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kSetVDiskReadOnly:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kRestartPDisk:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kSetPDiskReadOnly:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kStopPDisk:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kGetInterfaceVersion:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kMovePDisk:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kUpdateBridgeGroupInfo:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kReconfigureVirtualGroup:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kRecommissionGroups:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kDefineDDiskPool:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kReadDDiskPool:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kDeleteDDiskPool:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kMoveDDisk:
                        return 2; // read-write commands go with higher priority as they are needed to keep cluster intact

                    case NKikimrBlobStorage::TConfigRequest::TCommand::kReadHostConfig:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kReadBox:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kReadStoragePool:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kReadDriveStatus:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kReadSettings:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kQueryBaseConfig:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kReadIntent:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::COMMAND_NOT_SET:
                        // handle explicitly these commands to prevent forgetting priority handling when adding a new one
                        break;
                }
            }
            return 5; // no read-write commands were in the query, so lower the priority for observation requests
        }
    }

    Y_ABORT();
}

void TBlobStorageController::TStaticGroupInfo::UpdateStatus(TMonotonic mono, TBlobStorageController *controller) {
    if (!Info || Info->IsBridged()) {
        return;
    }

    const auto *topology = &Info->GetTopology();

    TBlobStorageGroupInfo::TGroupVDisks failed(topology);
    TBlobStorageGroupInfo::TGroupVDisks failedByPDisk(topology);

    for (const auto& vdisk : Info->GetVDisks()) {
        const TVDiskIdShort& vdiskId = vdisk.VDiskIdShort;
        const auto& [nodeId, pdiskId, vdiskSlotId] = DecomposeVDiskServiceId(
            Info->GetDynamicInfo().ServiceIdForOrderNumber[vdisk.OrderNumber]);
        const TVSlotId vslotId(nodeId, pdiskId, vdiskSlotId);

        if (const auto it = controller->StaticVSlots.find(vslotId); it != controller->StaticVSlots.end()) {
            if (mono <= it->second.ReadySince) { // VDisk can't be treated as READY one
                failed |= {topology, vdiskId};
            } else if (const TPDiskInfo *pdisk = controller->FindPDisk(vslotId.ComprisingPDiskId()); pdisk && !pdisk->HasGoodExpectedStatus()) {
                failedByPDisk |= {topology, vdiskId};
            }
        } else {
            failed |= {topology, vdiskId};
        }
    }

    Status = {
        .OperatingStatus = DeriveStatus(topology, failed),
        .ExpectedStatus = DeriveStatus(topology, failed | failedByPDisk),
    };
}

void TBlobStorageController::TStaticGroupInfo::UpdateLayoutCorrect(TBlobStorageController *controller) {
    LayoutCorrect = true;
    if (!Info || Info->IsBridged() || !controller->SelfManagementEnabled) {
        return;
    }

    NLayoutChecker::TGroupLayout layout(Info->GetTopology());
    NLayoutChecker::TDomainMapper mapper;
    TGroupGeometryInfo geom(Info->Type, controller->SelfManagementEnabled
        ? controller->StorageConfig->GetSelfManagementConfig().GetGeometry()
        : NKikimrBlobStorage::TGroupGeometry());

    for (size_t i = 0; i < Info->GetTotalVDisksNum(); ++i) {
        const auto& [nodeId, pdiskId, vdiskSlotId] = DecomposeVDiskServiceId(Info->GetDynamicInfo().ServiceIdForOrderNumber[i]);
        layout.AddDisk({mapper, controller->HostRecords->GetLocation(nodeId), {nodeId, pdiskId}, geom}, i);
    }

    LayoutCorrect = layout.IsCorrect();
}

namespace {

    template<typename T, typename TFinder>
    TBlobStorageController::TGroupInfo::TGroupStatus GetGroupStatus(const T& entity, TFinder&& finder,
            const TBridgeInfo *bridgeInfo) {
        const NKikimrBlobStorage::TGroupInfo *groupInfo = nullptr;
        if constexpr (std::is_same_v<T, TBlobStorageController::TGroupInfo>) {
            groupInfo = entity.BridgeGroupInfo
                ? &entity.BridgeGroupInfo.value()
                : nullptr;
        } else {
            Y_DEBUG_ABORT_UNLESS(entity.Info);
            Y_DEBUG_ABORT_UNLESS(entity.Info->Group);
            groupInfo = entity.Info && entity.Info->Group && entity.Info->Group->HasBridgeGroupState()
                ? &entity.Info->Group.value()
                : nullptr;
        }

        if (groupInfo) {
            Y_ABORT_UNLESS(bridgeInfo);
            Y_ABORT_UNLESS(groupInfo->HasBridgeGroupState());

            std::optional<TBlobStorageController::TGroupInfo::TGroupStatus> res;
            const auto& bridgeGroupState = groupInfo->GetBridgeGroupState();
            const auto& piles = bridgeGroupState.GetPile();

            for (int pileIndex = 0; pileIndex < piles.size(); ++pileIndex) {
                const TBridgePileId bridgePileId = TBridgePileId::FromPileIndex(pileIndex);
                if (NBridge::PileStateTraits(bridgeInfo->GetPile(bridgePileId)->State).RequiresDataQuorum) {
                    const auto& pile = piles[pileIndex];
                    const auto groupId = TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId);
                    if (const auto *group = finder(groupId)) {
                        const auto& s = group->GetStatus(finder, bridgeInfo);
                        if (res) {
                            res->MakeWorst(s.OperatingStatus, s.ExpectedStatus);
                        } else {
                            res.emplace(s);
                        }
                    }
                }
            }

            return res.value_or(TBlobStorageController::TGroupInfo::TGroupStatus());
        } else {
            return entity.Status;
        }
    }

}

TBlobStorageController::TGroupInfo::TGroupStatus TBlobStorageController::TGroupInfo::GetStatus(
        const TGroupFinder& finder, const TBridgeInfo *bridgeInfo) const {
    return GetGroupStatus(*this, finder, bridgeInfo);
}

TBlobStorageController::TGroupInfo::TGroupStatus TBlobStorageController::TStaticGroupInfo::GetStatus(
        const TStaticGroupFinder& finder, const TBridgeInfo *bridgeInfo) const {
    return GetGroupStatus(*this, finder, bridgeInfo);
}

bool TBlobStorageController::TStaticGroupInfo::IsLayoutCorrect(const TStaticGroupFinder& finder) const {
    if (Info && Info->IsBridged()) {
        for (TGroupId groupId : Info->GetBridgeGroupIds()) {
            if (TStaticGroupInfo *g = finder(groupId)) {
                if (!g->IsLayoutCorrect(finder)) {
                    return false;
                }
            } else {
                Y_DEBUG_ABORT();
            }
        }
        return true;
    } else {
        return LayoutCorrect;
    }
}

void TBlobStorageController::InvokeOnRoot(NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot&& request,
        std::function<void(NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult&)>&& callback) {
    const ui64 cookie = NextInvokeOnRootCookie++;
    STLOG(PRI_DEBUG, BS_CONTROLLER, BSC42, "InvokeOnRoot", (Request, request), (Cookie, cookie));
    const auto [it, inserted] = InvokeOnRootCommands.emplace(cookie, TInvokeOnRootCommand{
        .Request = std::move(request),
        .Callback = std::move(callback),
    });
    auto ev = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
    ev->Record.CopyFrom(it->second.Request);
    Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), ev.release(), 0, cookie);
}

void TBlobStorageController::Handle(NStorage::TEvNodeConfigInvokeOnRootResult::TPtr ev) {
    const auto it = InvokeOnRootCommands.find(ev->Cookie);
    Y_ABORT_UNLESS(it != InvokeOnRootCommands.end());
    TInvokeOnRootCommand& cmd = it->second;
    auto& record = ev->Get()->Record;
    const auto status = record.GetStatus();
    const bool success = status == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK;
    const bool retriable =
        status == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::RACE ||
        status == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::NO_QUORUM;
    STLOG(retriable ? PRI_INFO : success ? PRI_DEBUG : PRI_WARN, BS_CONTROLLER, BSC41, "TEvNodeConfigInvokeOnRootResult",
        (Cookie, ev->Cookie), (Response, ev->Get()->Record), (Success, success), (Retriable, retriable));
    if (retriable) {
        auto ev = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        ev->Record.CopyFrom(cmd.Request);
        TActivationContext::Schedule(cmd.Timer.Next(), new IEventHandle(
            MakeBlobStorageNodeWardenID(SelfId().NodeId()), SelfId(), ev.release(), 0, it->first));
    } else {
        cmd.Callback(record);
        InvokeOnRootCommands.erase(it);
    }
}

} // NBsController

} // NKikimr

template<>
void Out<NKikimr::NBsController::TPDiskId>(IOutputStream &str, const NKikimr::NBsController::TPDiskId &value) {
    str << value.ToString();
}

template<>
void Out<NKikimr::NBsController::TVSlotId>(IOutputStream &str, const NKikimr::NBsController::TVSlotId &value) {
    str << value.ToString();
}

template<>
void Out<NKikimr::NBsController::TResourceNormalizedValues>(IOutputStream &str, const NKikimr::NBsController::TResourceNormalizedValues &value) {
    str << value.ToString();
}

template<>
void Out<NKikimr::NBsController::TResourceRawValues>(IOutputStream &str, const NKikimr::NBsController::TResourceRawValues &value) {
    str << value.ToString();
}

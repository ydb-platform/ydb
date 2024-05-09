#include "impl.h"
#include "config.h"
#include "self_heal.h"
#include "sys_view.h"

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
        TVSlotReadyTimestampQ *vslotReadyTimestampQ, TInstant lastSeenReady, TDuration replicationTime)
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
        ++pdisk->NumActiveSlots;
    }
}

void TBlobStorageController::TGroupInfo::CalculateGroupStatus() {
    Status = {NKikimrBlobStorage::TGroupStatus::FULL, NKikimrBlobStorage::TGroupStatus::FULL};

    if ((VirtualGroupState == NKikimrBlobStorage::EVirtualGroupState::CREATE_FAILED ||
            VirtualGroupState == NKikimrBlobStorage::EVirtualGroupState::NEW) && VDisksInGroup.empty()) {
        Status.MakeWorst(NKikimrBlobStorage::TGroupStatus::DISINTEGRATED, NKikimrBlobStorage::TGroupStatus::DISINTEGRATED);
    }

    if (VDisksInGroup) {
        TBlobStorageGroupInfo::TGroupVDisks failed(Topology.get());
        TBlobStorageGroupInfo::TGroupVDisks failedByPDisk(Topology.get());
        for (const TVSlotInfo *slot : VDisksInGroup) {
            if (!slot->IsReady || slot->PDisk->Mood == TPDiskMood::Restarting) {
                failed |= {Topology.get(), slot->GetShortVDiskId()};
            } else if (!slot->PDisk->HasGoodExpectedStatus()) {
                failedByPDisk |= {Topology.get(), slot->GetShortVDiskId()};
            }
        }
        Status.MakeWorst(DeriveStatus(Topology.get(), failed), DeriveStatus(Topology.get(), failed | failedByPDisk));
    }
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
    // create stat processor
    StatProcessorActorId = Register(CreateStatProcessorActor());

    // create system views collector
    SystemViewsCollectorId = Register(CreateSystemViewsCollector());

    Executor()->RegisterExternalTabletCounters(TabletCountersPtr);
    if (!ResponsivenessPinger) {
        ResponsivenessPinger = new TTabletResponsivenessPinger(TabletCounters->Simple()[NBlobStorageController::COUNTER_RESPONSE_TIME_USEC], TDuration::Seconds(1));
        ResponsivenessActorID = RegisterWithSameMailbox(ResponsivenessPinger);
    }

    // request node list
    Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes(true));

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
    ev->Get()->Config->Swap(&StorageConfig);

    auto prevStaticPDisks = std::exchange(StaticPDisks, {});
    auto prevStaticVSlots = std::exchange(StaticVSlots, {});
    StaticVDiskMap.clear();

    if (StorageConfig.HasBlobStorageConfig()) {
        if (const auto& bsConfig = StorageConfig.GetBlobStorageConfig(); bsConfig.HasServiceSet()) {
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
                StaticVSlots.try_emplace(vslotId, vslot, prevStaticVSlots);
                const TVDiskID& vdiskId = VDiskIDFromVDiskID(vslot.GetVDiskID());
                StaticVDiskMap.emplace(vdiskId, vslotId);
                StaticVDiskMap.emplace(TVDiskID(vdiskId.GroupID, 0, vdiskId), vslotId);
                ++StaticPDisks.at(pdiskId).StaticSlotUsage;
                SysViewChangedVSlots.insert(vslotId);
                SysViewChangedGroups.insert(vdiskId.GroupID);
            }
        } else {
            Y_FAIL("no storage configuration provided");
        }
    }

    if (!std::exchange(StorageConfigObtained, true) && HostRecords) {
        Execute(CreateTxInitScheme());
    }

    if (Loaded) {
        ApplyStorageConfig();
    }

    PushStaticGroupsToSelfHeal();
}

void TBlobStorageController::Handle(TEvents::TEvUndelivered::TPtr ev) {
    if (ev->Get()->SourceType == TEvNodeWardenQueryStorageConfig::EventType) {
        Y_DEBUG_ABORT_UNLESS(false);
    }
}

void TBlobStorageController::ApplyStorageConfig() {
    if (!StorageConfig.HasBlobStorageConfig()) {
        return;
    }
    const auto& bsConfig = StorageConfig.GetBlobStorageConfig();

    if (!bsConfig.HasAutoconfigSettings()) {
        return;
    }
    const auto& autoconfigSettings = bsConfig.GetAutoconfigSettings();

    if (!autoconfigSettings.GetAutomaticBoxManagement()) {
        return;
    }

    if (Boxes.size() > 1) {
        return;
    }
    std::optional<ui64> generation;
    if (!Boxes.empty()) {
        const auto& [boxId, box] = *Boxes.begin();

        THashSet<THostConfigId> unusedHostConfigs;
        for (const auto& [hostConfigId, _] : HostConfigs) {
            unusedHostConfigs.insert(hostConfigId);
        }
        for (const auto& [_, host] : box.Hosts) {
            if (!HostConfigs.contains(host.HostConfigId)) {
                return;
            }
            unusedHostConfigs.erase(host.HostConfigId);
        }

        if (!unusedHostConfigs.empty()) {
            return;
        }

        generation = box.Generation.GetOrElse(0);
    }

    auto ev = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
    auto& r = ev->Record;
    auto *request = r.MutableRequest();
    for (const auto& hostConfig : autoconfigSettings.GetDefineHostConfig()) {
        auto *cmd = request->AddCommand();
        cmd->MutableDefineHostConfig()->CopyFrom(hostConfig);
    }
    auto *cmd = request->AddCommand();
    auto *defineBox = cmd->MutableDefineBox();
    defineBox->CopyFrom(autoconfigSettings.GetDefineBox());
    defineBox->SetBoxId(1);
    for (auto& host : *defineBox->MutableHost()) {
        const ui32 nodeId = host.GetEnforcedNodeId();
        host.ClearEnforcedNodeId();
        auto *key = host.MutableKey();
        const auto& resolved = HostRecords->GetHostId(nodeId);
        Y_ABORT_UNLESS(resolved);
        const auto& [fqdn, port] = *resolved;
        key->SetFqdn(fqdn);
        key->SetIcPort(port);
    }
    if (generation) {
        defineBox->SetItemConfigGeneration(*generation);
    }

    THashSet<THostConfigId> unusedHostConfigs;
    for (const auto& [hostConfigId, _] : HostConfigs) {
        unusedHostConfigs.insert(hostConfigId);
    }
    for (const auto& host : defineBox->GetHost()) {
        unusedHostConfigs.erase(host.GetHostConfigId());
    }
    for (const THostConfigId hostConfigId : unusedHostConfigs) {
        auto *cmd = request->AddCommand();
        auto *del = cmd->MutableDeleteHostConfig();
        del->SetHostConfigId(hostConfigId);
        del->SetItemConfigGeneration(HostConfigs[hostConfigId].Generation.GetOrElse(0));
    }

    STLOG(PRI_DEBUG, BS_CONTROLLER, BSC14, "ApplyStorageConfig", (Request, r));

    Send(SelfId(), ev.release());
}

void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr ev) {
    STLOG(PRI_DEBUG, BS_CONTROLLER, BSC15, "TEvControllerConfigResponse", (Response, ev->Get()->Record));
}

void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerUpdateGroupStat::TPtr& ev) {
    TActivationContext::Send(ev->Forward(StatProcessorActorId));
}

void TBlobStorageController::Handle(TEvInterconnect::TEvNodesInfo::TPtr &ev) {
    STLOG(PRI_DEBUG, BS_CONTROLLER, BSC01, "Handle TEvInterconnect::TEvNodesInfo");
    const bool initial = !HostRecords;
    HostRecords = std::make_shared<THostRecordMap::element_type>(ev->Get());
    if (initial) {
        SelfHealId = Register(CreateSelfHealActor());
        PushStaticGroupsToSelfHeal();
        if (StorageConfigObtained) {
            Execute(CreateTxInitScheme());
        }
    }
    Send(SelfHealId, new TEvPrivate::TEvUpdateHostRecords(HostRecords));
}

void TBlobStorageController::IssueInitialGroupContent() {
    auto ev = MakeHolder<TEvControllerNotifyGroupChange>();
    for (const auto& kv : GroupMap) {
        ev->Created.push_back(kv.first);
    }
    Send(StatProcessorActorId, ev.Release());
}

void TBlobStorageController::NotifyNodesAwaitingKeysForGroups(ui32 groupId) {
    if (const auto it = NodesAwaitingKeysForGroup.find(groupId); it != NodesAwaitingKeysForGroup.end()) {
        TSet<ui32> nodes = std::move(it->second);
        NodesAwaitingKeysForGroup.erase(it);
        for (const TNodeId nodeId : nodes) {
            Send(SelfId(), new TEvBlobStorage::TEvControllerGetGroup(nodeId, groupId));
        }
    }
}

void TBlobStorageController::ValidateInternalState() {
    // here we compare different structures to ensure that the memory state is sane
#ifndef NDEBUG
    for (const auto& [pdiskId, pdisk] : PDisks) {
        ui32 numActiveSlots = 0;
        for (const auto& [vslotId, vslot] : pdisk->VSlotsOnPDisk) {
            Y_ABORT_UNLESS(vslot == FindVSlot(TVSlotId(pdiskId, vslotId)));
            Y_ABORT_UNLESS(vslot->PDisk == pdisk.Get());
            numActiveSlots += !vslot->IsBeingDeleted();
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
            if (vslot->Status == NKikimrBlobStorage::EVDiskStatus::READY) {
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
                if (TVSlotInfo *slot = FindVSlot(vslotId); slot && slot->Status > item.GetStatus()) {
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

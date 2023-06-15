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
    Y_VERIFY(pdisk);
    const auto& [it, inserted] = pdisk->VSlotsOnPDisk.emplace(VSlotId.VSlotId, this);
    Y_VERIFY(inserted);

    if (!IsBeingDeleted()) {
        if (Mood != TMood::Donor) {
            Y_VERIFY(group);
            Group = group;
            group->AddVSlot(this);
        }
        ++pdisk->NumActiveSlots;
    }
}

void TBlobStorageController::TGroupInfo::CalculateGroupStatus() {
    Status = {NKikimrBlobStorage::TGroupStatus::FULL, NKikimrBlobStorage::TGroupStatus::FULL};

    if (VirtualGroupState == NKikimrBlobStorage::EVirtualGroupState::CREATE_FAILED ||
            (VirtualGroupState == NKikimrBlobStorage::EVirtualGroupState::NEW && VDisksInGroup.empty())) {
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
        auto deriveStatus = [&](const auto& failed) {
            auto& checker = *Topology->QuorumChecker;
            if (!failed.GetNumSetItems()) { // all disks of group are operational
                return NKikimrBlobStorage::TGroupStatus::FULL;
            } else if (!checker.CheckFailModelForGroup(failed)) { // fail model exceeded
                return NKikimrBlobStorage::TGroupStatus::DISINTEGRATED;
            } else if (checker.IsDegraded(failed)) { // group degraded
                return NKikimrBlobStorage::TGroupStatus::DEGRADED;
            } else if (failed.GetNumSetItems()) { // group partially available, but not degraded
                return NKikimrBlobStorage::TGroupStatus::PARTIAL;
            } else {
                Y_FAIL("unexpected case");
            }
        };
        Status.MakeWorst(deriveStatus(failed), deriveStatus(failed | failedByPDisk));
    }
}

void TBlobStorageController::OnActivateExecutor(const TActorContext&) {
    // fill in static disks
    if (const auto& ss = AppData()->StaticBlobStorageConfig) {
        for (const auto& pdisk : ss->GetPDisks()) {
            const TPDiskId pdiskId(pdisk.GetNodeID(), pdisk.GetPDiskID());
            StaticPDisks.emplace(pdiskId, pdisk);
            SysViewChangedPDisks.insert(pdiskId);
        }
        for (const auto& vslot : ss->GetVDisks()) {
            const auto& location = vslot.GetVDiskLocation();
            const TPDiskId pdiskId(location.GetNodeID(), location.GetPDiskID());
            const TVSlotId vslotId(pdiskId, location.GetVDiskSlotID());
            StaticVSlots.emplace(vslotId, vslot);
            const TVDiskID& vdiskId = VDiskIDFromVDiskID(vslot.GetVDiskID());
            StaticVDiskMap.emplace(vdiskId, vslotId);
            StaticVDiskMap.emplace(TVDiskID(vdiskId.GroupID, 0, vdiskId), vslotId);
            ++StaticPDisks.at(pdiskId).StaticSlotUsage;
            SysViewChangedVSlots.insert(vslotId);
            SysViewChangedGroups.insert(vdiskId.GroupID);
        }
    }

    // create self-heal actor
    SelfHealId = Register(CreateSelfHealActor());

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
    Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes);

    // create storage pool stats monitor
    StoragePoolStat = std::make_unique<TStoragePoolStat>(GetServiceCounters(AppData()->Counters, "storage_pool_stat"));

    // initiate timer
    ScrubState.HandleTimer();

    // initialize not-ready histograms
    VSlotNotReadyHistogramUpdate();
}

void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerUpdateGroupStat::TPtr& ev) {
    TActivationContext::Send(ev->Forward(StatProcessorActorId));
}

void TBlobStorageController::Handle(TEvInterconnect::TEvNodesInfo::TPtr &ev) {
    STLOG(PRI_DEBUG, BS_CONTROLLER, BSC01, "Handle TEvInterconnect::TEvNodesInfo");
    const bool initial = !HostRecords;
    HostRecords = std::make_shared<THostRecordMap::element_type>(ev->Get());
    Schedule(TDuration::Minutes(5), new TEvPrivate::TEvHostRecordsTimeToLiveExceeded);
    Send(SelfHealId, new TEvPrivate::TEvUpdateHostRecords(HostRecords));
    if (initial) {
        Execute(CreateTxInitScheme());
    }
}

void TBlobStorageController::HandleHostRecordsTimeToLiveExceeded() {
    Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes);
}

void TBlobStorageController::IssueInitialGroupContent() {
    auto ev = MakeHolder<TEvControllerNotifyGroupChange>();
    for (const auto& kv : GroupMap) {
        ev->Created.push_back(kv.first);
    }
    Send(StatProcessorActorId, ev.Release());
}

void TBlobStorageController::Handle(TEvents::TEvPoisonPill::TPtr&) {
    Become(&TThis::StateBroken);
    for (TActorId *ptr : {&SelfHealId, &StatProcessorActorId, &SystemViewsCollectorId}) {
        if (const TActorId actorId = std::exchange(*ptr, {})) {
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, SelfId(), nullptr, 0));
        }
    }
    for (const auto& [id, info] : GroupMap) {
        if (auto& actorId = info->VirtualGroupSetupMachineId) {
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, SelfId(), nullptr, 0));
            actorId = {};
        }
    }
    TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, Tablet(), SelfId(), nullptr, 0));
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
            Y_VERIFY(vslot == FindVSlot(TVSlotId(pdiskId, vslotId)));
            Y_VERIFY(vslot->PDisk == pdisk.Get());
            numActiveSlots += !vslot->IsBeingDeleted();
        }
        Y_VERIFY(pdisk->NumActiveSlots == numActiveSlots);
    }
    for (const auto& [vslotId, vslot] : VSlots) {
        Y_VERIFY(vslot->VSlotId == vslotId);
        Y_VERIFY(vslot->PDisk == FindPDisk(vslot->VSlotId.ComprisingPDiskId()));
        const auto it = vslot->PDisk->VSlotsOnPDisk.find(vslotId.VSlotId);
        Y_VERIFY(it != vslot->PDisk->VSlotsOnPDisk.end());
        Y_VERIFY(it->second == vslot.Get());
        const TGroupInfo *group = FindGroup(vslot->GroupId);
        if (!vslot->IsBeingDeleted() && vslot->Mood != TMood::Donor) {
            Y_VERIFY(group);
            Y_VERIFY(vslot->Group == group);
        } else {
            Y_VERIFY(!vslot->Group);
        }
        if (vslot->Mood == TMood::Donor) {
            const TVSlotInfo *acceptor = FindAcceptor(*vslot);
            Y_VERIFY(!acceptor->IsBeingDeleted());
            Y_VERIFY(acceptor->Mood != TMood::Donor);
            Y_VERIFY(acceptor->Donors.contains(vslotId));
        }
        for (const TVSlotId& donorVSlotId : vslot->Donors) {
            const TVSlotInfo *donor = FindVSlot(donorVSlotId);
            Y_VERIFY(donor);
            Y_VERIFY(donor->Mood == TMood::Donor);
            Y_VERIFY(donor->GroupId == vslot->GroupId);
            Y_VERIFY(donor->GroupGeneration < vslot->GroupGeneration + group->ContentChanged);
            Y_VERIFY(donor->GetShortVDiskId() == vslot->GetShortVDiskId());
        }
        if (vslot->Group) {
            if (vslot->Status == NKikimrBlobStorage::EVDiskStatus::READY) {
                Y_VERIFY_DEBUG(vslot->IsReady || vslot->IsInVSlotReadyTimestampQ());
            } else {
                Y_VERIFY_DEBUG(!vslot->IsReady && !vslot->IsInVSlotReadyTimestampQ());
            }
        } else {
            Y_VERIFY_DEBUG(!vslot->IsInVSlotReadyTimestampQ());
        }
    }
    for (const auto& [groupId, group] : GroupMap) {
        Y_VERIFY(groupId == group->ID);
        Y_VERIFY(FindGroup(groupId) == group.Get());
        for (const TVSlotInfo *vslot : group->VDisksInGroup) {
            Y_VERIFY(FindVSlot(vslot->VSlotId) == vslot);
            Y_VERIFY(vslot->Group == group.Get());
            Y_VERIFY(vslot->GroupId == groupId);
            Y_VERIFY(vslot->GroupGeneration == group->Generation);
        }
    }
    for (const auto& [key, value] : GroupLookup) {
        const auto it = GroupMap.find(key);
        Y_VERIFY(it != GroupMap.end());
        Y_VERIFY(value == it->second.Get());
    }
    Y_VERIFY(GroupLookup.size() == GroupMap.size());
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
        case TEvBlobStorage::EvControllerGroupReconfigureWipe:         return 2;
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

    Y_FAIL();
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

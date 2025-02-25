#include "impl.h"
#include "config.h"
#include "self_heal.h"
#include "sys_view.h"
#include "console_interaction.h"
#include "group_geometry_info.h"
#include "group_layout_checker.h"

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
            if (!slot->IsReady) {
                failed |= {Topology.get(), slot->GetShortVDiskId()};
            } else if (!slot->PDisk->HasGoodExpectedStatus()) {
                failedByPDisk |= {Topology.get(), slot->GetShortVDiskId()};
            }
        }
        Status.MakeWorst(DeriveStatus(Topology.get(), failed), DeriveStatus(Topology.get(), failed | failedByPDisk));
    }
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
    ev->Get()->Config->Swap(&StorageConfig);
    SelfManagementEnabled = ev->Get()->SelfManagementEnabled;

    auto prevStaticPDisks = std::exchange(StaticPDisks, {});
    auto prevStaticVSlots = std::exchange(StaticVSlots, {});
    StaticVDiskMap.clear();

    const TMonotonic mono = TActivationContext::Monotonic();

    if (StorageConfig.HasBlobStorageConfig()) {
        const auto& bsConfig = StorageConfig.GetBlobStorageConfig();

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
                SysViewChangedGroups.insert(vdiskId.GroupID);
            }
        } else {
            Y_FAIL("no storage configuration provided");
        }
    }

    if (SelfManagementEnabled) {
        // assuming that in autoconfig mode HostRecords are managed by the distconf; we need to apply it here to
        // avoid race with box autoconfiguration and node list change
        HostRecords = std::make_shared<THostRecordMap::element_type>(StorageConfig);
        if (SelfHealId) {
            Send(SelfHealId, new TEvPrivate::TEvUpdateHostRecords(HostRecords));
        }

        ConsoleInteraction->Stop(); // distconf will handle the Console from now on
    } else if (Loaded) {
        ConsoleInteraction->Start(); // we control the Console now
    }

    if (!std::exchange(StorageConfigObtained, true)) { // this is the first time we get StorageConfig in this instance of BSC
        if (SelfManagementEnabled) {
            OnHostRecordsInitiate();
        } else {
            Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes(true));
        }
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

void TBlobStorageController::ApplyStorageConfig(bool ignoreDistconf) {
    if (!StorageConfig.HasBlobStorageConfig()) {
        return;
    }
    const auto& bsConfig = StorageConfig.GetBlobStorageConfig();

    if (Boxes.size() > 1) {
        return;
    }

    if (!ignoreDistconf && (!SelfManagementEnabled || !StorageConfig.GetSelfManagementConfig().GetAutomaticBoxManagement())) {
        return; // not expected to be managed by BSC
    }

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
            const auto& resolved = HostRecords->GetHostId(host.GetEnforcedNodeId());
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
            const auto& resolved = HostRecords->GetHostId(nodeId);
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

    if (request->CommandSize()) {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC14, "ApplyStorageConfig", (Request, r));
        Send(SelfId(), ev.release());
    }
}

void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr ev) {
    auto& record = ev->Get()->Record;
    auto& response = record.GetResponse();
    STLOG(response.GetSuccess() ? PRI_DEBUG : PRI_ERROR, BS_CONTROLLER, BSC15, "TEvControllerConfigResponse",
        (Response, response));
}

void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerDistconfRequest::TPtr ev) {
    const auto& record = ev->Get()->Record;

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
                expectedStorageYamlConfigVersion, std::exchange(h, {})));
            break;
        }

        case NKikimrBlobStorage::TEvControllerDistconfRequest::ValidateConfig:
            break;
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
    if (!std::exchange(HostRecords, std::make_shared<THostRecordMap::element_type>(ev->Get()))) {
        OnHostRecordsInitiate();
    }
    Send(SelfHealId, new TEvPrivate::TEvUpdateHostRecords(HostRecords));
}

void TBlobStorageController::OnHostRecordsInitiate() {
    if (auto *appData = AppData()) {
        if (appData->Icb) {
            EnableSelfHealWithDegraded = std::make_shared<TControlWrapper>(0, 0, 1);
            appData->Icb->RegisterSharedControl(*EnableSelfHealWithDegraded,
                "BlobStorageControllerControls.EnableSelfHealWithDegraded");
        }
    }
    Y_ABORT_UNLESS(!SelfHealId);
    SelfHealId = Register(CreateSelfHealActor());
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
        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                STLOG(PRI_ERROR, BS_CONTROLLER, BSC06, "StateWork unexpected event", (Type, type),
                    (Event, ev->ToString()));
            }
        break;
    }

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
    for (TActorId *ptr : {&SelfHealId, &StatProcessorActorId, &SystemViewsCollectorId}) {
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

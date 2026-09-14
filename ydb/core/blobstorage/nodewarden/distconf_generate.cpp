#include "distconf.h"
#include "distconf_statestorage_config_generator.h"

#include <ydb/core/mind/bscontroller/group_geometry_info.h>
#include <ydb/library/yaml_config/yaml_config_helpers.h>
#include <ydb/library/yaml_json/yaml_to_json.h>
#include <library/cpp/streams/zstd/zstd.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_NODE

namespace NKikimr::NStorage {

    std::optional<TString> TDistributedConfigKeeper::GenerateFirstConfig(NKikimrBlobStorage::TStorageConfig *config,
            const TString& selfAssemblyUUID) {
        if (!config->GetSelfManagementConfig().GetEnabled()) {
            return "self-management is not enabled";
        }
        const auto& smConfig = config->GetSelfManagementConfig();

        const bool noStaticGroup = !config->HasBlobStorageConfig() || // either no BlobStorageConfig section at all
            !config->GetBlobStorageConfig().HasServiceSet() || // or no ServiceSet in there
            !config->GetBlobStorageConfig().GetServiceSet().GroupsSize(); // or no groups in ServiceSet
        if (noStaticGroup) {
            TStringStream prefix;

            try {
                if (!smConfig.HasErasureSpecies()) {
                    return "missing ErasureSpecies in SelfManagementConfig";
                }

                TBlobStorageGroupType::EErasureSpecies species;
                if (!TBlobStorageGroupType::ParseErasureName(species, smConfig.GetErasureSpecies())) {
                    throw TExConfigError() << "invalid erasure specified for static group"
                        << " Erasure# " << smConfig.GetErasureSpecies();
                }

                TGroupId groupId = TGroupId::Zero();

                auto allocateGroup = [&](TBridgePileId bridgePileId, std::optional<TGroupId> bridgeProxyGroupId) {
                    AllocateStaticGroup({
                        .Config = config,
                        .GroupId = groupId,
                        .GroupGeneration = 1,
                        .GroupType = TBlobStorageGroupType(species),
                        .IgnoreVSlotQuotaCheck = true,
                        .BridgePileId = bridgePileId,
                        .BridgeProxyGroupId = bridgeProxyGroupId,
                    });

                    const auto& groups = config->GetBlobStorageConfig().GetServiceSet().GetGroups();
                    const auto& allocatedGroup = groups.at(groups.size() - 1);
                    YDB_LOG_DEBUG("Allocated static group",
                        {"marker", "NWDC33"},
                        {"group", allocatedGroup});
                };

                if (const auto& bridge = Cfg->BridgeConfig) {
                    auto *group = config->MutableBlobStorageConfig()->MutableServiceSet()->AddGroups();
                    const TGroupId bridgeProxyGroupId = groupId;
                    groupId.CopyToProto(group, &NKikimrBlobStorage::TGroupInfo::SetGroupID);
                    ++groupId;
                    group->SetGroupGeneration(1);

                    const auto& piles = bridge->GetPiles();
                    for (int i = 0; i < piles.size(); ++i) {
                        prefix << "pile# " << i << ' ';
                        allocateGroup(TBridgePileId::FromPileIndex(i), bridgeProxyGroupId);
                        auto *pile = group->MutableBridgeGroupState()->AddPile();
                        groupId.CopyToProto(pile, &NKikimrBridge::TGroupState::TPile::SetGroupId);
                        pile->SetGroupGeneration(1);
                        pile->SetStage(NKikimrBridge::TGroupState::SYNCED);
                        ++groupId;
                    }
                } else {
                    allocateGroup(TBridgePileId(), std::nullopt);
                }
            } catch (const TExConfigError& ex) {
                return TStringBuilder() << "failed to allocate static group: " << ex.what() << ' ' << prefix.Str();
            }
        }

        // initial config YAML is taken from the Cfg->SelfManagementConfig as it is cleared in TStorageConfig while
        // deriving it from NodeWarden configuration
        if (!Cfg->StartupConfigYaml) {
            return "missing initial config YAML";
        }

        ui64 version = 0;
        try {
            version = NYamlConfig::GetMainMetadata(Cfg->StartupConfigYaml).Version.value_or(0);
        } catch (const std::exception& ex) {
            return TStringBuilder() << "failed to parse initial main YAML: " << ex.what();
        }
        if (version) {
            return TStringBuilder() << "initial main config version must be zero";
        }

        if (const auto& error = UpdateConfigComposite(*config, Cfg->StartupConfigYaml, std::nullopt)) {
            return TStringBuilder() << "failed to update config yaml: " << *error;
        }

        if (Cfg->StartupStorageYaml) {
            ui64 storageVersion = 0;
            try {
                storageVersion = NYamlConfig::GetStorageMetadata(*Cfg->StartupStorageYaml).Version.value_or(0);
            } catch (const std::exception& ex) {
                return TStringBuilder() << "failed to parse initial storage YAML: " << ex.what();
            }
            if (storageVersion) {
                return TStringBuilder() << "initial storage config version must be zero";
            }

            TString s;
            if (TStringOutput output(s); true) {
                TZstdCompress zstd(&output);
                zstd << *Cfg->StartupStorageYaml;
            }
            config->SetCompressedStorageYaml(s);
            config->SetExpectedStorageYamlVersion(storageVersion + 1);
        }

        if (Cfg->DomainsConfig && Cfg->DomainsConfig->StateStorageSize() == 1) { // the StateStorage config is already defined explicitly, just migrate it
            const auto& ss = Cfg->DomainsConfig->GetStateStorage(0);
            config->MutableStateStorageConfig()->CopyFrom(ss);
            config->MutableStateStorageBoardConfig()->CopyFrom(ss);
            config->MutableSchemeBoardConfig()->CopyFrom(ss);
        }

        std::unordered_set<ui32> usedNodes;
#define UPDATE_EXPLICIT_CONFIG(NAME) \
        if (Cfg->DomainsConfig && Cfg->DomainsConfig->HasExplicit##NAME##Config()) { \
            config->Mutable##NAME##Config()->CopyFrom(Cfg->DomainsConfig->GetExplicit##NAME##Config()); \
        } \
        if (!config->Has##NAME##Config()) { \
            std::unordered_set<ui32> nodesToUse; \
            if (Cfg->SelfManagementConfig && Cfg->SelfManagementConfig->NAME##SelfHealAllowedNodesSize()) { \
                const auto& allowedNodes = Cfg->SelfManagementConfig->Get##NAME##SelfHealAllowedNodes(); \
                nodesToUse.insert(allowedNodes.begin(), allowedNodes.end()); \
            } \
            GenerateStateStorageConfig(config->Mutable##NAME##Config(), *config, usedNodes, nodesToUse); \
        }

        UPDATE_EXPLICIT_CONFIG(StateStorage)
        UPDATE_EXPLICIT_CONFIG(StateStorageBoard)
        UPDATE_EXPLICIT_CONFIG(SchemeBoard)

        config->SetSelfAssemblyUUID(selfAssemblyUUID);

        return std::nullopt;
    }

    namespace {

        void AddBaseConfigToPlacementSnapshot(NBsController::TGroupMapper::TPlacementSnapshot& snapshot,
                                              const NKikimrBlobStorage::TBaseConfig& config) {
            for (const auto& group : config.GetGroup()) {
                snapshot.Groups.push_back({
                    .GroupId = group.GetGroupId(),
                    .GroupGeneration = group.GetGroupGeneration(),
                    .GroupSizeInUnits = group.GetGroupSizeInUnits(),
                });
            }
            for (const auto& vslot : config.GetVSlot()) {
                const auto& id = vslot.GetVSlotId();
                auto& state = snapshot.VSlots.emplace_back();
                state.VSlotId = NBsController::TVSlotId(id);
                state.PDiskId = state.VSlotId.ComprisingPDiskId();
                state.GroupId = vslot.GetGroupId();
                state.GroupGeneration = vslot.GetGroupGeneration();
                state.VDiskId = TVDiskIdShort(vslot.GetFailRealmIdx(), vslot.GetFailDomainIdx(), vslot.GetVDiskIdx());
                state.Ready = vslot.GetStatus() == "READY";
                state.Replicating = vslot.GetStatus() == "REPLICATING";
                if (vslot.HasVDiskMetrics() && vslot.GetVDiskMetrics().HasAllocatedSize()) {
                    state.AllocatedSize = static_cast<i64>(vslot.GetVDiskMetrics().GetAllocatedSize());
                    state.SpaceUsed = state.AllocatedSize;
                }
            }
        }

        void AddServiceSetToPlacementSnapshot(NBsController::TGroupMapper::TPlacementSnapshot& snapshot,
                                              const NKikimrBlobStorage::TNodeWardenServiceSet& serviceSet) {
            THashMap<NBsController::TVSlotId, size_t> vslotIndices;
            for (size_t i = 0; i < snapshot.VSlots.size(); ++i) {
                vslotIndices.try_emplace(snapshot.VSlots[i].VSlotId, i);
            }

            for (const auto& vdisk : serviceSet.GetVDisks()) {
                const TVDiskID vdiskId = VDiskIDFromVDiskID(vdisk.GetVDiskID());
                const TVDiskIdShort shortVDiskId(vdiskId);
                const auto& location = vdisk.GetVDiskLocation();
                const NBsController::TPDiskId pdiskId(location.GetNodeID(), location.GetPDiskID());
                const NBsController::TVSlotId vslotId(pdiskId, location.GetVDiskSlotID());
                const bool occupiedByGroup = vdisk.GetEntityStatus() != NKikimrBlobStorage::EEntityStatus::DESTROY;

                const auto [it, inserted] = vslotIndices.try_emplace(vslotId, snapshot.VSlots.size());
                if (!inserted) {
                    auto& state = snapshot.VSlots[it->second];
                    state.OccupiedByGroup = occupiedByGroup;
                    if (!occupiedByGroup) {
                        state.Replicating = false;
                    }
                    if (state.GroupId != vdiskId.GroupID.GetRawId()
                        || state.GroupGeneration != vdiskId.GroupGeneration
                        || state.VDiskId != shortVDiskId) {
                        state.GroupId = vdiskId.GroupID.GetRawId();
                        state.GroupGeneration = vdiskId.GroupGeneration;
                        state.VDiskId = shortVDiskId;
                        state.Ready = true;
                        state.Replicating = false;
                        state.AllocatedSize.reset();
                        state.SpaceUsed.reset();
                    }
                    continue;
                }

                snapshot.VSlots.push_back({
                    .VSlotId = vslotId,
                    .PDiskId = pdiskId,
                    .GroupId = vdiskId.GroupID.GetRawId(),
                    .GroupGeneration = vdiskId.GroupGeneration,
                    .VDiskId = shortVDiskId,
                    .OccupiedByGroup = occupiedByGroup,
                    .Ready = true,
                });
            }
        }

    } // anonymous namespace

    void TDistributedConfigKeeper::AllocateStaticGroup(TAllocateStaticGroupParams params) {
        using TPDiskId = NBsController::TPDiskId;

        auto& config = *params.Config;
        const auto& replacedDisks = params.ReplacedDisks;
        const auto& selfManagementConfig = config.GetSelfManagementConfig();
        const auto& selfHealAllowedNodes = selfManagementConfig.GetStaticGroupSelfHealAllowedNodes();
        NKikimrConfig::TBlobStorageConfig *bsConfig = config.MutableBlobStorageConfig();
        if (params.Reassignments) {
            params.Reassignments->clear();
        }

        // build node location map
        THashMap<ui32, TNodeLocation> nodeLocations;
        for (const auto& node : config.GetAllNodes()) {
            TNodeLocation location(node.GetLocation());
            nodeLocations.try_emplace(node.GetNodeId(), location);
        }

        // when restricting self-heal targets, only these node ids may host relocated vdisks
        const bool restrictSelfHealNodes = params.ApplySelfHealNodeAllowList && !selfHealAllowedNodes.empty();
        const THashSet<ui32> selfHealAllowedNodeSet = restrictSelfHealNodes
                                                      ? THashSet<ui32>(selfHealAllowedNodes.begin(), selfHealAllowedNodes.end())
                                                      : THashSet<ui32>{};

        struct TPDiskInfo {
            NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk Record;
            NBsController::TGroupMapper::TPDiskState State;
        };

        struct TPDiskMapperSettings {
            ui32 SlotCount = 0;
            ui32 SlotSizeInUnits = 0;
            ui64 SlotSizeInBytes = 0;
            ui32 MaxSlots = 0;
        };

        THashMap<TPDiskId, TPDiskInfo> pdisks;
        THashMap<TPDiskId, TPDiskMapperSettings> pdiskMapperSettings;
        THashMap<ui32, ui32> maxPDiskId;
        THashMap<TPDiskId, ui32> maxVSlotId;
        NBsController::TGroupMapper::TPlacementSnapshot placementSnapshot;

        auto applyPDiskConfig = [](TPDiskMapperSettings& settings, const NKikimrBlobStorage::TPDiskConfig& pdiskConfig) {
            if (pdiskConfig.HasExpectedSlotCount()) {
                settings.SlotCount = pdiskConfig.GetExpectedSlotCount();
            }
            if (pdiskConfig.HasSlotSizeInUnits()) {
                settings.SlotSizeInUnits = pdiskConfig.GetSlotSizeInUnits();
            }
            if (pdiskConfig.HasExpectedSlotSize()) {
                settings.SlotSizeInBytes = pdiskConfig.GetExpectedSlotSize();
            }
            if (pdiskConfig.HasMaxSlots()) {
                settings.MaxSlots = pdiskConfig.GetMaxSlots();
            }
        };

        auto checkMatch = [&](NKikimrBlobStorage::EPDiskType type, bool sharedWithOs, bool readCentric, ui64 kind) {
            if (selfManagementConfig.HasPDiskType() && type == selfManagementConfig.GetPDiskType()) {
                return true;
            }
            for (const auto& pdiskFilter : selfManagementConfig.GetPDiskFilter()) {
                bool m = true;
                for (const auto& p : pdiskFilter.GetProperty()) {
                    bool pMatch = false;
                    switch (p.GetPropertyCase()) {
                        case NKikimrBlobStorage::TPDiskFilter::TRequiredProperty::kType:
                            pMatch = p.GetType() == type;
                            break;
                        case NKikimrBlobStorage::TPDiskFilter::TRequiredProperty::kSharedWithOs:
                            pMatch = p.GetSharedWithOs() == sharedWithOs;
                            break;
                        case NKikimrBlobStorage::TPDiskFilter::TRequiredProperty::kReadCentric:
                            pMatch = p.GetReadCentric() == readCentric;
                            break;
                        case NKikimrBlobStorage::TPDiskFilter::TRequiredProperty::kKind:
                            pMatch = p.GetKind() == kind;
                            break;
                        case NKikimrBlobStorage::TPDiskFilter::TRequiredProperty::PROPERTY_NOT_SET:
                            throw TExConfigError() << "invalid TPDiskFilter record";
                    }
                    if (!pMatch) {
                        m = false;
                        break;
                    }
                }
                if (m) {
                    return true;
                }
            }
            return false;
        };

        ui32 defaultMaxSlots = 16;
        std::optional<NKikimrBlobStorage::TPDiskSpaceColor::E> pdiskSpaceColorBorder;
        ui32 pdiskSpaceMarginPromille = 150;

        if (params.BaseConfig) {
            const auto& baseConfig = *params.BaseConfig;
            THashSet<ui32> connectedNodes;
            for (const auto& node : baseConfig.GetNode()) {
                if (node.GetConnected()) {
                    connectedNodes.insert(node.GetNodeId());
                }
            }
            if (baseConfig.HasSettings()) {
                const auto& settings = baseConfig.GetSettings();
                if (settings.DefaultMaxSlotsSize()) {
                    defaultMaxSlots = settings.GetDefaultMaxSlots(0);
                }
                if (settings.PDiskSpaceColorBorderSize()) {
                    pdiskSpaceColorBorder.emplace(settings.GetPDiskSpaceColorBorder(0));
                }
                if (settings.PDiskSpaceMarginPromilleSize()) {
                    pdiskSpaceMarginPromille = settings.GetPDiskSpaceMarginPromille(0);
                }
            }

            for (const auto& pdisk : baseConfig.GetPDisk()) {
                const TPDiskId pdiskId(pdisk.GetNodeId(), pdisk.GetPDiskId());
                auto& maxId = maxPDiskId[pdiskId.NodeId];
                maxId = Max(maxId, pdiskId.PDiskId);

                if (!checkMatch(pdisk.GetType(), pdisk.GetSharedWithOs(), pdisk.GetReadCentric(), pdisk.GetKind())) {
                    continue;
                }

                if (const auto [it, inserted] = pdisks.try_emplace(pdiskId); inserted) {
                    TPDiskInfo& pdiskInfo = it->second;
                    auto& pdiskState = pdiskInfo.State;
                    pdiskState.PDiskId = pdiskId;
                    auto& r = pdiskInfo.Record;
                    r.SetNodeID(pdiskId.NodeId);
                    r.SetPDiskID(pdiskId.PDiskId);
                    r.SetPath(pdisk.GetPath());
                    r.SetPDiskGuid(pdisk.GetGuid());
                    r.SetPDiskCategory(TPDiskCategory(static_cast<NPDisk::EDeviceType>(pdisk.GetType()),
                        pdisk.GetKind()).GetRaw());
                    if (pdisk.HasPDiskConfig()) {
                        r.MutablePDiskConfig()->CopyFrom(pdisk.GetPDiskConfig());
                        applyPDiskConfig(pdiskMapperSettings[pdiskId], pdisk.GetPDiskConfig());
                    }
                    if (pdisk.GetExpectedSlotCount()) {
                        pdiskMapperSettings[pdiskId].SlotCount = pdisk.GetExpectedSlotCount();
                    }
                    if (pdisk.GetExpectedSlotSize()) {
                        pdiskMapperSettings[pdiskId].SlotSizeInBytes = pdisk.GetExpectedSlotSize();
                    }
                    const auto *metrics = pdisk.HasPDiskMetrics() ? &pdisk.GetPDiskMetrics() : nullptr;
                    pdiskState.Operational = NBsController::TGroupMapper::IsPDiskOperational(connectedNodes.contains(pdiskId.NodeId),
                                                                                             metrics);
                    pdiskState.DriveStatus = pdisk.GetDriveStatus();
                    pdiskState.MaintenanceStatus = pdisk.GetMaintenanceStatus();
                    pdiskState.DecommitStatus = pdisk.GetDecommitStatus();
                    if (pdisk.HasDiskScope()) {
                        pdiskState.DiskScope = pdisk.GetDiskScope();
                        r.SetDiskScope(pdisk.GetDiskScope());
                    }

                    if (metrics) {
                        const auto& m = *metrics;
                        pdiskState.Space = NBsController::TGroupMapper::CapturePDiskSpace(m);
                        auto& settings = pdiskMapperSettings[pdiskId];
                        if (settings.SlotSizeInBytes && m.HasSlotCount()) {
                            settings.SlotCount = m.GetSlotCount();
                        }
                        if (m.HasSlotSizeInUnits()) {
                            settings.SlotSizeInUnits = m.GetSlotSizeInUnits();
                        }
                    }
                } else {
                    throw TExConfigError() << "duplicate PDisk record in TBaseConfig";
                }
            }

            AddBaseConfigToPlacementSnapshot(placementSnapshot, baseConfig);
        }

        // then all existing drives from the current storage config; also extract group definition, if it exists
        TVector<NBsController::TGroupMapper::TVDiskPlacement> groupDisks;
        bool existingGroup = false;
        ui32 groupGeneration = 0;
        THashSet<TPDiskId> addedPDisks;
        THashMap<TVDiskIdShort, NKikimrBlobStorage::TVDiskLocation> vdiskLocations;

        if (bsConfig->HasServiceSet()) {
            const auto& ss = bsConfig->GetServiceSet();

            THashSet<TPDiskId> requiredPDiskIds;
            THashSet<TPDiskId> replacedPDiskIds;
            std::optional<ui32> generation;

            for (const auto& group : ss.GetGroups()) {
                if (TGroupId::FromProto(&group, &NKikimrBlobStorage::TGroupInfo::GetGroupID) == params.GroupId) {
                    existingGroup = true;
                    groupGeneration = group.GetGroupGeneration();
                    generation.emplace(group.GetGroupGeneration());

                    ui32 failRealmIdx = 0;
                    Y_DEBUG_ABORT_UNLESS(groupDisks.empty());

                    for (const auto& r : group.GetRings()) {
                        ui32 failDomainIdx = 0;

                        for (const auto& fd : r.GetFailDomains()) {
                            ui32 vdiskIdx = 0;

                            for (const auto& v : fd.GetVDiskLocations()) {
                                const TVDiskIdShort vdiskId(failRealmIdx, failDomainIdx, vdiskIdx);

                                TPDiskId pdiskId(v.GetNodeID(), v.GetPDiskID());
                                requiredPDiskIds.insert(pdiskId);
                                auto& disk = groupDisks.emplace_back();
                                disk.VDiskId = vdiskId;
                                disk.PDiskId = pdiskId;

                                if (const auto it = replacedDisks.find(vdiskId); it != replacedDisks.end()) {
                                    replacedPDiskIds.insert(pdiskId);
                                    if (it->second == TPDiskId()) {
                                        disk.Reassignment = NBsController::TGroupMapper::TReplaceVDisk{
                                            .RequireSameNode = params.UseSelfHealLocalPolicy,
                                        };
                                    } else {
                                        disk.Reassignment = NBsController::TGroupMapper::TReplaceVDiskOnPDisk{it->second};
                                    }
                                }

                                ++vdiskIdx;
                            }
                            ++failDomainIdx;
                        }
                        ++failRealmIdx;
                    }
                }
            }

            size_t numReplacedDisks = 0;
            for (const auto& disk : groupDisks) {
                numReplacedDisks += !std::holds_alternative<NBsController::TGroupMapper::TKeepVDisk>(disk.Reassignment);
            }
            if (numReplacedDisks != replacedDisks.size()) {
                throw TExConfigError() << "VDisk being replaced not found in group";
            }

            for (const auto& vdisk : ss.GetVDisks()) {
                const TVDiskID vdiskId = VDiskIDFromVDiskID(vdisk.GetVDiskID());
                if (vdiskId.GroupID == params.GroupId) {
                    if (!generation) {
                        throw TExConfigError() << "missing record for group being reconfigured";
                    } else if (vdiskId.GroupGeneration == *generation && !replacedDisks.contains(vdiskId)) {
                        Y_ABORT_UNLESS(vdisk.GetEntityStatus() != NKikimrBlobStorage::EEntityStatus::DESTROY);
                        Y_ABORT_UNLESS(!vdisk.HasDonorMode());
                        vdiskLocations.emplace(vdiskId, vdisk.GetVDiskLocation());
                    }
                }
            }

            for (const auto& pdisk : ss.GetPDisks()) {
                const TPDiskId pdiskId(pdisk.GetNodeID(), pdisk.GetPDiskID());
                if ((params.AllowUnusableDisks && requiredPDiskIds.contains(pdiskId)) || replacedPDiskIds.contains(pdiskId)) {
                    if (const auto [it, inserted] = pdisks.try_emplace(pdiskId); inserted) {
                        TPDiskInfo& pdiskInfo = it->second;
                        pdiskInfo.State.PDiskId = pdiskId;
                        pdiskInfo.Record.CopyFrom(pdisk);
                        pdiskInfo.State.Usable = false;
                        pdiskInfo.State.WhyUnusable += 'X';
                        if (pdisk.HasDiskScope()) {
                            pdiskInfo.State.DiskScope = pdisk.GetDiskScope();
                        }
                        if (pdisk.HasPDiskConfig()) {
                            applyPDiskConfig(pdiskMapperSettings[pdiskId], pdisk.GetPDiskConfig());
                        }
                    }
                }

                auto& m = maxPDiskId[pdiskId.NodeId];
                m = Max(m, pdiskId.PDiskId);

                addedPDisks.insert(pdiskId);
            }

            AddServiceSetToPlacementSnapshot(placementSnapshot, ss);
        }

        for (const auto& vslot : placementSnapshot.VSlots) {
            maxVSlotId[vslot.PDiskId] = Max(maxVSlotId[vslot.PDiskId], vslot.VSlotId.VSlotId);
        }

        // build PDisk locator map (nodeId:path -> pdiskId)
        THashSet<std::tuple<ui32, TString>> pdiskLocations;
        for (const auto& [pdiskId, item] : pdisks) {
            pdiskLocations.emplace(std::make_tuple(pdiskId.NodeId, item.Record.GetPath()));
        }

        // build host config map
        auto processDrive = [&](const auto& node, const auto& drive) {
            const ui32 nodeId = node.GetNodeId();
            if (pdiskLocations.contains(std::make_tuple(nodeId, drive.GetPath()))) {
                return;
            }
            if (checkMatch(drive.GetType(), drive.GetSharedWithOs(), drive.GetReadCentric(), drive.GetKind())) {
                const TPDiskId pdiskId(nodeId, ++maxPDiskId[nodeId]);
                if (const auto [it, inserted] = pdisks.try_emplace(pdiskId); inserted) {
                    TPDiskInfo& pdiskInfo = it->second;
                    pdiskInfo.State.PDiskId = pdiskId;
                    auto& r = pdiskInfo.Record;
                    r.SetNodeID(pdiskId.NodeId);
                    r.SetPDiskID(pdiskId.PDiskId);
                    r.SetPath(drive.GetPath());
                    r.SetPDiskGuid(RandomNumber<ui64>());
                    r.SetPDiskCategory(TPDiskCategory(static_cast<NPDisk::EDeviceType>(drive.GetType()),
                        drive.GetKind()));
                    if (drive.HasDiskScope()) {
                        pdiskInfo.State.DiskScope = drive.GetDiskScope();
                        r.SetDiskScope(drive.GetDiskScope());
                    }
                    if (drive.HasPDiskConfig()) {
                        r.MutablePDiskConfig()->CopyFrom(drive.GetPDiskConfig());
                        applyPDiskConfig(pdiskMapperSettings[pdiskId], drive.GetPDiskConfig());
                    }
                } else {
                    throw TExConfigError() << "duplicate PDiskId";
                }
            }
        };
        EnumerateConfigDrives(config, 0, processDrive, nullptr, true);

        for (const auto& [pdiskId, item] : pdisks) {
            const auto it = nodeLocations.find(pdiskId.NodeId);
            if (it == nodeLocations.end()) {
                throw TExConfigError() << "no location for node";
            }

            const auto settingsIt = pdiskMapperSettings.find(pdiskId);
            const TPDiskMapperSettings settings = settingsIt != pdiskMapperSettings.end()
                ? settingsIt->second
                : TPDiskMapperSettings{};

            ui32 maxSlots = defaultMaxSlots;
            if (settings.SlotCount) {
                maxSlots = settings.SlotCount;
            } else if (settings.SlotSizeInBytes) {
                // Slot count for byte-sized slots is calculated by NodeWarden and arrives via PDisk config or
                // metrics. Until then MaxSlots is its upper bound (ExpectedSlotCount = min(size/slot, MaxSlots)),
                // which keeps fresh drives usable for static group allocation during bootstrap.
                maxSlots = settings.MaxSlots;
            }

            const bool nodeAllowFilter = !restrictSelfHealNodes || selfHealAllowedNodeSet.contains(pdiskId.NodeId);

            auto pdiskState = item.State;
            pdiskState.Location = it->second;
            pdiskState.MaxSlots = maxSlots;
            pdiskState.SlotSizeInUnits = settings.SlotSizeInUnits;
            pdiskState.SlotSizeInBytes = settings.SlotSizeInBytes;
            pdiskState.BridgePileId = ResolveNodePileId(pdiskState.Location);
            pdiskState.Usable = pdiskState.Usable && nodeAllowFilter;
            if (!nodeAllowFilter) {
                pdiskState.WhyUnusable += 'H'; // node is not in the self-heal allow-list
            }
            placementSnapshot.PDisks.push_back(std::move(pdiskState));
        }

        NBsController::TGroupMapper::TReassignmentRequest request;
        request.GroupId = params.GroupId.GetRawId();
        request.GroupGeneration = groupGeneration;
        request.VDisks = std::move(groupDisks);
        request.ForbiddenPDisks = std::move(params.ForbiddenPDisks);
        request.GroupSizeInUnits = 1; // static groups are always single-unit
        request.MinimumRequiredSpace = params.RequiredSpace.value_or(existingGroup ? Min<i64>() : 0);
        request.ExistingGroup = existingGroup;
        request.TryToRelocateLocallyFirst = params.TryToRelocateBrokenDisksLocallyFirst;
        request.BridgePileId = params.BridgePileId;

        NBsController::TGroupGeometryInfo geometry(params.GroupType.GetErasure(), selfManagementConfig.GetGeometry());
        const NBsController::TGroupMapper::TOptions options{
            .PreferLessOccupiedRack = params.PreferLessOccupiedRack,
            .WithAttentionToReplication = params.WithAttentionToReplication,
            .IgnoreVSlotQuotaCheck = params.IgnoreVSlotQuotaCheck,
            .SettleOnlyOnOperationalDisks = params.SettleOnlyOnOperationalDisks,
            .IsSelfHealReasonDecommit = params.IsSelfHealReasonDecommit,
            .SpaceColorBorder = pdiskSpaceColorBorder.value_or(NKikimrBlobStorage::TPDiskSpaceColor::GREEN),
            .SpaceMarginPromille = pdiskSpaceMarginPromille,
        };
        auto outcome = NBsController::TGroupMapper::PlanGroupReassignment(std::move(geometry), options,
                                                                          std::move(placementSnapshot), std::move(request));
        auto& groupDefinition = outcome.Group;

        auto dumpGroupDefinition = [&] {
            TStringStream s;
            for (const auto& r : groupDefinition) {
                s << '{';
                for (const auto& d : r) {
                    s << '[';
                    bool first = true;
                    for (const auto& p : d) {
                        s << (std::exchange(first, false) ? "" : " ") << p;
                    }
                    s << ']';
                }
                s << '}';
            }
            return s.Str();
        };

        if (!outcome.Success) {
            throw TExConfigError() << "group allocation failed Error# " << outcome.Error.ErrorMessage
                << " groupDefinition# " << dumpGroupDefinition();
        }

        auto *sSet = bsConfig->MutableServiceSet();

        NKikimrBlobStorage::TGroupInfo *sGroup = nullptr;
        for (size_t i = 0; i < sSet->GroupsSize(); ++i) {
            if (const auto& group = sSet->GetGroups(i); TGroupId::FromProto(&group,
                    &NKikimrBlobStorage::TGroupInfo::GetGroupID) == params.GroupId) {
                sGroup = sSet->MutableGroups(i);
                break;
            }
        }
        if (!sGroup) {
            sGroup = sSet->AddGroups();
            params.GroupId.CopyToProto(sGroup, &NKikimrBlobStorage::TGroupInfo::SetGroupID);
            sGroup->SetErasureSpecies(params.GroupType.GetErasure());
        } else {
            sGroup->ClearRings();
        }
        sGroup->SetGroupGeneration(params.GroupGeneration);

        auto makeVSlotId = [](const NKikimrBlobStorage::TVDiskLocation& location) {
            NKikimrBlobStorage::TVSlotId vslotId;
            vslotId.SetNodeId(location.GetNodeID());
            vslotId.SetPDiskId(location.GetPDiskID());
            vslotId.SetVSlotId(location.GetVDiskSlotID());
            return vslotId;
        };

        if (params.BridgeProxyGroupId) {
            params.BridgeProxyGroupId->CopyToProto(sGroup, &NKikimrBlobStorage::TGroupInfo::SetBridgeProxyGroupId);
        }
        params.BridgePileId.CopyToProto(sGroup, &NKikimrBlobStorage::TGroupInfo::SetBridgePileId);

        THashMap<TVDiskIdShort, NProtoBuf::RepeatedPtrField<NKikimrBlobStorage::TNodeWardenServiceSet::TVDisk::TDonor>> donors;

        for (size_t i = 0; i < sSet->VDisksSize(); ++i) {
            const auto& vdisk = sSet->GetVDisks(i);
            const TVDiskID vdiskId = VDiskIDFromVDiskID(vdisk.GetVDiskID());
            if (vdiskId.GroupID != params.GroupId || vdisk.GetEntityStatus() == NKikimrBlobStorage::EEntityStatus::DESTROY) {
                continue;
            }
            auto *m = sSet->MutableVDisks(i);
            const TVDiskIdShort shortVDiskId(vdiskId);
            if (replacedDisks.contains(shortVDiskId)) {
                if (params.Reassignments && vdiskId.GroupGeneration + 1 == params.GroupGeneration) {
                    (*params.Reassignments)[shortVDiskId].SourceSlotId = makeVSlotId(vdisk.GetVDiskLocation());
                }
                if (m->HasDonorMode()) {
                    // this disk is already a donor, nothing to do about it
                } else if (params.ConvertToDonor) {
                    // make this disk a donor
                    auto *donorMode = m->MutableDonorMode();
                    donorMode->SetNumFailRealms(groupDefinition.size());
                    donorMode->SetNumFailDomainsPerFailRealm(groupDefinition.front().size());
                    donorMode->SetNumVDisksPerFailDomain(groupDefinition.front().front().size());
                    donorMode->SetErasureSpecies(sGroup->GetErasureSpecies());
                    m->ClearDonors();
                } else {
                    m->SetEntityStatus(NKikimrBlobStorage::EEntityStatus::DESTROY);
                    continue;
                }
                auto *donor = donors[vdiskId].Add();
                donor->MutableVDiskId()->CopyFrom(m->GetVDiskID());
                donor->MutableVDiskLocation()->CopyFrom(m->GetVDiskLocation());
            } else {
                m->MutableVDiskID()->SetGroupGeneration(params.GroupGeneration);
            }
        }

        TVDiskIdShort prev;
        NKikimrBlobStorage::TGroupInfo::TFailRealm *sRealm = nullptr;
        NKikimrBlobStorage::TGroupInfo::TFailRealm::TFailDomain *sDomain = nullptr;

        NBsController::TGroupMapper::Traverse(groupDefinition, [&](TVDiskIdShort vdiskId, TPDiskId pdiskId) {
            if (!sRealm || vdiskId.FailRealm != prev.FailRealm) {
                sRealm = sGroup->AddRings();
                sDomain = nullptr;
            }
            if (!sDomain || vdiskId.FailDomain != prev.FailDomain) {
                sDomain = sRealm->AddFailDomains();
            }
            prev = vdiskId;

            const auto pdiskIt = pdisks.find(pdiskId);
            Y_ABORT_UNLESS(pdiskIt != pdisks.end());
            const auto& pdisk = pdiskIt->second.Record;

            if (addedPDisks.insert(pdiskId).second) {
                sSet->AddPDisks()->CopyFrom(pdisk);
            }

            auto *sLoc = sDomain->AddVDiskLocations();
            if (const auto it = vdiskLocations.find(vdiskId); it != vdiskLocations.end()) {
                sLoc->CopyFrom(it->second);
            } else {
                sLoc->SetNodeID(pdiskId.NodeId);
                sLoc->SetPDiskID(pdiskId.PDiskId);
                sLoc->SetVDiskSlotID(++maxVSlotId[pdiskId]); // keep VDiskSlotID for unchanged items
                sLoc->SetPDiskGuid(pdisk.GetPDiskGuid());

                auto *sDisk = sSet->AddVDisks();
                VDiskIDFromVDiskID(TVDiskID(params.GroupId, params.GroupGeneration, vdiskId), sDisk->MutableVDiskID());
                sDisk->SetVDiskKind(NKikimrBlobStorage::TVDiskKind::Default);
                sDisk->MutableVDiskLocation()->CopyFrom(*sLoc);
                if (const auto it = donors.find(vdiskId); it != donors.end()) {
                    sDisk->MutableDonors()->Swap(&it->second);
                }
                if (params.Reassignments) {
                    if (const auto it = params.Reassignments->find(vdiskId); it != params.Reassignments->end()) {
                        it->second.TargetSlotId = makeVSlotId(*sLoc);
                    }
                }
            }
        });
    }

    bool TDistributedConfigKeeper::GenerateStateStorageConfig(NKikimrConfig::TStateStorageConfig *ss
            , const NKikimrBlobStorage::TStorageConfig& baseConfig, std::unordered_set<ui32>& usedNodes
            , const std::unordered_set<ui32>& nodesToUse
            , const NKikimrConfig::TStateStorageConfig *oldConfig
            , bool automaticManagement
            , ui32 overrideReplicasInRingCount
            , ui32 overrideRingsCount
            , ui32 replicasSpecificVolume
        ) {
        if (!automaticManagement) {
            if (oldConfig) {
                ss->CopyFrom(*oldConfig);
            }

            const auto collectNodes = [&](const auto& self, const auto& ring) -> void {
                for (ui32 nodeId : ring.GetNode()) {
                    usedNodes.insert(nodeId);
                }
                for (const auto& subRing : ring.GetRing()) {
                    self(self, subRing);
                }
            };

            if (oldConfig) {
                if (oldConfig->HasRing()) {
                    collectNodes(collectNodes, oldConfig->GetRing());
                }
                for (const auto& rg : oldConfig->GetRingGroups()) {
                    collectNodes(collectNodes, rg);
                }
            }

            return true;
        }
        std::map<TBridgePileId, THashMap<TString, std::vector<std::tuple<ui32, TNodeLocation>>>> nodes;
        bool goodConfig = true;
        for (const auto& node : baseConfig.GetAllNodes()) {
            if (!nodesToUse.empty() && !nodesToUse.contains(node.GetNodeId())) {
                continue;
            }
            TNodeLocation location(node.GetLocation());
            TBridgePileId pileId = ResolveNodePileId(location);
            nodes[pileId][location.GetDataCenterId()].emplace_back(node.GetNodeId(), location);
        }
        for (auto& [pileId, nodesByDataCenter] : nodes) {
            TStateStoragePerPileGenerator generator(nodesByDataCenter, SelfHealNodesState, pileId, usedNodes,
                oldConfig ? *oldConfig : NKikimrConfig::TStateStorageConfig(), overrideReplicasInRingCount,
                overrideRingsCount, replicasSpecificVolume);
            generator.AddRingGroup(ss);
            goodConfig &= generator.IsGoodConfig();
        }
        if (ss->RingGroupsSize() == 0) {
            // nodesToUse was non-empty, but none of the specified node IDs exist in baseConfig
            // (e.g. StateStorageSelfHealAllowedNodes referencing decommissioned nodes); avoid
            // returning a bogus empty state storage config that would trigger a destructive
            // full reconfiguration.
            return false;
        }
        return goodConfig;
    }

    bool TDistributedConfigKeeper::UpdateConfig(NKikimrBlobStorage::TStorageConfig *config) {
        return UpdateBridgeConfig(config);
    }

} // NKikimr::NStorage

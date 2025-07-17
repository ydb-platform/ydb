#include "distconf.h"

namespace NKikimr::NStorage {

    bool TDistributedConfigKeeper::GenerateFirstConfig(NKikimrBlobStorage::TStorageConfig *config) {
        bool changes = false;

        if (config->HasBlobStorageConfig()) {
            const auto& bsConfig = config->GetBlobStorageConfig();
            const bool noStaticGroup = !bsConfig.HasServiceSet() || !bsConfig.GetServiceSet().GroupsSize();
            if (noStaticGroup && bsConfig.HasAutoconfigSettings() && bsConfig.GetAutoconfigSettings().HasErasureSpecies()) {
                try {
                    const auto& settings = bsConfig.GetAutoconfigSettings();

                    const auto species = TBlobStorageGroupType::ErasureSpeciesByName(settings.GetErasureSpecies());
                    if (species == TBlobStorageGroupType::ErasureSpeciesCount) {
                        throw TExConfigError() << "invalid erasure specified for static group"
                            << " Erasure# " << settings.GetErasureSpecies();
                    }

                    AllocateStaticGroup(config, 0 /*groupId*/, 1 /*groupGeneration*/, TBlobStorageGroupType(species),
                        settings.GetGeometry(), settings.GetPDiskFilter(),
                        settings.HasPDiskType() ? std::make_optional(settings.GetPDiskType()) : std::nullopt, {}, {}, 0,
                        nullptr, false, true, false);
                    changes = true;
                    STLOG(PRI_DEBUG, BS_NODE, NWDC33, "Allocated static group", (Group, bsConfig.GetServiceSet().GetGroups(0)));
                } catch (const TExConfigError& ex) {
                    STLOG(PRI_ERROR, BS_NODE, NWDC10, "Failed to allocate static group", (Reason, ex.what()));
                }
            }
        }

        if (!Cfg->DomainsConfig) { // no automatic configuration required
        } else if (Cfg->DomainsConfig->StateStorageSize() == 1) { // the StateStorage config is already defined explicitly, just migrate it
            const auto& ss = Cfg->DomainsConfig->GetStateStorage(0);
            config->MutableStateStorageConfig()->CopyFrom(ss);
            config->MutableStateStorageBoardConfig()->CopyFrom(ss);
            config->MutableSchemeBoardConfig()->CopyFrom(ss);
        } else if (!Cfg->DomainsConfig->StateStorageSize()) { // no StateStorage config, generate a new one
            GenerateStateStorageConfig(config->MutableStateStorageConfig(), *config);
            GenerateStateStorageConfig(config->MutableStateStorageBoardConfig(), *config);
            GenerateStateStorageConfig(config->MutableSchemeBoardConfig(), *config);
        }

        if (!config->GetSelfAssemblyUUID()) {
            config->SetSelfAssemblyUUID(CreateGuidAsString());
            changes = true;
        }

        return changes;
    }

    void TDistributedConfigKeeper::AllocateStaticGroup(NKikimrBlobStorage::TStorageConfig *config, ui32 groupId,
            ui32 groupGeneration, TBlobStorageGroupType gtype, const NKikimrBlobStorage::TGroupGeometry& geometry,
            const NProtoBuf::RepeatedPtrField<NKikimrBlobStorage::TPDiskFilter>& pdiskFilters,
            std::optional<NKikimrBlobStorage::EPDiskType> pdiskType,
            THashMap<TVDiskIdShort, NBsController::TPDiskId> replacedDisks,
            const NBsController::TGroupMapper::TForbiddenPDisks& forbid, i64 requiredSpace,
            NKikimrBlobStorage::TBaseConfig *baseConfig, bool convertToDonor, bool ignoreVSlotQuotaCheck,
            bool isSelfHealReasonDecommit) {
        using TPDiskId = NBsController::TPDiskId;

        NKikimrConfig::TBlobStorageConfig *bsConfig = config->MutableBlobStorageConfig();

        // build node location map
        THashMap<ui32, TNodeLocation> nodeLocations;
        for (const auto& node : config->GetAllNodes()) {
            nodeLocations.try_emplace(node.GetNodeId(), node.GetLocation());
        }

        struct TPDiskInfo {
            NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk Record;
            ui32 UsedSlots = 0;
            bool Usable = true;
            TString WhyUnusable;
            i64 SpaceAvailable = 0;
            bool AdjustSpaceAvailable = false;
        };

        THashMap<TPDiskId, TPDiskInfo> pdisks;

        auto checkMatch = [&](NKikimrBlobStorage::EPDiskType type, bool sharedWithOs, bool readCentric, ui64 kind) {
            if (type == pdiskType) {
                return true;
            }
            for (const auto& pdiskFilter : pdiskFilters) {
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

        if (baseConfig) {
            std::optional<NKikimrBlobStorage::TPDiskSpaceColor::E> pdiskSpaceColorBorder;
            ui32 pdiskSpaceMarginPromille = 150;

            if (baseConfig->HasSettings()) {
                const auto& settings = baseConfig->GetSettings();
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

            for (const auto& pdisk : baseConfig->GetPDisk()) {
                if (!checkMatch(pdisk.GetType(), pdisk.GetSharedWithOs(), pdisk.GetReadCentric(), pdisk.GetKind())) {
                    continue;
                }

                const TPDiskId pdiskId(pdisk.GetNodeId(), pdisk.GetPDiskId());
                if (const auto [it, inserted] = pdisks.try_emplace(pdiskId); inserted) {
                    TPDiskInfo& pdiskInfo = it->second;
                    auto& r = pdiskInfo.Record;
                    r.SetNodeID(pdiskId.NodeId);
                    r.SetPDiskID(pdiskId.PDiskId);
                    r.SetPath(pdisk.GetPath());
                    r.SetPDiskGuid(pdisk.GetGuid());
                    r.SetPDiskCategory(TPDiskCategory(static_cast<NPDisk::EDeviceType>(pdisk.GetType()),
                        pdisk.GetKind()).GetRaw());
                    if (pdisk.HasPDiskConfig()) {
                        r.MutablePDiskConfig()->CopyFrom(pdisk.GetPDiskConfig());
                    }

                    // this 'usable' logic repeats the one in BS_CONTROLLER
                    if (pdisk.GetDriveStatus() != NKikimrBlobStorage::EDriveStatus::ACTIVE) {
                        pdiskInfo.Usable = false;
                        pdiskInfo.WhyUnusable += 'S';
                    }
                    const bool usableInTermsOfDecommission = 
                        pdisk.GetDecommitStatus() == NKikimrBlobStorage::EDecommitStatus::DECOMMIT_NONE ||
                        pdisk.GetDecommitStatus() == NKikimrBlobStorage::EDecommitStatus::DECOMMIT_REJECTED && !isSelfHealReasonDecommit;
                    if (!usableInTermsOfDecommission) {
                        pdiskInfo.Usable = false;
                        pdiskInfo.WhyUnusable += 'D';
                    }

                    if (!ignoreVSlotQuotaCheck && pdiskInfo.Usable && pdisk.HasPDiskMetrics() && baseConfig->HasSettings()) {
                        const auto& m = pdisk.GetPDiskMetrics();
                        if (m.HasEnforcedDynamicSlotSize() && pdiskSpaceColorBorder >= NKikimrBlobStorage::TPDiskSpaceColor::YELLOW) {
                            pdiskInfo.SpaceAvailable = m.GetEnforcedDynamicSlotSize() * (1000 - pdiskSpaceMarginPromille) / 1000;
                        } else {
                            pdiskInfo.SpaceAvailable = m.GetAvailableSize() - m.GetTotalSize() * pdiskSpaceMarginPromille / 1000;
                            pdiskInfo.AdjustSpaceAvailable = true;
                        }
                    }
                } else {
                    Y_ABORT("duplicate PDisk record in TBaseConfig");
                }
            }

            THashMap<ui32, ui64> maxGroupSlotSize;
            for (const auto& vslot : baseConfig->GetVSlot()) {
                if (vslot.HasVDiskMetrics()) {
                    if (const auto& m = vslot.GetVDiskMetrics(); m.HasAllocatedSize()) {
                        ui64& size = maxGroupSlotSize[vslot.GetGroupId()];
                        size = Max(size, m.GetAllocatedSize());
                    }
                }
            }

            for (const auto& vslot : baseConfig->GetVSlot()) {
                const auto& vslotId = vslot.GetVSlotId();
                const TPDiskId pdiskId(vslotId.GetNodeId(), vslotId.GetPDiskId());
                if (const auto it = pdisks.find(pdiskId); it != pdisks.end()) {
                    TPDiskInfo& pdiskInfo = it->second;
                    ++pdiskInfo.UsedSlots;
                    if (pdiskInfo.AdjustSpaceAvailable && vslot.GetStatus() != "READY" && vslot.HasVDiskMetrics()) {
                        if (const auto& m = vslot.GetVDiskMetrics(); m.HasAllocatedSize()) {
                            pdiskInfo.SpaceAvailable += m.GetAllocatedSize() - maxGroupSlotSize[vslot.GetGroupId()];
                        }
                    }
                }
            }
        }

        // then all existing drives from the current storage config; also extract group definition, if it exists
        NBsController::TGroupMapper::TGroupDefinition groupDefinition;
        THashMap<ui32, ui32> maxPDiskId;
        THashMap<TPDiskId, ui32> maxVSlotId;
        THashSet<TPDiskId> addedPDisks;
        THashMap<TVDiskIdShort, NKikimrBlobStorage::TVDiskLocation> vdiskLocations;

        if (bsConfig->HasServiceSet()) {
            const auto& ss = bsConfig->GetServiceSet();

            for (const auto& vdisk : ss.GetVDisks()) {
                const TVDiskID vdiskId = VDiskIDFromVDiskID(vdisk.GetVDiskID());
                if (vdiskId.GroupID.GetRawId() == groupId) {
                    vdiskLocations.emplace(vdiskId, vdisk.GetVDiskLocation());
                }
            }

            std::vector<std::tuple<TPDiskId, i32>> usageIncr;

            THashSet<TPDiskId> requiredPDiskIds;
            for (const auto& group : ss.GetGroups()) {
                if (group.GetGroupID() == groupId) {
                    ui32 failRealmIdx = 0;
                    Y_DEBUG_ABORT_UNLESS(groupDefinition.empty());
                    groupDefinition.clear();

                    for (const auto& r : group.GetRings()) {
                        ui32 failDomainIdx = 0;
                        auto& grDefRealm = groupDefinition.emplace_back();

                        for (const auto& fd : r.GetFailDomains()) {
                            ui32 vdiskIdx = 0;
                            auto& grDefDomain = grDefRealm.emplace_back();

                            for (const auto& v : fd.GetVDiskLocations()) {
                                const TVDiskIdShort vdiskId(failRealmIdx, failDomainIdx, vdiskIdx);

                                TPDiskId pdiskId(v.GetNodeID(), v.GetPDiskID());
                                requiredPDiskIds.insert(pdiskId);

                                if (const auto it = replacedDisks.find(vdiskId); it != replacedDisks.end()) {
                                    usageIncr.emplace_back(pdiskId, -1); // drop usage count of current PDisk
                                    std::swap(pdiskId, it->second);
                                    if (pdiskId != TPDiskId()) {
                                        usageIncr.emplace_back(pdiskId, +1); // and increase for the new PDisk
                                    }
                                    vdiskLocations.erase(vdiskId);
                                }
                                grDefDomain.emplace_back(pdiskId);

                                ++vdiskIdx;
                            }
                            ++failDomainIdx;
                        }
                        ++failRealmIdx;
                    }
                }
            }

            for (const auto& pdisk : ss.GetPDisks()) {
                const TPDiskId pdiskId(pdisk.GetNodeID(), pdisk.GetPDiskID());
                if (requiredPDiskIds.contains(pdiskId)) {
                    if (const auto [it, inserted] = pdisks.try_emplace(pdiskId); inserted) {
                        auto& r = it->second.Record;
                        r.CopyFrom(pdisk);
                    }
                }

                auto& m = maxPDiskId[pdiskId.NodeId];
                m = Max(m, pdiskId.PDiskId);

                addedPDisks.insert(pdiskId);
            }

            for (const auto& [pdiskId, incr] : usageIncr) {
                if (const auto it = pdisks.find(pdiskId); it != pdisks.end()) {
                    it->second.UsedSlots += incr;
                } else {
                    Y_ABORT("missing PDiskId from group");
                }
            }

            for (const auto& vdisk : ss.GetVDisks()) {
                const auto& loc = vdisk.GetVDiskLocation();
                const TPDiskId pdiskId(loc.GetNodeID(), loc.GetPDiskID());
                if (const auto it = pdisks.find(pdiskId); it != pdisks.end()) {
                    ++it->second.UsedSlots;
                }

                auto& m = maxVSlotId[pdiskId];
                m = Max(m, loc.GetVDiskSlotID());
            }
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
                    auto& r = it->second.Record;
                    r.SetNodeID(pdiskId.NodeId);
                    r.SetPDiskID(pdiskId.PDiskId);
                    r.SetPath(drive.GetPath());
                    r.SetPDiskGuid(RandomNumber<ui64>());
                    r.SetPDiskCategory(TPDiskCategory(static_cast<NPDisk::EDeviceType>(drive.GetType()),
                        drive.GetKind()));
                    if (drive.HasPDiskConfig()) {
                        r.MutablePDiskConfig()->CopyFrom(drive.GetPDiskConfig());
                    }
                } else {
                    Y_ABORT("duplicate PDiskId");
                }
            }
        };
        EnumerateConfigDrives(*config, 0, processDrive, nullptr, true);

        // group mapper
        NBsController::TGroupGeometryInfo geom(gtype.GetErasure(), geometry);
        NBsController::TGroupMapper mapper(geom);

        for (const auto& [pdiskId, item] : pdisks) {
            const auto it = nodeLocations.find(pdiskId.NodeId);
            if (it == nodeLocations.end()) {
                throw TExConfigError() << "no location for node";
            }

            ui32 maxSlots = defaultMaxSlots;
            if (item.Record.HasPDiskConfig()) {
                const auto& pdiskConfig = item.Record.GetPDiskConfig();
                if (pdiskConfig.HasExpectedSlotCount()) {
                    maxSlots = pdiskConfig.GetExpectedSlotCount();
                }
            }

            mapper.RegisterPDisk({
                .PDiskId = pdiskId,
                .Location = it->second,
                .Usable = item.Usable,
                .NumSlots = item.UsedSlots,
                .MaxSlots = maxSlots,
                .Groups{},
                .SpaceAvailable = item.SpaceAvailable,
                .Operational = true,
                .Decommitted = false,
                .WhyUnusable = item.WhyUnusable,
            });
        }

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

        TString error;
        if (!mapper.AllocateGroup(groupId, groupDefinition, replacedDisks, forbid, requiredSpace, false, error)) {
            throw TExConfigError() << "group allocation failed Error# " << error
                << " groupDefinition# " << dumpGroupDefinition();
        }

        auto *sSet = bsConfig->MutableServiceSet();

        NKikimrBlobStorage::TGroupInfo *sGroup = nullptr;
        for (size_t i = 0; i < sSet->GroupsSize(); ++i) {
            if (const auto& group = sSet->GetGroups(i); group.GetGroupID() == groupId) {
                sGroup = sSet->MutableGroups(i);
                break;
            }
        }
        if (!sGroup) {
            sGroup = sSet->AddGroups();
            sGroup->SetGroupID(groupId);
            sGroup->SetErasureSpecies(gtype.GetErasure());
        } else {
            sGroup->ClearRings();
        }
        sGroup->SetGroupGeneration(groupGeneration);

        TVDiskIdShort prev;
        NKikimrBlobStorage::TGroupInfo::TFailRealm *sRealm = nullptr;
        NKikimrBlobStorage::TGroupInfo::TFailRealm::TFailDomain *sDomain = nullptr;

        THashMap<TVDiskIdShort, NProtoBuf::RepeatedPtrField<NKikimrBlobStorage::TNodeWardenServiceSet::TVDisk::TDonor>> donors;

        for (size_t i = 0; i < sSet->VDisksSize(); ++i) {
            const auto& vdisk = sSet->GetVDisks(i);
            const TVDiskID vdiskId = VDiskIDFromVDiskID(vdisk.GetVDiskID());
            if (vdiskId.GroupID.GetRawId() != groupId || vdisk.GetEntityStatus() == NKikimrBlobStorage::EEntityStatus::DESTROY) {
                continue;
            }
            auto *m = sSet->MutableVDisks(i);
            if (replacedDisks.contains(vdiskId)) {
                if (m->HasDonorMode()) {
                    // this disk is already a donor, nothing to do about it
                } else if (convertToDonor) {
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
                m->MutableVDiskID()->SetGroupGeneration(groupGeneration);
            }
        }

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
                VDiskIDFromVDiskID(TVDiskID(TGroupId::FromValue(groupId), groupGeneration, vdiskId), sDisk->MutableVDiskID());
                sDisk->SetVDiskKind(NKikimrBlobStorage::TVDiskKind::Default);
                sDisk->MutableVDiskLocation()->CopyFrom(*sLoc);
                if (const auto it = donors.find(vdiskId); it != donors.end()) {
                    sDisk->MutableDonors()->Swap(&it->second);
                }
            }
        });
    }

    void TDistributedConfigKeeper::GenerateStateStorageConfig(NKikimrConfig::TDomainsConfig::TStateStorage *ss,
            const NKikimrBlobStorage::TStorageConfig& baseConfig) {
        auto *ring = ss->MutableRing();

        THashMap<TString, std::vector<std::tuple<ui32, TNodeLocation>>> nodesByDataCenter;

        for (const auto& node : baseConfig.GetAllNodes()) {
            TNodeLocation location(node.GetLocation());
            nodesByDataCenter[location.GetDataCenterId()].emplace_back(node.GetNodeId(), location);
        }

        auto pickNodes = [](std::vector<std::tuple<ui32, TNodeLocation>>& nodes, size_t count) {
            Y_ABORT_UNLESS(count <= nodes.size());
            auto comp = [](const auto& x, const auto& y) { return std::get<1>(x).GetRackId() < std::get<1>(y).GetRackId(); };
            std::ranges::sort(nodes, comp);
            std::vector<ui32> result;
            THashSet<ui32> disabled;
            auto iter = nodes.begin();
            while (result.size() < count) {
                const auto& [nodeId, location] = *iter++;
                if (disabled.contains(nodeId)) {
                    if (iter == nodes.end()) {
                        iter = nodes.begin();
                    }
                    continue;
                }
                result.push_back(nodeId);
                disabled.insert(nodeId);
                while (iter != nodes.end() && std::get<1>(*iter).GetRackId() == location.GetRackId()) {
                    ++iter;
                }
                if (iter == nodes.end()) {
                    iter = nodes.begin();
                }
            }
            return result;
        };

        std::vector<ui32> nodes;

        const size_t maxNodesPerDataCenter = nodesByDataCenter.size() == 1 ? 8 : 3;
        for (auto& [_, v] : nodesByDataCenter) {
            auto r = pickNodes(v, Min<size_t>(v.size(), maxNodesPerDataCenter));
            nodes.insert(nodes.end(), r.begin(), r.end());
        }

        for (ui32 nodeId : nodes) {
            ring->AddNode(nodeId);
        }

        ring->SetNToSelect(nodes.size() / 2 + 1);
    }

    bool TDistributedConfigKeeper::UpdateConfig(NKikimrBlobStorage::TStorageConfig *config) {
        (void)config;
        return false;
    }

} // NKikimr::NStorage

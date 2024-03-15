#include "distconf.h"

namespace NKikimr::NStorage {

    bool TDistributedConfigKeeper::GenerateFirstConfig(NKikimrBlobStorage::TStorageConfig *config) {
        bool changes = false;

        if (config->HasBlobStorageConfig()) {
            const auto& bsConfig = config->GetBlobStorageConfig();
            const bool noStaticGroup = !bsConfig.HasServiceSet() || !bsConfig.GetServiceSet().GroupsSize();
            if (noStaticGroup && bsConfig.HasAutoconfigSettings() && bsConfig.GetAutoconfigSettings().HasErasureSpecies()) {
                try {
                    AllocateStaticGroup(config);
                    changes = true;
                    STLOG(PRI_DEBUG, BS_NODE, NWDC33, "Allocated static group", (Group, bsConfig.GetServiceSet().GetGroups(0)));
                } catch (const TExConfigError& ex) {
                    STLOG(PRI_ERROR, BS_NODE, NWDC10, "Failed to allocate static group", (Reason, ex.what()));
                }
            }
        }

        if (!config->GetSelfAssemblyUUID()) {
            config->SetSelfAssemblyUUID(CreateGuidAsString());
            changes = true;
        }

        return changes;
    }

    void TDistributedConfigKeeper::AllocateStaticGroup(NKikimrBlobStorage::TStorageConfig *config) {
        NKikimrConfig::TBlobStorageConfig *bsConfig = config->MutableBlobStorageConfig();
        const auto& settings = bsConfig->GetAutoconfigSettings();

        // build node location map
        THashMap<ui32, TNodeLocation> nodeLocations;
        for (const auto& node : config->GetAllNodes()) {
            nodeLocations.try_emplace(node.GetNodeId(), node.GetLocation());
        }

        // group mapper
        const auto species = TBlobStorageGroupType::ErasureSpeciesByName(settings.GetErasureSpecies());
        if (species == TBlobStorageGroupType::ErasureSpeciesCount) {
            throw TExConfigError() << "invalid erasure specified for static group"
                << " Erasure# " << settings.GetErasureSpecies();
        }
        NBsController::TGroupGeometryInfo geom(species, settings.GetGeometry());
        NBsController::TGroupMapper mapper(geom);

        // build host config map
        THashMap<ui64, const NKikimrBlobStorage::TDefineHostConfig*> hostConfigs;
        for (const auto& hc : settings.GetDefineHostConfig()) {
            const bool inserted = hostConfigs.try_emplace(hc.GetHostConfigId(), &hc).second;
            Y_ABORT_UNLESS(inserted);
        }

        // find all drives
        THashMap<NBsController::TPDiskId, NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk> pdiskMap;
        const auto& defineBox = settings.GetDefineBox();
        for (const auto& host : defineBox.GetHost()) {
            const ui32 nodeId = host.GetEnforcedNodeId();
            if (!nodeId) {
                throw TExConfigError() << "EnforcedNodeId is not specified in DefineBox";
            }

            const auto it = hostConfigs.find(host.GetHostConfigId());
            if (it == hostConfigs.end()) {
                throw TExConfigError() << "no matching DefineHostConfig"
                    << " HostConfigId# " << host.GetHostConfigId();
            }
            const auto& defineHostConfig = *it->second;

            ui32 pdiskId = 1;
            for (const auto& drive : defineHostConfig.GetDrive()) {
                bool matching = false;
                for (const auto& pdiskFilter : settings.GetPDiskFilter()) {
                    bool m = true;
                    for (const auto& p : pdiskFilter.GetProperty()) {
                        bool pMatch = false;
                        switch (p.GetPropertyCase()) {
                            case NKikimrBlobStorage::TPDiskFilter::TRequiredProperty::kType:
                                pMatch = p.GetType() == drive.GetType();
                                break;
                            case NKikimrBlobStorage::TPDiskFilter::TRequiredProperty::kSharedWithOs:
                                pMatch = p.GetSharedWithOs() == drive.GetSharedWithOs();
                                break;
                            case NKikimrBlobStorage::TPDiskFilter::TRequiredProperty::kReadCentric:
                                pMatch = p.GetReadCentric() == drive.GetReadCentric();
                                break;
                            case NKikimrBlobStorage::TPDiskFilter::TRequiredProperty::kKind:
                                pMatch = p.GetKind() == drive.GetKind();
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
                        matching = true;
                        break;
                    }
                }
                if (matching) {
                    const auto it = nodeLocations.find(nodeId);
                    if (it == nodeLocations.end()) {
                        throw TExConfigError() << "no location for node";
                    }

                    NBsController::TPDiskId fullPDiskId{nodeId, pdiskId};
                    mapper.RegisterPDisk({
                        .PDiskId = fullPDiskId,
                        .Location = it->second,
                        .Usable = true,
                        .NumSlots = 0,
                        .MaxSlots = 1,
                        .Groups{},
                        .SpaceAvailable = 0,
                        .Operational = true,
                        .Decommitted = false,
                        .WhyUnusable{},
                    });

                    const auto [pdiskIt, inserted] = pdiskMap.try_emplace(fullPDiskId);
                    Y_ABORT_UNLESS(inserted);
                    auto& pdisk = pdiskIt->second;
                    pdisk.SetNodeID(nodeId);
                    pdisk.SetPDiskID(pdiskId);
                    pdisk.SetPath(drive.GetPath());
                    pdisk.SetPDiskGuid(RandomNumber<ui64>());
                    pdisk.SetPDiskCategory(TPDiskCategory(static_cast<NPDisk::EDeviceType>(drive.GetType()),
                        drive.GetKind()).GetRaw());
                    if (drive.HasPDiskConfig()) {
                        pdisk.MutablePDiskConfig()->CopyFrom(drive.GetPDiskConfig());
                    }
                }
                ++pdiskId;
            }
        }

        NBsController::TGroupMapper::TGroupDefinition group;
        const ui32 groupId = 0;
        const ui32 groupGeneration = 1;
        TString error;
        if (!mapper.AllocateGroup(groupId, group, {}, {}, 0, false, error)) {
            throw TExConfigError() << "group allocation failed"
                << " Error# " << error;
        }

        auto *sSet = bsConfig->MutableServiceSet();
        auto *sGroup = sSet->AddGroups();
        sGroup->SetGroupID(groupId);
        sGroup->SetGroupGeneration(groupGeneration);
        sGroup->SetErasureSpecies(species);

        THashSet<NBsController::TPDiskId> addedPDisks;

        for (size_t realmIdx = 0; realmIdx < group.size(); ++realmIdx) {
            const auto& realm = group[realmIdx];
            auto *sRealm = sGroup->AddRings();

            for (size_t domainIdx = 0; domainIdx < realm.size(); ++domainIdx) {
                const auto& domain = realm[domainIdx];
                auto *sDomain = sRealm->AddFailDomains();

                for (size_t vdiskIdx = 0; vdiskIdx < domain.size(); ++vdiskIdx) {
                    const NBsController::TPDiskId pdiskId = domain[vdiskIdx];

                    const auto pdiskIt = pdiskMap.find(pdiskId);
                    Y_ABORT_UNLESS(pdiskIt != pdiskMap.end());
                    const auto& pdisk = pdiskIt->second;

                    if (addedPDisks.insert(pdiskId).second) {
                        sSet->AddPDisks()->CopyFrom(pdisk);
                    }

                    auto *sDisk = sSet->AddVDisks();

                    VDiskIDFromVDiskID(TVDiskID(groupId, groupGeneration, realmIdx, domainIdx, vdiskIdx),
                        sDisk->MutableVDiskID());

                    auto *sLoc = sDisk->MutableVDiskLocation();
                    sLoc->SetNodeID(pdiskId.NodeId);
                    sLoc->SetPDiskID(pdiskId.PDiskId);
                    sLoc->SetVDiskSlotID(0);
                    sLoc->SetPDiskGuid(pdisk.GetPDiskGuid());

                    sDisk->SetVDiskKind(NKikimrBlobStorage::TVDiskKind::Default);

                    sDomain->AddVDiskLocations()->CopyFrom(*sLoc);
                }
            }
        }
    }

    bool TDistributedConfigKeeper::UpdateConfig(NKikimrBlobStorage::TStorageConfig *config) {
        (void)config;
        return false;
    }

} // NKikimr::NStorage

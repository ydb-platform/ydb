#include "distconf.h"

namespace NKikimr::NStorage {

    struct TExConfigError : yexception {};

    void TDistributedConfigKeeper::CheckRootNodeStatus() {
        if (RootState == ERootState::INITIAL && !Binding && HasQuorum()) {
            STLOG(PRI_DEBUG, BS_NODE, NWDC18, "Starting QUORUM_CHECK_TIMEOUT");
            TActivationContext::Schedule(QuorumCheckTimeout, new IEventHandle(TEvPrivate::EvQuorumCheckTimeout, 0,
                SelfId(), {}, nullptr, 0));
            RootState = ERootState::QUORUM_CHECK_TIMEOUT;
        }
    }

    void TDistributedConfigKeeper::HandleQuorumCheckTimeout() {
        if (HasQuorum()) {
            STLOG(PRI_DEBUG, BS_NODE, NWDC19, "Quorum check timeout hit, quorum remains");

            RootState = ERootState::COLLECT_CONFIG;

            TEvScatter task;
            task.MutableCollectConfigs();
            IssueScatterTask(true, std::move(task));
        } else {
            STLOG(PRI_DEBUG, BS_NODE, NWDC20, "Quorum check timeout hit, quorum reset");
            RootState = ERootState::INITIAL; // fall back to waiting for quorum
            IssueNextBindRequest();
        }
    }

    void TDistributedConfigKeeper::ProcessGather(TEvGather *res) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC27, "ProcessGather", (RootState, RootState), (Res, *res));

        switch (RootState) {
            case ERootState::COLLECT_CONFIG:
                if (res->HasCollectConfigs()) {
                    ProcessCollectConfigs(res->MutableCollectConfigs());
                } else {
                    // unexpected reply?
                }
                break;

            case ERootState::PROPOSE_NEW_STORAGE_CONFIG:
                if (res->HasProposeStorageConfig()) {
                    ProcessProposeStorageConig(res->MutableProposeStorageConfig());
                } else {
                    // ?
                }
                break;

            case ERootState::COMMIT_CONFIG:
                break;

            default:
                break;
        }
    }

    bool TDistributedConfigKeeper::HasQuorum() const {
        // we have strict majority of all nodes (including this one)
        return AllBoundNodes.size() + 1 > (NodeIds.size() + 1) / 2;
    }

    void TDistributedConfigKeeper::ProcessCollectConfigs(TEvGather::TCollectConfigs *res) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC31, "ProcessCollectConfigs", (RootState, RootState), (Res, *res));

        NKikimrBlobStorage::TStorageConfig bestConfig;
        ui32 bestNodes = 0;

        for (const auto& item : res->GetItems()) {
            if (!item.HasConfig()) {
                // incorrect reply
                continue;
            }

            const auto& config = item.GetConfig();
            if (!bestNodes || bestConfig.GetGeneration() < config.GetGeneration() ||
                    bestConfig.GetGeneration() == config.GetGeneration() && bestNodes < item.NodesSize()) {
                // check for split generations
                bestConfig.CopyFrom(config);
                bestNodes = item.NodesSize();
            }
        }

        if (!bestNodes) {
            // ?
            return;
        }

        STLOG(PRI_DEBUG, BS_NODE, NWDC08, "Voted config", (Config, bestConfig));

        auto *bsConfig = bestConfig.MutableBlobStorageConfig();
        bool changed = false;
        bool error = false;

        try {
            changed = EnrichBlobStorageConfig(bsConfig, bestConfig);
        } catch (const TExConfigError& ex) {
            STLOG(PRI_ERROR, BS_NODE, NWDC33, "Config generation failed", (Config, bestConfig), (Error, ex.what()));
            error = true;
        }

        if (!error) {
            // spin generation
            if (changed) {
                bestConfig.SetGeneration(bestConfig.GetGeneration() + 1);
            }

            bestConfig.SetFingerprint(CalculateFingerprint(bestConfig));

            STLOG(PRI_DEBUG, BS_NODE, NWDC10, "Final config", (Config, bestConfig));

            CurrentProposedStorageConfig.CopyFrom(bestConfig);

            TEvScatter task;
            auto *propose = task.MutableProposeStorageConfig();
            propose->MutableConfig()->Swap(&bestConfig);
            IssueScatterTask(true, std::move(task));
            RootState = ERootState::PROPOSE_NEW_STORAGE_CONFIG;
        }
    }

    void TDistributedConfigKeeper::ProcessProposeStorageConig(TEvGather::TProposeStorageConfig *res) {
        THashSet<ui32> successfulNodes, failedNodes;

        for (const auto& item : res->GetStatus()) {
            const ui32 nodeId = item.GetNodeId();
            switch (item.GetStatus()) {
                case TEvGather::TProposeStorageConfig::ACCEPTED:
                    successfulNodes.insert(nodeId);
                    break;

                case TEvGather::TProposeStorageConfig::HAVE_NEWER_GENERATION:
                    break;

                case TEvGather::TProposeStorageConfig::UNKNOWN:
                case TEvGather::TProposeStorageConfig::RACE:
                case TEvGather::TProposeStorageConfig::ERROR:
                    break;

                default:
                    break;
            }
        }

        failedNodes.insert(NodeIds.begin(), NodeIds.end());
        failedNodes.insert(SelfId().NodeId());
        for (const ui32 nodeId : successfulNodes) {
            failedNodes.erase(nodeId);
        }

        if (successfulNodes.size() > failedNodes.size()) {
            StorageConfig.CopyFrom(CurrentProposedStorageConfig);
            ProposedStorageConfig.reset();
            PersistConfig({});
            for (const auto& [nodeId, info] : DirectBoundNodes) {
                SendEvent(nodeId, info, std::make_unique<TEvNodeConfigReversePush>(GetRootNodeId(), &StorageConfig));
            }
            RootState = ERootState::COMMIT_CONFIG;
        }
    }

    bool TDistributedConfigKeeper::EnrichBlobStorageConfig(NKikimrConfig::TBlobStorageConfig *bsConfig,
            const NKikimrBlobStorage::TStorageConfig& config) {
        if (!bsConfig->HasServiceSet() || !bsConfig->GetServiceSet().GroupsSize()) {
            if (!bsConfig->HasAutoconfigSettings()) {
                // no autoconfig enabled at all
                return false;
            }
            const auto& settings = bsConfig->GetAutoconfigSettings();
            if (!settings.HasErasureSpecies()) {
                // automatic assembly is disabled for static group
                return false;
            }

            // build node location map
            THashMap<ui32, TNodeLocation> nodeLocations;
            for (const auto& node : config.GetAllNodes()) {
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
                Y_VERIFY(inserted);
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
                        Y_VERIFY(inserted);
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
                        Y_VERIFY(pdiskIt != pdiskMap.end());
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

            return true;
        }
        return false;
    }

    void TDistributedConfigKeeper::PrepareScatterTask(ui64 cookie, TScatterTask& task) {
        switch (task.Request.GetRequestCase()) {
            case TEvScatter::kCollectConfigs:
                break;

            case TEvScatter::kProposeStorageConfig:
                if (ProposedStorageConfigCookie) {
                    auto *status = task.Response.MutableProposeStorageConfig()->AddStatus();
                    status->SetNodeId(SelfId().NodeId());
                    status->SetStatus(TEvGather::TProposeStorageConfig::RACE);
                } else {
                    ProposedStorageConfig.emplace(task.Request.GetProposeStorageConfig().GetConfig());
                    ProposedStorageConfigCookie.emplace(cookie);
                    PersistConfig([this, cookie](TEvPrivate::TEvStorageConfigStored& msg) {
                        Y_VERIFY(ProposedStorageConfigCookie);
                        Y_VERIFY(cookie == ProposedStorageConfigCookie);
                        ProposedStorageConfigCookie.reset();

                        ui32 numOk = 0;
                        ui32 numError = 0;
                        for (const auto& [path, status] : msg.StatusPerPath) {
                            ++(status ? numOk : numError);
                        }

                        if (auto it = ScatterTasks.find(cookie); it != ScatterTasks.end()) {
                            TScatterTask& task = it->second;

                            auto *status = task.Response.MutableProposeStorageConfig()->AddStatus();
                            status->SetNodeId(SelfId().NodeId());

                            if (numOk > numError) { // stored successfully
                                status->SetStatus(TEvGather::TProposeStorageConfig::ACCEPTED);
                            } else {
                                status->SetStatus(TEvGather::TProposeStorageConfig::ERROR);
                            }

                            FinishAsyncOperation(cookie);
                        }
                    });
                    task.AsyncOperationsPending = true;
                }
                break;

            case TEvScatter::REQUEST_NOT_SET:
                break;
        }
    }

    void TDistributedConfigKeeper::PerformScatterTask(TScatterTask& task) {
        switch (task.Request.GetRequestCase()) {
            case TEvScatter::kCollectConfigs:
                Perform(task.Response.MutableCollectConfigs(), task.Request.GetCollectConfigs(), task);
                break;

            case TEvScatter::kProposeStorageConfig:
                Perform(task.Response.MutableProposeStorageConfig(), task.Request.GetProposeStorageConfig(), task);
                break;

            case TEvScatter::REQUEST_NOT_SET:
                // unexpected case
                break;
        }
    }

    void TDistributedConfigKeeper::Perform(TEvGather::TCollectConfigs *response,
            const TEvScatter::TCollectConfigs& /*request*/, TScatterTask& task) {
        THashMap<std::tuple<ui64, TString>, TEvGather::TCollectConfigs::TItem*> configs;

        auto addConfig = [&](const TEvGather::TCollectConfigs::TItem& item) {
            const auto& config = item.GetConfig();
            const auto key = std::make_tuple(config.GetGeneration(), config.GetFingerprint());
            auto& ptr = configs[key];
            if (!ptr) {
                ptr = response->AddItems();
                ptr->MutableConfig()->CopyFrom(config);
            }
            for (const auto& node : item.GetNodes()) {
                ptr->AddNodes()->CopyFrom(node);
            }
        };

        TEvGather::TCollectConfigs::TItem s;
        auto *node = s.AddNodes();
        node->SetHost(SelfHost);
        node->SetPort(SelfPort);
        node->SetNodeId(SelfId().NodeId());
        auto *cfg = s.MutableConfig();
        cfg->CopyFrom(StorageConfig);
        addConfig(s);

        for (const auto& reply : task.CollectedResponses) {
            if (reply.HasCollectConfigs()) {
                for (const auto& item : reply.GetCollectConfigs().GetItems()) {
                    addConfig(item);
                }
            }
        }
    }

    void TDistributedConfigKeeper::Perform(TEvGather::TProposeStorageConfig *response,
            const TEvScatter::TProposeStorageConfig& /*request*/, TScatterTask& task) {
        for (const auto& reply : task.CollectedResponses) {
            if (reply.HasProposeStorageConfig()) {
                const auto& item = reply.GetProposeStorageConfig();
                response->MutableStatus()->MergeFrom(item.GetStatus());
            }
        }
    }

} // NKikimr::NStorage

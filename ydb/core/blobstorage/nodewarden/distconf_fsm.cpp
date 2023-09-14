#include "distconf.h"

namespace NKikimr::NStorage {

    struct TExConfigError : yexception {};

    void TDistributedConfigKeeper::CheckRootNodeStatus() {
//        if (RootState == ERootState::INITIAL && !Binding && HasQuorum()) {
//            STLOG(PRI_DEBUG, BS_NODE, NWDC19, "Starting config collection");
//            RootState = ERootState::COLLECT_CONFIG;
//            TEvScatter task;
//            task.MutableCollectConfigs();
//            IssueScatterTask(true, std::move(task));
//        }
    }

    void TDistributedConfigKeeper::HandleErrorTimeout() {
        STLOG(PRI_DEBUG, BS_NODE, NWDC20, "Error timeout hit");
        RootState = ERootState::INITIAL;
        IssueNextBindRequest();
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
                    ProcessProposeStorageConfig(res->MutableProposeStorageConfig());
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
        THashSet<TNodeIdentifier> connectedNodes;
        for (const auto& [nodeId, node] : AllBoundNodes) {
            connectedNodes.emplace(nodeId);
        }
        return HasNodeQuorum(StorageConfig, nullptr, connectedNodes);
    }

    void TDistributedConfigKeeper::ProcessCollectConfigs(TEvGather::TCollectConfigs *res) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC31, "ProcessCollectConfigs", (RootState, RootState), (Res, *res));

        struct TConfigRecord {
            NKikimrBlobStorage::TStorageConfig Config; // full config
            THashSet<TNodeIdentifier> HavingNodeIds; // node ids having this config
        };
        THashMap<TStorageConfigMeta, TConfigRecord> configs;

        for (const auto& item : res->GetItems()) {
            if (!item.HasConfig()) {
                // incorrect reply
                continue;
            }

            const auto& config = item.GetConfig();
            TStorageConfigMeta meta(config);
            const auto [it, inserted] = configs.try_emplace(std::move(meta));
            TConfigRecord& record = it->second;
            if (inserted) {
                record.Config.CopyFrom(config);
            }
            for (const auto& nodeId : item.GetNodeIds()) {
                record.HavingNodeIds.emplace(nodeId);
            }
        }

        for (auto it = configs.begin(); it != configs.end(); ) {
            TConfigRecord& record = it->second;
            if (HasNodeQuorum(record.Config, nullptr, record.HavingNodeIds)) {
                ++it;
            } else {
                configs.erase(it++);
            }
        }

        if (configs.empty()) {
            STLOG(PRI_INFO, BS_NODE, NWDC38, "No possible quorum for CollectConfigs");
            RootState = ERootState::ERROR_TIMEOUT;
            TActivationContext::Schedule(ErrorTimeout, new IEventHandle(TEvPrivate::EvErrorTimeout, 0, SelfId(), {}, nullptr, 0));
            return;
        }

        // leave configs with the maximum possible generation
        ui64 maxGeneration = 0;
        for (const auto& [meta, record] : configs) {
            maxGeneration = Max(maxGeneration, meta.GetGeneration());
        }
        for (auto it = configs.begin(); it != configs.end(); ++it) {
            const TStorageConfigMeta& meta = it->first;
            if (meta.GetGeneration() == maxGeneration) {
                ++it;
            } else {
                configs.erase(it++);
            }
        }

        // ensure there was no split-brain for these nodes
        if (configs.size() != 1) {
            STLOG(PRI_ERROR, BS_NODE, NWDC39, "Different variations of collected config detected");
            RootState = ERootState::ERROR_TIMEOUT;
            TActivationContext::Schedule(ErrorTimeout, new IEventHandle(TEvPrivate::EvErrorTimeout, 0, SelfId(), {}, nullptr, 0));
            return;
        }

        auto it = configs.begin();
        auto& bestConfig = it->second.Config;

        bool canParticipate = false;
        for (const auto& node : bestConfig.GetAllNodes()) {
            if (SelfNode == TNodeIdentifier(node)) {
                canParticipate = true;
                break;
            }
        }
        if (!canParticipate) {
            STLOG(PRI_ERROR, BS_NODE, NWDC40, "Current node can't be coordinating one in voted config");
            RootState = ERootState::INITIAL;
            IssueNextBindRequest();
            // TODO: request direct bound nodes to re-decide
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
        } else {
            RootState = ERootState::ERROR_TIMEOUT;
            TActivationContext::Schedule(ErrorTimeout, new IEventHandle(TEvPrivate::EvErrorTimeout, 0, SelfId(), {}, nullptr, 0));
        }
    }

    void TDistributedConfigKeeper::ProcessProposeStorageConfig(TEvGather::TProposeStorageConfig *res) {
        THashSet<TNodeIdentifier> among;
        THashSet<TNodeIdentifier> successful;

        for (const auto& node : CurrentProposedStorageConfig.GetAllNodes()) {
            among.emplace(node);
        }

        for (const auto& item : res->GetStatus()) {
            TNodeIdentifier nodeId(item.GetNodeId());
            switch (item.GetStatus()) {
                case TEvGather::TProposeStorageConfig::ACCEPTED:
                    successful.insert(std::move(nodeId));
                    break;

                case TEvGather::TProposeStorageConfig::HAVE_NEWER_GENERATION:
                    break;

                case TEvGather::TProposeStorageConfig::UNKNOWN:
                case TEvGather::TProposeStorageConfig::RACE:
                case TEvGather::TProposeStorageConfig::ERROR:
                    break;

                case TEvGather::TProposeStorageConfig::NO_STORAGE:
                    among.erase(nodeId);
                    break;

                default:
                    break;
            }
        }

        if (HasNodeQuorum(CurrentProposedStorageConfig, &among, successful)) {
            TEvScatter task;
            auto *commit = task.MutableCommitStorageConfig();
            TStorageConfigMeta meta(CurrentProposedStorageConfig);
            commit->MutableMeta()->CopyFrom(meta);
            IssueScatterTask(true, std::move(task));
            RootState = ERootState::COMMIT_CONFIG;
        } else {
            CurrentProposedStorageConfig.Clear();
            STLOG(PRI_DEBUG, BS_NODE, NWDC04, "No quorum for ProposedStorageConfig, restarting");
            RootState = ERootState::ERROR_TIMEOUT;
            TActivationContext::Schedule(ErrorTimeout, new IEventHandle(TEvPrivate::EvErrorTimeout, 0, SelfId(), {}, nullptr, 0));
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
                    SelfNode.Serialize(status->MutableNodeId());
                    status->SetStatus(TEvGather::TProposeStorageConfig::RACE);
                } else {
                    ProposedStorageConfig.emplace(task.Request.GetProposeStorageConfig().GetConfig());
                    ProposedStorageConfigCookie.emplace(cookie);
                    PersistConfig([this, cookie](TEvPrivate::TEvStorageConfigStored& msg) {
                        Y_VERIFY(ProposedStorageConfigCookie);
                        Y_VERIFY(cookie == ProposedStorageConfigCookie);
                        ProposedStorageConfigCookie.reset();

                        if (auto it = ScatterTasks.find(cookie); it != ScatterTasks.end()) {
                            TScatterTask& task = it->second;

                            auto *status = task.Response.MutableProposeStorageConfig()->AddStatus();
                            SelfNode.Serialize(status->MutableNodeId());

                            auto pdiskMap = MakePDiskMap(*ProposedStorageConfig);
                            ui32 numOk = 0;
                            ui32 numError = 0;
                            for (const auto& [path, ok] : msg.StatusPerPath) {
                                if (ok) {
                                    if (const auto it = pdiskMap.find(path); it != pdiskMap.end()) {
                                        status->AddSuccessfulPDiskIds(it->second);
                                    }
                                    ++numOk;
                                } else {
                                    ++numError;
                                }
                            }

                            status->SetStatus(numOk + numError == 0 ? TEvGather::TProposeStorageConfig::NO_STORAGE :
                                numOk > numError ? TEvGather::TProposeStorageConfig::ACCEPTED :
                                TEvGather::TProposeStorageConfig::ERROR);

                            FinishAsyncOperation(cookie);
                        }
                    });
                    task.AsyncOperationsPending = true;
                }
                break;

            case TEvScatter::kCommitStorageConfig: {
                auto error = [&](const TString& reason) {
                    auto *status = task.Response.MutableCommitStorageConfig()->AddStatus();
                    SelfNode.Serialize(status->MutableNodeId());
                    status->SetReason(reason);
                };
                if (ProposedStorageConfigCookie) {
                    error("proposition is still in progress");
                } else if (CommitStorageConfigCookie) {
                    error("commit is still in progress");
                } else if (!ProposedStorageConfig) {
                    error("no proposed config found");
                } else if (const TStorageConfigMeta meta(task.Request.GetCommitStorageConfig().GetMeta()); meta != *ProposedStorageConfig) {
                    error("proposed config mismatch");
                } else {
                    CommitStorageConfigCookie.emplace(cookie);
                    ProposedStorageConfig->Swap(&StorageConfig);
                    ProposedStorageConfig.reset();
                    PersistConfig([this, cookie](TEvPrivate::TEvStorageConfigStored& msg) {
                        Y_VERIFY(CommitStorageConfigCookie);
                        Y_VERIFY(cookie == CommitStorageConfigCookie);
                        CommitStorageConfigCookie.reset();

                        if (const auto it = ScatterTasks.find(cookie); it != ScatterTasks.end()) {
                            TScatterTask& task = it->second;
                            auto *status = task.Response.MutableCommitStorageConfig()->AddStatus();
                            SelfNode.Serialize(status->MutableNodeId());

                            ui32 numOk = 0;
                            ui32 numError = 0;
                            for (const auto& [path, status] : msg.StatusPerPath) {
                                ++(status ? numOk : numError);
                            }
                            if (numOk > numError) {
                                status->SetSuccess(true);
                            } else {
                                status->SetReason("no PDisk quorum");
                            }

                            FinishAsyncOperation(cookie);
                        }
                    });
                    task.AsyncOperationsPending = true;
                }
                break;
            }

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

            case TEvScatter::kCommitStorageConfig:
                Perform(task.Response.MutableCommitStorageConfig(), task.Request.GetCommitStorageConfig(), task);
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
            for (const auto& node : item.GetNodeIds()) {
                ptr->AddNodeIds()->CopyFrom(node);
            }
        };

        TEvGather::TCollectConfigs::TItem s;
        SelfNode.Serialize(s.AddNodeIds());
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
                response->MutableStatus()->MergeFrom(reply.GetProposeStorageConfig().GetStatus());
            }
        }
    }

    void TDistributedConfigKeeper::Perform(TEvGather::TCommitStorageConfig *response,
            const TEvScatter::TCommitStorageConfig& /*request*/, TScatterTask& task) {
        for (const auto& reply : task.CollectedResponses) {
            if (reply.HasCommitStorageConfig()) {
                response->MutableStatus()->MergeFrom(reply.GetCommitStorageConfig().GetStatus());
            }
        }
    }

    THashMap<TString, ui32> TDistributedConfigKeeper::MakePDiskMap(const NKikimrBlobStorage::TStorageConfig& config) {
        if (!config.HasBlobStorageConfig()) {
            return {};
        }
        const auto& bsConfig = config.GetBlobStorageConfig();

        if (!bsConfig.HasServiceSet()) {
            return {};
        }
        const auto& serviceSet = bsConfig.GetServiceSet();

        THashMap<TString, ui32> res;
        for (const auto& pdisk : serviceSet.GetPDisks()) {
            if (pdisk.GetNodeID() == SelfId().NodeId()) {
                res.emplace(pdisk.GetPath(), pdisk.GetPDiskID());
            }
        }
        return res;
    }

    bool TDistributedConfigKeeper::HasNodeQuorum(const NKikimrBlobStorage::TStorageConfig& config,
            const THashSet<TNodeIdentifier> *among, const THashSet<TNodeIdentifier>& successful) const {
        ui32 numOk = 0;
        ui32 numError = 0;

        THashSet<TNodeIdentifier> nodesInConfig;
        for (const auto& node : config.GetAllNodes()) {
            const TNodeIdentifier ni(node);
            if (among && !among->contains(ni)) { // skip this node, not interested in its status
                continue;
            }
            ++(successful.contains(ni) ? numOk : numError);
        }

        return numOk > numError;
    }

} // NKikimr::NStorage

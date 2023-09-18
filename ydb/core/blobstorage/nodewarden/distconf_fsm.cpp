#include "distconf.h"

namespace NKikimr::NStorage {

    struct TExConfigError : yexception {};

    void TDistributedConfigKeeper::CheckRootNodeStatus() {
        if (RootState == ERootState::INITIAL && !Binding && HasQuorum()) {
            STLOG(PRI_DEBUG, BS_NODE, NWDC19, "Starting config collection");
            RootState = ERootState::COLLECT_CONFIG;
            TEvScatter task;
            task.MutableCollectConfigs();
            IssueScatterTask(true, std::move(task));
        }
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

            default:
                break;
        }
    }

    bool TDistributedConfigKeeper::HasQuorum() const {
        auto generateConnected = [&](auto&& callback) {
            for (const auto& [nodeId, node] : AllBoundNodes) {
                callback(nodeId);
            }
        };
        return StorageConfig && HasNodeQuorum(*StorageConfig, generateConnected);
    }

    void TDistributedConfigKeeper::ProcessCollectConfigs(TEvGather::TCollectConfigs *res) {
        auto generateSuccessful = [&](auto&& callback) {
            for (const auto& item : res->GetNodes()) {
                for (const auto& node : item.GetNodeIds()) {
                    callback(node);
                }
            }
        };
        const bool nodeQuorum = HasNodeQuorum(*StorageConfig, generateSuccessful);
        STLOG(PRI_DEBUG, BS_NODE, NWDC31, "ProcessCollectConfigs", (RootState, RootState), (NodeQuorum, nodeQuorum), (Res, *res));
        if (!nodeQuorum) {
            RootState = ERootState::ERROR_TIMEOUT;
            TActivationContext::Schedule(ErrorTimeout, new IEventHandle(TEvPrivate::EvErrorTimeout, 0, SelfId(), {}, nullptr, 0));
            return;
        }

        // TODO: validate self-assembly UUID

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Pick base config quorum (if we have one)

        struct TBaseConfigInfo {
            NKikimrBlobStorage::TStorageConfig Config;
            THashSet<TNodeIdentifier> HavingNodeIds;
        };
        THashMap<TStorageConfigMeta, TBaseConfigInfo> baseConfigs;
        for (const auto& node : res->GetNodes()) {
            if (node.HasBaseConfig()) {
                const auto& baseConfig = node.GetBaseConfig();
                const auto [it, inserted] = baseConfigs.try_emplace(baseConfig);
                TBaseConfigInfo& r = it->second;
                if (inserted) {
                    r.Config.CopyFrom(baseConfig);
                }
                for (const auto& nodeId : node.GetNodeIds()) {
                    r.HavingNodeIds.emplace(nodeId);
                }
            }
        }
        for (auto it = baseConfigs.begin(); it != baseConfigs.end(); ) { // filter out configs not having node quorum
            TBaseConfigInfo& r = it->second;
            auto generateNodeIds = [&](auto&& callback) {
                for (const auto& nodeId : r.HavingNodeIds) {
                    callback(nodeId);
                }
            };
            if (HasNodeQuorum(r.Config, generateNodeIds)) {
                ++it;
            } else {
                baseConfigs.erase(it++);
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Create quorums for committed and proposed configurations

        struct TDiskConfigInfo {
            NKikimrBlobStorage::TStorageConfig Config;
            THashSet<std::tuple<TNodeIdentifier, TString>> HavingDisks;
        };
        THashMap<TStorageConfigMeta, TDiskConfigInfo> committedConfigs;
        THashMap<TStorageConfigMeta, TDiskConfigInfo> proposedConfigs;
        for (auto& [field, set] : {
                    std::tie(res->GetCommittedConfigs(), committedConfigs),
                    std::tie(res->GetProposedConfigs(), proposedConfigs)
                }) {
            for (const TEvGather::TCollectConfigs::TPersistentConfig& config : field) {
                const auto [it, inserted] = set.try_emplace(config.GetConfig());
                TDiskConfigInfo& r = it->second;
                if (inserted) {
                    r.Config.CopyFrom(config.GetConfig());
                }
                for (const auto& disk : config.GetDisks()) {
                    r.HavingDisks.emplace(disk.GetNodeId(), disk.GetPath());
                }
            }
        }
        for (auto& set : {&committedConfigs, &proposedConfigs}) {
            for (auto it = set->begin(); it != set->end(); ) {
                TDiskConfigInfo& r = it->second;

                auto generateSuccessful = [&](auto&& callback) {
                    for (const auto& [node, path] : r.HavingDisks) {
                        callback(node, path);
                    }
                };

                const bool quorum = HasDiskQuorum(r.Config, generateSuccessful) &&
                    (!r.Config.HasPrevConfig() || HasDiskQuorum(r.Config.GetPrevConfig(), generateSuccessful));

                if (quorum) {
                    ++it;
                } else {
                    set->erase(it++);
                }
            }
        }

        if (baseConfigs.size() > 1 || committedConfigs.size() > 1 || proposedConfigs.size() > 1) {
            STLOG(PRI_CRIT, BS_NODE, NWDC08, "Multiple nonintersecting node sets have quorum",
                (BaseConfigs.size, baseConfigs.size()), (CommittedConfigs.size, committedConfigs.size()),
                (ProposedConfigs.size, proposedConfigs.size()));
            Y_VERIFY_DEBUG(false);
            Halt();
            return;
        }

        NKikimrBlobStorage::TStorageConfig *baseConfig = baseConfigs.empty() ? nullptr : &baseConfigs.begin()->second.Config;
        NKikimrBlobStorage::TStorageConfig *committedConfig = committedConfigs.empty() ? nullptr : &committedConfigs.begin()->second.Config;
        NKikimrBlobStorage::TStorageConfig *proposedConfig = proposedConfigs.empty() ? nullptr : &proposedConfigs.begin()->second.Config;

        if (committedConfig && ApplyStorageConfig(*committedConfig)) { // we have a committed config, apply and spread it
            for (const auto& [nodeId, info] : DirectBoundNodes) {
                SendEvent(nodeId, info, std::make_unique<TEvNodeConfigReversePush>(GetRootNodeId(), &StorageConfig.value()));
            }
        }

        NKikimrBlobStorage::TStorageConfig *configToPropose = nullptr;
        std::optional<NKikimrBlobStorage::TStorageConfig> propositionBase;

        if (proposedConfig) { // we have proposition in progress, resume
            configToPropose = proposedConfig;
        } else if (committedConfig) { // we have committed config, check if we need to update it
            propositionBase.emplace(*committedConfig);
            if (UpdateConfig(committedConfig)) {
                configToPropose = committedConfig;
            }
        } else if (baseConfig && !baseConfig->GetGeneration()) { // we have no committed storage config, but we can create one
            propositionBase.emplace(*baseConfig);
            if (GenerateFirstConfig(baseConfig)) {
                configToPropose = baseConfig;
            }
        }

        if (configToPropose) {
            if (propositionBase) {
                configToPropose->SetGeneration(configToPropose->GetGeneration() + 1);
                configToPropose->MutablePrevConfig()->CopyFrom(*propositionBase);
            }
            UpdateFingerprint(configToPropose);

            TEvScatter task;
            auto *propose = task.MutableProposeStorageConfig();
            CurrentProposedStorageConfig.CopyFrom(*configToPropose);
            propose->MutableConfig()->Swap(configToPropose);
            IssueScatterTask(true, std::move(task));
            RootState = ERootState::PROPOSE_NEW_STORAGE_CONFIG;
        } else {
            // TODO: nothing to do?
        }
    }

    void TDistributedConfigKeeper::ProcessProposeStorageConfig(TEvGather::TProposeStorageConfig *res) {
        auto generateSuccessful = [&](auto&& callback) {
            for (const auto& item : res->GetStatus()) {
                const TNodeIdentifier node(item.GetNodeId());
                for (const TString& path : item.GetSuccessfulDrives()) {
                    callback(node, path);
                }
            }
        };

        if (HasDiskQuorum(CurrentProposedStorageConfig, generateSuccessful) &&
                HasDiskQuorum(CurrentProposedStorageConfig.GetPrevConfig(), generateSuccessful)) {
            // apply configuration and spread it
            ApplyStorageConfig(CurrentProposedStorageConfig);
            for (const auto& [nodeId, info] : DirectBoundNodes) {
                SendEvent(nodeId, info, std::make_unique<TEvNodeConfigReversePush>(GetRootNodeId(), &StorageConfig.value()));
            }
            CurrentProposedStorageConfig.Clear();
        } else {
            CurrentProposedStorageConfig.Clear();
            STLOG(PRI_DEBUG, BS_NODE, NWDC04, "No quorum for ProposedStorageConfig, restarting");
            RootState = ERootState::ERROR_TIMEOUT;
            TActivationContext::Schedule(ErrorTimeout, new IEventHandle(TEvPrivate::EvErrorTimeout, 0, SelfId(), {}, nullptr, 0));
        }
    }

    bool TDistributedConfigKeeper::GenerateFirstConfig(NKikimrBlobStorage::TStorageConfig *config) {
        bool changes = false;

        if (config->HasBlobStorageConfig()) {
            const auto& bsConfig = config->GetBlobStorageConfig();
            const bool noStaticGroup = !bsConfig.HasServiceSet() || !bsConfig.GetServiceSet().GroupsSize();
            if (noStaticGroup && bsConfig.HasAutoconfigSettings() && bsConfig.GetAutoconfigSettings().HasErasureSpecies()) {
                try {
                    AllocateStaticGroup(config);
                    changes = true;
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
    }

    bool TDistributedConfigKeeper::UpdateConfig(NKikimrBlobStorage::TStorageConfig *config) {
        (void)config;
        return false;
    }

    void TDistributedConfigKeeper::PrepareScatterTask(ui64 cookie, TScatterTask& task) {
        switch (task.Request.GetRequestCase()) {
            case TEvScatter::kCollectConfigs: {
                std::vector<TString> drives;
                EnumerateConfigDrives(*StorageConfig, 0, [&](const auto& /*node*/, const auto& drive) {
                    drives.push_back(drive.GetPath());
                });
                if (ProposedStorageConfig) {
                    EnumerateConfigDrives(*ProposedStorageConfig, 0, [&](const auto& /*node*/, const auto& drive) {
                        drives.push_back(drive.GetPath());
                    });
                }
                std::sort(drives.begin(), drives.end());
                drives.erase(std::unique(drives.begin(), drives.end()), drives.end());
                auto query = std::bind(&TThis::ReadConfig, TActivationContext::ActorSystem(), SelfId(), drives, Cfg, cookie);
                Send(MakeIoDispatcherActorId(), new TEvInvokeQuery(std::move(query)));
                task.AsyncOperationsPending = true;
                break;
            }

            case TEvScatter::kProposeStorageConfig:
                if (ProposedStorageConfigCookie) {
                    auto *status = task.Response.MutableProposeStorageConfig()->AddStatus();
                    SelfNode.Serialize(status->MutableNodeId());
                    status->SetStatus(TEvGather::TProposeStorageConfig::RACE);
                } else {
                    ProposedStorageConfigCookie.emplace(cookie);
                    ProposedStorageConfig.emplace(task.Request.GetProposeStorageConfig().GetConfig());

                    PersistConfig([this, cookie](TEvPrivate::TEvStorageConfigStored& msg) {
                        Y_VERIFY(ProposedStorageConfigCookie);
                        Y_VERIFY(cookie == ProposedStorageConfigCookie);
                        ProposedStorageConfigCookie.reset();

                        if (auto it = ScatterTasks.find(cookie); it != ScatterTasks.end()) {
                            TScatterTask& task = it->second;

                            auto *status = task.Response.MutableProposeStorageConfig()->AddStatus();
                            SelfNode.Serialize(status->MutableNodeId());
                            status->SetStatus(TEvGather::TProposeStorageConfig::ACCEPTED);
                            for (const auto& [path, ok] : msg.StatusPerPath) {
                                if (ok) {
                                    status->AddSuccessfulDrives(path);
                                }
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
        THashMap<TStorageConfigMeta, TEvGather::TCollectConfigs::TNode*> baseConfigs;

        auto addBaseConfig = [&](const TEvGather::TCollectConfigs::TNode& item) {
            const auto& config = item.GetBaseConfig();
            auto& ptr = baseConfigs[config];
            if (!ptr) {
                ptr = response->AddNodes();
                ptr->MutableBaseConfig()->CopyFrom(config);
            }
            for (const auto& node : item.GetNodeIds()) {
                ptr->AddNodeIds()->CopyFrom(node);
            }
        };

        auto addPerDiskConfig = [&](const TEvGather::TCollectConfigs::TPersistentConfig& item, auto addFunc, auto& set) {
            const auto& config = item.GetConfig();
            auto& ptr = set[config];
            if (!ptr) {
                ptr = (response->*addFunc)();
                ptr->MutableConfig()->CopyFrom(config);
            }
            for (const auto& disk : item.GetDisks()) {
                ptr->AddDisks()->CopyFrom(disk);
            }
        };

        TEvGather::TCollectConfigs::TNode s;
        SelfNode.Serialize(s.AddNodeIds());
        auto *cfg = s.MutableBaseConfig();
        cfg->CopyFrom(BaseConfig);
        addBaseConfig(s);

        THashMap<TStorageConfigMeta, TEvGather::TCollectConfigs::TPersistentConfig*> committedConfigs;
        THashMap<TStorageConfigMeta, TEvGather::TCollectConfigs::TPersistentConfig*> proposedConfigs;

        for (const auto& reply : task.CollectedResponses) {
            if (reply.HasCollectConfigs()) {
                const auto& cc = reply.GetCollectConfigs();
                for (const auto& item : cc.GetNodes()) {
                    addBaseConfig(item);
                }
                for (const auto& item : cc.GetCommittedConfigs()) {
                    addPerDiskConfig(item, &TEvGather::TCollectConfigs::AddCommittedConfigs, committedConfigs);
                }
                for (const auto& item : cc.GetProposedConfigs()) {
                    addPerDiskConfig(item, &TEvGather::TCollectConfigs::AddProposedConfigs, proposedConfigs);
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

} // NKikimr::NStorage

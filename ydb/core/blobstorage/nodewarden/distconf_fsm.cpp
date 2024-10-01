#include "distconf.h"

namespace NKikimr::NStorage {

    void TDistributedConfigKeeper::CheckRootNodeStatus() {
        Y_VERIFY_S(Binding ? (RootState == ERootState::INITIAL || RootState == ERootState::ERROR_TIMEOUT) && !Scepter :
            RootState == ERootState::INITIAL || RootState == ERootState::ERROR_TIMEOUT ? !Scepter :
            static_cast<bool>(Scepter), "Binding# " << (Binding ? Binding->ToString() : "<null>")
            << " RootState# " << RootState << " Scepter# " << (Scepter ? ToString(Scepter->Id) : "<null>"));

        if (Binding) { // can't become root node
            return;
        }

        const bool hasQuorum = HasQuorum();

        if (RootState == ERootState::INITIAL && hasQuorum) { // becoming root node
            Y_ABORT_UNLESS(!Scepter);
            Scepter = std::make_shared<TScepter>();
            BecomeRoot();
        } else if (Scepter && !hasQuorum) { // unbecoming root node -- lost quorum
            SwitchToError("quorum lost");
        }
    }

    void TDistributedConfigKeeper::BecomeRoot() {
        auto makeAllBoundNodes = [&] {
            TStringStream s;
            const char *sep = "{";
            for (const auto& [nodeId, _] : AllBoundNodes) {
                s << std::exchange(sep, " ") << nodeId;
            }
            s << '}';
            return s.Str();
        };
        STLOG(PRI_DEBUG, BS_NODE, NWDC19, "Starting config collection", (Scepter, Scepter->Id),
            (AllBoundNodes, makeAllBoundNodes()));
        RootState = ERootState::IN_PROGRESS;
        TEvScatter task;
        task.SetTaskId(RandomNumber<ui64>());
        task.MutableCollectConfigs();
        IssueScatterTask(TActorId(), std::move(task));
    }

    void TDistributedConfigKeeper::UnbecomeRoot() {
    }

    void TDistributedConfigKeeper::SwitchToError(const TString& reason) {
        STLOG(PRI_ERROR, BS_NODE, NWDC38, "SwitchToError", (RootState, RootState), (Reason, reason));
        if (Scepter) {
            UnbecomeRoot();
        }
        Scepter.reset();
        RootState = ERootState::ERROR_TIMEOUT;
        ErrorReason = reason;
        CurrentProposedStorageConfig.reset();
        ApplyConfigUpdateToDynamicNodes(true);
        AbortAllScatterTasks(std::nullopt);
        const TDuration timeout = TDuration::FromValue(ErrorTimeout.GetValue() * (25 + RandomNumber(51u)) / 50);
        TActivationContext::Schedule(timeout, new IEventHandle(TEvPrivate::EvErrorTimeout, 0, SelfId(), {}, nullptr, 0));
    }

    void TDistributedConfigKeeper::HandleErrorTimeout() {
        STLOG(PRI_DEBUG, BS_NODE, NWDC20, "Error timeout hit");
        Y_ABORT_UNLESS(!Scepter);
        RootState = ERootState::INITIAL;
        ErrorReason = {};
        CheckRootNodeStatus();
    }

    void TDistributedConfigKeeper::ProcessGather(TEvGather *res) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC27, "ProcessGather", (RootState, RootState), (Res, *res));

        if (!res) {
            return SwitchToError("leadership lost while executing query");
        }

        switch (res->GetResponseCase()) {
            case TEvGather::kCollectConfigs:
                return ProcessCollectConfigs(res->MutableCollectConfigs());

            case TEvGather::kProposeStorageConfig:
                if (auto error = ProcessProposeStorageConfig(res->MutableProposeStorageConfig())) {
                    SwitchToError(*error);
                } else {
                    RootState = ERootState::RELAX;
                }
                return;

            case TEvGather::RESPONSE_NOT_SET:
                return SwitchToError("response not set");
        }

        SwitchToError("incorrect response from peer");
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

        auto generateSuccessfulDisks = [&](auto&& callback) {
            auto invoke = [&](const auto& disk) {
                callback(TNodeIdentifier(disk.GetNodeId()), disk.GetPath(),
                    disk.HasGuid() ? std::make_optional(disk.GetGuid()) : std::nullopt);
            };
            for (const auto& item : res->GetCommittedConfigs()) {
                for (const auto& disk : item.GetDisks()) {
                    invoke(disk);
                }
            }
            for (const auto& item : res->GetProposedConfigs()) {
                for (const auto& disk : item.GetDisks()) {
                    invoke(disk);
                }
            }
            for (const auto& disk : res->GetNoMetadata()) {
                invoke(disk);
            }
        };
        const bool configQuorum = HasConfigQuorum(*StorageConfig, generateSuccessfulDisks, *Cfg, false);

        STLOG(PRI_DEBUG, BS_NODE, NWDC31, "ProcessCollectConfigs", (RootState, RootState), (NodeQuorum, nodeQuorum),
            (ConfigQuorum, configQuorum), (Res, *res));
        if (!nodeQuorum || !configQuorum) {
            return SwitchToError("no quorum for CollectConfigs");
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
                if (!CheckFingerprint(baseConfig)) {
                    STLOG(PRI_ERROR, BS_NODE, NWDC57, "BaseConfig fingerprint error", (NodeRecord, node));
                    Y_DEBUG_ABORT("BaseConfig fingerprint error");
                    continue;
                }
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
        if (baseConfigs.size() > 1) {
            STLOG(PRI_CRIT, BS_NODE, NWDC08, "Multiple nonintersecting node sets have quorum of BaseConfig",
                (BaseConfigs.size, baseConfigs.size()));
            Y_DEBUG_ABORT("Multiple nonintersecting node sets have quorum of BaseConfig");
            return Halt();
        }
        NKikimrBlobStorage::TStorageConfig *baseConfig = nullptr;
        for (auto& [meta, info] : baseConfigs) {
            baseConfig = &info.Config;
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Create quorums for committed and proposed configurations

        struct TDiskConfigInfo {
            NKikimrBlobStorage::TStorageConfig Config;
            THashSet<std::tuple<TNodeIdentifier, TString, std::optional<ui64>>> HavingDisks;
        };
        THashMap<TStorageConfigMeta, TDiskConfigInfo> persistentConfigs; // all of them in one bucket: proposed, committed
        ui64 maxSeenGeneration = StorageConfig ? StorageConfig->GetGeneration() : 0;
        for (auto&& [field, isCommitted] : {
                    std::make_tuple(&res->GetCommittedConfigs(), true),
                    std::make_tuple(&res->GetProposedConfigs(), false),
                }) {
            for (const TEvGather::TCollectConfigs::TPersistentConfig& item : *field) {
                const NKikimrBlobStorage::TStorageConfig& config = item.GetConfig();
                if (!CheckFingerprint(config)) {
                    STLOG(PRI_ERROR, BS_NODE, NWDC58, "PersistentConfig fingerprint error", (ConfigRecord, config));
                    Y_DEBUG_ABORT("PersistentConfig fingerprint error");
                    continue;
                }
                if (isCommitted) {
                    maxSeenGeneration = Max(maxSeenGeneration, config.GetGeneration());
                }
                const auto [it, inserted] = persistentConfigs.try_emplace(config);
                TDiskConfigInfo& r = it->second;
                if (inserted) {
                    r.Config.CopyFrom(config);
                }
                for (const auto& disk : item.GetDisks()) {
                    r.HavingDisks.emplace(disk.GetNodeId(), disk.GetPath(), disk.HasGuid() ? std::make_optional(disk.GetGuid()) : std::nullopt);
                }
            }
        }
        std::map<ui64, std::vector<NKikimrBlobStorage::TStorageConfig*>> candidates;
        for (auto it = persistentConfigs.begin(); it != persistentConfigs.end(); ) {
            TDiskConfigInfo& r = it->second;
            auto generateSuccessful = [&](auto&& callback) {
                for (const auto& [node, path, guid] : r.HavingDisks) {
                    callback(node, path, guid);
                }
            };
            if (HasConfigQuorum(r.Config, generateSuccessful, *Cfg, false)) {
                const ui64 generation = r.Config.GetGeneration();
                candidates[generation].push_back(&r.Config);
                ++it;
            } else {
                persistentConfigs.erase(it++);
            }
        }

        // find the configuration that we can call 'committed' (persisted in any way -- either in committed, or in
        // proposed, but having a quorum)
        NKikimrBlobStorage::TStorageConfig *persistedConfig = nullptr;
        for (auto& [generation, configs] : candidates) {
            if (configs.size() > 1) {
                STLOG(PRI_CRIT, BS_NODE, NWDC37, "Multiple nonintersecting node sets have quorum of persistent config",
                    (Generation, generation), (Configs, configs));
                Y_DEBUG_ABORT("Multiple nonintersecting node sets have quorum of persistent config");
                return Halt();
            }
            Y_ABORT_UNLESS(configs.size() == 1);
            persistedConfig = configs.front();
        }
        if (maxSeenGeneration && (!persistedConfig || persistedConfig->GetGeneration() < maxSeenGeneration)) {
            return SwitchToError("couldn't obtain quorum for configuration that was seen in effect");
        }

        // let's try to find possibly proposed config, but without a quorum, and try to reconstruct it
        const NKikimrBlobStorage::TStorageConfig *proposedConfig = nullptr;
        bool noSingleProposedConfig = false;
        for (const TEvGather::TCollectConfigs::TPersistentConfig& item : res->GetProposedConfigs()) {
            if (const NKikimrBlobStorage::TStorageConfig& config = item.GetConfig(); CheckFingerprint(config)) {
                if (persistedConfig) {
                    if (config.GetGeneration() <= persistedConfig->GetGeneration()) {
                        continue; // some obsolete record
                    } else if (persistedConfig->GetGeneration() + 1 < config.GetGeneration()) {
                        STLOG(PRI_CRIT, BS_NODE, NWDC62, "persistently proposed config has too big generation",
                            (PersistentConfig, *persistedConfig), (ProposedConfig, config));
                        Y_DEBUG_ABORT("persistently proposed config has too big generation");
                        return Halt();
                    }
                }
                if (proposedConfig && (proposedConfig->GetGeneration() != config.GetGeneration() ||
                        proposedConfig->GetFingerprint() != config.GetFingerprint())) {
                    noSingleProposedConfig = true;
                } else {
                    proposedConfig = &config;
                }
            }
        }
        if (noSingleProposedConfig) {
            proposedConfig = nullptr;
        } else if (proposedConfig && persistedConfig) {
            Y_ABORT_UNLESS(persistedConfig->GetGeneration() + 1 == proposedConfig->GetGeneration());
        }

        if (persistedConfig) { // we have a committed config, apply and spread it
            ApplyStorageConfig(*persistedConfig);
            FanOutReversePush(&StorageConfig.value(), true /*recurseConfigUpdate*/);
        }

        NKikimrBlobStorage::TStorageConfig tempConfig;
        NKikimrBlobStorage::TStorageConfig *configToPropose = nullptr;
        std::optional<NKikimrBlobStorage::TStorageConfig> propositionBase;

        bool canPropose = false;
        if (StorageConfig->HasBlobStorageConfig()) {
            if (const auto& bsConfig = StorageConfig->GetBlobStorageConfig(); bsConfig.HasAutoconfigSettings()) {
                if (const auto& settings = bsConfig.GetAutoconfigSettings(); settings.HasDefineBox()) {
                    canPropose = true;
                }
            }
        }

        STLOG(PRI_DEBUG, BS_NODE, NWDC59, "ProcessCollectConfigs", (BaseConfig, baseConfig),
            (PersistedConfig, persistedConfig), (ProposedConfig, proposedConfig), (CanPropose, canPropose));

        if (!canPropose) {
            // we can't propose any configuration here, just ignore
        } else if (proposedConfig) { // we have proposition in progress, resume
            tempConfig.CopyFrom(*proposedConfig);
            configToPropose = &tempConfig;
        } else if (persistedConfig) { // we have committed config, check if we need to update it
            propositionBase.emplace(*persistedConfig);
            if (UpdateConfig(persistedConfig)) {
                configToPropose = persistedConfig;
            }
        } else if (baseConfig && !baseConfig->GetGeneration()) { // we have no committed storage config, but we can create one
            propositionBase.emplace(*baseConfig);
            if (GenerateFirstConfig(baseConfig)) {
                configToPropose = baseConfig;
            }
        }

        if (configToPropose) {
            if (propositionBase) {
                configToPropose->SetGeneration(propositionBase->GetGeneration() + 1);
                configToPropose->MutablePrevConfig()->CopyFrom(*propositionBase);
                configToPropose->MutablePrevConfig()->ClearPrevConfig();
            }
            UpdateFingerprint(configToPropose);

            const bool error = StorageConfig && configToPropose->GetGeneration() <= StorageConfig->GetGeneration();

            STLOG(error ? PRI_ERROR : PRI_INFO, BS_NODE, NWDC60, "ProcessCollectConfigs proposing config",
                (ConfigToPropose, *configToPropose),
                (PropositionBase, propositionBase),
                (StorageConfig, StorageConfig),
                (BaseConfig, static_cast<bool>(baseConfig)),
                (PersistedConfig, static_cast<bool>(persistedConfig)),
                (ProposedConfig, static_cast<bool>(proposedConfig)),
                (Error, error));

            if (error) {
                Y_DEBUG_ABORT("incorrect config proposition");
                return SwitchToError("incorrect config proposition");
            }

            if (propositionBase) {
                if (auto error = ValidateConfig(*propositionBase)) {
                    return SwitchToError(TStringBuilder() << "failed to propose configuration, base config contains errors: " << *error);
                }
                if (auto error = ValidateConfigUpdate(*propositionBase, *configToPropose)) {
                    Y_FAIL_S("incorrect config proposed: " << *error);
                }
            } else {
                if (auto error = ValidateConfig(*configToPropose)) {
                    Y_FAIL_S("incorrect config proposed: " << *error);
                }
            }

            TEvScatter task;
            task.SetTaskId(RandomNumber<ui64>());
            auto *propose = task.MutableProposeStorageConfig();
            Y_ABORT_UNLESS(!CurrentProposedStorageConfig);
            CurrentProposedStorageConfig.emplace(*configToPropose);
            propose->MutableConfig()->Swap(configToPropose);
            IssueScatterTask(TActorId(), std::move(task));
        } else {
            RootState = ERootState::RELAX; // nothing to do right now, just relax
        }
    }

    std::optional<TString> TDistributedConfigKeeper::ProcessProposeStorageConfig(TEvGather::TProposeStorageConfig *res) {
        auto generateSuccessful = [&](auto&& callback) {
            for (const auto& item : res->GetStatus()) {
                const TNodeIdentifier node(item.GetNodeId());
                for (const auto& drive : item.GetSuccessfulDrives()) {
                    callback(node, drive.GetPath(), drive.HasGuid() ? std::make_optional(drive.GetGuid()) : std::nullopt);
                }
            }
        };

        if (!CurrentProposedStorageConfig) {
            return "no currently proposed StorageConfig";
        } else if (HasConfigQuorum(*CurrentProposedStorageConfig, generateSuccessful, *Cfg)) {
            // apply configuration and spread it
            ApplyStorageConfig(*CurrentProposedStorageConfig);
            FanOutReversePush(&StorageConfig.value(), true /*recurseConfigUpdate*/);
            CurrentProposedStorageConfig.reset();
        } else {
            STLOG(PRI_DEBUG, BS_NODE, NWDC47, "no quorum for ProposedStorageConfig", (Record, *res),
                (CurrentProposedStorageConfig, *CurrentProposedStorageConfig));
            CurrentProposedStorageConfig.reset();
            return "no quorum for ProposedStorageConfig";
        }

        return {};
    }

    void TDistributedConfigKeeper::PrepareScatterTask(ui64 cookie, TScatterTask& task) {
        switch (task.Request.GetRequestCase()) {
            case TEvScatter::kCollectConfigs: {
                std::vector<TString> drives;
                auto callback = [&](const auto& /*node*/, const auto& drive) {
                    drives.push_back(drive.GetPath());
                };
                EnumerateConfigDrives(*StorageConfig, SelfId().NodeId(), callback);
                if (ProposedStorageConfig) {
                    EnumerateConfigDrives(*ProposedStorageConfig, SelfId().NodeId(), callback);
                }
                std::sort(drives.begin(), drives.end());
                drives.erase(std::unique(drives.begin(), drives.end()), drives.end());
                ReadConfig(cookie);
                ++task.AsyncOperationsPending;
                break;
            }

            case TEvScatter::kProposeStorageConfig:
                if (ProposedStorageConfigCookieUsage) {
                    auto *status = task.Response.MutableProposeStorageConfig()->AddStatus();
                    SelfNode.Serialize(status->MutableNodeId());
                    status->SetStatus(TEvGather::TProposeStorageConfig::RACE);
                } else if (const auto& proposed = task.Request.GetProposeStorageConfig().GetConfig();
                        proposed.GetGeneration() < StorageConfig->GetGeneration() || (
                            proposed.GetGeneration() == StorageConfig->GetGeneration() &&
                            proposed.GetFingerprint() != StorageConfig->GetFingerprint())) {
                    auto *status = task.Response.MutableProposeStorageConfig()->AddStatus();
                    SelfNode.Serialize(status->MutableNodeId());
                    status->SetStatus(TEvGather::TProposeStorageConfig::ERROR);
                    STLOG(PRI_ERROR, BS_NODE, NWDC49, "ProposedStorageConfig generation/fingerprint mismatch",
                        (StorageConfig, StorageConfig), (Request, task.Request), (RootNodeId, GetRootNodeId()));
                    Y_DEBUG_ABORT();
                } else {
                    ProposedStorageConfigCookie = cookie;
                    ProposedStorageConfig.emplace(proposed);

                    // issue notification to node warden
                    if (StorageConfig && StorageConfig->GetGeneration() &&
                            StorageConfig->GetGeneration() < ProposedStorageConfig->GetGeneration()) {
                        const TActorId wardenId = MakeBlobStorageNodeWardenID(SelfId().NodeId());
                        auto ev = std::make_unique<TEvNodeWardenStorageConfig>(*StorageConfig, &ProposedStorageConfig.value());
                        Send(wardenId, ev.release(), 0, cookie);
                        ++task.AsyncOperationsPending;
                        ++ProposedStorageConfigCookieUsage;
                    }

                    PersistConfig([this, cookie](TEvPrivate::TEvStorageConfigStored& msg) {
                        Y_ABORT_UNLESS(ProposedStorageConfigCookieUsage);
                        Y_ABORT_UNLESS(cookie == ProposedStorageConfigCookie);
                        --ProposedStorageConfigCookieUsage;

                        if (auto it = ScatterTasks.find(cookie); it != ScatterTasks.end()) {
                            TScatterTask& task = it->second;
                            auto *status = task.Response.MutableProposeStorageConfig()->AddStatus();
                            SelfNode.Serialize(status->MutableNodeId());
                            status->SetStatus(TEvGather::TProposeStorageConfig::ACCEPTED);
                            for (const auto& [path, ok, guid] : msg.StatusPerPath) {
                                if (ok) {
                                    auto *drive = status->AddSuccessfulDrives();
                                    drive->SetPath(path);
                                    if (guid) {
                                        drive->SetGuid(*guid);
                                    }
                                }
                            }
                            STLOG(PRI_DEBUG, BS_NODE, NWDC48, "ProposeStorageConfig TEvStorageConfigStored",
                                (Cookie, cookie), (Status, *status));
                        } else {
                            STLOG(PRI_DEBUG, BS_NODE, NWDC45, "ProposeStorageConfig TEvStorageConfigStored no scatter task",
                                (Cookie, cookie));
                        }

                        FinishAsyncOperation(cookie);
                    });

                    ++task.AsyncOperationsPending;
                    ++ProposedStorageConfigCookieUsage;
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
        THashSet<TNodeIdentifier> nodesAlreadyReplied{SelfNode};

        auto *ptr = response->AddNodes();
        ptr->MutableBaseConfig()->CopyFrom(BaseConfig);
        SelfNode.Serialize(ptr->AddNodeIds());
        baseConfigs.emplace(BaseConfig, ptr);

        THashMap<TStorageConfigMeta, TEvGather::TCollectConfigs::TPersistentConfig*> committedConfigs;
        THashMap<TStorageConfigMeta, TEvGather::TCollectConfigs::TPersistentConfig*> proposedConfigs;
        for (auto& item : *response->MutableCommittedConfigs()) {
            committedConfigs[item.GetConfig()] = &item;
        }
        for (auto& item : *response->MutableProposedConfigs()) {
            proposedConfigs[item.GetConfig()] = &item;
        }

        auto addBaseConfig = [&](const TEvGather::TCollectConfigs::TNode& item, auto *nodesToIgnore) {
            const auto& config = item.GetBaseConfig();
            auto& ptr = baseConfigs[config];
            for (const auto& nodeId : item.GetNodeIds()) {
                TNodeIdentifier n(nodeId);
                if (const auto [_, inserted] = nodesAlreadyReplied.emplace(n); inserted) {
                    if (!ptr) {
                        ptr = response->AddNodes();
                        ptr->MutableBaseConfig()->CopyFrom(config);
                    }
                    ptr->AddNodeIds()->CopyFrom(nodeId);
                } else {
                    nodesToIgnore->insert(std::move(n));
                }
            }
        };

        auto addPerDiskConfig = [&](const TEvGather::TCollectConfigs::TPersistentConfig& item, auto addFunc, auto& set,
                const auto& nodesToIgnore) {
            const auto& config = item.GetConfig();
            auto& ptr = set[config];
            for (const auto& disk : item.GetDisks()) {
                if (!nodesToIgnore.contains(TNodeIdentifier(disk.GetNodeId()))) {
                    if (!ptr) {
                        ptr = (response->*addFunc)();
                        ptr->MutableConfig()->CopyFrom(config);
                    }
                    ptr->AddDisks()->CopyFrom(disk);
                }
            }
        };

        for (const auto& reply : task.CollectedResponses) {
            if (reply.HasCollectConfigs()) {
                const auto& cc = reply.GetCollectConfigs();

                THashSet<TNodeIdentifier> nodesToIgnore;
                for (const auto& item : cc.GetNodes()) {
                    addBaseConfig(item, &nodesToIgnore);
                }
                for (const auto& item : cc.GetCommittedConfigs()) {
                    addPerDiskConfig(item, &TEvGather::TCollectConfigs::AddCommittedConfigs, committedConfigs, nodesToIgnore);
                }
                for (const auto& item : cc.GetProposedConfigs()) {
                    addPerDiskConfig(item, &TEvGather::TCollectConfigs::AddProposedConfigs, proposedConfigs, nodesToIgnore);
                }
                for (const auto& item : cc.GetNoMetadata()) {
                    if (!nodesToIgnore.contains(TNodeIdentifier(item.GetNodeId()))) {
                        response->AddNoMetadata()->CopyFrom(item);
                    }
                }
                for (const auto& item : cc.GetErrors()) {
                    if (!nodesToIgnore.contains(TNodeIdentifier(item.GetNodeId()))) {
                        response->AddErrors()->CopyFrom(item);
                    }
                }
            }
        }
    }

    void TDistributedConfigKeeper::Perform(TEvGather::TProposeStorageConfig *response,
            const TEvScatter::TProposeStorageConfig& /*request*/, TScatterTask& task) {
        THashSet<TNodeIdentifier> nodesAlreadyReplied;
        for (const auto& reply : task.CollectedResponses) {
            if (reply.HasProposeStorageConfig()) {
                const auto& config = reply.GetProposeStorageConfig();
                for (const auto& status : config.GetStatus()) {
                    if (const auto [_, inserted] = nodesAlreadyReplied.insert(status.GetNodeId()); inserted) {
                        response->AddStatus()->CopyFrom(status);
                    }
                }
            }
        }
    }

    void TDistributedConfigKeeper::FanOutReversePush(const NKikimrBlobStorage::TStorageConfig *config,
            bool recurseConfigUpdate) {
        for (const auto& [nodeId, info] : DirectBoundNodes) {
            SendEvent(nodeId, info, std::make_unique<TEvNodeConfigReversePush>(GetRootNodeId(), config,
                recurseConfigUpdate));
        }
    }

} // NKikimr::NStorage

#include "distconf.h"
#include "distconf_quorum.h"
#include "distconf_invoke.h"

namespace NKikimr::NStorage {

    void TDistributedConfigKeeper::CheckRootNodeStatus() {
        Y_VERIFY_S(Binding ? (RootState == ERootState::INITIAL || RootState == ERootState::ERROR_TIMEOUT) && !Scepter :
            RootState == ERootState::INITIAL || RootState == ERootState::ERROR_TIMEOUT ? !Scepter :
            static_cast<bool>(Scepter) || RootState == ERootState::SCEPTERLESS_OPERATION,
            "Binding# " << (Binding ? Binding->ToString() : "<null>")
            << " RootState# " << RootState
            << " Scepter# " << (Scepter ? ToString(Scepter->Id) : "<null>"));

        if (Binding) { // can't become root node
            return;
        }

        if (RootState == ERootState::INITIAL && GlobalQuorum) { // becoming root node
            Y_ABORT_UNLESS(!Scepter);
            Scepter = std::make_shared<TScepter>();
            BecomeRoot();
        } else if (Scepter && !GlobalQuorum) { // unbecoming root node -- lost quorum
            SwitchToError("quorum lost");
        }
    }

    void TDistributedConfigKeeper::BecomeRoot() {
        WorkingSyncersByNode.clear();
        WorkingSyncers.clear();
        SyncerArrangeInFlight = false;
        SyncerArrangePending = false;

        // start collecting syncers state if needed
        IssueQuerySyncers();

        // establish connection to console tablet (if we have means to do it)
        Y_ABORT_UNLESS(!ConsolePipeId);
        ConnectToConsole();

        // trigger first operation
        RootState = ERootState::RELAX;
        ChildActors.insert(RegisterWithSameMailbox(new TInvokeRequestHandlerActor(this)));
    }

    void TDistributedConfigKeeper::UnbecomeRoot() {
        if (StateStorageSelfHealActor) {
            Send(new IEventHandle(TEvents::TSystem::Poison, 0, StateStorageSelfHealActor.value(), SelfId(), nullptr, 0));
            StateStorageSelfHealActor.reset();
        }
        DisconnectFromConsole();
        OpQueueOnUnbecomeRoot();
    }

    void TDistributedConfigKeeper::SwitchToError(const TString& reason) {
        STLOG(PRI_NOTICE, BS_NODE, NWDC38, "SwitchToError", (RootState, RootState), (Reason, reason));
        if (Scepter) {
            UnbecomeRoot();
            Scepter.reset();
            ++ScepterCounter;
        }
        Y_ABORT_UNLESS(RootState != ERootState::ERROR_TIMEOUT);
        RootState = ERootState::ERROR_TIMEOUT;
        ErrorReason = reason;
        OpQueueOnError(reason);
        CurrentProposition.reset();
        CurrentSelfAssemblyUUID.reset();
        ApplyConfigUpdateToDynamicNodes(true);
        AbortAllScatterTasks(std::nullopt);
        const TDuration timeout = TDuration::FromValue(ErrorTimeout.GetValue() * (25 + RandomNumber(51u)) / 50);
        TActivationContext::Schedule(timeout, new IEventHandle(TEvPrivate::EvErrorTimeout, 0, SelfId(), {}, nullptr, 0));
    }

    void TDistributedConfigKeeper::HandleErrorTimeout() {
        STLOG(PRI_DEBUG, BS_NODE, NWDC20, "Error timeout hit");
        Y_ABORT_UNLESS(RootState == ERootState::ERROR_TIMEOUT);
        Y_ABORT_UNLESS(!Scepter);
        Y_ABORT_UNLESS(InvokeQ.empty());
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
                Y_DEBUG_ABORT();
                break;

            case TEvGather::kProposeStorageConfig:
                return ProcessProposeStorageConfig(res->MutableProposeStorageConfig());

            case TEvGather::kManageSyncers:
                return ProcessManageSyncers(res->MutableManageSyncers());

            case TEvGather::RESPONSE_NOT_SET:
                return SwitchToError("response not set");
        }

        SwitchToError("incorrect response from peer");
    }

    bool TDistributedConfigKeeper::HasConnectedNodeQuorum(const NKikimrBlobStorage::TStorageConfig& config,
            const THashSet<TBridgePileId>& specificBridgePileIds) const {
        auto generateConnected = [&](auto&& callback) {
            for (const auto& [nodeId, node] : AllBoundNodes) {
                callback(nodeId);
            }
        };
        return HasNodeQuorum(config, generateConnected, GetMandatoryPileIds(config, specificBridgePileIds), nullptr);
    }

    TDistributedConfigKeeper::TProcessCollectConfigsResult TDistributedConfigKeeper::ProcessCollectConfigs(
            TEvGather::TCollectConfigs *res, std::optional<TStringBuf> selfAssemblyUUID) {
        TStringStream err;

        // generate set of piles we need to have config quorum for
        THashSet<TBridgePileId> pileIdsForConfigQuorum = GetMandatoryPileIds(*StorageConfig, {});
        THashSet<TBridgePileId> unsyncedConfigPileIds; // these piles are treated separately
        for (const auto& pss : StorageConfig->GetClusterStateHistory().GetPileSyncState()) {
            if (pss.GetUnsyncedStorageConfig()) {
                auto bridgePileId = TBridgePileId::FromProto(&pss, &std::decay_t<decltype(pss)>::GetBridgePileId);
                pileIdsForConfigQuorum.erase(bridgePileId);
                unsyncedConfigPileIds.insert(bridgePileId);
            }
        }
        auto isNodeConfigAcceptable = [&](const auto& node) {
            return pileIdsForConfigQuorum.contains(TNodeIdentifier(node).BridgePileId.value_or(InvalidBridgePileId));
        };

        auto generateSuccessful = [&](auto&& callback) {
            for (const auto& item : res->GetNodes()) {
                for (const auto& node : item.GetNodeIds()) {
                    callback(node);
                }
            }
        };
        const bool nodeQuorum = HasNodeQuorum(*StorageConfig, generateSuccessful, &err);

        auto generateSuccessfulDisks = [&](auto&& callback) {
            auto invoke = [&](const auto& disk) {
                if (isNodeConfigAcceptable(disk.GetNodeId())) {
                    callback(TNodeIdentifier(disk.GetNodeId()), disk.GetPath(),
                        disk.HasGuid() ? std::make_optional(disk.GetGuid()) : std::nullopt);
                }
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
        const bool configQuorum = HasConfigQuorum(*StorageConfig, generateSuccessfulDisks, *Cfg, false,
            pileIdsForConfigQuorum, &err);

        STLOG(PRI_DEBUG, BS_NODE, NWDC31, "ProcessCollectConfigs", (RootState, RootState), (NodeQuorum, nodeQuorum),
            (ConfigQuorum, configQuorum), (Res, *res), (Error, err.Str()));

        if (nodeQuorum && !configQuorum) {
            // check if there is quorum of no-distconf config along the cluster
            auto generateNodesWithoutDistconf = [&](auto&& callback) {
                for (const auto& item : res->GetNodes()) {
                    if (item.GetBaseConfig().GetSelfManagementConfig().GetEnabled()) {
                        continue;
                    }
                    for (const auto& node : item.GetNodeIds()) {
                        callback(node);
                    }
                }
            };
            if (HasNodeQuorum(*StorageConfig, generateNodesWithoutDistconf, nullptr)) {
                // yes, distconf is disabled on the majority of the nodes, so we can't do anything about it
                return {.IsDistconfDisabledQuorum = true};
            }
        }

        if (!nodeQuorum || !configQuorum) {
            return {.ErrorReason = TStringBuilder() << "no quorum for CollectConfigs:" << err.Str()};
        }

        // TODO: validate self-assembly UUID

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Pick base config quorum (if we have one) -- it must be the same throughout the cluster, no matter what

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
            if (HasNodeQuorum(r.Config, generateNodeIds, nullptr)) {
                ++it;
            } else {
                baseConfigs.erase(it++);
            }
        }
        if (baseConfigs.size() > 1) {
            STLOG(PRI_CRIT, BS_NODE, NWDC08, "Multiple nonintersecting node sets have quorum of BaseConfig",
                (BaseConfigs.size, baseConfigs.size()));
            Y_DEBUG_ABORT("Multiple nonintersecting node sets have quorum of BaseConfig");
            Halt();
            return {.ErrorReason = "Multiple nonintersecting node sets have quorum of BaseConfig"};
        }
        NKikimrBlobStorage::TStorageConfig *baseConfig = nullptr;
        for (auto& [meta, info] : baseConfigs) {
            baseConfig = &info.Config;
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Create quorums for committed and proposed configurations; we have one such quorum for main cluster part
        // (when in bridge mode) and for every unsynced pile

        struct TDiskConfigInfo {
            NKikimrBlobStorage::TStorageConfig Config;
            THashSet<std::tuple<TNodeIdentifier, TString, std::optional<ui64>>> HavingDisks;
        };
        THashMap<TBridgePileId, THashMap<TStorageConfigMeta, TDiskConfigInfo>> persistentConfigs; // all of them in one bucket: proposed, committed
        ui64 maxSeenGeneration = StorageConfig ? StorageConfig->GetGeneration() : 0; // max seen generation in the main part
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
                for (const auto& disk : item.GetDisks()) {
                    TBridgePileId targetBridgePileId;
                    if (isNodeConfigAcceptable(disk.GetNodeId())) { // main cluster part
                        targetBridgePileId = InvalidBridgePileId;
                        if (isCommitted) { // update maximum committed config's seen generation for main part
                            maxSeenGeneration = Max(maxSeenGeneration, config.GetGeneration());
                        }
                    } else if (const TNodeIdentifier i(disk.GetNodeId()); i.BridgePileId && unsyncedConfigPileIds.contains(*i.BridgePileId)) {
                        targetBridgePileId = *i.BridgePileId;
                    } else {
                        continue; // strange, but okay
                    }

                    const auto [it, inserted] = persistentConfigs[targetBridgePileId].try_emplace(config);
                    TDiskConfigInfo& r = it->second;
                    if (inserted) {
                        r.Config.CopyFrom(config);
                    }
                    r.HavingDisks.emplace(disk.GetNodeId(), disk.GetPath(), disk.HasGuid() ? std::make_optional(disk.GetGuid()) : std::nullopt);
                }
            }
        }

        // find the configuration that we can call 'committed' (persisted in any way -- either in committed, or in
        // proposed, but having a quorum)
        NKikimrBlobStorage::TStorageConfig *persistedConfig = nullptr;
        THashMap<TBridgePileId, NKikimrBlobStorage::TStorageConfig*> persistedConfigForUnsyncedPile;

        for (auto& [targetBridgePileId, pileItems] : persistentConfigs) {
            for (auto& [meta, r] : pileItems) {
                auto generateSuccessful = [&](auto&& callback) {
                    for (const auto& [node, path, guid] : r.HavingDisks) {
                        callback(node, path, guid);
                    }
                };
                THashSet<TBridgePileId> pileIdsForQuorum;
                if (targetBridgePileId == InvalidBridgePileId) {
                    pileIdsForQuorum = pileIdsForConfigQuorum; // all piles in cluster that are config-synced
                } else {
                    pileIdsForQuorum.insert(targetBridgePileId); // only this distinct pile trying to join cluster
                }
                if (HasConfigQuorum(r.Config, generateSuccessful, *Cfg, false, std::move(pileIdsForQuorum))) {
                    const ui64 generation = r.Config.GetGeneration();
                    auto& ref = targetBridgePileId == InvalidBridgePileId
                        ? persistedConfig
                        : persistedConfigForUnsyncedPile[targetBridgePileId];
                    if (!ref || ref->GetGeneration() < generation) {
                        // we have no cadidate yet or candidate with older generation, insert/replace
                        ref = &r.Config;
                    } else if (ref->GetGeneration() == generation) {
                        STLOG(PRI_CRIT, BS_NODE, NWDC37, "Multiple nonintersecting node sets have quorum of persistent config",
                            (Generation, generation), (TargetBridgePileId, targetBridgePileId),
                            (Existing, *ref), (Candidate, r.Config));
                        Y_DEBUG_ABORT("Multiple nonintersecting node sets have quorum of persistent config");
                        Halt();
                        return {.ErrorReason = "Multiple nonintersecting node sets have quorum of persistent config"};
                    }
                }
            }
        }

        if (maxSeenGeneration && (!persistedConfig || persistedConfig->GetGeneration() < maxSeenGeneration)) {
            return {.ErrorReason = "Couldn't obtain quorum for configuration that was seen in effect"};
        }

        NKikimrBlobStorage::TStorageConfig *proposedConfig = nullptr;

        /*
        TODO(alexvru): check if this logic is valid at all VVV

        // let's try to find possibly proposed config, but without a quorum, and try to reconstruct it
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
                        Halt();
                        return {.ErrorReason = "Persistently proposed config has too big generation"};
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
        */

        if (persistedConfig) { // we have a committed config, apply and spread it
            ApplyStorageConfig(*persistedConfig);
            FanOutReversePush(StorageConfig.get(), true /*recurseConfigUpdate*/);
        }

        NKikimrBlobStorage::TStorageConfig tempConfig;
        NKikimrBlobStorage::TStorageConfig *configToPropose = nullptr;
        std::optional<NKikimrBlobStorage::TStorageConfig> propositionBase;

        auto& sc = *StorageConfig;
        const bool canPropose = sc.HasBlobStorageConfig() && sc.GetBlobStorageConfig().HasDefineBox();

        STLOG(PRI_DEBUG, BS_NODE, NWDC59, "ProcessCollectConfigs", (BaseConfig, baseConfig),
            (PersistedConfig, persistedConfig),
            (ProposedConfig, proposedConfig),
            (CanPropose, canPropose),
            (PersistedConfigForUnsyncedPile, persistedConfigForUnsyncedPile));

        if (!canPropose) {
            // we can't propose any configuration here, just ignore
        } else if (proposedConfig) { // we have proposition in progress, resume
            if (persistedConfig) {
                propositionBase.emplace(*persistedConfig);
            }
            configToPropose = proposedConfig;
        } else if (persistedConfig) { // we have committed config, check if we need to update it
            propositionBase.emplace(*persistedConfig);
            if (UpdateConfig(persistedConfig, persistedConfigForUnsyncedPile)) {
                configToPropose = persistedConfig;
            }
        } else if (baseConfig && !baseConfig->GetGeneration()) {
            const bool canBootstrapAutomatically = baseConfig->GetSelfManagementConfig().GetEnabled() &&
                baseConfig->GetSelfManagementConfig().GetAutomaticBootstrap();
            if (canBootstrapAutomatically || selfAssemblyUUID) {
                if (!selfAssemblyUUID) {
                    if (!CurrentSelfAssemblyUUID) {
                        CurrentSelfAssemblyUUID.emplace(CreateGuidAsString());
                    }
                    selfAssemblyUUID.emplace(CurrentSelfAssemblyUUID.value());
                }
                propositionBase.emplace(*baseConfig);
                if (auto error = GenerateFirstConfig(baseConfig, TString(*selfAssemblyUUID))) {
                    return {.ErrorReason = *error};
                }
                configToPropose = baseConfig;
            }
        }

        if (configToPropose) {
            return {
                .PropositionBase = std::move(propositionBase),
                .ConfigToPropose = *configToPropose,
            };
        }

        return {};
    }

    void TDistributedConfigKeeper::ProcessProposeStorageConfig(TEvGather::TProposeStorageConfig *res) {
        auto generateSuccessful = [&](auto&& callback) {
            for (const auto& item : res->GetStatus()) {
                const TNodeIdentifier node(item.GetNodeId());
                for (const auto& drive : item.GetSuccessfulDrives()) {
                    callback(node, drive.GetPath(), drive.HasGuid() ? std::make_optional(drive.GetGuid()) : std::nullopt);
                }
            }
        };

        if (!CurrentProposition) {
            Y_DEBUG_ABORT("no currently proposed StorageConfig");
            return SwitchToError("no currently proposed StorageConfig");
        }

        // reset current proposition in advance
        auto proposition = *std::exchange(CurrentProposition, std::nullopt);

        auto finishWithError = [&](TString error) {
            if (proposition.ActorId) {
                if (auto *actor = GetInvokeRequestHandlerActor(proposition.ActorId)) {
                    actor->OnConfigProposed(error);
                } else {
                    Y_ABORT_UNLESS(DeadActorWaitingForProposition);
                }
            } else {
                SwitchToError(error);
            }
        };

        if (TStringStream err; HasConfigQuorum(proposition.StorageConfig, generateSuccessful, *Cfg, true,
                proposition.SpecificBridgePileIds, &err)) {
            // apply configuration and spread it
            ApplyStorageConfig(proposition.StorageConfig);
            FanOutReversePush(StorageConfig.get(), true /*recurseConfigUpdate*/);

            // this proposition came from actor -- we notify that actor and finish operation
            Y_ABORT_UNLESS(proposition.ActorId);
            if (auto *actor = GetInvokeRequestHandlerActor(proposition.ActorId)) {
                actor->OnConfigProposed(std::nullopt);
            } else {
                Y_ABORT_UNLESS(DeadActorWaitingForProposition);
            }

            // in case of successful proposition we trigger syncers (if needed)
            if (proposition.CheckSyncersAfterCommit) {
                IssueQuerySyncers();
            }
        } else {
            STLOG(PRI_DEBUG, BS_NODE, NWDC47, "no quorum for ProposedStorageConfig", (Record, *res),
                (ProposedStorageConfig, proposition.StorageConfig),
                (SpecificBridgePileIds, proposition.SpecificBridgePileIds),
                (ActorId, proposition.ActorId),
                (CheckSyncersAfterCommit, proposition.CheckSyncersAfterCommit),
                (Error, err.Str()));
            finishWithError(TStringBuilder() << "no quorum for ProposedStorageConfig:" << err.Str());
        }

        // if this proposition was made by an actor, but it has died, then we have to return state to correct one
        if (DeadActorWaitingForProposition) {
            Y_ABORT_UNLESS(proposition.ActorId);
            Y_ABORT_UNLESS(!InvokeQ.empty());
            const auto& front = InvokeQ.front();
            Y_ABORT_UNLESS(proposition.ActorId == front.ActorId);
            Y_ABORT_UNLESS(!GetInvokeRequestHandlerActor(front.ActorId));

            if (RootState == ERootState::SCEPTERLESS_OPERATION) {
                Y_ABORT_UNLESS(front.Scepterless);
                RootState = ERootState::INITIAL;
            } else if (RootState == ERootState::IN_PROGRESS) {
                RootState = ERootState::RELAX;
            } else {
                return;
            }

            InvokeQ.pop_front();
            DeadActorWaitingForProposition = false;

            OpQueueProcessFront();
        }
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
                        (StorageConfig, StorageConfig.get()), (Request, task.Request), (RootNodeId, GetRootNodeId()));
                    Y_DEBUG_ABORT();
                } else {
                    ProposedStorageConfigCookie = cookie;
                    ProposedStorageConfig.emplace(proposed);

                    // issue notification to node warden
                    if (StorageConfig && StorageConfig->GetGeneration() &&
                            StorageConfig->GetGeneration() < ProposedStorageConfig->GetGeneration()) {
                        ReportStorageConfigToNodeWarden(cookie);
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

            case TEvScatter::kManageSyncers:
                PrepareScatterTask(cookie, task, task.Request.GetManageSyncers());
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

            case TEvScatter::kManageSyncers:
                Perform(task.Response.MutableManageSyncers(), task.Request.GetManageSyncers(), task);
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
        ptr->MutableBaseConfig()->CopyFrom(*BaseConfig);
        SelfNode.Serialize(ptr->AddNodeIds());
        baseConfigs.emplace(*BaseConfig, ptr);

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

    std::optional<TString> TDistributedConfigKeeper::StartProposition(NKikimrBlobStorage::TStorageConfig *configToPropose,
            const NKikimrBlobStorage::TStorageConfig *propositionBase, THashSet<TBridgePileId>&& specificBridgePileIds,
            TActorId actorId, bool checkSyncersAfterCommit) {
        // ensure we are not proposing any other config right now
        Y_ABORT_UNLESS(!CurrentProposition);

        if (propositionBase) {
            if (propositionBase->GetGeneration() == configToPropose->GetGeneration()) {
                configToPropose->SetGeneration(propositionBase->GetGeneration() + 1);
            } else {
                Y_ABORT_UNLESS(propositionBase->GetGeneration() < configToPropose->GetGeneration());
            }
            configToPropose->MutablePrevConfig()->CopyFrom(*propositionBase);
            configToPropose->MutablePrevConfig()->ClearPrevConfig();
        }
        UpdateFingerprint(configToPropose);

        STLOG(PRI_INFO, BS_NODE, NWDC60, "StartProposition",
            (ConfigToPropose, *configToPropose),
            (PropositionBase, propositionBase),
            (StorageConfig, StorageConfig.get()),
            (SpecificBridgePileIds, specificBridgePileIds),
            (ActorId, actorId),
            (CheckSyncersAfterCommit, checkSyncersAfterCommit));

        if (propositionBase) {
            if (auto error = ValidateConfig(*propositionBase)) {
                return TStringBuilder() << "failed to propose configuration, base config contains errors: " << *error;
            }
            if (auto error = ValidateConfigUpdate(*propositionBase, *configToPropose)) {
                return TStringBuilder() << "incorrect config proposed: " << *error
                    << " Base# " << SingleLineProto(*propositionBase)
                    << " Proposed# " << SingleLineProto(*configToPropose);
            }
        } else if (auto error = ValidateConfig(*configToPropose)) {
            return TStringBuilder() << "incorrect config proposed: " << *error;
        }

        // remember proposition
        CurrentProposition.emplace(TProposition{
            .StorageConfig = *configToPropose,
            .SpecificBridgePileIds = std::move(specificBridgePileIds),
            .ActorId = actorId,
            .CheckSyncersAfterCommit = checkSyncersAfterCommit,
        });

        // issue scatter task
        TEvScatter task;
        task.SetTaskId(RandomNumber<ui64>());
        task.MutableProposeStorageConfig()->MutableConfig()->Swap(configToPropose);
        IssueScatterTask(TActorId(), std::move(task));

        return std::nullopt;
    }

    void TDistributedConfigKeeper::CheckForConfigUpdate() {
        if (NKikimrBlobStorage::TStorageConfig config; UpdateConfig(&config, {})) {
            ChildActors.insert(RegisterWithSameMailbox(new TInvokeRequestHandlerActor(this, std::move(config))));
        }
    }

} // NKikimr::NStorage

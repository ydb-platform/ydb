#include "distconf.h"
#include "distconf_quorum.h"
#include "distconf_invoke.h"

namespace NKikimr::NStorage {

    void TDistributedConfigKeeper::UpdateQuorums() {
        // update quorum flags, if something has changed
        if (!std::exchange(QuorumValid, true)) {
            // create a list of connected nodes
            std::vector<TNodeIdentifier> connected;
            connected.reserve(AllBoundNodes.size());
            for (const auto& [nodeId, node] : AllBoundNodes) {
                connected.push_back(nodeId);
            }

            // recalculate global and local pile quorums
            Y_ABORT_UNLESS(StorageConfig);
            LocalPileQuorum = BridgeInfo && HasNodeQuorum(*StorageConfig, connected, BridgePileNameMap,
                BridgeInfo->SelfNodePile->BridgePileId, nullptr);
            GlobalQuorum = (!BridgeInfo || BridgeInfo->SelfNodePile->IsPrimary) && HasNodeQuorum(*StorageConfig,
                connected, BridgePileNameMap, TBridgePileId(), nullptr);

            // recalculate unsynced piles' quorum too
            if (BridgeInfo) {
                ConnectedUnsyncedPiles.clear();
                for (const auto& pile : BridgeInfo->Piles) {
                    if (pile.State == NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1 && HasNodeQuorum(*StorageConfig,
                            connected, BridgePileNameMap, pile.BridgePileId, nullptr)) {
                        ConnectedUnsyncedPiles.insert(pile.BridgePileId);
                    }
                }
                if (!ConnectedUnsyncedPiles.empty() && InvokeQ.empty()) {
                    CheckForConfigUpdate();
                }
            }
        }
    }

    void TDistributedConfigKeeper::CheckRootNodeStatus() {
        Y_VERIFY_S(Binding ? (RootState == ERootState::INITIAL || RootState == ERootState::ERROR_TIMEOUT) && !Scepter :
            RootState == ERootState::INITIAL || RootState == ERootState::ERROR_TIMEOUT ? !Scepter :
            static_cast<bool>(Scepter),
            "Binding# " << (Binding ? Binding->ToString() : "<null>")
            << " RootState# " << RootState
            << " Scepter# " << (Scepter ? ToString(Scepter->Id) : "<null>"));

        // check if we can't start any root activities right now
        if (Binding || RootState == ERootState::ERROR_TIMEOUT) {
            return;
        }

        if (!Scepter && GlobalQuorum) {
            Scepter = std::make_shared<TScepter>();
            BecomeRoot();
        } else if (Scepter && !GlobalQuorum) {
            // if we have local pile quorum, then do not switch into error state, we'll start collecting configs locally
            SwitchToError("quorum lost");
        }

        if (!LocalPileQuorum) {
            UnbindNodesFromOtherPiles("local pile quorum lost");
        }
    }

    void TDistributedConfigKeeper::HandleRetryCollectConfigsAndPropose(STATEFN_SIG) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC84, "HandleRetryCollectConfigsAndPropose", (Cookie, ev->Cookie),
            (InvokePipelineGeneration, InvokePipelineGeneration));
        if (ev->Cookie == InvokePipelineGeneration) {
            Y_ABORT_UNLESS(Scepter);
            Y_ABORT_UNLESS(!Binding);
            Invoke(TCollectConfigsAndPropose{});
        }
    }

    void TDistributedConfigKeeper::BecomeRoot() {
        STLOG(PRI_DEBUG, BS_NODE, NWDC85, "BecomeRoot", (RootState, RootState), (InvokeQ.size, InvokeQ.size()));

        // establish connection to console tablet (if we have means to do it)
        Y_ABORT_UNLESS(!ConsolePipeId);
        ConnectToConsole();

        // switch to correct state
        Y_VERIFY_S(RootState == ERootState::INITIAL, "RootState# " << RootState);
        RootState = ERootState::RELAX;

        // start config collection
        Invoke(TCollectConfigsAndPropose{});
        CollectConfigsBackoffTimer.Reset();
    }

    void TDistributedConfigKeeper::UnbecomeRoot() {
        if (StateStorageSelfHealActor) {
            Send(new IEventHandle(TEvents::TSystem::Poison, 0, StateStorageSelfHealActor.value(), SelfId(), nullptr, 0));
            StateStorageSelfHealActor.reset();
        }
        DisconnectFromConsole();
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
        InvokeQ = std::exchange(InvokePending, {});
        if (!InvokeQ.empty()) {
            TActivationContext::Send(new IEventHandle(TEvPrivate::EvExecuteQuery, 0, InvokeQ.front().ActorId, {}, nullptr, 0));
        }
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

            case TEvGather::RESPONSE_NOT_SET:
                return SwitchToError("response not set");
        }

        SwitchToError("incorrect response from peer");
    }

    bool TDistributedConfigKeeper::HasConnectedNodeQuorum(const NKikimrBlobStorage::TStorageConfig& config,
            bool local) const {
        std::vector<TNodeIdentifier> connected;
        connected.reserve(AllBoundNodes.size());
        for (const auto& [nodeId, node] : AllBoundNodes) {
            connected.push_back(nodeId);
        }
        return HasNodeQuorum(config, connected, BridgePileNameMap, local && BridgeInfo ?
            BridgeInfo->SelfNodePile->BridgePileId : TBridgePileId(), nullptr);
    }

    TDistributedConfigKeeper::TProcessCollectConfigsResult TDistributedConfigKeeper::ProcessCollectConfigs(
            TEvGather::TCollectConfigs *res, std::optional<TStringBuf> selfAssemblyUUID) {
        TStringStream err;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Calculate connected node quorum

        std::vector<TNodeIdentifier> successfulNodes;
        for (const auto& item : res->GetNodes()) {
            for (const auto& node : item.GetNodeIds()) {
                successfulNodes.emplace_back(node);
            }
        }
        const bool nodeQuorum = HasNodeQuorum(*StorageConfig, successfulNodes, BridgePileNameMap, TBridgePileId(), &err);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Calculate configuration quorum

        std::vector<TSuccessfulDisk> successfulDisks;
        auto addSuccessfulDisk = [&](const auto& disk) {
            successfulDisks.emplace_back(TNodeIdentifier(disk.GetNodeId()), disk.GetPath(),
                disk.HasGuid() ? std::make_optional(disk.GetGuid()) : std::nullopt);
        };
        for (const auto& item : res->GetCommittedConfigs()) {
            for (const auto& disk : item.GetDisks()) {
                addSuccessfulDisk(disk);
            }
        }
        for (const auto& item : res->GetProposedConfigs()) {
            for (const auto& disk : item.GetDisks()) {
                addSuccessfulDisk(disk);
            }
        }
        for (const auto& disk : res->GetNoMetadata()) {
            addSuccessfulDisk(disk);
        }
        const bool configQuorum = HasConfigQuorum(*StorageConfig, successfulDisks, BridgePileNameMap, TBridgePileId(),
            *Cfg, false, &err);

        STLOG(PRI_DEBUG, BS_NODE, NWDC31, "ProcessCollectConfigs", (RootState, RootState), (NodeQuorum, nodeQuorum),
            (ConfigQuorum, configQuorum), (Res, *res), (Error, err.Str()));

        if (nodeQuorum && !configQuorum) {
            // check if there is quorum of no-distconf config along the cluster
            std::vector<TNodeIdentifier> nodesWithoutDistconf;
            for (const auto& item : res->GetNodes()) {
                if (item.GetBaseConfig().GetSelfManagementConfig().GetEnabled()) {
                    continue;
                }
                for (const auto& node : item.GetNodeIds()) {
                    nodesWithoutDistconf.emplace_back(node);
                }
            }
            if (HasNodeQuorum(*StorageConfig, nodesWithoutDistconf, BridgePileNameMap, TBridgePileId(), nullptr)) {
                // yes, distconf is disabled on the majority of the nodes, so we can't do anything about it
                return {.IsDistconfDisabledQuorum = true};
            }
        }

        if (!nodeQuorum || !configQuorum) {
            return {.ErrorReason = TStringBuilder() << "no quorum for CollectConfigs:" << err.Str()};
        }

        // TODO: validate self-assembly UUID

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Pick base config quorum (if we have one) -- it must be the same throughout the cluster even for nodes that
        // are not part of the config quorum

        struct TBaseConfigInfo {
            NKikimrBlobStorage::TStorageConfig Config;
            std::vector<TNodeIdentifier> HavingNodeIds;
        };
        THashMap<TStorageConfigMeta, TBaseConfigInfo> baseConfigs;
        for (const auto& node : res->GetNodes()) {
            if (node.HasBaseConfig()) {
                const auto& baseConfig = node.GetBaseConfig();
                if (!CheckFingerprint(baseConfig)) {
                    STLOG(PRI_CRIT, BS_NODE, NWDC57, "BaseConfig fingerprint error", (NodeRecord, node));
                    Y_DEBUG_ABORT("BaseConfig fingerprint error");
                    continue;
                }
                const auto [it, inserted] = baseConfigs.try_emplace(baseConfig);
                TBaseConfigInfo& r = it->second;
                if (inserted) {
                    r.Config.CopyFrom(baseConfig);
                }
                for (const auto& nodeId : node.GetNodeIds()) {
                    r.HavingNodeIds.emplace_back(nodeId);
                }
            }
        }
        for (auto it = baseConfigs.begin(); it != baseConfigs.end(); ) { // filter out configs not having node quorum
            TBaseConfigInfo& r = it->second;
            if (HasNodeQuorum(r.Config, r.HavingNodeIds, BridgePileNameMap, TBridgePileId(), nullptr)) {
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

        // first pass: split all found configs into buckets according to the config body itself
        struct TDiskConfigInfo {
            NKikimrBlobStorage::TStorageConfig Config;
            std::vector<TSuccessfulDisk> HavingDisksProposedOrCommitted;
            std::vector<TSuccessfulDisk> HavingDisksCommitted;
        };
        THashMap<TStorageConfigMeta, TDiskConfigInfo> persistentConfigs;
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
                const auto [it, inserted] = persistentConfigs.try_emplace(config);
                TDiskConfigInfo& r = it->second;
                if (inserted) {
                    r.Config.CopyFrom(config);
                }
                for (const auto& disk : item.GetDisks()) {
                    r.HavingDisksProposedOrCommitted.emplace_back(disk.GetNodeId(), disk.GetPath(),
                        disk.HasGuid() ? std::make_optional(disk.GetGuid()) : std::nullopt);
                    if (isCommitted) {
                        r.HavingDisksCommitted.emplace_back(disk.GetNodeId(), disk.GetPath(),
                            disk.HasGuid() ? std::make_optional(disk.GetGuid()) : std::nullopt);
                    }
                }
            }
        }

        // find the configuration that we can call 'committed' (persisted in any way -- either in committed, or in
        // proposed, but having a quorum)
        std::map<ui64, std::tuple<bool, NKikimrBlobStorage::TStorageConfig*>> configsWithQuorum;
        for (auto& [meta, r] : persistentConfigs) {
            for (auto&& [candidateCommitted, havingDisksPtr] : {
                        std::make_tuple(true, &r.HavingDisksCommitted),
                        std::make_tuple(false, &r.HavingDisksProposedOrCommitted)
                    }) {
                if (HasConfigQuorum(r.Config, *havingDisksPtr, BridgePileNameMap, TBridgePileId(), *Cfg, false)) {
                    const ui64 generation = r.Config.GetGeneration();
                    auto& [committed, configPtr] = configsWithQuorum[generation];
                    if (configPtr && configPtr->GetFingerprint() != r.Config.GetFingerprint()) {
                        STLOG(PRI_ERROR, BS_NODE, NWDC37, "Persistent config quorum with different fingerprints",
                            (Generation, generation),
                            (Config, *configPtr),
                            (Committed, candidateCommitted),
                            (OtherConfig, r.Config),
                            (OtherCommitted, candidateCommitted));
                        Y_DEBUG_ABORT("Persistent config quorum with different fingerprints");
                        continue;
                    }
                    configPtr = &r.Config;
                    if (candidateCommitted) {
                        committed = true;
                        break; // no reason to check quorum for proposed-and-committed items
                    }
                }
            }
        }

        // find the latest actual configuration with quorum
        NKikimrBlobStorage::TStorageConfig *persistedConfig = nullptr;
        ui64 maxSeenGeneration = 0;
        for (auto& [generation, item] : configsWithQuorum) {
            auto& [committed, configPtr] = item;
            if (committed) {
                maxSeenGeneration = Max(maxSeenGeneration, configPtr->GetGeneration());
            }
            persistedConfig = configPtr; // we pick the latest
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
        }

        NKikimrBlobStorage::TStorageConfig tempConfig;
        NKikimrBlobStorage::TStorageConfig *configToPropose = nullptr;
        std::optional<NKikimrBlobStorage::TStorageConfig> propositionBase;

        auto& sc = *StorageConfig;
        const bool canPropose = sc.HasBlobStorageConfig() && sc.GetBlobStorageConfig().HasDefineBox();

        STLOG(PRI_DEBUG, BS_NODE, NWDC59, "ProcessCollectConfigs", (BaseConfig, baseConfig),
            (PersistedConfig, persistedConfig),
            (ProposedConfig, proposedConfig),
            (CanPropose, canPropose));

        if (!canPropose) {
            // we can't propose any configuration here, just ignore
        } else if (proposedConfig) { // we have proposition in progress, resume
            if (persistedConfig) {
                propositionBase.emplace(*persistedConfig);
            }
            configToPropose = proposedConfig;
        } else if (persistedConfig) { // we have committed config, check if we need to update it
            propositionBase.emplace(*persistedConfig);
            if (UpdateConfig(persistedConfig)) {
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
        if (!CurrentProposition) {
            Y_DEBUG_ABORT("no currently proposed StorageConfig");
            return SwitchToError("no currently proposed StorageConfig");
        }

        // reset current proposition in advance
        auto proposition = *std::exchange(CurrentProposition, std::nullopt);

        auto finishWithError = [&](TString error) {
            Y_ABORT_UNLESS(proposition.ActorId);
            Send(proposition.ActorId, new TEvPrivate::TEvConfigProposed(std::move(error)));
        };

        std::vector<TSuccessfulDisk> successfulDisks;
        for (const auto& item : res->GetStatus()) {
            const TNodeIdentifier node(item.GetNodeId());
            for (const auto& drive : item.GetSuccessfulDrives()) {
                successfulDisks.emplace_back(node, drive.GetPath(), drive.HasGuid() ? std::make_optional(drive.GetGuid()) : std::nullopt);
            }
        }

        if (TStringStream err; HasConfigQuorum(proposition.StorageConfig, successfulDisks, BridgePileNameMap, TBridgePileId(),
                *Cfg, proposition.MindPrev, &err)) {
            // apply configuration and spread it
            ApplyStorageConfig(proposition.StorageConfig);

            // this proposition came from actor -- we notify that actor and finish operation
            Y_ABORT_UNLESS(proposition.ActorId);
            Send(proposition.ActorId, new TEvPrivate::TEvConfigProposed(std::nullopt));
        } else {
            STLOG(PRI_DEBUG, BS_NODE, NWDC47, "no quorum for ProposedStorageConfig", (Record, *res),
                (ProposedStorageConfig, proposition.StorageConfig),
                (ActorId, proposition.ActorId),
                (Error, err.Str()));
            finishWithError(TStringBuilder() << "no quorum for ProposedStorageConfig:" << err.Str());
        }

        // if this proposition was made by an actor, but it has died, then we have to return state to correct one
        if (DeadActorWaitingForProposition) {
            Y_ABORT_UNLESS(proposition.ActorId);
            Y_ABORT_UNLESS(!InvokeQ.empty());
            const auto& front = InvokeQ.front();
            Y_ABORT_UNLESS(proposition.ActorId == front.ActorId);
            Y_ABORT_UNLESS(!TlsActivationContext->Mailbox.FindActor(front.ActorId.LocalId())); // actor is really dead

            Y_ABORT_UNLESS(RootState == ERootState::IN_PROGRESS);
            RootState = ERootState::RELAX;

            InvokeQ.pop_front();
            DeadActorWaitingForProposition = false;

            if (!InvokeQ.empty()) {
                TActivationContext::Send(new IEventHandle(TEvPrivate::EvExecuteQuery, 0, InvokeQ.front().ActorId,
                    SelfId(), nullptr, 0));
            }
        }
    }

    void TDistributedConfigKeeper::PrepareScatterTask(ui64 cookie, TScatterTask& task) {
        switch (task.Request.GetRequestCase()) {
            case TEvScatter::kCollectConfigs: {
                ReadConfig(GetDrivesToRead(false), cookie);
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
                } else if (proposed.HasClusterState() && (!BridgeInfo || !NBridge::PileStateTraits(proposed.GetClusterState().GetPerPileState(BridgeInfo->SelfNodePile->BridgePileId.GetPileIndex())).RequiresConfigQuorum)) {
                    // won't persist propsed config when this node is not part of the quorum
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

    void TDistributedConfigKeeper::FanOutReversePush() {
        Y_ABORT_UNLESS(StorageConfig);
        THashSet<ui32> nodeIdsToUpdate;
        if (StorageConfig) {
            for (auto& [nodeId, node] : AllBoundNodes) { // assume this gets to all bound nodes
                for (auto& meta : node.Configs) {
                    if (meta.GetGeneration() < StorageConfig->GetGeneration()) {
                        // this node has a chance of holding obsolete configuration: update it
                        nodeIdsToUpdate.insert(nodeId.NodeId());
                        meta = *StorageConfig;
                    }
                }
            }
        }
        const ui32 rootNodeId = GetRootNodeId();
        for (auto& [nodeId, info] : DirectBoundNodes) {
            const bool needToUpdateConfig = nodeIdsToUpdate.contains(nodeId);
            if (needToUpdateConfig || info.LastReportedRootNodeId != rootNodeId) {
                SendEvent(nodeId, info, std::make_unique<TEvNodeConfigReversePush>(rootNodeId,
                    needToUpdateConfig ? StorageConfig.get() : nullptr));
                info.LastReportedRootNodeId = GetRootNodeId();
            }
        }
    }

    std::optional<TString> TDistributedConfigKeeper::StartProposition(NKikimrBlobStorage::TStorageConfig *configToPropose,
            const NKikimrBlobStorage::TStorageConfig *propositionBase, TActorId actorId, bool mindPrev) {
        // ensure we are not proposing any other config right now
        Y_ABORT_UNLESS(!CurrentProposition);

        if (propositionBase) {
            if (propositionBase->GetGeneration() == configToPropose->GetGeneration()) {
                configToPropose->SetGeneration(propositionBase->GetGeneration() + 1);
            } else {
                Y_VERIFY_S(propositionBase->GetGeneration() < configToPropose->GetGeneration(),
                    "PropositionBase# " << SingleLineProto(*propositionBase)
                    << " ConfigToPropose# " << SingleLineProto(*configToPropose));
            }

            configToPropose->MutablePrevConfig()->CopyFrom(*propositionBase);
            configToPropose->MutablePrevConfig()->ClearPrevConfig();
        }

        if (auto error = TransformConfigBeforeCommit(configToPropose)) {
            return error;
        }

        UpdateFingerprint(configToPropose);

        STLOG(PRI_INFO, BS_NODE, NWDC60, "StartProposition",
            (ConfigToPropose, *configToPropose),
            (PropositionBase, propositionBase),
            (StorageConfig, StorageConfig.get()),
            (ActorId, actorId));

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
            .ActorId = actorId,
            .MindPrev = mindPrev,
        });

        // issue scatter task
        TEvScatter task;
        task.SetTaskId(RandomNumber<ui64>());
        task.MutableProposeStorageConfig()->MutableConfig()->Swap(configToPropose);
        IssueScatterTask(TActorId(), std::move(task));

        return std::nullopt;
    }

    void TDistributedConfigKeeper::CheckForConfigUpdate() {
        Y_ABORT_UNLESS(InvokeQ.empty()); // ensure there is nothing to do in parallel
        if (!StorageConfig || !StorageConfig->GetGeneration() || !Scepter) {
            return;
        }
        if (NKikimrBlobStorage::TStorageConfig config(*StorageConfig); UpdateConfig(&config)) {
            STLOG(PRI_DEBUG, BS_NODE, NWDC63, "CheckForConfigUpdate", (Config, config));
            Invoke(TProposeConfig{
                .Config = std::move(config),
            });
        } else {
            STLOG(PRI_DEBUG, BS_NODE, NWDC83, "CheckForConfigUpdate: no update");
        }
    }

} // NKikimr::NStorage

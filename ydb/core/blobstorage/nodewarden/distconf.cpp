#include "distconf.h"
#include "node_warden_impl.h"
#include <ydb/core/mind/dynamic_nameserver.h>
#include <ydb/core/protos/bridge.pb.h>
#include <ydb/library/yaml_config/yaml_config_helpers.h>
#include <ydb/library/yaml_config/yaml_config.h>
#include <library/cpp/streams/zstd/zstd.h>

namespace NKikimr::NStorage {

    TDistributedConfigKeeper::TDistributedConfigKeeper(TIntrusivePtr<TNodeWardenConfig> cfg,
            std::shared_ptr<const NKikimrBlobStorage::TStorageConfig> baseConfig, bool isSelfStatic)
        : IsSelfStatic(isSelfStatic)
        , Cfg(std::move(cfg))
        , BaseConfig(baseConfig)
        , InitialConfig(std::move(baseConfig))
    {}

    void TDistributedConfigKeeper::Bootstrap() {
        STLOG(PRI_DEBUG, BS_NODE, NWDC00, "Bootstrap");

        auto ns = NNodeBroker::BuildNameserverTable(Cfg->NameserviceConfig);
        auto nodes = MakeIntrusive<TIntrusiveVector<TEvInterconnect::TNodeInfo>>();

        for (const auto& [nodeId, item] : ns->StaticNodeTable) {
            nodes->emplace_back(nodeId, item.Address, item.Host, item.ResolveHost, item.Port, item.Location);
        }

        std::shared_ptr<TEvInterconnect::TEvNodesInfo::TPileMap> pileMap;
        if (AppData()->BridgeConfig && AppData()->BridgeConfig->PilesSize()) {
            pileMap = std::make_shared<TEvInterconnect::TEvNodesInfo::TPileMap>(AppData()->BridgeConfig->PilesSize());

            THashMap<TString, ui32> pileNames;
            for (size_t i = 0; i < AppData()->BridgeConfig->PilesSize(); ++i) {
                pileNames.emplace(AppData()->BridgeConfig->GetPiles(i).GetName(), i);
            }

            for (const auto& item : Cfg->NameserviceConfig.GetNode()) {
                const TNodeLocation location = item.HasLocation() ? TNodeLocation(item.GetLocation())
                    : item.HasWalleLocation() ? TNodeLocation(item.GetWalleLocation())
                    : TNodeLocation();
                if (const auto& bridgePileName = location.GetBridgePileName()) {
                    if (const auto it = pileNames.find(*bridgePileName); it != pileNames.end()) {
                        pileMap->at(it->second).push_back(item.GetNodeId());
                    }
                }
            }
        }

        auto ev = std::make_unique<TEvInterconnect::TEvNodesInfo>(nodes, std::move(pileMap));
        Send(SelfId(), ev.release());

        // and subscribe for the node list too
        Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes(true));

        // generate initial drive set and query stored configuration
        if (IsSelfStatic) {
            if (BaseConfig->GetSelfManagementConfig().GetEnabled()) {
                // read this only if it is possibly enabled
                EnumerateConfigDrives(*InitialConfig, SelfId().NodeId(), [&](const auto& /*node*/, const auto& drive) {
                    DrivesToRead.push_back(drive.GetPath());
                });
                std::sort(DrivesToRead.begin(), DrivesToRead.end());
            }
            ReadConfig();
        } else {
            StorageConfigLoaded = true;
        }

        Become(&TThis::StateWaitForInit);
    }

    void TDistributedConfigKeeper::PassAway() {
        for (const TActorId& actorId : ChildActors) {
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, SelfId(), nullptr, 0));
        }
        TActorBootstrapped::PassAway();
    }

    void TDistributedConfigKeeper::HandleGone(STATEFN_SIG) {
        const size_t numErased = ChildActors.erase(ev->Sender);
        Y_DEBUG_ABORT_UNLESS(numErased == 1);
    }

    void TDistributedConfigKeeper::Halt() {
        // TODO: implement
    }

    bool TDistributedConfigKeeper::ApplyStorageConfig(const NKikimrBlobStorage::TStorageConfig& config) {
        if (!StorageConfig || StorageConfig->GetGeneration() < config.GetGeneration() ||
                (!IsSelfStatic && !config.GetGeneration() && !config.GetSelfManagementConfig().GetEnabled())) {
            // extract the main config from newly applied section
            MainConfigYaml = MainConfigFetchYaml = {};
            MainConfigYamlVersion.reset();
            MainConfigFetchYamlHash = 0;

            if (config.HasConfigComposite()) {
                // parse the composite stream
                auto error = DecomposeConfig(config.GetConfigComposite(), &MainConfigYaml,
                    &MainConfigYamlVersion.emplace(), &MainConfigFetchYaml);
                if (error) {
                    Y_ABORT("ConfigComposite format incorrect: %s", error->data());
                }

                // and _fetched_ config hash
                MainConfigFetchYamlHash = NYaml::GetConfigHash(MainConfigFetchYaml);
            }

            // now extract the additional storage section
            StorageConfigYaml.reset();
            if (config.HasCompressedStorageYaml()) {
                try {
                    TStringInput ss(config.GetCompressedStorageYaml());
                    TZstdDecompress zstd(&ss);
                    StorageConfigYaml.emplace(zstd.ReadAll());
                } catch (const std::exception& ex) {
                    Y_ABORT("CompressedStorageYaml format incorrect: %s", ex.what());
                }
            }

            SelfManagementEnabled = (!IsSelfStatic || BaseConfig->GetSelfManagementConfig().GetEnabled()) &&
                config.GetSelfManagementConfig().GetEnabled() &&
                config.GetGeneration();

            if (config.HasClusterState() && Cfg->BridgeConfig) {
                BridgeInfo = GenerateBridgeInfo(config, Cfg.Get(), SelfNode.NodeId());
            } else {
                Y_ABORT_UNLESS(!BridgeInfo);
            }

            StorageConfig = std::make_shared<NKikimrBlobStorage::TStorageConfig>(config);
            if (ProposedStorageConfig && ProposedStorageConfig->GetGeneration() <= StorageConfig->GetGeneration()) {
                ProposedStorageConfig.reset();
            }

            ReportStorageConfigToNodeWarden(0);

            if (IsSelfStatic) {
                PersistConfig({});
                ApplyConfigUpdateToDynamicNodes(false);
                ConnectToConsole();
                SendConfigProposeRequest();
            }

            return true;
        } else if (StorageConfig->GetGeneration() && StorageConfig->GetGeneration() == config.GetGeneration() &&
                StorageConfig->GetFingerprint() != config.GetFingerprint()) {
            // TODO: fingerprint mismatch, abort operation
        }
        return false;
    }

    void TDistributedConfigKeeper::HandleConfigConfirm(STATEFN_SIG) {
        if (ev->Cookie) {
            STLOG(PRI_DEBUG, BS_NODE, NWDC46, "HandleConfigConfirm", (Cookie, ev->Cookie),
                (ProposedStorageConfigCookie, ProposedStorageConfigCookie),
                (ProposedStorageConfigCookieUsage, ProposedStorageConfigCookieUsage));
            if (ev->Cookie == ProposedStorageConfigCookie && ProposedStorageConfigCookieUsage) {
                --ProposedStorageConfigCookieUsage;
            }
            FinishAsyncOperation(ev->Cookie);
        }
    }

    void TDistributedConfigKeeper::SendEvent(ui32 nodeId, ui64 cookie, TActorId sessionId, std::unique_ptr<IEventBase> ev) {
        Y_ABORT_UNLESS(nodeId != SelfId().NodeId());
        auto handle = std::make_unique<IEventHandle>(MakeBlobStorageNodeWardenID(nodeId), SelfId(), ev.release(), 0, cookie);
        Y_ABORT_UNLESS(sessionId);
        handle->Rewrite(TEvInterconnect::EvForward, sessionId);
        TActivationContext::Send(handle.release());
    }

    void TDistributedConfigKeeper::SendEvent(const TBinding& binding, std::unique_ptr<IEventBase> ev) {
        Y_ABORT_UNLESS(binding.SessionId);
        SendEvent(binding.NodeId, binding.Cookie, binding.SessionId, std::move(ev));
    }

    void TDistributedConfigKeeper::SendEvent(const IEventHandle& handle, std::unique_ptr<IEventBase> ev) {
        SendEvent(handle.Sender.NodeId(), handle.Cookie, handle.InterconnectSession, std::move(ev));
    }

    void TDistributedConfigKeeper::SendEvent(ui32 nodeId, const TBoundNode& info, std::unique_ptr<IEventBase> ev) {
        SendEvent(nodeId, info.Cookie, info.SessionId, std::move(ev));
    }

#ifndef NDEBUG
    void TDistributedConfigKeeper::ConsistencyCheck() {
        for (const auto& [nodeId, info] : DirectBoundNodes) {
            Y_ABORT_UNLESS(std::binary_search(NodeIds.begin(), NodeIds.end(), nodeId));
        }
        if (Binding) {
            Y_ABORT_UNLESS(std::binary_search(NodeIds.begin(), NodeIds.end(), Binding->NodeId));
        }

        for (const auto& [cookie, task] : ScatterTasks) {
            for (const ui32 nodeId : task.PendingNodes) {
                const auto it = DirectBoundNodes.find(nodeId);
                Y_ABORT_UNLESS(it != DirectBoundNodes.end());
                TBoundNode& info = it->second;
                Y_ABORT_UNLESS(info.ScatterTasks.contains(cookie));
            }
        }

        for (const auto& [nodeId, info] : DirectBoundNodes) {
            for (const ui64 cookie : info.ScatterTasks) {
                const auto it = ScatterTasks.find(cookie);
                Y_ABORT_UNLESS(it != ScatterTasks.end());
                TScatterTask& task = it->second;
                Y_ABORT_UNLESS(task.PendingNodes.contains(nodeId));
            }
        }

        for (const auto& [cookie, task] : ScatterTasks) {
            if (task.Origin) {
                Y_ABORT_UNLESS(Binding);
                Y_ABORT_UNLESS(task.Origin == Binding);
            }
        }

        for (const auto& [nodeId, subs] : SubscribedSessions) {
            bool okay = false;
            if (Binding && Binding->NodeId == nodeId) {
                Y_VERIFY_S(subs.SessionId == Binding->SessionId || !Binding->SessionId,
                    "Binding# " << Binding->ToString() << " Subscription# " << subs.ToString());
                okay = true;
            }
            if (const auto it = DirectBoundNodes.find(nodeId); it != DirectBoundNodes.end()) {
                Y_VERIFY_S(!subs.SessionId || subs.SessionId == it->second.SessionId, "sessionId# " << subs.SessionId
                    << " node.SessionId# " << it->second.SessionId);
                okay = true;
            }
            if (!subs.SessionId) {
                okay = true; // may be just obsolete subscription request
            }
            if (ConnectedDynamicNodes.contains(nodeId)) {
                okay = true;
            }
            Y_ABORT_UNLESS(okay);
            if (subs.SubscriptionCookie) {
                const auto it = SubscriptionCookieMap.find(subs.SubscriptionCookie);
                Y_ABORT_UNLESS(it != SubscriptionCookieMap.end());
                Y_ABORT_UNLESS(it->second == nodeId);
            }
        }
        for (const auto& [cookie, nodeId] : SubscriptionCookieMap) {
            const auto it = SubscribedSessions.find(nodeId);
            Y_ABORT_UNLESS(it != SubscribedSessions.end());
            const TSessionSubscription& subs = it->second;
            Y_VERIFY_S(subs.SubscriptionCookie == cookie, "SubscriptionCookie# " << subs.SubscriptionCookie
                << " cookie# " << cookie);
        }

        if (Binding) {
            Y_ABORT_UNLESS(SubscribedSessions.contains(Binding->NodeId));
        }
        for (const auto& [nodeId, info] : DirectBoundNodes) {
            Y_VERIFY_S(SubscribedSessions.contains(nodeId), "NodeId# " << nodeId);
        }

        Y_ABORT_UNLESS(!StorageConfig || CheckFingerprint(*StorageConfig));
        Y_ABORT_UNLESS(!ProposedStorageConfig || CheckFingerprint(*ProposedStorageConfig));
        Y_ABORT_UNLESS(CheckFingerprint(*BaseConfig));
        Y_ABORT_UNLESS(!InitialConfig->GetFingerprint() || CheckFingerprint(*InitialConfig));

        if (Scepter) {
            Y_ABORT_UNLESS(StorageConfig && HasConnectedNodeQuorum(*StorageConfig));
            Y_ABORT_UNLESS(RootState != ERootState::INITIAL && RootState != ERootState::ERROR_TIMEOUT);
            Y_ABORT_UNLESS(!Binding);
        } else {
            Y_ABORT_UNLESS(RootState == ERootState::INITIAL || RootState == ERootState::ERROR_TIMEOUT ||
                ScepterlessOperationInProgress);

            // we can't have connection to the Console without being the root node
            Y_ABORT_UNLESS(!ConsolePipeId);
            Y_ABORT_UNLESS(!ConsoleConnected);
        }
    }
#endif

    STFUNC(TDistributedConfigKeeper::StateWaitForInit) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC53, "StateWaitForInit event", (Type, ev->GetTypeRewrite()),
            (StorageConfigLoaded, StorageConfigLoaded), (NodeListObtained, NodeListObtained),
            (PendingEvents.size, PendingEvents.size()));

        auto processPendingEvents = [&] {
            if (PendingEvents.empty()) {
                Become(&TThis::StateFunc);
            } else {
                TActivationContext::Send(new IEventHandle(TEvPrivate::EvProcessPendingEvent, 0, SelfId(), {}, nullptr, 0));
            }
        };

        bool change = false;
        const bool wasStorageConfigLoaded = StorageConfigLoaded;

        switch (ev->GetTypeRewrite()) {
            case TEvInterconnect::TEvNodesInfo::EventType:
                Handle(reinterpret_cast<TEvInterconnect::TEvNodesInfo::TPtr&>(ev));
                if (!NodeIds.empty() || !IsSelfStatic) {
                    change = !std::exchange(NodeListObtained, true);
                }
                break;

            case TEvPrivate::EvStorageConfigLoaded:
                Handle(reinterpret_cast<TEvPrivate::TEvStorageConfigLoaded::TPtr&>(ev));
                change = wasStorageConfigLoaded < StorageConfigLoaded;
                break;

            case TEvPrivate::EvProcessPendingEvent:
                Y_ABORT_UNLESS(!PendingEvents.empty());
                StateFunc(PendingEvents.front());
                PendingEvents.pop_front();
                processPendingEvents();
                break;

            default:
                PendingEvents.push_back(std::move(ev));
                break;
        }

        if (change && NodeListObtained && StorageConfigLoaded) {
            if (IsSelfStatic) {
                UpdateBound(SelfNode.NodeId(), SelfNode, *StorageConfig, nullptr);
                IssueNextBindRequest();
            }
            processPendingEvents();
        }
    }

    void TDistributedConfigKeeper::ReportStorageConfigToNodeWarden(ui64 cookie) {
        Y_ABORT_UNLESS(StorageConfig);
        const TActorId wardenId = MakeBlobStorageNodeWardenID(SelfId().NodeId());
        const auto& config = SelfManagementEnabled ? StorageConfig : BaseConfig;
        auto proposedConfig = ProposedStorageConfig && SelfManagementEnabled
            ? std::make_shared<NKikimrBlobStorage::TStorageConfig>(*ProposedStorageConfig)
            : nullptr;
        auto ev = std::make_unique<TEvNodeWardenStorageConfig>(config, std::move(proposedConfig), SelfManagementEnabled,
            BridgeInfo);
        Send(wardenId, ev.release(), 0, cookie);
    }

    void TDistributedConfigKeeper::Handle(TEvNodeWardenUpdateConfigFromPeer::TPtr ev) {
        ApplyStorageConfig(ev->Get()->StorageConfig);
    }

    STFUNC(TDistributedConfigKeeper::StateFunc) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC15, "StateFunc", (Type, ev->GetTypeRewrite()), (Sender, ev->Sender),
            (SessionId, ev->InterconnectSession), (Cookie, ev->Cookie));
        const ui32 senderNodeId = ev->Sender.NodeId();
        if (ev->InterconnectSession && SubscribedSessions.contains(senderNodeId)) {
            // keep session actors intact
            SubscribeToPeerNode(senderNodeId, ev->InterconnectSession);
        }
        STRICT_STFUNC_BODY(
            hFunc(TEvNodeConfigPush, Handle);
            hFunc(TEvNodeConfigReversePush, Handle);
            hFunc(TEvNodeConfigUnbind, Handle);
            hFunc(TEvNodeConfigScatter, Handle);
            hFunc(TEvNodeConfigGather, Handle);
            hFunc(TEvNodeConfigInvokeOnRoot, Handle);
            hFunc(TEvInterconnect::TEvNodesInfo, Handle);
            hFunc(TEvInterconnect::TEvNodeConnected, Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
            cFunc(TEvPrivate::EvErrorTimeout, HandleErrorTimeout);
            hFunc(TEvPrivate::TEvStorageConfigLoaded, Handle);
            hFunc(TEvPrivate::TEvStorageConfigStored, Handle);
            fFunc(TEvBlobStorage::EvNodeWardenStorageConfigConfirm, HandleConfigConfirm);
            fFunc(TEvBlobStorage::EvNodeWardenDynamicConfigSubscribe, HandleDynamicConfigSubscribe);
            hFunc(TEvNodeWardenDynamicConfigPush, Handle);
            cFunc(TEvPrivate::EvReconnect, HandleReconnect);
            hFunc(NMon::TEvHttpInfo, Handle);
            fFunc(TEvents::TSystem::Gone, HandleGone);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            cFunc(TEvents::TSystem::Poison, PassAway);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TEvBlobStorage::TEvControllerValidateConfigResponse, Handle);
            hFunc(TEvBlobStorage::TEvControllerProposeConfigResponse, Handle);
            hFunc(TEvBlobStorage::TEvControllerConsoleCommitResponse, Handle);
            hFunc(TEvNodeWardenUpdateCache, Handle);
            hFunc(TEvNodeWardenQueryCache, Handle);
            hFunc(TEvNodeWardenUnsubscribeFromCache, Handle);
            hFunc(TEvNodeWardenUpdateConfigFromPeer, Handle);
            hFunc(TEvNodeWardenManageSyncersResult, Handle);
        )
        for (ui32 nodeId : std::exchange(UnsubscribeQueue, {})) {
            UnsubscribeInterconnect(nodeId);
        }
        if (IsSelfStatic && StorageConfig && NodeListObtained) {
            IssueNextBindRequest();
        }
        ConsistencyCheck();
    }

    void TNodeWarden::StartDistributedConfigKeeper() {
        auto *appData = AppData();
        if (!appData->FeatureFlags.GetForceDistconfDisable()) {
            const bool isSelfStatic = !appData->DynamicNameserviceConfig ||
                SelfId().NodeId() <= appData->DynamicNameserviceConfig->MaxStaticNodeId;
            DistributedConfigKeeperId = Register(new TDistributedConfigKeeper(Cfg, StorageConfig, isSelfStatic));
        }
    }

    void TNodeWarden::ForwardToDistributedConfigKeeper(STATEFN_SIG) {
        ev->Rewrite(ev->GetTypeRewrite(), DistributedConfigKeeperId);
        TActivationContext::Send(ev.Release());
    }


    std::optional<TString> DecomposeConfig(const TString& configComposite, TString *mainConfigYaml,
            ui64 *mainConfigVersion, TString *mainConfigFetchYaml) {
        try {
            TStringInput ss(configComposite);
            TZstdDecompress zstd(&ss);

            TString yaml = TString::Uninitialized(LoadSize(&zstd));
            zstd.LoadOrFail(yaml.Detach(), yaml.size());
            if (mainConfigVersion) {
                auto metadata = NYamlConfig::GetMainMetadata(yaml);
                Y_DEBUG_ABORT_UNLESS(metadata.Version.has_value());
                *mainConfigVersion = metadata.Version.value_or(0);
            }
            if (mainConfigYaml) {
                *mainConfigYaml = std::move(yaml);
            }

            if (mainConfigFetchYaml) {
                *mainConfigFetchYaml = TString::Uninitialized(LoadSize(&zstd));
                zstd.LoadOrFail(mainConfigFetchYaml->Detach(), mainConfigFetchYaml->size());
            }
        } catch (const std::exception& ex) {
            return ex.what();
        }
        return std::nullopt;
    }

    std::optional<TString> UpdateClusterState(NKikimrBlobStorage::TStorageConfig *config) {
        // copy bridge info into state storage configs for easier access in replica processors/proxies
        if (config->HasClusterState()) {
            auto fillInBridge = [&](auto *pb) -> std::optional<TString> {
                auto& clusterState = config->GetClusterState();

                // copy cluster state generation
                pb->SetClusterStateGeneration(clusterState.GetGeneration());
                
                auto &history = config->GetClusterStateHistory();
                if (history.UnsyncedEntriesSize() > 0) {
                    auto &entry = history.GetUnsyncedEntries(0);
                    pb->SetClusterStateGuid(entry.GetOperationGuid());
                } else {
                    pb->SetClusterStateGuid(0);
                }

                if (!pb->RingGroupsSize() || pb->HasRing()) {
                    return "configuration has Ring field set or no RingGroups";
                }

                auto *groups = pb->MutableRingGroups();
                for (int i = 0, count = groups->size(); i < count; ++i) {
                    auto *group = groups->Mutable(i);
                    if (!group->HasBridgePileId()) {
                        return "bridge pile id is not set for a ring group";
                    } else if (ui32 pileId = group->GetBridgePileId(); pileId < clusterState.PerPileStateSize()) {
                        using T = NKikimrConfig::TDomainsConfig::TStateStorage;
                        std::optional<T::EPileState> state;
                        if (pileId == clusterState.GetPrimaryPile()) {
                            state = pileId == clusterState.GetPromotedPile()
                                ? T::PRIMARY
                                : T::DEMOTED;
                        } else if (pileId == clusterState.GetPromotedPile()) {
                            state = T::PROMOTED;
                        } else {
                            switch (clusterState.GetPerPileState(pileId)) {
                                case NKikimrBridge::TClusterState::DISCONNECTED:
                                    state = T::DISCONNECTED;
                                    break;

                                case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED:
                                    state = T::NOT_SYNCHRONIZED;
                                    break;

                                case NKikimrBridge::TClusterState::SYNCHRONIZED:
                                    state = T::SYNCHRONIZED;
                                    break;

                                case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MIN_SENTINEL_DO_NOT_USE_:
                                case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MAX_SENTINEL_DO_NOT_USE_:
                                    Y_DEBUG_ABORT("unexpected value");
                            }
                        }
                        if (!state) {
                            return "can't determine correct pile state for ring group";
                        }
                        group->SetPileState(*state);
                    } else {
                        return "bridge pile id is out of bounds";
                    }
                }
                return std::nullopt;
            };

            std::optional<TString> error;
            if (!error && config->HasStateStorageConfig()) {
                error = fillInBridge(config->MutableStateStorageConfig());
            }
            if (!error && config->HasStateStorageBoardConfig()) {
                error = fillInBridge(config->MutableStateStorageBoardConfig());
            }
            if (!error && config->HasSchemeBoardConfig()) {
                error = fillInBridge(config->MutableSchemeBoardConfig());
            }
            return error;
        }

        return std::nullopt;
    }

    TBridgeInfo::TPtr GenerateBridgeInfo(const NKikimrBlobStorage::TStorageConfig& config, const TNodeWardenConfig *cfg,
            ui32 selfNodeId) {
        const auto& state = config.GetClusterState();

        // prepare empty structure
        auto bridgeInfo = std::make_shared<TBridgeInfo>();
        bridgeInfo->Piles.resize(cfg->BridgeConfig->PilesSize());

        for (const auto& node : config.GetAllNodes()) {
            if (node.HasBridgePileId()) {
                const ui32 nodeId = node.GetNodeId();
                const ui32 pileId = node.GetBridgePileId();
                Y_ABORT_UNLESS(pileId < bridgeInfo->Piles.size());
                auto& pile = bridgeInfo->Piles[pileId];
                pile.StaticNodeIds.push_back(node.GetNodeId());
                bridgeInfo->StaticNodeIdToPile[nodeId] = &pile;
                if (nodeId == selfNodeId) {
                    bridgeInfo->SelfNodePile = &pile;
                }
            }
        }

        if (cfg->DynamicNodeConfig && cfg->DynamicNodeConfig->HasNodeInfo()) {
            const auto& nodeInfo = cfg->DynamicNodeConfig->GetNodeInfo();
            if (!nodeInfo.HasLocation()) {
                Y_ABORT("missing Location in dynamic TNodeInfo");
            }
            const auto& bridgePileName = TNodeLocation(nodeInfo.GetLocation()).GetBridgePileName();
            if (!bridgePileName) {
                Y_ABORT("missing BridgePileName in dynamic TNodeLocation");
            }
            for (ui32 pileId = 0; pileId < AppData()->BridgeConfig->PilesSize(); ++pileId) {
                if (AppData()->BridgeConfig->GetPiles(pileId).GetName() == bridgePileName) {
                    bridgeInfo->SelfNodePile = &bridgeInfo->Piles[pileId];
                    break;
                }
            }
            if (!bridgeInfo->SelfNodePile) {
                Y_ABORT("incorrect bridge pile name in dynamic TNodeLocation: %s", bridgePileName->c_str());
            }
        }

        Y_ABORT_UNLESS(bridgeInfo->SelfNodePile);

        Y_ABORT_UNLESS(state.PerPileStateSize() == cfg->BridgeConfig->PilesSize());
        for (size_t i = 0; i < state.PerPileStateSize(); ++i) {
            auto& pile = bridgeInfo->Piles[i];
            pile.BridgePileId = TBridgePileId::FromValue(i);
            pile.State = state.GetPerPileState(i);
            std::ranges::sort(pile.StaticNodeIds);
        }

        const ui32 primary = state.GetPrimaryPile();
        Y_ABORT_UNLESS(primary < cfg->BridgeConfig->PilesSize());
        bridgeInfo->Piles[primary].IsPrimary = true;
        bridgeInfo->PrimaryPile = &bridgeInfo->Piles[primary];

        if (const ui32 promoted = state.GetPromotedPile(); promoted != primary) {
            Y_ABORT_UNLESS(promoted < cfg->BridgeConfig->PilesSize());
            auto& pile = bridgeInfo->Piles[promoted];
            Y_ABORT_UNLESS(pile.State == NKikimrBridge::TClusterState::SYNCHRONIZED);
            pile.IsBeingPromoted = true;
            bridgeInfo->BeingPromotedPile = &pile;
        }

        return bridgeInfo;
    }

} // NKikimr::NStorage

template<>
void Out<NKikimr::NStorage::TDistributedConfigKeeper::ERootState>(IOutputStream& s, NKikimr::NStorage::TDistributedConfigKeeper::ERootState state) {
    using E = decltype(state);
    switch (state) {
        case E::INITIAL:       s << "INITIAL";       return;
        case E::ERROR_TIMEOUT: s << "ERROR_TIMEOUT"; return;
        case E::IN_PROGRESS:   s << "IN_PROGRESS";   return;
        case E::RELAX:         s << "RELAX";         return;
    }
    Y_ABORT();
}

template<>
void Out<NKikimr::NStorage::TNodeIdentifier>(IOutputStream& s, const NKikimr::NStorage::TNodeIdentifier& value) {
    s << std::get<0>(value) << ':' << std::get<1>(value) << '/' << std::get<2>(value);
}

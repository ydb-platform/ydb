#include "distconf.h"
#include "node_warden_impl.h"
#include <ydb/core/mind/dynamic_nameserver.h>
#include <ydb/core/protos/bridge.pb.h>
#include <ydb/library/yaml_config/yaml_config_helpers.h>
#include <ydb/library/yaml_config/yaml_config.h>
#include <library/cpp/streams/zstd/zstd.h>

namespace NKikimr::NStorage {

    TDistributedConfigKeeper::TDistributedConfigKeeper(TIntrusivePtr<TNodeWardenConfig> cfg,
            TStorageConfigPtr baseConfig, bool isSelfStatic)
        : IsSelfStatic(isSelfStatic)
        , Cfg(std::move(cfg))
        , BaseConfig(baseConfig)
    {
        if (Cfg && Cfg->BridgeConfig) {
            const auto& piles = Cfg->BridgeConfig->GetPiles();
            for (int i = 0; i < piles.size(); ++i) {
                const auto [it, inserted] = BridgePileNameMap.emplace(piles[i].GetName(), TBridgePileId::FromPileIndex(i));
                Y_ABORT_UNLESS(inserted);
            }
        }
    }

    void TDistributedConfigKeeper::Bootstrap() {
        STLOG(PRI_DEBUG, BS_NODE, NWDC00, "Bootstrap");

        auto ns = NNodeBroker::BuildNameserverTable(Cfg->NameserviceConfig);
        auto nodes = MakeIntrusive<TIntrusiveVector<TEvInterconnect::TNodeInfo>>();

        for (const auto& [nodeId, item] : ns->StaticNodeTable) {
            nodes->emplace_back(nodeId, item.Address, item.Host, item.ResolveHost, item.Port, item.Location);
        }

        std::shared_ptr<TEvInterconnect::TEvNodesInfo::TPileMap> pileMap;
        if (AppData()->BridgeModeEnabled) {
            const auto& bridge = AppData()->BridgeConfig;
            pileMap = std::make_shared<TEvInterconnect::TEvNodesInfo::TPileMap>(bridge.PilesSize());

            THashMap<TString, ui32> pileNames;
            for (size_t i = 0; i < bridge.PilesSize(); ++i) {
                pileNames.emplace(bridge.GetPiles(i).GetName(), i);
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
            PrevDrivesToRead = GetDrives(*BaseConfig);
            ReadConfig(PrevDrivesToRead);
        } else {
            StorageConfigLoaded = true;
        }

        Become(&TThis::StateWaitForInit);
    }

    void TDistributedConfigKeeper::PassAway() {
        for (const auto& item : InvokeQ) {
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, item.ActorId, SelfId(), nullptr, 0));
        }
        TActorBootstrapped::PassAway();
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

            if (Cfg->BridgeConfig) {
                BridgeInfo = GenerateBridgeInfo(config);
            } else {
                Y_ABORT_UNLESS(!BridgeInfo);
            }

            StorageConfig = std::make_shared<NKikimrBlobStorage::TStorageConfig>(config);

            if (IsSelfStatic) {
                ApplyConfigUpdateToDynamicNodes(false);
                ConnectToConsole();
                SendConfigProposeRequest();
            }

            std::vector<std::tuple<TNodeIdentifier, TNodeLocation>> newNodeList;
            newNodeList.reserve(StorageConfig->AllNodesSize());
            for (const auto& node : StorageConfig->GetAllNodes()) {
                newNodeList.emplace_back(node, node.GetLocation());
            }
            if (!newNodeList.empty()) {
                ApplyNewNodeList(newNodeList);
            }

            QuorumValid = false;

            if (BridgeInfo && !BridgeInfo->SelfNodePile->IsPrimary) {
                UnbindNodesFromOtherPiles("not primary pile anymore");
            }

            return true;
        } else if (StorageConfig->GetGeneration() && StorageConfig->GetGeneration() == config.GetGeneration() &&
                StorageConfig->GetFingerprint() != config.GetFingerprint()) {
            // TODO: fingerprint mismatch, abort operation
        }
        return false;
    }

    void TDistributedConfigKeeper::ApplyCommittedStorageConfig(const NKikimrBlobStorage::TStorageConfig& config) {
        FanOutReversePush(&config); // send this configuration to all direct bound nodes (before they get possibly unbound)
        ApplyStorageConfig(config);

        if (!CommittedStorageConfig || CommittedStorageConfig->GetGeneration() < config.GetGeneration()) {
            // there can be cases when config has been edited manually and has greater version than provided by the leader
            LocalCommittedStorageConfig = CommittedStorageConfig = std::make_shared<NKikimrBlobStorage::TStorageConfig>(config);

            std::vector<TString> drives;
            EnumerateConfigDrives(config, SelfId().NodeId(), [&](auto& /*node*/, auto& drive) {
                const TString& path = drive.GetPath();
                if (NKikimrBlobStorage::TPDiskMetadataRecord& m = MetadataByPath[path]; !m.HasCommittedStorageConfig()) {
                    m.MutableCommittedStorageConfig()->CopyFrom(config);
                } else if (const auto& committed = m.GetCommittedStorageConfig(); committed.GetGeneration() < config.GetGeneration()) {
                    m.MutableCommittedStorageConfig()->CopyFrom(config);
                } else if (config.GetGeneration() < committed.GetGeneration()) {
                    Y_DEBUG_ABORT(); // this is a bit very odd
                    return;
                } else if (config.GetFingerprint() != committed.GetFingerprint()) {
                    Y_ABORT("config fingerprint mismatch");
                } else {
                    return;
                }
                drives.push_back(path);
            });
            if (!drives.empty()) {
                PersistConfig({}, drives); // persist committed storage config
            }
        } else {
            Y_DEBUG_ABORT_UNLESS(StorageConfig->GetGeneration() == CommittedStorageConfig->GetGeneration());
        }
    }

    void TDistributedConfigKeeper::Handle(TEvNodeWardenUpdateConfigFromPeer::TPtr ev) {
        auto& msg = *ev->Get();

        // descend config through root if it is committed
        if (IsSelfStatic && msg.CommittedConfig) {
            auto query = std::make_unique<TEvNodeConfigInvokeOnRoot>();
            auto *cmd = query->Record.MutableDescendCommittedStorageConfig();
            cmd->MutableCommittedStorageConfig()->CopyFrom(*msg.CommittedConfig);
            Send(SelfId(), query.release());
        }

        // apply volatile config locally
        ApplyStorageConfig(msg.Config);
    }

    void TDistributedConfigKeeper::HandleConfigConfirm(STATEFN_SIG) {
        Y_UNUSED(ev);
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
        for (const auto& [nodeId, info] : DirectBoundNodes) { // validate incoming binding
            if (std::ranges::binary_search(NodeIdsForIncomingBinding, nodeId) ||
                    std::ranges::binary_search(NodeIdsForOutgoingBinding, nodeId)) {
                continue; // okay
            } else if (BridgeInfo && BridgeInfo->SelfNodePile->IsPrimary && !NodesFromSamePile.contains(nodeId) &&
                    AllNodeIds.contains(nodeId) && (!Binding || !Binding->RootNodeId)) {
                continue; // okay too -- other pile connecting to primary
            }
            Y_ABORT_S("unexpected incoming bound node NodeId# " << nodeId
                << " NodeIdsForIncomingBinding# " << FormatList(NodeIdsForIncomingBinding)
                << " NodeIdsForOutgoingBinding# " << FormatList(NodeIdsForOutgoingBinding)
                << " NodesFromSamePile# " << FormatList(NodesFromSamePile)
                << " Binding# " << (Binding ? Binding->ToString() : "<null>"));
        }
        if (Binding) { // validate outgoing binding
            Y_ABORT_UNLESS(std::ranges::binary_search(NodeIdsForOutgoingBinding, Binding->NodeId) ||
                std::ranges::binary_search(NodeIdsForIncomingBinding, Binding->NodeId) ||
                std::ranges::binary_search(NodeIdsForPrimaryPileOutgoingBinding, Binding->NodeId));
        }

        for (const auto& [cookie, task] : ScatterTasks) {
            for (const ui32 nodeId : task.PendingNodes) {
                if (const auto it = DirectBoundNodes.find(nodeId); it != DirectBoundNodes.end()) {
                    TBoundNode& info = it->second;
                    Y_ABORT_UNLESS(info.ScatterTasks.contains(cookie));
                } else {
                    Y_ABORT_UNLESS(AddedNodesScatterTasks.contains({nodeId, cookie}));
                }
            }
        }

        for (const auto& [nodeId, cookie] : AddedNodesScatterTasks) {
            const auto it = ScatterTasks.find(cookie);
            Y_ABORT_UNLESS(it != ScatterTasks.end());
            TScatterTask& task = it->second;
            Y_ABORT_UNLESS(task.PendingNodes.contains(nodeId));
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
            std::visit(TOverloaded{
                [&](const TBinding& origin) { Y_ABORT_UNLESS(origin == Binding); },
                [&](const TActorId& /*actorId*/) { Y_ABORT_UNLESS(!Binding); },
                [&](const TScatterTaskOriginFsm&) {},
                [&](const TScatterTaskOriginTargeted&) {}
            }, task.Origin);
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
            if (UnsubscribeQueue.contains(nodeId)) {
                okay = true;
            }
            if (!okay) {
                const auto it = AddedNodesScatterTasks.lower_bound({nodeId, 0});
                okay = it != AddedNodesScatterTasks.end() && std::get<0>(*it) == nodeId;
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

        Y_ABORT_UNLESS(BaseConfig);
        Y_ABORT_UNLESS(CheckFingerprint(*BaseConfig));

        Y_ABORT_UNLESS(!StorageConfig || CheckFingerprint(*StorageConfig));

        if (CommittedStorageConfig) {
            Y_ABORT_UNLESS(CheckFingerprint(*CommittedStorageConfig));
            Y_ABORT_UNLESS(StorageConfig);
            Y_ABORT_UNLESS(CommittedStorageConfig->GetGeneration() <= StorageConfig->GetGeneration());
            Y_ABORT_UNLESS(LocalCommittedStorageConfig);
            Y_ABORT_UNLESS(LocalCommittedStorageConfig == CommittedStorageConfig);
        } else if (LocalCommittedStorageConfig) {
            Y_ABORT_UNLESS(CheckFingerprint(*LocalCommittedStorageConfig));
            Y_ABORT_UNLESS(StorageConfig);
            Y_ABORT_UNLESS(LocalCommittedStorageConfig->GetGeneration() <= StorageConfig->GetGeneration());
        }

        if (IsSelfStatic && StorageConfig && NodeListObtained) {
            Y_VERIFY_S(HasConnectedNodeQuorum(*StorageConfig) == GlobalQuorum, "GlobalQuorum# " << GlobalQuorum);
            //Y_VERIFY_S((BridgeInfo && HasConnectedNodeQuorum(*StorageConfig, true)) == LocalPileQuorum,
            //    "LocalPileQuorum# " << LocalPileQuorum);
        }

        if (Scepter) {
            Y_ABORT_UNLESS(StorageConfig);
            Y_ABORT_UNLESS(GlobalQuorum);
            Y_VERIFY_S(RootState != ERootState::INITIAL && RootState != ERootState::ERROR_TIMEOUT, "RootState# " << RootState);
            Y_ABORT_UNLESS(!Binding);
        } else {
            Y_VERIFY_S(RootState == ERootState::INITIAL || RootState == ERootState::ERROR_TIMEOUT, "RootState# " << RootState);

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
            hFunc(TEvInterconnect::TEvNodesInfo, [&](auto& ev) {
                if (!ev->Get()->NodesPtr->empty()) {
                    Handle(ev);
                    change = !std::exchange(NodeListObtained, true);
                }
            })

            hFunc(TEvPrivate::TEvStorageConfigLoaded, [&](auto& ev) {
                Handle(ev);
                change = wasStorageConfigLoaded < StorageConfigLoaded;
            });

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
                UpdateQuorums();
                IssueNextBindRequest();
            }
            processPendingEvents();
        }
    }

    void TDistributedConfigKeeper::ReportStorageConfigToNodeWarden() {
        Y_ABORT_UNLESS(StorageConfig);
        auto t = std::make_tuple(SelfManagementEnabled ? StorageConfig : BaseConfig, SelfManagementEnabled, BridgeInfo,
            CommittedStorageConfig);
        if (t != LastReportedStorageConfig) {
            Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), std::apply([&](auto&&... args) {
                return new TEvNodeWardenStorageConfig(std::forward<decltype(args)>(args)...);
            }, t));
            LastReportedStorageConfig = std::move(t);
        }
    }

    STFUNC(TDistributedConfigKeeper::StateFunc) {
        const ui32 type = ev->GetTypeRewrite();
        THPTimer timer;
        Y_DEFER {
            if (auto duration = TDuration::Seconds(timer.Passed()); duration >= TDuration::MilliSeconds(5)) {
                STLOG(PRI_WARN, BS_NODE, NWDC01, "StateFunc too long", (Type, type), (Duration, duration));
            }
        };
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
            hFunc(TEvNodeConfigInvokeOnRoot, HandleInvokeOnRoot);
            IgnoreFunc(TEvNodeConfigInvokeOnRootResult);
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
            fFunc(TEvPrivate::EvQueryFinished, HandleQueryFinished);
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
            fFunc(TEvPrivate::EvRetryCollectConfigsAndPropose, HandleRetryCollectConfigsAndPropose);
            cFunc(TEvPrivate::EvRetryPersistConfig, HandleRetryPersistConfig);
        )
        for (ui32 nodeId : std::exchange(UnsubscribeQueue, {})) {
            UnsubscribeInterconnect(nodeId);
        }
        if (IsSelfStatic && StorageConfig && NodeListObtained) {
            UpdateQuorums();
            IssueNextBindRequest();
            CheckRootNodeStatus();
        }
        if (StorageConfig && NodeListObtained) {
            ReportStorageConfigToNodeWarden();
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

    TBridgeInfo::TPtr TDistributedConfigKeeper::GenerateBridgeInfo(const NKikimrBlobStorage::TStorageConfig& config) {
        // prepare empty structure
        auto bridgeInfo = std::make_shared<TBridgeInfo>();
        const size_t numPiles = Cfg->BridgeConfig->PilesSize();
        bridgeInfo->Piles.resize(numPiles);

        for (const auto& node : config.GetAllNodes()) {
            const TBridgePileId bridgePileId = ResolveNodePileId(TNodeLocation(node.GetLocation()));
            Y_ABORT_UNLESS(bridgePileId);
            auto& pile = bridgeInfo->Piles[bridgePileId.GetPileIndex()];
            const ui32 nodeId = node.GetNodeId();
            pile.StaticNodeIds.push_back(node.GetNodeId());
            bridgeInfo->StaticNodeIdToPile[nodeId] = &pile;
            if (nodeId == SelfNode.NodeId()) {
                bridgeInfo->SelfNodePile = &pile;
            }
        }

        if (Cfg->DynamicNodeConfig && Cfg->DynamicNodeConfig->HasNodeInfo()) {
            const auto& nodeInfo = Cfg->DynamicNodeConfig->GetNodeInfo();
            if (!nodeInfo.HasLocation()) {
                Y_ABORT("missing Location in dynamic TNodeInfo");
            }
            const auto& bridgePileName = TNodeLocation(nodeInfo.GetLocation()).GetBridgePileName();
            if (!bridgePileName) {
                Y_ABORT("missing BridgePileName in dynamic TNodeLocation");
            }
            const auto it = BridgePileNameMap.find(*bridgePileName);
            Y_ABORT_UNLESS(it != BridgePileNameMap.end(), "incorrect bridge pile name in dynamic TNodeLocation: %s",
                bridgePileName->c_str());
            bridgeInfo->SelfNodePile = &bridgeInfo->Piles[it->second.GetPileIndex()];
        }

        Y_VERIFY_S(bridgeInfo->SelfNodePile, "SelfNodeId# " << SelfNode.NodeId() << " Config# " << SingleLineProto(config));

        Y_ABORT_UNLESS(config.HasClusterState());
        const NKikimrBridge::TClusterState& state = config.GetClusterState();

        Y_ABORT_UNLESS(state.PerPileStateSize() == numPiles);
        for (size_t i = 0; i < numPiles; ++i) {
            auto& pile = bridgeInfo->Piles[i];
            pile.BridgePileId = TBridgePileId::FromPileIndex(i);
            pile.Name = Cfg->BridgeConfig->GetPiles(i).GetName();
            pile.State = state.GetPerPileState(i);
            std::ranges::sort(pile.StaticNodeIds);
        }

        const TBridgePileId primary = TBridgePileId::FromProto(&state, &NKikimrBridge::TClusterState::GetPrimaryPile);
        Y_ABORT_UNLESS(primary.GetPileIndex() < numPiles);
        auto& pile = bridgeInfo->Piles[primary.GetPileIndex()];
        pile.IsPrimary = true;
        bridgeInfo->PrimaryPile = &pile;

        const TBridgePileId promoted = TBridgePileId::FromProto(&state, &NKikimrBridge::TClusterState::GetPromotedPile);
        if (promoted != primary) {
            Y_ABORT_UNLESS(promoted.GetPileIndex() < numPiles);
            auto& pile = bridgeInfo->Piles[promoted.GetPileIndex()];
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
        case E::INITIAL:       s << "INITIAL";         return;
        case E::ERROR_TIMEOUT: s << "ERROR_TIMEOUT";   return;
        case E::IN_PROGRESS:   s << "IN_PROGRESS";     return;
        case E::RELAX:         s << "RELAX";           return;
    }
    Y_ABORT();
}

template<>
void Out<NKikimr::NStorage::TNodeIdentifier>(IOutputStream& s, const NKikimr::NStorage::TNodeIdentifier& value) {
    s << std::get<0>(value) << ':' << std::get<1>(value) << '/' << std::get<2>(value);
}

#include "distconf.h"

#define YDB_LOG_THIS_FILE_COMPONENT BS_NODE

namespace NKikimr::NStorage {

    class TDistributedConfigKeeper::TDistconfBridgeConnectionCheckerActor
        : public TActorBootstrapped<TDistconfBridgeConnectionCheckerActor>
    {
        TBridgePileId SelfBridgePileId; // resolved against local bridge config until storage config is received
        const std::optional<TString> SelfBridgePileName;

        TStorageConfigPtr StorageConfig;
        TStorageConfigPtr CommittedStorageConfig;
        TBridgeInfo::TPtr BridgeInfo;
        bool SelfManagementEnabled = false;
        bool IsSelfStatic = false;
        ui32 MaxStaticNodeId = Max<ui32>();

        std::deque<std::unique_ptr<IEventHandle>> PendingEvents;

        // peers which haven't reported in the handshake of their session that they take pile ids from pile registry;
        // the sessions are not checked again, so they are disconnected once the list of piles has been changed
        THashSet<ui32> PileListChangeIncapablePeers;

        static constexpr TStringBuf DistconfKey = "distconf";
        // the payload is sent with this key once the list of piles has been changed; older versions don't know it, so
        // they treat such peer as not bridge-capable one and neither connect, nor take its configuration
        static constexpr TStringBuf PileListChangedDistconfKey = "distconf-pile-list-changed";

        using TClusterState = NKikimrBridge::TClusterState;

    public:
        TDistconfBridgeConnectionCheckerActor(TBridgePileId selfBridgePileId, std::optional<TString> selfBridgePileName)
            : SelfBridgePileId(selfBridgePileId)
            , SelfBridgePileName(std::move(selfBridgePileName))
        {}

        void Bootstrap() {
            // determine maximum static node id to distinguish static and dynamic nodes when handling connections
            if (const auto& dynconfig = AppData()->DynamicNameserviceConfig) {
                MaxStaticNodeId = dynconfig->MaxStaticNodeId;
            }

            // check for ourself -- is this static or dynamic node?
            IsSelfStatic = SelfId().NodeId() <= MaxStaticNodeId;
            Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvNodeWardenQueryStorageConfig(true));
            Become(&TThis::StateFunc);
        }

        void Handle(TEvNodeWardenStorageConfig::TPtr ev) {
            // store just received parameters
            auto& msg = *ev->Get();
            StorageConfig = std::move(msg.Config);
            CommittedStorageConfig = std::move(msg.CommittedConfig);
            BridgeInfo = std::move(msg.BridgeInfo);
            SelfManagementEnabled = msg.SelfManagementEnabled;

            // ensure they all are filled in correctly
            Y_ABORT_UNLESS(StorageConfig);
            Y_ABORT_UNLESS(AppData()->BridgeModeEnabled);
            Y_ABORT_UNLESS(BridgeInfo);

            // pile ids come from pile registry of the storage config, which may differ from the local config order
            Y_ABORT_UNLESS(BridgeInfo->SelfNodePile);
            SelfBridgePileId = BridgeInfo->SelfNodePile->BridgePileId;

            YDB_LOG_DEBUG("TEvNodeWardenStorageConfig received",
                {"marker", "NWDCC06"},
                {"storageConfig", StorageConfig});

            // process any pending events
            for (auto& ev : std::exchange(PendingEvents, {})) {
                TAutoPtr<IEventHandle> temp(ev.release());
                Receive(temp);
            }

            // peers of older versions can't work with the configuration once the list of piles has been changed
            if (IsPileListChanged(*StorageConfig)) {
                for (const ui32 peerNodeId : std::exchange(PileListChangeIncapablePeers, {})) {
                    DisconnectIncapablePeer(peerNodeId);
                }
            }

            // disconnect peers if needed
            TActorSystem* const as = TActivationContext::ActorSystem();
            for (const auto& pile : BridgeInfo->Piles) {
                if (NBridge::PileStateTraits(pile.State).AllowsConnection &&
                        NBridge::PileStateTraits(BridgeInfo->SelfNodePile->State).AllowsConnection) {
                    continue;
                }
                if (pile.BridgePileId == SelfBridgePileId &&
                        NBridge::PileStateTraits(pile.State).AllowsConnectionWithinPile) {
                    continue; // do not disconnect from the same pile
                }
                for (const ui32 nodeId : pile.StaticNodeIds) {
                    if (nodeId == SelfId().NodeId()) {
                        continue;
                    }
                    YDB_LOG_DEBUG("Disconnecting",
                        {"marker", "NWDCC00"},
                        {"peerNodeId", nodeId},
                        {"bridgePileId", pile.BridgePileId});
                    as->Send(new IEventHandle(TEvInterconnect::EvDisconnect, 0, as->InterconnectProxy(nodeId),
                        {}, nullptr, 0));
                }
                // request nameservice for node list to disconnect from dynamic nodes too
                Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes);
            }
        }

        void Handle(TEvInterconnect::TEvNodesInfo::TPtr ev) {
            Y_ABORT_UNLESS(BridgeInfo);
            TActorSystem* const as = TActivationContext::ActorSystem();

            // piles of dynamic nodes are resolved against our bridge info, as the pile map of the nameservice may be
            // built against another configuration (the one before or after ours)
            for (const auto& node : ev->Get()->Nodes) {
                if (node.NodeId <= MaxStaticNodeId) {
                    continue; // static nodes were already processed
                }
                const auto *pile = BridgeInfo->ResolvePile(node.Location.GetBridgePileName());
                if (!pile || (NBridge::PileStateTraits(pile->State).AllowsConnection &&
                        NBridge::PileStateTraits(BridgeInfo->SelfNodePile->State).AllowsConnection)) {
                    continue;
                }
                YDB_LOG_DEBUG("Disconnecting dynamic",
                    {"marker", "NWDCC03"},
                    {"peerNodeId", node.NodeId},
                    {"bridgePileId", pile->BridgePileId});
                as->Send(new IEventHandle(TEvInterconnect::EvDisconnect, 0, as->InterconnectProxy(node.NodeId),
                    {}, nullptr, 0));
            }
        }

        THashMap<TString, TString> CreateParams(const NKikimrBlobStorage::TConnectivityPayload& outgoing) {
            THashMap<TString, TString> res;
            if (outgoing.ByteSizeLong()) {
                const TStringBuf key = StorageConfig && IsPileListChanged(*StorageConfig)
                    ? PileListChangedDistconfKey
                    : DistconfKey;
                const bool success = outgoing.SerializeToString(&res[key]);
                Y_ABORT_UNLESS(success);
            }
            return res;
        }

        // returns false if the payload is present, but can't be parsed
        static bool ParsePayload(const THashMap<TString, TString>& params,
                NKikimrBlobStorage::TConnectivityPayload *incoming) {
            for (const TStringBuf key : {PileListChangedDistconfKey, DistconfKey}) {
                if (const auto it = params.find(key); it != params.end()) {
                    return incoming->ParseFromString(it->second);
                }
            }
            return true;
        }

        // peer of older version can't work with the configuration once the list of piles has been changed
        bool IsPeerIncompatible(const NKikimrBlobStorage::TConnectivityPayload& incoming) const {
            return StorageConfig && IsPileListChanged(*StorageConfig) && !incoming.GetPileListChangeCapable();
        }

        // remember whether the peer of just established session can work with the changed list of piles
        void RegisterPeer(ui32 peerNodeId, const NKikimrBlobStorage::TConnectivityPayload& incoming) {
            if (incoming.GetPileListChangeCapable()) {
                PileListChangeIncapablePeers.erase(peerNodeId);
            } else if (IsPeerIncompatible(incoming)) {
                DisconnectIncapablePeer(peerNodeId);
            } else {
                PileListChangeIncapablePeers.insert(peerNodeId);
            }
        }

        void DisconnectIncapablePeer(ui32 peerNodeId) {
            YDB_LOG_WARN("Disconnecting peer running version not supporting changes of the list of piles",
                {"marker", "NWDCC08"},
                {"peerNodeId", peerNodeId});
            TActivationContext::Send(new IEventHandle(TEvInterconnect::EvDisconnect, 0,
                TActivationContext::InterconnectProxy(peerNodeId), {}, nullptr, 0));
        }

        void Handle(TEvInterconnect::TEvPrepareOutgoingConnection::TPtr ev) {
            if (IsSelfStatic && !StorageConfig) {
                // we are still waiting for storage config at static node, so postpone this message
                PendingEvents.emplace_back(ev.Release());
                return;
            }

            std::optional<TString> error;
            NKikimrBlobStorage::TConnectivityPayload outgoing;

            // definitely fill out self pile id and our configuration to connect with peer
            FillInSelfPile(&outgoing);
            if (StorageConfig) {
                outgoing.MutableStorageConfig()->CopyFrom(*StorageConfig);
            }
            if (CommittedStorageConfig) {
                outgoing.MutableCommittedStorageConfig()->CopyFrom(*CommittedStorageConfig);
            }

            const ui32 peerNodeId = ev->Get()->PeerNodeId;

            if (IsSelfStatic) {
                // we validate peer before starting connection only if we are at a static node -- dynamic nodes may not
                // know peer states when running as they may haven't received any configuration yet
                const auto *pile = BridgeInfo->GetPileForNode(peerNodeId);
                if (!pile) {
                    // peer node is a dynamic node
                } else if (pile == BridgeInfo->SelfNodePile && IsSelfStatic && peerNodeId <= MaxStaticNodeId &&
                        NBridge::PileStateTraits(pile->State).AllowsConnectionWithinPile) {
                    // allow connecting static nodes within the same pile, even disconnected
                } else if (!NBridge::PileStateTraits(pile->State).AllowsConnection) {
                    error = "can't establish connection to node belonging to disconnected pile";
                }
            }

            YDB_LOG_DEBUG("Handle TEvPrepareOutgoingConnection",
                {"marker", "NWDCC01"},
                {"peerNodeId", peerNodeId},
                {"error", error},
                {"outgoing", outgoing});

            if (error) {
                Send(ev->Sender, new TEvInterconnect::TEvPrepareOutgoingConnectionResult(std::move(*error)), 0, ev->Cookie);
            } else {
                Send(ev->Sender, new TEvInterconnect::TEvPrepareOutgoingConnectionResult(CreateParams(outgoing)), 0, ev->Cookie);
            }
        }

        void Handle(TEvInterconnect::TEvCheckIncomingConnection::TPtr ev) {
            if (IsSelfStatic && !StorageConfig) {
                // the same logic as for outgoing connection: postpone only for static nodes when no configuration has
                // been received
                PendingEvents.emplace_back(ev.Release());
                return;
            }

            std::optional<TString> error;
            NKikimrBlobStorage::TConnectivityPayload outgoing;
            outgoing.SetPileListChangeCapable(true); // the peer takes our config only if we report it

            const ui32 peerNodeId = ev->Get()->PeerNodeId;

            // extract the payload, if we have one
            NKikimrBlobStorage::TConnectivityPayload incoming;
            auto& params = ev->Get()->Params;
            if (!ParsePayload(params, &incoming)) {
                error = "failed to parse incoming connectivity check payload";
            }

            // obtain peer's pile id (we must have one)
            TBridgePileId peerBridgePileId;
            if (BridgeInfo) { // this may be null if this is dynamic node
                if (const auto *pile = BridgeInfo->GetPileForNode(peerNodeId)) {
                    peerBridgePileId = pile->BridgePileId;
                }
            }
            if (error) {
                // we already have an error, do nothing more
            } else if (!incoming.HasBridgePileId()) {
                // peer is not bridge-capable; this is allowed only until the cluster gets converted to bridged one
                if (!StorageConfig || !StorageConfig->HasClusterState()) {
                    YDB_LOG_DEBUG("Accepting connection from not bridge-capable peer",
                        {"marker", "NWDCC07"},
                        {"peerNodeId", peerNodeId});
                    RegisterPeer(peerNodeId, incoming);
                    Send(ev->Sender, new TEvInterconnect::TEvCheckIncomingConnectionResult(std::nullopt, {}), 0,
                        ev->Cookie);
                    return;
                }
                error = "missing mandatory peer bridge pile id";
            } else if (IsPeerIncompatible(incoming)) {
                error = "peer runs version not supporting changes of the list of piles, it has to be upgraded";
            } else if (const auto value = ResolvePeerPile(incoming); !value) {
                error = "can't resolve peer bridge pile";
            } else if (peerBridgePileId && peerBridgePileId != value) {
                // not the one we expect from this node
                error = "incorrect peer bridge pile id provided";
            } else if ((BridgeInfo ? BridgeInfo->Piles.size() : AppData()->BridgeConfig.PilesSize()) <= value.GetPileIndex()) {
                // out of bounds
                error = "peer bridge pile id out of range";
            } else {
                peerBridgePileId = value;
            }

            const bool isPeerStatic = peerNodeId <= MaxStaticNodeId;
            const bool fromSamePile = SelfBridgePileId == peerBridgePileId;
            const bool hasPeerConfig = incoming.HasStorageConfig();
            const bool hasSelfConfig = static_cast<bool>(StorageConfig);

            if (error) {
                // already have an error
            } else if (isPeerStatic && !hasPeerConfig) {
                error = "missing mandatory peer storage configuration section in handshake";
            } else if (IsSelfStatic && !StorageConfig) {
                Y_ABORT();
            }

            // process the peer's configuration
            bool configChecked = false;
            if (error) {
                // we already have an error
            } else if (hasPeerConfig && hasSelfConfig) {
                error = CheckPeerConfig(peerNodeId, peerBridgePileId, incoming, &outgoing);
                configChecked = true;
            } else if (hasSelfConfig < hasPeerConfig) {
                UpdateLocalConfig(incoming);
            } else if (hasPeerConfig < hasSelfConfig) {
                outgoing.MutableStorageConfig()->CopyFrom(*StorageConfig);
                if (CommittedStorageConfig) {
                    outgoing.MutableCommittedStorageConfig()->CopyFrom(*CommittedStorageConfig);
                }
            }

            if (!error && !configChecked && !fromSamePile) {
                error = TStringBuilder() << "can't determine if two dynamic nodes from different piles can connect"
                    << " hasSelfConfig# " << hasSelfConfig
                    << " hasPeerConfig# " << hasPeerConfig
                    << " isSelfStatic# " << IsSelfStatic
                    << " isPeerStatic# " << isPeerStatic;
            }

            YDB_LOG_DEBUG("Handle TEvCheckIncomingConnection",
                {"marker", "NWDCC02"},
                {"peerNodeId", peerNodeId},
                {"error", error},
                {"outgoing", outgoing});

            if (!error) {
                RegisterPeer(peerNodeId, incoming);
            }

            Send(ev->Sender, new TEvInterconnect::TEvCheckIncomingConnectionResult(std::move(error),
                CreateParams(outgoing)), 0, ev->Cookie);
        }

        void FillInSelfPile(NKikimrBlobStorage::TConnectivityPayload *outgoing) {
            SelfBridgePileId.CopyToProto(outgoing, &NKikimrBlobStorage::TConnectivityPayload::SetBridgePileId);
            outgoing->SetPileListChangeCapable(true);
            if (SelfBridgePileName) {
                outgoing->SetBridgePileName(*SelfBridgePileName);
            }
        }

        TBridgePileId ResolvePeerPile(const NKikimrBlobStorage::TConnectivityPayload& incoming) {
            const auto bridgePileId = TBridgePileId::FromProto(&incoming,
                &NKikimrBlobStorage::TConnectivityPayload::GetBridgePileId);
            if (!incoming.HasBridgePileName() || !BridgeInfo) {
                // peer of older version (or the one belonging to the implicit pile) provides only the pile id, which
                // matches pile registry until it differs from the one implied by bridge config; also we can't resolve
                // pile names until we get storage config
                return bridgePileId;
            } else if (const auto *pile = BridgeInfo->ResolvePile(incoming.GetBridgePileName())) {
                return pile->BridgePileId;
            } else {
                return TBridgePileId();
            }
        }

        void Handle(TEvInterconnect::TEvNotifyOutgoingConnectionEstablished::TPtr ev) {
            auto& params = ev->Get()->Params;
            NKikimrBlobStorage::TConnectivityPayload incoming;
            if (!ParsePayload(params, &incoming)) {
                incoming.Clear(); // the peer is treated as not reporting anything
            } else if (!IsPeerIncompatible(incoming) && incoming.HasStorageConfig()) {
                YDB_LOG_DEBUG("Applying config from TEvNotifyOutgoingConnectionEstablished",
                    {"marker", "NWDCC04"},
                    {"config", incoming.GetStorageConfig()});
                UpdateLocalConfig(incoming);
            }
            if (ev->Get()->Success) {
                RegisterPeer(ev->Get()->PeerNodeId, incoming);
            }
        }

        void UpdateLocalConfig(const NKikimrBlobStorage::TConnectivityPayload& incoming) {
            if (IsSelfStatic && StorageConfig && !StorageConfig->GetGeneration() &&
                    std::ranges::none_of(incoming.GetStorageConfig().GetAllNodes(), [this](const auto& node) {
                        return node.GetNodeId() == SelfId().NodeId();
                    })) {
                return;
            }
            Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvNodeWardenUpdateConfigFromPeer(
                incoming.GetStorageConfig(),
                incoming.HasCommittedStorageConfig()
                    ? std::make_optional(incoming.GetCommittedStorageConfig())
                    : std::nullopt
            ));
        }

        std::optional<TString> CheckPeerConfig(ui32 peerNodeId, TBridgePileId peerBridgePileId,
                const NKikimrBlobStorage::TConnectivityPayload& incoming,
                NKikimrBlobStorage::TConnectivityPayload *outgoing) {
            const NKikimrBlobStorage::TStorageConfig& config = incoming.GetStorageConfig();
            Y_ABORT_UNLESS(StorageConfig);
            Y_ABORT_UNLESS(BridgeInfo);

            if (!StorageConfig->HasClusterState() || !config.HasClusterState()) {
                // at least one of parties runs config of the cluster not yet converted to bridged one; this is possible
                // only for single-pile configuration and the party with older config will catch up through the
                // config exchange
                std::optional<TString> error;
                if (StorageConfig->HasClusterState() && StorageConfig->GetGeneration() <= config.GetGeneration()) {
                    error = "peer storage config is not bridged one while having newer generation";
                } else if (config.HasClusterState() && config.GetGeneration() <= StorageConfig->GetGeneration()) {
                    error = "local storage config is not bridged one while having newer generation";
                }
                ExchangeConfigs(incoming, outgoing);
                return error;
            }

            // obtain cluster state from other party and check if they are the same as the one we have; this may trigger
            // generation sync (but still would return error)
            if (auto error = ValidateClusterState(config)) {
                return TStringBuilder() << "peer storage config invalid: " << *error;
            } else if (auto error = ValidateClusterState(*StorageConfig)) {
                return TStringBuilder() << "local storage config invalid: " << *error;
            } else if (auto error = ValidateClusterStateDetails(config)) {
                return TStringBuilder() << "peer cluster state history invalid: " << *error;
            } else if (auto error = ValidateClusterStateDetails(*StorageConfig)) {
                return TStringBuilder() << "local cluster state history invalid: " << *error;
            }

            std::optional<TString> error;

            const NKikimrBlobStorage::TStorageConfig& newerConfig =
                StorageConfig->GetGeneration() < config.GetGeneration()
                    ? config
                    : *StorageConfig;
            const auto& cs = newerConfig.GetClusterState();

            const auto isJoiningNode = [this](ui32 nodeId, const auto& own, const auto& other) {
                return nodeId <= MaxStaticNodeId && !own.GetGeneration() && !own.GetClusterState().GetGeneration()
                    && std::ranges::none_of(other.GetAllNodes(), [nodeId](const auto& node) {
                        return node.GetNodeId() == nodeId;
                    });
            };
            const bool joining = isJoiningNode(SelfId().NodeId(), *StorageConfig, config)
                || isJoiningNode(peerNodeId, config, *StorageConfig);

            auto selfPolicyPileId = SelfBridgePileId;
            auto peerPolicyPileId = peerBridgePileId;
            if (joining) {
                const auto registry = GetPileRegistry(newerConfig, &AppData()->BridgeConfig);
                Y_ABORT_UNLESS(registry);
                selfPolicyPileId = NBridge::ResolvePileId(*registry, SelfBridgePileName);
                if (incoming.HasBridgePileName()) {
                    peerPolicyPileId = NBridge::ResolvePileId(*registry, incoming.GetBridgePileName());
                }
            }

            const auto *peerPile = BridgeInfo->GetPile(peerBridgePileId);
            if (peerPile == BridgeInfo->SelfNodePile) {
                if (!selfPolicyPileId || !NBridge::PileStateTraits(NBridge::GetPileState(cs, selfPolicyPileId)).AllowsConnectionWithinPile) {
                    error = "nodes of the pile not added to the cluster can't connect to each other";
                }
            } else { // extra checks when connecting between different piles
                if (auto error = CheckHistoryCompatibility(*StorageConfig, config)) {
                    // histories are incompatible, connection won't ever be possible
                    return error;
                }

                if (StorageConfig->GetGeneration() == config.GetGeneration() && StorageConfig->GetFingerprint() != config.GetFingerprint()) {
                    return "config fingerprint mismatch";
                }

                if (!peerPolicyPileId || !NBridge::PileStateTraits(NBridge::GetPileState(cs, peerPolicyPileId)).AllowsConnection) {
                    error = "peer is not allowed to connect";
                } else if (!selfPolicyPileId || !NBridge::PileStateTraits(NBridge::GetPileState(cs, selfPolicyPileId)).AllowsConnection) {
                    error = "local node is not allowed to accept peer";
                }

                if (!error && !joining) {
                    const auto& myClusterState = StorageConfig->GetClusterState();
                    const auto& peerClusterState = config.GetClusterState();
                    if (myClusterState.GetGeneration() < peerClusterState.GetGeneration()) {
                        error = "local cluster state is obsolete";
                    } else if (peerClusterState.GetGeneration() < myClusterState.GetGeneration()) {
                        error = "peer cluster state is obsolete";
                    }
                }
            }

            ExchangeConfigs(incoming, outgoing);

            return error;
        }

        void ExchangeConfigs(const NKikimrBlobStorage::TConnectivityPayload& incoming,
                NKikimrBlobStorage::TConnectivityPayload *outgoing) {
            const NKikimrBlobStorage::TStorageConfig& config = incoming.GetStorageConfig();
            if (StorageConfig) {
                if (config.GetGeneration() < StorageConfig->GetGeneration()) {
                    outgoing->MutableStorageConfig()->CopyFrom(*StorageConfig);
                    if (CommittedStorageConfig) {
                        outgoing->MutableCommittedStorageConfig()->CopyFrom(*CommittedStorageConfig);
                    }
                } else if (StorageConfig->GetGeneration() < config.GetGeneration()) {
                    UpdateLocalConfig(incoming);
                }
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////

        std::optional<TString> ValidateClusterState(const NKikimrBlobStorage::TStorageConfig& config) {
            if (!config.HasClusterState()) {
                return "ClusterState section is missing in StorageConfig";
            }
            const auto registry = GetPileRegistry(config, &AppData()->BridgeConfig);
            Y_ABORT_UNLESS(registry);
            const size_t numPiles = registry->PilesSize();
            const auto& cs = config.GetClusterState();
            const auto primaryPileId = TBridgePileId::FromProto(&cs, &NKikimrBridge::TClusterState::GetPrimaryPile);
            const auto promotedPileId = TBridgePileId::FromProto(&cs, &NKikimrBridge::TClusterState::GetPromotedPile);
            if (cs.PerPileStateSize() != numPiles) {
                return "incorrect number of piles in ClusterState";
            } else if (numPiles <= primaryPileId.GetPileIndex()) {
                return "PrimaryPile value is out of range";
            } else if (cs.GetPerPileState(primaryPileId.GetPileIndex()) != TClusterState::SYNCHRONIZED) {
                return "PrimaryPile is not in SYNCHRONIZED state";
            } else if (numPiles <= promotedPileId.GetPileIndex()) {
                return "PromotedPile value is out of range";
            } else if (cs.GetPerPileState(promotedPileId.GetPileIndex()) != TClusterState::SYNCHRONIZED) {
                return "PromotedPile is not in SYNCHRONIZED state";
            }
            return std::nullopt;
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////

        std::optional<TString> ValidateClusterStateDetails(const NKikimrBlobStorage::TStorageConfig& config) {
            if (!config.HasClusterStateDetails()) {
                return "ClusterStateDetails section is missing in StorageConfig";
            }
            return std::nullopt;
        }

        std::optional<TString> CheckHistoryCompatibility(const NKikimrBlobStorage::TStorageConfig& myConfig,
                const NKikimrBlobStorage::TStorageConfig& peerConfig) {
            const auto& myState = myConfig.GetClusterState();
            const auto& peerState = peerConfig.GetClusterState();
            if (myState.GetGeneration() < peerState.GetGeneration()) {
                if (myState.GetGeneration() == 0 && myConfig.GetGeneration() == 0) {
                    return std::nullopt; // fresh cluster, may be just formatted
                }
                // we have to validate our state through peer's history that it reaches peer's state
                const auto& history = peerConfig.GetClusterStateDetails().GetUnsyncedHistory();
                const auto it = std::ranges::lower_bound(history, myState.GetGeneration(), std::less<ui64>(),
                    [](const auto& x) { return x.GetClusterState().GetGeneration(); });
                if (it == history.end() || !NBridge::IsSameClusterState(it->GetClusterState(), myState)) {
                    return "cluster state history has been diverged";
                }
            } else if (peerState.GetGeneration() < myState.GetGeneration()) { // just check in reverse
                return CheckHistoryCompatibility(peerConfig, myConfig);
            } else if (!NBridge::IsSameClusterState(myState, peerState)) {
                return "cluster state history has been diverged";
            }
            return std::nullopt;
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////

        const NKikimrBridge::TClusterStateDetails::TPileSyncState *GetPileSyncState(
                const NKikimrBlobStorage::TStorageConfig& config, TBridgePileId bridgePileId) {
            if (!config.HasClusterStateDetails()) {
                return nullptr;
            }
            for (const auto& item : config.GetClusterStateDetails().GetPileSyncState()) {
                const auto id = TBridgePileId::FromProto(&item, &NKikimrBridge::TClusterStateDetails::TPileSyncState::GetBridgePileId);
                if (id == bridgePileId) {
                    return &item;
                }
            }
            return nullptr;
        }

        void Handle(TEvNodeConfigInvokeOnRootResult::TPtr ev) {
            YDB_LOG_DEBUG("TEvNodeConfigInvokeOnRootResult",
                {"marker", "NWDCC05"},
                {"record", ev->Get()->Record});
        }

        STRICT_STFUNC(StateFunc,
            hFunc(TEvNodeWardenStorageConfig, Handle)
            hFunc(TEvInterconnect::TEvNodesInfo, Handle)
            hFunc(TEvInterconnect::TEvPrepareOutgoingConnection, Handle)
            hFunc(TEvInterconnect::TEvCheckIncomingConnection, Handle)
            hFunc(TEvInterconnect::TEvNotifyOutgoingConnectionEstablished, Handle)
            hFunc(TEvNodeConfigInvokeOnRootResult, Handle)
        )
    };

} // NKikimr::NStorage

namespace NKikimr {

    IActor *CreateDistconfBridgeConnectionCheckerActor(TBridgePileId selfBridgePileId,
            std::optional<TString> selfBridgePileName) {
        return new NStorage::TDistributedConfigKeeper::TDistconfBridgeConnectionCheckerActor(selfBridgePileId,
            std::move(selfBridgePileName));
    }

} // NKikimr

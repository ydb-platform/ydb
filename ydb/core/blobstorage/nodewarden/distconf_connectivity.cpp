#include "distconf.h"

namespace NKikimr::NStorage {

    class TDistributedConfigKeeper::TDistconfBridgeConnectionCheckerActor
        : public TActorBootstrapped<TDistconfBridgeConnectionCheckerActor>
    {
        const TBridgePileId SelfBridgePileId;

        std::shared_ptr<const NKikimrBlobStorage::TStorageConfig> StorageConfig;
        TBridgeInfo::TPtr BridgeInfo;
        bool SelfManagementEnabled = false;
        bool IsSelfStatic = false;
        ui32 MaxStaticNodeId = Max<ui32>();

        std::deque<std::unique_ptr<IEventHandle>> PendingEvents;

        static constexpr TStringBuf DistconfKey = "distconf";

        using TClusterState = NKikimrBridge::TClusterState;

    public:
        TDistconfBridgeConnectionCheckerActor(TBridgePileId selfBridgePileId)
            : SelfBridgePileId(selfBridgePileId)
        {}

        void Bootstrap() {
            // determine maximum static node id to distinguish static and dynamic nodes when handling connections
            if (const auto& dynconfig = AppData()->DynamicNameserviceConfig) {
                MaxStaticNodeId = dynconfig->MaxStaticNodeId;
            }

            // check for ourself -- is this static or dynamic node?
            IsSelfStatic = SelfId().NodeId() <= MaxStaticNodeId;

            // for static nodes we query configuration from nodewarden; dynamic nodes do not use this, because it would
            // case deadlock (as the NW reports configuration only when gets one, but it gets one only through
            // interconnect)
            if (IsSelfStatic) {
                Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvNodeWardenQueryStorageConfig(true));
            }

            Become(&TThis::StateFunc);
        }

        void Handle(TEvNodeWardenStorageConfig::TPtr ev) {
            // store just received parameters
            auto& msg = *ev->Get();
            StorageConfig = std::move(msg.Config);
            BridgeInfo = std::move(msg.BridgeInfo);
            SelfManagementEnabled = msg.SelfManagementEnabled;

            // ensure they all are filled in correctly
            Y_ABORT_UNLESS(StorageConfig);
            Y_ABORT_UNLESS(AppData()->BridgeModeEnabled);
            Y_ABORT_UNLESS(BridgeInfo);

            // process any pending events
            for (auto& ev : std::exchange(PendingEvents, {})) {
                TAutoPtr<IEventHandle> temp(ev.release());
                Receive(temp);
            }

            // disconnect peers if needed
            TActorSystem* const as = TActivationContext::ActorSystem();
            for (const auto& pile : BridgeInfo->Piles) {
                if (NBridge::PileStateTraits(pile.State).AllowsConnection) {
                    continue;
                }
                if (pile.BridgePileId == SelfBridgePileId) {
                    continue; // do not disconnect from the same pile
                }
                for (const ui32 nodeId : pile.StaticNodeIds) {
                    STLOG(PRI_DEBUG, BS_NODE, NWDCC00, "disconnecting", (PeerNodeId, nodeId),
                        (BridgePileId, pile.BridgePileId));
                    as->Send(new IEventHandle(TEvInterconnect::EvDisconnect, 0, as->InterconnectProxy(nodeId),
                        {}, nullptr, 0));
                }
                // request nameservice for node list to disconnect from dynamic nodes too
                Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes);
            }
        }

        void Handle(TEvInterconnect::TEvNodesInfo::TPtr ev) {
            Y_ABORT_UNLESS(BridgeInfo);
            Y_ABORT_UNLESS(ev->Get()->PileMap);

            const auto& map = ev->Get()->PileMap;
            TActorSystem* const as = TActivationContext::ActorSystem();

            for (const auto& pile : BridgeInfo->Piles) {
                if (NBridge::PileStateTraits(pile.State).AllowsConnection) {
                    continue;
                }
                const size_t index = pile.BridgePileId.GetRawId();
                Y_ABORT_UNLESS(index < map->size());
                for (const ui32 nodeId : map->at(index)) {
                    if (nodeId <= MaxStaticNodeId) {
                        continue; // static nodes were already processed
                    }
                    STLOG(PRI_DEBUG, BS_NODE, NWDCC03, "disconnecting dynamic", (PeerNodeId, nodeId),
                        (BridgePileId, pile.BridgePileId));
                    as->Send(new IEventHandle(TEvInterconnect::EvDisconnect, 0, as->InterconnectProxy(nodeId),
                        {}, nullptr, 0));
                }
            }
        }

        THashMap<TString, TString> CreateParams(const NKikimrBlobStorage::TConnectivityPayload& outgoing) {
            THashMap<TString, TString> res;
            if (outgoing.ByteSizeLong()) {
                const bool success = outgoing.SerializeToString(&res[DistconfKey]);
                Y_ABORT_UNLESS(success);
            }
            return res;
        }

        void Handle(TEvInterconnect::TEvPrepareOutgoingConnection::TPtr ev) {
            if (IsSelfStatic && !StorageConfig) {
                // we are still waiting for storage config at static node, so postpone this message
                PendingEvents.emplace_back(ev.Release());
                return;
            }

            std::optional<TString> error;
            NKikimrBlobStorage::TConnectivityPayload outgoing;

            // definitely fill out self pile id
            SelfBridgePileId.CopyToProto(&outgoing, &decltype(outgoing)::SetBridgePileId);

            const ui32 peerNodeId = ev->Get()->PeerNodeId;

            if (IsSelfStatic) {
                // we validate peer before starting connection only if we are at a static node -- dynamic nodes may not
                // know peer states when running as they may haven't received any configuration yet
                const auto *pile = BridgeInfo->GetPileForNode(peerNodeId);
                if (!pile) {
                    // peer node is a dynamic node
                } else if (pile == BridgeInfo->SelfNodePile && IsSelfStatic && peerNodeId <= MaxStaticNodeId) {
                    // allow connecting static nodes within the same pile, even disconnected
                } else if (!NBridge::PileStateTraits(pile->State).AllowsConnection) {
                    error = "can't establish connection to node belonging to disconnected pile";
                }
                if (!error) {
                    auto *config = outgoing.MutableStorageConfig();
                    config->SetGeneration(StorageConfig->GetGeneration());
                    config->SetFingerprint(StorageConfig->GetFingerprint());
                    if (StorageConfig->HasClusterState()) {
                        config->MutableClusterState()->CopyFrom(StorageConfig->GetClusterState());
                    }
                    if (StorageConfig->HasClusterStateDetails()) {
                        config->MutableClusterStateDetails()->CopyFrom(StorageConfig->GetClusterStateDetails());
                    }
                }
            }

            STLOG(PRI_DEBUG, BS_NODE, NWDCC01, "handle TEvPrepareOutgoingConnection", (PeerNodeId, peerNodeId),
                (Error, error), (Outgoing, outgoing));

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

            const ui32 peerNodeId = ev->Get()->PeerNodeId;

            // extract the payload, if we have one
            NKikimrBlobStorage::TConnectivityPayload incoming;
            auto& params = ev->Get()->Params;
            if (const auto it = params.find(DistconfKey); it != params.end() && !incoming.ParseFromString(it->second)) {
                error = "failed to parse incoming connectivity check payload";
            }

            // obtain peer's pile id (we must have one)
            std::optional<TBridgePileId> peerBridgePileId;
            if (BridgeInfo) { // this may be null if this is dynamic node
                if (const auto *pile = BridgeInfo->GetPileForNode(peerNodeId)) {
                    peerBridgePileId = pile->BridgePileId;
                }
            }
            if (error) {
                // we already have an error, do nothing more
            } else if (!incoming.HasBridgePileId()) {
                error = "missing mandatory peer bridge pile id";
            } else if (const auto value = TBridgePileId::FromProto(&incoming, &decltype(incoming)::GetBridgePileId);
                    peerBridgePileId && *peerBridgePileId != value) {
                // not the one we expect from this node
                error = "incorrect peer bridge pile id provided";
            } else if (AppData()->BridgeConfig.PilesSize() <= value.GetRawId()) {
                // out of bounds
                error = "peer bridge pile id out of range";
            } else {
                peerBridgePileId.emplace(value);
            }

            // process the peer's configuration
            if (error) {
                // we already have an error
            } else if (!IsSelfStatic || MaxStaticNodeId < peerNodeId) {
                error = ValidateConnectionWithDynamicNodes(
                    *peerBridgePileId,
                    peerNodeId <= MaxStaticNodeId,
                    incoming.HasStorageConfig()
                        ? incoming.MutableStorageConfig()
                        : nullptr
                );
            } else if (!incoming.HasStorageConfig()) {
                error = "missing mandatory peer storage configuration section in handshake";
            } else if (auto res = CheckPeerConfig(*peerBridgePileId, incoming.GetStorageConfig())) {
                error = std::move(res);
            }

            STLOG(PRI_DEBUG, BS_NODE, NWDCC02, "handle TEvCheckIncomingConnection", (PeerNodeId, peerNodeId),
                (Error, error), (Outgoing, outgoing));

            Send(ev->Sender, new TEvInterconnect::TEvCheckIncomingConnectionResult(std::move(error),
                CreateParams(outgoing)), 0, ev->Cookie);
        }

        void Handle(TEvInterconnect::TEvNotifyOutgoingConnectionEstablished::TPtr ev) {
            auto& params = ev->Get()->Params;
            NKikimrBlobStorage::TConnectivityPayload incoming;
            if (const auto it = params.find(DistconfKey); it != params.end() && incoming.ParseFromString(it->second)) {
                // ?
            }
        }

        std::optional<TString> ValidateConnectionWithDynamicNodes(TBridgePileId peerBridgePileId, bool isPeerStatic,
                const NKikimrBlobStorage::TStorageConfig *peerConfig) {
            // this function is invoked when one of peers is dynamic -- in this case we accept connection only when they
            // are both in synchronized state
            if (BridgeInfo) { // this may be missing if this node is dynamic and no configuration yet received
                if (!NBridge::PileStateTraits(BridgeInfo->SelfNodePile->State).AllowsConnection) {
                    return "can't establish connection to node belonging to disconnected pile: local disconnected";
                } else if (!NBridge::PileStateTraits(BridgeInfo->GetPile(peerBridgePileId)->State).AllowsConnection) {
                    return "can't establish connection to node belonging to disconnected pile: remote disconnected";
                }
            }
            if (peerConfig) { // validate peer configuraiton, if it is provided
                if (auto error = ValidateClusterState(*peerConfig)) { // check the config for required fields
                    return error;
                }
                const auto& cs = peerConfig->GetClusterState();
                if (!NBridge::PileStateTraits(cs.GetPerPileState(SelfBridgePileId.GetRawId())).AllowsConnection) {
                    return "can't establish connection to node belonging to disconnected pile (as seen by peer): local disconnected";
                } else if (!NBridge::PileStateTraits(cs.GetPerPileState(peerBridgePileId.GetRawId())).AllowsConnection) {
                    return "can't establish connection to node belonging to disconnected pile (as seen by peer): remote disconnected";
                }
            } else if (isPeerStatic) {
                return "missing mandatory peer storage configuration section in handshake from static peer";
            }
            return std::nullopt;
        }

        std::optional<TString> CheckPeerConfig(TBridgePileId peerBridgePileId,
                const NKikimrBlobStorage::TStorageConfig& config) {
            Y_ABORT_UNLESS(StorageConfig);
            Y_ABORT_UNLESS(BridgeInfo);

            // obtain cluster state from other party and check if they are the same as the one we have; this may trigger
            // generation sync (but still would return error)
            if (auto error = ValidateClusterState(config)) {
                return TStringBuilder() << "peer storage config invalid: " << *error;
            } else if (auto error = ValidateClusterState(*StorageConfig)) {
                return TStringBuilder() << "local storage config invalid: " << *error;
            } else if (auto error = CheckStateCompatibility(StorageConfig->GetClusterState(), config.GetClusterState())) {
                return error;
            }

            // local/peer side is returning from DISCONNECTED state, validate cluster histories to ensure there were no
            // definite split brain
            if (auto error = ValidateClusterStateDetails(config)) {
                return TStringBuilder() << "peer cluster state history invalid: " << *error;
            } else if (auto error = ValidateClusterStateDetails(*StorageConfig)) {
                return TStringBuilder() << "local cluster state history invalid: " << *error;
            } else if (auto error = CheckHistoryCompatibility(StorageConfig->GetClusterStateDetails(), config.GetClusterStateDetails())) {
                // histories are incompatible, connection won't ever be possible
                return error;
            }

            const auto *peerPile = BridgeInfo->GetPile(peerBridgePileId);
            if (peerPile == BridgeInfo->SelfNodePile) {
                // no extra checks when connecting nodes from the same pile
                return std::nullopt;
            }

            if (StorageConfig->GetGeneration() == config.GetGeneration() && StorageConfig->GetFingerprint() != config.GetFingerprint()) {
                return "config fingerprint mismatch";
            }

            const NKikimrBlobStorage::TStorageConfig& newerConfig =
                StorageConfig->GetGeneration() < config.GetGeneration()
                    ? config
                    : *StorageConfig;

            const auto& cs = newerConfig.GetClusterState();

            if (!NBridge::PileStateTraits(cs.GetPerPileState(peerBridgePileId.GetRawId())).AllowsConnection) {
                return "peer is not allowed to connect";
            } else if (!NBridge::PileStateTraits(cs.GetPerPileState(SelfBridgePileId.GetRawId())).AllowsConnection) {
                return "local node is not allowed to accept peer";
            }

            return std::nullopt;
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////

        std::optional<TString> ValidateClusterState(const NKikimrBlobStorage::TStorageConfig& config) {
            if (!config.HasClusterState()) {
                return "ClusterState section is missing in StorageConfig";
            }
            const size_t numPiles = AppData()->BridgeConfig.PilesSize();
            if (const auto& cs = config.GetClusterState(); cs.PerPileStateSize() != numPiles) {
                return "incorrect number of piles in ClusterState";
            } else if (numPiles <= cs.GetPrimaryPile()) {
                return "PrimaryPile value is out of range";
            } else if (cs.GetPerPileState(cs.GetPrimaryPile()) != TClusterState::SYNCHRONIZED) {
                return "PrimaryPile is not in SYNCHRONIZED state";
            } else if (numPiles <= cs.GetPromotedPile()) {
                return "PromotedPile value is out of range";
            } else if (cs.GetPerPileState(cs.GetPromotedPile()) != TClusterState::SYNCHRONIZED) {
                return "PromotedPile is not in SYNCHRONIZED state";
            }
            return std::nullopt;
        }

        std::optional<TString> CheckStateCompatibility(const NKikimrBridge::TClusterState& my,
                const NKikimrBridge::TClusterState& peer) {
            Y_ABORT_UNLESS(my.PerPileStateSize() == peer.PerPileStateSize());
            return std::nullopt;
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////

        std::optional<TString> ValidateClusterStateDetails(const NKikimrBlobStorage::TStorageConfig& config) {
            if (!config.HasClusterStateDetails()) {
                return "ClusterStateDetails section is missing in StorageConfig";
            }
            if (const auto& history = config.GetClusterStateDetails().GetUnsyncedHistory(); history.empty()) {
                return "empty UnsyncedEntries in ClusterStateDetails";
            } else {
                const auto& lastItem = history[history.size() - 1];
                if (!NBridge::IsSameClusterState(lastItem.GetClusterState(), config.GetClusterState())) {
                    return "last item in UnsyncedHistory does not match ClusterState";
                }
            }
            return std::nullopt;
        }

        std::optional<TString> CheckHistoryCompatibility(const NKikimrBridge::TClusterStateDetails& my,
                const NKikimrBridge::TClusterStateDetails& peer) {
            const auto& myHistory = my.GetUnsyncedHistory();
            const auto& peerHistory = peer.GetUnsyncedHistory();
            int myIndex = 0;
            int peerIndex = 0;

            while (myIndex < myHistory.size() && peerIndex < peerHistory.size()) {
                const auto& myItem = myHistory[myIndex];
                const auto& peerItem = peerHistory[peerIndex];
                if (myItem.GetClusterState().GetGeneration() < peerItem.GetClusterState().GetGeneration()) {
                    if (myItem.UnsyncedPilesSize()) {
                        return "local history has extra item";
                    }
                    ++myIndex;
                } else if (peerItem.GetClusterState().GetGeneration() < myItem.GetClusterState().GetGeneration()) {
                    if (myItem.UnsyncedPilesSize()) {
                        return "peer history has extra item";
                    }
                    ++peerIndex;
                } else if (!NBridge::IsSameClusterState(myItem.GetClusterState(), peerItem.GetClusterState()) ||
                        myItem.GetOperationGuid() != peerItem.GetOperationGuid()) {
                    return "history of local and peer piles have been has been diverged";
                } else {
                    ++myIndex, ++peerIndex;
                }
            }

            return std::nullopt;
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
            STLOG(PRI_DEBUG, BS_NODE, NWDCC05, "TEvNodeConfigInvokeOnRootResult", (Record, ev->Get()->Record));
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

    IActor *CreateDistconfBridgeConnectionCheckerActor(TBridgePileId selfBridgePileId) {
        return new NStorage::TDistributedConfigKeeper::TDistconfBridgeConnectionCheckerActor(selfBridgePileId);
    }

} // NKikimr

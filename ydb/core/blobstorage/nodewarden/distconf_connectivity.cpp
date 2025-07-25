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
                if (pile.State != TClusterState::DISCONNECTED) {
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
                if (pile.State != TClusterState::DISCONNECTED) {
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
                } else if (pile->State == TClusterState::DISCONNECTED) {
                    error = "can't establish connection to node belonging to disconnected pile";
                }
                if (!error) {
                    outgoing.MutableStorageConfig()->CopyFrom(*StorageConfig);
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

            // definitely fill out self pile id
            SelfBridgePileId.CopyToProto(&outgoing, &decltype(outgoing)::SetBridgePileId);

            const ui32 peerNodeId = ev->Get()->PeerNodeId;

            // extract the payload, if we have one
            NKikimrBlobStorage::TConnectivityPayload incoming;
            auto& params = ev->Get()->Params;
            if (const auto it = params.find(DistconfKey); it != params.end() && !incoming.ParseFromString(it->second)) {
                error = "failed to parse incoming connectivity check payload";
            }

            // report this node's configuration into outgoing message (unless this is unconfigured dynamic node)
            if (StorageConfig) {
                outgoing.MutableStorageConfig()->CopyFrom(*StorageConfig);
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
            } else if (auto res = CheckPeerConfig(*peerBridgePileId, std::move(*incoming.MutableStorageConfig()))) {
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
            }
        }

        std::optional<TString> ValidateConnectionWithDynamicNodes(TBridgePileId peerBridgePileId, bool isPeerStatic,
                const NKikimrBlobStorage::TStorageConfig *peerConfig) {
            // this function is invoked when one of peers is dynamic -- in this case we accept connection only when they
            // are both in synchronized state
            if (BridgeInfo) { // this may be missing if this node is dynamic and no configuration yet received
                if (BridgeInfo->SelfNodePile->State == TClusterState::DISCONNECTED) {
                    return "can't establish connection to node belonging to disconnected pile: local disconnected";
                } else if (BridgeInfo->GetPile(peerBridgePileId)->State == TClusterState::DISCONNECTED) {
                    return "can't establish connection to node belonging to disconnected pile: remote disconnected";
                }
            }
            if (peerConfig) { // validate peer configuraiton, if it is provided
                if (auto error = ValidateClusterState(*peerConfig)) { // check the config for required fields
                    return error;
                }
                const auto& cs = peerConfig->GetClusterState();
                if (cs.GetPerPileState(SelfBridgePileId.GetRawId()) == TClusterState::DISCONNECTED) {
                    return "can't establish connection to node belonging to disconnected pile (as seen by peer): local disconnected";
                } else if (cs.GetPerPileState(peerBridgePileId.GetRawId()) == TClusterState::DISCONNECTED) {
                    return "can't establish connection to node belonging to disconnected pile (as seen by peer): remote disconnected";
                }
            } else if (isPeerStatic) {
                return "missing mandatory peer storage configuration section in handshake from static peer";
            }
            return std::nullopt;
        }

        std::optional<TString> CheckPeerConfig(TBridgePileId peerBridgePileId,
                NKikimrBlobStorage::TStorageConfig&& config) {
            Y_ABORT_UNLESS(StorageConfig);
            Y_ABORT_UNLESS(BridgeInfo);

            const auto *peerPile = BridgeInfo->GetPile(peerBridgePileId);
            if (peerPile == BridgeInfo->SelfNodePile) {
                // no extra checks when connecting nodes from the same pile
                return std::nullopt;
            } else if (peerPile->State == TClusterState::DISCONNECTED) {
                // peer pile is considered disconnected according to our config; this is incoming connection and we
                // should definitely drop it
                return "can't establish connection to node belonging to disconnected pile: remote disconnected";
            } else if (BridgeInfo->SelfNodePile->State == TClusterState::DISCONNECTED) {
                // we are self-isolating, so we drop connections from any other piles
                return "can't establish connection to node belonging to disconnected pile: local disconnected";
            }

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
            if (auto error = ValidateClusterStateHistory(config)) {
                return TStringBuilder() << "peer cluster state history invalid: " << *error;
            } else if (auto error = ValidateClusterStateHistory(*StorageConfig)) {
                return TStringBuilder() << "local cluster state history invalid: " << *error;
            } else if (auto error = CheckHistoryCompatibility(StorageConfig->GetClusterStateHistory(), config.GetClusterStateHistory())) {
                // histories are incompatible, connection won't ever be possible
                return error;
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

        std::optional<TString> ValidateClusterStateHistory(const NKikimrBlobStorage::TStorageConfig& config) {
            if (!config.HasClusterStateHistory()) {
                return "ClusterStateHistory section is missing in StorageConfig";
            }
            if (const auto& entries = config.GetClusterStateHistory().GetUnsyncedEntries(); entries.empty()) {
                return "empty UnsyncedEntries in ClusterStateHistory";
            } else {
                const auto& item = entries[entries.size() - 1];
                const auto& itemState = item.GetClusterState();
                const auto& clusterState = config.GetClusterState();
                if (!std::ranges::equal(itemState.GetPerPileState(), clusterState.GetPerPileState()) ||
                        itemState.GetPrimaryPile() != clusterState.GetPrimaryPile() ||
                        itemState.GetPromotedPile() != clusterState.GetPromotedPile() ||
                        itemState.GetGeneration() != clusterState.GetGeneration()) {
                    return "last item in UnsyncedEntries does not match ClusterState";
                }
            }
            return std::nullopt;
        }

        std::optional<TString> CheckHistoryCompatibility(const NKikimrBridge::TClusterStateHistory& my,
                const NKikimrBridge::TClusterStateHistory& peer) {
            // check if we have to run config synchronization procedure
            const auto& myEntries = my.GetUnsyncedEntries();
            const auto& peerEntries = peer.GetUnsyncedEntries();
            if (!myEntries.empty() && !peerEntries.empty()) {
                const auto& myItem = myEntries[myEntries.size() - 1];
                const auto& peerItem = peerEntries[peerEntries.size() - 1];
                if (!NProtoBuf::util::MessageDifferencer::Equals(myItem, peerItem)) {
                    // last entries are out of sync, we have to bring them together
                }
            }

            return std::nullopt;
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////

        const NKikimrBridge::TClusterStateHistory::TPileSyncState *GetPileSyncState(
                const NKikimrBlobStorage::TStorageConfig& config, TBridgePileId bridgePileId) {
            if (!config.HasClusterStateHistory()) {
                return nullptr;
            }
            for (const auto& item : config.GetClusterStateHistory().GetPileSyncState()) {
                const auto id = TBridgePileId::FromProto(&item, &NKikimrBridge::TClusterStateHistory::TPileSyncState::GetBridgePileId);
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

#include "distconf.h"

namespace NKikimr::NStorage {

    namespace {

        class TDistconfConnectionCheckerActor : public TActorBootstrapped<TDistconfConnectionCheckerActor> {
            std::shared_ptr<const NKikimrBlobStorage::TStorageConfig> StorageConfig;
            TBridgeInfo::TPtr BridgeInfo;
            bool SelfManagementEnabled = false;

            TString SerializedStorageConfig;

            std::deque<std::unique_ptr<IEventHandle>> PendingEvents;

            bool ListNodesPending = false;

            static constexpr TStringBuf StorageConfigKey = "distconf_config";
            static constexpr TStringBuf BridgePileIdKey = "bridge_pile_id";

        public:
            TDistconfConnectionCheckerActor()
            {}

            void Bootstrap() {
                Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvNodeWardenQueryStorageConfig(true));
                Become(&TThis::StateFunc);
            }

            void Handle(TEvNodeWardenStorageConfig::TPtr ev) {
                auto& msg = *ev->Get();
                StorageConfig = std::move(msg.Config);
                BridgeInfo = std::move(msg.BridgeInfo);
                SelfManagementEnabled = msg.SelfManagementEnabled;

                // preserialize configuration
                const bool success = StorageConfig->SerializeToString(&SerializedStorageConfig);
                Y_ABORT_UNLESS(success);

                // process any pending events
                for (auto& ev : std::exchange(PendingEvents, {})) {
                    TAutoPtr<IEventHandle> temp(ev.release());
                    Receive(temp);
                }

                // disconnect peers if needed
                if (BridgeInfo) {
                    TActorSystem* const as = TActivationContext::ActorSystem();
                    for (const auto& pile : BridgeInfo->Piles) {
                        if (pile.State != NKikimrBridge::TClusterState::DISCONNECTED) {
                            continue;
                        }
                        if (pile.BridgePileId == BridgeInfo->SelfNodePile->BridgePileId) {
                            continue; // do not disconnect from the same pile
                        }
                        for (const ui32 nodeId : pile.StaticNodeIds) {
                            STLOG(PRI_DEBUG, BS_NODE, NWDCC00, "disconnecting", (PeerNodeId, nodeId),
                                (BridgePileId, pile.BridgePileId));
                            as->Send(new IEventHandle(TEvInterconnect::EvDisconnect, 0, as->InterconnectProxy(nodeId),
                                {}, nullptr, 0));
                        }
                        if (!ListNodesPending) {
                            // request nameservice for node list to disconnect from dynamic nodes too
                            Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes);
                            ListNodesPending = true;
                        }
                    }
                }
            }

            void Handle(TEvInterconnect::TEvNodesInfo::TPtr ev) {
                Y_ABORT_UNLESS(ListNodesPending);
                ListNodesPending = false;

                if (BridgeInfo && ev->Get()->PileMap) {
                    TActorSystem* const as = TActivationContext::ActorSystem();

                    for (const auto& pile : BridgeInfo->Piles) {
                        if (pile.State != NKikimrBridge::TClusterState::DISCONNECTED) {
                            continue;
                        }
                        const size_t index = pile.BridgePileId.GetRawId();
                        Y_ABORT_UNLESS(index < ev->Get()->PileMap->size());
                        for (const ui32 nodeId : ev->Get()->PileMap->at(index)) {
                            if (nodeId <= AppData()->DynamicNameserviceConfig->MaxStaticNodeId) {
                                continue; // static nodes were already processed
                            }
                            STLOG(PRI_DEBUG, BS_NODE, NWDCC03, "disconnecting dynamic", (PeerNodeId, nodeId),
                                (BridgePileId, pile.BridgePileId));
                            as->Send(new IEventHandle(TEvInterconnect::EvDisconnect, 0, as->InterconnectProxy(nodeId),
                                {}, nullptr, 0));
                        }
                    }
                }
            }

            void Handle(TEvInterconnect::TEvPrepareOutgoingConnection::TPtr ev) {
                if (!StorageConfig) {
                    PendingEvents.emplace_back(ev.Release());
                    return;
                }

                std::optional<TString> error;

                if (BridgeInfo) {
                    if (const auto *pile = BridgeInfo->GetPileForNode(ev->Get()->PeerNodeId); pile &&
                            pile != BridgeInfo->SelfNodePile && pile->State == NKikimrBridge::TClusterState::DISCONNECTED) {
                        error = "can't establish connection to node belonging to disconnected pile";
                    } else {
                        // dynamic node?
                    }
                }

                STLOG(PRI_DEBUG, BS_NODE, NWDCC01, "handle TEvPrepareOutgoingConnection", (PeerNodeId, ev->Get()->PeerNodeId),
                    (Error, error));

                if (error) {
                    Send(ev->Sender, new TEvInterconnect::TEvPrepareOutgoingConnectionResult(std::move(*error)), 0, ev->Cookie);
                } else {
                    Send(ev->Sender, new TEvInterconnect::TEvPrepareOutgoingConnectionResult(CreateParams()), 0, ev->Cookie);
                }
            }

            void Handle(TEvInterconnect::TEvCheckIncomingConnection::TPtr ev) {
                if (!StorageConfig) {
                    PendingEvents.emplace_back(ev.Release());
                    return;
                }

                std::optional<TString> error;
                THashMap<TString, TString> paramsToSend;

                if (BridgeInfo) {
                    auto& params = ev->Get()->Params;

                    std::optional<TBridgePileId> peerBridgePileId;
                    if (const auto *pile = BridgeInfo->GetPileForNode(ev->Get()->PeerNodeId)) {
                        peerBridgePileId = pile->BridgePileId;
                    }

                    if (const auto it = params.find(BridgePileIdKey); it != params.end()) {
                        TBridgePileId::Type value;
                        if (!TryFromString(it->second, value)) {
                            error = "failed to parse peer bridge pile id";
                        } else if (!peerBridgePileId) {
                            peerBridgePileId.emplace(TBridgePileId::FromValue(value));
                        } else if (peerBridgePileId->GetRawId() != value) {
                            error = "incorrect peer bridge pile id provided";
                        }
                    } else {
                        error = "missing mandatory peer bridge pile id";
                    }

                    if (const auto it = params.find(StorageConfigKey); it != params.end() && !error) {
                        Y_ABORT_UNLESS(peerBridgePileId);

                        NKikimrBlobStorage::TStorageConfig config;
                        if (!config.ParseFromString(it->second)) {
                            error = "failed to parse storage configuration protobuf";
                        } else if (auto res = CheckPeerConfig(*peerBridgePileId, config)) {
                            error = std::move(res);
                        } else {
                            Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvNodeWardenUpdateConfigFromPeer(std::move(config)));
                        }
                    } else if (BridgeInfo) {
                        error = "missing mandatory peer storage configuration section in handshake";
                    }
                }

                STLOG(PRI_DEBUG, BS_NODE, NWDCC02, "handle TEvCheckIncomingConnection", (PeerNodeId, ev->Get()->PeerNodeId),
                    (Error, error));

                if (!error) {
                    paramsToSend = CreateParams();
                }
                Send(ev->Sender, new TEvInterconnect::TEvCheckIncomingConnectionResult(std::move(error),
                    std::move(paramsToSend)), 0, ev->Cookie);
            }

            THashMap<TString, TString> CreateParams() {
                return BridgeInfo ? THashMap<TString, TString>{{
                    {TString{StorageConfigKey}, SerializedStorageConfig},
                    {TString{BridgePileIdKey}, TStringBuilder() << BridgeInfo->SelfNodePile->BridgePileId},
                }} : THashMap<TString, TString>{};
            }

            void Handle(TEvInterconnect::TEvNotifyOutgoingConnectionEstablished::TPtr ev) {
                auto& params = ev->Get()->Params;
                if (const auto it = params.find(StorageConfigKey); it != params.end()) {
                    if (NKikimrBlobStorage::TStorageConfig config; config.ParseFromString(it->second)) {
                        Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvNodeWardenUpdateConfigFromPeer(
                            std::move(config)));
                    }
                }
            }

            std::optional<TString> CheckPeerConfig(TBridgePileId peerBridgePileId,
                    const NKikimrBlobStorage::TStorageConfig& config) {
                const auto *peerPile = BridgeInfo->GetPile(peerBridgePileId);
                if (peerPile->State == NKikimrBridge::TClusterState::DISCONNECTED && peerPile != BridgeInfo->SelfNodePile) {
                    // peer pile is considered disconnected according to our config; this is incoming connection and we
                    // should definitely drop it
                    return "can't establish connection to node belonging to disconnected pile -- remote disconnected";
                }

                if (!config.HasClusterState()) {
                    return "peer storage config doesn't contain ClusterState";
                }
                const auto& clusterState = config.GetClusterState();
                if (clusterState.PerPileStateSize() != BridgeInfo->Piles.size()) {
                    return "peer storage config contains incorrect number of piles";
                } else if (clusterState.GetPerPileState(BridgeInfo->SelfNodePile->BridgePileId.GetRawId()) == NKikimrBridge::TClusterState::DISCONNECTED) {
                    return "can't establish connection to node belonging to disconnected pile -- local disconnected";
                }

                if (!config.HasClusterStateHistory()) {
                    return "peer storage config doesn't contain ClusterStateHistory";
                }

                if (auto error = CheckHistoryCompatibility(StorageConfig->GetClusterStateHistory(),
                        config.GetClusterStateHistory())) {
                    // histories are incompatible, connection won't ever be possible
                    return error;
                }

                // check if we have to propose new config in order to restore connectivity between piles

                return std::nullopt;
            }

            std::optional<TString> CheckHistoryCompatibility(const NKikimrBridge::TClusterStateHistory& my,
                    const NKikimrBridge::TClusterStateHistory& peer) {
                (void)my, (void)peer;
                return std::nullopt;
            }

            STRICT_STFUNC(StateFunc,
                hFunc(TEvNodeWardenStorageConfig, Handle)
                hFunc(TEvInterconnect::TEvNodesInfo, Handle)
                hFunc(TEvInterconnect::TEvPrepareOutgoingConnection, Handle)
                hFunc(TEvInterconnect::TEvCheckIncomingConnection, Handle)
                hFunc(TEvInterconnect::TEvNotifyOutgoingConnectionEstablished, Handle)
            )
        };

    } // anonymous

} // NKikimr::NStorage

namespace NKikimr {

    IActor *CreateDistconfConnectionCheckerActor() {
        return new NStorage::TDistconfConnectionCheckerActor();
    }

} // NKikimr

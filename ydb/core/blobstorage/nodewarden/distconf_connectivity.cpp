#include "distconf.h"

#include <ydb/core/base/nameservice.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>

#include <ydb/library/actors/interconnect/interconnect.h>

#include <array>

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
        std::optional<ui32> PendingFeatureRequirement;

        std::deque<std::unique_ptr<IEventHandle>> PendingEvents;

        // bridge feature levels reported by peers in the handshakes of their sessions (absent for the ones reporting
        // nothing); the sessions are not checked again, so the peers are disconnected once the config requires more
        THashMap<ui32, ui32> PeerFeatureLevels;

        // the payload is sent with the key of the bridge feature level the config requires; older versions don't know the
        // keys of the levels they don't support, so they treat such peer as not bridge-capable one and neither connect,
        // nor take its configuration
        static constexpr std::array<TStringBuf, 5> DistconfKeys{
            "distconf",
            "distconf-pile-list-changed", // BridgeFeatureLevelPileList
            "distconf-pile-ops", // BridgeFeatureLevelPileOps
            "distconf-non-uniform-storage", // BridgeFeatureLevelNonUniformStorage
            "distconf-pile-overrides", // BridgeFeatureLevelPileOverrides
        };
        static_assert(DistconfKeys.size() == BridgeFeatureLevel + 1);

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

            DisconnectIncapablePeers();

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
                const ui32 required = GetRequiredFeatureLevel();
                const TStringBuf key = DistconfKeys[Min<ui32>(required, BridgeFeatureLevel)];
                const bool success = outgoing.SerializeToString(&res[key]);
                Y_ABORT_UNLESS(success);
            }
            return res;
        }

        // returns false if the payload is present, but can't be parsed
        static bool ParsePayload(const THashMap<TString, TString>& params,
                NKikimrBlobStorage::TConnectivityPayload *incoming) {
            for (auto it = DistconfKeys.rbegin(); it != DistconfKeys.rend(); ++it) {
                if (const auto jt = params.find(*it); jt != params.end()) {
                    return incoming->ParseFromString(jt->second);
                }
            }
            return true;
        }

        static ui32 GetPeerFeatureLevel(const NKikimrBlobStorage::TConnectivityPayload& incoming) {
            return GetReportedBridgeFeatureLevel(incoming.GetBridgeFeatureLevel(), incoming.GetPileListChangeCapable());
        }

        // peer of older version can't work with the configuration requiring the features it doesn't support
        bool IsPeerIncompatible(const NKikimrBlobStorage::TConnectivityPayload& incoming) const {
            return GetPeerFeatureLevel(incoming) < GetRequiredFeatureLevel();
        }

        ui32 GetRequiredFeatureLevel() const {
            return Max(StorageConfig ? GetRequiredBridgeFeatureLevel(*StorageConfig) : 0,
                PendingFeatureRequirement.value_or(0));
        }

        void DisconnectIncapablePeers() {
            const ui32 required = GetRequiredFeatureLevel();
            for (auto it = PeerFeatureLevels.begin(); it != PeerFeatureLevels.end(); ) {
                const auto [peerNodeId, level] = *it++;
                if (level < required) {
                    DisconnectIncapablePeer(peerNodeId, level);
                    PeerFeatureLevels.erase(peerNodeId);
                }
            }
        }

        void Handle(TEvPrivate::TEvUpdateBridgeFeatureRequirement::TPtr ev) {
            PendingFeatureRequirement = ev->Get()->RequiredLevel;
            DisconnectIncapablePeers();
        }

        // remember bridge feature level of the peer of just established session
        void RegisterPeer(ui32 peerNodeId, const NKikimrBlobStorage::TConnectivityPayload& incoming) {
            if (const ui32 level = GetPeerFeatureLevel(incoming); level >= BridgeFeatureLevel) {
                PeerFeatureLevels.erase(peerNodeId); // no feature can be required beyond this version's level
            } else if (IsPeerIncompatible(incoming)) {
                DisconnectIncapablePeer(peerNodeId, level);
            } else {
                PeerFeatureLevels[peerNodeId] = level;
            }
        }

        void DisconnectIncapablePeer(ui32 peerNodeId, ui32 level) {
            YDB_LOG_WARN("Disconnecting peer running version not supporting bridge features of the config",
                {"marker", "NWDCC08"},
                {"peerNodeId", peerNodeId},
                {"bridgeFeatureLevel", level});
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
            // the peer takes our config only if we report the features it requires
            outgoing.SetPileListChangeCapable(true);
            outgoing.SetBridgeFeatureLevel(BridgeFeatureLevel);

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
                error = "peer runs version not supporting bridge features of the config, it has to be upgraded";
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
            outgoing->SetBridgeFeatureLevel(BridgeFeatureLevel);
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
                const std::optional<TString> peerPileName = incoming.HasBridgePileName()
                    ? std::make_optional(incoming.GetBridgePileName())
                    : std::nullopt;
                if (IsOlderPartyOfReaddedPile(config, peerPileName)) {
                    // its history doesn't matter, it just gets the current config
                } else if (auto error = CheckHistoryCompatibility(*StorageConfig, config)) {
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

        // The party having older cluster state is the node of the pile that has been removed from the cluster and added
        // again since then (e.g. on the same hosts, not formatted): the history of its cluster state is not in the one of
        // the cluster, as the history is trimmed once the pile is removed, but it doesn't matter, as the data of the pile
        // was dropped along with it. Both parties decide so by the config of the newer one; the older one must have older
        // config too, so that the config of the pile running on its own (diverged) never replaces the cluster's one.
        bool IsOlderPartyOfReaddedPile(const NKikimrBlobStorage::TStorageConfig& peerConfig,
                const std::optional<TString>& peerPileName) {
            const auto& myConfig = *StorageConfig;
            const ui64 myStateGeneration = myConfig.GetClusterState().GetGeneration();
            const ui64 peerStateGeneration = peerConfig.GetClusterState().GetGeneration();
            if (myStateGeneration == peerStateGeneration) {
                return false;
            }
            const bool selfOlder = myStateGeneration < peerStateGeneration;
            const auto& older = selfOlder ? myConfig : peerConfig;
            const auto& newer = selfOlder ? peerConfig : myConfig;
            const std::optional<TString>& olderPileName = selfOlder ? SelfBridgePileName : peerPileName;
            if (!olderPileName || newer.GetGeneration() <= older.GetGeneration()) {
                return false;
            }
            for (const auto& pile : newer.GetClusterStateDetails().GetPileRegistry().GetPiles()) {
                if (pile.HasName() && pile.GetName() == *olderPileName) {
                    return older.GetClusterState().GetGeneration() < pile.GetAddedGeneration();
                }
            }
            return false;
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
            hFunc(TEvPrivate::TEvUpdateBridgeFeatureRequirement, Handle)
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

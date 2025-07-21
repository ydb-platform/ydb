#pragma once

#include "bind_queue.h"
#include "node_warden.h"
#include "node_warden_events.h"

#include <ydb/core/protos/bridge.pb.h>
#include <util/generic/hash_multi_map.h>
#include <ydb/core/mind/bscontroller/group_mapper.h>

namespace NKikimr::NStorage {

    struct TNodeIdentifier : std::tuple<TString, ui32, ui32> {
        std::optional<TBridgePileId> BridgePileId;

        TNodeIdentifier() = default;

        TNodeIdentifier(TString host, ui32 port, ui32 nodeId, std::optional<TBridgePileId> bridgePileId)
            : std::tuple<TString, ui32, ui32>(std::move(host), port, nodeId)
            , BridgePileId(bridgePileId)
        {}

        TNodeIdentifier(const NKikimrBlobStorage::TNodeIdentifier& proto)
            : std::tuple<TString, ui32, ui32>(proto.GetHost(), proto.GetPort(), proto.GetNodeId())
        {
            if (proto.HasBridgePileId()) {
                BridgePileId.emplace(TBridgePileId::FromProto(&proto, &NKikimrBlobStorage::TNodeIdentifier::GetBridgePileId));
            }
        }

        ui32 NodeId() const {
            return std::get<2>(*this);
        }

        void Serialize(NKikimrBlobStorage::TNodeIdentifier *proto) const {
            proto->SetHost(std::get<0>(*this));
            proto->SetPort(std::get<1>(*this));
            proto->SetNodeId(std::get<2>(*this));
            if (BridgePileId) {
                BridgePileId->CopyToProto(proto, &NKikimrBlobStorage::TNodeIdentifier::SetBridgePileId);
            }
        }
    };

    struct TStorageConfigMeta : NKikimrBlobStorage::TStorageConfigMeta {
        TStorageConfigMeta(const NKikimrBlobStorage::TStorageConfigMeta& m)
            : NKikimrBlobStorage::TStorageConfigMeta(m)
        {}

        TStorageConfigMeta(const NKikimrBlobStorage::TStorageConfig& config) {
            SetGeneration(config.GetGeneration());
            SetFingerprint(config.GetFingerprint());
        }

        friend bool operator ==(const TStorageConfigMeta& x, const TStorageConfigMeta& y) {
            return x.GetGeneration() == y.GetGeneration()
                && x.GetFingerprint() == y.GetFingerprint();
        }

        friend bool operator !=(const TStorageConfigMeta& x, const TStorageConfigMeta& y) {
            return !(x == y);
        }
    };

} // NKikimr::NStorage

template<>
struct THash<NKikimr::NStorage::TNodeIdentifier> : THash<std::tuple<TString, ui32, ui32>> {};

template<>
struct THash<NKikimr::NStorage::TStorageConfigMeta> {
    size_t operator ()(const NKikimr::NStorage::TStorageConfigMeta& m) const {
        return MultiHash(m.GetGeneration(), m.GetFingerprint());
    }
};

namespace NKikimr::NStorage {

    class TDistributedConfigKeeper : public TActorBootstrapped<TDistributedConfigKeeper> {
        using TEvGather = NKikimrBlobStorage::TEvNodeConfigGather;
        using TEvScatter = NKikimrBlobStorage::TEvNodeConfigScatter;

        struct TEvPrivate {
            enum {
               EvProcessPendingEvent = EventSpaceBegin(TEvents::ES_PRIVATE),
               EvErrorTimeout,
               EvStorageConfigLoaded,
               EvStorageConfigStored,
               EvReconnect,
               EvUpdateRootState,
               EvOpQueueEnd,
            };

            struct TEvStorageConfigLoaded : TEventLocal<TEvStorageConfigLoaded, EvStorageConfigLoaded> {
                std::vector<std::tuple<TString, NKikimrBlobStorage::TPDiskMetadataRecord, std::optional<ui64>>> MetadataPerPath;
                std::vector<std::tuple<TString, std::optional<ui64>>> NoMetadata;
                std::vector<TString> Errors;
            };

            struct TEvStorageConfigStored : TEventLocal<TEvStorageConfigStored, EvStorageConfigStored> {
                std::vector<std::tuple<TString, bool, std::optional<ui64>>> StatusPerPath;
            };

            struct TEvUpdateRootState : TEventLocal<TEvUpdateRootState, EvUpdateRootState> {
                const bool HasScepter;
                const std::optional<ui32> RootNodeId;
                const bool HasQuorumInPile;

                TEvUpdateRootState(bool hasScepter, std::optional<ui32> rootNodeId, bool hasQuorumInPile)
                    : HasScepter(hasScepter)
                    , RootNodeId(rootNodeId)
                    , HasQuorumInPile(hasQuorumInPile)
                {}
            };
        };

        struct TBinding {
            ui32 NodeId; // we have direct binding to this node
            ui32 RootNodeId = 0; // this is the terminal node id for the whole binding chain
            ui64 Cookie; // binding cookie within the session
            TActorId SessionId; // interconnect session actor

            TBinding(ui32 nodeId, ui64 cookie)
                : NodeId(nodeId)
                , Cookie(cookie)
            {}

            TBinding(const TBinding& origin)
                : NodeId(origin.NodeId)
                , RootNodeId(origin.RootNodeId)
                , Cookie(origin.Cookie)
                , SessionId(origin.SessionId)
            {}

            bool Expected(IEventHandle& ev) const {
                return NodeId == ev.Sender.NodeId()
                    && Cookie == ev.Cookie
                    && SessionId == ev.InterconnectSession;
            }

            TString ToString() const {
                return TStringBuilder() << '{' << NodeId << '.' << RootNodeId << '/' << Cookie
                    << '@' << SessionId << '}';
            }

            friend bool operator ==(const TBinding& x, const TBinding& y) {
                return x.NodeId == y.NodeId && x.Cookie == y.Cookie && x.SessionId == y.SessionId;
            }

            friend bool operator !=(const TBinding& x, const TBinding& y) {
                return !(x == y);
            }
        };

        struct TBoundNode {
            ui64 Cookie; // cookie presented in original TEvNodeConfig push message
            TActorId SessionId; // interconnect session for this peer
            THashSet<ui64> ScatterTasks; // unanswered scatter queries
            THashSet<TNodeIdentifier> BoundNodeIds; // a set of provided bound nodes by this peer (not including itself)

            TBoundNode(ui64 cookie, TActorId sessionId)
                : Cookie(cookie)
                , SessionId(sessionId)
            {}

            bool Expected(IEventHandle& ev) const {
                return Cookie == ev.Cookie
                    && SessionId == ev.InterconnectSession;
            }
        };

        struct TScepter { // the Chosen One has the scepter; it's guaranteed that only one node holds it
            ui64 Id = RandomNumber<ui64>(); // unique id
        };

        struct TScatterTask {
            const std::optional<TBinding> Origin;
            const ui64 ScepterCounter;
            const TActorId ActorId;

            THashSet<ui32> PendingNodes;
            ui32 AsyncOperationsPending = 0;
            TEvScatter Request;
            TEvGather Response;
            std::vector<TEvGather> CollectedResponses; // from bound nodes

            TScatterTask(const std::optional<TBinding>& origin, TEvScatter&& request,
                    ui64 scepterCounter, TActorId actorId)
                : Origin(origin)
                , ScepterCounter(scepterCounter)
                , ActorId(actorId)
            {
                Request.Swap(&request);
                if (Request.HasCookie()) {
                    Response.SetCookie(Request.GetCookie());
                }
            }
        };

        const bool IsSelfStatic = false;
        TIntrusivePtr<TNodeWardenConfig> Cfg;
        bool SelfManagementEnabled = false;
        TBridgeInfo::TPtr BridgeInfo;

        // currently active storage config
        std::shared_ptr<const NKikimrBlobStorage::TStorageConfig> StorageConfig;
        TString MainConfigYaml; // the part we have to push (unless this is storage-only) to console
        std::optional<ui64> MainConfigYamlVersion;
        TString MainConfigFetchYaml; // the part we would get is we fetch from console
        ui64 MainConfigFetchYamlHash = 0;
        std::optional<TString> StorageConfigYaml; // set if dedicated storage yaml is enabled; otherwise nullopt

        // base config from config file
        std::shared_ptr<const NKikimrBlobStorage::TStorageConfig> BaseConfig;

        // initial config based on config file and stored committed configs
        std::shared_ptr<const NKikimrBlobStorage::TStorageConfig> InitialConfig;
        std::vector<TString> DrivesToRead;

        // proposed storage configuration of the cluster
        std::optional<NKikimrBlobStorage::TStorageConfig> ProposedStorageConfig; // proposed one
        ui64 ProposedStorageConfigCookie; // if set, then this configuration is being written right now
        ui32 ProposedStorageConfigCookieUsage = 0;

        // most relevant proposed config
        using TPersistCallback = std::function<void(TEvPrivate::TEvStorageConfigStored&)>;
        struct TPersistQueueItem {
            THPTimer Timer;
            std::vector<TString> Drives;
            NKikimrBlobStorage::TPDiskMetadataRecord Record; // what we are going to write
            TPersistCallback Callback; // what will be called upon completion
        };
        std::deque<TPersistQueueItem> PersistQ;

        // initialization state
        bool NodeListObtained = false;
        bool StorageConfigLoaded = false;

        // outgoing binding
        std::optional<TBinding> Binding;
        ui64 BindingCookie = RandomNumber<ui64>();
        TBindQueue BindQueue;
        bool Scheduled = false;

        // incoming bindings
        struct TIndirectBoundNode {
            std::list<TStorageConfigMeta> Configs; // last one is the latest one
            THashMap<ui32, std::list<TStorageConfigMeta>::iterator> Refs;
        };
        THashMap<ui32, TBoundNode> DirectBoundNodes; // a set of nodes directly bound to this one
        THashMap<TNodeIdentifier, TIndirectBoundNode> AllBoundNodes; // a set of all bound nodes in tree, including this one

        // pending event queue
        std::deque<TAutoPtr<IEventHandle>> PendingEvents;
        std::vector<ui32> NodeIds;
        THashSet<ui32> NodeIdsSet;
        TNodeIdentifier SelfNode;

        // scatter tasks
        ui64 NextScatterCookie = RandomNumber<ui64>();
        using TScatterTasks = THashMap<ui64, TScatterTask>;
        TScatterTasks ScatterTasks;

        std::optional<TActorId> StateStorageSelfHealActor;

        // root node operation
        enum class ERootState {
            INITIAL,
            ERROR_TIMEOUT,
            IN_PROGRESS,
            RELAX,
            SCEPTERLESS_OPERATION,
        };
        static constexpr TDuration ErrorTimeout = TDuration::Seconds(3);
        ERootState RootState = ERootState::INITIAL;

        struct TProposition {
            NKikimrBlobStorage::TStorageConfig StorageConfig; // storage config being proposed
            THashSet<TBridgePileId> SpecificBridgePileIds; // a set of piles making up required quorum
            TActorId ActorId; // actor id waiting for this operation to complete
            bool CheckSyncersAfterCommit; // shall we check Bridge syncers after commit has been made
        };
        std::optional<TProposition> CurrentProposition;

        std::shared_ptr<TScepter> Scepter;
        ui64 ScepterCounter = 0; // increased every time Scepter gets changed
        TString ErrorReason;
        std::optional<TString> CurrentSelfAssemblyUUID;

        // bridge-related logic
        std::set<std::tuple<ui32, TGroupId, TBridgePileId>> WorkingSyncersByNode;
        std::set<std::tuple<TGroupId, TBridgePileId, ui32>> WorkingSyncers;
        bool SyncerArrangeInFlight = false;
        bool SyncerArrangePending = false;

        // subscribed IC sessions
        struct TSessionSubscription {
            TActorId SessionId;
            ui64 SubscriptionCookie = 0; // when nonzero, we didn't have TEvNodeConnected yet

            TSessionSubscription(TActorId sessionId) : SessionId(sessionId) {}

            TString ToString() const {
                return TStringBuilder() << "{SessionId# " << SessionId << " SubscriptionCookie# " << SubscriptionCookie << "}";
            }
        };
        THashMap<ui32, TSessionSubscription> SubscribedSessions;
        ui64 NextSubscribeCookie = 1;
        THashMap<ui64, ui32> SubscriptionCookieMap;
        THashSet<ui32> UnsubscribeQueue;

        // child actors
        THashSet<TActorId> ChildActors;

        // pipe to Console
        TActorId ConsolePipeId;
        bool ConsoleConnected = false;
        bool ConfigCommittedToConsole = false;
        ui64 ValidateRequestCookie = 0;
        ui64 ProposeRequestCookie = 0;
        ui64 CommitRequestCookie = 0;
        bool ProposeRequestInFlight = false;
        std::optional<std::tuple<ui64, ui32>> ProposedConfigHashVersion;
        std::vector<std::tuple<TActorId, TString, ui64>> ConsoleConfigValidationQ;

        // cache subsystem
        struct TCacheItem {
            ui32 Generation; // item generation
            std::optional<TString> Value; // item binary value
        };
        THashMap<TString, TCacheItem> Cache;
        std::set<std::tuple<TString, TActorId>> CacheSubscriptions;

        friend void ::Out<ERootState>(IOutputStream&, ERootState);

    public:
        class TDistconfBridgeConnectionCheckerActor;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::NODEWARDEN_DISTRIBUTED_CONFIG;
        }

        TDistributedConfigKeeper(TIntrusivePtr<TNodeWardenConfig> cfg,
            std::shared_ptr<const NKikimrBlobStorage::TStorageConfig> baseConfig, bool isSelfStatic);

        void Bootstrap();
        void PassAway() override;
        void Halt(); // cease any distconf activity, unbind and reject any bindings
        bool ApplyStorageConfig(const NKikimrBlobStorage::TStorageConfig& config);
        void HandleConfigConfirm(STATEFN_SIG);
        void ReportStorageConfigToNodeWarden(ui64 cookie);
        void Handle(TEvNodeWardenUpdateConfigFromPeer::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // PDisk configuration retrieval and storing

        void ReadConfig(ui64 cookie = 0);
        void WriteConfig(std::vector<TString> drives, NKikimrBlobStorage::TPDiskMetadataRecord record);
        void PersistConfig(TPersistCallback callback);
        void Handle(TEvPrivate::TEvStorageConfigStored::TPtr ev);
        void Handle(TEvPrivate::TEvStorageConfigLoaded::TPtr ev);

        static TString CalculateFingerprint(const NKikimrBlobStorage::TStorageConfig& config);
        static void UpdateFingerprint(NKikimrBlobStorage::TStorageConfig *config);
        static bool CheckFingerprint(const NKikimrBlobStorage::TStorageConfig& config);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Node handling

        void Handle(TEvInterconnect::TEvNodesInfo::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Binding to peer nodes

        void IssueNextBindRequest();
        void BindToSession(TActorId sessionId);
        void Handle(TEvInterconnect::TEvNodeConnected::TPtr ev);
        void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr ev);
        void HandleDisconnect(ui32 nodeId, TActorId sessionId);
        void UnsubscribeInterconnect(ui32 nodeId);
        TActorId SubscribeToPeerNode(ui32 nodeId, TActorId sessionId);
        void AbortBinding(const char *reason, bool sendUnbindMessage = true);
        void HandleWakeup();
        void Handle(TEvNodeConfigReversePush::TPtr ev);
        void FanOutReversePush(const NKikimrBlobStorage::TStorageConfig *config, bool recurseConfigUpdate = false);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Binding requests from peer nodes

        void UpdateBound(ui32 refererNodeId, TNodeIdentifier nodeId, const TStorageConfigMeta& meta, TEvNodeConfigPush *msg);
        void DeleteBound(ui32 refererNodeId, const TNodeIdentifier& nodeId, TEvNodeConfigPush *msg);
        void Handle(TEvNodeConfigPush::TPtr ev);
        void Handle(TEvNodeConfigUnbind::TPtr ev);
        void UnbindNode(ui32 nodeId, const char *reason);
        ui32 GetRootNodeId() const;
        bool PartOfNodeQuorum() const;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Root node operation

        void CheckRootNodeStatus();
        void BecomeRoot();
        void UnbecomeRoot();
        void HandleErrorTimeout();
        void ProcessGather(TEvGather *res);
        bool HasConnectedNodeQuorum(const NKikimrBlobStorage::TStorageConfig& config,
            const THashSet<TBridgePileId>& specificBridgePileIds = {}) const;

        void UpdateRootStateToConnectionChecker();

        struct TProcessCollectConfigsResult {
            std::optional<TString> ErrorReason;
            bool IsDistconfDisabledQuorum = false;
            std::optional<NKikimrBlobStorage::TStorageConfig> PropositionBase;
            std::optional<NKikimrBlobStorage::TStorageConfig> ConfigToPropose;
        };
        TProcessCollectConfigsResult ProcessCollectConfigs(TEvGather::TCollectConfigs *res,
            std::optional<TStringBuf> selfAssemblyUUID);

        void ProcessProposeStorageConfig(TEvGather::TProposeStorageConfig *res);

        struct TExConfigError : yexception {};

        std::optional<TString> GenerateFirstConfig(NKikimrBlobStorage::TStorageConfig *config, const TString& selfAssemblyUUID);

        void AllocateStaticGroup(NKikimrBlobStorage::TStorageConfig *config, TGroupId groupId, ui32 groupGeneration,
            TBlobStorageGroupType gtype, const NKikimrBlobStorage::TGroupGeometry& geometry,
            const NProtoBuf::RepeatedPtrField<NKikimrBlobStorage::TPDiskFilter>& pdiskFilters,
            std::optional<NKikimrBlobStorage::EPDiskType> pdiskType,
            THashMap<TVDiskIdShort, NBsController::TPDiskId> replacedDisks,
            const NBsController::TGroupMapper::TForbiddenPDisks& forbid,
            i64 requiredSpace, NKikimrBlobStorage::TBaseConfig *baseConfig,
            bool convertToDonor, bool ignoreVSlotQuotaCheck, bool isSelfHealReasonDecommit,
            std::optional<TBridgePileId> bridgePileId);

        bool UpdateConfig(NKikimrBlobStorage::TStorageConfig *config);

        void PrepareScatterTask(ui64 cookie, TScatterTask& task);

        void PerformScatterTask(TScatterTask& task);
        void Perform(TEvGather::TCollectConfigs *response, const TEvScatter::TCollectConfigs& request, TScatterTask& task);
        void Perform(TEvGather::TProposeStorageConfig *response, const TEvScatter::TProposeStorageConfig& request, TScatterTask& task);

        void SwitchToError(const TString& reason);

        std::optional<TString> StartProposition(NKikimrBlobStorage::TStorageConfig *configToPropose,
            const NKikimrBlobStorage::TStorageConfig *propositionBase, THashSet<TBridgePileId>&& specificBridgePileIds,
            TActorId actorId, bool checkSyncersAfterCommit, bool forceGeneration);

        void CheckForConfigUpdate();


        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Generate state storage config

        std::unordered_map<ui32, ui32> SelfHealNodesState;
        
        bool GenerateStateStorageConfig(NKikimrConfig::TDomainsConfig::TStateStorage *ss,
            const NKikimrBlobStorage::TStorageConfig& baseConfig);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Bridge ops

        // preparation (may be async)
        void PrepareScatterTask(ui64 cookie, TScatterTask& task, const TEvScatter::TManageSyncers& request);
        void Handle(TEvNodeWardenManageSyncersResult::TPtr ev);

        // execute per-node action
        void Perform(TEvGather::TManageSyncers *response, const TEvScatter::TManageSyncers& request, TScatterTask& task);

        // handle gather result (when completed all along the cluster)
        void ProcessManageSyncers(TEvGather::TManageSyncers *res);

        void RearrangeSyncing();
        void OnSyncerUnboundNode(ui32 nodeId);
        void IssueQuerySyncers();

        bool UpdateBridgeConfig(NKikimrBlobStorage::TStorageConfig *config);

        static void GenerateBridgeInitialState(const TNodeWardenConfig& cfg, NKikimrBlobStorage::TStorageConfig *config);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Scatter/gather logic

        void IssueScatterTask(std::optional<TActorId> actorId, TEvScatter&& request);
        void CheckCompleteScatterTask(TScatterTasks::iterator it);
        void FinishAsyncOperation(ui64 cookie);
        void IssueScatterTaskForNode(ui32 nodeId, TBoundNode& info, ui64 cookie, TScatterTask& task);
        void CompleteScatterTask(TScatterTask& task);
        void AbortScatterTask(ui64 cookie, ui32 nodeId);
        void AbortAllScatterTasks(const std::optional<TBinding>& binding);
        void Handle(TEvNodeConfigScatter::TPtr ev);
        void Handle(TEvNodeConfigGather::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // NodeWarden RPC

        struct TInvokeOperation {
            TActorId ActorId; // originating actor id or empty if this was triggered by system
            bool Scepterless = false; // was this operation marked as scepterless?
        };
        std::deque<TInvokeOperation> InvokeQ; // operations queue; first is always the being executed one
        bool DeadActorWaitingForProposition = false;

        class TInvokeRequestHandlerActor;
        struct TLifetimeToken {};
        std::shared_ptr<TLifetimeToken> LifetimeToken = std::make_shared<TLifetimeToken>();

        ui64 InvokeActorQueueGeneration = 1;

        void Handle(TEvNodeConfigInvokeOnRoot::TPtr ev);

        void OpQueueOnBecomeRoot();
        void OpQueueOnUnbecomeRoot();
        void OpQueueOnError(const TString& errorReason);

        void OpQueueBegin(TActorId actorId, bool scepterless);
        void OpQueueProcessFront();
        void HandleOpQueueEnd(STFUNC_SIG);

        TInvokeRequestHandlerActor *GetInvokeRequestHandlerActor(TActorId actorId);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Dynamic node interaction

        TBindQueue StaticBindQueue;
        THashMap<TActorId, TActorId> DynamicConfigSubscribers; // <session id> -> <actor id>
        ui32 ConnectedToStaticNode = 0;
        TActorId StaticNodeSessionId;
        bool ReconnectScheduled = false;
        THashSet<ui32> ConnectedDynamicNodes;

        // these are used on the dynamic nodes
        void ApplyStaticNodeIds(const std::vector<ui32>& nodeIds);
        void ConnectToStaticNode();
        void HandleReconnect();
        void OnStaticNodeConnected(ui32 nodeId, TActorId sessionId);
        void OnStaticNodeDisconnected(ui32 nodeId, TActorId sessionId);
        void Handle(TEvNodeWardenDynamicConfigPush::TPtr ev);

        // these are used on the static nodes
        void ApplyConfigUpdateToDynamicNodes(bool drop);
        void OnDynamicNodeDisconnected(ui32 nodeId, TActorId sessionId);
        void HandleDynamicConfigSubscribe(STATEFN_SIG);
        void PushConfigToDynamicNode(TActorId actorId, TActorId sessionId, bool addCache);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Event delivery

        void SendEvent(ui32 nodeId, ui64 cookie, TActorId sessionId, std::unique_ptr<IEventBase> ev);
        void SendEvent(const TBinding& binding, std::unique_ptr<IEventBase> ev);
        void SendEvent(const IEventHandle& handle, std::unique_ptr<IEventBase> ev);
        void SendEvent(ui32 nodeId, const TBoundNode& info, std::unique_ptr<IEventBase> ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Monitoring

        void Handle(NMon::TEvHttpInfo::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Cache update

        void ApplyCacheUpdates(NKikimrBlobStorage::TCacheUpdate *cacheUpdate, ui32 senderNodeId);

        void AddCacheUpdate(NKikimrBlobStorage::TCacheUpdate *cacheUpdate, THashMap<TString, TCacheItem>::const_iterator it,
            bool addValue);

        void Handle(TEvNodeWardenUpdateCache::TPtr ev);
        void Handle(TEvNodeWardenQueryCache::TPtr ev);
        void Handle(TEvNodeWardenUnsubscribeFromCache::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Console interaction

        void ConnectToConsole(bool enablingDistconf = false);
        void DisconnectFromConsole();
        void SendConfigProposeRequest();
        void Handle(TEvBlobStorage::TEvControllerValidateConfigResponse::TPtr ev);
        void Handle(TEvBlobStorage::TEvControllerProposeConfigResponse::TPtr ev);
        void Handle(TEvBlobStorage::TEvControllerConsoleCommitResponse::TPtr ev);
        void Handle(TEvTabletPipe::TEvClientConnected::TPtr ev);
        void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr ev);
        void OnConsolePipeError();
        bool EnqueueConsoleConfigValidation(TActorId queryId, bool enablingDistconf, TString yaml);

        static std::optional<TString> UpdateConfigComposite(NKikimrBlobStorage::TStorageConfig& config, const TString& yaml,
            const std::optional<TString>& fetched);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Consistency checking

#ifdef NDEBUG
        void ConsistencyCheck() {}
#else
        void ConsistencyCheck();
#endif

        STFUNC(StateWaitForInit);
        STFUNC(StateFunc);
    };

    template<typename T>
    void EnumerateConfigDrives(const NKikimrBlobStorage::TStorageConfig& config, ui32 nodeId, T&& callback,
            THashMap<ui32, const NKikimrBlobStorage::TNodeIdentifier*> *nodeMap = nullptr, bool fillInPDiskConfig = false) {
        if (!config.HasBlobStorageConfig()) {
            return;
        }
        const auto& bsConfig = config.GetBlobStorageConfig();

        if (!bsConfig.HasDefineBox()) {
            return;
        }
        const auto& defineBox = bsConfig.GetDefineBox();

        THashMap<ui64, const NKikimrBlobStorage::TDefineHostConfig*> defineHostConfigMap;
        for (const auto& defineHostConfig : bsConfig.GetDefineHostConfig()) {
            defineHostConfigMap.emplace(defineHostConfig.GetHostConfigId(), &defineHostConfig);
        }

        THashMap<ui32, const NKikimrBlobStorage::TNodeIdentifier*> tempNodeMap;
        if (!nodeMap) {
            nodeMap = &tempNodeMap;
        }
        for (const auto& node : config.GetAllNodes()) {
            if (nodeId && nodeId != node.GetNodeId()) {
                continue;
            }
            nodeMap->emplace(node.GetNodeId(), &node);
        }

        for (const auto& host : defineBox.GetHost()) {
            if (nodeId && nodeId != host.GetEnforcedNodeId()) {
                continue;
            }
            if (const auto it = nodeMap->find(host.GetEnforcedNodeId()); it != nodeMap->end()) {
                const auto& node = *it->second;
                if (const auto it = defineHostConfigMap.find(host.GetHostConfigId()); it != defineHostConfigMap.end()) {
                    const auto& hostConfig = *it->second;
                    auto processDrive = [&](const auto& drive) {
                        if (fillInPDiskConfig && !drive.HasPDiskConfig() && hostConfig.HasDefaultHostPDiskConfig()) {
                            NKikimrBlobStorage::THostConfigDrive temp;
                            temp.CopyFrom(drive);
                            temp.MutablePDiskConfig()->CopyFrom(hostConfig.GetDefaultHostPDiskConfig());
                            callback(node, temp);
                        } else {
                            callback(node, drive);
                        }
                    };
                    for (const auto& drive : hostConfig.GetDrive()) {
                        processDrive(drive);
                    }
                    auto processTypedDrive = [&](const auto& field, NKikimrBlobStorage::EPDiskType type) {
                        for (const auto& path : field) {
                            NKikimrBlobStorage::THostConfigDrive drive;
                            drive.SetType(type);
                            drive.SetPath(path);
                            processDrive(drive);
                        }
                    };
                    processTypedDrive(hostConfig.GetRot(), NKikimrBlobStorage::EPDiskType::ROT);
                    processTypedDrive(hostConfig.GetSsd(), NKikimrBlobStorage::EPDiskType::SSD);
                    processTypedDrive(hostConfig.GetNvme(), NKikimrBlobStorage::EPDiskType::NVME);
                }
            }
        }
    }

    std::optional<TString> ValidateConfigUpdate(const NKikimrBlobStorage::TStorageConfig& current,
            const NKikimrBlobStorage::TStorageConfig& proposed, bool forceGeneration);

    std::optional<TString> ValidateConfig(const NKikimrBlobStorage::TStorageConfig& config);

    std::optional<TString> DecomposeConfig(const TString& configComposite, TString *mainConfigYaml,
        ui64 *mainConfigVersion, TString *mainConfigFetchYaml);

    std::optional<TString> UpdateClusterState(NKikimrBlobStorage::TStorageConfig *config);

    TBridgeInfo::TPtr GenerateBridgeInfo(const NKikimrBlobStorage::TStorageConfig& config,
        const TNodeWardenConfig *cfg, ui32 selfNodeId);

} // NKikimr::NStorage

template<>
struct THash<std::optional<ui64>> {
    size_t operator ()(std::optional<ui64> x) const { return MultiHash(x.has_value(), x.value_or(0)); }
};

#pragma once

#include "defs.h"
#include "bind_queue.h"
#include "node_warden.h"
#include "node_warden_events.h"

namespace NKikimr::NStorage {

    struct TNodeIdentifier : std::tuple<TString, ui32, ui32> {
        TNodeIdentifier() = default;

        TNodeIdentifier(TString host, ui32 port, ui32 nodeId)
            : std::tuple<TString, ui32, ui32>(std::move(host), port, nodeId)
        {}

        TNodeIdentifier(const NKikimrBlobStorage::TNodeIdentifier& proto)
            : std::tuple<TString, ui32, ui32>(proto.GetHost(), proto.GetPort(), proto.GetNodeId())
        {}

        ui32 NodeId() const {
            return std::get<2>(*this);
        }

        void Serialize(NKikimrBlobStorage::TNodeIdentifier *proto) const {
            proto->SetHost(std::get<0>(*this));
            proto->SetPort(std::get<1>(*this));
            proto->SetNodeId(std::get<2>(*this));
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
            };

            struct TEvStorageConfigLoaded : TEventLocal<TEvStorageConfigLoaded, EvStorageConfigLoaded> {
                std::vector<std::tuple<TString, NKikimrBlobStorage::TPDiskMetadataRecord>> MetadataPerPath;
            };

            struct TEvStorageConfigStored : TEventLocal<TEvStorageConfigStored, EvStorageConfigStored> {
                std::vector<std::tuple<TString, bool>> StatusPerPath;
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

        struct TScatterTask {
            std::optional<TBinding> Origin;
            THashSet<ui32> PendingNodes;
            bool AsyncOperationsPending = false;
            TEvScatter Request;
            TEvGather Response;
            std::vector<TEvGather> CollectedResponses; // from bound nodes

            TScatterTask(const std::optional<TBinding>& origin, TEvScatter&& request)
                : Origin(origin)
            {
                Request.Swap(&request);
                if (Request.HasCookie()) {
                    Response.SetCookie(Request.GetCookie());
                }
            }
        };

        TIntrusivePtr<TNodeWardenConfig> Cfg;

        // currently active storage config
        std::optional<NKikimrBlobStorage::TStorageConfig> StorageConfig;

        // base config from config file
        NKikimrBlobStorage::TStorageConfig BaseConfig;

        // initial config based on config file and stored committed configs
        NKikimrBlobStorage::TStorageConfig InitialConfig;
        std::vector<TString> DrivesToRead;

        // proposed storage configuration being persisted right now
        std::optional<NKikimrBlobStorage::TStorageConfig> ProposedStorageConfig;
        std::optional<ui64> ProposedStorageConfigCookie;

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
        TNodeIdentifier SelfNode;

        // scatter tasks
        ui64 NextScatterCookie = RandomNumber<ui64>();
        THashMap<ui64, TScatterTask> ScatterTasks;

        // root node operation
        enum class ERootState {
            INITIAL,
            COLLECT_CONFIG,
            PROPOSE_NEW_STORAGE_CONFIG,
            ERROR_TIMEOUT,
        };
        static constexpr TDuration ErrorTimeout = TDuration::Seconds(3);
        ERootState RootState = ERootState::INITIAL;
        NKikimrBlobStorage::TStorageConfig CurrentProposedStorageConfig;

        // subscribed IC sessions
        THashMap<ui32, TActorId> SubscribedSessions;

        friend void ::Out<ERootState>(IOutputStream&, ERootState);

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::NODEWARDEN_DISTRIBUTED_CONFIG;
        }

        TDistributedConfigKeeper(TIntrusivePtr<TNodeWardenConfig> cfg, const NKikimrBlobStorage::TStorageConfig& baseConfig);

        void Bootstrap();
        void Halt(); // cease any distconf activity, unbind and reject any bindings
        bool ApplyStorageConfig(const NKikimrBlobStorage::TStorageConfig& config);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // PDisk configuration retrieval and storing

        static void ReadConfig(TActorSystem *actorSystem, TActorId selfId, const std::vector<TString>& drives,
            const TIntrusivePtr<TNodeWardenConfig>& cfg, ui64 cookie);

        static void WriteConfig(TActorSystem *actorSystem, TActorId selfId, const std::vector<TString>& drives,
            const TIntrusivePtr<TNodeWardenConfig>& cfg, const NKikimrBlobStorage::TPDiskMetadataRecord& record);

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
        void Handle(TEvents::TEvUndelivered::TPtr ev);
        void UnsubscribeInterconnect(ui32 nodeId);
        void AbortBinding(const char *reason, bool sendUnbindMessage = true);
        void HandleWakeup();
        void Handle(TEvNodeConfigReversePush::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Binding requests from peer nodes

        void UpdateBound(ui32 refererNodeId, TNodeIdentifier nodeId, const TStorageConfigMeta& meta, TEvNodeConfigPush *msg);
        void DeleteBound(ui32 refererNodeId, const TNodeIdentifier& nodeId, TEvNodeConfigPush *msg);
        void Handle(TEvNodeConfigPush::TPtr ev);
        void Handle(TEvNodeConfigUnbind::TPtr ev);
        void UnbindNode(ui32 nodeId, const char *reason);
        ui32 GetRootNodeId() const;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Root node operation

        void CheckRootNodeStatus();
        void HandleErrorTimeout();
        void ProcessGather(TEvGather *res);
        bool HasQuorum() const;
        void ProcessCollectConfigs(TEvGather::TCollectConfigs *res);
        void ProcessProposeStorageConfig(TEvGather::TProposeStorageConfig *res);

        bool GenerateFirstConfig(NKikimrBlobStorage::TStorageConfig *config);
        void AllocateStaticGroup(NKikimrBlobStorage::TStorageConfig *config);
        bool UpdateConfig(NKikimrBlobStorage::TStorageConfig *config);

        void PrepareScatterTask(ui64 cookie, TScatterTask& task);

        void PerformScatterTask(TScatterTask& task);
        void Perform(TEvGather::TCollectConfigs *response, const TEvScatter::TCollectConfigs& request, TScatterTask& task);
        void Perform(TEvGather::TProposeStorageConfig *response, const TEvScatter::TProposeStorageConfig& request, TScatterTask& task);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Scatter/gather logic

        void IssueScatterTask(bool locallyGenerated, TEvScatter&& request);
        void FinishAsyncOperation(ui64 cookie);
        void IssueScatterTaskForNode(ui32 nodeId, TBoundNode& info, ui64 cookie, TScatterTask& task);
        void CompleteScatterTask(TScatterTask& task);
        void AbortScatterTask(ui64 cookie, ui32 nodeId);
        void AbortAllScatterTasks(const TBinding& binding);
        void Handle(TEvNodeConfigScatter::TPtr ev);
        void Handle(TEvNodeConfigGather::TPtr ev);

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
            THashMap<ui32, const NKikimrBlobStorage::TNodeIdentifier*> *nodeMap = nullptr) {
        if (!config.HasBlobStorageConfig()) {
            return;
        }
        const auto& bsConfig = config.GetBlobStorageConfig();

        if (!bsConfig.HasAutoconfigSettings()) {
            return;
        }
        const auto& autoconfigSettings = bsConfig.GetAutoconfigSettings();

        if (!autoconfigSettings.HasDefineBox()) {
            return;
        }
        const auto& defineBox = autoconfigSettings.GetDefineBox();

        THashMap<ui64, const NKikimrBlobStorage::TDefineHostConfig*> defineHostConfigMap;
        for (const auto& defineHostConfig : autoconfigSettings.GetDefineHostConfig()) {
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
                    for (const auto& drive : hostConfig.GetDrive()) {
                        callback(node, drive);
                    }
                }
            }
        }
    }

    template<typename T>
    bool HasDiskQuorum(const NKikimrBlobStorage::TStorageConfig& config, T&& generateSuccessful) {
        // generate set of all required drives
        THashMap<TString, std::tuple<ui32, ui32>> status; // dc -> {ok, err}
        THashMap<ui32, const NKikimrBlobStorage::TNodeIdentifier*> nodeMap;
        THashSet<std::tuple<TNodeIdentifier, TString>> allDrives;
        auto cb = [&status, &allDrives](const auto& node, const auto& drive) {
            auto& [ok, err] = status[TNodeLocation(node.GetLocation()).GetDataCenterId()];
            ++err;
            allDrives.emplace(node, drive.GetPath());
        };
        EnumerateConfigDrives(config, 0, cb, &nodeMap);

        // process responses
        generateSuccessful([&](const TNodeIdentifier& node, const TString& path) {
            const auto it = nodeMap.find(node.NodeId());
            if (it == nodeMap.end() || TNodeIdentifier(*it->second) != node) { // unexpected node answers
                return;
            }
            if (!allDrives.erase(std::make_tuple(node, path))) { // unexpected drive
                return;
            }
            auto& [ok, err] = status[TNodeLocation(it->second->GetLocation()).GetDataCenterId()];
            Y_VERIFY(err);
            ++ok;
            --err;
        });

        // calculate number of good and bad datacenters
        ui32 ok = 0;
        ui32 err = 0;
        for (const auto& [_, value] : status) {
            const auto [dcOk, dcErr] = value;
            ++(dcOk > dcErr ? ok : err);
        }

        // strict datacenter majority
        return ok > err;
    }

    template<typename T>
    bool HasNodeQuorum(const NKikimrBlobStorage::TStorageConfig& config, T&& generateSuccessful) {
        // generate set of all nodes
        THashMap<TString, std::tuple<ui32, ui32>> status; // dc -> {ok, err}
        THashMap<ui32, const NKikimrBlobStorage::TNodeIdentifier*> nodeMap;
        for (const auto& node : config.GetAllNodes()) {
            auto& [ok, err] = status[TNodeLocation(node.GetLocation()).GetDataCenterId()];
            ++err;
            nodeMap.emplace(node.GetNodeId(), &node);
        }

        // process responses
        generateSuccessful([&](const TNodeIdentifier& node) {
            const auto it = nodeMap.find(node.NodeId());
            if (it == nodeMap.end() || TNodeIdentifier(*it->second) != node) { // unexpected node answers
                return;
            }
            auto& [ok, err] = status[TNodeLocation(it->second->GetLocation()).GetDataCenterId()];
            Y_VERIFY(err);
            ++ok;
            --err;
        });

        // calculate number of good and bad datacenters
        ui32 ok = 0;
        ui32 err = 0;
        for (const auto& [_, value] : status) {
            const auto [dcOk, dcErr] = value;
            ++(dcOk > dcErr ? ok : err);
        }

        // strict datacenter majority
        return ok > err;
    }

} // NKikimr::NStorage

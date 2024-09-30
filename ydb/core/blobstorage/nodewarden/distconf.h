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
               EvReconnect,
            };

            struct TEvStorageConfigLoaded : TEventLocal<TEvStorageConfigLoaded, EvStorageConfigLoaded> {
                std::vector<std::tuple<TString, NKikimrBlobStorage::TPDiskMetadataRecord, std::optional<ui64>>> MetadataPerPath;
                std::vector<std::tuple<TString, std::optional<ui64>>> NoMetadata;
                std::vector<TString> Errors;
            };

            struct TEvStorageConfigStored : TEventLocal<TEvStorageConfigStored, EvStorageConfigStored> {
                std::vector<std::tuple<TString, bool, std::optional<ui64>>> StatusPerPath;
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
            const std::weak_ptr<TScepter> Scepter;
            const TActorId ActorId;

            THashSet<ui32> PendingNodes;
            ui32 AsyncOperationsPending = 0;
            TEvScatter Request;
            TEvGather Response;
            std::vector<TEvGather> CollectedResponses; // from bound nodes

            TScatterTask(const std::optional<TBinding>& origin, TEvScatter&& request,
                    const std::shared_ptr<TScepter>& scepter, TActorId actorId)
                : Origin(origin)
                , Scepter(scepter)
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

        // currently active storage config
        std::optional<NKikimrBlobStorage::TStorageConfig> StorageConfig;

        // base config from config file
        NKikimrBlobStorage::TStorageConfig BaseConfig;

        // initial config based on config file and stored committed configs
        NKikimrBlobStorage::TStorageConfig InitialConfig;
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
        TNodeIdentifier SelfNode;

        // scatter tasks
        ui64 NextScatterCookie = RandomNumber<ui64>();
        using TScatterTasks = THashMap<ui64, TScatterTask>;
        TScatterTasks ScatterTasks;

        // root node operation
        enum class ERootState {
            INITIAL,
            ERROR_TIMEOUT,
            IN_PROGRESS,
            RELAX,
        };
        static constexpr TDuration ErrorTimeout = TDuration::Seconds(3);
        ERootState RootState = ERootState::INITIAL;
        std::optional<NKikimrBlobStorage::TStorageConfig> CurrentProposedStorageConfig;
        std::shared_ptr<TScepter> Scepter;
        TString ErrorReason;

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

        friend void ::Out<ERootState>(IOutputStream&, ERootState);

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::NODEWARDEN_DISTRIBUTED_CONFIG;
        }

        TDistributedConfigKeeper(TIntrusivePtr<TNodeWardenConfig> cfg, const NKikimrBlobStorage::TStorageConfig& baseConfig,
            bool isSelfStatic);

        void Bootstrap();
        void PassAway() override;
        void HandleGone(STATEFN_SIG);
        void Halt(); // cease any distconf activity, unbind and reject any bindings
        bool ApplyStorageConfig(const NKikimrBlobStorage::TStorageConfig& config);
        void HandleConfigConfirm(STATEFN_SIG);

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
        bool HasQuorum() const;
        void ProcessCollectConfigs(TEvGather::TCollectConfigs *res);
        std::optional<TString> ProcessProposeStorageConfig(TEvGather::TProposeStorageConfig *res);

        struct TExConfigError : yexception {};

        bool GenerateFirstConfig(NKikimrBlobStorage::TStorageConfig *config);

        void AllocateStaticGroup(NKikimrBlobStorage::TStorageConfig *config, ui32 groupId, ui32 groupGeneration,
            TBlobStorageGroupType gtype, const NKikimrBlobStorage::TGroupGeometry& geometry,
            const NProtoBuf::RepeatedPtrField<NKikimrBlobStorage::TPDiskFilter>& pdiskFilters,
            std::optional<NKikimrBlobStorage::EPDiskType> pdiskType,
            THashMap<TVDiskIdShort, NBsController::TPDiskId> replacedDisks,
            const NBsController::TGroupMapper::TForbiddenPDisks& forbid,
            i64 requiredSpace, NKikimrBlobStorage::TBaseConfig *baseConfig,
            bool convertToDonor, bool ignoreVSlotQuotaCheck, bool isSelfHealReasonDecommit);

        void GenerateStateStorageConfig(NKikimrConfig::TDomainsConfig::TStateStorage *ss,
            const NKikimrBlobStorage::TStorageConfig& baseConfig);
        bool UpdateConfig(NKikimrBlobStorage::TStorageConfig *config);

        void PrepareScatterTask(ui64 cookie, TScatterTask& task);

        void PerformScatterTask(TScatterTask& task);
        void Perform(TEvGather::TCollectConfigs *response, const TEvScatter::TCollectConfigs& request, TScatterTask& task);
        void Perform(TEvGather::TProposeStorageConfig *response, const TEvScatter::TProposeStorageConfig& request, TScatterTask& task);

        void SwitchToError(const TString& reason);

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

        class TInvokeRequestHandlerActor;
        struct TLifetimeToken {};
        std::shared_ptr<TLifetimeToken> LifetimeToken = std::make_shared<TLifetimeToken>();

        void Handle(TEvNodeConfigInvokeOnRoot::TPtr ev);

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
        void PushConfigToDynamicNode(TActorId actorId, TActorId sessionId);

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
            THashMap<ui32, const NKikimrBlobStorage::TNodeIdentifier*> *nodeMap = nullptr, bool fillInPDiskConfig = false) {
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
        generateSuccessful([&](const TNodeIdentifier& node, const TString& path, std::optional<ui64> /*guid*/) {
            const auto it = nodeMap.find(node.NodeId());
            if (it == nodeMap.end() || TNodeIdentifier(*it->second) != node) { // unexpected node answers
                return;
            }
            if (!allDrives.erase(std::make_tuple(node, path))) { // unexpected drive
                return;
            }
            auto& [ok, err] = status[TNodeLocation(it->second->GetLocation()).GetDataCenterId()];
            Y_ABORT_UNLESS(err);
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
        std::set<TNodeIdentifier> seen;
        generateSuccessful([&](const TNodeIdentifier& node) {
            const auto& [_, inserted] = seen.insert(node);
            Y_ABORT_UNLESS(inserted);

            const auto it = nodeMap.find(node.NodeId());
            if (it == nodeMap.end() || TNodeIdentifier(*it->second) != node) { // unexpected node answers
                return;
            }
            auto& [ok, err] = status[TNodeLocation(it->second->GetLocation()).GetDataCenterId()];
            Y_ABORT_UNLESS(err);
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
    bool HasStorageQuorum(const NKikimrBlobStorage::TStorageConfig& config, T&& generateSuccessful,
            const TNodeWardenConfig& nwConfig, bool allowUnformatted) {
        auto makeError = [&](TString error) -> bool {
            STLOG(PRI_CRIT, BS_NODE, NWDC41, "configuration incorrect", (Error, error));
            Y_DEBUG_ABORT("%s", error.c_str());
            return false;
        };
        if (!config.HasBlobStorageConfig()) { // no storage config at all -- however, this is quite strange
            return makeError("no BlobStorageConfig section in config");
        }
        const auto& bsConfig = config.GetBlobStorageConfig();
        if (!bsConfig.HasServiceSet()) { // maybe this is initial configuration
            return !config.GetGeneration() || makeError("non-initial configuration with missing ServiceSet");
        }
        const auto& ss = bsConfig.GetServiceSet();

        // build map of group infos
        struct TGroupRecord {
            TIntrusivePtr<TBlobStorageGroupInfo> Info;
            TBlobStorageGroupInfo::TGroupVDisks Confirmed; // a set of confirmed group disks

            TGroupRecord(TIntrusivePtr<TBlobStorageGroupInfo>&& info)
                : Info(std::move(info))
                , Confirmed(&Info->GetTopology())
            {}
        };
        THashMap<ui32, TGroupRecord> groups;
        for (const auto& group : ss.GetGroups()) {
            const ui32 groupId = group.GetGroupID();
            if (TGroupID(groupId).ConfigurationType() != EGroupConfigurationType::Static) {
                return makeError("nonstatic group id in static configuration section");
            }

            TStringStream err;
            TIntrusivePtr<TBlobStorageGroupInfo> info = TBlobStorageGroupInfo::Parse(group, &nwConfig.StaticKey, &err);
            if (!info) {
                return makeError(TStringBuilder() << "failed to parse static group " << groupId << ": " << err.Str());
            }

            if (const auto [it, inserted] = groups.emplace(groupId, std::move(info)); !inserted) {
                return makeError("duplicate group id in static configuration section");
            }
        }

        // fill in pdisk map
        THashMap<std::tuple<ui32, ui32, ui64>, TString> pdiskIdToPath; // (nodeId, pdiskId, pdiskGuid) -> path
        for (const auto& pdisk : ss.GetPDisks()) {
            const auto [it, inserted] = pdiskIdToPath.emplace(std::make_tuple(pdisk.GetNodeID(), pdisk.GetPDiskID(),
                pdisk.GetPDiskGuid()), pdisk.GetPath());
            if (!inserted) {
                return makeError("duplicate pdisk in static configuration section");
            }
        }

        // create confirmation map
        THashMultiMap<std::tuple<ui32, TString, std::optional<ui64>>, TVDiskID> confirm;
        for (const auto& vdisk : ss.GetVDisks()) {
            if (!vdisk.HasVDiskID() || !vdisk.HasVDiskLocation()) {
                return makeError("incorrect TVDisk record");
            }
            if (vdisk.GetEntityStatus() == NKikimrBlobStorage::EEntityStatus::DESTROY) {
                continue;
            }
            if (vdisk.HasDonorMode()) {
                continue;
            }
            const auto vdiskId = VDiskIDFromVDiskID(vdisk.GetVDiskID());
            const auto it = groups.find(vdiskId.GroupID.GetRawId());
            if (it == groups.end()) {
                return makeError(TStringBuilder() << "VDisk " << vdiskId << " does not match any static group");
            }
            const TGroupRecord& group = it->second;
            if (vdiskId.GroupGeneration != group.Info->GroupGeneration) {
                return makeError(TStringBuilder() << "VDisk " << vdiskId << " group generation mismatch");
            }
            const auto& location = vdisk.GetVDiskLocation();
            const auto jt = pdiskIdToPath.find(std::make_tuple(location.GetNodeID(), location.GetPDiskID(),
                location.GetPDiskGuid()));
            if (jt == pdiskIdToPath.end()) {
                return makeError(TStringBuilder() << "VDisk " << vdiskId << " points to incorrect PDisk record");
            }
            confirm.emplace(std::make_tuple(location.GetNodeID(), jt->second, location.GetPDiskGuid()), vdiskId);
            if (allowUnformatted) {
                confirm.emplace(std::make_tuple(location.GetNodeID(), jt->second, std::nullopt), vdiskId);
            }
        }

        // process responded nodes
        generateSuccessful([&](const TNodeIdentifier& node, const TString& path, std::optional<ui64> guid) {
            const auto key = std::make_tuple(node.NodeId(), path, guid);
            const auto [begin, end] = confirm.equal_range(key);
            for (auto it = begin; it != end; ++it) {
                const TVDiskID& vdiskId = it->second;
                TGroupRecord& group = groups.at(vdiskId.GroupID.GetRawId());
                group.Confirmed |= {&group.Info->GetTopology(), vdiskId};
            }
        });

        // scan all groups and find ones without quorum
        for (const auto& [groupId, group] : groups) {
            if (const auto& checker = group.Info->GetQuorumChecker(); !checker.CheckQuorumForGroup(group.Confirmed)) {
                return false;
            }
        }

        return true; // all group meet their quorums
    }

    // Ensure configuration has quorum in both disk and storage ways for current and previous configuration.
    template<typename T>
    bool HasConfigQuorum(const NKikimrBlobStorage::TStorageConfig& config, T&& generateSuccessful,
            const TNodeWardenConfig& nwConfig, bool mindPrev = true) {
        return HasDiskQuorum(config, generateSuccessful) &&
            HasStorageQuorum(config, generateSuccessful, nwConfig, true) && (!mindPrev || !config.HasPrevConfig() || (
            HasDiskQuorum(config.GetPrevConfig(), generateSuccessful) &&
            HasStorageQuorum(config.GetPrevConfig(), generateSuccessful, nwConfig, false)));
    }

    std::optional<TString> ValidateConfigUpdate(const NKikimrBlobStorage::TStorageConfig& current,
            const NKikimrBlobStorage::TStorageConfig& proposed);

    std::optional<TString> ValidateConfig(const NKikimrBlobStorage::TStorageConfig& config);

} // NKikimr::NStorage

template<>
struct THash<std::optional<ui64>> {
    size_t operator ()(std::optional<ui64> x) const { return MultiHash(x.has_value(), x.value_or(0)); }
};

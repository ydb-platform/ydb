#pragma once

#include "defs.h"
#include "bind_queue.h"
#include "node_warden.h"
#include "node_warden_events.h"

namespace NKikimr::NStorage {

    class TDistributedConfigKeeper : public TActorBootstrapped<TDistributedConfigKeeper> {
        using TEvGather = NKikimrBlobStorage::TEvNodeConfigGather;
        using TEvScatter = NKikimrBlobStorage::TEvNodeConfigScatter;

        struct TEvPrivate {
            enum {
               EvProcessPendingEvent = EventSpaceBegin(TEvents::ES_PRIVATE),
               EvQuorumCheckTimeout,
               EvStorageConfigLoaded,
               EvStorageConfigStored,
            };

            struct TEvStorageConfigLoaded : TEventLocal<TEvStorageConfigLoaded, EvStorageConfigLoaded> {
                bool Success = false;
                NKikimrBlobStorage::TPDiskMetadataRecord Record;
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
            THashSet<ui32> BoundNodeIds; // a set of provided bound nodes by this peer (not including itself)
            THashSet<ui64> ScatterTasks; // unanswered scatter queries

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
        TString State; // configuration state

        // current most relevant storage config
        NKikimrBlobStorage::TStorageConfig StorageConfig;

        // most relevant proposed config
        std::optional<NKikimrBlobStorage::TStorageConfig> ProposedStorageConfig;
        std::optional<ui64> ProposedStorageConfigCookie;
        using TPersistCallback = std::function<void(TEvPrivate::TEvStorageConfigStored&)>;
        struct TPersistQueueItem {
            THPTimer Timer;
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
        THashMap<ui32, TBoundNode> DirectBoundNodes; // a set of nodes directly bound to this one
        THashMap<ui32, ui32> AllBoundNodes; // counter may be more than 2 in case of races, but not for long

        // pending event queue
        std::deque<TAutoPtr<IEventHandle>> PendingEvents;
        std::vector<ui32> NodeIds;
        TString SelfHost;
        ui16 SelfPort = 0;

        // scatter tasks
        ui64 NextScatterCookie = RandomNumber<ui64>();
        THashMap<ui64, TScatterTask> ScatterTasks;

        // root node operation
        enum class ERootState {
            INITIAL,
            QUORUM_CHECK_TIMEOUT,
            COLLECT_CONFIG,
            PROPOSE_NEW_STORAGE_CONFIG,
            COMMIT_CONFIG,
        };
        static constexpr TDuration QuorumCheckTimeout = TDuration::Seconds(3); // time to wait after obtaining quorum
        ERootState RootState = ERootState::INITIAL;
        NKikimrBlobStorage::TStorageConfig CurrentProposedStorageConfig;

        // subscribed IC sessions
        THashMap<ui32, TActorId> SubscribedSessions;

        friend void ::Out<ERootState>(IOutputStream&, ERootState);

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::NODEWARDEN_DISTRIBUTED_CONFIG;
        }

        TDistributedConfigKeeper(TIntrusivePtr<TNodeWardenConfig> cfg);

        void Bootstrap();

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // PDisk configuration retrieval and storing

        using TPerDriveCallback = std::function<void(const TString&)>;
        static void InvokeForAllDrives(TActorId selfId, const TIntrusivePtr<TNodeWardenConfig>& cfg, const TPerDriveCallback& callback);

        static void ReadConfig(TActorSystem *actorSystem, TActorId selfId, const TIntrusivePtr<TNodeWardenConfig>& cfg,
            const TString& state);
        static void ReadConfigFromPDisk(TEvPrivate::TEvStorageConfigLoaded& msg, const TString& path, const NPDisk::TMainKey& key,
            const TString& state);
        static void MergeMetadataRecord(NKikimrBlobStorage::TPDiskMetadataRecord *to, NKikimrBlobStorage::TPDiskMetadataRecord *from);

        static void WriteConfig(TActorSystem *actorSystem, TActorId selfId, const TIntrusivePtr<TNodeWardenConfig>& cfg,
            const NKikimrBlobStorage::TPDiskMetadataRecord& record);
        static void WriteConfigToPDisk(TEvPrivate::TEvStorageConfigStored& msg,
            const NKikimrBlobStorage::TPDiskMetadataRecord& record, const TString& path, const NPDisk::TMainKey& key);

        void PersistConfig(TPersistCallback callback);
        void Handle(TEvPrivate::TEvStorageConfigStored::TPtr ev);

        void Handle(TEvPrivate::TEvStorageConfigLoaded::TPtr ev);

        static TString CalculateFingerprint(const NKikimrBlobStorage::TStorageConfig& config);
        static bool CheckFingerprint(const NKikimrBlobStorage::TStorageConfig& config);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Node handling

        void Handle(TEvInterconnect::TEvNodesInfo::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Binding to peer nodes

        void IssueNextBindRequest();
        void Handle(TEvInterconnect::TEvNodeConnected::TPtr ev);
        void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr ev);
        void Handle(TEvents::TEvUndelivered::TPtr ev);
        void UnsubscribeInterconnect(ui32 nodeId);
        void AbortBinding(const char *reason, bool sendUnbindMessage = true);
        void HandleWakeup();
        void Handle(TEvNodeConfigReversePush::TPtr ev);
        bool UpdateConfig(const NKikimrBlobStorage::TStorageConfig& config);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Binding requests from peer nodes

        void AddBound(ui32 nodeId, TEvNodeConfigPush *msg);
        void DeleteBound(ui32 nodeId, TEvNodeConfigPush *msg);
        void Handle(TEvNodeConfigPush::TPtr ev);
        void Handle(TEvNodeConfigUnbind::TPtr ev);
        void UnbindNode(ui32 nodeId, const char *reason);
        ui32 GetRootNodeId() const;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Root node operation

        void CheckRootNodeStatus();
        void HandleQuorumCheckTimeout();
        void ProcessGather(TEvGather *res);
        bool HasQuorum() const;
        void ProcessCollectConfigs(TEvGather::TCollectConfigs *res);
        void ProcessProposeStorageConig(TEvGather::TProposeStorageConfig *res);
        bool EnrichBlobStorageConfig(NKikimrConfig::TBlobStorageConfig *bsConfig,
            const NKikimrBlobStorage::TStorageConfig& config);

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

} // NKikimr::NStorage

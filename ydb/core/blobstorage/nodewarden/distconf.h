#pragma once

#include "bind_queue.h"
#include "node_warden.h"
#include "node_warden_events.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/blobstorage/base/blobstorage_console_events.h>

#include <ydb/core/protos/config.pb.h>

#include <ydb/core/base/bridge.h>
#include <ydb/core/util/backoff.h>
#include <library/cpp/json/json_value.h>
#include <util/generic/hash_multi_map.h>
#include <ydb/core/mind/bscontroller/group_mapper.h>

#include <set>

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

    using TStorageConfigPtr = std::shared_ptr<const NKikimrBlobStorage::TStorageConfig>;

} // NKikimr::NStorage

template<>
struct THash<NKikimr::NStorage::TNodeIdentifier> : THash<std::tuple<TString, ui32, ui32>> {};

template<>
struct THash<NKikimr::NStorage::TStorageConfigMeta> {
    size_t operator ()(const NKikimr::NStorage::TStorageConfigMeta& m) const {
        return MultiHash(m.GetGeneration(), m.GetFingerprint());
    }
};

namespace NKikimrConfig {

    class TStateStorageConfig;

} // NKikimrConfig

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
               EvExecuteQuery,
               EvAbortQuery,
               EvQueryFinished,
               EvConfigProposed,
               EvRetryCollectConfigsAndPropose,
               EvRetryPersistConfig,
               EvFlushRetroTraceBatch,
               EvRootProbeTimeout,
               EvBindingTimeout,
               EvRetryConfigPropagation,
               EvScatterTaskTimeout,
               EvUpdateBridgeFeatureRequirement,
            };

            struct TEvUpdateBridgeFeatureRequirement
                : TEventLocal<TEvUpdateBridgeFeatureRequirement, EvUpdateBridgeFeatureRequirement> {
                const std::optional<ui32> RequiredLevel;

                explicit TEvUpdateBridgeFeatureRequirement(std::optional<ui32> requiredLevel)
                    : RequiredLevel(requiredLevel)
                {}
            };

            struct TEvScatterTaskTimeout : TEventLocal<TEvScatterTaskTimeout, EvScatterTaskTimeout> {
                const ui64 Cookie;
                const bool LocalOnly;

                TEvScatterTaskTimeout(ui64 cookie, bool localOnly)
                    : Cookie(cookie)
                    , LocalOnly(localOnly)
                {}
            };

            struct TEvStorageConfigLoaded : TEventLocal<TEvStorageConfigLoaded, EvStorageConfigLoaded> {
                std::vector<std::tuple<TString, NKikimrBlobStorage::TPDiskMetadataRecord, std::optional<ui64>>> MetadataPerPath;
                std::vector<std::tuple<TString, std::optional<ui64>>> NoMetadata;
                std::vector<TString> Errors;
                bool Complete = true;
            };

            struct TEvStorageConfigStored : TEventLocal<TEvStorageConfigStored, EvStorageConfigStored> {
                std::vector<std::tuple<TString, bool, std::optional<ui64>>> StatusPerPath;
                bool Complete = false;
            };

            struct TEvAbortQuery : TEventLocal<TEvAbortQuery, EvAbortQuery> {
                TString ErrorReason;

                TEvAbortQuery(TString errorReason)
                    : ErrorReason(std::move(errorReason))
                {}
            };

            struct TEvConfigProposed : TEventLocal<TEvConfigProposed, EvConfigProposed> {
                std::optional<TString> ErrorReason;

                TEvConfigProposed(std::optional<TString> errorReason)
                    : ErrorReason(std::move(errorReason))
                {}
            };
        };

        struct TBinding {
            ui32 NodeId; // we have direct binding to this node
            ui32 RootNodeId = 0; // this is the terminal node id for the whole binding chain
            ui64 Cookie; // binding cookie within the session
            TActorId SessionId; // interconnect session actor
            bool SupportsDirectRootQueries = false; // advertised by the direct parent

            TBinding(ui32 nodeId, ui64 cookie)
                : NodeId(nodeId)
                , Cookie(cookie)
            {}

            TBinding(const TBinding& origin)
                : NodeId(origin.NodeId)
                , RootNodeId(origin.RootNodeId)
                , Cookie(origin.Cookie)
                , SessionId(origin.SessionId)
                , SupportsDirectRootQueries(origin.SupportsDirectRootQueries)
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
            THashSet<ui64> ExpiredScatterTasks;
            THashSet<TNodeIdentifier> BoundNodeIds; // subtree nodes, including this peer
            ui32 LastReportedRootNodeId;
            std::optional<ui32> ReportedSubtreeHeight;
            bool SupportsMetadataOnlyCollect = false;

            TBoundNode(ui64 cookie, TActorId sessionId, ui32 lastReportedRootNodeId)
                : Cookie(cookie)
                , SessionId(sessionId)
                , LastReportedRootNodeId(lastReportedRootNodeId)
            {}

            bool Expected(IEventHandle& ev) const {
                return Cookie == ev.Cookie
                    && SessionId == ev.InterconnectSession;
            }

            ui32 GetSubtreeHeight() const {
                const ui32 limit = BoundNodeIds.empty() ? 0
                    : ui32(Min<size_t>(BoundNodeIds.size(), Max<ui32>())) - 1;
                return Min(ReportedSubtreeHeight.value_or(limit), limit);
            }
        };

        struct TScepter { // the Chosen One has the scepter; it's guaranteed that only one node holds it
            ui64 Id = RandomNumber<ui64>(); // unique id
        };

        struct TScatterTaskOriginFsm {
            TString ToString() const { return "fsm"; }
        };

        struct TScatterTaskOriginTargeted {
            TActorId Sender;
            ui64 Cookie;
            TActorId InterconnectSessionId;

            TString ToString() const { return TStringBuilder() << "{Sender# " << Sender << " Cookie# " << Cookie
                << " InterconnectSessionId# " << InterconnectSessionId << '}'; }
        };

        using TScatterTaskOrigin = std::variant<
            TBinding, // when scatter is received from root
            TActorId, // locally generated by invoke processor
            TScatterTaskOriginFsm, // locally generated by configuration change FSM
            TScatterTaskOriginTargeted // when targeted scatter is issued by cluster leader to newly added nodes
        >;

        struct TScatterTask {
            const TScatterTaskOrigin Origin;
            const ui64 ScepterCounter;

            THashSet<ui32> PendingNodes;
            ui32 AsyncOperationsPending = 0;
            TEvScatter Request;
            TEvGather Response;
            std::vector<TEvGather> CollectedResponses; // from bound nodes
            TMonotonic StartTimestamp;
            TActorId TargetedPropositionValidationActor;
            bool TargetedPropositionAdmitted = false;
            TActorId ConfigReaderActor;
            ui64 PersistCookie = 0;
            bool LocalContributionExpired = false;

            TScatterTask(TScatterTaskOrigin&& origin, TEvScatter&& request, ui64 scepterCounter, TMonotonic startTimestamp)
                : Origin(std::move(origin))
                , ScepterCounter(scepterCounter)
                , StartTimestamp(startTimestamp)
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

        // the cluster runs in bridge mode; the one not converted yet runs single pile in implicit state, while it is the
        // ordinary cluster for configuration management
        bool IsBridged() const {
            return BridgeInfo && !BridgeInfo->IsImplicit;
        }

        // currently active storage config
        TStorageConfigPtr StorageConfig;
        TString MainConfigYaml; // the part we have to push (unless this is storage-only) to console
        std::optional<ui64> MainConfigYamlVersion;
        TString MainConfigFetchYaml; // the part we would get is we fetch from console
        ui64 MainConfigFetchYamlHash = 0;
        std::optional<TString> StorageConfigYaml; // set if dedicated storage yaml is enabled; otherwise nullopt

        // base config from config file
        TStorageConfigPtr BaseConfig;

        // newest committed storage config
        TStorageConfigPtr CommittedStorageConfig;

        // locally committed storage config
        TStorageConfigPtr LocalCommittedStorageConfig;

        // last reported storage config to NW
        std::tuple<TStorageConfigPtr, bool, TBridgeInfo::TPtr, TStorageConfigPtr> LastReportedStorageConfig;

        // proposed storage configuration of the cluster
        bool PersistProposedStorageConfigInFlight = false;

        // most relevant proposed config
        using TPersistCallback = std::function<void(TEvPrivate::TEvStorageConfigStored&)>;
        struct TPersistOperation {
            TPersistCallback Callback;
            size_t RepliesPending = 0;
        };
        struct TPersistQueueItem {
            THPTimer Timer;
            NKikimrBlobStorage::TPDiskMetadataRecord Metadata;
            ui64 Cookie = 0;
        };
        ui64 NextPersistCookie = 1;
        THashMap<ui64, TPersistOperation> PersistOperations;
        // The front is already writing; only pending callbackless tails may be coalesced.
        THashMap<TString, std::deque<TPersistQueueItem>> PersistQ;
        THashMap<TString, NKikimrBlobStorage::TPDiskMetadataRecord> MetadataByPath;
        THashSet<TString> PathsToRetry;
        bool PersistRetryScheduled = false;
        std::vector<TString> PrevDrivesToRead;

        // initialization state
        bool NodeListObtained = false;
        bool StorageConfigLoaded = false;

        // outgoing binding
        static constexpr TDuration BindRequestTimeout = TDuration::Seconds(3);
        std::optional<TBinding> Binding;
        std::optional<TBinding> RootProbe;
        class TBindingLivenessActor;
        TActorId BindingLivenessActorId;
        ui64 BindingCookie = RandomNumber<ui64>();
        TBindQueue BindQueue;
        TBindQueue RevBindQueue;
        TBindQueue OtherPilesBindQueue;
        ui32 NextBindQueue = 0;
        bool Scheduled = false;

        // unbound-state diagnostic
        ui32 BindFailuresStreak = 0;
        TInstant LastUnboundWarnAt = TInstant::Zero();

        // incoming bindings
        struct TIndirectBoundNode {
            std::list<TStorageConfigMeta> Configs; // last one is the latest one
            THashMap<ui32, std::list<TStorageConfigMeta>::iterator> Refs;
        };
        THashMap<ui32, TBoundNode> DirectBoundNodes; // a set of nodes directly bound to this one
        std::multiset<ui32> BoundSubtreeHeights;
        bool ConfigPropagationRetryScheduled = false;
        TDuration ConfigPropagationRetryDelay = TDuration::Seconds(1);
        std::optional<TStorageConfigMeta> KnownAppliedConfig;
        bool KnownAppliedConfigConflicting = false;
        ui64 MaxObservedConfigGeneration = 0;
        ui64 ConfigRecoveryGeneration = 0;
        bool ConfigRecoveryRequired = false;
        bool ConfigRecoveryScheduled = false;
        class TConfigRefillActor;
        TActorId ConfigRefillActorId;
        std::optional<NKikimrBlobStorage::TStorageConfig> PendingConfigRefill;
        bool LegacyConfigRefill = false;
        THashMap<TNodeIdentifier, TIndirectBoundNode> AllBoundNodes; // a set of all bound nodes in tree, including this one
        THashSet<TBridgePileId> ConnectedUnsyncedPiles;

        // pending event queue
        std::deque<TAutoPtr<IEventHandle>> PendingEvents;
        std::vector<ui32> NodeIdsForOutgoingBinding;
        std::vector<ui32> NodeIdsForIncomingBinding;
        std::vector<ui32> NodeIdsForOtherPilesOutgoingBinding;
        THashMap<ui32, TNodeIdentifier> AllNodeIds;
        THashSet<ui32> NodesFromSamePile;
        TNodeIdentifier SelfNode;
        TBridgePileId SelfNodeBridgePileId;
        THashSet<std::tuple<TString, ui32>> Hosts;

        // scatter tasks
        ui64 NextScatterCookie = RandomNumber<ui64>();
        using TScatterTasks = THashMap<ui64, TScatterTask>;
        TScatterTasks ScatterTasks;
        std::set<std::tuple<ui32, ui64>> AddedNodesScatterTasks;

        std::optional<TActorId> StateStorageSelfHealActor;
        class TStateStorageSelfHealRefreshActor;
        std::optional<TActorId> StateStorageSelfHealRefreshActor;
        bool StateStorageSelfHealRefreshStarted = false;
        void StartStateStorageSelfHealRefresh();
        void CancelStateStorageSelfHealRefresh();

        // root node operation
        enum class ERootState {
            INITIAL,       // possible binding, no scepter, no operation in progress
            ERROR_TIMEOUT, // possible binding, no scepter, no operation in progress
            IN_PROGRESS,   // no binding, scepter, global quorum operation in progress
            RELAX,         // no binding, scepter, no operation in progress
        };
        static constexpr TDuration ErrorTimeout = TDuration::Seconds(3);
        ERootState RootState = ERootState::INITIAL;
        TBackoffTimer CollectConfigsBackoffTimer{10, 1000};

        struct TProposition {
            NKikimrBlobStorage::TStorageConfig StorageConfig; // storage config being proposed
            TActorId ActorId; // actor id waiting for this operation to complete
            bool MindPrev; // mind previous configuration quorum
            ui64 ConfigRecoveryGeneration;
            bool AdvancesConflictingGeneration = false;
            bool RecoveryProposition = false;
            std::vector<TNodeIdentifier> AddedOrChangedNodeIdentifiers; // identifiers of added nodes or changed endpoints
        };
        std::optional<TProposition> CurrentProposition;

        std::shared_ptr<TScepter> Scepter;
        ui64 ScepterCounter = 1; // increased every time Scepter gets changed
        TString ErrorReason;
        std::optional<TString> CurrentSelfAssemblyUUID;
        bool GlobalQuorum = false;
        bool QuorumValid = false;

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

        // pipe to Console
        TActorId ConsolePipeId;
        bool ConsoleConnected = false;
        ui64 ValidateRequestCookie = 0;
        ui64 ProposeRequestCookie = 0;
        ui64 CommitRequestCookie = 0;
        bool ProposeRequestInFlight = false;
        bool CommitRequestInFlight = false;
        std::optional<std::tuple<ui64, ui32>> ProposedConfigHashVersion;
        TString ProposedConfigYaml;
        std::vector<std::tuple<TActorId, TString, bool, ui64>> ConsoleConfigValidationQ;

        // retro trace root-side batching
        TControlWrapper RootRetroTraceBatchIntervalSec = TControlWrapper(10, 1, 3600);
        std::vector<NWilson::TTraceId> PendingRetroTraceIds;
        bool RetroTraceBatchFlushScheduled = false;

        void HandleFlushRetroTraceBatch();
        void FlushRetroTraceBatch();

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

        TDistributedConfigKeeper(TIntrusivePtr<TNodeWardenConfig> cfg, TStorageConfigPtr baseConfig, bool isSelfStatic);

        void Bootstrap();
        void PassAway() override;
        void Halt(); // cease any distconf activity, unbind and reject any bindings
        bool ApplyStorageConfig(const NKikimrBlobStorage::TStorageConfig& config);
        void ApplyCommittedStorageConfig(const NKikimrBlobStorage::TStorageConfig& config);
        void Handle(TEvNodeWardenUpdateConfigFromPeer::TPtr ev);
        void HandleConfigConfirm(STATEFN_SIG);
        void ReportStorageConfigToNodeWarden();

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // PDisk configuration retrieval and storing

        TActorId ReadConfig(std::vector<TString> paths, ui64 cookie = 0);
        void WriteConfig(TString path, NKikimrBlobStorage::TPDiskMetadataRecord record, ui64 cookie);
        void StartPersistWrite(const TString& path);
        ui64 PersistConfig(TPersistCallback callback, const std::vector<TString>& drives);
        void CancelPersistConfig(ui64 cookie);
        void Handle(TEvPrivate::TEvStorageConfigStored::TPtr ev);
        void HandleRetryPersistConfig();
        void Handle(TEvPrivate::TEvStorageConfigLoaded::TPtr ev);

        std::vector<TString> GetDrives(const NKikimrBlobStorage::TStorageConfig& config) const;

        static TString CalculateFingerprint(const NKikimrBlobStorage::TStorageConfig& config);
        static void UpdateFingerprint(NKikimrBlobStorage::TStorageConfig *config);
        static bool CheckFingerprint(const NKikimrBlobStorage::TStorageConfig& config);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Node handling

        void Handle(TEvInterconnect::TEvNodesInfo::TPtr ev);

        void ApplyNewNodeList(std::span<std::tuple<TNodeIdentifier, TNodeLocation>> newNodeList);

        // pile registry of the provided config as this node sees it (see GetPileRegistry)
        std::optional<NKikimrBridge::TPileRegistry> GetConfigPileRegistry(
            const NKikimrBlobStorage::TStorageConfig& config) const;
        // pile registry of the current storage config (or the base one, if there is no current yet)
        std::optional<NKikimrBridge::TPileRegistry> GetCurrentPileRegistry() const;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Binding to peer nodes

        void IssueNextBindRequest();
        void StartBinding(ui32 nodeId);
        void BindToSession(TActorId sessionId);
        bool HasStaticGroupConfig() const;
        void StartRootProbe(ui32 nodeId);
        void SendRootProbe(TActorId sessionId);
        void AbortRootProbe();
        void HandleRootProbeTimeout(STATEFN_SIG);
        void HandleBindingTimeout(STATEFN_SIG);
        void CheckBindingLiveness();
        void CancelBindingLiveness();
        void Handle(TEvNodeConfigInvokeOnRootResult::TPtr ev);
        void Handle(TEvInterconnect::TEvNodeConnected::TPtr ev);
        void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr ev);
        void HandleDisconnect(ui32 nodeId, TActorId sessionId);
        bool IsProvisionalPeer(ui32 nodeId) const;
        void UnsubscribeInterconnect(ui32 nodeId);
        TActorId SubscribeToPeerNode(ui32 nodeId, TActorId sessionId);
        void AbortBinding(const char *reason, bool sendUnbindMessage = true, bool sendUpdate = true);
        void LogUnboundBindingWarning();
        void HandleWakeup();
        void Handle(TEvNodeConfigReversePush::TPtr ev);
        void RequestConfigRefill();
        void CancelConfigRefill();
        bool DeferConfigRefill(const NKikimrBlobStorage::TStorageConfig& config);
        void FanOutReversePush(const NKikimrBlobStorage::TStorageConfig *committedStorageConfig, bool onlyOutdated = false);
        bool NeedsConfigUpdate(ui32 refererNodeId, const TBoundNode& node,
                               const NKikimrBlobStorage::TStorageConfig& config) const;
        void HandleRetryConfigPropagation(STATEFN_SIG);
        bool RememberAppliedConfig(const TStorageConfigMeta& meta);
        void RequestConfigRecovery(bool invalidateInFlight);
        void ScheduleConfigRecovery();
        bool HasLocalConfig() const;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Binding requests from peer nodes

        TBindQueue *GetBindQueue(ui32 nodeId);
        bool UpdateBound(ui32 refererNodeId, TNodeIdentifier nodeId, const TStorageConfigMeta& meta, TEvNodeConfigPush *msg);
        void DeleteBound(ui32 refererNodeId, const TNodeIdentifier& nodeId, TEvNodeConfigPush *msg);
        void Handle(TEvNodeConfigPush::TPtr ev);
        void Handle(TEvNodeConfigUnbind::TPtr ev);
        void UnbindNode(ui32 nodeId, const char *reason);
        ui32 GetRootNodeId() const;
        bool PartOfNodeQuorum() const;
        ui32 GetSubtreeHeight() const;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Root node operation

        void UpdateQuorums();
        void ReconcileNodeRole();
        void HandleRetryCollectConfigsAndPropose(STATEFN_SIG);
        void BecomeRoot();
        void UnbecomeRoot();
        void HandleErrorTimeout();
        void UndoCurrentPropositionNodeChange(TProposition& proposition);
        void ProcessGather(TEvGather *res);
        bool HasConnectedNodeQuorum(const NKikimrBlobStorage::TStorageConfig& config) const;
        bool ObserveConfigGeneration(const NKikimrBlobStorage::TStorageConfig& config);

        struct TProcessCollectConfigsResult {
            std::optional<TString> ErrorReason;
            bool IsDistconfDisabledQuorum = false;
            std::optional<NKikimrBlobStorage::TStorageConfig> RecoveredConfig;
            bool RequiresGenerationAdvance = false;
            bool RequiresRecoveryProposition = false;
            std::optional<NKikimrBlobStorage::TStorageConfig> PropositionBase;
            std::optional<NKikimrBlobStorage::TStorageConfig> ConfigToPropose;
            bool AutomaticBootstrap = false;
        };
        TProcessCollectConfigsResult ProcessCollectConfigs(TEvGather::TCollectConfigs *res,
            std::optional<TString> selfAssemblyUUID, bool dryRun = false, bool allowConflictingGenerationAdvance = false,
            const NKikimrBlobStorage::TStorageConfig *collectionQuorumConfig = nullptr);

        void ProcessProposeStorageConfig(TEvGather::TProposeStorageConfig *res);
        bool IsBelowKnownAppliedConfig(const NKikimrBlobStorage::TStorageConfig& config) const;
        bool IsConflictingGenerationAdvance(const TProposition& proposition) const;
        bool IsPropositionCurrent(const TProposition& proposition) const;

        struct TExConfigError : yexception {};

        std::optional<TString> GenerateFirstConfig(NKikimrBlobStorage::TStorageConfig *config, const TString& selfAssemblyUUID);

        struct TStaticGroupReassignment {
            std::optional<NKikimrBlobStorage::TVSlotId> SourceSlotId;
            std::optional<NKikimrBlobStorage::TVSlotId> TargetSlotId;
        };

        using TStaticGroupReassignments = THashMap<TVDiskIdShort, TStaticGroupReassignment>;

        struct TAllocateStaticGroupParams {
            NKikimrBlobStorage::TStorageConfig *Config = nullptr;
            TGroupId GroupId;
            ui32 GroupGeneration = 0;
            TBlobStorageGroupType GroupType;
            THashMap<TVDiskIdShort, NBsController::TPDiskId> ReplacedDisks;
            NBsController::TGroupMapper::TForbiddenPDisks ForbiddenPDisks;
            std::optional<i64> RequiredSpace;
            const NKikimrBlobStorage::TBaseConfig *BaseConfig = nullptr;
            bool ConvertToDonor = false;
            bool IgnoreVSlotQuotaCheck = false;
            NBsController::TGroupLayoutPolicy LayoutPolicy;
            bool AllowUnusableDisks = false;
            bool SettleOnlyOnOperationalDisks = false;
            bool IsSelfHealReasonDecommit = false;
            bool PreferLessOccupiedRack = false;
            bool WithAttentionToReplication = false;
            bool UseSelfHealLocalPolicy = false;
            bool TryToRelocateBrokenDisksLocallyFirst = false;
            TBridgePileId BridgePileId;
            std::optional<TGroupId> BridgeProxyGroupId;
            bool ApplySelfHealNodeAllowList = false;
            TStaticGroupReassignments *Reassignments = nullptr;
        };

        static TAllocateStaticGroupParams BuildStaticGroupReassignParams(NKikimrBlobStorage::TStorageConfig *config,
                                                                         const NKikimrBlobStorage::TBaseConfig *baseConfig,
                                                                         const NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot::TReassignGroupDisk& command,
                                                                         const NKikimrBlobStorage::TGroupInfo& group,
                                                                         const NKikimrBlobStorage::TNodeWardenServiceSet& serviceSet,
                                                                         TStaticGroupReassignments *reassignments);

        void AllocateStaticGroup(TAllocateStaticGroupParams params);

        bool UpdateConfig(NKikimrBlobStorage::TStorageConfig *config);

        void PrepareScatterTask(ui64 cookie, TScatterTask& task);
        class TTargetedPropositionValidationActor;
        void CompleteTargetedPropositionValidation(ui64 cookie, TActorId actorId,
                                                  const TStorageConfigMeta& meta, bool confirmed);

        void PerformScatterTask(TScatterTask& task);
        void Perform(TEvGather::TCollectConfigs *response, const TEvScatter::TCollectConfigs& request, TScatterTask& task);
        void Perform(TEvGather::TProposeStorageConfig *response, const TEvScatter::TProposeStorageConfig& request, TScatterTask& task);
        void Perform(TEvGather::TDemandRetroTrace *response, const TEvScatter::TDemandRetroTrace& request, TScatterTask& task);
        void Perform(TEvGather::TCollectBridgeCapability *response, const TEvScatter::TCollectBridgeCapability& request,
            TScatterTask& task);

        void SwitchToError(const TString& reason);
        void StopRootActivities(const TString& reason);

        std::optional<TString> StartProposition(NKikimrBlobStorage::TStorageConfig *configToPropose,
            const NKikimrBlobStorage::TStorageConfig *propositionBase, TActorId actorId, bool mindPrev,
            bool advancesConflictingGeneration = false, bool recoveryProposition = false);

        void CheckForConfigUpdate();

        static std::optional<TString> TransformConfigBeforeCommit(NKikimrBlobStorage::TStorageConfig *config);
        static std::optional<TString> CheckSynchronizedPiles(NKikimrBlobStorage::TStorageConfig *config);
        static std::optional<TString> UpdateClusterStateGeneration(NKikimrBlobStorage::TStorageConfig *config);
        static std::optional<TString> TrimHistoryEntries(NKikimrBlobStorage::TStorageConfig *config);
        static std::optional<TString> UpdateBridgeGroupStateGenerations(NKikimrBlobStorage::TStorageConfig *config);
        static std::optional<TString> ExtractStateStorageConfig(NKikimrBlobStorage::TStorageConfig *config);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Generate state storage config

        std::unordered_map<ui32, ui32> SelfHealNodesState;

        bool GenerateStateStorageConfig(NKikimrConfig::TStateStorageConfig *ss
            , const NKikimrBlobStorage::TStorageConfig& baseConfig
            , std::unordered_set<ui32>& usedNodes
            , const std::unordered_set<ui32>& nodesToUse = {}
            , const NKikimrConfig::TStateStorageConfig *oldConfig = nullptr
            , bool automaticManagement = true
            , ui32 overrideReplicasInRingCount = 0
            , ui32 overrideRingsCount = 0
            , ui32 replicasSpecificVolume = 200
        );

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Bridge ops

        bool UpdateBridgeConfig(NKikimrBlobStorage::TStorageConfig *config);

        static void GenerateBridgeInitialState(const TNodeWardenConfig& cfg, NKikimrBlobStorage::TStorageConfig *config);

        // Add INACTIVE pile to the cluster in the config: allocate parts of bridged static groups and state storage ring
        // groups in the pile, make it DISCONNECTED and unsynced, and leave the pending operation for BSC to add parts of
        // its groups; baseConfig provides PDisks already known to BSC.
        std::optional<TString> AddBridgePileToConfig(NKikimrBlobStorage::TStorageConfig *config, TBridgePileId pileId,
            const NKikimrBlobStorage::TBaseConfig *baseConfig);

        // Finish removal of DISCONNECTED pile from the cluster in the config, when BSC has deleted parts of its groups in
        // the pile: drop parts of bridged static groups (their VDisks are destroyed by their nodes) and state storage ring
        // groups of the pile, forget its sync state, make it INACTIVE and clear the pending operation.
        static std::optional<TString> RemoveBridgePileFromConfig(NKikimrBlobStorage::TStorageConfig *config,
            TBridgePileId pileId);

        TBridgeInfo::TPtr GenerateBridgeInfo(const NKikimrBlobStorage::TStorageConfig& config);
        static TBridgeInfo::TPtr GenerateBridgeInfo(const NKikimrBlobStorage::TStorageConfig& config,
            const TNodeWardenConfig& cfg, ui32 selfNodeId);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Scatter/gather logic

        void IssueScatterTask(TScatterTaskOrigin&& origin, TEvScatter&& request, std::span<const TNodeIdentifier> targetedNodes = {});
        void PrepareScatterTaskRequest(ui64 cookie, const TScatterTask& task, ui32 subtreeHeight, TEvScatter& request);
        void IssueAddedNodeScatterTask(ui32 nodeId, ui64 cookie, TScatterTask& task);
        void CheckCompleteScatterTask(TScatterTasks::iterator it);
        void FinishAsyncOperation(ui64 cookie);
        void ExpireScatterTaskLocalContribution(TScatterTask& task);
        void Handle(TEvPrivate::TEvScatterTaskTimeout::TPtr ev);
        void IssueScatterTaskForNode(ui32 nodeId, TBoundNode& info, ui64 cookie, TScatterTask& task);
        void CompleteScatterTask(TScatterTask& task);
        void AbortScatterTask(ui64 cookie, ui32 nodeId);
        void AbortAllScatterTasks(const std::optional<TBinding>& binding);
        void Handle(TEvNodeConfigScatter::TPtr ev);
        void Handle(TEvNodeConfigGather::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // NodeWarden RPC

        struct TInvokeExternalOperation {
            NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot Command;
            TActorId Sender;
            TActorId SessionId;
            ui64 Cookie;
        };

        struct TFreshConfigQueryGroup {
            std::vector<TInvokeExternalOperation> AdditionalRequests;
            THashMap<ui32, TInvokeExternalOperation> RefillRequests;
            TActorId ActorId;
            bool Started = false;
        };
        std::weak_ptr<TFreshConfigQueryGroup> PendingFreshConfigQueryGroup;
        THashMap<ui32, std::weak_ptr<TFreshConfigQueryGroup>> PendingRefillQueryGroups;

        struct TCollectConfigsAndPropose {
        };

        struct TProposeConfig {
            NKikimrBlobStorage::TStorageConfig Config;
        };

        using TInvokeQuery = std::variant<
            TInvokeExternalOperation,
            TCollectConfigsAndPropose,
            TProposeConfig
        >;

        struct TInvokeOperation {
            TActorId ActorId; // originating actor id or empty if this was triggered by system
        };
        std::deque<TInvokeOperation> InvokeQ; // operations queue; first is always the being executed one
        std::deque<TInvokeOperation> InvokePending; // collected while in ERROR_TIMEOUT state
        std::deque<TEvNodeConfigInvokeOnRoot::TPtr> InvokeOnRootPending;
        bool DeadActorWaitingForProposition = false;

        class TInvokeRequestHandlerActor;
        struct TLifetimeToken {};
        std::shared_ptr<TLifetimeToken> LifetimeToken = std::make_shared<TLifetimeToken>();

        THashSet<TActorId> DetachedQueries;

        ui64 InvokePipelineGeneration = 1;

        void HandleInvokeOnRoot(TEvNodeConfigInvokeOnRoot::TPtr ev);
        void Invoke(TInvokeQuery&& query);
        void HandleQueryFinished(STFUNC_SIG);

        void OpQueueOnError(const TString& errorReason);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Dynamic node interaction

        TBindQueue StaticBindQueue;
        THashMap<TActorId, TActorId> DynamicConfigSubscribers; // <session id> -> <actor id>
        ui32 ConnectedToStaticNode = 0;
        TActorId StaticNodeSessionId;
        bool ReconnectScheduled = false;
        THashSet<ui32> ConnectedDynamicNodes;
        THashSet<ui32> BridgeCapableDynamicNodes; // subset of ConnectedDynamicNodes running with BridgeConfig
        THashMap<ui32, ui32> DynamicNodeFeatureLevels; // bridge feature levels of the ConnectedDynamicNodes reporting any
        ui64 StaticNodeSubscriptionCookie = 0;

        // these are used on the dynamic nodes
        void ApplyStaticNodeIds(const std::vector<ui32>& nodeIds);
        void ConnectToStaticNode();
        void HandleReconnect();
        void OnStaticNodeConnected(ui32 nodeId, TActorId sessionId, ui64 cookie);
        void OnStaticNodeDisconnected(ui32 nodeId, TActorId sessionId, ui64 cookie);
        void Handle(TEvNodeWardenDynamicConfigPush::TPtr ev);

        // these are used on the static nodes
        void ApplyConfigUpdateToDynamicNodes(bool drop);
        void OnDynamicNodeDisconnected(ui32 nodeId, TActorId sessionId);
        void Handle(TEvNodeWardenDynamicConfigSubscribe::TPtr ev);
        // the node doesn't get the config requiring higher bridge feature level than the one it supports
        void PushConfigToDynamicNode(TActorId actorId, TActorId sessionId, bool addCache, ui32 bridgeFeatureLevel);

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
        void ResetConsoleSession();
        bool EnqueueConsoleConfigValidation(TActorId queryId, bool enablingDistconf, TString yaml, bool allowUnknownFields);

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
            const NKikimrBlobStorage::TStorageConfig& proposed);

    std::optional<TString> ValidateConfig(const NKikimrBlobStorage::TStorageConfig& config);

    std::optional<TString> DecomposeConfig(const TString& configComposite, TString *mainConfigYaml,
        ui64 *mainConfigVersion, TString *mainConfigFetchYaml);

    std::optional<TString> GetStorageYaml(const NKikimrBlobStorage::TStorageConfig& config);

    void UpdateClusterStateGuid(NKikimrBridge::TClusterState *clusterState);

    // Pile registry of the bridged cluster: the one persisted in the config, or the one implied by node's BridgeConfig
    // when there is none persisted; nullopt when the node is not bridge-capable (has no BridgeConfig).
    std::optional<NKikimrBridge::TPileRegistry> GetPileRegistry(const NKikimrBlobStorage::TStorageConfig& config,
        const NKikimrConfig::TBridgeConfig *bridgeConfig);

    // Nodes of older versions take pile ids from positions of piles in their BridgeConfig, so they can't work with the
    // config (and must not get it) once the list of piles has been changed.
    inline bool IsPileListChanged(const NKikimrBlobStorage::TStorageConfig& config) {
        return config.GetClusterStateDetails().GetPileRegistry().GetPileListChanged();
    }

    // Levels of bridge features supported by nodes; nodes supporting lower level than the config requires can't work with
    // it, so they don't get it and they are not connected to.
    enum EBridgeFeatureLevel : ui32 {
        // pile ids are taken from pile registry; older versions report it by PileListChangeCapable flag
        BridgeFeatureLevelPileList = 1,
        // piles are added and removed with pending operations, static group ids are never reused, static groups going
        // away are ejected, inline pile may be removed
        BridgeFeatureLevelPileOps = 2,
        BridgeFeatureLevelNonUniformStorage = 3,
        BridgeFeatureLevelPileOverrides = 4,
    };

    // bridge feature level of this version; every node has to support it to change the list of piles, add and remove them
    constexpr ui32 BridgeFeatureLevel = BridgeFeatureLevelPileOverrides;

    inline ui32 GetRequiredBridgeFeatureLevel(const NKikimrBlobStorage::TStorageConfig& config) {
        const auto& registry = config.GetClusterStateDetails().GetPileRegistry();
        return Max<ui32>(registry.GetRequiredFeatureLevel(),
            registry.GetPileListChanged() ? BridgeFeatureLevelPileList : 0);
    }

    // bridge feature level of the node from its report (older versions report only PileListChangeCapable flag)
    inline ui32 GetReportedBridgeFeatureLevel(ui32 level, bool pileListChangeCapable) {
        return level ? level : pileListChangeCapable ? BridgeFeatureLevelPileList : 0;
    }

    // Resolve pile of the node against pile registry (as GetPileRegistry provides it, or nullopt for the config that is
    // not a bridged one): default pile (invalid pile id) when there is no registry, nullopt when the pile can't be
    // resolved.
    std::optional<TBridgePileId> ResolveNodePileId(const std::optional<NKikimrBridge::TPileRegistry>& registry,
        const TNodeLocation& location);

    // Bridge config nodes get from the main config merged with the storage one, where the main config has precedence;
    // storageConfig is null when there is no dedicated storage config. Empty when neither of them has the section.
    NKikimrConfig::TBridgeConfig GetMergedBridgeConfig(const NJson::TJsonValue& mainConfig,
        const NJson::TJsonValue *storageConfig, bool allowUnknownFields);

    // Assign pile ids to the piles of the bridge config provided with new cluster configuration and store the resulting
    // pile registry in this configuration, along with the states of the piles (new ones are INACTIVE, and only INACTIVE
    // ones may be dropped); localBridgeConfig is the one this node runs with, currentBridgeConfig is the one of the
    // config being replaced (if known). pileListChanged is set when piles are added, dropped or moved to other
    // positions in BridgeConfig, which is allowed only when every node supports it.
    std::optional<TString> UpdatePileRegistry(const NKikimrConfig::TBridgeConfig& bridgeConfig,
        NKikimrBlobStorage::TStorageConfig *config, const NKikimrConfig::TBridgeConfig *localBridgeConfig,
        const NKikimrConfig::TBridgeConfig *currentBridgeConfig, bool *pileListChanged);

    // Turn the config of ordinary (not bridged) cluster into the config of the single-pile bridged one: add ClusterState
    // and make every static group an inline pile of itself. BSC converts existing pools and their groups afterwards
    // unless their conversion is skipped, which activates non-uniform storage.
    std::optional<TString> ConvertStorageConfigToBridge(NKikimrBlobStorage::TStorageConfig *config,
        const NKikimrConfig::TBridgeConfig& bridgeConfig, bool skipStoragePoolConversion = false);

    // Make VDisks of the static groups follow their new generations (each one is bumped by one); donors and VDisks being
    // destroyed have older generations and keep them.
    void UpdateStaticVDiskGenerations(NKikimrBlobStorage::TNodeWardenServiceSet *ss,
        const THashMap<TGroupId, ui32>& newGenerations);

    // Make parts of bridged static groups in the piles unsynced as of the cluster state generation provided: they are
    // synced from the very beginning, and generations of all the parts and of their bridge proxy groups are bumped, so
    // that requests routed by older group state are refused by VDisks.
    void ResetStaticGroupPileParts(NKikimrBlobStorage::TStorageConfig *config, const std::vector<TBridgePileId>& pileIds,
        ui64 becameUnsyncedGeneration);

} // NKikimr::NStorage

template<>
struct THash<std::optional<ui64>> {
    size_t operator ()(std::optional<ui64> x) const { return MultiHash(x.has_value(), x.value_or(0)); }
};

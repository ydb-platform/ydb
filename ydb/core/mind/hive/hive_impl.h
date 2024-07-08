#pragma once
#include <bitset>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/subdomain.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/mind/local.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/tablet/tablet_exception.h>
#include <ydb/core/tablet/tablet_responsiveness_pinger.h>
#include <ydb/core/scheme/scheme_types_defs.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/tablet/tablet_pipe_client_cache.h>
#include <ydb/core/tablet/pipe_tracker.h>
#include <ydb/core/tablet/tablet_impl.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tablet_flat/flat_executor_counters.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/util/event_priority_queue.h>

#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/containers/ring_buffer/ring_buffer.h>

#include <util/generic/queue.h>
#include <util/random/random.h>

#include <ydb/core/tablet/tablet_metrics.h>

#include "hive.h"
#include "hive_transactions.h"
#include "hive_events.h"
#include "hive_domains.h"
#include "hive_schema.h"
#include "domain_info.h"
#include "tablet_info.h"
#include "leader_tablet_info.h"
#include "follower_tablet_info.h"
#include "follower_group.h"
#include "node_info.h"
#include "storage_group_info.h"
#include "storage_pool_info.h"
#include "sequencer.h"
#include "boot_queue.h"
#include "object_distribution.h"

#define DEPRECATED_CTX (ActorContext())
#define DEPRECATED_NOW (TActivationContext::Now())

template <typename T>
inline IOutputStream& operator <<(IOutputStream& out, const TVector<T>& vec) {
    out << '[';
    for (auto it = vec.begin(); it != vec.end(); ++it) {
        if (it != vec.begin())
            out << ',';
        out << *it;
    }
    out << ']';
    return out;
}

template <typename T>
inline IOutputStream& operator <<(IOutputStream& out, const std::vector<T>& vec) {
    out << '[';
    for (auto it = vec.begin(); it != vec.end(); ++it) {
        if (it != vec.begin())
            out << ',';
        out << *it;
    }
    out << ']';
    return out;
}

template <size_t I>
inline IOutputStream& operator <<(IOutputStream& out, const std::bitset<I>& vec) {
    out << '[';
    size_t bits = 0;
    for (size_t i = 0; i < I; ++i) {
        if (vec.test(i)) {
            if (bits > 0) {
                out << ',';
            }
            out << i;
            ++bits;
        }
    }
    out << ']';
    return out;
}

inline IOutputStream& operator <<(IOutputStream& out, std::pair<ui64, ui64> pr) {
    return out << std::tuple<ui64, ui64>(pr);
}

inline IOutputStream& operator <<(IOutputStream& out, NKikimr::NHive::TSequencer::TSequence sq) {
    return out << sq.Next << "@[" << sq.Begin << ".." << sq.End << ')';
}

namespace std {
    template <>
    struct hash<NKikimr::TSubDomainKey> {
        std::size_t operator()(const NKikimr::TSubDomainKey& key) const {
            return key.Hash();
        }
    };
}

namespace NKikimr {
namespace NHive {

TResourceRawValues ResourceRawValuesFromMetrics(const NKikimrTabletBase::TMetrics& metrics);
NKikimrTabletBase::TMetrics MetricsFromResourceRawValues(const TResourceRawValues& values);
TResourceRawValues ResourceRawValuesFromMetrics(const NKikimrHive::TTabletMetrics& tabletMetrics);
TString GetResourceValuesText(const NKikimrTabletBase::TMetrics& values);
TString GetResourceValuesText(const TTabletInfo& tablet);
TString GetResourceValuesText(const TResourceRawValues& values);
NJson::TJsonValue GetResourceValuesJson(const TResourceRawValues& values);
TString GetResourceValuesText(const TResourceNormalizedValues& values);
NJson::TJsonValue GetResourceValuesJson(const TResourceNormalizedValues& values);
TString GetResourceValuesHtml(const TResourceRawValues& values);
NJson::TJsonValue GetResourceValuesJson(const TResourceRawValues& values);
NJson::TJsonValue GetResourceValuesJson(const TResourceRawValues& values, const TResourceRawValues& maximum);
TString GetResourceValuesHtml(const NKikimrTabletBase::TMetrics& values);
NJson::TJsonValue GetResourceValuesJson(const NKikimrTabletBase::TMetrics& values);
ui64 GetReadThroughput(const NKikimrTabletBase::TMetrics& values);
ui64 GetWriteThroughput(const NKikimrTabletBase::TMetrics& values);
TString GetCounter(i64 counter, const TString& zero = "0");
TString GetBytes(i64 bytes, const TString& zero = "0B");
TString GetBytesPerSecond(i64 bytes, const TString& zero = "0B/s");
TString GetTimes(i64 times, const TString& zero = "0.00%");
TString GetConditionalGreyString(const TString& str, bool condition);
TString GetConditionalBoldString(const TString& str, bool condition);
TString GetConditionalRedString(const TString& str, bool condition);
TString GetValueWithColoredGlyph(double val, double maxVal);
TString GetDataCenterName(ui64 dataCenterId);
TString LongToShortTabletName(const TString& longTabletName);
TString GetLocationString(const NActors::TNodeLocation& location);
void MakeTabletTypeSet(std::vector<TTabletTypes::EType>& list);
bool IsValidTabletType(TTabletTypes::EType type);
bool IsValidObjectId(const TFullObjectId& objectId);
TString GetRunningTabletsText(ui64 runningTablets, ui64 totalTablets, bool warmUp);
bool IsResourceDrainingState(TTabletInfo::EVolatileState state);
bool IsAliveState(TTabletInfo::EVolatileState state);

class THive : public TActor<THive>, public TTabletExecutedFlat, public THiveSharedSettings {
public:
    using IActorOps::Register;

protected:
    friend class THiveBalancer;
    friend class THiveDrain;
    friend class THiveFill;
    friend class TReassignTabletWaitActor;
    friend class TMoveTabletWaitActor;
    friend class TStopTabletWaitActor;
    friend class TResumeTabletWaitActor;
    friend class TInitMigrationWaitActor;
    friend class TQueryMigrationWaitActor;
    friend class TReleaseTabletsWaitActor;
    friend class TDrainNodeWaitActor;
    friend class THiveStorageBalancer;;
    friend struct TNodeInfo;

    friend class TTxInitScheme;
    friend class TTxDeleteBase;
    friend class TTxDeleteTablet;
    friend class TTxDeleteOwnerTablets;
    friend class TTxReassignGroups;
    friend class TTxReassignGroupsOnDecommit;
    friend class TTxStartTablet;
    friend class TTxCreateTablet;
    friend class TTxCutTabletHistory;
    friend class TTxBlockStorageResult;
    friend class TTxAdoptTablet;
    friend class TTxDeleteTabletResult;
    friend class TTxMonEvent_MemStateTablets;
    friend class TTxMonEvent_MemStateNodes;
    friend class TTxMonEvent_MemStateDomains;
    friend class TTxMonEvent_Resources;
    friend class TTxMonEvent_Settings;
    friend class TTxMonEvent_Landing;
    friend class TTxMonEvent_LandingData;
    friend class TTxMonEvent_SetDown;
    friend class TTxMonEvent_SetFreeze;
    friend class TTxMonEvent_KickNode;
    friend class TTxMonEvent_DrainNode;
    friend class TTxMonEvent_Rebalance;
    friend class TTxMonEvent_Storage;
    friend class TTxMonEvent_FindTablet;
    friend class TTxMonEvent_MoveTablet;
    friend class TTxMonEvent_StopTablet;
    friend class TTxMonEvent_ResumeTablet;
    friend class TTxMonEvent_InitMigration;
    friend class TTxMonEvent_QueryMigration;
    friend class TTxMonEvent_RebalanceFromScratch;
    friend class TTxMonEvent_ObjectStats;
    friend class TTxMonEvent_StorageRebalance;
    friend class TTxMonEvent_Subactors;
    friend class TTxKillNode;
    friend class TTxLoadEverything;
    friend class TTxRestartTablet;
    friend class TTxLockTabletExecution;
    friend class TTxMonEvent_ReassignTablet;
    friend class TTxRegisterNode;
    friend class TTxSyncTablets;
    friend class TTxRequestTabletSequence;
    friend class TTxResponseTabletSequence;
    friend class TTxDisconnectNode;
    friend class TTxProcessPendingOperations;
    friend class TTxStopTablet;
    friend class TTxResumeTablet;
    friend class TTxUpdateTabletStatus;
    friend class TTxUpdateTabletMetrics;
    friend class TTxSeizeTablets;
    friend class TTxSeizeTabletsReply;
    friend class TTxReleaseTablets;
    friend class TTxReleaseTabletsReply;
    friend class TTxConfigureSubdomain;
    friend class TTxStatus;
    friend class TTxSwitchDrainOn;
    friend class TTxSwitchDrainOff;
    friend class TTxTabletOwnersReply;
    friend class TTxRequestTabletOwners;
    friend class TTxUpdateTabletsObject;
    friend class TTxUpdateTabletGroups;

    friend class TDeleteTabletActor;

    friend struct TStoragePoolInfo;

    bool IsItPossibleToStartBalancer(EBalancerType balancerType);
    void StartHiveBalancer(TBalancerSettings&& settings);
    void StartHiveDrain(TNodeId nodeId, TDrainSettings settings);
    void StartHiveFill(TNodeId nodeId, const TActorId& initiator);
    void StartHiveStorageBalancer(TStorageBalancerSettings settings);
    void CreateEvMonitoring(NMon::TEvRemoteHttpInfo::TPtr& ev, const TActorContext& ctx);
    NJson::TJsonValue GetBalancerProgressJson();
    ITransaction* CreateDeleteTablet(TEvHive::TEvDeleteTablet::TPtr& ev);
    ITransaction* CreateDeleteOwnerTablets(TEvHive::TEvDeleteOwnerTablets::TPtr& ev);
    ITransaction* CreateDeleteTabletResult(TEvTabletBase::TEvDeleteTabletResult::TPtr& ev);
    ITransaction* CreateCutTabletHistory(TEvHive::TEvCutTabletHistory::TPtr& ev);
    ITransaction* CreateBlockStorageResult(TEvTabletBase::TEvBlockBlobStorageResult::TPtr& ev);
    ITransaction* CreateRestartTablet(TFullTabletId tabletId);
    ITransaction* CreateRestartTablet(TFullTabletId tabletId, TNodeId preferredNodeId);
    ITransaction* CreateInitScheme();
    ITransaction* CreateAdoptTablet(NKikimrHive::TEvAdoptTablet &rec, const TActorId &sender, const ui64 cookie);
    ITransaction* CreateCreateTablet(NKikimrHive::TEvCreateTablet rec, const TActorId& sender, const ui64 cookie);
    ITransaction* CreateLoadEverything();
    ITransaction* CreateRegisterNode(const TActorId& local, NKikimrLocal::TEvRegisterNode rec);
    ITransaction* CreateStatus(const TActorId& local, NKikimrLocal::TEvStatus rec);
    ITransaction* CreateUpdateTabletStatus(TTabletId tabletId,
                                           const TActorId &local,
                                           ui32 generation,
                                           TFollowerId followerId,
                                           TEvLocal::TEvTabletStatus::EStatus status,
                                           TEvTablet::TEvTabletDead::EReason reason);
    ITransaction* CreateBootTablet(TTabletId tabletId);
    ITransaction* CreateKillNode(TNodeId nodeId, const TActorId& local);
    ITransaction* CreateUpdateTabletGroups(TTabletId tabletId, TVector<NKikimrBlobStorage::TEvControllerSelectGroupsResult::TGroupParameters> groups = {});
    ITransaction* CreateCheckTablets();
    ITransaction* CreateSyncTablets(const TActorId &local, NKikimrLocal::TEvSyncTablets& rec);
    ITransaction* CreateStopTablet(TTabletId tabletId, const TActorId& actorToNotify);
    ITransaction* CreateResumeTablet(TTabletId tabletId, const TActorId& actorToNotify);
    ITransaction* CreateStartTablet(TFullTabletId tabletId, const TActorId& local, ui64 cookie, bool external = false);
    ITransaction* CreateUpdateTabletMetrics(TEvHive::TEvTabletMetrics::TPtr& ev);
    ITransaction* CreateReassignGroups(TTabletId tabletId, const TActorId& actorToNotify, const std::bitset<MAX_TABLET_CHANNELS>& channelProfileNewGroup);
    ITransaction* CreateReassignGroupsOnDecommit(ui32 groupId, std::unique_ptr<IEventHandle> reply);
    ITransaction* CreateLockTabletExecution(const NKikimrHive::TEvLockTabletExecution& rec, const TActorId& sender, const ui64 cookie);
    ITransaction* CreateUnlockTabletExecution(const NKikimrHive::TEvUnlockTabletExecution& rec, const TActorId& sender, const ui64 cookie);
    ITransaction* CreateUnlockTabletExecution(ui64 tabletId, ui64 seqNo);
    ITransaction* CreateRequestTabletSequence(TEvHive::TEvRequestTabletIdSequence::TPtr event);
    ITransaction* CreateResponseTabletSequence(TEvHive::TEvResponseTabletIdSequence::TPtr event);
    ITransaction* CreateDisconnectNode(THolder<TEvInterconnect::TEvNodeDisconnected> event);
    ITransaction* CreateProcessPendingOperations();
    ITransaction* CreateProcessBootQueue();
    ITransaction* CreateSeizeTablets(TEvHive::TEvSeizeTablets::TPtr event);
    ITransaction* CreateSeizeTabletsReply(TEvHive::TEvSeizeTabletsReply::TPtr event);
    ITransaction* CreateReleaseTablets(TEvHive::TEvReleaseTablets::TPtr event);
    ITransaction* CreateReleaseTabletsReply(TEvHive::TEvReleaseTabletsReply::TPtr event);
    ITransaction* CreateConfigureSubdomain(TEvHive::TEvConfigureHive::TPtr event);
    ITransaction* CreateSwitchDrainOn(TNodeId nodeId, TDrainSettings settings, const TActorId& initiator);
    ITransaction* CreateSwitchDrainOff(TNodeId nodeId, TDrainSettings settings, NKikimrProto::EReplyStatus status, ui32 movements);
    ITransaction* CreateTabletOwnersReply(TEvHive::TEvTabletOwnersReply::TPtr event);
    ITransaction* CreateRequestTabletOwners(TEvHive::TEvRequestTabletOwners::TPtr event);
    ITransaction* CreateUpdateTabletsObject(TEvHive::TEvUpdateTabletsObject::TPtr event);
    ITransaction* CreateUpdateDomain(TSubDomainKey subdomainKey, TEvHive::TEvUpdateDomain::TPtr event = {});

public:
    TDomainsView DomainsView;

protected:
    TActorId BSControllerPipeClient;
    TActorId RootHivePipeClient;
    ui64 HiveUid; // Hive Personal Identifier - identifies a unique individual hive
    ui32 HiveDomain;
    TTabletId RootHiveId;
    TTabletId HiveId;
    ui64 HiveGeneration;
    TSubDomainKey RootDomainKey;
    TSubDomainKey PrimaryDomainKey;
    TString RootDomainName;
    TIntrusivePtr<NTabletPipe::TBoundedClientCacheConfig> PipeClientCacheConfig;
    THolder<NTabletPipe::IClientCache> PipeClientCache;
    TPipeTracker PipeTracker;
    NTabletPipe::TClientRetryPolicy PipeRetryPolicy;
    std::unordered_map<TNodeId, TNodeInfo> Nodes;
    std::unordered_map<TTabletId, TLeaderTabletInfo> Tablets;
    std::unordered_map<TOwnerIdxType::TValueType, TTabletId> OwnerToTablet;
    std::unordered_map<TTabletCategoryId, TTabletCategoryInfo> TabletCategories;
    std::unordered_map<TTabletTypes::EType, TVector<i64>> TabletTypeAllowedMetrics;
    std::unordered_map<TString, TStoragePoolInfo> StoragePools;
    std::unordered_map<TSubDomainKey, TDomainInfo> Domains;
    std::unordered_set<TOwnerId> BlockedOwners;
    ui32 ConfigurationGeneration = 0;
    ui64 TabletsTotal = 0;
    ui64 TabletsAlive = 0;
    ui32 DataCenters = 1;
    ui32 RegisteredDataCenters = 1;
    TObjectDistributions ObjectDistributions;
    double StorageScatter = 0;

    bool AreWeRootHive() const { return RootHiveId == HiveId; }
    bool AreWeSubDomainHive() const { return RootHiveId != HiveId; }

    struct TAggregateMetrics {
        NKikimrTabletBase::TMetrics Metrics;
        ui64 Counter = 0;

        void IncreaseCount(ui64 value = 1) {
            Counter += value;
        }

        void DecreaseCount() {
            Y_ABORT_UNLESS(Counter > 0);
            --Counter;
        }

        void AggregateDiff(const NKikimrTabletBase::TMetrics& before, const NKikimrTabletBase::TMetrics& after, const TTabletInfo* tablet) {
            AggregateMetricsDiff(Metrics, before, after, tablet);
        }

        NKikimrTabletBase::TMetrics GetAverage() const {
            NKikimrTabletBase::TMetrics metrics;
            if (Counter > 0) {
                metrics.CopyFrom(Metrics);
                DivideMetrics(metrics, Counter);
            }
            return metrics;
        }
    };

    std::unordered_map<TFullObjectId, TAggregateMetrics> ObjectToTabletMetrics;
    std::unordered_map<TTabletTypes::EType, TAggregateMetrics> TabletTypeToTabletMetrics;

    TBootQueue BootQueue;
    bool ProcessWaitQueueScheduled = false;
    bool ProcessBootQueueScheduled = false;
    bool ProcessBootQueuePostponed = false;
    TInstant LastConnect;
    TInstant ProcessBootQueuePostponedUntil;
    TDuration MaxTimeBetweenConnects;
    bool WarmUp;
    ui64 ExpectedNodes;

    THashMap<ui32, TEvInterconnect::TNodeInfo> NodesInfo;
    TTabletCountersBase* TabletCounters;
    TAutoPtr<TTabletCountersBase> TabletCountersPtr;
    std::unordered_set<TNodeId> BalancerNodes; // all nodes, affected by running balancers
    EBalancerType LastBalancerTrigger = EBalancerType::Manual;
    std::array<TBalancerStats, EBalancerTypeSize> BalancerStats;
    NKikimrHive::EMigrationState MigrationState = NKikimrHive::EMigrationState::MIGRATION_UNKNOWN;
    i32 MigrationProgress = 0;
    NKikimrHive::TEvSeizeTablets MigrationFilter;

    TActorId ResponsivenessActorID;
    TTabletResponsivenessPinger *ResponsivenessPinger;
    // remove after upgrade to sub hives
    ui64 NextTabletId = 0x10000;
    /////////////////////////////////////
    bool RequestingSequenceNow = false;
    size_t RequestingSequenceIndex = 0;
    bool ProcessTabletBalancerScheduled = false;
    bool ProcessTabletBalancerPostponed = false;
    bool ProcessPendingOperationsScheduled = false;
    bool LogTabletMovesScheduled = false;
    bool ProcessStorageBalancerScheduled = false;
    TResourceRawValues TotalRawResourceValues = {};
    TResourceNormalizedValues TotalNormalizedResourceValues = {};
    TInstant LastResourceChangeReaction;
    //TDuration ResourceChangeReactionPeriod = TDuration::Seconds(10);
    TVector<ISubActor*> SubActors;
    TResourceProfilesPtr ResourceProfiles;
    NKikimrLocal::TLocalConfig LocalConfig;
    bool ReadyForConnections = false; // is Hive ready for incoming connections?
    ui64 NextTabletUnlockSeqNo = 1; // sequence number for unlock events
    bool SpreadNeighbours = true; // spread tablets of the same object across cluster
    TSequenceGenerator Sequencer;
    TOwnershipKeeper Keeper;
    TEventPriorityQueue<THive> EventQueue{*this};
    std::vector<TActorId> ActorsWaitingToMoveTablets;

    struct TPendingCreateTablet {
        NKikimrHive::TEvCreateTablet CreateTablet;
        TActorId Sender;
        ui64 Cookie;
    };

    std::unordered_map<std::pair<ui64, ui64>, TPendingCreateTablet> PendingCreateTablets;
    std::deque<THolder<IEventHandle>> PendingOperations;

    ui64 UpdateTabletMetricsInProgress = 0;
    static constexpr ui64 MAX_UPDATE_TABLET_METRICS_IN_PROGRESS = 10000; // 10K

    TString BootStateBooting = "Booting";
    TString BootStateStarting = "Starting";
    TString BootStateRunning = "Running";
    TString BootStateTooManyStarting = "Too many tablets starting";
    TString BootStateLeaderNotRunning = "Leader not running";
    TString BootStateAllNodesAreDead = "All nodes are dead";
    TString BootStateAllNodesAreDeadOrDown = "All nodes are dead or down";
    TString BootStateNoNodesAllowedToRun = "No nodes allowed to run";
    TString BootStateNotEnoughDatacenters = "Not enough datacenters";
    TString BootStateNotEnoughResources = "Not enough resources";
    TString BootStateNodesLocationUnknown = "Nodes location unknown";
    TString BootStateWeFilledAllAvailableNodes = "All available nodes are already filled with someone from our family";

    NKikimrConfig::THiveConfig ClusterConfig;
    NKikimrConfig::THiveConfig DatabaseConfig;
    std::unordered_map<TTabletTypes::EType, NKikimrConfig::THiveTabletLimit> TabletLimit; // built from CurrentConfig
    std::unordered_map<TTabletTypes::EType, NKikimrHive::TDataCentersPreference> DefaultDataCentersPreference;
    std::unordered_map<TDataCenterId, std::unordered_set<TNodeId>> RegisteredDataCenterNodes;
    std::unordered_set<TNodeId> ConnectedNodes;

    // normalized to be sorted list of unique values
    std::vector<TTabletTypes::EType> BalancerIgnoreTabletTypes; // built from CurrentConfig

    struct TTabletMoveInfo {
        TInstant Timestamp;
        TFullTabletId Tablet;
        TNodeId From;
        TNodeId To;
        double Priority;
        TTabletTypes::EType TabletType;


        TTabletMoveInfo(TInstant timestamp, const TTabletInfo& tablet, TNodeId from, TNodeId to);

        TString ToHTML() const;

        std::weak_ordering operator<=>(const TTabletMoveInfo& other) const;
    };

    TStaticRingBuffer<TTabletMoveInfo, 5> TabletMoveHistory;
    std::vector<TTabletMoveInfo> TabletMoveSamplesForLog; // stores (at most) MOVE_SAMPLES_PER_LOG_ENTRY highest priority moves in a heap
    static constexpr size_t MOVE_SAMPLES_PER_LOG_ENTRY = 10;
    std::unordered_map<TTabletTypes::EType, ui64> TabletMovesByTypeForLog;
    TInstant LogTabletMovesSchedulingTime;


    // to be removed later
    bool TabletOwnersSynced = false;
    // to be removed later

    void OnActivateExecutor(const TActorContext& ctx) override;
    void DefaultSignalTabletActive(const TActorContext& ctx) override;
    void OnDetach(const TActorContext&) override;
    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext&) override;
    bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext&) override;

    bool ReassignChannelsEnabled() const override {
        return true;
    }

    void BuildLocalConfig();
    void BuildCurrentConfig();
    void Cleanup();

    void Handle(TEvHive::TEvCreateTablet::TPtr&);
    void Handle(TEvHive::TEvAdoptTablet::TPtr&);
    void Handle(TEvHive::TEvStopTablet::TPtr&);
    void Handle(TEvHive::TEvBootTablet::TPtr&);
    void Handle(TEvLocal::TEvStatus::TPtr&);
    void Handle(TEvLocal::TEvTabletStatus::TPtr&);
    void Handle(TEvLocal::TEvRegisterNode::TPtr&);
    void Handle(TEvBlobStorage::TEvControllerSelectGroupsResult::TPtr&);
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr&);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&);
    void Handle(TEvTabletPipe::TEvServerConnected::TPtr&);
    void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr&);
    void Handle(TEvInterconnect::TEvNodeConnected::TPtr&);
    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr&);
    void Handle(TEvInterconnect::TEvNodeInfo::TPtr&);
    void Handle(TEvInterconnect::TEvNodesInfo::TPtr&);
    void Handle(TEvents::TEvUndelivered::TPtr&);
    void Handle(TEvHive::TEvTabletMetrics::TPtr&);
    void Handle(TEvHive::TEvReassignTablet::TPtr&);
    void Handle(TEvHive::TEvInitiateBlockStorage::TPtr&);
    void Handle(TEvLocal::TEvSyncTablets::TPtr&);
    void Handle(TEvTabletBase::TEvBlockBlobStorageResult::TPtr&);
    void Handle(TEvTabletBase::TEvDeleteTabletResult::TPtr&);
    void Handle(TEvHive::TEvDeleteTablet::TPtr&);
    void Handle(TEvHive::TEvDeleteOwnerTablets::TPtr&);
    void Handle(TEvHive::TEvRequestHiveInfo::TPtr&);
    void Handle(TEvHive::TEvLookupTablet::TPtr&);
    void Handle(TEvHive::TEvLookupChannelInfo::TPtr&);
    void Handle(TEvHive::TEvCutTabletHistory::TPtr&);
    void Handle(TEvHive::TEvDrainNode::TPtr&);
    void Handle(TEvHive::TEvFillNode::TPtr&);
    void Handle(TEvHive::TEvInitiateDeleteStorage::TPtr&);
    void Handle(TEvHive::TEvGetTabletStorageInfo::TPtr&);
    void Handle(TEvHive::TEvLockTabletExecution::TPtr&);
    void Handle(TEvHive::TEvUnlockTabletExecution::TPtr&);
    void Handle(TEvHive::TEvInitiateTabletExternalBoot::TPtr&);
    void Handle(TEvHive::TEvRequestHiveDomainStats::TPtr&);
    void Handle(TEvHive::TEvRequestHiveNodeStats::TPtr&);
    void Handle(TEvHive::TEvRequestHiveStorageStats::TPtr&);
    void Handle(TEvHive::TEvInvalidateStoragePools::TPtr&);
    void Handle(TEvHive::TEvReassignOnDecommitGroup::TPtr&);
    void Handle(TEvHive::TEvRequestTabletIdSequence::TPtr&);
    void Handle(TEvHive::TEvResponseTabletIdSequence::TPtr&);
    void Handle(TEvHive::TEvSeizeTablets::TPtr&);
    void Handle(TEvHive::TEvSeizeTabletsReply::TPtr&);
    void Handle(TEvHive::TEvReleaseTablets::TPtr&);
    void Handle(TEvHive::TEvReleaseTabletsReply::TPtr&);
    void Handle(TEvSubDomain::TEvConfigure::TPtr&);
    void Handle(TEvHive::TEvConfigureHive::TPtr& ev);
    void Handle(TEvHive::TEvInitMigration::TPtr&);
    void Handle(TEvHive::TEvQueryMigration::TPtr&);
    void Handle(TEvPrivate::TEvKickTablet::TPtr&);
    void Handle(TEvPrivate::TEvBootTablets::TPtr&);
    void Handle(TEvPrivate::TEvCheckTabletNodeAlive::TPtr&);
    void HandleInit(TEvPrivate::TEvProcessBootQueue::TPtr&);
    void Handle(TEvPrivate::TEvProcessBootQueue::TPtr&);
    void Handle(TEvPrivate::TEvPostponeProcessBootQueue::TPtr&);
    void Handle(TEvPrivate::TEvProcessDisconnectNode::TPtr&);
    void HandleInit(TEvPrivate::TEvProcessTabletBalancer::TPtr&);
    void Handle(TEvPrivate::TEvProcessTabletBalancer::TPtr&);
    void Handle(TEvPrivate::TEvUnlockTabletReconnectTimeout::TPtr&);
    void Handle(TEvPrivate::TEvProcessPendingOperations::TPtr&);
    void Handle(TEvPrivate::TEvBalancerOut::TPtr&);
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
    void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev);
    void Handle(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr& ev);
    void Handle(NSysView::TEvSysView::TEvGetTabletIdsRequest::TPtr& ev);
    void Handle(NSysView::TEvSysView::TEvGetTabletsRequest::TPtr& ev);
    void Handle(TEvHive::TEvRequestTabletOwners::TPtr& ev);
    void Handle(TEvHive::TEvTabletOwnersReply::TPtr& ev);
    void Handle(TEvHive::TEvUpdateTabletsObject::TPtr& ev);
    void Handle(TEvPrivate::TEvRefreshStorageInfo::TPtr& ev);
    void Handle(TEvPrivate::TEvLogTabletMoves::TPtr& ev);
    void Handle(TEvPrivate::TEvStartStorageBalancer::TPtr& ev);
    void Handle(TEvPrivate::TEvProcessStorageBalancer::TPtr& ev);
    void Handle(TEvPrivate::TEvProcessIncomingEvent::TPtr& ev);
    void Handle(TEvHive::TEvUpdateDomain::TPtr& ev);
    void Handle(TEvPrivate::TEvDeleteNode::TPtr& ev);

protected:
    void RestartPipeTx(ui64 tabletId);
    bool TryToDeleteNode(TNodeInfo* node);

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HIVE_ACTOR;
    }

    THive(TTabletStorageInfo *info, const TActorId &tablet);

protected:
    STATEFN(StateInit);
    STATEFN(StateWork);

    void SendToBSControllerPipe(IEventBase* payload);
    void SendToRootHivePipe(IEventBase* payload);
    void RestartBSControllerPipe();
    void RestartRootHivePipe();

    struct TNoNodeFound {};
    struct TTooManyTabletsStarting {};
    using TBestNodeResult = std::variant<TNodeInfo*, TNoNodeFound, TTooManyTabletsStarting>;

    TBestNodeResult FindBestNode(const TTabletInfo& tablet);

    struct TSelectedNode {
        double Usage;
        TNodeInfo* Node;

        TSelectedNode(double usage, TNodeInfo* node)
            : Usage(usage)
            , Node(node)
        {}

        bool operator <(const TSelectedNode& b) const {
            return Usage < b.Usage;
        }
    };

    template <NKikimrConfig::THiveConfig::EHiveNodeSelectStrategy Strategy>
    TNodeInfo* SelectNode(const std::vector<TSelectedNode>& selectedNodes);
    TVector<TSelectedNode> SelectMaxPriorityNodes(TVector<TSelectedNode> selectedNodes, const TTabletInfo& tablet) const;

public:
    void AssignTabletGroups(TLeaderTabletInfo& tablet);
    TNodeInfo& GetNode(TNodeId nodeId);
    TNodeInfo* FindNode(TNodeId nodeId);
    TLeaderTabletInfo& GetTablet(TTabletId tabletId);
    TLeaderTabletInfo* FindTablet(TTabletId tabletId);
    TLeaderTabletInfo* FindTabletEvenInDeleting(TTabletId tabletId); // find tablets, even deleting ones
    TTabletInfo& GetTablet(TTabletId tabletId, TFollowerId followerId);
    TTabletInfo* FindTablet(TTabletId tabletId, TFollowerId followerId);
    TTabletInfo* FindTablet(const TFullTabletId& tabletId) { return FindTablet(tabletId.first, tabletId.second); }
TTabletInfo* FindTabletEvenInDeleting(TTabletId tabletId, TFollowerId followerId);
    TStoragePoolInfo& GetStoragePool(const TString& name);
    TStoragePoolInfo* FindStoragePool(const TString& name);
    TDomainInfo* FindDomain(TSubDomainKey key);
    const TDomainInfo* FindDomain(TSubDomainKey key) const;
    const TNodeLocation& GetNodeLocation(TNodeId nodeId) const;
    void DeleteTablet(TTabletId tabletId);
    void DeleteNode(TNodeId nodeId);
    TVector<TNodeId> GetNodesForWhiteboardBroadcast(size_t maxNodesToReturn = 3);
    void ReportTabletStateToWhiteboard(const TLeaderTabletInfo& tablet, NKikimrWhiteboard::TTabletStateInfo::ETabletState state);
    void ReportStoppedToWhiteboard(const TLeaderTabletInfo& tablet);
    void ReportDeletedToWhiteboard(const TLeaderTabletInfo& tablet);
    TTabletCategoryInfo& GetTabletCategory(TTabletCategoryId tabletCategoryId);
    void KillNode(TNodeId nodeId, const TActorId& local);
    void AddToBootQueue(TTabletInfo* tablet);
    void UpdateDomainTabletsTotal(const TSubDomainKey& objectDomain, i64 tabletsTotalDiff);
    void UpdateDomainTabletsAlive(const TSubDomainKey& objectDomain, i64 tabletsAliveDiff, const TSubDomainKey& tabletNodeDomain);
    void SetCounterTabletsTotal(ui64 tabletsTotal);
    void UpdateCounterTabletsTotal(i64 tabletsTotalDiff);
    void UpdateCounterTabletsAlive(i64 tabletsAliveDiff);
    void UpdateCounterBootQueueSize(ui64 bootQueueSize);
    void UpdateCounterEventQueueSize(i64 eventQueueSizeDiff);
    void UpdateCounterNodesConnected(i64 nodesConnectedDiff);
    void RecordTabletMove(const TTabletMoveInfo& info);
    bool DomainHasNodes(const TSubDomainKey &domainKey) const;
    void ProcessBootQueue();
    void ProcessWaitQueue();
    void PostponeProcessBootQueue(TDuration after);
    void ProcessPendingOperations();
    void ProcessTabletBalancer();
    void ProcessStorageBalancer();
    const TVector<i64>& GetTabletTypeAllowedMetricIds(TTabletTypes::EType type) const;
    static const TVector<i64>& GetDefaultAllowedMetricIdsForType(TTabletTypes::EType type);
    static bool IsValidMetrics(const NKikimrTabletBase::TMetrics& metrics);
    static bool IsValidMetricsCPU(const NKikimrTabletBase::TMetrics& metrics);
    static bool IsValidMetricsMemory(const NKikimrTabletBase::TMetrics& metrics);
    static bool IsValidMetricsNetwork(const NKikimrTabletBase::TMetrics& metrics);
    void UpdateTotalResourceValues(
            const TNodeInfo* node,
            const TTabletInfo* tablet,
            const NKikimrTabletBase::TMetrics& before,
            const NKikimrTabletBase::TMetrics& after,
            NKikimr::NHive::TResourceRawValues deltaRaw,
            NKikimr::NHive::TResourceNormalizedValues deltaNormalized);
    void FillTabletInfo(NKikimrHive::TEvResponseHiveInfo& response, ui64 tabletId, const TLeaderTabletInfo* info, const NKikimrHive::TEvRequestHiveInfo& req);
    void ExecuteStartTablet(TFullTabletId tabletId, const TActorId& local, ui64 cookie, bool external);
    ui32 GetDataCenters();
    ui32 GetRegisteredDataCenters();
    void UpdateRegisteredDataCenters();
    void AddRegisteredDataCentersNode(TDataCenterId dataCenterId, TNodeId nodeId);
    void RemoveRegisteredDataCentersNode(TDataCenterId dataCenterId, TNodeId nodeId);
    void SendPing(const TActorId& local, TNodeId id);
    void SendReconnect(const TActorId& local);
    static THolder<TGroupFilter> BuildGroupParametersForChannel(const TLeaderTabletInfo& tablet, ui32 channelId);
    void KickTablet(const TTabletInfo& tablet);
    void StopTablet(const TActorId& local, const TTabletInfo& tablet);
    void StopTablet(const TActorId& local, TFullTabletId tabletId);
    void ExecuteProcessBootQueue(NIceDb::TNiceDb& db, TSideEffects& sideEffects);
    void UpdateTabletFollowersNumber(TLeaderTabletInfo& tablet, NIceDb::TNiceDb& db, TSideEffects& sideEffects);
    TDuration GetBalancerCooldown(EBalancerType balancerType) const;
    void UpdateObjectCount(const TLeaderTabletInfo& tablet, const TNodeInfo& node, i64 diff);
    ui64 GetObjectImbalance(TFullObjectId object);

    ui32 GetEventPriority(IEventHandle* ev);
    void PushProcessIncomingEvent();
    void ProcessEvent(std::unique_ptr<NActors::IEventHandle> event);

    TTabletMetricsAggregates DefaultResourceMetricsAggregates;
    ui64 MetricsWindowSize = TDuration::Minutes(1).MilliSeconds();
    const TTabletMetricsAggregates& GetDefaultResourceMetricsAggregates() const;
    bool CheckForForwardTabletRequest(TTabletId tabletId, NKikimrHive::TForwardRequest& forwardRequest);
    TSubDomainKey GetRootDomainKey() const;
    TString GetLogPrefix() const;

    double GetResourceOvercommitment() const {
        return CurrentConfig.GetResourceOvercommitment();
    }

    TDuration GetTabletKickCooldownPeriod() const {
        return TDuration::Seconds(CurrentConfig.GetTabletKickCooldownPeriod());
    }

    TDuration GetResourceChangeReactionPeriod() const {
        return TDuration::Seconds(CurrentConfig.GetResourceChangeReactionPeriod());
    }

    TDuration GetMinPeriodBetweenBalance() const {
        return TDuration::Seconds(CurrentConfig.GetMinPeriodBetweenBalance());
    }

    TDuration GetMinPeriodBetweenEmergencyBalance() const {
        return TDuration::Seconds(CurrentConfig.GetMinPeriodBetweenEmergencyBalance());
    }

    TDuration GetMinPeriodBetweenReassign() const {
        return TDuration::Seconds(CurrentConfig.GetMinPeriodBetweenReassign());
    }

    TDuration GetTabletRestartWatchPeriod() const {
        return TDuration::Seconds(CurrentConfig.GetTabletRestartWatchPeriod());
    }

    TDuration GetNodeRestartWatchPeriod() const {
        return TDuration::Seconds(CurrentConfig.GetNodeRestartWatchPeriod());
    }

    TDuration GetNodeDeletePeriod() const {
        return TDuration::Seconds(CurrentConfig.GetNodeDeletePeriod());
    }

    ui64 GetDrainInflight() const {
        return CurrentConfig.GetDrainInflight();
    }

    ui64 GetBalancerInflight() const {
        return CurrentConfig.GetBalancerInflight();
    }

    ui64 GetEmergencyBalancerInflight() const {
        return CurrentConfig.GetEmergencyBalancerInflight();
    }

    ui64 GetMaxBootBatchSize() const {
        return CurrentConfig.GetMaxBootBatchSize();
    }

    TResourceNormalizedValues GetMinScatterToBalance() const {
        TResourceNormalizedValues minScatter;
        std::get<NMetrics::EResource::CPU>(minScatter) = CurrentConfig.GetMinCPUScatterToBalance();
        std::get<NMetrics::EResource::Memory>(minScatter) = CurrentConfig.GetMinMemoryScatterToBalance();
        std::get<NMetrics::EResource::Network>(minScatter) = CurrentConfig.GetMinNetworkScatterToBalance();
        std::get<NMetrics::EResource::Counter>(minScatter) = CurrentConfig.GetMinCounterScatterToBalance();

        if (CurrentConfig.HasMinScatterToBalance()) {
            if (!CurrentConfig.HasMinCPUScatterToBalance()) {
                std::get<NMetrics::EResource::CPU>(minScatter) = CurrentConfig.GetMinScatterToBalance();
            }
            if (!CurrentConfig.HasMinNetworkScatterToBalance()) {
                std::get<NMetrics::EResource::Network>(minScatter) = CurrentConfig.GetMinScatterToBalance();
            }
            if (!CurrentConfig.HasMinMemoryScatterToBalance()) {
                std::get<NMetrics::EResource::Memory>(minScatter) = CurrentConfig.GetMinScatterToBalance();
            }
        }

        return minScatter;
    }

    double GetMaxNodeUsageToKick() const {
        return CurrentConfig.GetMaxNodeUsageToKick();
    }

    TResourceNormalizedValues GetMinNodeUsageToBalance() const {
        // MinNodeUsageToBalance is needed so that small fluctuations in metrics do not cause scatter
        // when cluster load is low. Counter does not fluctuate, so it does not need it.
        double minUsageToBalance = CurrentConfig.GetMinNodeUsageToBalance();
        TResourceNormalizedValues minValuesToBalance;
        std::get<NMetrics::EResource::CPU>(minValuesToBalance) = minUsageToBalance;
        std::get<NMetrics::EResource::Memory>(minValuesToBalance) = minUsageToBalance;
        std::get<NMetrics::EResource::Network>(minValuesToBalance) = minUsageToBalance;
        std::get<NMetrics::EResource::Counter>(minValuesToBalance) = 0;
        return minValuesToBalance;
    }

    ui64 GetMaxTabletsScheduled() const {
        return CurrentConfig.GetMaxTabletsScheduled();
    }

    bool GetSpreadNeighbours() const {
        return SpreadNeighbours;
    }

    ui64 GetDefaultUnitIOPS() const {
        return CurrentConfig.GetDefaultUnitIOPS();
    }

    ui64 GetDefaultUnitSize() const {
        return CurrentConfig.GetDefaultUnitSize();
    }

    ui64 GetDefaultUnitThroughput() const {
        return CurrentConfig.GetDefaultUnitThroughput();
    }

    ui64 GetRequestSequenceSize() const {
        return CurrentConfig.GetRequestSequenceSize();
    }

    ui64 GetMetricsWindowSize() const {
        return CurrentConfig.GetMetricsWindowSize();
    }

    ui64 GetMinRequestSequenceSize() const {
        return CurrentConfig.GetMinRequestSequenceSize();
    }

    ui64 GetMaxRequestSequenceSize() const {
        return CurrentConfig.GetMaxRequestSequenceSize();
    }

    bool GetEnableFastTabletMove() const {
        return CurrentConfig.GetEnableFastTabletMove();
    }

    TDuration GetTabletRestartsPeriod() const {
        return TDuration::MilliSeconds(CurrentConfig.GetTabletRestartsPeriod());
    }

    ui64 GetTabletRestartsMaxCount() const {
        if (CurrentConfig.HasTabletRestarsMaxCount() && !CurrentConfig.HasTabletRestartsMaxCount()) {
            return CurrentConfig.GetTabletRestarsMaxCount();
        }
        return CurrentConfig.GetTabletRestartsMaxCount();
    }

    TDuration GetPostponeStartPeriod() const {
        return TDuration::MilliSeconds(CurrentConfig.GetPostponeStartPeriod());
    }

    bool GetCheckMoveExpediency() const {
        return CurrentConfig.GetCheckMoveExpediency();
    }

    ui64 GetObjectImbalanceToBalance() {
        if (GetSpreadNeighbours()) {
            return CurrentConfig.GetObjectImbalanceToBalance();
        } else {
            return std::numeric_limits<ui64>::max();
        }
    }

    const std::unordered_map<TTabletTypes::EType, NKikimrConfig::THiveTabletLimit>& GetTabletLimit() const {
        return TabletLimit;
    }

    TArrayRef<const NKikimrHive::TDataCentersGroup*> GetDefaultDataCentersPreference(TTabletTypes::EType type) {
        auto itDCsPreference = DefaultDataCentersPreference.find(type);
        if (itDCsPreference != DefaultDataCentersPreference.end()) {
            return TArrayRef<const NKikimrHive::TDataCentersGroup*>(
                        const_cast<const NKikimrHive::TDataCentersGroup**>(itDCsPreference->second.GetDataCentersGroups().data()),
                        itDCsPreference->second.GetDataCentersGroups().size());
        }
        return {};
    }

    TResourceRawValues GetResourceInitialMaximumValues() const {
        TResourceRawValues initialMaximum;
        std::get<NMetrics::EResource::CPU>(initialMaximum) = CurrentConfig.GetMaxResourceCPU();
        std::get<NMetrics::EResource::Memory>(initialMaximum) = CurrentConfig.GetMaxResourceMemory();
        std::get<NMetrics::EResource::Network>(initialMaximum) = CurrentConfig.GetMaxResourceNetwork();
        std::get<NMetrics::EResource::Counter>(initialMaximum) = CurrentConfig.GetMaxResourceCounter();
        return initialMaximum;
    }

    bool IsInBalancerIgnoreList(TTabletTypes::EType type) const {
        const auto& ignoreList = BalancerIgnoreTabletTypes;
        auto found = std::find(ignoreList.begin(), ignoreList.end(), type);
        return (found != ignoreList.end());
    }

    double GetSpaceUsagePenaltyThreshold() {
        return CurrentConfig.GetSpaceUsagePenaltyThreshold();
    }

    double GetSpaceUsagePenalty() {
        return CurrentConfig.GetSpaceUsagePenalty();
    }

    TDuration GetWarmUpBootWaitingPeriod() const {
        return TDuration::MilliSeconds(CurrentConfig.GetWarmUpBootWaitingPeriod());
    }

    TDuration GetMaxWarmUpPeriod() const {
        return TDuration::Seconds(CurrentConfig.GetMaxWarmUpPeriod());
    }

    ui64 GetNodeRestartsToIgnoreInWarmup() const {
        return CurrentConfig.GetNodeRestartsToIgnoreInWarmup();
    }

    NKikimrConfig::THiveConfig::EHiveBootStrategy GetBootStrategy() const {
        return CurrentConfig.GetBootStrategy();
    }

    NKikimrConfig::THiveConfig::EHiveChannelBalanceStrategy GetChannelBalanceStrategy() const {
        return CurrentConfig.GetChannelBalanceStrategy();
    }

    ui64 GetMaxChannelHistorySize() const {
        return CurrentConfig.GetMaxChannelHistorySize();
    }

    TDuration GetStorageInfoRefreshFrequency() const {
        return TDuration::MilliSeconds(CurrentConfig.GetStorageInfoRefreshFrequency());
    }

    double GetMinStorageScatterToBalance() const {
        return CurrentConfig.GetMinStorageScatterToBalance();
    }

    ui64 GetStorageBalancerInflight() const {
        return CurrentConfig.GetStorageBalancerInflight();
    }

    static void ActualizeRestartStatistics(google::protobuf::RepeatedField<google::protobuf::uint64>& restartTimestamps, ui64 barrier);
    static ui64 GetRestartsPerPeriod(const google::protobuf::RepeatedField<google::protobuf::uint64>& restartTimestamps, ui64 barrier);
    static bool IsSystemTablet(TTabletTypes::EType type);

protected:
    void ScheduleDisconnectNode(THolder<TEvPrivate::TEvProcessDisconnectNode> event);
    void DeleteTabletWithoutStorage(TLeaderTabletInfo* tablet);
    void DeleteTabletWithoutStorage(TLeaderTabletInfo* tablet, TSideEffects& sideEffects);
    TInstant GetAllowedBootingTime();
    void ScheduleUnlockTabletExecution(TNodeInfo& node);
    TString DebugDomainsActiveNodes() const;
    TResourceNormalizedValues GetStDevResourceValues() const;
    bool IsTabletMoveExpedient(const TTabletInfo& tablet, const TNodeInfo& node) const;
    TResourceRawValues GetDefaultResourceInitialMaximumValues();
    double GetScatter() const;
    double GetUsage() const;
    // If the scatter is considered okay, returns nullopt. Otherwise, returns the resource that should be better balanced.
    std::optional<EResourceToBalance> CheckScatter(const TResourceNormalizedValues& scatterByResource) const;

    struct THiveStats {
        struct TNodeStat {
            TNodeId NodeId;
            double Usage;
            TResourceNormalizedValues ResourceNormValues;

            TNodeStat(TNodeId node, double usage, TResourceNormalizedValues values)
                : NodeId(node)
                , Usage(usage)
                , ResourceNormValues(values)
            {
            }
        };

        double MinUsage;
        TNodeId MinUsageNodeId;
        double MaxUsage;
        TNodeId MaxUsageNodeId;
        double Scatter;
        TResourceNormalizedValues ScatterByResource;
        std::vector<TNodeStat> Values;
    };

    THiveStats GetStats() const;
    void RemoveSubActor(ISubActor* subActor);
    bool StopSubActor(TSubActorId subActorId);
    void WaitToMoveTablets(TActorId actor);
    const NKikimrLocal::TLocalConfig &GetLocalConfig() const { return LocalConfig; }
    NKikimrTabletBase::TMetrics GetDefaultResourceValuesForObject(TFullObjectId objectId);
    NKikimrTabletBase::TMetrics GetDefaultResourceValuesForTabletType(TTabletTypes::EType type);
    NKikimrTabletBase::TMetrics GetDefaultResourceValuesForProfile(TTabletTypes::EType type, const TString& resourceProfile);
    static void AggregateMetricsMax(NKikimrTabletBase::TMetrics& aggregate, const NKikimrTabletBase::TMetrics& value);
    static void AggregateMetricsDiff(NKikimrTabletBase::TMetrics& aggregate,
            const NKikimrTabletBase::TMetrics& before,
            const NKikimrTabletBase::TMetrics& after,
            const TTabletInfo* tablet);
    static void DivideMetrics(NKikimrTabletBase::TMetrics& metrics, ui64 divider);
    TVector<TTabletId> UpdateStoragePools(const google::protobuf::RepeatedPtrField<NKikimrBlobStorage::TEvControllerSelectGroupsResult::TGroupParameters>& groups);
    void InitDefaultChannelBind(TChannelBind& bind);
    void RequestPoolsInformation();
    void RequestFreeSequence();
    void EnqueueIncomingEvent(STATEFN_SIG);

    bool SeenDomain(TSubDomainKey domain);
    void ResolveDomain(TSubDomainKey domain);
    TString GetDomainName(TSubDomainKey domain);
    TSubDomainKey GetMySubDomainKey() const;
};

} // NHive
} // NKikimr

inline IOutputStream& operator <<(IOutputStream& o, NKikimr::NHive::ETabletState s) {
    return o << "ETabletState(" << static_cast<ui64>(s) << ")";
}

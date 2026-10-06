#include "kqp_rm_service.h"

#include <ydb/core/kqp/compile_service/kqp_warmup_compile_actor.h>
#include <ydb/core/base/location.h>
#include <ydb/core/base/localdb.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/mind/tenant_pool.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/tablet/resource_broker.h>


#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <yql/essentials/utils/yql_panic.h>

#include <algorithm>
#include <cmath>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KQP_RESOURCE_MANAGER

namespace NKikimr {
namespace NKqp {
namespace NRm {

using namespace NActors;
using namespace NResourceBroker;

TTxState::TTxState(std::shared_ptr<IKqpResourceManager>& resourceManager, ui64 txId, TInstant now, const TString& poolId,
    const TString& database, bool collectBacktrace)
    : ResourceManager(resourceManager)
    , Counters(resourceManager->GetCounters())
    , TxId(txId)
    , CreatedAt(now)
    , PoolId(poolId)
    , Database(database)
    , CollectBacktrace(collectBacktrace)
{}

TTxState::~TTxState() {
    ResourceManager->FinishTx(*this);
    delete TxMaxAllocationBacktrace.load();
}

namespace {

static constexpr double MYEPS = 1e-9;

// Percents come from the config unchecked: anything outside [0, 100] would turn into a negative double and wrap on
// the conversion to ui64, so they are clamped here, the one place they are applied. Above 100 behaves as 100
// (the spilling threshold at the limit itself), below 0 as 0.
double ClampPercent(double percent) {
    return std::clamp(percent, 0.0, 100.0);
}

ui64 OverPercentage(ui64 limit, double percent) {
    return static_cast<double>(limit) / 100 * (100 - ClampPercent(percent)) + MYEPS;
}

// The memory of the node services other than the queries (see TKqpResourcesRequest).
// TODO: move these services under the root of the compute scheduler. For now their limit is the one of the resource
//       broker queue of the queries, which is the same query execution limit the compute scheduler gets from the
//       memory controller - so this memory is counted twice.
class TMemoryResource {
public:
    TMemoryResource(ui64 limit, double overPercent)
        : Limit(limit)
        , OverPercent(overPercent)
    {
        SetActualLimits();
    }

    ui64 Available() const {
        return Limit > Used ? Limit - Used : 0;
    }

    // Bytes left before the spilling threshold, negative when the threshold is exceeded.
    i64 GetMemoryAvailability() const {
        return static_cast<i64>(Limit) - static_cast<i64>(Used) - static_cast<i64>(OverLimit);
    }

    bool Has(ui64 amount) const {
        return Available() >= amount;
    }

    // Whether a request of `value` may be charged: within the limit and, for an optional one, also within the
    // spilling threshold. Has() goes first, a huge value must not reach the signed check.
    bool Admits(ui64 value, bool optional) const {
        return Has(value) && (!optional || GetMemoryAvailability() >= static_cast<i64>(value));
    }

    void Acquire(ui64 value) {
        Used += value;
    }

    void Release(ui64 value) {
        Used = Used > value ? Used - value : 0;
    }

    void SetLimit(ui64 limit) {
        Limit = limit;
        SetActualLimits();
    }

    void SetOverPercent(double overPercent) {
        OverPercent = overPercent;
        SetActualLimits();
    }

    ui64 GetLimit() const {
        return Limit;
    }

    TString ToString() const {
        return TStringBuilder() << Used << '/' << Limit;
    }

private:
    void SetActualLimits() {
        OverLimit = OverPercentage(Limit, OverPercent);
    }

private:
    ui64 Limit;
    ui64 OverLimit = 0;
    ui64 Used = 0;
    double OverPercent;
};

struct TEvPrivate {
    enum EEv {
        EvPublishResources = EventSpaceBegin(TEvents::ES_PRIVATE),
        EvSchedulePublishResources,
        EvTakeResourcesSnapshot,
        EvWarmupDeadline,
    };

    struct TEvPublishResources : public TEventLocal<TEvPublishResources, EEv::EvPublishResources> {
    };

    struct TEvSchedulePublishResources : public TEventLocal<TEvSchedulePublishResources, EEv::EvSchedulePublishResources> {
    };

    struct TEvWarmupDeadline : public TEventLocal<TEvWarmupDeadline, EEv::EvWarmupDeadline> {
    };
};

class TKqpResourceManager : public IKqpResourceManager {
public:

    TKqpResourceManager(const NKikimrConfig::TTableServiceConfig::TResourceManager& config, TIntrusivePtr<TKqpCounters> counters)
        : Counters(counters)
        , ExecutionUnitsResource(config.GetComputeActorsCount())
        , ExecutionUnitsLimit(config.GetComputeActorsCount())
        , TotalMemoryResource(config.GetQueryMemoryLimit(), config.GetSpillingPercent())
        // Until the compute scheduler pushes the state of the query memory, it's the initial one of the scheduler.
        , QueryMemoryLimit(config.GetQueryMemoryLimit())
        , ResourceSnapshotState(std::make_shared<TResourceSnapshotState>())
    {
        PublishAfterBootstrap.clear();
        SetConfigValues(config);
    }

    void Registered(NKikimrConfig::TTableServiceConfig::TResourceManager& config, TActorSystem* actorSystem, TActorId selfId) {
        ActorSystem = actorSystem;
        SelfId = selfId;
        if (!Counters) {
            Counters = MakeIntrusive<TKqpCounters>(AppData(ActorSystem)->Counters);
        }
        UpdatePatternCache(config.GetKqpPatternCacheCapacityBytes(),
            config.GetKqpPatternCacheCompiledCapacityBytes(),
            config.GetKqpPatternCachePatternAccessTimesBeforeTryToCompile());

        CreateResourceInfoExchanger(config.GetInfoExchangerSettings());

        if (PublishAfterBootstrap.test()) {
            FireResourcesPublishing();
            PublishAfterBootstrap.clear();
        }
    }

    const TIntrusivePtr<TKqpCounters>& GetCounters() const override {
        return Counters;
    }

    TPlannerPlacingOptions GetPlacingOptions() override {
        return TPlannerPlacingOptions{
            .MaxNonParallelTasksExecutionLimit = MaxNonParallelTasksExecutionLimit.load(),
            .MaxNonParallelDataQueryTasksLimit = MaxNonParallelDataQueryTasksLimit.load(),
            .MaxNonParallelTopStageExecutionLimit = MaxNonParallelTopStageExecutionLimit.load(),
            .PreferLocalDatacenterExecution = PreferLocalDatacenterExecution.load(),
        };
    }

    void CreateResourceInfoExchanger(
            const NKikimrConfig::TTableServiceConfig::TResourceManager::TInfoExchangerSettings& settings) {
        auto exchanger = CreateKqpResourceInfoExchangerActor(
            Counters, ResourceSnapshotState, settings);
        ResourceInfoExchanger = ActorSystem->Register(exchanger);
    }

    bool AllocateExecutionUnits(ui32 cnt) {
        i32 prev = ExecutionUnitsResource.fetch_sub(cnt);
        if (prev < (i32)cnt) {
            ExecutionUnitsResource.fetch_add(cnt);
            return false;
        } else {
            return true;
        }
    }

    TKqpRMAllocateResult AllocateResources(TTxState& tx, ui64 taskId, const TKqpResourcesRequest& resources) override
    {
        const ui64 txId = tx.TxId;

        TKqpRMAllocateResult result;
        if (resources.ExecutionUnits) {
            if (!AllocateExecutionUnits(resources.ExecutionUnits)) {
                Counters->RmNotEnoughComputeActors->Inc();
                TStringBuilder error;
                error << "TxId: " << txId << ", NodeId: " << SelfId.NodeId() << ", not enough compute actors resource.";
                result.SetError(NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_EXECUTION_UNITS, error);
                return result;
            }
        }

        if (!resources.Memory) {
            tx.Allocated(resources);
            FireResourcesPublishing();
            return result;
        }

        Y_DEFER {
            if (!result && resources.ExecutionUnits) {
                // return allocated resource to free pool
                ExecutionUnitsResource.fetch_add(resources.ExecutionUnits);
            }
        };

        bool hasMemory = true;

        with_lock (Lock) {
            if (Y_UNLIKELY(!ResourceBroker)) {
                TStringBuilder reason;
                reason << "AllocateResources: not ready yet. TxId: " << txId << ", taskId: " << taskId;
                result.SetError(NKikimrKqp::TEvStartKqpTasksResponse::INTERNAL_ERROR, reason);
                return result;
            }

            hasMemory = TotalMemoryResource.Admits(resources.Memory, resources.Optional);
            if (hasMemory) {
                TotalMemoryResource.Acquire(resources.Memory);
            }
        }

        // an optional request refused at the spilling threshold is the spilling signal of its caller, not a failure:
        // not counted as one and not recorded in the tx
        if (!hasMemory && resources.Optional) {
            Counters->RmOptionalMemoryRefused->Inc();
            TStringBuilder reason;
            reason << "TxId: " << txId << ", taskId: " << taskId << ". Optional memory refused at the spilling threshold, requested: "
                << resources.Memory;
            result.SetError(NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_MEMORY, reason);
            return result;
        }

        if (!hasMemory) {
            Counters->RmNotEnoughMemory->Inc();
            tx.AckFailedMemoryAlloc(resources.Memory);
            TStringBuilder reason;
            reason << "TxId: " << txId << ", taskId: " << taskId << ". Not enough memory, requested: " << resources.Memory
                << ". " << tx.ToString();
            result.SetError(NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_MEMORY, reason);
            return result;
        }

        Y_DEFER {
            if (!result) {
                Counters->RmNotEnoughMemory->Inc();
                tx.AckFailedMemoryAlloc(resources.Memory);
                with_lock (Lock) {
                    TotalMemoryResource.Release(resources.Memory);
                }
            }
        };

        ui64 rbTaskId = LastResourceBrokerTaskId.fetch_add(1) + 1;
        TString rbTaskName = TStringBuilder() << "kqp-" << txId << '-' << taskId << '-' << rbTaskId;

        bool allocated = ResourceBroker->SubmitTaskInstant(
            TEvResourceBroker::TEvSubmitTask(rbTaskId, rbTaskName, {0, resources.Memory}, NLocalDb::KqpResourceManagerTaskName, 0, {}),
            SelfId);

        if (!allocated) {
            TStringBuilder reason;
            reason << "TxId: " << txId << ", taskId: " << taskId << ". Not enough memory, requested: " << resources.Memory
                << ". " << tx.ToString();
            if (ActorSystem) {
                YDB_LOG_NOTICE_CTX(*ActorSystem, "",
                    {"reason", reason});
            }
            result.SetError(NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_MEMORY, reason);
            return result;
        }

        tx.Allocated(resources);

        ui64 currentRbTaskId = 0;
        if (!tx.TxResourceBrokerTaskId.compare_exchange_strong(currentRbTaskId, rbTaskId)) {
            bool merged = ResourceBroker->MergeTasksInstant(currentRbTaskId, rbTaskId, SelfId);
            Y_ABORT_UNLESS(merged);
        }

        if (ActorSystem) {
            YDB_LOG_DEBUG_CTX(*ActorSystem, "Allocated",
                {"txId", txId},
                {"taskId", taskId},
                {"resources", resources});
        }
        FireResourcesPublishing();
        return result;
    }

    void FreeResourcesImpl(TTxState& tx, ui64 taskId, const TKqpResourcesRequest& resources, bool reduceResourceBrokerTask) {

        auto released = tx.Released(resources);
        Y_ABORT_UNLESS(released);

        if (resources.Memory && reduceResourceBrokerTask) {
            auto currentRbTaskId = tx.TxResourceBrokerTaskId.load();
            Y_DEBUG_ABORT_UNLESS(currentRbTaskId);
            bool reduced = ResourceBroker->ReduceTaskResourcesInstant(currentRbTaskId, {0, resources.Memory}, SelfId);
            Y_DEBUG_ABORT_UNLESS(reduced);
        }

        if (resources.Memory > 0) {
            with_lock (Lock) {
                TotalMemoryResource.Release(resources.Memory);
            }
        }

        if (resources.ExecutionUnits) {
            ExecutionUnitsResource.fetch_add(resources.ExecutionUnits);
        }

        if (ActorSystem) {
            YDB_LOG_DEBUG_CTX(*ActorSystem, "Released resources, Free",
                {"txId", tx.TxId},
                {"taskId", taskId},
                {"memory", resources.Memory},
                {"executionUnits", resources.ExecutionUnits});
        }

        FireResourcesPublishing();
    }

    void FreeResources(TTxState& tx, ui64 taskId, const TKqpResourcesRequest& resources) override {
        FreeResourcesImpl(tx, taskId, resources, true);
    }

    void FinishTx(TTxState& tx) override {
        if (auto currentRbTaskId = tx.TxResourceBrokerTaskId.exchange(0); currentRbTaskId) {
            bool finished = ResourceBroker->FinishTaskInstant(TEvResourceBroker::TEvFinishTask(currentRbTaskId), SelfId);
            Y_DEBUG_ABORT_UNLESS(finished);
        }
        FreeResourcesImpl(tx, 0, tx.FreeResourcesRequest(), false);
    }

    TVector<NKikimrKqp::TKqpNodeResources> GetClusterResources() const override {
        TVector<NKikimrKqp::TKqpNodeResources> resources;
        std::shared_ptr<TVector<NKikimrKqp::TKqpNodeResources>> infos;
        with_lock (ResourceSnapshotState->Lock) {
            infos = ResourceSnapshotState->Snapshot;
        }
        if (infos != nullptr) {
            resources = *infos;
        }

        return resources;
    }

    void RequestClusterResourcesInfo(TOnResourcesSnapshotCallback&& callback) override {
        if (ActorSystem) {
            YDB_LOG_DEBUG_CTX(*ActorSystem, "Schedule Snapshot request");
        }
        std::shared_ptr<TVector<NKikimrKqp::TKqpNodeResources>> infos;
        with_lock (ResourceSnapshotState->Lock) {
            infos = ResourceSnapshotState->Snapshot;
        }
        TVector<NKikimrKqp::TKqpNodeResources> resources;
        if (infos != nullptr) {
            resources = *infos;
        }
        callback(std::move(resources));
    }

    bool GetInitialBoardSyncDone() const override {
        with_lock (ResourceSnapshotState->Lock) {
            return ResourceSnapshotState->InitialBoardSyncReceived;
        }
    }

    TVector<ui32> GetInitialBoardNodeIds() const override {
        with_lock (ResourceSnapshotState->Lock) {
            return ResourceSnapshotState->InitialBoardNodeIds;
        }
    }

    TKqpLocalNodeResources GetLocalResources() const override {
        TKqpLocalNodeResources result;
        result.ExecutionUnits = ExecutionUnitsResource.load();
        result.Memory = GetQueryMemoryAvailable();
        return result;
    }

    std::shared_ptr<NMiniKQL::TComputationPatternLRUCache> GetPatternCache() override {
        with_lock (Lock) {
            return PatternCache;
        }
    }

    TTaskResourceEstimation EstimateTaskResources(const NYql::NDqProto::TDqTask& task, const ui32 tasksCount) override
    {
        TTaskResourceEstimation ret = BuildInitialTaskResources(task);
        EstimateTaskResources(ret, tasksCount);
        return ret;
    }

    void EstimateTaskResources(TTaskResourceEstimation& ret, const ui32 tasksCount) override
    {
        NKikimr::NKqp::EstimateTaskResources(ret, {
            .ChannelBufferSize = ChannelBufferSize.load(),
            .MinChannelBufferSize = MinChannelBufferSize.load(),
            .MaxTotalChannelBuffersSize = MaxTotalChannelBuffersSize.load(),
            .MkqlHeavyProgramMemoryLimit = MkqlHeavyProgramMemoryLimit.load(),
            .MkqlLightProgramMemoryLimit = MkqlLightProgramMemoryLimit.load(),
        }, tasksCount);
    }

    // A new node total (the resource broker queue limit)
    void SetTotalMemoryLimit(ui64 limit) {
        with_lock (Lock) {
            TotalMemoryResource.SetLimit(limit);
        }
    }

    // The memory of the queries, see TEvQueryMemoryState. Returns true when it's changed.
    bool SetQueryMemoryState(ui64 limit, ui64 usage) {
        const bool changed = QueryMemoryLimit.exchange(limit) != limit;
        return QueryMemoryUsage.exchange(usage) != usage || changed;
    }

    ui64 GetQueryMemoryAvailable() const {
        const ui64 limit = QueryMemoryLimit.load();
        const ui64 usage = QueryMemoryUsage.load();
        return limit > usage ? limit - usage : 0;
    }

    // Called under Lock from the config notification handler; the constructor calls it before anything else
    // can see the resource manager
    void SetConfigValues(const NKikimrConfig::TTableServiceConfig::TResourceManager& config) {
        MkqlHeavyProgramMemoryLimit.store(config.GetMkqlHeavyProgramMemoryLimit());
        MkqlLightProgramMemoryLimit.store(config.GetMkqlLightProgramMemoryLimit());
        ChannelBufferSize.store(config.GetChannelBufferSize());
        MinChannelBufferSize.store(config.GetMinChannelBufferSize());
        MaxTotalChannelBuffersSize.store(config.GetMaxTotalChannelBuffersSize());
        TotalMemoryResource.SetOverPercent(config.GetSpillingPercent());
        MaxNonParallelTopStageExecutionLimit.store(config.GetMaxNonParallelTopStageExecutionLimit());
        MaxNonParallelTasksExecutionLimit.store(config.GetMaxNonParallelTasksExecutionLimit());
        PreferLocalDatacenterExecution.store(config.GetPreferLocalDatacenterExecution());
        MaxNonParallelDataQueryTasksLimit.store(config.GetMaxNonParallelDataQueryTasksLimit());
    }

    ui32 GetNodeId() override {
        return SelfId.NodeId();
    }

    void FireResourcesPublishing() {
        bool prev = PublishScheduled.test_and_set();
        if (!prev) {
            if (Y_LIKELY(ActorSystem)) {
                ActorSystem->Send(SelfId, new TEvPrivate::TEvSchedulePublishResources);
            } else {
                PublishAfterBootstrap.test_and_set();
            }
        }
    }

    void UpdatePatternCache(ui64 maxSizeBytes, ui64 maxCompiledSizeBytes, ui64 patternAccessTimesBeforeTryToCompile) {
        std::shared_ptr<NMiniKQL::TComputationPatternLRUCache> tmp;
        with_lock(Lock) {
            if (maxSizeBytes == 0) {
                tmp.swap(PatternCache);
                return;
            }

            NMiniKQL::TComputationPatternLRUCache::Config config{maxSizeBytes, maxCompiledSizeBytes, patternAccessTimesBeforeTryToCompile};
            if (!PatternCache) {
                PatternCache = std::make_shared<NMiniKQL::TComputationPatternLRUCache>(config, Counters->GetKqpCounters());
                return;
            }

            auto currentConfig = PatternCache->GetConfiguration();
            if (currentConfig == config) {
                return;
            }

            if (currentConfig.PatternAccessTimesBeforeTryToCompile == config.PatternAccessTimesBeforeTryToCompile) {
                auto unguard = Unguard(Lock);
                PatternCache->UpdateConfiguration(config);
            } else {
                tmp = std::make_shared<NMiniKQL::TComputationPatternLRUCache>(config, Counters->GetKqpCounters());
                tmp.swap(PatternCache);
            }
        }
    }

    TActorId SelfId;

    std::atomic<ui64> MkqlHeavyProgramMemoryLimit;
    std::atomic<ui64> MkqlLightProgramMemoryLimit;
    std::atomic<ui64> ChannelBufferSize;
    std::atomic<ui64> MinChannelBufferSize;
    std::atomic<ui64> MaxTotalChannelBuffersSize;

    TIntrusivePtr<TKqpCounters> Counters;
    TIntrusivePtr<NResourceBroker::IResourceBroker> ResourceBroker;
    TActorSystem* ActorSystem = nullptr;

    // common guard
    TAdaptiveLock Lock;

    // limits (guarded by Lock)
    std::atomic<i32> ExecutionUnitsResource;
    std::atomic<i32> ExecutionUnitsLimit;
    TMemoryResource TotalMemoryResource;
    std::atomic<ui64> MaxNonParallelTopStageExecutionLimit = 1;
    std::atomic<ui64> MaxNonParallelTasksExecutionLimit = 8;
    std::atomic<bool> PreferLocalDatacenterExecution = true;
    std::atomic<ui64> MaxNonParallelDataQueryTasksLimit = 1000;

    // The memory of the queries, pushed by the compute scheduler
    std::atomic<ui64> QueryMemoryLimit;
    std::atomic<ui64> QueryMemoryUsage = 0;

    // current state
    std::atomic<ui64> LastResourceBrokerTaskId = 0;

    std::atomic_flag PublishAfterBootstrap;
    std::atomic_flag PublishScheduled;
    // pattern cache for different actors
    std::shared_ptr<NMiniKQL::TComputationPatternLRUCache> PatternCache;

    // state for resource info exchanger
    std::shared_ptr<TResourceSnapshotState> ResourceSnapshotState;
    TActorId ResourceInfoExchanger = TActorId();
};

struct TResourceManagers {
    std::weak_ptr<TKqpResourceManager> Default;

    TMutex Lock;
    std::unordered_map<ui32, std::weak_ptr<TKqpResourceManager>> ByNodeId;
};

TResourceManagers ResourceManagers;

} // namespace

class TKqpResourceManagerActor : public TActorBootstrapped<TKqpResourceManagerActor> {
    using TBase = TActorBootstrapped<TKqpResourceManagerActor>;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_RESOURCE_MANAGER;
    }

    TKqpResourceManagerActor(const NKikimrConfig::TTableServiceConfig::TResourceManager& config,
        TIntrusivePtr<TKqpCounters> counters,
        std::shared_ptr<TKqpProxySharedResources>&& kqpProxySharedResources, ui32 nodeId,
        TDuration warmupDeadline)
        : NodeId(nodeId)
        , Config(config)
        , KqpProxySharedResources(std::move(kqpProxySharedResources))
        , WarmupInProgress(warmupDeadline > TDuration::Zero())
        , WarmupDeadline(warmupDeadline)
    {
        ResourceManager = std::make_shared<TKqpResourceManager>(config, counters);
    }

    // Is called right after service registration
    // and before any usual actor can try to get ResourceManager
    void Registered(TActorSystem* sys, const TActorId& owner) override {
        TActorBootstrapped::Registered(sys, owner);

        ResourceManager->Registered(Config, sys, SelfId());

        with_lock (ResourceManagers.Lock) {
            if (ResourceManagers.Default.expired()) { // There can be several managers in tests
                ResourceManagers.Default = ResourceManager;
            }
            ResourceManagers.ByNodeId[NodeId] = ResourceManager;
        }
    }

    void Bootstrap() {
        YDB_LOG_DEBUG("Start KqpResourceManagerActor",
            {"selfId", SelfId()});

        ToBroker(new TEvResourceBroker::TEvResourceBrokerRequest);
        ToBroker(new TEvResourceBroker::TEvConfigRequest(NLocalDb::KqpResourceManagerQueue, /*subscribe=*/ true));

        // Subscribe for tenant changes
        Send(MakeTenantPoolRootID(), new TEvents::TEvSubscribe);

        // Subscribe for TableService config changes
        ui32 tableServiceConfigKind = (ui32) NKikimrConsole::TConfigItem::TableServiceConfigItem;

        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
             new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({tableServiceConfigKind}),
             IEventHandle::FlagTrackDelivery);

        if (auto* mon = AppData()->Mon) {
            NMonitoring::TIndexMonPage* actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
            mon->RegisterActorPage(actorsMonPage, "kqp_resource_manager", "KQP Resource Manager", false,
                ResourceManager->ActorSystem, SelfId());
        }

        WhiteBoardService = NNodeWhiteboard::MakeNodeWhiteboardServiceId(SelfId().NodeId());

        if (WarmupInProgress) {
            YDB_LOG_INFO("Warmup in progress, resource publishing delayed for up",
                {"warmupDeadline", WarmupDeadline});
            Schedule(WarmupDeadline, new TEvPrivate::TEvWarmupDeadline());
        }

        Become(&TKqpResourceManagerActor::WorkState);

        AskSelfNodeInfo();
        SendWhiteboardRequest();
    }

public:
    void SendWhiteboardRequest() {
        auto ev = std::make_unique<NNodeWhiteboard::TEvWhiteboard::TEvSystemStateRequest>();
        Send(WhiteBoardService, ev.release(), IEventHandle::FlagTrackDelivery, SelfId().NodeId());
    }

    void Handle(NNodeWhiteboard::TEvWhiteboard::TEvSystemStateResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.SystemStateInfoSize() != 1)  {
            YDB_LOG_DEBUG("Unexpected whiteboard info");
            return;
        }

        const auto& info = record.GetSystemStateInfo(0);
        if (AppData()->UserPoolId >= info.PoolStatsSize()) {
            YDB_LOG_DEBUG("Unexpected whiteboard info: pool size is smaller than user pool id pool user pool",
                {"size", info.PoolStatsSize()},
                {"id", AppData()->UserPoolId});
            return;
        }

        const auto& pool = info.GetPoolStats(AppData()->UserPoolId);

        YDB_LOG_DEBUG("Received node white board pool",
            {"stats", pool.usage()});
        ProxyNodeResources.SetCpuUsage(pool.usage());
        ProxyNodeResources.SetThreads(pool.threads());
    }

private:
    STATEFN(WorkState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvInterconnect::TEvNodeInfo, Handle);
            hFunc(TEvPrivate::TEvPublishResources, HandleWork);
            hFunc(TEvPrivate::TEvSchedulePublishResources, HandleWork);
            hFunc(NNodeWhiteboard::TEvWhiteboard::TEvSystemStateResponse, Handle);
            hFunc(TEvKqp::TEvKqpProxyPublishRequest, HandleWork);
            hFunc(TEvResourceBroker::TEvConfigResponse, HandleWork);
            hFunc(TEvResourceBroker::TEvResourceBrokerResponse, HandleWork);
            hFunc(TEvQueryMemoryState, HandleWork);
            hFunc(TEvTenantPool::TEvTenantPoolStatus, HandleWork);
            hFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, HandleWork);
            hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, HandleWork);
            hFunc(TEvKqpWarmupComplete, HandleWarmupComplete);
            cFunc(TEvPrivate::EvWarmupDeadline, HandleWarmupDeadline);
            hFunc(TEvents::TEvUndelivered, HandleWork);
            hFunc(TEvents::TEvPoison, HandleWork);
            hFunc(NMon::TEvHttpInfo, HandleWork);
            default: {
                Y_ABORT("Unexpected event 0x%x at TKqpResourceManagerActor::WorkState", ev->GetTypeRewrite());
            }
        }
    }

    void HandleWork(TEvPrivate::TEvPublishResources::TPtr&) {
        PublishResourcesScheduledAt.reset();

        PublishResourceUsage("batching");
    }

    void HandleWork(TEvPrivate::TEvSchedulePublishResources::TPtr&) {
        PublishResourceUsage("alloc");
    }

    void HandleWork(TEvKqp::TEvKqpProxyPublishRequest::TPtr&) {
        SendWhiteboardRequest();
        if (AppData()->TenantName.empty() || !SelfDataCenterId) {
            YDB_LOG_INFO("Cannot start publishing usage for kqp_proxy",
                {"tenants", AppData()->TenantName},
                {"selfDataCenterId", SelfDataCenterId.value_or("empty")});
            return;
        }
        PublishResourceUsage("kqp_proxy");
    }

    void HandleWork(TEvResourceBroker::TEvConfigResponse::TPtr& ev) {
        if (!ev->Get()->QueueConfig) {
            YDB_LOG_ERROR("Resource broker queue is not configured",
                {"queueName", NLocalDb::KqpResourceManagerQueue});
            return;
        }
        auto& queueConfig = *ev->Get()->QueueConfig;

        if (queueConfig.GetLimit().GetMemory() > 0) {
            ResourceManager->SetTotalMemoryLimit(queueConfig.GetLimit().GetMemory());
            YDB_LOG_INFO("Total node memory for the services other than the queries",
                {"bytes", queueConfig.GetLimit().GetMemory()});
        }
    }

    void HandleWork(TEvResourceBroker::TEvResourceBrokerResponse::TPtr& ev) {
        with_lock (ResourceManager->Lock) {
            ResourceManager->ResourceBroker = ev->Get()->ResourceBroker;
        }
    }

    void HandleWork(TEvQueryMemoryState::TPtr& ev) {
        if (ResourceManager->SetQueryMemoryState(ev->Get()->Limit, ev->Get()->Usage)) {
            ResourceManager->FireResourcesPublishing();
        }
    }

    void AskSelfNodeInfo() {
        Send(GetNameserviceActorId(), new TEvInterconnect::TEvGetNode(SelfId().NodeId()));
    }

    void Handle(TEvInterconnect::TEvNodeInfo::TPtr& ev) {
        SelfDataCenterId = TString();
        if (const auto& node = ev->Get()->Node) {
            SelfDataCenterId = node->Location.GetDataCenterId();
        }

        ProxyNodeResources.SetNodeId(SelfId().NodeId());
        ProxyNodeResources.SetDataCenterNumId(DataCenterFromString(*SelfDataCenterId));
        ProxyNodeResources.SetDataCenterId(*SelfDataCenterId);
        PublishResourceUsage("data_center update");
    }

    void HandleWork(TEvTenantPool::TEvTenantPoolStatus::TPtr& ev) {
        TString tenant;
        for (auto &slot : ev->Get()->Record.GetSlots()) {
            if (slot.HasAssignedTenant()) {
                if (tenant.empty()) {
                    tenant = slot.GetAssignedTenant();
                } else {
                    YDB_LOG_ERROR("Multiple tenants are served by the",
                        {"node", ev->Get()->Record.ShortDebugString()});
                }
            }
        }

        WbState.Tenant = tenant;
        WbState.BoardPath = MakeKqpRmBoardPath(tenant);

        if (auto *domain = AppData()->DomainsInfo->GetDomain(); domain->Name != ExtractDomain(tenant)) {
            WbState.DomainNotFound = true;
        }

        YDB_LOG_INFO("Received tenant pool status, serving",
            {"tenant", tenant},
            {"board", WbState.BoardPath});

        PublishResourceUsage("tenant updated");
    }

    static void HandleWork(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr&) {
        YDB_LOG_DEBUG("Subscribed for config changes");
    }

    void HandleWork(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
        auto& event = ev->Get()->Record;
        Send(ev->Sender, new NConsole::TEvConsole::TEvConfigNotificationResponse(event), IEventHandle::FlagTrackDelivery, ev->Cookie);

        auto& config = *event.MutableConfig()->MutableTableServiceConfig()->MutableResourceManager();
        ResourceManager->UpdatePatternCache(config.GetKqpPatternCacheCapacityBytes(),
            config.GetKqpPatternCacheCompiledCapacityBytes(),
            config.GetKqpPatternCachePatternAccessTimesBeforeTryToCompile());

#define FORCE_VALUE(name) if (!config.Has ## name ()) config.Set ## name(config.Get ## name());
        FORCE_VALUE(ComputeActorsCount)
        FORCE_VALUE(ChannelBufferSize)
        FORCE_VALUE(MkqlLightProgramMemoryLimit)
        FORCE_VALUE(MkqlHeavyProgramMemoryLimit)
        FORCE_VALUE(PublishStatisticsIntervalSec);
        FORCE_VALUE(MaxTotalChannelBuffersSize);
        FORCE_VALUE(MinChannelBufferSize);
#undef FORCE_VALUE

        YDB_LOG_INFO("Updated table service",
            {"config", config.DebugString()});

        with_lock (ResourceManager->Lock) {
            i32 prev = ResourceManager->ExecutionUnitsLimit.load();
            ResourceManager->ExecutionUnitsLimit.store(config.GetComputeActorsCount());
            ResourceManager->ExecutionUnitsResource.fetch_add((i32)config.GetComputeActorsCount() - prev);
            ResourceManager->SetConfigValues(config);
            Config.Swap(&config);
        }
    }

    static void HandleWork(TEvents::TEvUndelivered::TPtr& ev) {
        switch (ev->Get()->SourceType) {
            case NConsole::TEvConfigsDispatcher::EvSetConfigSubscriptionRequest:
                YDB_LOG_CRIT("Failed to deliver subscription request to config dispatcher");
                break;

            case NConsole::TEvConsole::EvConfigNotificationResponse:
                YDB_LOG_ERROR("Failed to deliver config notification response");
                break;

            default:
                YDB_LOG_CRIT("Undelivered event with unexpected source",
                    {"type", ev->Get()->SourceType});
                break;
        }
    }

    void HandleWork(TEvents::TEvPoison::TPtr&) {
        PassAway();
    }

    void HandleWork(NMon::TEvHttpInfo::TPtr& ev) {
        TStringStream str;
        str.Reserve(8 * 1024);

        auto snapshot = ResourceManager->GetClusterResources();

        HTML(str) {
            PRE() {
                str << "State storage key: " << WbState.Tenant << Endl;
                str << "Query memory: " << ResourceManager->QueryMemoryUsage.load() << '/' << ResourceManager->QueryMemoryLimit.load() << Endl;
                with_lock (ResourceManager->Lock) {
                    str << "Services memory resource: " << ResourceManager->TotalMemoryResource.ToString() << Endl;
                }
                str << "ExecutionUnits resource: " << ResourceManager->ExecutionUnitsResource.load() << Endl;
                str << "Last resource broker task id: " << ResourceManager->LastResourceBrokerTaskId.load() << Endl;
                if (WbState.LastPublishTime) {
                    str << "Last publish time: " << *WbState.LastPublishTime << Endl;
                }

                if (PublishResourcesScheduledAt) {
                    str << "Next publish time: " << *PublishResourcesScheduledAt << Endl;
                }

                if (snapshot.empty()) {
                    str << "No nodes resource info" << Endl;
                } else {
                    str << Endl << "Resources info: " << Endl;
                    str << "Nodes count: " << snapshot.size() << Endl;
                    str << Endl;
                    for(const auto& entry : snapshot) {
                        str << "  NodeId: " << entry.GetNodeId() << Endl;
                        str << "    ResourceManagerActorId: " << entry.GetResourceManagerActorId() << Endl;
                        str << "    AvailableComputeActors: " << entry.GetAvailableComputeActors() << Endl;
                        str << "    UsedMemory: " << entry.GetUsedMemory() << Endl;
                        str << "    TotalMemory: " << entry.GetTotalMemory() << Endl;
                        str << "    Timestamp: " << entry.GetTimestamp() << Endl;
                        str << "    Memory:" << Endl;;
                        for (const auto& memoryInfo: entry.GetMemory()) {
                            str << "      Pool: " << memoryInfo.GetPool() << Endl;
                            str << "      Available: " << memoryInfo.GetAvailable() << Endl;
                        }
                        str << "    ExecutionUnits: " << entry.GetExecutionUnits() << Endl;
                    }
                 }
            } // PRE()
        }

        Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
    }

private:
    void PassAway() override {
        ToBroker(new TEvResourceBroker::TEvNotifyActorDied);
        if (ResourceManager->ResourceInfoExchanger) {
            Send(ResourceManager->ResourceInfoExchanger, new TEvents::TEvPoison);
            ResourceManager->ResourceInfoExchanger = TActorId();
        }
        ResourceManager->ResourceSnapshotState.reset();
        TActor::PassAway();
    }

    void ToBroker(IEventBase* ev) {
        Send(MakeResourceBrokerID(), ev);
    }

    static TString MakeKqpRmBoardPath(TStringBuf database) {
        return TStringBuilder() << "kqprm+" << database;
    }

    void HandleWarmupComplete(TEvKqpWarmupComplete::TPtr&) {
        if (WarmupInProgress) {
            WarmupInProgress = false;
            YDB_LOG_INFO("Warmup complete, starting resource publishing");
            PublishResourceUsage("warmup_complete");
        }
    }

    void HandleWarmupDeadline() {
        if (WarmupInProgress) {
            WarmupInProgress = false;
            YDB_LOG_WARN("Warmup deadline exceeded, forcing resource publishing");
            PublishResourceUsage("warmup_deadline");
        }
    }

    void PublishResourceUsage(TStringBuf reason) {
        const TDuration publishInterval = TDuration::Seconds(Config.GetPublishStatisticsIntervalSec());
        if (PublishResourcesScheduledAt) {
            return;
        }

        auto now = ResourceManager->ActorSystem->Timestamp();
        if (publishInterval && WbState.LastPublishTime && now - *WbState.LastPublishTime < publishInterval) {
            PublishResourcesScheduledAt = *WbState.LastPublishTime + publishInterval;

            Schedule(*PublishResourcesScheduledAt - now, new TEvPrivate::TEvPublishResources);
            YDB_LOG_DEBUG("Scheduled resource usage publish",
                {"publishAt", *PublishResourcesScheduledAt},
                {"delay", (*PublishResourcesScheduledAt - now)});
            return;
        }

        // starting resources publishing.
        // saying resource manager that we are ready for the next publishing.
        ResourceManager->PublishScheduled.clear();

        NKikimrKqp::TKqpNodeResources payload;
        payload.SetNodeId(SelfId().NodeId());
        payload.SetTimestamp(now.Seconds());
        if (KqpProxySharedResources) {
            if (SelfDataCenterId) {
                auto* proxyNodeResources = payload.MutableKqpProxyNodeResources();
                ProxyNodeResources.SetActiveWorkersCount(KqpProxySharedResources->AtomicLocalSessionCount.load());
                if (SelfDataCenterId) {
                    *proxyNodeResources = ProxyNodeResources;
                }
            }
        } else {
            YDB_LOG_DEBUG("Don't set KqpProxySharedResources");
        }
        ActorIdToProto(MakeKqpResourceManagerServiceID(SelfId().NodeId()), payload.MutableResourceManagerActorId()); // legacy

        if (WarmupInProgress) {
            // Publish with zero compute resources during warmup to prevent other nodes
            // from assigning compute tasks, while keeping discovery and gossip working
            payload.SetAvailableComputeActors(0);
            payload.SetTotalMemory(0);
            payload.SetUsedMemory(0);
            payload.SetExecutionUnits(0);
            auto* pool = payload.MutableMemory()->Add();
            pool->SetPool(1); // legacy ScanQuery pool id
            pool->SetAvailable(0);
        } else {
            // The memory for the tasks of the queries
            const ui64 memoryLimit = ResourceManager->QueryMemoryLimit.load();
            const ui64 memoryAvailable = Min(ResourceManager->GetQueryMemoryAvailable(), memoryLimit);
            payload.SetAvailableComputeActors(ResourceManager->ExecutionUnitsResource.load()); // legacy
            payload.SetTotalMemory(memoryLimit); // legacy
            payload.SetUsedMemory(memoryLimit - memoryAvailable); // legacy

            payload.SetExecutionUnits(ResourceManager->ExecutionUnitsResource.load());
            auto* pool = payload.MutableMemory()->Add();
            pool->SetPool(1); // legacy ScanQuery pool id
            pool->SetAvailable(memoryAvailable);
        }

        YDB_LOG_INFO("Sending resource usage to publish",
            {"reason", reason},
            {"warmupInProgress", WarmupInProgress},
            {"payload", payload.ShortDebugString()});
        WbState.LastPublishTime = now;
        if (ResourceManager->ResourceInfoExchanger) {
            Send(ResourceManager->ResourceInfoExchanger,
                new TEvKqpResourceInfoExchanger::TEvPublishResource(std::move(payload)));
        }
    }

private:
    const ui32 NodeId;
    NKikimrConfig::TTableServiceConfig::TResourceManager Config;

    // Whiteboard specific fields
    struct TWhiteBoardState {
        TString Tenant;
        TString BoardPath;
        bool DomainNotFound = false;
        std::optional<TInstant> LastPublishTime;
    };
    TWhiteBoardState WbState;

    std::shared_ptr<TKqpProxySharedResources> KqpProxySharedResources;
    NKikimrKqp::TKqpProxyNodeResources ProxyNodeResources;

    TActorId WhiteBoardService;

    std::shared_ptr<TKqpResourceManager> ResourceManager;

    std::optional<TInstant> PublishResourcesScheduledAt;
    std::optional<TString> SelfDataCenterId;

    bool WarmupInProgress = false;
    TDuration WarmupDeadline;
};

} // namespace NRm


NActors::IActor* CreateKqpResourceManagerActor(const NKikimrConfig::TTableServiceConfig::TResourceManager& config,
    TIntrusivePtr<TKqpCounters> counters,
    std::shared_ptr<TKqpProxySharedResources> kqpProxySharedResources, ui32 nodeId, TDuration warmupDeadline)
{
    return new NRm::TKqpResourceManagerActor(config, counters, std::move(kqpProxySharedResources), nodeId, warmupDeadline);
}

std::shared_ptr<NRm::IKqpResourceManager> GetKqpResourceManager(TMaybe<ui32> _nodeId) {
    if (auto rm = TryGetKqpResourceManager(_nodeId)) {
        return rm;
    }

    ui32 nodeId = _nodeId ? *_nodeId : TActivationContext::ActorSystem()->NodeId;
    if (auto rm = TryGetKqpResourceManager(nodeId)) {
        return rm;
    }

    Y_ABORT("KqpResourceManager not ready yet, node #%" PRIu32, nodeId);
}

std::shared_ptr<NRm::IKqpResourceManager> TryGetKqpResourceManager(TMaybe<ui32> _nodeId) {
    ui32 nodeId = _nodeId ? *_nodeId : TActivationContext::ActorSystem()->NodeId;
    std::shared_ptr<NRm::TKqpResourceManager> rm = NRm::ResourceManagers.Default.lock();
    if (Y_LIKELY(rm && rm->GetNodeId() == nodeId)) {
        return rm;
    }

    // for tests only
    with_lock (NRm::ResourceManagers.Lock) {
        auto it = NRm::ResourceManagers.ByNodeId.find(nodeId);
        if (it != NRm::ResourceManagers.ByNodeId.end()) {
            return it->second.lock();
        }
    }

    return nullptr;
}

} // namespace NKqp
} // namespace NKikimr

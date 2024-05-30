#include "kqp_rm_service.h"

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

#include <ydb/library/yql/utils/yql_panic.h>

namespace NKikimr {
namespace NKqp {
namespace NRm {

using namespace NActors;
using namespace NResourceBroker;

#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_RESOURCE_MANAGER, stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_RESOURCE_MANAGER, stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_RESOURCE_MANAGER, stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_RESOURCE_MANAGER, stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_RESOURCE_MANAGER, stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_RESOURCE_MANAGER, stream)

#define LOG_AS_C(stream) LOG_CRIT_S(*ActorSystem, NKikimrServices::KQP_RESOURCE_MANAGER, stream)
#define LOG_AS_D(stream) LOG_DEBUG_S(*ActorSystem, NKikimrServices::KQP_RESOURCE_MANAGER, stream)
#define LOG_AS_I(stream) LOG_INFO_S(*ActorSystem, NKikimrServices::KQP_RESOURCE_MANAGER, stream)
#define LOG_AS_E(stream) LOG_ERROR_S(*ActorSystem, NKikimrServices::KQP_RESOURCE_MANAGER, stream)
#define LOG_AS_W(stream) LOG_WARN_S(*ActorSystem, NKikimrServices::KQP_RESOURCE_MANAGER, stream)
#define LOG_AS_N(stream) LOG_NOTICE_S(*ActorSystem, NKikimrServices::KQP_RESOURCE_MANAGER, stream)

namespace {

template <typename T>
class TLimitedResource {
public:
    explicit TLimitedResource(T limit)
        : Limit(limit)
        , Used(0) {}

    T Available() const {
        return Limit > Used ? Limit - Used : 0;
    }

    bool Has(T amount) const {
        return Available() >= amount;
    }

    bool Acquire(T value) {
        if (Available() >= value) {
            Used += value;
            return true;
        }
        return false;
    }

    void Release(T value) {
        if (Used > value) {
            Used -= value;
        } else {
            Used = 0;
        }
    }

    void SetNewLimit(T limit) {
        Limit = limit;
    }

    T GetLimit() const {
        return Limit;
    }

    TString ToString() const {
        return TStringBuilder() << Used << '/' << Limit;
    }

private:
    T Limit;
    T Used;
};

struct TTaskState {
    bool AllocatedExecutionUnit = false;
    ui64 ScanQueryMemory = 0;
    ui64 ExternalDataQueryMemory = 0;
    ui32 ExecutionUnits = 0;
    ui64 ResourceBrokerTaskId = 0;
    TInstant CreatedAt;
};

struct TTxState {
    std::unordered_map<ui64, TTaskState> Tasks;
    ui64 TxScanQueryMemory = 0;
    ui64 TxExternalDataQueryMemory = 0;
    ui32 TxExecutionUnits = 0;
    TInstant CreatedAt;

    TTaskState& Allocated(ui64 taskId, TInstant now, const TKqpResourcesRequest& resources, bool memoryAsExternal = false) {
        ui64 externalMemory = resources.ExternalMemory;
        ui64 resourceBrokerMemory = 0;
        if (memoryAsExternal) {
            externalMemory += resources.Memory;
        } else {
            resourceBrokerMemory = resources.Memory;
        }

        TxExternalDataQueryMemory += externalMemory;
        TxScanQueryMemory += resourceBrokerMemory;
        if (!CreatedAt) {
            CreatedAt = now;
        }

        if (resources.ExecutionUnits) {
            Y_ABORT_UNLESS(!Tasks.contains(taskId));
        }

        auto& taskState = Tasks[taskId];
        taskState.ExecutionUnits += resources.ExecutionUnits;
        taskState.ScanQueryMemory += resourceBrokerMemory;
        taskState.ExternalDataQueryMemory += externalMemory;
        if (!taskState.CreatedAt) {
            taskState.CreatedAt = now;
        }

        return taskState;
    }
};

struct TTxStatesBucket {
    std::unordered_map<ui64, TTxState> Txs;  // TxId -> TxState
    TMutex Lock;
};

constexpr ui64 BucketsCount = 64;

struct TEvPrivate {
    enum EEv {
        EvPublishResources = EventSpaceBegin(TEvents::ES_PRIVATE),
        EvSchedulePublishResources,
        EvTakeResourcesSnapshot,
    };

    struct TEvPublishResources : public TEventLocal<TEvPublishResources, EEv::EvPublishResources> {
    };

    struct TEvSchedulePublishResources : public TEventLocal<TEvSchedulePublishResources, EEv::EvSchedulePublishResources> {
    };

    struct TEvTakeResourcesSnapshot : public TEventLocal<TEvTakeResourcesSnapshot, EEv::EvTakeResourcesSnapshot> {
        std::function<void(TVector<NKikimrKqp::TKqpNodeResources>&&)> Callback;
    };
};

class TKqpResourceManager : public IKqpResourceManager {
public:

    TKqpResourceManager(const NKikimrConfig::TTableServiceConfig::TResourceManager& config, TIntrusivePtr<TKqpCounters> counters)
        : Config(config)
        , Counters(counters)
        , ExecutionUnitsResource(Config.GetComputeActorsCount())
        , ExecutionUnitsLimit(Config.GetComputeActorsCount())
        , ScanQueryMemoryResource(Config.GetQueryMemoryLimit())
        , PublishResourcesByExchanger(Config.GetEnablePublishResourcesByExchanger()) {

    }

    void Bootstrap(TActorSystem* actorSystem, TActorId selfId) {
        if (!Counters) {
            Counters = MakeIntrusive<TKqpCounters>(AppData()->Counters);
        }
        ActorSystem = actorSystem;
        SelfId = selfId;
        UpdatePatternCache(Config.GetKqpPatternCacheCapacityBytes(),
            Config.GetKqpPatternCacheCompiledCapacityBytes(),
            Config.GetKqpPatternCachePatternAccessTimesBeforeTryToCompile());

        if (PublishResourcesByExchanger) {
            CreateResourceInfoExchanger(Config.GetInfoExchangerSettings());
            return;
        }
    }

    void CreateResourceInfoExchanger(
            const NKikimrConfig::TTableServiceConfig::TResourceManager::TInfoExchangerSettings& settings) {
        PublishResourcesByExchanger = true;
        if (!ResourceInfoExchanger) {
            ResourceSnapshotState = std::make_shared<TResourceSnapshotState>();
            auto exchanger = CreateKqpResourceInfoExchangerActor(
                Counters, ResourceSnapshotState, settings);
            ResourceInfoExchanger = ActorSystem->Register(exchanger);
            return;
        }
    }

    bool AllocateExecutionUnits(ui32 cnt) {
        i32 prev = ExecutionUnitsResource.fetch_sub(cnt);
        if (prev < (i32)cnt) {
            ExecutionUnitsResource.fetch_add(cnt);
            return false;
        } else {
            Counters->RmComputeActors->Add(cnt);
            return true;
        }
    }

    void FreeExecutionUnits(ui32 cnt) {
        if (cnt == 0) {
            return;
        }

        ExecutionUnitsResource.fetch_add(cnt);
        Counters->RmComputeActors->Sub(cnt);
    }

    TKqpRMAllocateResult AllocateResources(ui64 txId, ui64 taskId, const TKqpResourcesRequest& resources) override
    {
        TKqpRMAllocateResult result;
        if (resources.ExecutionUnits) {
            if (!AllocateExecutionUnits(resources.ExecutionUnits)) {
                TStringBuilder error;
                error << "TxId: " << txId << ", NodeId: " << SelfId.NodeId() << ", not enough compute actors resource.";
                result.SetError(NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_EXECUTION_UNITS, error);
                return result;
            }
        }

        Y_DEFER {
            if (!result) {
                if (resources.ExecutionUnits) {
                    FreeExecutionUnits(resources.ExecutionUnits);
                }
            }
        };

        if (Y_UNLIKELY(resources.Memory == 0)) {
            return result;
        }

        auto now = ActorSystem->Timestamp();
        bool hasScanQueryMemory = true;
        ui64 queryMemoryLimit = 0;
        // NOTE(gvit): the first memory request from the data query pool always satisfied.
        // all other requests are not guaranteed to be satisfied.
        // In the nearest future we need to implement several layers of memory requests.
        bool isFirstAllocationRequest = (resources.ExecutionUnits > 0 && resources.MemoryPool == EKqpMemoryPool::DataQuery);
        if (isFirstAllocationRequest) {
            auto& txBucket = TxBucket(txId);
            with_lock(txBucket.Lock) {
                auto& tx = txBucket.Txs[txId];
                tx.Allocated(taskId, now, resources, /*memoryAsExternal=*/true);
                ExternalDataQueryMemory.fetch_add(resources.Memory + resources.ExternalMemory);
                Counters->RmExternalMemory->Add(resources.Memory + resources.ExternalMemory);
            }

            return result;
        }

        with_lock (Lock) {
            if (Y_UNLIKELY(!ResourceBroker)) {
                TStringBuilder reason;
                reason << "AllocateResources: not ready yet. TxId: " << txId << ", taskId: " << taskId;
                result.SetError(NKikimrKqp::TEvStartKqpTasksResponse::INTERNAL_ERROR, reason);
                return result;
            }

            hasScanQueryMemory = ScanQueryMemoryResource.Has(resources.Memory);
            if (hasScanQueryMemory) {
                ScanQueryMemoryResource.Acquire(resources.Memory);
                queryMemoryLimit = Config.GetQueryMemoryLimit();
            }
        } // with_lock (Lock)

        if (!hasScanQueryMemory) {
            Counters->RmNotEnoughMemory->Inc();
            TStringBuilder reason;
            reason << "TxId: " << txId << ", taskId: " << taskId << ". Not enough memory for query, requested: " << resources.Memory;
            result.SetError(NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_MEMORY, reason);
            return result;
        }

        ui64 rbTaskId = LastResourceBrokerTaskId.fetch_add(1) + 1;
        TString rbTaskName = TStringBuilder() << "kqp-" << txId << '-' << taskId << '-' << rbTaskId;
        bool extraAlloc = false;

        auto& txBucket = TxBucket(txId);
        with_lock (txBucket.Lock) {
            Y_DEFER {
                if (!result) {
                    auto unguard = ::Unguard(txBucket.Lock);
                    Counters->RmNotEnoughMemory->Inc();
                    with_lock (Lock) {
                        ScanQueryMemoryResource.Release(resources.Memory);
                    } // with_lock (Lock)
                }
            };

            auto& tx = txBucket.Txs[txId];
            ui64 txTotalRequestedMemory = tx.TxScanQueryMemory + resources.Memory;
            if (txTotalRequestedMemory > queryMemoryLimit) {
                TStringBuilder reason;
                reason << "TxId: " << txId << ", taskId: " << taskId << ". Query memory limit exceeded: "
                    << "requested " << txTotalRequestedMemory;
                result.SetError(NKikimrKqp::TEvStartKqpTasksResponse::QUERY_MEMORY_LIMIT_EXCEEDED, reason);
                return result;
            }

            bool allocated = ResourceBroker->SubmitTaskInstant(
                TEvResourceBroker::TEvSubmitTask(rbTaskId, rbTaskName, {0, resources.Memory}, "kqp_query", 0, {}),
                SelfId);

            if (!allocated) {
                TStringBuilder reason;
                reason << "TxId: " << txId << ", taskId: " << taskId << ". Not enough ScanQueryMemory: "
                    << "requested " << resources.Memory;
                LOG_AS_N(reason);
                result.SetError(NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_MEMORY, reason);
                return result;
            }

            auto& taskState = tx.Allocated(taskId, now, resources);
            if (!taskState.ResourceBrokerTaskId) {
                taskState.ResourceBrokerTaskId = rbTaskId;
            } else {
                extraAlloc = true;
                bool merged = ResourceBroker->MergeTasksInstant(taskState.ResourceBrokerTaskId, rbTaskId, SelfId);
                Y_ABORT_UNLESS(merged);
            }
        } // with_lock (txBucket.Lock)

        LOG_AS_D("TxId: " << txId << ", taskId: " << taskId << ". Allocated " << resources.ToString());

        Counters->RmMemory->Add(resources.Memory);
        if (extraAlloc) {
            Counters->RmExtraMemAllocs->Inc();
        }

        FireResourcesPublishing();
        return result;
    }

    void FreeResources(ui64 txId, ui64 taskId, const TKqpResourcesRequest& resources) override {

        if (resources.MemoryPool == EKqpMemoryPool::DataQuery) {
            return;
        }

        auto& txBucket = TxBucket(txId);

        {
            TGuard<TMutex> guard(txBucket.Lock);

            auto txIt = txBucket.Txs.find(txId);
            if (txIt == txBucket.Txs.end()) {
                return;
            }

            auto taskIt = txIt->second.Tasks.find(taskId);
            if (taskIt == txIt->second.Tasks.end()) {
                return;
            }

            taskIt->second.ScanQueryMemory -= resources.Memory;

            bool reduced = ResourceBroker->ReduceTaskResourcesInstant(
                taskIt->second.ResourceBrokerTaskId, {0, resources.Memory}, SelfId);
            Y_DEBUG_ABORT_UNLESS(reduced);

            txIt->second.TxScanQueryMemory -= resources.Memory;

            ScanQueryMemoryResource.Release(resources.Memory);
        }

        Counters->RmMemory->Sub(resources.Memory);

        Y_DEBUG_ABORT_UNLESS(Counters->RmComputeActors->Val() >= 0);
        Y_DEBUG_ABORT_UNLESS(Counters->RmMemory->Val() >= 0);

        FireResourcesPublishing();
    }

    void FreeResources(ui64 txId, ui64 taskId) override {
        ui64 releaseScanQueryMemory = 0;
        ui64 releaseExternalDataQueryMemory = 0;
        ui32 remainsTasks = 0;

        auto& txBucket = TxBucket(txId);

        {
            TMaybe<TGuard<TMutex>> guard;
            guard.ConstructInPlace(txBucket.Lock);

            auto txIt = txBucket.Txs.find(txId);
            if (txIt == txBucket.Txs.end()) {
                return;
            }

            auto& tx = txIt->second;
            auto taskIt = tx.Tasks.find(taskId);
            if (taskIt == tx.Tasks.end()) {
                return;
            }

            auto& task = taskIt->second;
            if (task.ExecutionUnits) {
                FreeExecutionUnits(task.ExecutionUnits);
            }

            releaseExternalDataQueryMemory = task.ExternalDataQueryMemory;
            releaseScanQueryMemory = task.ScanQueryMemory;

            if (task.ResourceBrokerTaskId) {
                bool finished = ResourceBroker->FinishTaskInstant(
                    TEvResourceBroker::TEvFinishTask(task.ResourceBrokerTaskId), SelfId);
                Y_DEBUG_ABORT_UNLESS(finished);
            }

            remainsTasks = tx.Tasks.size() - 1;

            if (remainsTasks == 0) {
                txBucket.Txs.erase(txIt);
            } else {
                tx.Tasks.erase(taskIt);
                tx.TxScanQueryMemory -= releaseScanQueryMemory;
                tx.TxExternalDataQueryMemory -= releaseExternalDataQueryMemory;
            }

            i64 prev = ExternalDataQueryMemory.fetch_sub(releaseExternalDataQueryMemory);
            Counters->RmExternalMemory->Sub(releaseExternalDataQueryMemory);
            Y_DEBUG_ABORT_UNLESS(prev >= 0);
        } // with_lock (txBucket.Lock)

        with_lock (Lock) {
            ScanQueryMemoryResource.Release(releaseScanQueryMemory);
        } // with_lock (Lock)

        LOG_AS_D("TxId: " << txId << ", taskId: " << taskId << ". Released resources, "
            << "ScanQueryMemory: " << releaseScanQueryMemory << ". "
            << "Remains " << remainsTasks << " tasks in this tx.");

        Counters->RmMemory->Sub(releaseScanQueryMemory);

        Y_DEBUG_ABORT_UNLESS(Counters->RmComputeActors->Val() >= 0);
        Y_DEBUG_ABORT_UNLESS(Counters->RmMemory->Val() >= 0);

        FireResourcesPublishing();
    }

    void NotifyExternalResourcesAllocated(ui64 txId, ui64 taskId, const TKqpResourcesRequest& resources) override {
        LOG_AS_D("TxId: " << txId << ", taskId: " << taskId << ". External allocation: " << resources.ToString());

        // we don't register data execution units for now
        //YQL_ENSURE(resources.ExecutionUnits == 0);
        YQL_ENSURE(resources.MemoryPool == EKqpMemoryPool::DataQuery);

        auto& txBucket = TxBucket(txId);
        with_lock (txBucket.Lock) {
            txBucket.Txs[txId].Allocated(taskId, TInstant(), resources);
            ExternalDataQueryMemory.fetch_add(resources.ExternalMemory);
            Counters->RmExternalMemory->Add(resources.ExternalMemory);
        } // with_lock (txBucket.Lock)


        FireResourcesPublishing();
    }

    TVector<NKikimrKqp::TKqpNodeResources> GetClusterResources() const override {
        TVector<NKikimrKqp::TKqpNodeResources> resources;
        Y_ABORT_UNLESS(PublishResourcesByExchanger);

        if (PublishResourcesByExchanger) {
            std::shared_ptr<TVector<NKikimrKqp::TKqpNodeResources>> infos;
            with_lock (ResourceSnapshotState->Lock) {
                infos = ResourceSnapshotState->Snapshot;
            }
            if (infos != nullptr) {
                resources = *infos;
            }
        }

        return resources;
    }

    void RequestClusterResourcesInfo(TOnResourcesSnapshotCallback&& callback) override {
        LOG_AS_D("Schedule Snapshot request");
        if (PublishResourcesByExchanger) {
            std::shared_ptr<TVector<NKikimrKqp::TKqpNodeResources>> infos;
            with_lock (ResourceSnapshotState->Lock) {
                infos = ResourceSnapshotState->Snapshot;
            }
            TVector<NKikimrKqp::TKqpNodeResources> resources;
            if (infos != nullptr) {
                resources = *infos;
            }
            callback(std::move(resources));
            return;
        }
        auto ev = MakeHolder<TEvPrivate::TEvTakeResourcesSnapshot>();
        ev->Callback = std::move(callback);
        TAutoPtr<IEventHandle> handle = new IEventHandle(SelfId, SelfId, ev.Release());
        ActorSystem->Send(handle);
    }

    TKqpLocalNodeResources GetLocalResources() const override {
        TKqpLocalNodeResources result;
        result.Memory.fill(0);

        with_lock (Lock) {
            result.ExecutionUnits = ExecutionUnitsResource.load();
            result.Memory[EKqpMemoryPool::ScanQuery] = ScanQueryMemoryResource.Available();
        }

        return result;
    }

    NKikimrConfig::TTableServiceConfig::TResourceManager GetConfig() override {
        with_lock (Lock) {
            return Config;
        }
    }

    std::shared_ptr<NMiniKQL::TComputationPatternLRUCache> GetPatternCache() override {
        with_lock (Lock) {
            return PatternCache;
        }
    }

    ui32 GetNodeId() override {
        return SelfId.NodeId();
    }

    TTxStatesBucket& TxBucket(ui64 txId) {
        return Buckets[txId % Buckets.size()];
    }

    void FireResourcesPublishing() {
        with_lock (Lock) {
            if (PublishScheduledAt) {
                return;
            }
        }

        ActorSystem->Send(SelfId, new TEvPrivate::TEvSchedulePublishResources);
    }

    void UpdatePatternCache(ui64 maxSizeBytes, ui64 maxCompiledSizeBytes, ui64 patternAccessTimesBeforeTryToCompile) {
        if (maxSizeBytes == 0) {
            PatternCache.reset();
            return;
        }

        NMiniKQL::TComputationPatternLRUCache::Config config{maxSizeBytes, maxCompiledSizeBytes, patternAccessTimesBeforeTryToCompile};
        if (!PatternCache || PatternCache->GetConfiguration() != config) {
            PatternCache = std::make_shared<NMiniKQL::TComputationPatternLRUCache>(config, Counters->GetKqpCounters());
        }
    }

    TActorId SelfId;

    NKikimrConfig::TTableServiceConfig::TResourceManager Config;  // guarded by Lock
    TIntrusivePtr<TKqpCounters> Counters;
    TIntrusivePtr<NResourceBroker::IResourceBroker> ResourceBroker;
    TActorSystem* ActorSystem = nullptr;

    // common guard
    TAdaptiveLock Lock;

    // limits (guarded by Lock)
    std::atomic<i32> ExecutionUnitsResource;
    std::atomic<i32> ExecutionUnitsLimit;
    TLimitedResource<ui64> ScanQueryMemoryResource;
    std::atomic<i64> ExternalDataQueryMemory = 0;

    // current state
    std::array<TTxStatesBucket, BucketsCount> Buckets;
    std::atomic<ui64> LastResourceBrokerTaskId = 0;

    // schedule info (guarded by Lock)
    std::optional<TInstant> PublishScheduledAt;

    // pattern cache for different actors
    std::shared_ptr<NMiniKQL::TComputationPatternLRUCache> PatternCache;

    // state for resource info exchanger
    std::shared_ptr<TResourceSnapshotState> ResourceSnapshotState;
    bool PublishResourcesByExchanger;
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
        TIntrusivePtr<TKqpCounters> counters, const TActorId& resourceBrokerId,
        std::shared_ptr<TKqpProxySharedResources>&& kqpProxySharedResources)
        : ResourceBrokerId(resourceBrokerId ? resourceBrokerId : MakeResourceBrokerID())
        , KqpProxySharedResources(std::move(kqpProxySharedResources))
        , PublishResourcesByExchanger(config.GetEnablePublishResourcesByExchanger())
    {
        ResourceManager = std::make_shared<TKqpResourceManager>(config, counters);
    }

    void Bootstrap() {
        ResourceManager->Bootstrap(TlsActivationContext->ActorSystem(), SelfId());

        LOG_D("Start KqpResourceManagerActor at " << SelfId() << " with ResourceBroker at " << ResourceBrokerId);

        // Subscribe for tenant changes
        Send(MakeTenantPoolRootID(), new TEvents::TEvSubscribe);

        // Subscribe for TableService config changes
        ui32 tableServiceConfigKind = (ui32) NKikimrConsole::TConfigItem::TableServiceConfigItem;

        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
             new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({tableServiceConfigKind}),
             IEventHandle::FlagTrackDelivery);

        ToBroker(new TEvResourceBroker::TEvResourceBrokerRequest);
        ToBroker(new TEvResourceBroker::TEvConfigRequest(NLocalDb::KqpResourceManagerQueue));

        if (auto* mon = AppData()->Mon) {
            NMonitoring::TIndexMonPage* actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
            mon->RegisterActorPage(actorsMonPage, "kqp_resource_manager", "KQP Resource Manager", false,
                ResourceManager->ActorSystem, SelfId());
        }

        WhiteBoardService = NNodeWhiteboard::MakeNodeWhiteboardServiceId(SelfId().NodeId());

        Become(&TKqpResourceManagerActor::WorkState);

        AskSelfNodeInfo();
        SendWhiteboardRequest();

        with_lock (ResourceManagers.Lock) {
            ResourceManagers.ByNodeId[SelfId().NodeId()] = ResourceManager;
            ResourceManagers.Default = ResourceManager;
        }
    }

public:
    void SendWhiteboardRequest() {
        auto ev = std::make_unique<NNodeWhiteboard::TEvWhiteboard::TEvSystemStateRequest>();
        Send(WhiteBoardService, ev.release(), IEventHandle::FlagTrackDelivery, SelfId().NodeId());
    }

    void Handle(NNodeWhiteboard::TEvWhiteboard::TEvSystemStateResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.SystemStateInfoSize() != 1)  {
            LOG_D("Unexpected whiteboard info");
            return;
        }

        const auto& info = record.GetSystemStateInfo(0);
        if (AppData()->UserPoolId >= info.PoolStatsSize()) {
            LOG_D("Unexpected whiteboard info: pool size is smaller than user pool id"
                << ", pool size: " << info.PoolStatsSize()
                << ", user pool id: " << AppData()->UserPoolId);
            return;
        }

        const auto& pool = info.GetPoolStats(AppData()->UserPoolId);

        LOG_D("Received node white board pool stats: " << pool.usage());
        ProxyNodeResources.SetCpuUsage(pool.usage());
        ProxyNodeResources.SetThreads(pool.threads());
    }

private:
    STATEFN(WorkState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvInterconnect::TEvNodeInfo, Handle);
            hFunc(TEvPrivate::TEvPublishResources, HandleWork);
            hFunc(TEvPrivate::TEvSchedulePublishResources, HandleWork);
            hFunc(TEvPrivate::TEvTakeResourcesSnapshot, HandleWork);
            hFunc(NNodeWhiteboard::TEvWhiteboard::TEvSystemStateResponse, Handle);
            hFunc(TEvKqp::TEvKqpProxyPublishRequest, HandleWork);
            hFunc(TEvResourceBroker::TEvConfigResponse, HandleWork);
            hFunc(TEvResourceBroker::TEvResourceBrokerResponse, HandleWork);
            hFunc(TEvTenantPool::TEvTenantPoolStatus, HandleWork);
            hFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, HandleWork);
            hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, HandleWork);
            hFunc(TEvents::TEvUndelivered, HandleWork);
            hFunc(TEvents::TEvPoison, HandleWork);
            hFunc(NMon::TEvHttpInfo, HandleWork);
            default: {
                Y_ABORT("Unexpected event 0x%x at TKqpResourceManagerActor::WorkState", ev->GetTypeRewrite());
            }
        }
    }

    void HandleWork(TEvPrivate::TEvPublishResources::TPtr&) {
        with_lock (ResourceManager->Lock) {
            ResourceManager->PublishScheduledAt.reset();
        }

        PublishResourceUsage("batching");
    }

    void HandleWork(TEvPrivate::TEvSchedulePublishResources::TPtr&) {
        PublishResourceUsage("alloc");
    }

    void HandleWork(TEvKqp::TEvKqpProxyPublishRequest::TPtr&) {
        SendWhiteboardRequest();
        if (AppData()->TenantName.empty() || !SelfDataCenterId) {
            LOG_I("Cannot start publishing usage for kqp_proxy, tenants: " << AppData()->TenantName << ", " <<  SelfDataCenterId.value_or("empty"));
            return;
        }
        PublishResourceUsage("kqp_proxy");
    }

    void HandleWork(TEvPrivate::TEvTakeResourcesSnapshot::TPtr& ev) {
        if (WbState.DomainNotFound) {
            LOG_E("Can not take resources snapshot, ssGroupId not set. Tenant: " << WbState.Tenant
                << ", Board: " << WbState.BoardPath);
            ev->Get()->Callback({});
            return;
        }

        LOG_D("Create Snapshot actor, board: " << WbState.BoardPath);

        Register(
            CreateTakeResourcesSnapshotActor(WbState.BoardPath, std::move(ev->Get()->Callback)));
    }

    void HandleWork(TEvResourceBroker::TEvConfigResponse::TPtr& ev) {
        if (!ev->Get()->QueueConfig) {
            LOG_E(NLocalDb::KqpResourceManagerQueue << " not configured!");
            return;
        }
        auto& queueConfig = *ev->Get()->QueueConfig;

        if (queueConfig.GetLimit().GetMemory() > 0) {
            with_lock (ResourceManager->Lock) {
                ResourceManager->ScanQueryMemoryResource.SetNewLimit(queueConfig.GetLimit().GetMemory());
            }
            LOG_I("Total node memory for scan queries: " << queueConfig.GetLimit().GetMemory() << " bytes");
        }
    }

    void HandleWork(TEvResourceBroker::TEvResourceBrokerResponse::TPtr& ev) {
        with_lock (ResourceManager->Lock) {
            ResourceManager->ResourceBroker = ev->Get()->ResourceBroker;
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
                    LOG_E("Multiple tenants are served by the node: " << ev->Get()->Record.ShortDebugString());
                }
            }
        }

        WbState.Tenant = tenant;
        WbState.BoardPath = MakeKqpRmBoardPath(tenant);

        if (auto *domain = AppData()->DomainsInfo->GetDomain(); domain->Name != ExtractDomain(tenant)) {
            WbState.DomainNotFound = true;
        }

        LOG_I("Received tenant pool status, serving tenant: " << tenant << ", board: " << WbState.BoardPath);

        PublishResourceUsage("tenant updated");
    }

    static void HandleWork(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr&) {
        LOG_D("Subscribed for config changes");
    }

    void HandleWork(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
        auto& event = ev->Get()->Record;
        Send(ev->Sender, new NConsole::TEvConsole::TEvConfigNotificationResponse(event), IEventHandle::FlagTrackDelivery, ev->Cookie);

        auto& config = *event.MutableConfig()->MutableTableServiceConfig()->MutableResourceManager();
        ResourceManager->UpdatePatternCache(config.GetKqpPatternCacheCapacityBytes(),
            config.GetKqpPatternCacheCompiledCapacityBytes(),
            config.GetKqpPatternCachePatternAccessTimesBeforeTryToCompile());

        bool enablePublishResourcesByExchanger = config.GetEnablePublishResourcesByExchanger();
        if (enablePublishResourcesByExchanger != PublishResourcesByExchanger) {
            PublishResourcesByExchanger = enablePublishResourcesByExchanger;
            if (enablePublishResourcesByExchanger) {
                ResourceManager->CreateResourceInfoExchanger(config.GetInfoExchangerSettings());
                PublishResourceUsage("exchanger enabled");
            } else {
                if (ResourceManager->ResourceInfoExchanger) {
                    Send(ResourceManager->ResourceInfoExchanger, new TEvents::TEvPoison);
                    ResourceManager->ResourceInfoExchanger = TActorId();
                }
                ResourceManager->PublishResourcesByExchanger = false;
                ResourceManager->ResourceSnapshotState.reset();
                PublishResourceUsage("exchanger disabled");
            }
        }

#define FORCE_VALUE(name) if (!config.Has ## name ()) config.Set ## name(config.Get ## name());
        FORCE_VALUE(ComputeActorsCount)
        FORCE_VALUE(ChannelBufferSize)
        FORCE_VALUE(MkqlLightProgramMemoryLimit)
        FORCE_VALUE(MkqlHeavyProgramMemoryLimit)
        FORCE_VALUE(QueryMemoryLimit)
        FORCE_VALUE(PublishStatisticsIntervalSec);
        FORCE_VALUE(EnableInstantMkqlMemoryAlloc);
        FORCE_VALUE(MaxTotalChannelBuffersSize);
        FORCE_VALUE(MinChannelBufferSize);
#undef FORCE_VALUE

        LOG_I("Updated table service config: " << config.DebugString());

        with_lock (ResourceManager->Lock) {
            i32 prev = ResourceManager->ExecutionUnitsLimit.load();
            ResourceManager->ExecutionUnitsLimit.store(config.GetComputeActorsCount());
            ResourceManager->ExecutionUnitsResource.fetch_add((i32)config.GetComputeActorsCount() - prev);
            ResourceManager->Config.Swap(&config);
        }

    }

    static void HandleWork(TEvents::TEvUndelivered::TPtr& ev) {
        switch (ev->Get()->SourceType) {
            case NConsole::TEvConfigsDispatcher::EvSetConfigSubscriptionRequest:
                LOG_C("Failed to deliver subscription request to config dispatcher");
                break;

            case NConsole::TEvConsole::EvConfigNotificationResponse:
                LOG_E("Failed to deliver config notification response");
                break;

            default:
                LOG_C("Undelivered event with unexpected source type: " << ev->Get()->SourceType);
                break;
        }
    }

    void HandleWork(TEvents::TEvPoison::TPtr&) {
        PassAway();
    }

    void HandleWork(NMon::TEvHttpInfo::TPtr& ev) {
        TStringStream str;
        str.Reserve(8 * 1024);

        auto snapshot = TVector<NKikimrKqp::TKqpNodeResources>();

        if (PublishResourcesByExchanger) {
            ResourceManager->RequestClusterResourcesInfo(
                [&snapshot](TVector<NKikimrKqp::TKqpNodeResources>&& resources) {
                    snapshot = std::move(resources);
                });
        }

        HTML(str) {
            PRE() {
                str << "Current config:" << Endl;
                with_lock (ResourceManager->Lock) {
                    str << ResourceManager->Config.DebugString() << Endl;
                }

                str << "State storage key: " << WbState.Tenant << Endl;
                with_lock (ResourceManager->Lock) {
                    str << "ScanQuery memory resource: " << ResourceManager->ScanQueryMemoryResource.ToString() << Endl;
                    str << "External DataQuery memory: " << ResourceManager->ExternalDataQueryMemory.load() << Endl;
                    str << "ExecutionUnits resource: " << ResourceManager->ExecutionUnitsResource.load() << Endl;
                }
                str << "Last resource broker task id: " << ResourceManager->LastResourceBrokerTaskId.load() << Endl;
                if (WbState.LastPublishTime) {
                    str << "Last publish time: " << *WbState.LastPublishTime << Endl;
                }

                std::optional<TInstant> publishScheduledAt;
                with_lock (ResourceManager->Lock) {
                    publishScheduledAt = ResourceManager->PublishScheduledAt;
                }

                if (publishScheduledAt) {
                    str << "Next publish time: " << *publishScheduledAt << Endl;
                }

                str << Endl << "Transactions:" << Endl;
                for (auto& bucket : ResourceManager->Buckets) {
                    with_lock (bucket.Lock) {
                        for (auto& [txId, txState] : bucket.Txs) {
                            str << "  TxId: " << txId << Endl;
                            str << "    ScanQuery memory: " << txState.TxScanQueryMemory << Endl;
                            str << "    External DataQuery memory: " << txState.TxExternalDataQueryMemory << Endl;
                            str << "    Execution units: " << txState.TxExecutionUnits << Endl;
                            str << "    Create at: " << txState.CreatedAt << Endl;
                            str << "    Tasks:" << Endl;
                            for (auto& [taskId, taskState] : txState.Tasks) {
                                str << "      TaskId: " << taskId << Endl;
                                str << "        ScanQuery memory: " << taskState.ScanQueryMemory << Endl;
                                str << "        External DataQuery memory: " << taskState.ExternalDataQueryMemory << Endl;
                                str << "        Execution units: " << taskState.ExecutionUnits << Endl;
                                str << "        ResourceBroker TaskId: " << taskState.ResourceBrokerTaskId << Endl;
                                str << "        Created at: " << taskState.CreatedAt << Endl;
                            }
                        }
                    } // with_lock (bucket.Lock)
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
                        str << "    Transactions:" << Endl;
                        for (const auto& tx: entry.GetTransactions()) {
                            str << "      TxId: " << tx.GetTxId() << Endl;
                            str << "        ComputeActors: " << tx.GetComputeActors() << Endl;
                            str << "        Memory: " << tx.GetMemory() << Endl;
                            str << "        StartTimestamp: " << tx.GetStartTimestamp() << Endl;
                        }
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
        if (WbState.BoardPublisherActorId) {
            Send(WbState.BoardPublisherActorId, new TEvents::TEvPoison);
        }
        TActor::PassAway();
    }

    void ToBroker(IEventBase* ev) {
        Send(ResourceBrokerId, ev);
    }

    static TString MakeKqpRmBoardPath(TStringBuf database) {
        return TStringBuilder() << "kqprm+" << database;
    }

    void PublishResourceUsage(TStringBuf reason) {
        TDuration publishInterval;
        std::optional<TInstant> publishScheduledAt;

        with_lock (ResourceManager->Lock) {
            publishInterval = TDuration::Seconds(ResourceManager->Config.GetPublishStatisticsIntervalSec());
            publishScheduledAt = ResourceManager->PublishScheduledAt;
        }

        if (publishScheduledAt) {
            return;
        }

        auto now = ResourceManager->ActorSystem->Timestamp();
        if (publishInterval && WbState.LastPublishTime && now - *WbState.LastPublishTime < publishInterval) {
            publishScheduledAt = *WbState.LastPublishTime + publishInterval;

            with_lock (ResourceManager->Lock) {
                ResourceManager->PublishScheduledAt = publishScheduledAt;
            }

            Schedule(*publishScheduledAt - now, new TEvPrivate::TEvPublishResources);
            LOG_D("Schedule publish at " << *publishScheduledAt << ", after " << (*publishScheduledAt - now));
            return;
        }

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
            LOG_D("Don't set KqpProxySharedResources");
        }
        ActorIdToProto(MakeKqpResourceManagerServiceID(SelfId().NodeId()), payload.MutableResourceManagerActorId()); // legacy
        with_lock (ResourceManager->Lock) {
            payload.SetAvailableComputeActors(ResourceManager->ExecutionUnitsResource.load()); // legacy
            payload.SetTotalMemory(ResourceManager->ScanQueryMemoryResource.GetLimit()); // legacy
            payload.SetUsedMemory(ResourceManager->ScanQueryMemoryResource.GetLimit() - ResourceManager->ScanQueryMemoryResource.Available()); // legacy

            payload.SetExecutionUnits(ResourceManager->ExecutionUnitsResource.load());
            auto* pool = payload.MutableMemory()->Add();
            pool->SetPool(EKqpMemoryPool::ScanQuery);
            pool->SetAvailable(ResourceManager->ScanQueryMemoryResource.Available());
        }

        if (PublishResourcesByExchanger) {
            LOG_I("Send to publish resource usage for "
                << "reason: " << reason
                << ", payload: " << payload.ShortDebugString());
            WbState.LastPublishTime = now;
            if (ResourceManager->ResourceInfoExchanger) {
                Send(ResourceManager->ResourceInfoExchanger,
                    new TEvKqpResourceInfoExchanger::TEvPublishResource(std::move(payload)));
            }
            return;
        }

        if (WbState.BoardPublisherActorId) {
            LOG_I("Kill previous board publisher for '" << WbState.BoardPath
                << "' at " << WbState.BoardPublisherActorId << ", reason: " << reason);
            Send(WbState.BoardPublisherActorId, new TEvents::TEvPoison);
        }

        WbState.BoardPublisherActorId = TActorId();

        if (WbState.DomainNotFound) {
            LOG_E("Can not find default state storage group for database " << WbState.Tenant);
            return;
        }

        auto boardPublisher = CreateBoardPublishActor(WbState.BoardPath, payload.SerializeAsString(), SelfId(),
            /* ttlMs */ 0, /* reg */ true);
        WbState.BoardPublisherActorId = Register(boardPublisher);

        WbState.LastPublishTime = now;

        LOG_I("Publish resource usage for '" << WbState.BoardPath << "' at " << WbState.BoardPublisherActorId
            << ", reason: " << reason << ", payload: " << payload.ShortDebugString());
    }

private:
    const TActorId ResourceBrokerId;

    // Whiteboard specific fields
    struct TWhiteBoardState {
        TString Tenant;
        TString BoardPath;
        bool DomainNotFound = false;
        TActorId BoardPublisherActorId;
        std::optional<TInstant> LastPublishTime;
    };
    TWhiteBoardState WbState;

    std::shared_ptr<TKqpProxySharedResources> KqpProxySharedResources;
    NKikimrKqp::TKqpProxyNodeResources ProxyNodeResources;

    TActorId WhiteBoardService;

    std::shared_ptr<TKqpResourceManager> ResourceManager;

    bool PublishResourcesByExchanger;
    std::optional<TString> SelfDataCenterId;
};

} // namespace NRm


NActors::IActor* CreateKqpResourceManagerActor(const NKikimrConfig::TTableServiceConfig::TResourceManager& config,
    TIntrusivePtr<TKqpCounters> counters, NActors::TActorId resourceBroker,
    std::shared_ptr<TKqpProxySharedResources> kqpProxySharedResources)
{
    return new NRm::TKqpResourceManagerActor(config, counters, resourceBroker, std::move(kqpProxySharedResources));
}

std::shared_ptr<NRm::IKqpResourceManager> GetKqpResourceManager(TMaybe<ui32> _nodeId) {
    if (auto rm = TryGetKqpResourceManager(_nodeId)) {
        return rm;
    }

    ui32 nodeId = _nodeId ? *_nodeId : TActivationContext::ActorSystem()->NodeId;
    Y_ABORT("KqpResourceManager not ready yet, node #%" PRIu32, nodeId);
}

std::shared_ptr<NRm::IKqpResourceManager> TryGetKqpResourceManager(TMaybe<ui32> _nodeId) {
    ui32 nodeId = _nodeId ? *_nodeId : TActivationContext::ActorSystem()->NodeId;
    auto rm = NRm::ResourceManagers.Default.lock();
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

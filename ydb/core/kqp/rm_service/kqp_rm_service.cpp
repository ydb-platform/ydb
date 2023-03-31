#include "kqp_rm_service.h"

#include <ydb/core/base/statestorage.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/mind/tenant_pool.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/tablet/resource_broker.h>
#include <ydb/core/kqp/common/kqp.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
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

    bool IsDataQuery = false;
};

struct TTxStatesBucket {
    std::unordered_map<ui64, TTxState> Txs;  // TxId -> TxState
    TMutex Lock;
};

constexpr ui64 BucketsCount = 64;

struct TKqpNodeResourceManager {
    ui32 NodeId;
    IKqpResourceManager* Instance;

    explicit TKqpNodeResourceManager(ui32 nodeId, IKqpResourceManager* instance)
        : NodeId(nodeId)
        , Instance(instance) {}
};

struct TResourceManagers {
    std::atomic<TKqpNodeResourceManager*> Default = nullptr;

    TMutex Lock;
    std::unordered_map<ui32, TKqpNodeResourceManager*> ByNodeId;

    ~TResourceManagers() {
        with_lock(Lock) {
            for (auto [nodeId, rm] : ByNodeId) {
                delete rm;
            }
        }
    }
};

TResourceManagers ResourceManagers;

} // namespace


class TKqpResourceManagerActor : public IKqpResourceManager, public TActorBootstrapped<TKqpResourceManagerActor> {
    using TBase = TActorBootstrapped<TKqpResourceManagerActor>;

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

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_RESOURCE_MANAGER;
    }

    TKqpResourceManagerActor(const NKikimrConfig::TTableServiceConfig::TResourceManager& config,
        TIntrusivePtr<TKqpCounters> counters, const TActorId& resourceBrokerId)
        : Config(config)
        , Counters(counters)
        , ResourceBrokerId(resourceBrokerId ? resourceBrokerId : MakeResourceBrokerID())
        , ExecutionUnitsResource(Config.GetComputeActorsCount())
        , ScanQueryMemoryResource(Config.GetQueryMemoryLimit())
    {}

    void Bootstrap() {
        ActorSystem = TlsActivationContext->ActorSystem();
        if (!Counters) {
            Counters = MakeIntrusive<TKqpCounters>(AppData()->Counters);
        }
        UpdatePatternCache(Config.GetKqpPatternCacheCapacityBytes(), PatternCache, Counters->GetKqpCounters());

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
                ActorSystem, SelfId());
        }

        Become(&TKqpResourceManagerActor::WorkState);

        auto rm = new TKqpNodeResourceManager(SelfId().NodeId(), this);
        with_lock (ResourceManagers.Lock) {
            ResourceManagers.ByNodeId[SelfId().NodeId()] = rm;
        }
        ResourceManagers.Default.store(rm, std::memory_order_release);
    }

public:
    bool AllocateResources(ui64 txId, ui64 taskId, const TKqpResourcesRequest& resources,
        TKqpNotEnoughResources* details) override
    {
        if (resources.MemoryPool == EKqpMemoryPool::DataQuery) {
            NotifyExternalResourcesAllocated(txId, taskId, resources);
            return true;
        }
        Y_VERIFY(resources.MemoryPool == EKqpMemoryPool::ScanQuery);
        if (Y_UNLIKELY(resources.Memory == 0 && resources.ExecutionUnits == 0)) {
            return true;
        }

        auto now = ActorSystem->Timestamp();
        bool hasScanQueryMemory = true;
        bool hasExecutionUnits = true;
        ui64 queryMemoryLimit = 0;

        with_lock (Lock) {
            if (Y_UNLIKELY(!ResourceBroker)) {
                LOG_E("AllocateResources: not ready yet. TxId: " << txId << ", taskId: " << taskId);
                if (details) {
                    details->SetNotReady();
                }
                return false;
            }

            hasScanQueryMemory = ScanQueryMemoryResource.Has(resources.Memory);
            hasExecutionUnits = ExecutionUnitsResource.Has(resources.ExecutionUnits);

            if (hasScanQueryMemory && hasExecutionUnits) {
                ScanQueryMemoryResource.Acquire(resources.Memory);
                ExecutionUnitsResource.Acquire(resources.ExecutionUnits);
                queryMemoryLimit = Config.GetQueryMemoryLimit();
            }
        } // with_lock (Lock)

        if (!hasScanQueryMemory) {
            Counters->RmNotEnoughMemory->Inc();
            LOG_N("TxId: " << txId << ", taskId: " << taskId << ". Not enough ScanQueryMemory, requested: " << resources.Memory);
            if (details) {
                details->SetScanQueryMemory();
            }
            return false;
        }

        if (!hasExecutionUnits) {
            Counters->RmNotEnoughComputeActors->Inc();
            LOG_N("TxId: " << txId << ", taskId: " << taskId << ". Not enough ExecutionUnits, requested: " << resources.ExecutionUnits);
            if (details) {
                details->SetExecutionUnits();
            }
            return false;
        }

        ui64 rbTaskId = LastResourceBrokerTaskId.fetch_add(1) + 1;
        TString rbTaskName = TStringBuilder() << "kqp-" << txId << '-' << taskId << '-' << rbTaskId;
        bool extraAlloc = false;

        auto& txBucket = TxBucket(txId);
        with_lock (txBucket.Lock) {
            if (auto it = txBucket.Txs.find(txId); it != txBucket.Txs.end()) {
                if (it->second.TxScanQueryMemory + resources.Memory > queryMemoryLimit) {
                    auto unguard = ::Unguard(txBucket.Lock);

                    with_lock (Lock) {
                        ScanQueryMemoryResource.Release(resources.Memory);
                        ExecutionUnitsResource.Release(resources.ExecutionUnits);
                    } // with_lock (Lock)

                    Counters->RmNotEnoughMemory->Inc();
                    LOG_N("TxId: " << txId << ", taskId: " << taskId << ". Query memory limit exceeded: "
                        << "requested " << (it->second.TxScanQueryMemory + resources.Memory));
                    if (details) {
                        details->SetQueryMemoryLimit();
                    }
                    return false;
                }
            }

            bool allocated = ResourceBroker->SubmitTaskInstant(
                TEvResourceBroker::TEvSubmitTask(rbTaskId, rbTaskName, {0, resources.Memory}, "kqp_query", 0, {}),
                SelfId());

            if (!allocated) {
                auto unguard = ::Unguard(txBucket.Lock);

                with_lock (Lock) {
                    ScanQueryMemoryResource.Release(resources.Memory);
                    ExecutionUnitsResource.Release(resources.ExecutionUnits);
                } // with_lock (Lock)

                Counters->RmNotEnoughMemory->Inc();
                LOG_N("TxId: " << txId << ", taskId: " << taskId << ". Not enough ScanQueryMemory: "
                    << "requested " << resources.Memory);
                if (details) {
                    details->SetScanQueryMemory();
                }
                return false;
            }

            auto& txState = txBucket.Txs[txId];

            txState.TxScanQueryMemory += resources.Memory;
            txState.TxExecutionUnits += resources.ExecutionUnits;
            if (!txState.CreatedAt) {
                txState.CreatedAt = now;
            }

            auto& taskState = txState.Tasks[taskId];
            taskState.ScanQueryMemory += resources.Memory;
            taskState.ExecutionUnits += resources.ExecutionUnits;
            if (!taskState.CreatedAt) {
                taskState.CreatedAt = now;
            }

            if (!taskState.ResourceBrokerTaskId) {
                taskState.ResourceBrokerTaskId = rbTaskId;
            } else {
                extraAlloc = true;
                bool merged = ResourceBroker->MergeTasksInstant(taskState.ResourceBrokerTaskId, rbTaskId, SelfId());
                Y_VERIFY(merged);
            }
        } // with_lock (txBucket.Lock)

        LOG_D("TxId: " << txId << ", taskId: " << taskId << ". Allocated " << resources.ToString());

        Counters->RmComputeActors->Add(resources.ExecutionUnits);
        Counters->RmMemory->Add(resources.Memory);
        if (extraAlloc) {
            Counters->RmExtraMemAllocs->Inc();
        }

        FireResourcesPublishing();
        return true;
    }

    bool AllocateResources(ui64 txId, ui64 taskId, const TKqpResourcesRequest& resources,
        TResourcesAllocatedCallback&& onSuccess, TNotEnoughtResourcesCallback&& onFail, TDuration timeout) override
    {
        Y_UNUSED(txId, taskId, resources, onSuccess, onFail, timeout);

        // TODO: for DataQuery resources only
        return false;
    }

    void FreeResources(ui64 txId, ui64 taskId) override {
        ui64 releaseScanQueryMemory = 0;
        ui32 releaseExecutionUnits = 0;
        ui32 remainsTasks = 0;

        auto& txBucket = TxBucket(txId);

        {
            TMaybe<TGuard<TMutex>> guard;
            guard.ConstructInPlace(txBucket.Lock);

            auto txIt = txBucket.Txs.find(txId);
            if (txIt == txBucket.Txs.end()) {
                return;
            }

            if (txIt->second.IsDataQuery) {
                guard.Clear();
                return NotifyExternalResourcesFreed(txId, taskId);
            }

            auto taskIt = txIt->second.Tasks.find(taskId);
            if (taskIt == txIt->second.Tasks.end()) {
                return;
            }

            releaseScanQueryMemory = taskIt->second.ScanQueryMemory;
            releaseExecutionUnits = taskIt->second.ExecutionUnits;

            bool finished = ResourceBroker->FinishTaskInstant(
                TEvResourceBroker::TEvFinishTask(taskIt->second.ResourceBrokerTaskId), SelfId());
            Y_VERIFY_DEBUG(finished);

            remainsTasks = txIt->second.Tasks.size() - 1;

            if (remainsTasks == 0) {
                txBucket.Txs.erase(txIt);
            } else {
                txIt->second.Tasks.erase(taskIt);
                txIt->second.TxScanQueryMemory -= releaseScanQueryMemory;
                txIt->second.TxExecutionUnits -= releaseExecutionUnits;
            }
        } // with_lock (txBucket.Lock)

        with_lock (Lock) {
            ScanQueryMemoryResource.Release(releaseScanQueryMemory);
            ExecutionUnitsResource.Release(releaseExecutionUnits);
        } // with_lock (Lock)

        LOG_D("TxId: " << txId << ", taskId: " << taskId << ". Released resources, "
            << "ScanQueryMemory: " << releaseScanQueryMemory << ", ExecutionUnits: " << releaseExecutionUnits << ". "
            << "Remains " << remainsTasks << " tasks in this tx.");

        Counters->RmComputeActors->Sub(releaseExecutionUnits);
        Counters->RmMemory->Sub(releaseScanQueryMemory);

        Y_VERIFY_DEBUG(Counters->RmComputeActors->Val() >= 0);
        Y_VERIFY_DEBUG(Counters->RmMemory->Val() >= 0);

        FireResourcesPublishing();
    }

    void FreeResources(ui64 txId) override {
        ui64 releaseScanQueryMemory = 0;
        ui32 releaseExecutionUnits = 0;

        auto& txBucket = TxBucket(txId);

        {
            TMaybe<TGuard<TMutex>> guard;
            guard.ConstructInPlace(txBucket.Lock);

            auto txIt = txBucket.Txs.find(txId);
            if (txIt == txBucket.Txs.end()) {
                return;
            }
            if (txIt->second.IsDataQuery) {
                guard.Clear();
                return NotifyExternalResourcesFreed(txId);
            }

            for (auto& [taskId, taskState] : txIt->second.Tasks) {
                bool finished = ResourceBroker->FinishTaskInstant(
                    TEvResourceBroker::TEvFinishTask(taskState.ResourceBrokerTaskId), SelfId());
                Y_VERIFY_DEBUG(finished);
            }

            releaseScanQueryMemory = txIt->second.TxScanQueryMemory;
            releaseExecutionUnits = txIt->second.TxExecutionUnits;

            txBucket.Txs.erase(txIt);
        } // with_lock (txBucket.Lock)

        with_lock (Lock) {
            ScanQueryMemoryResource.Release(releaseScanQueryMemory);
            ExecutionUnitsResource.Release(releaseExecutionUnits);
        } // with_lock (Lock)

        LOG_D("TxId: " << txId << ". Released resources, "
            << "ScanQueryMemory: " << releaseScanQueryMemory << ", ExecutionUnits: " << releaseExecutionUnits << ". "
            << "Tx completed.");

        Counters->RmComputeActors->Sub(releaseExecutionUnits);
        Counters->RmMemory->Sub(releaseScanQueryMemory);

        Y_VERIFY_DEBUG(Counters->RmComputeActors->Val() >= 0);
        Y_VERIFY_DEBUG(Counters->RmMemory->Val() >= 0);

        FireResourcesPublishing();
    }

    void NotifyExternalResourcesAllocated(ui64 txId, ui64 taskId, const TKqpResourcesRequest& resources) override {
        LOG_D("TxId: " << txId << ", taskId: " << taskId << ". External allocation: " << resources.ToString());

        // we don't register data execution units for now
        //YQL_ENSURE(resources.ExecutionUnits == 0);
        YQL_ENSURE(resources.MemoryPool == EKqpMemoryPool::DataQuery);

        auto& txBucket = TxBucket(txId);
        with_lock (txBucket.Lock) {
            auto& tx = txBucket.Txs[txId];
            tx.IsDataQuery = true;
            auto& task = tx.Tasks[taskId];

            task.ExternalDataQueryMemory = resources.Memory;
            tx.TxExternalDataQueryMemory += resources.Memory;
        } // with_lock (txBucket.Lock)

        with_lock (Lock) {
            ExternalDataQueryMemory += resources.Memory;
        } // with_lock (Lock)

        Counters->RmExternalMemory->Add(resources.Memory);

        FireResourcesPublishing();
    }

    void NotifyExternalResourcesFreed(ui64 txId, ui64 taskId) override {
        LOG_D("TxId: " << txId << ", taskId: " << taskId << ". External free.");

        ui64 releaseMemory = 0;

        auto& txBucket = TxBucket(txId);
        with_lock (txBucket.Lock) {
            auto txIt = txBucket.Txs.find(txId);
            if (txIt == txBucket.Txs.end()) {
                return;
            }

            auto taskIt = txIt->second.Tasks.find(taskId);
            if (taskIt == txIt->second.Tasks.end()) {
                return;
            }

            releaseMemory = taskIt->second.ExternalDataQueryMemory;

            if (txIt->second.Tasks.size() == 1) {
                txBucket.Txs.erase(txId);
            } else {
                txIt->second.Tasks.erase(taskIt);
                txIt->second.TxExternalDataQueryMemory -= releaseMemory;
            }
        } // with_lock (txBucket.Lock)

        with_lock (Lock) {
            Y_VERIFY_DEBUG(ExternalDataQueryMemory >= releaseMemory);
            ExternalDataQueryMemory -= releaseMemory;
        } // with_lock (Lock)

        Counters->RmExternalMemory->Sub(releaseMemory);
        Y_VERIFY_DEBUG(Counters->RmExternalMemory->Val() >= 0);

        FireResourcesPublishing();
    }

    void NotifyExternalResourcesFreed(ui64 txId) {
        LOG_D("TxId: " << txId << ". External free.");

        ui64 releaseMemory = 0;

        auto& txBucket = TxBucket(txId);
        with_lock (txBucket.Lock) {
            auto txIt = txBucket.Txs.find(txId);
            if (txIt == txBucket.Txs.end()) {
                return;
            }

            for (auto task : txIt->second.Tasks) {
                releaseMemory += task.second.ExternalDataQueryMemory;
            }
            txBucket.Txs.erase(txId);
        } // with_lock (txBucket.Lock)

        with_lock (Lock) {
            Y_VERIFY_DEBUG(ExternalDataQueryMemory >= releaseMemory);
            ExternalDataQueryMemory -= releaseMemory;
        } // with_lock (Lock)

        Counters->RmExternalMemory->Sub(releaseMemory);
        Y_VERIFY_DEBUG(Counters->RmExternalMemory->Val() >= 0);

        FireResourcesPublishing();
    }

    void RequestClusterResourcesInfo(TOnResourcesSnapshotCallback&& callback) override {
        LOG_DEBUG_S(*ActorSystem, NKikimrServices::KQP_RESOURCE_MANAGER, "Schedule Snapshot request");
        auto ev = MakeHolder<TEvPrivate::TEvTakeResourcesSnapshot>();
        ev->Callback = std::move(callback);
        TAutoPtr<IEventHandle> handle = new IEventHandle(SelfId(), SelfId(), ev.Release());
        ActorSystem->Send(handle);
    }

    TKqpLocalNodeResources GetLocalResources() const override {
        TKqpLocalNodeResources result;
        result.Memory.fill(0);

        with_lock (Lock) {
            result.ExecutionUnits = ExecutionUnitsResource.Available();
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

private:
    STATEFN(WorkState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvPublishResources, HandleWork);
            hFunc(TEvPrivate::TEvSchedulePublishResources, HandleWork);
            hFunc(TEvPrivate::TEvTakeResourcesSnapshot, HandleWork);
            hFunc(TEvResourceBroker::TEvConfigResponse, HandleWork);
            hFunc(TEvResourceBroker::TEvResourceBrokerResponse, HandleWork);
            hFunc(TEvTenantPool::TEvTenantPoolStatus, HandleWork);
            hFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, HandleWork);
            hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, HandleWork);
            hFunc(TEvents::TEvUndelivered, HandleWork);
            hFunc(TEvents::TEvPoison, HandleWork);
            hFunc(NMon::TEvHttpInfo, HandleWork);
            default: {
                Y_FAIL("Unexpected event 0x%x at TKqpResourceManagerActor::WorkState", ev->GetTypeRewrite());
            }
        }
    }

    void HandleWork(TEvPrivate::TEvPublishResources::TPtr&) {
        with_lock (Lock) {
            PublishScheduledAt.reset();
        }

        PublishResourceUsage("batching");
    }

    void HandleWork(TEvPrivate::TEvSchedulePublishResources::TPtr&) {
        PublishResourceUsage("alloc");
    }

    void HandleWork(TEvPrivate::TEvTakeResourcesSnapshot::TPtr& ev) {
        if (WbState.StateStorageGroupId == std::numeric_limits<ui32>::max()) {
            LOG_E("Can not take resources snapshot, ssGroupId not set. Tenant: " << WbState.Tenant
                << ", Board: " << WbState.BoardPath << ", ssGroupId: " << WbState.StateStorageGroupId);
            ev->Get()->Callback({});
            return;
        }

        LOG_D("Create Snapshot actor, board: " << WbState.BoardPath << ", ssGroupId: " << WbState.StateStorageGroupId);

        Register(
            CreateTakeResourcesSnapshotActor(WbState.BoardPath, WbState.StateStorageGroupId, std::move(ev->Get()->Callback)));
    }

    void HandleWork(TEvResourceBroker::TEvConfigResponse::TPtr& ev) {
        if (!ev->Get()->QueueConfig) {
            LOG_E(NLocalDb::KqpResourceManagerQueue << " not configured!");
            return;
        }
        auto& queueConfig = *ev->Get()->QueueConfig;

        if (queueConfig.GetLimit().GetMemory() > 0) {
            with_lock (Lock) {
                ScanQueryMemoryResource.SetNewLimit(queueConfig.GetLimit().GetMemory());
            }
            LOG_I("Total node memory for scan queries: " << queueConfig.GetLimit().GetMemory() << " bytes");
        }
    }

    void HandleWork(TEvResourceBroker::TEvResourceBrokerResponse::TPtr& ev) {
        with_lock (Lock) {
            ResourceBroker = ev->Get()->ResourceBroker;
        }
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

        if (auto* domainInfo = AppData()->DomainsInfo->GetDomainByName(ExtractDomain(tenant))) {
            WbState.StateStorageGroupId = domainInfo->DefaultStateStorageGroup;
        } else {
            WbState.StateStorageGroupId = std::numeric_limits<ui32>::max();
        }

        LOG_I("Received tenant pool status, serving tenant: " << tenant << ", board: " << WbState.BoardPath
            << ", ssGroupId: " << WbState.StateStorageGroupId);

        PublishResourceUsage("tenant updated");
    }

    static void HandleWork(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr&) {
        LOG_D("Subscribed for config changes");
    }

    static void UpdatePatternCache(ui64 size, std::shared_ptr<NMiniKQL::TComputationPatternLRUCache>& cache, NMonitoring::TDynamicCounterPtr counters) {
        if (size) {
            if (!cache || cache->GetMaxSize() != size) {
                cache = std::make_shared<NMiniKQL::TComputationPatternLRUCache>(size, counters);
            }
        } else {
            cache.reset();
        }
    }

    void HandleWork(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
        auto& event = ev->Get()->Record;
        Send(ev->Sender, new NConsole::TEvConsole::TEvConfigNotificationResponse(event), IEventHandle::FlagTrackDelivery, ev->Cookie);

        auto& config = *event.MutableConfig()->MutableTableServiceConfig()->MutableResourceManager();
        UpdatePatternCache(config.GetKqpPatternCacheCapacityBytes(), PatternCache, Counters->GetKqpCounters());

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

        with_lock (Lock) {
            ExecutionUnitsResource.SetNewLimit(config.GetComputeActorsCount());
            Config.Swap(&config);
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
        HTML(str) {
            PRE() {
                str << "Current config:" << Endl;
                with_lock (Lock) {
                    str << Config.DebugString() << Endl;
                }

                str << "State storage key: " << WbState.Tenant << Endl;
                with_lock (Lock) {
                    str << "ScanQuery memory resource: " << ScanQueryMemoryResource.ToString() << Endl;
                    str << "External DataQuery memory: " << ExternalDataQueryMemory << Endl;
                    str << "ExecutionUnits resource: " << ExecutionUnitsResource.ToString() << Endl;
                }
                str << "Last resource broker task id: " << LastResourceBrokerTaskId.load() << Endl;
                if (WbState.LastPublishTime) {
                    str << "Last publish time: " << *WbState.LastPublishTime << Endl;
                }

                std::optional<TInstant> publishScheduledAt;
                with_lock (Lock) {
                    publishScheduledAt = PublishScheduledAt;
                }

                if (publishScheduledAt) {
                    str << "Next publish time: " << *publishScheduledAt << Endl;
                }

                str << Endl << "Transactions:" << Endl;
                for (auto& bucket : Buckets) {
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
            } // PRE()
        }

        Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
    }

private:
    TTxStatesBucket& TxBucket(ui64 txId) {
        return Buckets[txId % Buckets.size()];
    }

    void PassAway() override {
        ToBroker(new TEvResourceBroker::TEvNotifyActorDied);
        TActor::PassAway();
    }

    void ToBroker(IEventBase* ev) {
        Send(ResourceBrokerId, ev);
    }

    static TString MakeKqpRmBoardPath(TStringBuf database) {
        return TStringBuilder() << "kqprm+" << database;
    }

    void FireResourcesPublishing() {
        with_lock (Lock) {
            if (PublishScheduledAt) {
                return;
            }
        }

        ActorSystem->Send(SelfId(), new TEvPrivate::TEvSchedulePublishResources);
    }

    void PublishResourceUsage(TStringBuf reason) {
        TDuration publishInterval;
        std::optional<TInstant> publishScheduledAt;

        with_lock (Lock) {
            publishInterval = TDuration::Seconds(Config.GetPublishStatisticsIntervalSec());
            publishScheduledAt = PublishScheduledAt;
        }

        if (publishScheduledAt) {
            return;
        }

        auto now = ActorSystem->Timestamp();
        if (publishInterval && WbState.LastPublishTime && now - *WbState.LastPublishTime < publishInterval) {
            publishScheduledAt = *WbState.LastPublishTime + publishInterval;

            with_lock (Lock) {
                PublishScheduledAt = publishScheduledAt;
            }

            Schedule(*publishScheduledAt - now, new TEvPrivate::TEvPublishResources);
            LOG_D("Schedule publish at " << *publishScheduledAt << ", after " << (*publishScheduledAt - now));
            return;
        }

        if (WbState.BoardPublisherActorId) {
            LOG_I("Kill previous board publisher for '" << WbState.BoardPath
                << "' at " << WbState.BoardPublisherActorId << ", reason: " << reason);
            Send(WbState.BoardPublisherActorId, new TEvents::TEvPoison);
        }

        WbState.BoardPublisherActorId = TActorId();

        if (WbState.StateStorageGroupId == std::numeric_limits<ui32>::max()) {
            LOG_E("Can not find default state storage group for database " << WbState.Tenant);
            return;
        }

        NKikimrKqp::TKqpNodeResources payload;
        payload.SetNodeId(SelfId().NodeId());
        payload.SetTimestamp(now.Seconds());
        ActorIdToProto(MakeKqpResourceManagerServiceID(SelfId().NodeId()), payload.MutableResourceManagerActorId()); // legacy
        with_lock (Lock) {
            payload.SetAvailableComputeActors(ExecutionUnitsResource.Available()); // legacy
            payload.SetTotalMemory(ScanQueryMemoryResource.GetLimit()); // legacy
            payload.SetUsedMemory(ScanQueryMemoryResource.GetLimit() - ScanQueryMemoryResource.Available()); // legacy

            payload.SetExecutionUnits(ExecutionUnitsResource.Available());
            auto* pool = payload.MutableMemory()->Add();
            pool->SetPool(EKqpMemoryPool::ScanQuery);
            pool->SetAvailable(ScanQueryMemoryResource.Available());
        }

        auto boardPublisher = CreateBoardPublishActor(WbState.BoardPath, payload.SerializeAsString(), SelfId(),
            WbState.StateStorageGroupId, /* ttlMs */ 0, /* reg */ true);
        WbState.BoardPublisherActorId = Register(boardPublisher);

        WbState.LastPublishTime = now;

        LOG_I("Publish resource usage for '" << WbState.BoardPath << "' at " << WbState.BoardPublisherActorId
            << ", reason: " << reason << ", groupId: " << WbState.StateStorageGroupId
            << ", payload: " << payload.ShortDebugString());
    }

private:
    NKikimrConfig::TTableServiceConfig::TResourceManager Config;  // guarded by Lock
    TIntrusivePtr<TKqpCounters> Counters;
    const TActorId ResourceBrokerId;
    TIntrusivePtr<NResourceBroker::IResourceBroker> ResourceBroker;
    TActorSystem* ActorSystem = nullptr;

    // common guard
    TAdaptiveLock Lock;

    // limits (guarded by Lock)
    TLimitedResource<ui32> ExecutionUnitsResource;
    TLimitedResource<ui64> ScanQueryMemoryResource;
    ui64 ExternalDataQueryMemory = 0;

    // current state
    std::array<TTxStatesBucket, BucketsCount> Buckets;
    std::atomic<ui64> LastResourceBrokerTaskId = 0;

    // schedule info (guarded by Lock)
    std::optional<TInstant> PublishScheduledAt;

    // Whiteboard specific fields
    struct TWhiteBoardState {
        TString Tenant;
        TString BoardPath;
        ui32 StateStorageGroupId = std::numeric_limits<ui32>::max();
        TActorId BoardPublisherActorId;
        std::optional<TInstant> LastPublishTime;
    };
    TWhiteBoardState WbState;

    // pattern cache for different actors
    std::shared_ptr<NMiniKQL::TComputationPatternLRUCache> PatternCache;
};

} // namespace NRm


NActors::IActor* CreateKqpResourceManagerActor(const NKikimrConfig::TTableServiceConfig::TResourceManager& config,
    TIntrusivePtr<TKqpCounters> counters, NActors::TActorId resourceBroker)
{
    return new NRm::TKqpResourceManagerActor(config, counters, resourceBroker);
}

NRm::IKqpResourceManager* GetKqpResourceManager(TMaybe<ui32> _nodeId) {
    if (auto* rm = TryGetKqpResourceManager(_nodeId)) {
        return rm;
    }

    ui32 nodeId = _nodeId ? *_nodeId : TActivationContext::ActorSystem()->NodeId;
    Y_FAIL("KqpResourceManager not ready yet, node #%" PRIu32, nodeId);
}

NRm::IKqpResourceManager* TryGetKqpResourceManager(TMaybe<ui32> _nodeId) {
    ui32 nodeId = _nodeId ? *_nodeId : TActivationContext::ActorSystem()->NodeId;
    auto rm = NRm::ResourceManagers.Default.load(std::memory_order_acquire);
    if (Y_LIKELY(rm && rm->NodeId == nodeId)) {
        return rm->Instance;
    }

    // for tests only
    with_lock (NRm::ResourceManagers.Lock) {
        auto it = NRm::ResourceManagers.ByNodeId.find(nodeId);
        if (it != NRm::ResourceManagers.ByNodeId.end()) {
            return it->second->Instance;
        }
    }

    return nullptr;
}

} // namespace NKqp
} // namespace NKikimr

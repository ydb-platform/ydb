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
#include <library/cpp/html/pcdata/pcdata.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <yql/essentials/utils/yql_panic.h>

#include <library/cpp/containers/absl/flat_hash_map.h>

#include <algorithm>
#include <cmath>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KQP_RESOURCE_MANAGER

namespace NKikimr {
namespace NKqp {
namespace NRm {

using namespace NActors;
using namespace NResourceBroker;

static double NormalizePoolPercent(double percent) {
    if (!std::isfinite(percent) || percent < 0) {
        return -1;
    }
    return Min(percent, 100.0);
}

// The rule of TTxState::MemoryPoolLimited, also needed before a TTxState exists (the cookie hand-out).
// The percent must already be normalized.
static bool IsMemoryPoolLimited(const TString& poolId, double memoryPoolPercent) {
    return !poolId.empty() && poolId != NResourcePool::DEFAULT_POOL_ID
        && memoryPoolPercent > 0 && memoryPoolPercent < 100;
}

TTxState::TTxState(std::shared_ptr<IKqpResourceManager>& resourceManager, ui64 txId, TInstant now, const TString& poolId, const double memoryPoolPercent,
    const TString& database, bool collectBacktrace)
    : TTxState(resourceManager, txId, now, poolId, memoryPoolPercent, database, collectBacktrace,
        resourceManager->GetMemoryResourceCookies(database, poolId, NormalizePoolPercent(memoryPoolPercent)))
{}

TTxState::TTxState(std::shared_ptr<IKqpResourceManager>& resourceManager, ui64 txId, TInstant now, const TString& poolId, const double memoryPoolPercent,
    const TString& database, bool collectBacktrace, TMemoryResourceCookies cookies)
    : ResourceManager(resourceManager)
    , Counters(resourceManager->GetCounters())
    , TxId(txId)
    , CreatedAt(now)
    , PoolId(poolId)
    , MemoryPoolPercent(NormalizePoolPercent(memoryPoolPercent))
    , Database(database)
    , MemoryPoolLimited(IsMemoryPoolLimited(PoolId, MemoryPoolPercent))
    , CollectBacktrace(collectBacktrace)
    , TotalMemoryCookie(std::move(cookies.Total))
    , PoolMemoryCookie(std::move(cookies.Pool))
{}

TTxState::~TTxState() {
    ResourceManager->FinishTx(*this);
    delete TxMaxAllocationBacktrace.load();
}

namespace {

static constexpr double MYEPS = 1e-9;

// Percents come from the config and the resource pool settings unchecked: anything outside [0, 100] would turn
// into a negative double and wrap on the conversion to ui64, so they are clamped here, the one place they are
// applied. Above 100 behaves as 100 (the spilling threshold at the limit itself), below 0 as 0.
double ClampPercent(double percent) {
    return std::clamp(percent, 0.0, 100.0);
}

ui64 OverPercentage(ui64 limit, double percent) {
    return static_cast<double>(limit) / 100 * (100 - ClampPercent(percent)) + MYEPS;
}

ui64 Percentage(ui64 limit, double percent) {
    return static_cast<double>(limit) / 100 * ClampPercent(percent) + MYEPS;
}

struct TPoolSensors {
    NMonitoring::TDynamicCounters::TCounterPtr Limit;
    NMonitoring::TDynamicCounters::TCounterPtr Allocated;
    NMonitoring::TDynamicCounters::TCounterPtr DeniedRequests;

    explicit operator bool() const {
        return Limit != nullptr;
    }
};

TPoolSensors MakePoolSensors(const TIntrusivePtr<TKqpCounters>& counters, const TString& database, const TString& poolId) {
    auto group = counters->GetWorkloadManagerCounters()->GetSubgroup("pool", TStringBuilder() << database << '/' << poolId);
    return TPoolSensors{
        .Limit = group->GetCounter("MemoryLimit", false),
        .Allocated = group->GetCounter("MemoryAllocated", false),
        .DeniedRequests = group->GetCounter("MemoryDeniedRequests", true),
    };
}

class TMemoryResource : public TAtomicRefCount<TMemoryResource> {
public:
    explicit TMemoryResource(ui64 baseLimit, double memoryPoolPercent, double overPercent)
        : BaseLimit(baseLimit)
        , Used(0)
        , MemoryPoolPercent(memoryPoolPercent)
        , OverPercent(overPercent)
        , SpillingCookie(MakeIntrusive<TMemoryResourceCookie>())
    {
        SetActualLimits();
    }

    ui64 Available() const {
        return Limit > Used ? Limit - Used : 0;
    }

    // Bytes left before the spilling threshold, negative when the threshold is exceeded. Negative whenever Used
    // is past Limit - OverLimit, including a threshold equal to the limit (SpillingPercent = 100, OverLimit = 0)
    // with Used above Limit after the limit was lowered under live usage; the former SpillingPercentReached flag
    // compared the clamped Available() with OverLimit and stayed silent there.
    i64 GetMemoryAvailability() const {
        return static_cast<i64>(Limit) - static_cast<i64>(Used) - static_cast<i64>(OverLimit);
    }

    bool Has(ui64 amount) const {
        return Available() >= amount;
    }

    // Whether a request of `value` may be charged: within the limit and, for an optional one, also within the
    // spilling threshold (see GetMemoryAvailability). Has() goes first, a huge value must not reach the signed check.
    bool Admits(ui64 value, bool optional) const {
        return Has(value) && (!optional || GetMemoryAvailability() >= static_cast<i64>(value));
    }

    // The charge itself, after Admits() or unconditionally for the memory arena: Used may go past Limit, which
    // Available() reads as 0 and the cookies as a negative availability.
    void ForceAcquire(ui64 value) {
        Used += value;
        UpdateCookie();
        if (Sensors) {
            Sensors.Allocated->Set(Used);
        }
    }

    bool HasSensors() const {
        return static_cast<bool>(Sensors);
    }

    void AttachSensors(TPoolSensors sensors) {
        Sensors = std::move(sensors);
        Sensors.Limit->Set(Limit);
        Sensors.Allocated->Set(Used);
        Sensors.DeniedRequests->Add(DeniedRequests);
    }

    void RecordDenied() {
        ++DeniedRequests;
        if (Sensors) {
            Sensors.DeniedRequests->Inc();
        }
    }

    ui64 GetDeniedRequests() const {
        return DeniedRequests;
    }

    TIntrusivePtr<TMemoryResourceCookie> GetSpillingCookie() const {
        return SpillingCookie;
    }

    void UpdateCookie() {
        SpillingCookie->MemoryAvailability.store(GetMemoryAvailability());
    }

    ui64 GetUsed() const {
        return Used;
    }

    void Release(ui64 value) {
        if (Used > value) {
            Used -= value;
        } else {
            Used = 0;
        }

        UpdateCookie();
        if (Sensors) {
            Sensors.Allocated->Set(Used);
        }
    }

    void SetNewLimit(ui64 baseLimit, double memoryPoolPercent, double overPercent) {
        // std::fabs, not abs: unqualified abs may resolve to int abs(int) and truncate, and both percents are
        // legitimately fractional (SpillingPercent in particular), so a sub-1.0 change must not compare equal
        if (baseLimit == BaseLimit && std::fabs(memoryPoolPercent - MemoryPoolPercent) < MYEPS && std::fabs(overPercent - OverPercent) < MYEPS) {
            return;
        }

        BaseLimit = baseLimit;
        MemoryPoolPercent = memoryPoolPercent;
        OverPercent = overPercent;
        SetActualLimits();
    }

    // A runtime SpillingPercent change: the spilling threshold moves, the limit stays
    void SetOverPercent(double overPercent) {
        SetNewLimit(BaseLimit, MemoryPoolPercent, overPercent);
    }

    // A new base (the node total of a pool): the limit follows, the share and the threshold percent stay
    void SetBaseLimit(ui64 baseLimit) {
        SetNewLimit(baseLimit, MemoryPoolPercent, OverPercent);
    }

    // The configured spilling percent, the node total is its one holder (see TKqpResourceManager::SetConfigValues)
    double GetOverPercent() const {
        return OverPercent;
    }

    void SetActualLimits() {
        Limit = Percentage(BaseLimit, MemoryPoolPercent);
        OverLimit = OverPercentage(Limit, OverPercent);
        UpdateCookie();
        if (Sensors) {
            Sensors.Limit->Set(Limit);
        }
    }

    ui64 GetLimit() const {
        return Limit;
    }

    TString ToString() const {
        return TStringBuilder() << Used << '/' << Limit;
    }

private:
    ui64 BaseLimit;
    ui64 OverLimit;
    ui64 Limit;
    ui64 Used;
    double MemoryPoolPercent;
    double OverPercent;
    ui64 DeniedRequests = 0;

    TIntrusivePtr<TMemoryResourceCookie> SpillingCookie;
    TPoolSensors Sensors;
};

struct TEvPrivate {
    enum EEv {
        EvPublishResources = EventSpaceBegin(TEvents::ES_PRIVATE),
        EvSchedulePublishResources,
        EvTakeResourcesSnapshot,
        EvWarmupDeadline,
        EvAdjustArena,
    };

    struct TEvPublishResources : public TEventLocal<TEvPublishResources, EEv::EvPublishResources> {
    };

    struct TEvSchedulePublishResources : public TEventLocal<TEvSchedulePublishResources, EEv::EvSchedulePublishResources> {
    };

    struct TEvWarmupDeadline : public TEventLocal<TEvWarmupDeadline, EEv::EvWarmupDeadline> {
    };

    struct TEvAdjustArena : public TEventLocal<TEvAdjustArena, EEv::EvAdjustArena> {
    };
};

class TKqpResourceManager : public IKqpResourceManager {
public:

    TKqpResourceManager(const NKikimrConfig::TTableServiceConfig::TResourceManager& config, TIntrusivePtr<TKqpCounters> counters)
        : Counters(counters)
        , ExecutionUnitsResource(config.GetComputeActorsCount())
        , ExecutionUnitsLimit(config.GetComputeActorsCount())
        , TotalMemoryResource(MakeIntrusive<TMemoryResource>(config.GetQueryMemoryLimit(), (double)100, config.GetSpillingPercent()))
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

    TMemoryResourceCookies GetMemoryResourceCookies(const TString& database, const TString& poolId, double memoryPoolPercent) override {
        TMemoryResourceCookies cookies;
        with_lock (Lock) {
            cookies.Total = TotalMemoryResource->GetSpillingCookie();
            if (IsMemoryPoolLimited(poolId, memoryPoolPercent)) {
                cookies.Pool = GetOrCreatePoolMemoryResource(TTxState::MakePoolId(database, poolId), memoryPoolPercent)->GetSpillingCookie();
            }
        }
        return cookies;
    }

    // Must be called under Lock. The pool resource is created on its first use with the percent of that tx.
    // The limit of an existing pool is not touched here: it follows the txs that allocate from the pool
    // (see AllocateResources), a tx that merely gets constructed must not move the threshold under the
    // running ones.
    TIntrusivePtr<TMemoryResource> GetOrCreatePoolMemoryResource(const std::pair<TString, TString>& poolKey, double memoryPoolPercent) {
        auto [it, success] = MemoryNamedPools.emplace(poolKey, nullptr);
        if (success) {
            it->second = MakeIntrusive<TMemoryResource>(TotalMemoryResource->GetLimit(), memoryPoolPercent, TotalMemoryResource->GetOverPercent());
        }
        return it->second;
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

        // the arena never refuses, and its demand is applied after everything that can fail has succeeded: no rollback
        if (Y_UNLIKELY(resources.Memory == 0)) {
            tx.Allocated(resources);
            ApplyArenaDemand(resources, /* allocate */ true);
            return result;
        }

        Y_DEFER {
            if (!result && resources.ExecutionUnits) {
                // return allocated resource to free pool
                ExecutionUnitsResource.fetch_add(resources.ExecutionUnits);
            }
        };

        bool hasScanQueryMemory = true;

        with_lock (Lock) {
            if (Y_UNLIKELY(!ResourceBroker)) {
                TStringBuilder reason;
                reason << "AllocateResources: not ready yet. TxId: " << txId << ", taskId: " << taskId;
                result.SetError(NKikimrKqp::TEvStartKqpTasksResponse::INTERNAL_ERROR, reason);
                return result;
            }

            hasScanQueryMemory = TotalMemoryResource->Admits(resources.Memory, resources.Optional);

            TIntrusivePtr<TMemoryResource> poolMemory;
            if (hasScanQueryMemory && tx.HasMemoryPoolLimit()) {
                poolMemory = GetOrCreatePoolMemoryResource(tx.MakePoolId(), tx.MemoryPoolPercent);
                // the pool limit follows the latest tx that allocates from the pool
                poolMemory->SetNewLimit(TotalMemoryResource->GetLimit(), tx.MemoryPoolPercent, TotalMemoryResource->GetOverPercent());
                if (!poolMemory->HasSensors() && PoolSensorsEnabled()) {
                    poolMemory->AttachSensors(MakePoolSensors(Counters, tx.Database, tx.PoolId));
                }
                if (!poolMemory->Admits(resources.Memory, resources.Optional)) {
                    hasScanQueryMemory = false;
                    if (!resources.Optional) {
                        poolMemory->RecordDenied();
                    }
                }
            }

            // charged once both have admitted it, so that a refusal does not charge and release the node total: that would
            // dip its cookie for a moment
            if (hasScanQueryMemory) {
                TotalMemoryResource->ForceAcquire(resources.Memory);
                if (poolMemory) {
                    poolMemory->ForceAcquire(resources.Memory);
                }
            }
        }

        // an optional request refused at the spilling threshold is the spilling signal of its caller, not a failure:
        // not counted as one and not recorded in the tx (its last failed allocation is reported on OOM)
        if (!hasScanQueryMemory && resources.Optional) {
            Counters->RmOptionalMemoryRefused->Inc();
            if (ActorSystem) {
                YDB_LOG_DEBUG_CTX(*ActorSystem, "Optional memory refused at the spilling threshold",
                    {"txId", txId},
                    {"taskId", taskId},
                    {"memory", resources.Memory});
            }
            TStringBuilder reason;
            reason << "TxId: " << txId << ", taskId: " << taskId << ". Optional memory refused at the spilling threshold, requested: "
                << resources.Memory;
            result.SetError(NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_MEMORY, reason);
            return result;
        }

        if (!hasScanQueryMemory) {
            Counters->RmNotEnoughMemory->Inc();
            tx.AckFailedMemoryAlloc(resources.Memory);
            TStringBuilder reason;
            reason << "TxId: " << txId << ", taskId: " << taskId << ". Not enough memory for query, requested: " << resources.Memory
                << ". " << tx.ToString();
            result.SetError(NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_MEMORY, reason);
            return result;
        }

        Y_DEFER {
            if (!result) {
                Counters->RmNotEnoughMemory->Inc();
                tx.AckFailedMemoryAlloc(resources.Memory);
                with_lock (Lock) {
                    TotalMemoryResource->Release(resources.Memory);
                    if (tx.HasMemoryPoolLimit()) {
                        auto it = MemoryNamedPools.find(tx.MakePoolId());
                        if (it != MemoryNamedPools.end()) {
                            it->second->Release(resources.Memory);
                        }
                    }
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
            reason << "TxId: " << txId << ", taskId: " << taskId << ". Not enough memory for query, requested: " << resources.Memory
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

        ApplyArenaDemand(resources, /* allocate */ true);

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
                TotalMemoryResource->Release(resources.Memory);
                if (tx.HasMemoryPoolLimit()) {
                    auto it = MemoryNamedPools.find(tx.MakePoolId());
                    if (it != MemoryNamedPools.end()) {
                        it->second->Release(resources.Memory);
                    }
                }
            }
        }

        // before the execution units go back, so that their next taker is not counted in the arena together with them
        ApplyArenaDemand(resources, /* allocate */ false);

        if (resources.ExecutionUnits) {
            ExecutionUnitsResource.fetch_add(resources.ExecutionUnits);
        }

        if (ActorSystem) {
            YDB_LOG_DEBUG_CTX(*ActorSystem, "Released resources, Free",
                {"txId", tx.TxId},
                {"taskId", taskId},
                {"memory", resources.Memory},
                {"externalMemory", resources.ExternalMemory},
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

        with_lock (Lock) {
            result.ExecutionUnits = ExecutionUnitsResource.load();
            result.Memory = TotalMemoryResource->Available();
            result.ExternalMemory = ArenaExternalMemory.load();
        }

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

    // A new node total (the resource broker queue limit): every pool is a share of it, so the pools follow
    void SetTotalMemoryLimit(ui64 limit) {
        with_lock (Lock) {
            TotalMemoryResource->SetNewLimit(limit, (double)100, TotalMemoryResource->GetOverPercent());
            for (auto& [poolKey, poolMemory] : MemoryNamedPools) {
                poolMemory->SetBaseLimit(TotalMemoryResource->GetLimit());
            }
        }
    }

    // Called under Lock from the config notification handler; the constructor calls it before anything else
    // can see the resource manager
    void SetConfigValues(const NKikimrConfig::TTableServiceConfig::TResourceManager& config) {
        MkqlHeavyProgramMemoryLimit.store(config.GetMkqlHeavyProgramMemoryLimit());
        MkqlLightProgramMemoryLimit.store(config.GetMkqlLightProgramMemoryLimit());
        ChannelBufferSize.store(config.GetChannelBufferSize());
        MinChannelBufferSize.store(config.GetMinChannelBufferSize());
        MaxTotalChannelBuffersSize.store(config.GetMaxTotalChannelBuffersSize());
        QueryMemoryLimit.store(config.GetQueryMemoryLimit());
        // the spilling thresholds of the node total and of every pool follow the new percent right away,
        // the cookies of the running transactions with them; the node total is the holder of the percent
        TotalMemoryResource->SetOverPercent(config.GetSpillingPercent());
        for (auto& [poolKey, poolMemory] : MemoryNamedPools) {
            poolMemory->SetOverPercent(TotalMemoryResource->GetOverPercent());
        }
        MaxNonParallelTopStageExecutionLimit.store(config.GetMaxNonParallelTopStageExecutionLimit());
        MaxNonParallelTasksExecutionLimit.store(config.GetMaxNonParallelTasksExecutionLimit());
        PreferLocalDatacenterExecution.store(config.GetPreferLocalDatacenterExecution());
        MaxNonParallelDataQueryTasksLimit.store(config.GetMaxNonParallelDataQueryTasksLimit());
        EnableMemoryArena.store(config.GetEnableMemoryArena());
        ExecutionUnitMemory.store(config.GetExecutionUnitMemory());
        // the thresholds are compared and summed as signed values, and a max below the min would make the arena
        // oscillate between growing and shrinking
        constexpr ui64 thresholdLimit = static_cast<ui64>(Max<i64>()) / 4;
        const ui64 minFree = Min(config.GetMemoryArenaMinFreeSize(), thresholdLimit);
        MemoryArenaMinFreeSize.store(minFree);
        MemoryArenaMaxFreeSize.store(Max(minFree, Min(config.GetMemoryArenaMaxFreeSize(), thresholdLimit)));
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

    bool PoolSensorsEnabled() const {
        return Counters && ActorSystem && AppData(ActorSystem)->FeatureFlags.GetEnableResourcePoolsCounters();
    }

    // The memory arena (issue #53093): one long lived kqp_query task of the resource broker backs the external
    // memory and the execution units in use. The demand is always satisfied and tracked lock-free. The periodic
    // pass resizes the arena to Used + (MinFree + MaxFree) / 2 whenever its free part leaves that band; a demand
    // that overruns the arena by more than MaxFree grows it right away, on the allocation path. The footprint
    // Max(Size, Used) as of the last resize is charged to the node total, so Memory admission, the spilling cookies
    // and the published resources count it. A Memory request that the free part keeps out is refused: the pass
    // shrinks the arena once its free part exceeds MaxFree, or the node total no longer leaves room for it.

    ui64 ArenaUsed() const {
        return ArenaExternalMemory.load() + ArenaExecutionUnits.load() * ExecutionUnitMemory.load();
    }

    // execution units are granted as a ui32 count (AllocateExecutionUnits) and a transaction holds them as one,
    // so the arena prices that count and not the untruncated request
    static ui32 ArenaUnitsOf(const TKqpResourcesRequest& resources) {
        return static_cast<ui32>(resources.ExecutionUnits);
    }

    bool ArenaActiveLocked() const {
        return EnableMemoryArena.load() && !Arena.Stopped;
    }

    ui64 ArenaDeficitLocked(ui64 used) const {
        const ui64 size = Arena.Size.load();
        return ArenaActiveLocked() && used > size ? used - size : 0;
    }

    // Returns true when the charge moved.
    bool ReconcileArenaLocked() {
        const ui64 used = ArenaUsed();
        const ui64 size = Arena.Size.load();
        const ui64 footprint = ArenaActiveLocked() ? Max(size, used) : 0;
        const bool changed = footprint != Arena.Charged;
        if (footprint > Arena.Charged) {
            TotalMemoryResource->ForceAcquire(footprint - Arena.Charged);
        } else if (footprint < Arena.Charged) {
            TotalMemoryResource->Release(Arena.Charged - footprint);
        }
        Arena.Charged = footprint;
        // only what moved: an idle resource manager must not publish over a busy one
        if (used != Arena.ShownUsed) {
            Counters->RmArenaUsed->Set(used);
            Arena.ShownUsed = used;
        }
        if (size != Arena.ShownSize) {
            Counters->RmArenaSize->Set(size);
            Arena.ShownSize = size;
        }
        if (const ui64 deficit = ArenaDeficitLocked(used); deficit != Arena.ShownDeficit) {
            Counters->RmArenaDeficit->Set(deficit);
            Arena.ShownDeficit = deficit;
        }
        return changed;
    }

    ui64 ArenaTargetLocked(ui64 used) const {
        // An arena backing nothing is given back rather than kept for the next query: its task would otherwise
        // hold the resource broker's count of running tasks above zero for good, and the broker admits work that
        // exceeds its own total limit only while nothing is running at all.
        if (!EnableMemoryArena.load() || used == 0) {
            return 0;
        }
        const i64 minFree = MemoryArenaMinFreeSize.load();
        const i64 maxFree = MemoryArenaMaxFreeSize.load();
        const i64 free = static_cast<i64>(Arena.Size.load()) - static_cast<i64>(used);
        if (free < minFree || free > maxFree) {
            return used + static_cast<ui64>((minFree + maxFree) / 2);
        }
        // in band: a trickle of small queries costs no resource broker call
        return Arena.Size.load();
    }

    // What the node total leaves for the arena once the transactions have taken their share. The resource broker
    // grants the first task of an idle queue whatever its size, so this is the limit the arena respects instead;
    // the demand beyond it stays a charged deficit.
    ui64 ArenaSizeCapLocked() const {
        const ui64 limit = TotalMemoryResource->GetLimit();
        const ui64 used = TotalMemoryResource->GetUsed();
        const ui64 others = used > Arena.Charged ? used - Arena.Charged : 0; // the charge is part of the node total
        return limit > others ? limit - others : 0;
    }

    // The size the arena should be resized to, its size when it should not.
    ui64 ArenaPlanLocked(ui64 used) const {
        const ui64 size = Arena.Size.load();
        if (!ResourceBroker) {
            return size;
        }
        const ui64 cap = ArenaSizeCapLocked();
        // The arena may hold what it backs and, beyond that, only what the node total leaves it, so a total that
        // has dropped or transactions that have taken more of it pull the arena down even while the band is asking
        // for a bigger one. It keeps backing its own demand: the resource broker is never told less than the node
        // is really using.
        ui64 planned = Min(ArenaTargetLocked(used), Max(used, cap));
        if (planned > size) {
            planned = Max(size, Min(planned, cap)); // growing stops at the cap, see ArenaSizeCapLocked
        }
        return planned;
    }

    void ApplyArenaDemand(const TKqpResourcesRequest& resources, bool allocate) {
        if (!resources.ExternalMemory && !resources.ExecutionUnits) {
            return;
        }
        if (!allocate) {
            const ui64 external = ArenaExternalMemory.fetch_sub(resources.ExternalMemory);
            const ui64 units = ArenaExecutionUnits.fetch_sub(ArenaUnitsOf(resources));
            // TTxState::Released has already verified the tx part of the demand
            Y_DEBUG_ABORT_UNLESS(external >= resources.ExternalMemory);
            Y_DEBUG_ABORT_UNLESS(units >= ArenaUnitsOf(resources));
            return; // the surplus is left to the periodic pass
        }
        ArenaExternalMemory.fetch_add(resources.ExternalMemory);
        ArenaExecutionUnits.fetch_add(ArenaUnitsOf(resources));
        // a burst that overruns the arena by far is not left to the periodic pass; a resize in progress is not
        // waited for, and a growth the resource broker or the node total withheld is not asked again
        if (EnableMemoryArena.load() && !ArenaGrowWithheld.load(std::memory_order_relaxed)
            && ArenaUsed() > Arena.Size.load() + MemoryArenaMaxFreeSize.load())
        {
            if (TTryGuard<TMutex> guard(ArenaLock); guard.WasAcquired()) {
                ResizeArenaLocked(/* burst */ true);
            }
        }
    }

    // The periodic pass (TKqpResourceManagerActor::HandleAdjustArena).
    void AdjustArena() {
        with_lock (ArenaLock) {
            ResizeArenaLocked(/* burst */ false);
        }
    }

    // Under ArenaLock, held across the resource broker calls; Lock is taken only to plan and to commit. A burst
    // (ApplyArenaDemand) only grows.
    void ResizeArenaLocked(bool burst) {
        if (Arena.Stopped) {
            return;
        }
        TIntrusivePtr<IResourceBroker> broker;
        ui64 used = 0;
        ui64 target = 0;
        with_lock (Lock) {
            broker = ResourceBroker;
            used = ArenaUsed();
            target = ArenaPlanLocked(used);
        }
        ui64 size = Arena.Size.load();
        ui64 taskId = Arena.TaskId;
        if (target > size) {
            if (burst) {
                Counters->RmArenaBurstGrows->Inc();
            }
            const ui64 sizeBefore = size;
            const ui64 delta = target - size;
            const ui64 deficit = used > size ? used - size : 0;
            const ui64 granted = GrowArena(*broker, taskId, size, delta, Min(deficit, delta));
            NoteArenaGrowth(sizeBefore, delta, granted, deficit);
        } else if (target < size && !burst) {
            ShrinkArena(*broker, taskId, size, size - target);
        }
        bool publish = false;
        with_lock (Lock) {
            Arena.Size.store(size);
            Arena.TaskId = taskId;
            publish = ReconcileArenaLocked();
            // the band still wants more than the resource broker or the node total gave: the allocation path
            // leaves it to the next pass rather than ask on every request
            const bool withheld = ArenaTargetLocked(used) > size;
            ArenaGrowWithheld.store(withheld, std::memory_order_relaxed);
            if (!withheld) {
                Arena.GrowRefused = false; // stale: a later refusal is news, a later grant is not
            }
        }
        if (publish) {
            FireResourcesPublishing();
        }
    }

    // Logged on a change of outcome only, not on every refused pass.
    void NoteArenaGrowth(ui64 sizeBefore, ui64 delta, ui64 granted, ui64 deficit) {
        const bool refused = granted < delta;
        if (std::exchange(Arena.GrowRefused, refused) == refused || !ActorSystem) {
            return;
        }
        if (refused && granted) {
            YDB_LOG_NOTICE_CTX(*ActorSystem, "Memory arena growth partly granted by the resource broker",
                {"size", sizeBefore},
                {"delta", delta},
                {"granted", granted},
                {"deficit", deficit});
        } else if (refused) {
            YDB_LOG_NOTICE_CTX(*ActorSystem, "Memory arena growth refused by the resource broker",
                {"size", sizeBefore},
                {"delta", delta},
                {"granted", granted},
                {"deficit", deficit});
        } else {
            YDB_LOG_NOTICE_CTX(*ActorSystem, "Memory arena growth granted again by the resource broker",
                {"size", sizeBefore},
                {"delta", delta});
        }
    }

    // Grows by delta, or by the deficit alone when the full delta is refused. Returns what was granted.
    ui64 GrowArena(IResourceBroker& broker, ui64& taskId, ui64& size, ui64 delta, ui64 deficit) {
        const ui64 asks[2] = {delta, deficit < delta ? deficit : 0}; // the second try only when it is a different ask
        for (ui64 ask : asks) {
            if (!ask) {
                continue;
            }
            const ui64 id = LastResourceBrokerTaskId.fetch_add(1) + 1;
            const TString name = TStringBuilder() << "kqp-arena-" << id;
            if (!broker.SubmitTaskInstant(TEvResourceBroker::TEvSubmitTask(id, name, {0, ask}, NLocalDb::KqpResourceManagerTaskName, 0, {}), SelfId)) {
                continue; // refused, and the resource broker has removed the task again
            }
            if (taskId == 0) {
                taskId = id;
            } else {
                // the donor is finished by the merge; the arena task is dropped by the resource broker only after
                // StopArena, which waits for this resize
                const bool merged = broker.MergeTasksInstant(taskId, id, SelfId);
                Y_ABORT_UNLESS(merged);
            }
            size += ask;
            Counters->RmArenaGrows->Inc();
            return ask;
        }
        Counters->RmArenaGrowFailures->Inc();
        return 0;
    }

    void ShrinkArena(IResourceBroker& broker, ui64& taskId, ui64& size, ui64 by) {
        bool shrunk = false;
        if (by >= size) {
            // cancelled, not finished: the arena outlives the queries it backs, and the resource broker averages
            // the lifetime of finished tasks into the execution time it shows for the type and orders a waiting
            // queue by (TScheduler::FinishTask); the delta tasks a growth merges in are finished by the merge, as
            // the per tx ones are, but they live for the length of the merge
            shrunk = broker.FinishTaskInstant(TEvResourceBroker::TEvFinishTask(taskId, /* cancel */ true), SelfId);
            taskId = 0;
            size = 0;
        } else {
            shrunk = broker.ReduceTaskResourcesInstant(taskId, {0, by}, SelfId);
            size -= by;
        }
        Y_DEBUG_ABORT_UNLESS(shrunk);
        Counters->RmArenaShrinks->Inc();
    }

    // The resource manager outlives its actor: after PassAway the resource broker, which drops the arena task with
    // the per tx ones, is not called any more, and the demand is not charged any more.
    void StopArena() {
        with_lock (ArenaLock) {
            ArenaGrowWithheld.store(true, std::memory_order_relaxed); // for good, there is no pass any more
            with_lock (Lock) {
                Arena.Stopped = true;
                Arena.Size.store(0);
                Arena.TaskId = 0;
                ReconcileArenaLocked();
            }
        }
    }

    TActorId SelfId;

    std::atomic<ui64> QueryMemoryLimit;
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
    TIntrusivePtr<TMemoryResource> TotalMemoryResource;
    std::atomic<ui64> MaxNonParallelTopStageExecutionLimit = 1;
    std::atomic<ui64> MaxNonParallelTasksExecutionLimit = 8;
    std::atomic<bool> PreferLocalDatacenterExecution = true;
    std::atomic<ui64> MaxNonParallelDataQueryTasksLimit = 1000;

    // current state
    std::atomic<ui64> LastResourceBrokerTaskId = 0;

    // the demand, tracked without the lock; the execution units are a count, priced when it is read, so that a
    // config change re-prices the ones in use
    std::atomic<ui64> ArenaExternalMemory = 0;
    std::atomic<ui64> ArenaExecutionUnits = 0;
    // the allocation path leaves the growth to the periodic pass, see ResizeArenaLocked
    std::atomic<bool> ArenaGrowWithheld = false;

    // serializes the resizes of the arena; taken before Lock, never under it
    TMutex ArenaLock;
    struct TMemoryArena {
        // the supply: the memory of the arena task, 0 <=> TaskId == 0; written under both locks, the allocation
        // path reads it with none
        std::atomic<ui64> Size = 0;
        ui64 TaskId = 0; // written under both locks
        bool Stopped = false; // written under both locks, TKqpResourceManagerActor::PassAway
        bool GrowRefused = false; // ArenaLock: the last growth did not fully go through, see NoteArenaGrowth
        // Lock
        ui64 Charged = 0; // force-acquired from TotalMemoryResource, Max(Size, Used) after every reconcile
        // the last values published to the gauges
        ui64 ShownUsed = 0;
        ui64 ShownSize = 0;
        ui64 ShownDeficit = 0;
    };
    TMemoryArena Arena;
    std::atomic<bool> EnableMemoryArena = true;
    std::atomic<ui64> ExecutionUnitMemory = 0;
    std::atomic<ui64> MemoryArenaMinFreeSize = 0;
    std::atomic<ui64> MemoryArenaMaxFreeSize = 0;

    std::atomic_flag PublishAfterBootstrap;
    std::atomic_flag PublishScheduled;
    // pattern cache for different actors
    std::shared_ptr<NMiniKQL::TComputationPatternLRUCache> PatternCache;

    // state for resource info exchanger
    std::shared_ptr<TResourceSnapshotState> ResourceSnapshotState;
    TActorId ResourceInfoExchanger = TActorId();

    // Pool resources are never erased, not even when their usage drops to zero: the transactions of a pool keep
    // the spilling cookie attached at their construction (TTxState::PoolMemoryCookie, read lock-free), so the
    // resource that updates it has to stay the same one for as long as the pool is in use.
    absl::flat_hash_map<std::pair<TString, TString>, TIntrusivePtr<TMemoryResource>, THash<std::pair<TString, TString>>> MemoryNamedPools;
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
        std::shared_ptr<TKqpProxySharedResources>&& kqpProxySharedResources, ui32 nodeId,
        TDuration warmupDeadline)
        : NodeId(nodeId)
        , Config(config)
        , ResourceBrokerId(resourceBrokerId ? resourceBrokerId : MakeResourceBrokerID())
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
        YDB_LOG_DEBUG("Start KqpResourceManagerActor at with ResourceBroker",
            {"selfId", SelfId()},
            {"resourceBrokerId", ResourceBrokerId});

        // Subscribe for tenant changes
        Send(MakeTenantPoolRootID(), new TEvents::TEvSubscribe);

        // Subscribe for TableService config changes
        ui32 tableServiceConfigKind = (ui32) NKikimrConsole::TConfigItem::TableServiceConfigItem;

        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
             new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({tableServiceConfigKind}),
             IEventHandle::FlagTrackDelivery);

        ToBroker(new TEvResourceBroker::TEvResourceBrokerRequest);
        ToBroker(new TEvResourceBroker::TEvConfigRequest(NLocalDb::KqpResourceManagerQueue, /*subscribe=*/ true));

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

        Schedule(ArenaAdjustPeriod, new TEvPrivate::TEvAdjustArena());

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
            hFunc(TEvTenantPool::TEvTenantPoolStatus, HandleWork);
            hFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, HandleWork);
            hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, HandleWork);
            hFunc(TEvKqpWarmupComplete, HandleWarmupComplete);
            cFunc(TEvPrivate::EvWarmupDeadline, HandleWarmupDeadline);
            cFunc(TEvPrivate::EvAdjustArena, HandleAdjustArena);
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

    // Shrinks the memory arena, retries a growth that was withheld, and picks up a new resource broker, queue limit
    // or config, see TKqpResourceManager::ResizeArenaLocked.
    void HandleAdjustArena() {
        ResourceManager->AdjustArena();
        Schedule(ArenaAdjustPeriod, new TEvPrivate::TEvAdjustArena());
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
            YDB_LOG_INFO("Total node memory for scan bytes",
                {"queries", queueConfig.GetLimit().GetMemory()});
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
        FORCE_VALUE(QueryMemoryLimit)
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
                with_lock (ResourceManager->Lock) {
                    const auto& arena = ResourceManager->Arena;
                    const ui64 arenaExternal = ResourceManager->ArenaExternalMemory.load();
                    const ui64 arenaUnits = ResourceManager->ArenaExecutionUnits.load();
                    const ui64 unitMemory = ResourceManager->ExecutionUnitMemory.load();
                    const ui64 arenaUsed = arenaExternal + arenaUnits * unitMemory;
                    str << "ScanQuery memory resource: " << ResourceManager->TotalMemoryResource->ToString() << Endl;
                    str << "Memory arena: size " << arena.Size.load()
                        << ", used " << arenaUsed
                        << " (external " << arenaExternal
                        << ", execution units " << arenaUnits << " x " << unitMemory << ")"
                        << ", charged " << arena.Charged
                        << ", deficit " << ResourceManager->ArenaDeficitLocked(arenaUsed)
                        << ", broker task " << arena.TaskId
                        << (ResourceManager->EnableMemoryArena.load() ? "" : ", disabled")
                        << Endl;
                    str << "ExecutionUnits resource: " << ResourceManager->ExecutionUnitsResource.load() << Endl;
                }
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

            struct TPoolRow {
                TString Database;
                TString Pool;
                ui64 Limit;
                ui64 Used;
                ui64 DeniedRequests;
            };

            TVector<TPoolRow> pools;
            with_lock (ResourceManager->Lock) {
                pools.reserve(ResourceManager->MemoryNamedPools.size());
                for (const auto& [key, pool] : ResourceManager->MemoryNamedPools) {
                    pools.push_back({key.first, key.second, pool->GetLimit(), pool->GetUsed(), pool->GetDeniedRequests()});
                }
            }

            if (!pools.empty()) {
                str << "<h3>Memory Pools</h3>";
                str << "<table border='1' cellpadding='4'>";
                str << "<tr><th>Database</th><th>Pool</th><th>Limit</th><th>Allocated</th><th>DeniedRequests</th></tr>";
                for (const auto& row : pools) {
                    str << "<tr>"
                        << "<td>" << EncodeHtmlPcdata(row.Database) << "</td>"
                        << "<td>" << EncodeHtmlPcdata(row.Pool) << "</td>"
                        << "<td>" << row.Limit << "</td>"
                        << "<td>" << row.Used << "</td>"
                        << "<td>" << row.DeniedRequests << "</td>"
                        << "</tr>";
                }
                str << "</table>";
            }
        }

        Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
    }

private:
    void PassAway() override {
        ResourceManager->StopArena();
        ToBroker(new TEvResourceBroker::TEvNotifyActorDied);
        if (ResourceManager->ResourceInfoExchanger) {
            Send(ResourceManager->ResourceInfoExchanger, new TEvents::TEvPoison);
            ResourceManager->ResourceInfoExchanger = TActorId();
        }
        ResourceManager->ResourceSnapshotState.reset();
        TActor::PassAway();
    }

    void ToBroker(IEventBase* ev) {
        Send(ResourceBrokerId, ev);
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
            with_lock (ResourceManager->Lock) {
                payload.SetAvailableComputeActors(ResourceManager->ExecutionUnitsResource.load()); // legacy
                payload.SetTotalMemory(ResourceManager->TotalMemoryResource->GetLimit()); // legacy
                payload.SetUsedMemory(ResourceManager->TotalMemoryResource->GetLimit() - ResourceManager->TotalMemoryResource->Available()); // legacy

                payload.SetExecutionUnits(ResourceManager->ExecutionUnitsResource.load());
                auto* pool = payload.MutableMemory()->Add();
                pool->SetPool(1); // legacy ScanQuery pool id
                pool->SetAvailable(ResourceManager->TotalMemoryResource->Available());
            }
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
    static constexpr TDuration ArenaAdjustPeriod = TDuration::Seconds(1);

    const ui32 NodeId;
    NKikimrConfig::TTableServiceConfig::TResourceManager Config;

    const TActorId ResourceBrokerId;

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
    TIntrusivePtr<TKqpCounters> counters, NActors::TActorId resourceBroker,
    std::shared_ptr<TKqpProxySharedResources> kqpProxySharedResources, ui32 nodeId, TDuration warmupDeadline)
{
    return new NRm::TKqpResourceManagerActor(config, counters, resourceBroker, std::move(kqpProxySharedResources), nodeId, warmupDeadline);
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

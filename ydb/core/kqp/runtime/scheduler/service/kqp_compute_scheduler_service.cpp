#include "service.h"

#include <ydb/core/kqp/runtime/scheduler/kqp_compute_scheduler_service.h>

#include <ydb/core/kqp/runtime/scheduler/tree/dynamic.h>

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/path.h>
#include <ydb/core/cms/console/events/configs_dispatcher.h>
#include <ydb/core/cms/console/events/console.h>
#include <ydb/services/workload_manager/events.h>
#include <ydb/services/workload_manager/common/service_id.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/protos/table_service_config.pb.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/subsystems/stats.h>

#include <algorithm>
#include <cmath>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KQP_COMPUTE_SCHEDULER

using namespace NKikimr;
using namespace NKikimr::NKqp;
using namespace NKikimr::NKqp::NScheduler;
using namespace NKikimr::NKqp::NScheduler::NHdrf::NDynamic;

namespace {

bool IsValidWeight(double weight) {
    return std::isfinite(weight) && weight > 0.0;
}

class TComputeSchedulerService : public NActors::TActorBootstrapped<TComputeSchedulerService> {
public:
    explicit TComputeSchedulerService(TDuration updateFairSharePeriod) : UpdateFairSharePeriod(updateFairSharePeriod) {}

    void Bootstrap() {
        Scheduler = AppData()->KqpComputeScheduler;
        Y_ENSURE(Scheduler);

        Send(
            NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
            new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({(ui32)NKikimrConsole::TConfigItem::FeatureFlagsItem}),
            NActors::IEventHandle::FlagTrackDelivery
        );

        if (Scheduler->IsEnabled()) {
            YDB_LOG_INFO("Enabled on start");
        } else {
            YDB_LOG_INFO("Disabled on start");
        }

        Configuration = Scheduler->GetStaticTree()->Clone();
        Configuration->TotalCpuLimit = CalculateTotalCpuLimit(); // TODO: take total cpu limit from outside

        // The node's own database is known before the scheduler may be used by anyone - the state is
        // not entered yet, so no event can be handled before it is registered. The other databases
        // (serverless) are registered by the proxy service once they are resolved.
        if (const auto& tenantName = AppData()->TenantName; !tenantName.empty()) {
            Configuration->GetOrCreateDatabase(CanonizePath(tenantName));
        }
        PublishConfiguration();

        if (Scheduler->IsMemoryEnabled()) {
            YDB_LOG_INFO("Memory of the queries is accounted");
            Send(NKikimr::NMemory::MakeMemoryControllerId(), new NKikimr::NMemory::TEvConsumerRegister(NKikimr::NMemory::EMemoryConsumerKind::QueryExecution));
        }

        Become(&TComputeSchedulerService::State);
        Schedule(UpdateFairSharePeriod, new NActors::TEvents::TEvWakeup());
    }

    STATEFN(State) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, Handle);
            hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);

            hFunc(TEvAddDatabase, Handle);
            hFunc(TEvRemoveDatabase, Handle);
            hFunc(TEvAddPool, Handle);
            hFunc(NWorkloadManager::TEvUpdatePoolInfo, Handle);
            hFunc(TEvRemovePool, Handle);
            hFunc(TEvAddQuery, Handle);
            hFunc(TEvRemoveQuery, Handle);

            hFunc(NActors::TEvents::TEvWakeup, Handle);

            hFunc(NKikimr::NMemory::TEvConsumerRegistered, Handle);
            hFunc(NKikimr::NMemory::TEvConsumerLimit, Handle);

            default:
                YDB_LOG_ERROR("Unexpected",
                    {"event", ev->GetTypeRewrite()});
        }
    }

    void Handle(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr&) {
        YDB_LOG_DEBUG("Subscribed to config changes");
    }

    void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
        const auto& event = ev->Get()->Record;

        // Only the CPU is toggled at runtime - the memory is scheduled or not since the start, see IsMemoryEnabled().
        // The memory is never scheduled without the CPU, so the scheduler is not disabled till the restart.
        const bool enable = event.GetConfig().GetFeatureFlags().GetEnableResourcePoolsScheduler();
        if (!enable && Scheduler->IsMemoryEnabled()) {
            YDB_LOG_WARN("The scheduler stays enabled till the restart: the memory of the queries is scheduled");
        } else {
            Scheduler->ToggleEnabled(enable);
        }
        if (Scheduler->IsEnabled()) {
            YDB_LOG_INFO("Become enabled");
        } else {
            YDB_LOG_INFO("Become disabled");
        }

        auto responseEvent = std::make_unique<NKikimr::NConsole::TEvConsole::TEvConfigNotificationResponse>(event);
        Send(ev->Sender, responseEvent.release(), NActors::IEventHandle::FlagTrackDelivery, ev->Cookie);
    }

    void Handle(TEvAddDatabase::TPtr& ev) {
        const auto weight = ev->Get()->Weight;
        if (!IsValidWeight(weight)) {
            YDB_LOG_ERROR("Rejected invalid database weight", {"database", ev->Get()->DatabaseId}, {"weight", weight});
            return;
        }
        if (const auto* database = Configuration->GetDatabase(ev->Get()->DatabaseId); database && database->GetWeight() == weight) {
            return;
        }
        const NHdrf::TStaticAttributes attrs{.Weight = weight};
        Configuration->GetOrCreateDatabase(ev->Get()->DatabaseId).SetAttributes(attrs);
        PublishConfiguration();

        YDB_LOG_DEBUG("Add",
            {"database", ev->Get()->DatabaseId},
            {"attrs", attrs});
    }

    void Handle(TEvRemoveDatabase::TPtr&) {
        Y_ABORT("Unsupported yet");
    }

    void Handle(TEvAddPool::TPtr& ev) {
        const auto& msg = *ev->Get();
        EnsurePoolRegistered(msg.DatabaseId, msg.PoolId, msg.Params, msg.Weight);
    }

    void EnsurePoolRegistered(const TString& databaseId, const TString& poolId, const NResourcePool::TPoolSettings& params = {}, double weight = 1.) {
        if (!IsValidWeight(weight) || poolId.empty()) {
            YDB_LOG_ERROR("Rejected invalid pool registration", {"database", databaseId}, {"pool", poolId}, {"weight", weight});
            return;
        }
        if (!CheckPoolPercentages(databaseId, poolId, params)) {
            return;
        }
        const NHdrf::TFullPoolId fullPoolId{databaseId, poolId};
        if (!PoolSubscribtions.emplace(fullPoolId, false).second) {
            return;
        }

        NHdrf::TStaticAttributes attrs{.Weight = weight};
        SetCpuAttributes(params, attrs);
        SetMemoryAttributes(fullPoolId, params, attrs);

        YDB_LOG_DEBUG("Add",
            {"pool", databaseId},
            {"poolId", poolId},
            {"attrs", attrs});

        ApplyPoolConfig(databaseId, poolId, attrs);
        Send(NWorkloadManager::MakeServiceId(SelfId().NodeId()), new NWorkloadManager::TEvSubscribeOnPoolChanges(databaseId, poolId));
    }

    void Handle(TEvRemovePool::TPtr&) {
        Y_ABORT("Unsupported yet");
    }

    void Handle(NWorkloadManager::TEvUpdatePoolInfo::TPtr& ev) {
        const auto& databaseId = ev->Get()->DatabaseId;
        const auto& poolId = ev->Get()->PoolId;
        auto poolIt = PoolSubscribtions.find(NHdrf::TFullPoolId{databaseId, poolId});

        if (ev->Get()->Config) {
            Y_ENSURE(poolIt != PoolSubscribtions.end());
            if (!CheckPoolPercentages(databaseId, poolId, *ev->Get()->Config)) {
                return;
            }
            poolIt->second = false;

            // A full replacement: omitted attributes return to their defaults.
            NHdrf::TStaticAttributes attrs;
            attrs.Weight = Configuration->GetOrCreateDatabase(databaseId).GetOrCreatePool(poolId).Weight;
            PoolMemoryLimitPercents.erase(poolIt->first);
            SetCpuAttributes(*ev->Get()->Config, attrs);
            SetMemoryAttributes(poolIt->first, *ev->Get()->Config, attrs);
            ApplyPoolConfig(databaseId, poolId, attrs);

            YDB_LOG_DEBUG("Update",
                {"pool", databaseId},
                {"poolId", poolId},
                {"attrs", attrs});
        } else if (poolIt != PoolSubscribtions.end()) {
            if (!poolIt->second) {
                // The first removal - try to re-subscribe in case it's just the pool removal from cache.
                poolIt->second = true;
                Send(NWorkloadManager::MakeServiceId(SelfId().NodeId()), new NWorkloadManager::TEvSubscribeOnPoolChanges(databaseId, poolId));
            } else {
                // The second removal - the pool was really removed.
                PoolMemoryLimitPercents.erase(poolIt->first);
                PoolSubscribtions.erase(poolIt);
                Configuration->GetOrCreateDatabase(databaseId).RemovePool(poolId);
                PublishConfiguration();
            }
        } else {
            YDB_LOG_ERROR("Trying to remove unknown",
                {"pool", databaseId},
                {"poolId", poolId});
            // TODO: the removing message for unknown pool - should we check?
        }
    }

    void Handle(TEvAddQuery::TPtr& ev) {
        const auto& databaseId = ev->Get()->DatabaseId;
        const auto& poolId = ev->Get()->PoolId.empty() ? NResourcePool::DEFAULT_POOL_ID : ev->Get()->PoolId;
        const auto& queryId = ev->Get()->QueryId;

        auto response = MakeHolder<TEvQueryResponse>();
        // Register both in one actor turn so a pool update cannot remove policy between them.
        EnsurePoolRegistered(databaseId, poolId);
        if (Scheduler->IsEnabled()) {
            auto query = Scheduler->AddOrUpdateQuery(databaseId, poolId, queryId);
            response->Query = query;
            YDB_LOG_DEBUG("Add",
                {"query", databaseId},
                {"poolId", poolId},
                {"txId", queryId});
        }
        Send(ev->Sender, response.Release(), 0, queryId);
    }

    void Handle(TEvRemoveQuery::TPtr& ev) {
        const auto& queryId = ev->Get()->QueryId;
        const bool isForceRemove = ev->Get()->IsForceRemove;
        if (!Scheduler->RemoveQuery(queryId, isForceRemove)) {
            YDB_LOG_ERROR("Trying to remove unknown query",
                {"queryId", queryId},
                {"isForceRemove", isForceRemove});
        } else {
            YDB_LOG_DEBUG("Remove query",
                {"txId", queryId});
        }
    }

    void Handle(NActors::TEvents::TEvWakeup::TPtr&) {
        Scheduler->UpdateFairShare();
        if (Scheduler->IsMemoryEnabled()) {
            ReportMemoryConsumption();
            PublishMemoryState();
        }
        Schedule(UpdateFairSharePeriod, new NActors::TEvents::TEvWakeup());
    }

    void Handle(NKikimr::NMemory::TEvConsumerRegistered::TPtr& ev) {
        MemoryConsumer = std::move(ev->Get()->Consumer);
        ReportMemoryConsumption();

        YDB_LOG_INFO("Registered as memory consumer");
    }

    void Handle(NKikimr::NMemory::TEvConsumerLimit::TPtr& ev) {
        const ui64 limit = ev->Get()->LimitBytes;
        if (limit == Scheduler->GetTotalMemoryLimit()) {
            return;
        }

        YDB_LOG_INFO("Total memory limit",
            {"limit", limit},
            {"previous", Scheduler->GetTotalMemoryLimit()});

        Configuration->TotalMemoryLimit = limit;

        // The memory limits of the pools are the shares of the total limit - they follow it.
        for (const auto& [fullPoolId, memoryLimitPercent] : PoolMemoryLimitPercents) {
            Configuration->GetOrCreateDatabase(fullPoolId.DatabaseId).GetOrCreatePool(fullPoolId.PoolId).MemoryLimit = ToMemoryLimit(memoryLimitPercent);
        }
        PublishConfiguration();
    }

private:
    ui64 CalculateTotalCpuLimit() {
        auto poolId = SelfId().PoolID();
        NActors::TExecutorPoolStats poolStats;
        TVector<NActors::TExecutorThreadStats> threadsStats;
        NActors::GetActorSystemStats().GetPoolStats(poolId, poolStats, threadsStats);
        return Max<ui64>(poolStats.MaxThreadCount, 1);
    }

    // Check the raw settings before converting to integers or changing desired policy and subscriptions.
    bool CheckPoolPercentages(const TString& databaseId, const TString& poolId, const NResourcePool::TPoolSettings& config) const {
        const auto valid = [](double value) { return value == -1 || (value >= 0 && value <= 100); };
        if (valid(config.TotalCpuLimitPercentPerNode) && valid(config.TotalCpuGuaranteePercentPerNode)
            && valid(config.TotalMemoryLimitPercentPerNode)) {
            return true;
        }
        YDB_LOG_ERROR("Rejected invalid pool percentages", {"database", databaseId}, {"pool", poolId},
            {"cpuLimitPercent", config.TotalCpuLimitPercentPerNode}, {"cpuGuaranteePercent", config.TotalCpuGuaranteePercentPerNode},
            {"memoryLimitPercent", config.TotalMemoryLimitPercentPerNode});
        return false;
    }

    // All fields describe the new configuration; -1 leaves an attribute unset.
    void SetCpuAttributes(const NResourcePool::TPoolSettings& config, NHdrf::TStaticAttributes& attrs) const {
        const auto totalCpuLimit = Configuration->TotalCpuLimit;

        if (const auto& cpuLimitPercent = config.TotalCpuLimitPercentPerNode; cpuLimitPercent >= 0) {
            if (cpuLimitPercent == 0) {
                attrs.CpuLimit = 0;
                attrs.ReadLimit = TDuration::Zero();
            } else {
                attrs.CpuLimit = std::max<ui64>(1, cpuLimitPercent * totalCpuLimit / 100);
                attrs.ReadLimit = TDuration::MilliSeconds(cpuLimitPercent * 10);
            }
        }

        if (config.TotalCpuGuaranteePercentPerNode >= 0) {
            attrs.CpuGuarantee = static_cast<ui64>(config.TotalCpuGuaranteePercentPerNode * totalCpuLimit / 100);
        }
    }

    // As SetCpuAttributes. The percent is remembered, since the limit follows the total memory limit.
    void SetMemoryAttributes(const NHdrf::TFullPoolId& fullPoolId, const NResourcePool::TPoolSettings& config,  NHdrf::TStaticAttributes& attrs) {
        if (!Scheduler->IsMemoryEnabled()) {
            return;
        }
        if (const auto& memoryLimitPercent = config.TotalMemoryLimitPercentPerNode; memoryLimitPercent >= 0) {
            PoolMemoryLimitPercents[fullPoolId] = memoryLimitPercent;
            attrs.MemoryLimit = ToMemoryLimit(memoryLimitPercent);
        }
    }

    ui64 ToMemoryLimit(double memoryLimitPercent) const {
        const auto totalMemoryLimit = Configuration->TotalMemoryLimit;
        return totalMemoryLimit == NHdrf::Infinity()
            ? NHdrf::Infinity()
            : static_cast<ui64>(std::clamp(memoryLimitPercent, 0.0, 100.0) / 100 * totalMemoryLimit);
    }

    // The demand is the expected memory of the queries - but never less than the one they already use.
    void ReportMemoryConsumption() {
        if (MemoryConsumer) {
            const ui64 usage = Scheduler->GetTotalMemoryUsage();
            MemoryConsumer->SetReport({
                .Used = usage,
                .Demand = Max(usage, Scheduler->GetTotalMemoryDemand()),
            });
        }
    }

    // The resource manager publishes the memory of the queries to the other nodes
    void PublishMemoryState() {
        const ui64 limit = Scheduler->GetTotalMemoryLimit();
        const ui64 usage = Scheduler->GetTotalMemoryUsage();
        if (limit != PublishedMemoryLimit || usage != PublishedMemoryUsage) {
            Send(MakeKqpRmServiceID(SelfId().NodeId()), new NRm::TEvQueryMemoryState(limit, usage));
            PublishedMemoryLimit = limit;
            PublishedMemoryUsage = usage;
        }
    }

    void ApplyPoolConfig(const TString& databaseId, const TString& poolId, const NHdrf::TStaticAttributes& attrs) {
        Configuration->GetOrCreateDatabase(databaseId).GetOrCreatePool(poolId).SetAttributes(attrs);
        PublishConfiguration();
    }

    // Keep the desired configuration intact, so every update retries the complete set of
    // reservations, including updates delivered before a sibling released its guarantee.
    void PublishConfiguration() {
        try {
            if (!Scheduler->UpdateConfiguration(Configuration->Clone())) {
                YDB_LOG_WARN("CPU guarantees are disabled for multiple databases");
            }
            return;
        } catch (const TCpuGuaranteeError& e) {
            YDB_LOG_ERROR("Rejected guarantees in the full configuration",
                {"error", TString(e.what())});
        }

        auto fallback = Configuration->Clone();
        auto clearGuarantees = [&](auto self, NHdrf::NStatic::TPool& pool) -> void {
            pool.CpuGuarantee.reset();
            pool.ForEachChild<NHdrf::NStatic::TPool>([&](NHdrf::NStatic::TPool* child, size_t) {
                self(self, *child);
            });
        };
        clearGuarantees(clearGuarantees, *fallback);
        Scheduler->UpdateConfiguration(std::move(fallback));
    }

private:
    TComputeSchedulerPtr Scheduler;
    std::unique_ptr<NHdrf::NStatic::TRoot> Configuration; // actor-owned desired policy, including pending reservations
    TIntrusivePtr<NKikimr::NMemory::IMemoryConsumer> MemoryConsumer;
    const TDuration UpdateFairSharePeriod;
    THashMap<NHdrf::TFullPoolId, bool /* IsFirstRemoval */> PoolSubscribtions;
    THashMap<NHdrf::TFullPoolId, double> PoolMemoryLimitPercents;
    ui64 PublishedMemoryLimit = 0;
    ui64 PublishedMemoryUsage = 0;
};

} // namespace

namespace NKikimr::NKqp {

IActor* CreateKqpComputeSchedulerService(TDuration updateFairSharePeriod) {
    Y_ENSURE(updateFairSharePeriod > TDuration::Zero());
    return new TComputeSchedulerService(updateFairSharePeriod);
}

} // namespace NKikimr::NKqp

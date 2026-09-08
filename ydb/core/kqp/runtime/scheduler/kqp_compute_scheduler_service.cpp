#include "kqp_compute_scheduler_service.h"

#include "tree/dynamic.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/services/workload_manager/events.h>
#include <ydb/services/workload_manager/service/service.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/protos/table_service_config.pb.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/subsystems/stats.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KQP_COMPUTE_SCHEDULER

using namespace NKikimr;
using namespace NKikimr::NKqp;
using namespace NKikimr::NKqp::NScheduler;
using namespace NKikimr::NKqp::NScheduler::NHdrf::NDynamic;

namespace {

using TDynamicElement = NHdrf::TTreeElementBase<NHdrf::ETreeType::DYNAMIC>;

// Validates the final (merged) configuration which is about to be applied to a tree element.
// `element` is the element itself when it already exists, `parent` is its parent-to-be - passing
// no parent means that the element is not validated against the parent's guarantee at all.
void ValidateAttributes(const NHdrf::TStaticAttributes& attrs, const TDynamicElement* element, const TDynamicElement* parent) {
    // Validate weight
    Y_ENSURE(attrs.GetWeight() > 0.0, "Weight should be positive");

    // Validate guarantee
    if (attrs.CpuGuarantee) {
        const auto guarantee = attrs.GetCpuGuarantee();

        Y_ENSURE(guarantee <= attrs.GetCpuLimit(),
            "CpuGuarantee (" << guarantee << ") should not exceed CpuLimit (" << attrs.GetCpuLimit() << ")");

        // A zero guarantee reserves nothing from the parent - resetting is always allowed
        if (parent && guarantee > 0) {
            Y_ENSURE(parent->CpuGuarantee, "Child cannot set CpuGuarantee until the parent's guarantee is set");

            // Calculate unreserved parent guarantee excluding `element`
            ui64 unreserved = *parent->CpuGuarantee;

            parent->ForEachChild<TDynamicElement>([&](const TDynamicElement* child, size_t) {
                if (child != element) {
                    // TODO: replace with std::sub_sat() in C++26
                    const auto guarantee = child->GetCpuGuarantee();
                    unreserved = unreserved > guarantee ? unreserved - guarantee : 0;
                }
            });

            Y_ENSURE(guarantee <= unreserved,
                "CpuGuarantee (" << guarantee << ") exceeds the guarantee left by the parent (" << unreserved << ")");
        }

        if (element) {
            const auto reserved = element->GetChildrenCpuGuarantee();
            Y_ENSURE(guarantee >= reserved,
                "CpuGuarantee (" << guarantee << ") is less than the guarantees already reserved by children (" << reserved << ")");
        }
    }
}

class TComputeSchedulerService : public NActors::TActorBootstrapped<TComputeSchedulerService> {
public:
    explicit TComputeSchedulerService(const TDuration& updateFairSharePeriod) : UpdateFairSharePeriod(updateFairSharePeriod) {}

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

        Scheduler->SetTotalCpuLimit(CalculateTotalCpuLimit()); // TODO: take total cpu limit from outside

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

        Scheduler->ToggleEnabled(event.GetConfig().GetFeatureFlags().GetEnableResourcePoolsScheduler());
        if (Scheduler->IsEnabled()) {
            YDB_LOG_INFO("Become enabled");
        } else {
            YDB_LOG_INFO("Become disabled");
        }

        auto responseEvent = std::make_unique<NKikimr::NConsole::TEvConsole::TEvConfigNotificationResponse>(event);
        Send(ev->Sender, responseEvent.release(), NActors::IEventHandle::FlagTrackDelivery, ev->Cookie);
    }

    void Handle(TEvAddDatabase::TPtr& ev) {
        NHdrf::TStaticAttributes const attrs {
            .Weight = ev->Get()->Weight, // TODO: weight shouldn't be negative!
            .CpuGuarantee = Scheduler->GetTotalCpuLimit(), // TODO: set database guarantee properly in the future
        };
        Scheduler->AddOrUpdateDatabase(ev->Get()->DatabaseId, attrs);

        YDB_LOG_DEBUG("Add",
            {"database", ev->Get()->DatabaseId},
            {"attrs", attrs});
    }

    void Handle(TEvRemoveDatabase::TPtr&) {
        Y_ABORT("Unsupported yet");
    }

    void Handle(TEvAddPool::TPtr& ev) {
        const auto& databaseId = ev->Get()->DatabaseId;
        const auto& poolId = ev->Get()->PoolId;
        NHdrf::TStaticAttributes attrs = {
            .Weight = ev->Get()->Weight, // TODO: weight shouldn't be negative!
        };

        SetCpuAttributes(ev->Get()->Params, attrs);

        Y_ASSERT(!poolId.empty());

        YDB_LOG_DEBUG("Add",
            {"pool", databaseId},
            {"poolId", poolId},
            {"attrs", attrs});

        if (PoolSubscribtions.emplace(NHdrf::TFullPoolId{.DatabaseId=databaseId, .PoolId=poolId}, false).second) {
            ApplyPoolConfig(databaseId, poolId, attrs);
            Send(NWorkloadManager::MakeServiceId(SelfId().NodeId()), new NWorkloadManager::TEvSubscribeOnPoolChanges(databaseId, poolId));
        }
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
            poolIt->second = false;

            NHdrf::TStaticAttributes attrs;
            SetCpuAttributes(*ev->Get()->Config, attrs);
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
                PoolSubscribtions.erase(poolIt);
                // TODO: Scheduler->RemovePool(…);
                // TODO: Scheduler->UpdatePool(…);
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
        const auto& poolId = ev->Get()->PoolId;
        const auto& queryId = ev->Get()->QueryId;
        NHdrf::TStaticAttributes const attrs {
            .Weight = ev->Get()->Weight, // TODO: weight shouldn't be negative!
        };

        auto response = MakeHolder<TEvQueryResponse>();
        if (Scheduler->IsEnabled()) {
            auto query = Scheduler->AddOrUpdateQuery(databaseId, poolId.empty() ? NKikimr::NResourcePool::DEFAULT_POOL_ID : poolId, queryId, attrs);
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
        if (!Scheduler->RemoveQuery(queryId)) {
            YDB_LOG_ERROR("Trying to remove unknown",
                {"query", queryId});
        } else {
            YDB_LOG_DEBUG("Remove query",
                {"txId", queryId});
        }
    }

    void Handle(NActors::TEvents::TEvWakeup::TPtr&) {
        Scheduler->UpdateFairShare();
        Schedule(UpdateFairSharePeriod, new NActors::TEvents::TEvWakeup());
    }

private:
    ui64 CalculateTotalCpuLimit() {
        auto poolId = SelfId().PoolID();
        NActors::TExecutorPoolStats poolStats;
        TVector<NActors::TExecutorThreadStats> threadsStats;
        NActors::GetActorSystemStats().GetPoolStats(poolId, poolStats, threadsStats);
        return Max<ui64>(poolStats.MaxThreadCount, 1);
    }

    // Update limit and guarantee - they are applied together,
    // since lowering the limit of a pool has to lower its guarantee as well.
    // A limit percent of -1 means that the setting is not configured, so the previous value is kept.
    // The guarantee is different: the config always carries the whole pool description, so -1 means
    // that the pool has no guarantee anymore and the reservation it holds has to be released.
    void SetCpuAttributes(const NResourcePool::TPoolSettings& config, NHdrf::TStaticAttributes& attrs) const {
        const auto totalCpuLimit = Scheduler->GetTotalCpuLimit();

        if (const auto& cpuLimitPercent = config.TotalCpuLimitPercentPerNode; cpuLimitPercent >= 0) {
            if (cpuLimitPercent == 0) {
                attrs.CpuLimit = 0;
                attrs.ReadLimit = TDuration::Zero();
            } else {
                attrs.CpuLimit = std::max<ui64>(1, cpuLimitPercent * totalCpuLimit / 100);
                attrs.ReadLimit = TDuration::MilliSeconds(cpuLimitPercent * 10);
            }
        }

        const auto cpuGuaranteePercent = std::max(config.TotalCpuGuaranteePercentPerNode, 0.0);
        attrs.CpuGuarantee = cpuGuaranteePercent * totalCpuLimit / 100;
    }

    // TODO: handle invalid configuration on DDL level.
    void ApplyPoolConfig(const TString& databaseId, const TString& poolId, NHdrf::TStaticAttributes attrs) {
        try {
            Scheduler->AddOrUpdatePool(databaseId, poolId, attrs);
        } catch (const std::exception& e) {
            YDB_LOG_ERROR("Rejected guarantee",
                {"pool", databaseId},
                {"poolId", poolId},
                {"attrs", attrs},
                {"error", TString(e.what())});

            attrs.CpuGuarantee = 0;
            Scheduler->AddOrUpdatePool(databaseId, poolId, attrs);
        }
    }

private:
    TComputeSchedulerPtr Scheduler;
    const TDuration UpdateFairSharePeriod;
    THashMap<NHdrf::TFullPoolId, bool /* IsFirstRemoval */> PoolSubscribtions;
};

} // namespace

namespace NKikimr::NKqp {

namespace NScheduler {

TComputeScheduler::TComputeScheduler(const TIntrusivePtr<TKqpCounters>& counters, const TOptions& options)
    : Enabled(options.Enabled)
    , Root(std::make_shared<TRoot>(counters))
    , DelayParams(options.DelayParams)
    , FairShareMode(options.FairShareMode)
    , KqpCounters(counters)
{
    auto group = counters->GetKqpCounters();
    Counters.UpdateFairShare = group->GetCounter("scheduler/UpdateFairShare", true);
}

void TComputeScheduler::SetTotalCpuLimit(ui64 cpu) {
    Root->TotalLimit = cpu;
    Root->CpuGuarantee = cpu;
}

ui64 TComputeScheduler::GetTotalCpuLimit() const {
    return Root->TotalLimit;
}

void TComputeScheduler::AddOrUpdateDatabase(const TString& databaseId, const NHdrf::TStaticAttributes& attrs) {
    TWriteGuard lock(Mutex);

    auto database = Root->GetDatabase(databaseId);

    // Databases are intentionally not validated against the root's guarantee.
    ValidateAttributes(database ? database->MergedWith(attrs) : attrs, database.get(), nullptr);

    if (database) {
        database->Update(attrs);
    } else {
        Root->AddDatabase(std::make_shared<TDatabase>(databaseId, attrs));
    }
}

void TComputeScheduler::AddOrUpdatePool(const TString& databaseId, const TString& poolId, const NHdrf::TStaticAttributes& attrs) {
    Y_ENSURE(!poolId.empty());

    TWriteGuard lock(Mutex);
    auto database = Root->GetDatabase(databaseId);
    Y_ENSURE(database, "Database not found: " << databaseId);

    auto pool = database->GetPool(poolId);
    ValidateAttributes(pool ? pool->MergedWith(attrs) : attrs, pool.get(), database.get());

    if (pool) {
        pool->Update(attrs);
    } else {
        pool = std::make_shared<TPool>(poolId, KqpCounters, attrs);
        database->AddPool(pool);

        bool allowMinFairShare = (!pool->CpuLimit || *pool->CpuLimit > 0)
            && (FairShareMode >= NHdrf::NSnapshot::ELeafFairShare::ALLOW_OVERLIMIT);

        // Since they are not visible by query id - use the same id for each pool
        auto query = std::make_shared<TQuery>(READ_QUERY_ID, &DelayParams, allowMinFairShare, NHdrf::TStaticAttributes());
        pool->AddQuery(query);

        // Add read query
        ReadQueries.emplace(NHdrf::TFullPoolId{databaseId, poolId}, query);
    }
}

TQueryPtr TComputeScheduler::AddOrUpdateQuery(const NHdrf::TDatabaseId& databaseId, const NHdrf::TPoolId& poolId, const NHdrf::TQueryId& queryId, const NHdrf::TStaticAttributes& attrs) {
    Y_ENSURE(!poolId.empty());

    TWriteGuard lock(Mutex);
    auto database = Root->GetDatabase(databaseId);
    Y_ENSURE(database, "Database not found: " << databaseId);
    auto pool = database->GetPool(poolId);
    Y_ENSURE(pool, "Pool not found: " << poolId);

    TQueryPtr query = std::static_pointer_cast<TQuery>(pool->GetQuery(queryId));
    ValidateAttributes(query ? query->MergedWith(attrs) : attrs, query.get(), pool.get());

    if (query) {
        query->Update(attrs);
    } else {
        bool allowMinFairShare = (!pool->CpuLimit || *pool->CpuLimit > 0)
            && (FairShareMode >= NHdrf::NSnapshot::ELeafFairShare::ALLOW_OVERLIMIT);
        query = std::make_shared<TQuery>(queryId, &DelayParams, allowMinFairShare, attrs);
        pool->AddQuery(query);
        Y_ENSURE(Queries.emplace(queryId, query).second);
    }

    return query;
}

NHdrf::NDynamic::TQueryPtr TComputeScheduler::GetReadQuery(const NHdrf::TDatabaseId& databaseId, const NHdrf::TPoolId& poolId) const {
    if (!IsEnabled()) {
        return {};
    }

    TReadGuard lock(Mutex);

    if (auto queryIt = ReadQueries.find(NHdrf::TFullPoolId{databaseId, poolId}); queryIt != ReadQueries.end()) {
        return queryIt->second;
    }

    return {};
}

bool TComputeScheduler::RemoveQuery(const NHdrf::TQueryId& queryId) {
    TWriteGuard lock(Mutex);

    if (auto queryIt = Queries.find(queryId); queryIt != Queries.end()) {
        queryIt->second->GetParent()->RemoveQuery(queryId);
        Queries.erase(queryIt);
        return true;
    }

    return false;
}

THashMap<NHdrf::TFullPoolId, double> TComputeScheduler::GetLeafPoolFairShares() const {
    THashMap<NHdrf::TFullPoolId, double> result;
    const auto totalCpu = GetTotalCpuLimit();
    if (!totalCpu) {
        return result;
    }
    auto snapshot = Root->GetSnapshot();
    if (!snapshot) {
        return result;
    }

    auto visitPool = [&](auto self, const NHdrf::TDatabaseId& databaseId, const auto* pool) -> void {
        if (pool->IsLeaf()) {
            NHdrf::TFullPoolId fullPoolId{databaseId, std::get<NHdrf::TPoolId>(pool->GetId())};
            result[fullPoolId] = double(pool->FairShare) / totalCpu;
        } else {
            pool->template ForEachChild<NHdrf::NSnapshot::TPool>([&](auto* child, size_t) {
                self(self, databaseId, child);
            });
        }
    };

    snapshot->ForEachChild<NHdrf::NSnapshot::TDatabase>([&](auto* database, size_t) {
        const auto& databaseId = std::get<NHdrf::TDatabaseId>(database->GetId());
        database->template ForEachChild<NHdrf::NSnapshot::TPool>([&](auto* pool, size_t) {
            visitPool(visitPool, databaseId, pool);
        });
    });
    return result;
}

void TComputeScheduler::UpdateFairShare() {
    auto startTime = TMonotonic::Now();

    NHdrf::NSnapshot::TRootPtr snapshot;
    {
        TReadGuard lock(Mutex);
        snapshot = NHdrf::NSnapshot::TRootPtr(Root->TakeSnapshot());
    }

    snapshot->UpdateBottomUp(Root->TotalLimit);
    snapshot->UpdateTopDown(FairShareMode);

    {
        TWriteGuard lock(Mutex);
        if (auto oldSnapshot = Root->SetSnapshot(snapshot)) {
            snapshot->AccountPreviousSnapshot(oldSnapshot);
        }
    }

    Counters.UpdateFairShare->Add((TMonotonic::Now() - startTime).MicroSeconds());
}

} // namespace NScheduler

NScheduler::TComputeSchedulerPtr CreateKqpComputeScheduler(const NMonitoring::TDynamicCounterPtr& counters, const NKikimrConfig::TAppConfig& appConfig) {
    const auto& schedulerSettings = appConfig.GetTableServiceConfig().GetComputeSchedulerSettings();

    auto options = TOptions{
        .Enabled = appConfig.GetFeatureFlags().GetEnableResourcePoolsScheduler(),
        .DelayParams = TDelayParams{
            .MaxDelay = TDuration::MicroSeconds(schedulerSettings.GetMaxTaskDelayUs()),
            .MinDelay = TDuration::MicroSeconds(schedulerSettings.GetMinTaskDelayUs()),
            .AttemptBonus = TDuration::MicroSeconds(schedulerSettings.GetAttemptTaskBonusUs()),
            .MaxRandomDelay = TDuration::MicroSeconds(schedulerSettings.GetMaxTaskRandomDelayUs()),
        }
    };

    Y_ENSURE(options.DelayParams.MaxDelay > TDuration::Zero());
    Y_ENSURE(options.DelayParams.MinDelay > TDuration::Zero());
    Y_ENSURE(options.DelayParams.AttemptBonus > TDuration::Zero());
    Y_ENSURE(options.DelayParams.MaxRandomDelay > TDuration::Zero());
    return std::make_shared<NScheduler::TComputeScheduler>(MakeIntrusive<NKqp::TKqpCounters>(counters), options);
}

IActor* CreateKqpComputeSchedulerService(const TDuration& updateFairSharePeriod) {
    Y_ENSURE(updateFairSharePeriod > TDuration::Zero());
    return new TComputeSchedulerService(updateFairSharePeriod);
}

} // namespace NKikimr::NKqp

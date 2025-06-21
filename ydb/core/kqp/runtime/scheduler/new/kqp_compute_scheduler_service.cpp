#include "kqp_compute_scheduler_service.h"

#include "tree/dynamic.h"
#include "kqp_schedulable_actor.h"

#include <ydb/core/kqp/common/events/workload_service.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>

using namespace NKikimr::NKqp;
using namespace NKikimr::NKqp::NScheduler;
using namespace NKikimr::NKqp::NScheduler::NHdrf::NDynamic;

namespace {

constexpr double Epsilon = 1e-8;

class TComputeSchedulerService : public NActors::TActorBootstrapped<TComputeSchedulerService> {
public:
    explicit TComputeSchedulerService(const NScheduler::TComputeSchedulerPtr& scheduler, const NScheduler::TOptions& options)
        : Scheduler(scheduler)
        , UpdateFairSharePeriod(options.UpdateFairSharePeriod)
    {}

    void Bootstrap() {
        Scheduler->SetTotalCpuLimit(CalculateTotalCpuLimit()); // TODO: take total cpu limit from outside

        Become(&TComputeSchedulerService::State);
        Schedule(UpdateFairSharePeriod, new NActors::TEvents::TEvWakeup());
    }

    STATEFN(State) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvAddDatabase, Handle);
            hFunc(TEvRemoveDatabase, Handle);
            hFunc(TEvAddPool, Handle);
            hFunc(NWorkload::TEvUpdatePoolInfo, Handle);
            hFunc(TEvRemovePool, Handle);
            hFunc(TEvAddQuery, Handle);
            hFunc(TEvRemoveQuery, Handle);

            hFunc(NActors::TEvents::TEvWakeup, Handle);

            default:
                Y_ABORT("Unexpected event for TComputeSchedulerService: %u", ev->GetTypeRewrite());
        }
    }

    void Handle(TEvAddDatabase::TPtr& ev) {
        NHdrf::TStaticAttributes const attrs {
            .Weight = std::max(ev->Get()->Weight, 0.0), // TODO: weight shouldn't be negative!
        };
        Scheduler->AddOrUpdateDatabase(ev->Get()->Id, attrs);
    }

    void Handle(TEvRemoveDatabase::TPtr&) {
        Y_ABORT("Unsupported yet");
    }

    void Handle(TEvAddPool::TPtr& ev) {
        const auto& databaseId = ev->Get()->DatabaseId;
        const auto& poolId = ev->Get()->PoolId;
        const auto resourceWeight = std::max(ev->Get()->Params.ResourceWeight, 0.0); // TODO: resource weight shouldn't be negative!
        NHdrf::TStaticAttributes const attrs = {
            .Limit = ev->Get()->Params.TotalCpuLimitPercentPerNode * Scheduler->GetTotalCpuLimit() / 100,
            .Weight = std::max(ev->Get()->Weight, 0.0), // TODO: weight shouldn't be negative!
        };

        Y_ASSERT(!poolId.empty());

        if (PoolSubscribtions.insert({std::make_pair(databaseId, poolId), {false, resourceWeight}}).second) {
            PoolExternalWeightSum += resourceWeight;
            Scheduler->AddOrUpdatePool(databaseId, poolId, attrs);
            Send(MakeKqpWorkloadServiceId(SelfId().NodeId()), new NWorkload::TEvSubscribeOnPoolChanges(databaseId, poolId));
            if (resourceWeight > Epsilon) {
                UpdatePoolsGuarantee();
            }
        }
    }

    void Handle(TEvRemovePool::TPtr&) {
        Y_ABORT("Unsupported yet");
    }

    void Handle(NWorkload::TEvUpdatePoolInfo::TPtr& ev) {
        const auto& databaseId = ev->Get()->DatabaseId;
        const auto& poolId = ev->Get()->PoolId;
        auto poolIt = PoolSubscribtions.find(std::make_pair(databaseId, poolId));

        if (ev->Get()->Config) {
            Y_ENSURE(poolIt != PoolSubscribtions.end());
            poolIt->second.IsFirstRemoval = false;

            // Update external weight
            PoolExternalWeightSum -= poolIt->second.ExternalWeight;
            poolIt->second.ExternalWeight = ev->Get()->Config->ResourceWeight;
            PoolExternalWeightSum += poolIt->second.ExternalWeight;
            UpdatePoolsGuarantee();

            // Update limit
            Scheduler->AddOrUpdatePool(databaseId, poolId, {
                .Limit = ev->Get()->Config->TotalCpuLimitPercentPerNode * Scheduler->GetTotalCpuLimit() / 100
            });
        } else if (poolIt != PoolSubscribtions.end()) {
            if (!poolIt->second.IsFirstRemoval) {
                // The first removal - try to re-subscribe in case it's just the pool removal from cache.
                poolIt->second.IsFirstRemoval = true;
                Send(MakeKqpWorkloadServiceId(SelfId().NodeId()), new NWorkload::TEvSubscribeOnPoolChanges(databaseId, poolId));
            } else {
                // The second removal - the pool was really removed.
                PoolSubscribtions.erase(poolIt);
                // TODO: Scheduler->RemovePool(…);
                // TODO: Scheduler->UpdatePool(…);
            }
        } else {
            // TODO: the removing message for unknown pool - should we check?
        }
    }

    void Handle(TEvAddQuery::TPtr& ev) {
        const auto& databaseId = ev->Get()->DatabaseId;
        const auto& poolId = ev->Get()->PoolId;
        const auto& queryId = ev->Get()->QueryId;
        NHdrf::TStaticAttributes const attrs {
            .Weight = std::max(ev->Get()->Weight, 0.0), // TODO: weight shouldn't be negative!
        };

        Scheduler->AddOrUpdateQuery(databaseId, poolId.empty() ? NKikimr::NResourcePool::DEFAULT_POOL_ID : poolId, queryId, attrs);
    }

    void Handle(TEvRemoveQuery::TPtr& ev) {
        const auto& databaseId = ev->Get()->DatabaseId;
        const auto& poolId = ev->Get()->PoolId;
        const auto& queryId = ev->Get()->QueryId;

        Scheduler->RemoveQuery(databaseId, poolId, queryId);
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
        NActors::TlsActivationContext->ActorSystem()->GetPoolStats(poolId, poolStats, threadsStats);
        return Max<ui64>(poolStats.MaxThreadCount, 1);
    }

    void UpdatePoolsGuarantee() {
        if (PoolExternalWeightSum <= Epsilon) {
            for (const auto& [key, _] : PoolSubscribtions) {
                Scheduler->AddOrUpdatePool(key.first, key.second, {.Guarantee = 0});
            }
        } else {
            for (const auto& [key, params] : PoolSubscribtions) {
                Scheduler->AddOrUpdatePool(key.first, key.second,
                    {.Guarantee = params.ExternalWeight / PoolExternalWeightSum * Scheduler->GetTotalCpuLimit()});
            }
        }
    }

private:
    TComputeSchedulerPtr Scheduler;
    const TDuration UpdateFairSharePeriod;

    struct TPoolParams {
        bool IsFirstRemoval = false;
        double ExternalWeight = 0.0;
    };
    THashMap<std::pair<TString /* databaseId */, TString /* poolId */>, TPoolParams> PoolSubscribtions;
    double PoolExternalWeightSum = 0.0;
};

} // namespace

namespace NKikimr::NKqp {

namespace NScheduler {

TComputeScheduler::TComputeScheduler(TIntrusivePtr<TKqpCounters> counters)
    : Root(std::make_shared<TRoot>(counters))
    , KqpCounters(counters)
{
    DetachedPool = std::make_shared<TPool>("(detached)", counters);

    auto group = counters->GetKqpCounters();
    Counters.UpdateFairShare = group->GetCounter("scheduler/UpdateFairShare", true);
}

TSchedulableTaskFactory TComputeScheduler::CreateSchedulableTaskFactory() {
    return [ptr = this->shared_from_this()](const NHdrf::TQueryId& queryId) {
        return MakeHolder<TSchedulableTask>(ptr->GetQuery(queryId));
    };
}

void TComputeScheduler::SetTotalCpuLimit(ui64 cpu) {
    Root->TotalLimit = cpu;
}

ui64 TComputeScheduler::GetTotalCpuLimit() const {
    return Root->TotalLimit;
}

void TComputeScheduler::AddOrUpdateDatabase(const TString& databaseId, const NHdrf::TStaticAttributes& attrs) {
    TWriteGuard lock(Mutex);

    if (auto database = Root->GetDatabase(databaseId)) {
        database->Update(attrs);
    } else {
        Root->AddDatabase(std::make_shared<TDatabase>(databaseId, attrs));
    }
}

void TComputeScheduler::AddOrUpdatePool(const TString& databaseId, const TString& poolId, const NHdrf::TStaticAttributes& attrs) {
    Y_ENSURE(!poolId.empty());

    TWriteGuard lock(Mutex);
    auto database = Root->GetDatabase(databaseId);
    Y_ENSURE(database);

    if (auto pool = database->GetPool(poolId)) {
        pool->Update(attrs);
    } else {
        database->AddPool(std::make_shared<TPool>(poolId, KqpCounters, attrs));
    }
}

void TComputeScheduler::AddOrUpdateQuery(const TString& databaseId, const TString& poolId, const NHdrf::TQueryId& queryId, const NHdrf::TStaticAttributes& attrs) {
    Y_ENSURE(!poolId.empty());

    TWriteGuard lock(Mutex);
    auto database = Root->GetDatabase(databaseId);
    Y_ENSURE(database);
    auto pool = database->GetPool(poolId);
    Y_ENSURE(pool);

    TQueryPtr query;
    if (auto queryIt = DetachedQueries.find(queryId); queryIt != DetachedQueries.end()) {
        query = queryIt->second;
        query->Update(attrs);
        DetachedQueries.erase(queryIt);
        DetachedPool->RemoveQuery(queryId);
        pool->AddQuery(query);
        Y_ENSURE(Queries.emplace(queryId, query).second);
    } else if (query = pool->GetQuery(queryId)) {
        query->Update(attrs);
    }

    if (!query) {
        query = std::make_shared<TQuery>(queryId, attrs);
        pool->AddQuery(query);
        Y_ENSURE(Queries.emplace(queryId, query).second);
    }
}

void TComputeScheduler::RemoveQuery(const TString& databaseId, const TString& poolId, const NHdrf::TQueryId& queryId) {
    TWriteGuard lock(Mutex);

    auto database = Root->GetDatabase(databaseId);
    auto pool = database->GetPool(poolId);
    pool->RemoveQuery(queryId);
    Queries.erase(queryId);
}

void TComputeScheduler::UpdateFairShare() {
    auto startTime = TMonotonic::Now();

    NHdrf::NSnapshot::TRootPtr snapshot;
    {
        TReadGuard lock(Mutex);
        snapshot = NHdrf::NSnapshot::TRootPtr(Root->TakeSnapshot());
    }

    snapshot->UpdateBottomUp(Root->TotalLimit);
    snapshot->UpdateTopDown();

    {
        TWriteGuard lock(Mutex);
        if (auto oldSnapshot = Root->SetSnapshot(snapshot)) {
            snapshot->AccountFairShare(oldSnapshot);
        }
    }

    Counters.UpdateFairShare->Add((TMonotonic::Now() - startTime).MicroSeconds());
}

TQueryPtr TComputeScheduler::GetQuery(const NHdrf::TQueryId& queryId) {
    {
        TReadGuard lock(Mutex);
        if (auto queryIt = Queries.find(queryId); queryIt != Queries.end()) {
            return queryIt->second;
        }
        if (auto queryIt = DetachedQueries.find(queryId); queryIt != DetachedQueries.end()) {
            return queryIt->second;
        }
    }

    TWriteGuard lock(Mutex);
    auto [query, isEmplaced] = DetachedQueries.emplace(queryId, std::make_shared<TQuery>(queryId));
    if (isEmplaced) {
        DetachedPool->AddQuery(query->second);
    }

    return query->second;
}

} // namespace NScheduler

IActor* CreateKqpComputeSchedulerService(const NScheduler::TComputeSchedulerPtr& scheduler, const NScheduler::TOptions& options) {
    return new TComputeSchedulerService(scheduler, options);
}

} // namespace NKikimr::NKqp

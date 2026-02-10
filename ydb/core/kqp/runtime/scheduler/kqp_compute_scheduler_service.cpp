#include "kqp_compute_scheduler_service.h"

#include "tree/dynamic.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/kqp/common/events/workload_service.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>

#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE_SCHEDULER, stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE_SCHEDULER, stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE_SCHEDULER, stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE_SCHEDULER, stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE_SCHEDULER, stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE_SCHEDULER, stream)
#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE_SCHEDULER, stream)

using namespace NKikimr;
using namespace NKikimr::NKqp;
using namespace NKikimr::NKqp::NScheduler;
using namespace NKikimr::NKqp::NScheduler::NHdrf::NDynamic;

namespace {

constexpr double Epsilon = 1e-8;

class TComputeSchedulerService : public NActors::TActorBootstrapped<TComputeSchedulerService> {
public:
    explicit TComputeSchedulerService(const NScheduler::TOptions& options)
        : Scheduler(std::make_shared<NScheduler::TComputeScheduler>(options.Counters, options.DelayParams))
        , UpdateFairSharePeriod(options.UpdateFairSharePeriod)
    {}

    void Bootstrap() {
        Send(
            NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
            new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({(ui32)NKikimrConsole::TConfigItem::FeatureFlagsItem}),
            NActors::IEventHandle::FlagTrackDelivery
        );

        if (Enabled = AppData()->FeatureFlags.GetEnableResourcePools()) {
            LOG_I("Enabled on start");
        } else {
            LOG_I("Disabled on start");
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
            hFunc(NWorkload::TEvUpdatePoolInfo, Handle);
            hFunc(TEvRemovePool, Handle);
            hFunc(TEvAddQuery, Handle);
            hFunc(TEvRemoveQuery, Handle);

            hFunc(NActors::TEvents::TEvWakeup, Handle);

            default:
                LOG_E("Unexpected event: " << ev->GetTypeRewrite());
        }
    }

    void Handle(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr&) {
        LOG_D("Subscribed to config changes");
    }

    void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
        const auto& event = ev->Get()->Record;

        if (Enabled = event.GetConfig().GetFeatureFlags().GetEnableResourcePoolsScheduler()) {
            LOG_I("Become enabled");
        } else {
            LOG_I("Become disabled");
        }

        auto responseEvent = std::make_unique<NKikimr::NConsole::TEvConsole::TEvConfigNotificationResponse>(event);
        Send(ev->Sender, responseEvent.release(), NActors::IEventHandle::FlagTrackDelivery, ev->Cookie);
    }

    void Handle(TEvAddDatabase::TPtr& ev) {
        NHdrf::TStaticAttributes const attrs {
            .Weight = ev->Get()->Weight, // TODO: weight shouldn't be negative!
        };
        Scheduler->AddOrUpdateDatabase(ev->Get()->Id, attrs);

        LOG_D("Add database: " << ev->Get()->Id);
    }

    void Handle(TEvRemoveDatabase::TPtr&) {
        Y_ABORT("Unsupported yet");
    }

    void Handle(TEvAddPool::TPtr& ev) {
        const auto& databaseId = ev->Get()->DatabaseId;
        const auto& poolId = ev->Get()->PoolId;
        const auto resourceWeight = std::max(ev->Get()->Params.ResourceWeight, 0.0); // TODO: resource weight shouldn't be negative!
        NHdrf::TStaticAttributes attrs = {
            .Weight = ev->Get()->Weight, // TODO: weight shouldn't be negative!
        };

        if (ev->Get()->Params.TotalCpuLimitPercentPerNode >= 0) {
            attrs.Limit = ev->Get()->Params.TotalCpuLimitPercentPerNode * Scheduler->GetTotalCpuLimit() / 100;
        }

        Y_ASSERT(!poolId.empty());

        LOG_D("Add pool: " << databaseId << "/" << poolId);

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
            if (ev->Get()->Config->TotalCpuLimitPercentPerNode >= 0) {
                Scheduler->AddOrUpdatePool(databaseId, poolId, {
                    .Limit = ev->Get()->Config->TotalCpuLimitPercentPerNode * Scheduler->GetTotalCpuLimit() / 100,
                });
            }
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
            LOG_E("Trying to remove unknown pool: " << databaseId << "/" << poolId);
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
        if (Enabled) {
            auto query = Scheduler->AddOrUpdateQuery(databaseId, poolId.empty() ? NKikimr::NResourcePool::DEFAULT_POOL_ID : poolId, queryId, attrs);
            response->Query = query;
            LOG_D("Add query: " << databaseId << "/" << poolId << ", TxId: " << queryId);
        }
        Send(ev->Sender, response.Release(), 0, queryId);
    }

    void Handle(TEvRemoveQuery::TPtr& ev) {
        const auto& queryId = ev->Get()->QueryId;
        if (!Scheduler->RemoveQuery(queryId)) {
            LOG_E("Trying to remove unknown query: " << queryId);
        } else {
            LOG_D("Remove query: TxId: " << queryId);
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
    bool Enabled = true;
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

TComputeScheduler::TComputeScheduler(TIntrusivePtr<TKqpCounters> counters, const TDelayParams& delayParams, NHdrf::NSnapshot::ELeafFairShare fairShareMode)
    : Root(std::make_shared<TRoot>(counters))
    , DelayParams(delayParams)
    , FairShareMode(fairShareMode)
    , KqpCounters(counters)
{
    auto group = counters->GetKqpCounters();
    Counters.UpdateFairShare = group->GetCounter("scheduler/UpdateFairShare", true);
}

void TComputeScheduler::SetTotalCpuLimit(ui64 cpu) {
    Root->TotalLimit = cpu;
}

ui64 TComputeScheduler::GetTotalCpuLimit() const {
    return Root->TotalLimit;
}

void TComputeScheduler::AddOrUpdateDatabase(const TString& databaseId, const NHdrf::TStaticAttributes& attrs) {
    TWriteGuard lock(Mutex);

    Y_ENSURE(attrs.GetWeight() > 0.0, "Weight should be positive");

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
    Y_ENSURE(database, "Database not found: " << databaseId);

    Y_ENSURE(attrs.GetWeight() > 0.0, "Weight should be positive");

    if (auto pool = database->GetPool(poolId)) {
        pool->Update(attrs);
    } else {
        database->AddPool(std::make_shared<TPool>(poolId, KqpCounters, attrs));
    }
}

TQueryPtr TComputeScheduler::AddOrUpdateQuery(const NHdrf::TDatabaseId& databaseId, const NHdrf::TPoolId& poolId, const NHdrf::TQueryId& queryId, const NHdrf::TStaticAttributes& attrs) {
    Y_ENSURE(!poolId.empty());

    TWriteGuard lock(Mutex);
    auto database = Root->GetDatabase(databaseId);
    Y_ENSURE(database, "Database not found: " << databaseId);
    auto pool = database->GetPool(poolId);
    Y_ENSURE(pool, "Pool not found: " << poolId);

    Y_ENSURE(attrs.GetWeight() > 0.0, "Weight should be positive");
    TQueryPtr query;

    if (query = std::static_pointer_cast<TQuery>(pool->GetQuery(queryId))) {
        query->Update(attrs);
    } else {
        bool allowMinFairShare = (!pool->Limit || *pool->Limit > 0)
            && (FairShareMode >= NHdrf::NSnapshot::ELeafFairShare::ALLOW_OVERLIMIT);
        query = std::make_shared<TQuery>(queryId, &DelayParams, allowMinFairShare, attrs);
        pool->AddQuery(query);
        Y_ENSURE(Queries.emplace(queryId, query).second);
    }

    return query;
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

IActor* CreateKqpComputeSchedulerService(const NScheduler::TOptions& options) {
    Y_ENSURE(options.UpdateFairSharePeriod > TDuration::Zero());
    Y_ENSURE(options.DelayParams.MaxDelay > TDuration::Zero());
    Y_ENSURE(options.DelayParams.MinDelay > TDuration::Zero());
    Y_ENSURE(options.DelayParams.AttemptBonus > TDuration::Zero());
    Y_ENSURE(options.DelayParams.MaxRandomDelay > TDuration::Zero());
    return new TComputeSchedulerService(options);
}

} // namespace NKikimr::NKqp

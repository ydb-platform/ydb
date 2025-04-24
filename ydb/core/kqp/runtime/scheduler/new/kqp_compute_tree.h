#pragma once

#include "fwd.h"

#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/resource_pools/resource_pool_settings.h>

namespace NKikimr::NKqp::NScheduler::NHdrf {

static constexpr ui64 Infinity() {
    return std::numeric_limits<ui64>::max();
}

struct TStaticAttributes {
    std::optional<ui64> Limit;
    std::optional<ui64> Guarantee;
    std::optional<double> Weight;

    inline auto GetLimit() const {
        return Limit.value_or(Infinity());
    }

    inline auto GetGuarantee() const {
        return Guarantee.value_or(0);
    }

    inline auto GetWeight() const {
        return Weight.value_or(1.);
    }

    inline void Update(const TStaticAttributes& other) {
        if (other.Limit) {
            Limit = other.Limit;
        }
        if (other.Guarantee) {
            Guarantee = other.Guarantee;
        }
        if (other.Weight) {
            Weight = other.Weight;
        }
    }
};

struct TTreeElementBase : public TStaticAttributes {
    ui64 TotalLimit = Infinity();

    ui64 FairShare = 0;

    std::atomic<ui64> Usage = 0;
    std::atomic<ui64> Demand = 0;

    std::atomic<ui64> BurstUsage = 0;
    std::atomic<ui64> BurstThrottle = 0;

    TTreeElementBase* Parent = nullptr;
    std::vector<TTreeElementPtr> Children;

    virtual ~TTreeElementBase() = default;
    virtual TTreeElementBase* TakeSnapshot() const = 0;

    void AddChild(const TTreeElementPtr& element);
    void RemoveChild(const TTreeElementPtr& element);

    bool IsRoot() const {
        return !Parent;
    }

    bool IsLeaf() const {
        return Children.empty();
    }

    virtual void AccountFairShare(const TDuration& period);
    virtual void UpdateBottomUp(ui64 totalLimit);
    void UpdateTopDown();
};

class TQuery : public TTreeElementBase {
public:
    explicit TQuery(const TQueryId& queryId, const TStaticAttributes& attrs = {})
        : QueryId(queryId)
    {
        Update(attrs);
    }

    TQuery* TakeSnapshot() const override {
        auto* newQuery = new TQuery(GetId(), *this);
        newQuery->Demand = Demand.load();
        return newQuery;
    }

    void AddTask(const TSchedulableTaskPtr& task);
    void RemoveTask(const TSchedulableTaskPtr& task);

    void IncreaseDemand();
    void DecreaseDemand();

    const TQueryId& GetId() const {
        return QueryId;
    }

private:
    const TQueryId QueryId;
};

class TPool : public TTreeElementBase {
public:
    TPool(const TString& id, const TIntrusivePtr<TKqpCounters>& counters, const TStaticAttributes& attrs = {})
        : TPool(id, attrs)
    {
        auto group = counters->GetKqpCounters()->GetSubgroup("scheduler/pool", Id);

        // TODO: since counters don't support float-point values, then use CPU * 1'000'000 to account the microseconds precision
        Counters.Limit     = group->GetCounter("Limit",     false);
        Counters.Guarantee = group->GetCounter("Guarantee", false);
        Counters.Demand    = group->GetCounter("Demand",    false);
        Counters.Usage     = group->GetCounter("Usage",     true);
        Counters.Throttle  = group->GetCounter("Throttle",  true);
        Counters.FairShare = group->GetCounter("FairShare", true);
    }

    TPool* TakeSnapshot() const override {
        auto* newPool = new TPool(GetId(), *this);
        newPool->Counters.Demand = Counters.Demand;
        newPool->Counters.FairShare = Counters.FairShare;

        Counters.Limit->Set(GetLimit() * 1'000'000);
        Counters.Guarantee->Set(GetGuarantee() * 1'000'000);
        Counters.Usage->Set(BurstUsage);
        Counters.Throttle->Set(BurstThrottle);

        for (const auto& child : Children) {
            newPool->AddQuery(std::shared_ptr<TQuery>(std::dynamic_pointer_cast<TQuery>(child)->TakeSnapshot()));
        }

        return newPool;
    }

    void AccountFairShare(const TDuration& period) override {
        Counters.FairShare->Add(FairShare * period.MicroSeconds());
        TTreeElementBase::AccountFairShare(period);
    }

    void UpdateBottomUp(ui64 totalLimit) override {
        TTreeElementBase::UpdateBottomUp(totalLimit);
        Counters.Demand->Set(Demand * 1'000'000);
    }

    const TString& GetId() const {
        return Id;
    }

    void AddQuery(const TQueryPtr& query) {
        Y_ENSURE(!Queries.contains(query->GetId()));
        Queries.emplace(query->GetId(), query);
        AddChild(query);
    }

    void RemoveQuery(const TQueryId& queryId) {
        auto queryIt = Queries.find(queryId);
        Y_ENSURE(queryIt != Queries.end());
        RemoveChild(queryIt->second);
        Queries.erase(queryIt);
    }

    TQueryPtr GetQuery(const TQueryId& queryId) const {
        auto it = Queries.find(queryId);
        return it == Queries.end() ? nullptr : it->second;
    }

private:
    explicit TPool(const TString& id, const TStaticAttributes& attrs = {})
        : Id(id)
    {
        Update(attrs);
    }

private:
    const TString Id;
    THashMap<TQueryId, TQueryPtr> Queries;

    struct {
        NMonitoring::TDynamicCounters::TCounterPtr Limit;
        NMonitoring::TDynamicCounters::TCounterPtr Guarantee;
        NMonitoring::TDynamicCounters::TCounterPtr Demand;
        NMonitoring::TDynamicCounters::TCounterPtr Usage;
        NMonitoring::TDynamicCounters::TCounterPtr Throttle;
        NMonitoring::TDynamicCounters::TCounterPtr FairShare;
    } Counters;
};

class TDatabase : public TTreeElementBase {
public:
    explicit TDatabase(const TString& id, const TStaticAttributes& attrs = {})
        : Id(id)
    {
        Update(attrs);
    }

    TDatabase* TakeSnapshot() const override {
        auto* newDatabase = new TDatabase(GetId(), *this);
        for (const auto& child : Children) {
            newDatabase->AddPool(std::shared_ptr<TPool>(std::dynamic_pointer_cast<TPool>(child)->TakeSnapshot()));
        }
        return newDatabase;
    }

    void AddPool(const TPoolPtr& pool) {
        Y_ENSURE(!Pools.contains(pool->GetId()));
        Pools.emplace(pool->GetId(), pool);
        AddChild(pool);
    }

    TPoolPtr GetPool(const TString& poolId) const {
        auto it = Pools.find(poolId);
        return it == Pools.end() ? nullptr : it->second;
    }

    const TString& GetId() const {
        return Id;
    }

private:
    const TString Id;
    THashMap<TString /* poolId */, TPoolPtr> Pools;
};

class TRoot : public TTreeElementBase {
public:
    explicit TRoot(TIntrusivePtr<TKqpCounters> counters) {
        auto group = counters->GetKqpCounters();
        Counters.TotalLimit = group->GetCounter("scheduler/TotalLimit", false);
    }

    TRoot* TakeSnapshot() const override {
        auto* newRoot = new TRoot();

        Counters.TotalLimit->Set(TotalLimit * 1'000'000);

        newRoot->TotalLimit = TotalLimit;
        for (const auto& child : Children) {
            newRoot->AddDatabase(std::shared_ptr<TDatabase>(std::dynamic_pointer_cast<TDatabase>(child)->TakeSnapshot()));
        }

        return newRoot;
    }

    void AddDatabase(const TDatabasePtr& database) {
        Y_ENSURE(!Databases.contains(database->GetId()));
        Databases.emplace(database->GetId(), database);
        AddChild(database);
    }

    TDatabasePtr GetDatabase(const TString& id) const {
        auto it = Databases.find(id);
        return it == Databases.end() ? nullptr : it->second;
    }

    void SetSnapshot(const TRootPtr& snapshot) {
        ui8 newSnapshotIdx = 1 - SnapshotIdx;
        Snapshots.at(newSnapshotIdx) = snapshot;
        if (Snapshots.at(SnapshotIdx)) {
            Snapshots.at(SnapshotIdx)->AccountFairShare(snapshot->SnapshotTimestamp - Snapshots.at(SnapshotIdx)->SnapshotTimestamp);
        }
        SnapshotIdx = newSnapshotIdx;
    }

    TRootPtr GetSnapshot() const {
        return Snapshots.at(SnapshotIdx);
    }

private:
    TRoot() = default;

private:
    const TMonotonic SnapshotTimestamp = TMonotonic::Now();

    THashMap<TString /* name */, TDatabasePtr> Databases;
    std::array<TRootPtr, 2> Snapshots;
    std::atomic<ui8> SnapshotIdx = 0;

    struct {
        NMonitoring::TDynamicCounters::TCounterPtr TotalLimit;
    } Counters;
};

} // namespace NKikimr::NKqp::NScheduler::NHdrf

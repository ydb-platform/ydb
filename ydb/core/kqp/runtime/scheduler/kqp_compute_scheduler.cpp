#include "kqp_compute_scheduler.h"

#include "kqp_compute_pool.h"

#include <ydb/core/protos/table_service_config.pb.h>

#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/common/events/workload_service.h>

#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>

namespace {
    static constexpr TDuration ToDuration(double t) {
        return TDuration::MicroSeconds(t);
    }

    static constexpr TDuration AvgBatch = TDuration::MicroSeconds(100);

    static constexpr double MinCapacity = 1e-9;
}

namespace NKikimr::NKqp::NScheduler {

class TShare : public IObservableValue<double> {
protected:
    double DoUpdateValue() override {
        return Base->GetValue() * Share->GetValue();
    }

public:
    TShare(IObservableValue* base, IObservableValue* share)
        : Base(base)
        , Share(share)
    {
        AddDependency(base);
        AddDependency(share);
        Update();
    }

private:
    IObservableValue* Base;
    IObservableValue* Share;
};

class TRatio : public IObservableValue<double> {
protected:
    double DoUpdateValue() override {
        return Part->GetValue() / Base->GetValue();
    }

public:
    TRatio(IObservableValue* base, IObservableValue* part)
        : Base(base)
        , Part(part)
    {
        AddDependency(base);
        AddDependency(part);
        Update();
    }

private:
    IObservableValue* Base;
    IObservableValue* Part;
};

template<typename T>
class TParameter;

class TObservableUpdater {
public:
    void UpdateAll() {
        TVector<TSet<IObservable*>> queue;
        for (auto& dep : ToUpdate_) {
            queue.resize(Max(queue.size(), dep.GetDepth() + 1));
            queue[dep.GetDepth()].insert(&dep);
        }
        ToUpdate_.Clear();

        for (size_t i = 0; i < queue.size(); ++i) {
            TSet<IObservable*> cur;
            queue[i].swap(cur);
            for (auto* node : cur) {
                if (node->Update()) {
                    node->ForAllDependents([&](auto* dep){
                        queue.resize(Max(queue.size(), dep->GetDepth() + 1));
                        queue[dep->GetDepth()].insert(dep);
                    });
                }
            }
        }

    }

    void ToUpdate(IObservable* dep) {
        ToUpdate_.PushBack(dep);
    }

    using TParameterKey = std::pair<TString, ui32>;

    template<typename T>
    T* FindValue(TParameterKey key) {
        if (auto* ptr = Params.FindPtr(key)) {
            return ptr->Get<T>();
        }
        return nullptr;
    }

    template<typename T>
    void AddValue(TParameterKey key, THolder<T> value) {
        Params.emplace(key, std::move(value));
    }

    template<typename T>
    TParameter<T>* FindOrAddParameter(TParameterKey key, T def);

    ui64 ValuesCount() {
        return Params.size();
    }

    void CollectValues() {
        std::vector<TParameterKey> toerase;
        for (auto& [k, v] : Params) {
            if (!v.Holder->HasDependents()) {
                toerase.push_back(k);
            }
        }
        for (auto& key : toerase) {
            Params.erase(key);
        }
    }

private:
    struct TValueContainer {
        TParameter<double>* AsDoubleParameter = nullptr;
        TParameter<bool>* AsBoolParameter = nullptr;
        TParameter<i64>* AsIntParameter = nullptr;

        THolder<IObservable> Holder;

        TValueContainer() = default;
        TValueContainer(TValueContainer&&) = default;

        TValueContainer(THolder<IObservable> value) {
            Holder = std::move(value);
        }

        TValueContainer(THolder<TParameter<double>> value);
        TValueContainer(THolder<TParameter<bool>> value);
        TValueContainer(THolder<TParameter<i64>> value);

        template<typename T>
        T* Get();
    };

    TIntrusiveList<IObservable> ToUpdate_;
    THashMap<TParameterKey, TValueContainer> Params;
};

template<typename T>
class TParameter : public IObservableValue<T> {
public:
    TParameter(TObservableUpdater* engine, double initialValue)
        : Value_(initialValue)
        , Updater_(engine)
    {
        Updater_->ToUpdate(this);
    }

    T SetValue(T val) {
        auto oldValue = Value_;
        Value_ = val;
        Updater_->ToUpdate(this);
        return oldValue;
    }

    void Add(T val) {
        Value_ += val;
        Updater_->ToUpdate(this);
    }

protected:
    T DoUpdateValue() override {
        return Value_;
    }

    T Value_;

private:
    TObservableUpdater* Updater_;
};

template<typename T>
TParameter<T>* TObservableUpdater::FindOrAddParameter(TParameterKey key, T def) {
    if (auto* ptr = FindValue<TParameter<T>>(key)) {
        return ptr;
    }
    auto value = MakeHolder<TParameter<T>>(this, def);
    auto* result = value.Get();
    AddValue<TParameter<T>>(key, std::move(value));
    return result;
}

TObservableUpdater::TValueContainer::TValueContainer(THolder<TParameter<double>> value) {
    AsDoubleParameter = value.Get();
    Holder = THolder<IObservable>(value.Release());
}

TObservableUpdater::TValueContainer::TValueContainer(THolder<TParameter<bool>> value) {
    AsBoolParameter = value.Get();
    Holder = THolder<IObservable>(value.Release());
}

TObservableUpdater::TValueContainer::TValueContainer(THolder<TParameter<i64>> value) {
    AsIntParameter = value.Get();
    Holder = THolder<IObservable>(value.Release());
}

template<typename T>
T* TObservableUpdater::TValueContainer::Get() {
    if constexpr (std::is_same_v<T, TParameter<double>>) {
        return AsDoubleParameter;
    } else if constexpr (std::is_same_v<T, TParameter<bool>>) {
        return AsBoolParameter;
    } else if constexpr (std::is_same_v<T, TParameter<i64>>) {
        return AsIntParameter;
    } else {
        return Holder.Get();
    }
}

struct TResourceWeightIntrusiveListTag {};

class IResourcesWeightLimitValue : public TParameter<double>, public TIntrusiveListItem<IResourcesWeightLimitValue, TResourceWeightIntrusiveListTag> {
public:
    using TParameter<double>::TParameter;

    virtual IObservableValue<bool>* Enabled() = 0;

    virtual IObservableValue<double>* Weight() = 0;

    virtual IObservableValue<double>* HardLimit() = 0;
};

class TResourcesWeightCalculator : public IObservable {
public:
    void Register(IResourcesWeightLimitValue* entry) {
        AddDependency(entry->Enabled());
        AddDependency(entry->Weight());
        AddDependency(entry->HardLimit());
        Entries.PushBack(entry);
    }

    bool Update() {
        SortBuffer.clear();
        double sumWeight = 0;
        for (auto& entry : Entries) {
            if (entry.Enabled()->GetValue()) {
                sumWeight += entry.Weight()->GetValue();
                SortBuffer.push_back({entry.HardLimit()->GetValue() / entry.Weight()->GetValue(), &entry});
            }
        }
        Sort(SortBuffer);

        double level = 0;
        double consumedShare = 0;
        for (auto& [entryLimit, sortedEntry] : SortBuffer) {
            double limit = entryLimit - level;
            double maxToConsume = (1 - consumedShare) / sumWeight;

            double actual = Min(limit, maxToConsume);

            level += actual;
            consumedShare += actual * sumWeight;

            if (maxToConsume <= limit) {
                break;
            }

            sumWeight -= sortedEntry->Weight()->GetValue();
        }

        for (auto& [entryLimit, sortedEntry] : SortBuffer) {
            sortedEntry->SetValue(Min(level * sortedEntry->Weight()->GetValue(), sortedEntry->HardLimit()->GetValue()));
        }

        return true;
    }

private:
    TIntrusiveList<IResourcesWeightLimitValue, TResourceWeightIntrusiveListTag> Entries;

    TVector<std::pair<double, IResourcesWeightLimitValue*>> SortBuffer;
};

class TResourcesWeightLimitValue : public IResourcesWeightLimitValue {
public:
    TResourcesWeightLimitValue(
        TParameter<double>* sumCores,
        TParameter<i64>* tasksCount,
        IObservableValue<double>* staticLimit,
        TParameter<double>* resourceWeight,
        TParameter<bool>* enabled,
        TResourcesWeightCalculator* calculator,
        TObservableUpdater* updater)
    : IResourcesWeightLimitValue(updater, staticLimit->GetValue())
    , EnabledFlag(enabled, tasksCount)
    , HardLimitValue(staticLimit, tasksCount, sumCores)
    , ResourceWeightValue(resourceWeight)
    , Calculator_(calculator)
    , Updater_(updater)
    {
        calculator->Register(this);
        AddDependency(calculator);
    }

    ~TResourcesWeightLimitValue() {
        Updater_->ToUpdate(Calculator_);
    }

    IObservableValue<double>* Weight() override {
        return ResourceWeightValue;
    }

    IObservableValue<bool>* Enabled() override {
        return &EnabledFlag;
    }

    IObservableValue<double>* HardLimit() override {
        return &HardLimitValue;
    }

private:
    struct TEnabledFlag : public IObservableValue<bool> {
        TEnabledFlag(TParameter<bool>* enabled, TParameter<i64>* taskscount)
            : Enabled(enabled)
            , Taskscount(taskscount)
        {
            AddDependency(enabled);
            AddDependency(taskscount);
            Update();
        }

        bool DoUpdateValue() override {
            return Enabled->GetValue() && Taskscount->GetValue() > 0;
        }

        TParameter<bool>* Enabled;
        TParameter<i64>* Taskscount;
    } EnabledFlag;

    struct THardLimit : public IObservableValue<double> {
        THardLimit(IObservableValue<double>* staticLimit, TParameter<i64>* taskscount, TParameter<double>* sumCores)
            : StaticLimit(staticLimit)
            , TasksCount(taskscount)
            , SumCores(sumCores)
        {
            AddDependency(StaticLimit);
            AddDependency(TasksCount);
            AddDependency(SumCores);
        }

        double DoUpdateValue() override {
            return Min(StaticLimit->GetValue(), TasksCount->GetValue() / SumCores->GetValue());
        }

        IObservableValue<double>* StaticLimit;
        TParameter<i64>* TasksCount;
        TParameter<double>* SumCores;
    } HardLimitValue;

private:
    TParameter<double>* ResourceWeightValue;
    TResourcesWeightCalculator* Calculator_;
    TObservableUpdater* Updater_;
};

TSchedulerEntity::TSchedulerEntity(TPool* pool)
    : Pool(pool)
    , BatchTime(AvgBatch)
{
    ++Pool->EntitiesCount;
}

TSchedulerEntity::~TSchedulerEntity() {
    --Pool->EntitiesCount;
}

void TSchedulerEntity::TrackTime(TDuration time, TMonotonic) {
    Pool->TrackedMicroSeconds.fetch_add(time.MicroSeconds());
}

void TSchedulerEntity::UpdateBatchTime(TDuration time) {
    Wakeups = 0;
    auto newBatch = BatchTime * BatchCalcDecay + time * (1 - BatchCalcDecay);
    if (IsThrottled) {
        MarkResumed();
        BatchTime = newBatch;
        MarkThrottled();
    } else {
        BatchTime = newBatch;
    }
}

TMaybe<TDuration> TSchedulerEntity::Delay(TMonotonic now, TPool* pool) {
    auto current = pool->MutableStats.Current();
    auto limit = current.get()->Limit(now);
    auto tracked = pool->TrackedMicroSeconds.load();
    if (limit > tracked) {
        return {};
    } else {
        if (current.get()->Capacity < MinCapacity) {
            return MaxDelay;
        }
        return Min(MaxDelay, ToDuration((tracked - limit +
                    Max<i64>(0, pool->DelayedSumBatches.load()) + BatchTime.MicroSeconds() +
                    ActivationPenalty.MicroSeconds() * (pool->DelayedCount.load() + 1) +
                    current.get()->MaxLimitDeviation) / current.get()->Capacity));
    }
}

TMaybe<TDuration> TSchedulerEntity::Delay(TMonotonic now) {
    TMaybe<TDuration> result;
    auto poolResult = Delay(now, Pool);
    if (!result) {
        result = poolResult;
    } else if (poolResult && *result < *poolResult) {
        result = poolResult;
    }
    return result;
}

void TSchedulerEntity::MarkThrottled() {
    IsThrottled = true;
    Pool->DelayedSumBatches.fetch_add(BatchTime.MicroSeconds());
    Pool->DelayedCount.fetch_add(1);
}

void TSchedulerEntity::MarkResumed() {
    IsThrottled = false;
    Pool->DelayedSumBatches.fetch_sub(BatchTime.MicroSeconds());
    Pool->DelayedCount.fetch_sub(1);
}

void TSchedulerEntity::MarkResumed(TMonotonic now) {
    MarkResumed();
    if (auto lastNow = Pool->MutableStats.Current().get()->LastNowRecalc; now > lastNow) {
        Pool->ThrottledMicroSeconds.fetch_add((now - lastNow).MicroSeconds());
    }
}

struct TComputeScheduler::TImpl {
    THashMap<TString, size_t> PoolId;
    std::vector<std::unique_ptr<TPool>> Pools;
    std::vector<double> ResourceWeights;
    double ResourceWeightSum = 0.0;

    TResourcesWeightCalculator ResourceWeightsCalculator;
    TObservableUpdater WeightsUpdater;
    TParameter<double> SumCores{&WeightsUpdater, 1};

    enum : ui32 {
        TotalShare = 1,

        PerQueryShare = 2,

        ResourceWeight = 3,
        ResourceWeightEnabled = 4,
        TasksCount = 5,

        CompositeShare = 6,
        ResourceLimitValue = 7,
    };

    TIntrusivePtr<TKqpCounters> Counters;
    TDuration SmoothPeriod = TDuration::MilliSeconds(100);
    TDuration ForgetInterval = TDuration::Seconds(2);

    TDuration MaxDelay = TDuration::Seconds(10);

    void CreatePool(const TString& name, THolder<IObservableValue<double>> share, NMonotonic::TMonotonic now, double resourceWeight = 0.0) {
        auto pool = std::make_unique<TPool>(name, std::move(share), Counters);
        PoolId[name] = Pools.size();
        pool->AdvanceTime(now, SmoothPeriod, ForgetInterval);
        Pools.push_back(std::move(pool));
        ResourceWeights.push_back(resourceWeight);
        ResourceWeightSum += resourceWeight;
    }

    void CollectPools() {
        std::vector<i64> remap;
        std::vector<std::unique_ptr<TPool>> pools;
        std::vector<double> weights;
        double weightsSum = 0;

        for (size_t i = 0; i < Pools.size(); ++i) {
            if (Pools.at(i)->IsActive()) {
                remap.push_back(pools.size());
                pools.emplace_back(Pools[i].release());
                weights.emplace_back(ResourceWeights[i]);
                weightsSum += ResourceWeights[i];
            } else {
                // to delete
                remap.push_back(-1);
            }
        }

        Pools.swap(pools);
        ResourceWeights.swap(weights);
        ResourceWeightSum = weightsSum;

        {
            std::vector<TString> toerase;
            for (auto& [k, v] : PoolId) {
                if (remap[v] >= 0) {
                    v = remap[v];
                } else {
                    toerase.push_back(k);
                }
            }
            for (auto& k: toerase) {
                PoolId.erase(k);
            }
        }

        WeightsUpdater.CollectValues();
    }
};

TComputeScheduler::TComputeScheduler(TIntrusivePtr<TKqpCounters> counters) {
    Impl = std::make_unique<TImpl>();
    Impl->Counters = counters;
}

TComputeScheduler::~TComputeScheduler() = default;

THolder<TSchedulerEntity> TComputeScheduler::Enroll(TString poolName, i64 weight, TMonotonic now) {
    Y_ENSURE(Impl->PoolId.contains(poolName), "unknown scheduler pool");
    auto* pool = Impl->Pools.at(Impl->PoolId.at(poolName)).get();

    auto result = MakeHolder<TSchedulerEntity>(pool);
    result->Weight = weight;
    result->MaxDelay = Impl->MaxDelay;

    pool->AddEntity(result);
    auto* tasksCount = Impl->WeightsUpdater.FindOrAddParameter<i64>({poolName, TImpl::TasksCount}, 0);
    if (result->Weight > 0) {
        tasksCount->Add(1);
    }
    pool->AdvanceTime(now, Impl->SmoothPeriod, Impl->ForgetInterval);

    return result;
}

void TComputeScheduler::AdvanceTime(TMonotonic now) {
    Impl->WeightsUpdater.UpdateAll();
    for (size_t i = 0; i < Impl->Pools.size(); ++i) {
        Impl->Pools[i]->AdvanceTime(now, Impl->SmoothPeriod, Impl->ForgetInterval);
    }
    Impl->CollectPools();
    if (Impl->Counters) {
        Impl->Counters->SchedulerPoolsCount->Set(Impl->Pools.size());
        Impl->Counters->SchedulerValuesCount->Set(Impl->WeightsUpdater.ValuesCount());
    }
}

void TComputeScheduler::Unregister(THolder<TSchedulerEntity>& entity, TMonotonic now) {
    auto* pool = entity->Pool;
    auto* param = Impl->WeightsUpdater.FindValue<TParameter<i64>>({pool->GetName(), TImpl::TasksCount});
    if (param) {
        param->Add(-1);
    }
    pool->AdvanceTime(now, Impl->SmoothPeriod, Impl->ForgetInterval);
}

void TComputeScheduler::SetMaxDeviation(TDuration period) {
    Impl->SmoothPeriod = period;
}

void TComputeScheduler::SetForgetInterval(TDuration period) {
    Impl->ForgetInterval = period;
}

bool TComputeScheduler::Disabled(TString pool) {
    auto* ptr = Impl->PoolId.FindPtr(pool);
    return !ptr || Impl->Pools.at(*ptr)->IsDisabled();
}


void TComputeScheduler::Disable(TString pool, TMonotonic now) {
    // if ptr == 0 it's already disabled
    if (auto* ptr = Impl->PoolId.FindPtr(pool)) {
        Impl->Pools.at(*ptr)->Disable();
        Impl->Pools.at(*ptr)->AdvanceTime(now, Impl->SmoothPeriod, Impl->ForgetInterval);
    }
}

class TCompositePoolShare : public IObservableValue<double> {
protected:
    double DoUpdateValue() override {
        if (ResourceWeightEnabled->GetValue()) {
            if (ResourceWeightLimit->Enabled()->GetValue()) {
                return Min(TotalLimit->GetValue(), ResourceWeightLimit->GetValue());
            } else {
                return 0;
            }
        } else {
            return TotalLimit->GetValue();
        }
    }

public:
    TCompositePoolShare(IObservableValue<double>* totalLimit, TResourcesWeightLimitValue* resourceWeightLimit, IObservableValue<bool>* resourceWeightEnabled)
        : ResourceWeightEnabled(resourceWeightEnabled)
        , TotalLimit(totalLimit)
        , ResourceWeightLimit(resourceWeightLimit)
    {
        AddDependency(resourceWeightEnabled);
        AddDependency(totalLimit);
        AddDependency(resourceWeightLimit);
        AddDependency(resourceWeightLimit->Enabled());
        Update();
    }

private:
    IObservableValue<bool>* ResourceWeightEnabled;
    IObservableValue<double>* TotalLimit;
    TResourcesWeightLimitValue* ResourceWeightLimit;
};

void TComputeScheduler::UpdatePoolShare(TString poolName, double share, TMonotonic now, std::optional<double> resourceWeight) {
    auto ptr = Impl->PoolId.FindPtr(poolName);

    auto* shareValue = Impl->WeightsUpdater.FindOrAddParameter<double>({poolName, TImpl::TotalShare}, share);
    shareValue->SetValue(share);

    TParameter<bool>* weightEnabled = Impl->WeightsUpdater.FindOrAddParameter<bool>({poolName, TImpl::ResourceWeightEnabled}, resourceWeight.has_value());
    weightEnabled->SetValue(resourceWeight.has_value());

    if (!ptr) {
        TParameter<double>* resourceWeightValue = Impl->WeightsUpdater.FindOrAddParameter<double>({poolName, TImpl::ResourceWeight}, resourceWeight.value_or(0));
        TParameter<i64>* taskscount = Impl->WeightsUpdater.FindOrAddParameter<i64>({poolName, TImpl::TasksCount}, 0);

        auto resourceLimitValue = MakeHolder<TResourcesWeightLimitValue>(
            &Impl->SumCores,
            taskscount,
            shareValue,
            resourceWeightValue,
            weightEnabled,
            &Impl->ResourceWeightsCalculator,
            &Impl->WeightsUpdater);

        auto compositeWeight = MakeHolder<TCompositePoolShare>(shareValue, resourceLimitValue.Get(), weightEnabled);
        auto cap = MakeHolder<TShare>(&Impl->SumCores, compositeWeight.Get());
        Impl->WeightsUpdater.AddValue<IObservable>({poolName, TImpl::ResourceLimitValue}, THolder(resourceLimitValue.Release()));
        Impl->WeightsUpdater.AddValue({poolName, TImpl::CompositeShare}, std::move(compositeWeight));
        Impl->CreatePool(poolName, std::move(cap), now, resourceWeight.value_or(0));

        for (uint i = 0; i < Impl->ResourceWeights.size(); ++i) {
            if (Impl->ResourceWeights.at(i) > 0 && Impl->ResourceWeightSum > 0) {
                Impl->Pools.at(i)->UpdateGuarantee(Impl->ResourceWeights.at(i) / Impl->ResourceWeightSum * Impl->SumCores.GetValue() * 1'000'000);
            } else {
                Impl->Pools.at(i)->UpdateGuarantee(0);
            }
        }
    } else {
        auto& pool = Impl->Pools.at(*ptr);
        pool->Enable();
        pool->AdvanceTime(now, Impl->SmoothPeriod, Impl->ForgetInterval);
    }
}

void TComputeScheduler::UpdatePerQueryShare(TString poolName, double share, TMonotonic) {
    auto ptr = Impl->WeightsUpdater.FindOrAddParameter<double>({poolName, TImpl::PerQueryShare}, share);
    ptr->SetValue(share);
}

void TComputeScheduler::SetCapacity(ui64 cores) {
    Impl->SumCores.SetValue(cores);
}

::NMonitoring::TDynamicCounters::TCounterPtr TComputeScheduler::GetPoolUsageCounter(TString poolName) const {
    return Impl->Counters
        ->GetKqpCounters()
        ->GetSubgroup("NodeScheduler/Pool", poolName)
        ->GetCounter("Usage", true);
}


struct TEvPingPool : public TEventLocal<TEvPingPool, TKqpComputeSchedulerEvents::EvPingPool> {
    TString DatabaseId;
    TString Pool;

    TEvPingPool(TString databaseId, TString pool)
        : DatabaseId(databaseId)
        , Pool(pool)
    {
    }
};

class TSchedulerActor : public TActorBootstrapped<TSchedulerActor> {
public:
    TSchedulerActor(TSchedulerActorOptions options)
        : Opts(options)
    {
        if (!Opts.Scheduler) {
            Opts.Scheduler = std::make_shared<TComputeScheduler>(Opts.Counters);
        }
        Opts.Scheduler->SetForgetInterval(Opts.ForgetOverflowTimeout);
    }

    void Bootstrap() {
        Schedule(Opts.AdvanceTimeInterval, new TEvents::TEvWakeup());

        ui32 tableServiceConfigKind = (ui32) NKikimrConsole::TConfigItem::TableServiceConfigItem;
        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
             new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({tableServiceConfigKind}),
             IEventHandle::FlagTrackDelivery);

        Become(&TSchedulerActor::State);
        SetCapacity(SelfId().PoolID());
    }

    void SetCapacity(ui32 pool) {
        NActors::TExecutorPoolStats poolStats;
        TVector<NActors::TExecutorThreadStats> threadsStats;
        TlsActivationContext->ActorSystem()->GetPoolStats(pool, poolStats, threadsStats);
        ui64 threads = Max<ui64>(poolStats.MaxThreadCount, 1);
        Opts.Counters->SchedulerCapacity->Set(threads);
        Opts.Scheduler->SetCapacity(threads);
    }

    STATEFN(State) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, Handle);
            hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);

            hFunc(NWorkload::TEvUpdatePoolInfo, Handle);

            hFunc(TEvSchedulerUnregister, Handle);
            hFunc(TEvSchedulerNewPool, Handle);
            hFunc(TEvPingPool, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
            default: {
                Y_ABORT("Unexpected event 0x%x for TKqpSchedulerService", ev->GetTypeRewrite());
            }
        }
    }

    void Handle(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr&) {
        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_NODE, "Subscribed for config changes");
    }

    void Handle(TEvSchedulerUnregister::TPtr& ev) {
        if (ev->Get()->SchedulerEntity) {
            Opts.Scheduler->Unregister(ev->Get()->SchedulerEntity, TlsActivationContext->Monotonic());
        }
    }

    void Handle(TEvSchedulerNewPool::TPtr& ev) {
        Send(MakeKqpWorkloadServiceId(SelfId().NodeId()), new NWorkload::TEvSubscribeOnPoolChanges(ev->Get()->DatabaseId, ev->Get()->Pool));
    }

    void Handle(TEvPingPool::TPtr& ev) {
        Send(MakeKqpWorkloadServiceId(SelfId().NodeId()), new NWorkload::TEvSubscribeOnPoolChanges(ev->Get()->DatabaseId, ev->Get()->Pool));
    }

    void Handle(NWorkload::TEvUpdatePoolInfo::TPtr& ev) {
        if (ev->Get()->Config.has_value()) {
            auto totalShare = ev->Get()->Config->TotalCpuLimitPercentPerNode / 100.0;
            auto queryShare = ev->Get()->Config->QueryCpuLimitPercentPerNode / 100.0;
            std::optional<double> resourceWeight;
            if (ev->Get()->Config->ResourceWeight >= 0) {
                resourceWeight = ev->Get()->Config->ResourceWeight;
            }

            if (totalShare <= 0 && (queryShare > 0 || resourceWeight)) {
                totalShare = 1;
            }

            if (queryShare <= 0) {
                queryShare = 1;
            }

            Opts.Scheduler->UpdatePoolShare(ev->Get()->PoolId, totalShare, TlsActivationContext->Monotonic(), resourceWeight);
            Opts.Scheduler->UpdatePerQueryShare(ev->Get()->PoolId, queryShare, TlsActivationContext->Monotonic());
        } else {
            Opts.Scheduler->Disable(ev->Get()->PoolId, TlsActivationContext->Monotonic());
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr&) {
        SetCapacity(SelfId().PoolID());
        Opts.Scheduler->AdvanceTime(TlsActivationContext->Monotonic());
        Schedule(Opts.AdvanceTimeInterval, new TEvents::TEvWakeup());
    }

    void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
        auto &event = ev->Get()->Record;
        auto& config = event.GetConfig().GetTableServiceConfig().GetComputeSchedulerSettings();

        Opts.AdvanceTimeInterval = TDuration::MicroSeconds(config.GetAdvanceTimeIntervalUsec());
        Opts.ActivePoolPollingTimeout = TDuration::Seconds(config.GetActivePoolPollingSec());
        Opts.Scheduler->SetForgetInterval(TDuration::MicroSeconds(config.GetForgetOverflowTimeoutUsec()));
    }

private:
    TSchedulerActorOptions Opts;
};

IActor* CreateSchedulerActor(TSchedulerActorOptions opts) {
    return new TSchedulerActor(opts);
}

} // namespace NKikimr::NKqp::NScheduler

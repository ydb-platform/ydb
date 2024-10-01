#include "kqp_compute_scheduler.h"

#include <ydb/core/protos/table_service_config.pb.h>

#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/common/events/workload_service.h>

#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>

namespace {
    static constexpr ui64 FromDuration(TDuration d) {
        return d.MicroSeconds();
    }

    static constexpr TDuration ToDuration(double t) {
        return TDuration::MicroSeconds(t);
    }

    static constexpr TDuration AvgBatch = TDuration::MicroSeconds(100);
}

namespace NKikimr {
namespace NKqp {

class IObservable : TNonCopyable {
public:
    virtual bool Update() = 0;

    void AddDependency(IObservable* dep) {
        Depth = Max<size_t>(Depth, dep->Depth + 1);
        Dependencies.insert(dep);
        dep->Dependents.insert(this);
    }

    bool HasDependents() {
        return !Dependents.empty();
    }

    virtual ~IObservable() {
        for (auto& dep : Dependencies) {
            dep->Dependents.erase(this);
        }
        for (auto& dep : Dependents) {
            dep->Dependencies.erase(this);
        }
    }

    size_t GetDepth() {
        return Depth;
    }

    template<typename T>
    void ForAllDependents(T&& f) {
        for (auto* dep : Dependents) {
            f(dep);
        }
    }

protected:
    TSet<IObservable*> CutAllDependents() {
        TSet<IObservable*> res;
        Dependents.swap(res);
        for (auto* dep : res) {
            dep->Dependencies.erase(this);
        }
        return res;
    }

private:
    size_t Depth = 0;

    TSet<IObservable*> Dependencies;
    TSet<IObservable*> Dependents;
};

template<typename T>
class IObservableValue : public IObservable {
protected:
    virtual double DoUpdateValue() = 0;

public:
    bool Update() override {
        if (auto val = DoUpdateValue()) {
            Value = val;
            return true;
        } else {
            return false;
        }
    }

    T GetValue() {
        return Value;
    }

private:
    T Value;
};

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

template<typename T>
class TParameter;

class TObservableUpdater : IObservable {
private:
    bool Update() override {
        return false;
    }

public:
    void UpdateAll() {
        TVector<TSet<IObservable*>> queue;
        auto deps = CutAllDependents();
        for (auto* dep : deps) {
            queue.resize(Max(queue.size(), dep->GetDepth() + 1));
            queue[dep->GetDepth()].insert(dep);
        }

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
        dep->AddDependency(this);
    }

    using TParameterKey = std::pair<TString, ui32>;

    template<typename T>
    T* FindValue(TParameterKey key) {
        if (auto ptr = Params.FindPtr(key)) {
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

        THolder<IObservable> Holder;

        TValueContainer() = default;
        TValueContainer(TValueContainer&&) = default;

        TValueContainer(THolder<IObservable> value) {
            Holder = std::move(value);
        }

        TValueContainer(THolder<TParameter<double>> value);
        TValueContainer(THolder<TParameter<bool>> value);

        template<typename T>
        T* Get();
    };

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

protected:
    double DoUpdateValue() override {
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

template<typename T>
T* TObservableUpdater::TValueContainer::Get() {
    if constexpr (std::is_same_v<T, TParameter<double>>) {
        return AsDoubleParameter;
    } else if constexpr (std::is_same_v<T, TParameter<bool>>) {
        return AsBoolParameter;
    } else {
        return Holder.Get();
    }
}

template<typename T>
class TMultiThreadView {
public:
    TMultiThreadView(std::atomic<ui64>* usage, T* slot)
        : Usage(usage)
        , Slot(slot)
    {
        Usage->fetch_add(1);
    }
    const T* get() {
        return Slot;
    }

    ~TMultiThreadView() {
        Usage->fetch_sub(1);
    }

private:
    std::atomic<ui64>* Usage;
    T* Slot;
};

template<typename T>
class TMultithreadPublisher {
public:
    void Publish() {
        auto oldVal = CurrentT.load();
        auto newVal = 1 - oldVal;
        CurrentT.store(newVal);
        while (true) {
            if (Usage[oldVal].load() == 0) {
                Slots[oldVal] = Slots[newVal];
                return;
            }
        }
    }

    T* Next() {
        return &Slots[1 - CurrentT.load()];
    }

    TMultiThreadView<T> Current() {
        while (true) {
            auto val = CurrentT.load();
            TMultiThreadView<T> view(&Usage[val], &Slots[val]);
            if (CurrentT.load() == val) {
                return view;
            }
        }
    }

private:
    std::atomic<ui32> CurrentT = 0;
    std::atomic<ui64> Usage[2] = {0, 0};
    T Slots[2];
};

TSchedulerEntityHandle::TSchedulerEntityHandle(TSchedulerEntity* ptr)
    : Ptr(ptr)
{
}

TSchedulerEntityHandle::TSchedulerEntityHandle(){} 

TSchedulerEntityHandle::TSchedulerEntityHandle(TSchedulerEntityHandle&& other) {
    Ptr.swap(other.Ptr);
}

TSchedulerEntityHandle& TSchedulerEntityHandle::operator = (TSchedulerEntityHandle&& other) {
    Ptr.swap(other.Ptr);
    return *this;
}

TSchedulerEntityHandle::~TSchedulerEntityHandle() = default;

class TSumResourceWeightsHolder : public TParameter<double> {
public:
    using TParameter<double>::TParameter;

    TSumResourceWeightsHolder(TObservableUpdater* engine)
        : TParameter(engine, 0)
    {
    }

    void HandleUpdate(double delta) {
        SetValue(Value_ + delta);
    }

};

class TResourceWeightsUpdater {
public:
    TResourceWeightsUpdater(TParameter<double>* param, double initial)
        : Param(param)
        , Value_(initial)
    {
        param->SetValue(Value_);
    }

    void Track(TSumResourceWeightsHolder* holder) {
        if (!Holder) {
            Holder = holder;
            Holder->HandleUpdate(GetValue());
        }
    }

    void Untrack() {
        if (Holder) {
            Holder->HandleUpdate(-GetValue());
            Holder = nullptr;
        }
    }

    double GetValue() {
        return Value_;
    }

    void SetValue(double val) {
        if (Holder) {
            Holder->HandleUpdate(val - Value_);
        }
        Value_ = val;
        Param->SetValue(Value_);
    }

private:
    TSumResourceWeightsHolder* Holder = nullptr;
    TParameter<double>* Param;
    double Value_;
};

class TSchedulerEntity {
public:
    TSchedulerEntity() {}
    ~TSchedulerEntity() {}

    struct TGroupMutableStats {
        double Capacity = 0;
        TMonotonic LastNowRecalc;
        bool Disabled = false;
        i64 EntitiesWeight = 0;
        double MaxDeviation = 0;
        double MaxLimitDeviation = 0;

        ssize_t TrackedBefore = 0;

        double Limit(TMonotonic now) const {
            return FromDuration(now - LastNowRecalc) * Capacity + MaxLimitDeviation + TrackedBefore;
        }
    };

    struct TGroupRecord {
        std::atomic<i64> TrackedMicroSeconds = 0;
        std::atomic<i64> DelayedSumBatches = 0;
        std::atomic<i64> DelayedCount = 0;

        THolder<IObservableValue<double>> Share;
        THolder<TResourceWeightsUpdater> ResourceWeightUpdater;

        ::NMonitoring::TDynamicCounters::TCounterPtr Vtime;
        ::NMonitoring::TDynamicCounters::TCounterPtr EntitiesWeight;
        ::NMonitoring::TDynamicCounters::TCounterPtr Limit;
        ::NMonitoring::TDynamicCounters::TCounterPtr Weight;

        ::NMonitoring::TDynamicCounters::TCounterPtr SchedulerClock;
        ::NMonitoring::TDynamicCounters::TCounterPtr SchedulerLimitUs;
        ::NMonitoring::TDynamicCounters::TCounterPtr SchedulerTrackedUs;

        TString Name;

        void AssignWeight() {
            MutableStats.Next()->Capacity = Share->GetValue();
        }

        void InitCounters(const TIntrusivePtr<TKqpCounters>& counters) {
            if (Vtime || !Name) {
                return;
            }

            auto group = counters->GetKqpCounters()->GetSubgroup("NodeScheduler/Group", Name);
            Vtime = group->GetCounter("VTime", true);
            EntitiesWeight = group->GetCounter("Entities", false);
            Limit = group->GetCounter("Limit", true);
            Weight = group->GetCounter("Weight", false);
            SchedulerClock = group->GetCounter("Clock", false);
            SchedulerTrackedUs = group->GetCounter("Tracked", true);
            SchedulerLimitUs = group->GetCounter("AbsoluteLimit", true);
        }

        TMultithreadPublisher<TGroupMutableStats> MutableStats;
    };

    TStackVec<TGroupRecord*, 2, true, std::allocator<TGroupRecord*>> Groups;
    i64 Weight;
    double Vruntime = 0;
    double Vstart;

    double Vcurrent;

    TDuration MaxDelay;

    static constexpr double WakeupDelay = 1.1;
    static constexpr double BatchCalcDecay = 0;
    TDuration BatchTime = AvgBatch;

    TDuration OverflowToleranceTimeout = TDuration::Seconds(1);

    static constexpr TDuration ActivationPenalty = TDuration::MicroSeconds(10);

    size_t Wakeups = 0;
    bool isThrottled = false;

    void TrackTime(TDuration time, TMonotonic) {
        for (auto group : Groups) {
            //auto current = group->MutableStats.Current();
            group->TrackedMicroSeconds.fetch_add(time.MicroSeconds());
        }
    }

    void UpdateBatchTime(TDuration time) {
        Wakeups = 0;
        auto newBatch = BatchTime * BatchCalcDecay + time * (1 - BatchCalcDecay);
        if (isThrottled) {
            MarkResumed();
            BatchTime = newBatch;
            MarkThrottled();
        } else {
            BatchTime = newBatch;
        }
    }

    TMaybe<TDuration> GroupDelay(TMonotonic now, TGroupRecord* group) {
        auto current = group->MutableStats.Current();
        auto limit = current.get()->Limit(now);
        auto tracked = group->TrackedMicroSeconds.load();
        //double Coeff = pow(WakeupDelay, Wakeups);
        if (limit > tracked) {
            return {};
        } else {
            return Min(MaxDelay, ToDuration(/*Coeff * */(tracked - limit +
                        Max<i64>(0, group->DelayedSumBatches.load()) + BatchTime.MicroSeconds() +
                        ActivationPenalty.MicroSeconds() * (group->DelayedCount.load() + 1) +
                        current.get()->MaxLimitDeviation) / current.get()->Capacity));
        }
    }

    TMaybe<TDuration> GroupDelay(TMonotonic now) {
        TMaybe<TDuration> result;
        for (auto group : Groups) {
            auto groupResult = GroupDelay(now, group);
            if (!result) {
                result = groupResult;
            } else if (groupResult && *result < *groupResult) {
                result = groupResult;
            }
        }
        return result;
    }

    void MarkThrottled() {
        isThrottled = true;
        for (auto group : Groups) {
            group->DelayedSumBatches.fetch_add(BatchTime.MicroSeconds());
            group->DelayedCount.fetch_add(1);
        }
    }

    void MarkResumed() {
        isThrottled = false;
        for (auto group : Groups) {
            group->DelayedSumBatches.fetch_sub(BatchTime.MicroSeconds());
            group->DelayedCount.fetch_sub(1);
        }
    }
};

struct TComputeScheduler::TImpl {
    THashMap<TString, size_t> GroupId;
    std::vector<std::unique_ptr<TSchedulerEntity::TGroupRecord>> Records;

    TObservableUpdater WeightsUpdater;
    TParameter<double> SumCores{&WeightsUpdater, 1};
    TSumResourceWeightsHolder SumResourceWeights{&WeightsUpdater};

    enum : ui32 {
        TotalShare = 1,

        PerQueryShare = 2,

        ResourceWeight = 3,
        ResourceWeightEnabled = 4,

        CompositeShare = 5,
    };

    TIntrusivePtr<TKqpCounters> Counters;
    TDuration SmoothPeriod = TDuration::MilliSeconds(100);
    TDuration ForgetInteval = TDuration::Seconds(2);

    TDuration MaxDelay = TDuration::Seconds(10);

    void CreateGroup(THolder<IObservableValue<double>> share, NMonotonic::TMonotonic now, std::optional<TString> groupName = std::nullopt) {
        auto group = std::make_unique<TSchedulerEntity::TGroupRecord>();
        group->Share = std::move(share);
        if (groupName) {
            group->Name = *groupName;
            GroupId[*groupName] = Records.size();
        }
        AdvanceTime(now, group.get());
        Records.push_back(std::move(group));
    }

    void CollectGroups() {
        std::vector<i64> remap;
        std::vector<std::unique_ptr<TSchedulerEntity::TGroupRecord>> records;

        for (size_t i = 0; i < Records.size(); ++i) {
            auto record = Records[i]->MutableStats.Current();
            if (record.get()->EntitiesWeight > 0 || Records[i]->Share->HasDependents()) {
                remap.push_back(records.size());
                records.emplace_back(Records[i].release());
            } else {
                // to delete
                remap.push_back(-1);
            }
        }

        Records.swap(records);

        {
            std::vector<TString> toerase;
            for (auto& [k, v] : GroupId) {
                if (remap[v] >= 0) {
                    v = remap[v];
                } else {
                    toerase.push_back(k);
                }
            }
            for (auto& k: toerase) {
                GroupId.erase(k);
            }
        }

        WeightsUpdater.CollectValues();
    }

    void AdvanceTime(TMonotonic now, TSchedulerEntity::TGroupRecord* record);
};

TComputeScheduler::TComputeScheduler() {
    Impl = std::make_unique<TImpl>();
}

TComputeScheduler::~TComputeScheduler() = default;

void TComputeScheduler::AddToGroup(TMonotonic now, ui64 id, TSchedulerEntityHandle& handle) {
    auto group = Impl->Records[id].get();
    (*handle).Groups.push_back(group);
    group->MutableStats.Next()->EntitiesWeight += (*handle).Weight;
    if ((*handle).Weight > 0 && group->ResourceWeightUpdater) {
        group->ResourceWeightUpdater->Track(&Impl->SumResourceWeights);
    }
    Impl->AdvanceTime(now, group);
}

TSchedulerEntityHandle TComputeScheduler::Enroll(TString groupName, i64 weight, TMonotonic now) {
    Y_ENSURE(Impl->GroupId.contains(groupName), "unknown scheduler group");
    auto id = Impl->GroupId.at(groupName);

    TSchedulerEntityHandle result{new TSchedulerEntity()};
    (*result).Weight = weight;
    (*result).MaxDelay = Impl->MaxDelay;

    AddToGroup(now, id, result);
    return result;
}

void TComputeScheduler::TImpl::AdvanceTime(TMonotonic now, TSchedulerEntity::TGroupRecord* record) {
    if (Counters) {
        record->InitCounters(Counters);
    }
    WeightsUpdater.UpdateAll();
    record->MutableStats.Next()->Capacity = record->Share->GetValue();
    auto& v = record->MutableStats;
    {
        auto group = v.Current();
        if (group.get()->LastNowRecalc > now) {
            return;
        }
        double delta = 0;

        auto tracked = record->TrackedMicroSeconds.load();
        v.Next()->MaxLimitDeviation = SmoothPeriod.MicroSeconds() * v.Next()->Capacity;
        v.Next()->LastNowRecalc = now;
        v.Next()->TrackedBefore = 
            Max<ssize_t>(
                tracked - FromDuration(ForgetInteval) * group.get()->Capacity, 
                Min<ssize_t>(group.get()->Limit(now) - group.get()->MaxLimitDeviation, tracked));

        v.Next()->MaxDeviation = (FromDuration(SmoothPeriod) * v.Next()->Capacity) / v.Next()->Capacity;

        //if (group.get()->EntitiesWeight > 0) {
        //    delta = FromDuration(now - group.get()->LastNowRecalc) * group.get()->Capacity / group.get()->EntitiesWeight;
        //}

        if (record->Vtime) {
            record->SchedulerLimitUs->Set(group.get()->Limit(now));
            record->SchedulerTrackedUs->Set(record->TrackedMicroSeconds.load());
            record->SchedulerClock->Add(now.MicroSeconds() - group.get()->LastNowRecalc.MicroSeconds());
            record->Vtime->Add(delta);
            record->EntitiesWeight->Set(v.Next()->EntitiesWeight);
            record->Limit->Add(FromDuration(now - group.get()->LastNowRecalc) * group.get()->Capacity);
            record->Weight->Set(group.get()->Capacity);
        }
    }
    v.Publish();
}

void TComputeScheduler::AdvanceTime(TMonotonic now) {
    for (size_t i = 0; i < Impl->Records.size(); ++i) {
        Impl->AdvanceTime(now, Impl->Records[i].get());
    }
    Impl->CollectGroups();
    if (Impl->Counters) {
        Impl->Counters->SchedulerGroupsCount->Set(Impl->Records.size());
        Impl->Counters->SchedulerValuesCount->Set(Impl->WeightsUpdater.ValuesCount());
    }
}

void TComputeScheduler::Deregister(TSchedulerEntityHandle& self, TMonotonic now) {
    for (auto group : (*self).Groups) {
        auto* next = group->MutableStats.Next();
        next->EntitiesWeight -= (*self).Weight;
        if (next->EntitiesWeight <= 0) {
            group->ResourceWeightUpdater->Untrack();
        }
        Impl->AdvanceTime(now, group);
    }
}

ui64 TComputeScheduler::MakePerQueryGroup(TMonotonic now, double share, TString baseGroup) {
    auto baseId = Impl->GroupId.at(baseGroup);
    auto perQueryShare = Impl->WeightsUpdater.FindOrAddParameter<double>({baseGroup, TImpl::PerQueryShare}, share);

    Impl->CreateGroup(MakeHolder<TShare>(Impl->Records[baseId]->Share.Get(), perQueryShare), now);
    ui64 res = Impl->Records.size() - 1;
    Impl->AdvanceTime(now, Impl->Records[res].get());
    return res;
}

void TSchedulerEntityHandle::TrackTime(TDuration time, TMonotonic now) {
    Ptr->TrackTime(time, now);
}

void TSchedulerEntityHandle::ReportBatchTime(TDuration time) {
    Ptr->UpdateBatchTime(time);
}

TMaybe<TDuration> TSchedulerEntityHandle::Delay(TMonotonic now) {
    return Ptr->GroupDelay(now);
}

void TSchedulerEntityHandle::MarkResumed() {
    Ptr->MarkResumed();
}

void TSchedulerEntityHandle::MarkThrottled() {
    Ptr->MarkThrottled();
}

void TSchedulerEntityHandle::Clear() {
    Ptr.reset();
}

void TComputeScheduler::ReportCounters(TIntrusivePtr<TKqpCounters> counters) {
    Impl->Counters = counters;
}

void TComputeScheduler::SetMaxDeviation(TDuration period) {
    Impl->SmoothPeriod = period;
}

void TComputeScheduler::SetForgetInterval(TDuration period) {
    Impl->ForgetInteval = period;
}

bool TComputeScheduler::Disabled(TString group) {
    auto ptr = Impl->GroupId.FindPtr(group);
    return !ptr || Impl->Records[*ptr]->MutableStats.Current().get()->Disabled;
}


void TComputeScheduler::Disable(TString group, TMonotonic now) {
    auto ptr = Impl->GroupId.FindPtr(group);
    // if ptr == 0 it's already disabled
    if (ptr) {
        Impl->Records[*ptr]->MutableStats.Next()->Disabled = true;
        Impl->AdvanceTime(now, Impl->Records[*ptr].get());
    }
}

class TCompositeGroupShare : public IObservableValue<double> {
protected:
    double DoUpdateValue() override {
        if (ResourceWeightEnabled->GetValue()) {
            return Min(TotalLimit->GetValue(), ResourceWeight->GetValue() / SumResourceWeights->GetValue());
        } else {
            return TotalLimit->GetValue();
        }
    }

public:
    TCompositeGroupShare(IObservableValue<double>* resourceWeight, IObservableValue<bool>* resourceWeightEnabled, IObservableValue<double>* sumResourceWeights, IObservableValue<double>* totalLimit)
        : ResourceWeight(resourceWeight)
        , ResourceWeightEnabled(resourceWeightEnabled)
        , SumResourceWeights(sumResourceWeights)
        , TotalLimit(totalLimit)
    {
        Update();
    }

private:
    IObservableValue<double>* ResourceWeight;
    IObservableValue<bool>* ResourceWeightEnabled;
    IObservableValue<double>* SumResourceWeights;
    IObservableValue<double>* TotalLimit;
};

void TComputeScheduler::UpdateGroupShare(TString group, double share, TMonotonic now, std::optional<double> resourceWeight) {
    auto ptr = Impl->GroupId.FindPtr(group);

    auto* shareValue = Impl->WeightsUpdater.FindOrAddParameter<double>({group, TImpl::TotalShare}, share);
    shareValue->SetValue(share);

    TParameter<bool>* weightEnabled = Impl->WeightsUpdater.FindOrAddParameter<bool>({group, TImpl::ResourceWeightEnabled}, resourceWeight.has_value());
    weightEnabled->SetValue(resourceWeight.has_value());

    TParameter<double>* resourceWeightValue = Impl->WeightsUpdater.FindOrAddParameter<double>({group, TImpl::ResourceWeight}, resourceWeight.value_or(0));

    if (!ptr) {
        auto compositeWeight = MakeHolder<TCompositeGroupShare>(resourceWeightValue, weightEnabled, &Impl->SumResourceWeights, shareValue);
        auto resourceWeightsUpdater = MakeHolder<TResourceWeightsUpdater>(resourceWeightValue, resourceWeight.value_or(0));
        auto cap = MakeHolder<TShare>(&Impl->SumCores, compositeWeight.Get());
        Impl->WeightsUpdater.AddValue({group, TImpl::CompositeShare}, std::move(compositeWeight));
        Impl->CreateGroup(std::move(cap), now, group);

        Impl->Records.back()->ResourceWeightUpdater = std::move(resourceWeightsUpdater);
    } else {
        auto& record = Impl->Records[*ptr];
        record->MutableStats.Next()->Disabled = false;
        Impl->AdvanceTime(now, record.get());
    }
}

void TComputeScheduler::UpdatePerQueryShare(TString group, double share, TMonotonic) {
    auto ptr = Impl->WeightsUpdater.FindOrAddParameter<double>({group, TImpl::PerQueryShare}, share);
    ptr->SetValue(share);
}

void TComputeScheduler::SetCapacity(ui64 cores) {
    Impl->SumCores.SetValue(cores);
}

::NMonitoring::TDynamicCounters::TCounterPtr TComputeScheduler::GetGroupUsageCounter(TString group) const {
    return Impl->Counters
        ->GetKqpCounters()
        ->GetSubgroup("NodeScheduler/Group", group)
        ->GetCounter("Usage", true);
}


struct TEvPingPool : public TEventLocal<TEvPingPool, TKqpComputeSchedulerEvents::EvPingPool> {
    TString Database;
    TString Pool;

    TEvPingPool(TString database, TString pool)
        : Database(database)
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
            Opts.Scheduler = std::make_shared<TComputeScheduler>();
        }
        Opts.Scheduler->SetForgetInterval(Opts.ForgetOverflowTimeout);
        Opts.Scheduler->ReportCounters(Opts.Counters);
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

            hFunc(TEvSchedulerDeregister, Handle);
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

    void Handle(TEvSchedulerDeregister::TPtr& ev) {
        if (ev->Get()->SchedulerEntity) {
            Opts.Scheduler->Deregister(ev->Get()->SchedulerEntity, TlsActivationContext->Monotonic());
        }
    }

    void Handle(TEvSchedulerNewPool::TPtr& ev) {
        Send(MakeKqpWorkloadServiceId(SelfId().NodeId()), new NWorkload::TEvSubscribeOnPoolChanges(ev->Get()->Database, ev->Get()->Pool));
    }

    void Handle(TEvPingPool::TPtr& ev) {
        Send(MakeKqpWorkloadServiceId(SelfId().NodeId()), new NWorkload::TEvSubscribeOnPoolChanges(ev->Get()->Database, ev->Get()->Pool));
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

            Opts.Scheduler->UpdateGroupShare(ev->Get()->PoolId, totalShare, TlsActivationContext->Monotonic(), resourceWeight);
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

} // namespace NKqp
} // namespace NKikimr

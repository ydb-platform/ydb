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

class TSchedulerEntity {
public:
    TSchedulerEntity() {}
    ~TSchedulerEntity() {}

    struct TGroupMutableStats {
        double Weight = 0;
        TMonotonic LastNowRecalc;
        bool Disabled = false;
        i64 EntitiesWeight = 0;
        double MaxDeviation = 0;
        double MaxLimitDeviation = 0;

        ssize_t TrackedBefore = 0;

        double Limit(TMonotonic now) const {
            return FromDuration(now - LastNowRecalc) * Weight + MaxLimitDeviation + TrackedBefore;
        }
    };

    struct TGroupRecord {
        std::atomic<i64> TrackedMicroSeconds = 0;
        std::atomic<i64> DelayedSumBatches = 0;
        std::atomic<i64> DelayedCount = 0;

        double Share;

        ::NMonitoring::TDynamicCounters::TCounterPtr Vtime;
        ::NMonitoring::TDynamicCounters::TCounterPtr EntitiesWeight;
        ::NMonitoring::TDynamicCounters::TCounterPtr Limit;
        ::NMonitoring::TDynamicCounters::TCounterPtr Weight;

        ::NMonitoring::TDynamicCounters::TCounterPtr SchedulerClock;
        ::NMonitoring::TDynamicCounters::TCounterPtr SchedulerLimitUs;
        ::NMonitoring::TDynamicCounters::TCounterPtr SchedulerTrackedUs;

        TString Name;

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

    TGroupRecord* Group;
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
        auto group = Group->MutableStats.Current();
        Group->TrackedMicroSeconds.fetch_add(time.MicroSeconds());
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

    TMaybe<TDuration> GroupDelay(TMonotonic now) {
        auto group = Group->MutableStats.Current();
        auto limit = group.get()->Limit(now);
        auto tracked = Group->TrackedMicroSeconds.load();
        //double Coeff = pow(WakeupDelay, Wakeups);
        if (limit > tracked) {
            return {};
        } else {
            return Min(MaxDelay, ToDuration(/*Coeff * */(tracked - limit +
                        Max<i64>(0, Group->DelayedSumBatches.load()) + BatchTime.MicroSeconds() +
                        ActivationPenalty.MicroSeconds() * (Group->DelayedCount.load() + 1) +
                        group.get()->MaxLimitDeviation) / group.get()->Weight));
        }
    }

    void MarkThrottled() {
        isThrottled = true;
        Group->DelayedSumBatches.fetch_add(BatchTime.MicroSeconds());
        Group->DelayedCount.fetch_add(1);
    }

    void MarkResumed() {
        isThrottled = false;
        Group->DelayedSumBatches.fetch_sub(BatchTime.MicroSeconds());
        Group->DelayedCount.fetch_sub(1);
    }
};

struct TComputeScheduler::TImpl {
    THashMap<TString, size_t> PoolId;
    std::vector<std::unique_ptr<TSchedulerEntity::TGroupRecord>> Records;

    double SumCores;

    TIntrusivePtr<TKqpCounters> Counters;
    TDuration SmoothPeriod = TDuration::MilliSeconds(100);
    TDuration ForgetInteval = TDuration::Seconds(2);

    TDuration MaxDelay = TDuration::Seconds(10);

    void AssignWeight(TSchedulerEntity::TGroupRecord* record) {
        record->MutableStats.Next()->Weight = SumCores * record->Share;
    }

    void AssignWeights() {
        for (auto& record : Records) {
            AssignWeight(record.get());
        }
    }

    void CreateGroup(TString groupName, double maxShare) {
        PoolId[groupName] = Records.size();
        auto group = std::make_unique<TSchedulerEntity::TGroupRecord>();
        group->Share = maxShare;
        group->Name = groupName;
        AssignWeight(group.get());
        Records.push_back(std::move(group));
    }

    void CompactGroups() {
        std::vector<i64> remap;
        std::vector<std::unique_ptr<TSchedulerEntity::TGroupRecord>> records;

        std::vector<::NMonitoring::TDynamicCounters::TCounterPtr> vtimeCounters;
        std::vector<::NMonitoring::TDynamicCounters::TCounterPtr> entitiesWeightCounters;
        std::vector<::NMonitoring::TDynamicCounters::TCounterPtr> limitCounters;
        std::vector<::NMonitoring::TDynamicCounters::TCounterPtr> weightCounters;

        std::vector<::NMonitoring::TDynamicCounters::TCounterPtr> schedulerClock;
        std::vector<::NMonitoring::TDynamicCounters::TCounterPtr> schedulerLimitUs;
        std::vector<::NMonitoring::TDynamicCounters::TCounterPtr> schedulerTrackedUs;

        for (size_t i = 0; i < Records.size(); ++i) {
            auto record = Records[i]->MutableStats.Current();
            if (record.get()->Disabled || record.get()->EntitiesWeight == 0) {
                // to delete
                remap.push_back(-1);
            } else {
                records.emplace_back(std::move(Records[i]));
                remap.push_back(i);
            }
        }

        Records.swap(records);

        for (auto& [k, v] : PoolId) {
            if (remap[v] >= 0) {
                v = remap[v];
            }
        }
    }
};

TComputeScheduler::TComputeScheduler() {
    Impl = std::make_unique<TImpl>();
}

TComputeScheduler::~TComputeScheduler() = default;

TSchedulerEntityHandle TComputeScheduler::Enroll(TString groupName, i64 weight, TMonotonic now) {
    Y_ENSURE(Impl->PoolId.contains(groupName), "unknown scheduler group");
    auto* groupEntry = Impl->Records[Impl->PoolId.at(groupName)].get();
    groupEntry->MutableStats.Next()->EntitiesWeight += weight;
    Impl->AssignWeights();
    AdvanceTime(now);

    auto result = std::make_unique<TSchedulerEntity>();
    result->Group = groupEntry;
    result->Weight = weight;
    result->MaxDelay = Impl->MaxDelay;

    return TSchedulerEntityHandle(result.release());
}

void TComputeScheduler::AdvanceTime(TMonotonic now) {
    for (size_t i = 0; i < Impl->Records.size(); ++i) {
        auto* record = Impl->Records[i].get();
        if (Impl->Counters) {
            record->InitCounters(Impl->Counters);
        }
        auto& v = record->MutableStats;
        {
            auto group = v.Current();
            if (group.get()->LastNowRecalc > now) {
                continue;
            }
            double delta = 0;

            auto tracked = Impl->Records[i]->TrackedMicroSeconds.load();
            v.Next()->MaxLimitDeviation = Impl->SmoothPeriod.MicroSeconds() * v.Next()->Weight;
            v.Next()->LastNowRecalc = now;
            v.Next()->TrackedBefore = 
                Max<ssize_t>(
                    tracked - FromDuration(Impl->ForgetInteval) * group.get()->Weight, 
                    Min<ssize_t>(group.get()->Limit(now) - group.get()->MaxLimitDeviation, tracked));

            if (!group.get()->Disabled && group.get()->EntitiesWeight > 0) {
                delta = FromDuration(now - group.get()->LastNowRecalc) * group.get()->Weight / group.get()->EntitiesWeight;
                v.Next()->MaxDeviation = (FromDuration(Impl->SmoothPeriod) * v.Next()->Weight) / v.Next()->EntitiesWeight;
            }

            if (Impl->Records[i]->Vtime) {
                record->SchedulerLimitUs->Set(group.get()->Limit(now));
                record->SchedulerTrackedUs->Set(Impl->Records[i]->TrackedMicroSeconds.load());
                record->SchedulerClock->Add(now.MicroSeconds() - group.get()->LastNowRecalc.MicroSeconds());
                record->Vtime->Add(delta);
                record->EntitiesWeight->Set(v.Next()->EntitiesWeight);
                record->Limit->Add(FromDuration(now - group.get()->LastNowRecalc) * group.get()->Weight);
                record->Weight->Set(group.get()->Weight);
            }
        }
        v.Publish();
    }
    Impl->CompactGroups();
}

void TComputeScheduler::Deregister(TSchedulerEntity& self, TMonotonic now) {
    auto* group = self.Group->MutableStats.Next();
    group->EntitiesWeight -= self.Weight;

    Impl->AssignWeights();
    AdvanceTime(now);
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
    auto ptr = Impl->PoolId.FindPtr(group);
    return !ptr || Impl->Records[*ptr]->MutableStats.Current().get()->Disabled;
}


bool TComputeScheduler::Disable(TString group, TMonotonic now) {
    auto ptr = Impl->PoolId.FindPtr(group);
    if (Impl->Records[*ptr]->MutableStats.Current().get()->Weight > 0) {
        return false;
    }
    Impl->Records[*ptr]->MutableStats.Next()->Disabled = true;
    AdvanceTime(now);
    return true;
}

void TComputeScheduler::UpdateMaxShare(TString group, double share, TMonotonic now) {
    auto ptr = Impl->PoolId.FindPtr(group);
    if (!ptr) {
        Impl->CreateGroup(group, share);
    } else {
        auto& record = Impl->Records[*ptr];
        record->Share = share;
    }
    Impl->AssignWeights();
    AdvanceTime(now);
}

void TComputeScheduler::SetCapacity(ui64 cores) {
    Impl->SumCores = cores;
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
            Opts.Scheduler->Deregister(*ev->Get()->SchedulerEntity, TlsActivationContext->Monotonic());
        }
    }

    void Handle(TEvSchedulerNewPool::TPtr& ev) {
        Send(MakeKqpWorkloadServiceId(SelfId().NodeId()), new NWorkload::TEvSubscribeOnPoolChanges(ev->Get()->Database, ev->Get()->Pool));
        Opts.Scheduler->UpdateMaxShare(ev->Get()->Pool, ev->Get()->MaxShare, TlsActivationContext->Monotonic());
    }

    void Handle(TEvPingPool::TPtr& ev) {
        Send(MakeKqpWorkloadServiceId(SelfId().NodeId()), new NWorkload::TEvSubscribeOnPoolChanges(ev->Get()->Database, ev->Get()->Pool));
    }

    void Handle(NWorkload::TEvUpdatePoolInfo::TPtr& ev) {
        if (ev->Get()->Config.has_value()) {
            Opts.Scheduler->UpdateMaxShare(ev->Get()->PoolId, ev->Get()->Config->TotalCpuLimitPercentPerNode / 100.0, TlsActivationContext->Monotonic());
        } else {
            if (!Opts.Scheduler->Disable(ev->Get()->PoolId, TlsActivationContext->Monotonic())) {
                Schedule(Opts.ActivePoolPollingTimeout.ToDeadLine(), new TEvPingPool(ev->Get()->Database, ev->Get()->PoolId));
            }
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

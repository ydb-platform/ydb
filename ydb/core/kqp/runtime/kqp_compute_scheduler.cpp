#include "kqp_compute_scheduler.h"

namespace {
    static constexpr ui64 FromDuration(TDuration d) {
        return d.MicroSeconds();
    }

    static constexpr TDuration ToDuration(double t) {
        return TDuration::MicroSeconds(t);
    }

    static constexpr double MinEntitiesWeight = 1e-8;

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
        double EntitiesWeight = 0;
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

        TMultithreadPublisher<TGroupMutableStats> MutableStats;
    };

    TGroupRecord* Group;
    double Weight;
    double Vruntime = 0;
    double Vstart;

    double Vcurrent;

    TDuration MaxDelay;

    static constexpr double WakeupDelay = 1.1;
    static constexpr double BatchCalcDecay = 0;
    TDuration BatchTime = AvgBatch;

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
    TVector<::NMonitoring::TDynamicCounters::TCounterPtr> VtimeCounters;
    TVector<::NMonitoring::TDynamicCounters::TCounterPtr> EntitiesWeightCounters;
    TVector<::NMonitoring::TDynamicCounters::TCounterPtr> LimitCounters;
    TVector<::NMonitoring::TDynamicCounters::TCounterPtr> WeightCounters;

    TVector<::NMonitoring::TDynamicCounters::TCounterPtr> SchedulerClock;
    TVector<::NMonitoring::TDynamicCounters::TCounterPtr> SchedulerLimitUs;
    TVector<::NMonitoring::TDynamicCounters::TCounterPtr> SchedulerTrackedUs;

    THashMap<TString, size_t> PoolId;
    std::vector<std::unique_ptr<TSchedulerEntity::TGroupRecord>> Records;

    struct TRule {
        size_t Parent;
        double Weight = 0;

        double Share;
        TMaybe<size_t> RecordId = {};
        double SubRulesSum = 0;
        bool Empty = true;
    };
    std::vector<TRule> Rules;

    double SumCores;

    TIntrusivePtr<TKqpCounters> Counters;
    TDuration SmoothPeriod = TDuration::MilliSeconds(100);

    TDuration MaxDelay = TDuration::Seconds(10);

    void AssignWeights() {
        ssize_t rootRule = static_cast<ssize_t>(Rules.size()) - 1;
        for (size_t i = 0; i < Rules.size(); ++i) {
            Rules[i].SubRulesSum = 0;
            Rules[i].Empty = true;
        }
        for (ssize_t i = 0; i < static_cast<ssize_t>(Rules.size()); ++i) {
            if (Rules[i].RecordId) {
                Rules[i].Empty = Records[*Rules[i].RecordId]->MutableStats.Next()->EntitiesWeight < MinEntitiesWeight;
                Rules[i].SubRulesSum = Rules[i].Share;
            }
            if (i != rootRule && !Rules[i].Empty) {
                Rules[Rules[i].Parent].Empty = false;
                Rules[Rules[i].Parent].SubRulesSum += Rules[i].SubRulesSum;
            }
        }
        for (ssize_t i = static_cast<ssize_t>(Rules.size()) - 1; i >= 0; --i) {
            if (i == static_cast<ssize_t>(Rules.size()) - 1) {
                Rules[i].Weight = SumCores * Rules[i].Share;
            } else if (!Rules[i].Empty) {
                Rules[i].Weight = Rules[Rules[i].Parent].Weight * Rules[i].Share / Rules[Rules[i].Parent].SubRulesSum;
            } else {
                Rules[i].Weight = 0;
            }
            if (Rules[i].RecordId) {
                Records[*Rules[i].RecordId]->MutableStats.Next()->Weight = Rules[i].Weight;
            }
        }
     }
};

TComputeScheduler::TComputeScheduler() {
    Impl = std::make_unique<TImpl>();
}

TComputeScheduler::~TComputeScheduler() = default;

void TComputeScheduler::SetPriorities(TDistributionRule rule, double cores, TMonotonic now) {
    THashSet<TString> seenNames;
    std::function<void(TDistributionRule&)> exploreNames = [&](TDistributionRule& rule) {
        if (rule.SubRules.empty()) {
            seenNames.insert(rule.Name);
        } else {
            for (auto& subRule : rule.SubRules) {
                exploreNames(subRule);
            }
        }
    };
    exploreNames(rule);

    for (auto& k : seenNames) {
        auto ptr = Impl->PoolId.FindPtr(k);
        if (!ptr) {
            Impl->PoolId[k] = Impl->Records.size();
            auto group = std::make_unique<TSchedulerEntity::TGroupRecord>();
            group->MutableStats.Next()->LastNowRecalc = now;
            Impl->Records.push_back(std::move(group));
        }
    }
    for (auto& [k, v] : Impl->PoolId) {
        if (!seenNames.contains(k)) {
            auto& group = Impl->Records[Impl->PoolId[k]]->MutableStats;
            group.Next()->Weight = 0;
            group.Next()->Disabled = true;
            group.Publish();
        }
    }
    Impl->SumCores = cores;

    TVector<TImpl::TRule> rules;
    std::function<size_t(TDistributionRule&)> makeRules = [&](TDistributionRule& rule) {
        size_t result;
        if (rule.SubRules.empty()) {
            result = rules.size();
            rules.push_back(TImpl::TRule{.Share = rule.Share, .RecordId=Impl->PoolId[rule.Name]});
        } else {
            TVector<size_t> toAssign;
            for (auto& subRule : rule.SubRules) {
                toAssign.push_back(makeRules(subRule));
            }
            size_t result = rules.size();
            rules.push_back(TImpl::TRule{.Share = rule.Share});
            for (auto i : toAssign) {
                rules[i].Parent = result;
            }
            return result;
        }
        return result;
    };
    makeRules(rule);
    Impl->Rules.swap(rules);

    Impl->AssignWeights();
    for (auto& record : Impl->Records) {
        record->MutableStats.Publish();
    }
}


TSchedulerEntityHandle TComputeScheduler::Enroll(TString groupName, double weight, TMonotonic now) {
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
    if (Impl->Counters) {
        if (Impl->VtimeCounters.size() < Impl->Records.size()) {
            Impl->VtimeCounters.resize(Impl->Records.size());
            Impl->EntitiesWeightCounters.resize(Impl->Records.size());
            Impl->LimitCounters.resize(Impl->Records.size());
            Impl->WeightCounters.resize(Impl->Records.size());
            Impl->SchedulerClock.resize(Impl->Records.size());
            Impl->SchedulerLimitUs.resize(Impl->Records.size());
            Impl->SchedulerTrackedUs.resize(Impl->Records.size());

            for (auto& [k, i] : Impl->PoolId) {
                auto group = Impl->Counters->GetKqpCounters()->GetSubgroup("NodeScheduler/Group", k);
                Impl->VtimeCounters[i] = group->GetCounter("VTime", true);
                Impl->EntitiesWeightCounters[i] = group->GetCounter("Entities", false);
                Impl->LimitCounters[i] = group->GetCounter("Limit", true);
                Impl->WeightCounters[i] = group->GetCounter("Weight", false);
                Impl->SchedulerClock[i] = group->GetCounter("Clock", false);
                Impl->SchedulerTrackedUs[i] = group->GetCounter("Tracked", true);
                Impl->SchedulerLimitUs[i] = group->GetCounter("AbsoluteLimit", true);
            }
        }
    }
    for (size_t i = 0; i < Impl->Records.size(); ++i) {
        auto& v = Impl->Records[i]->MutableStats;
        {
            auto group = v.Current();
            if (group.get()->LastNowRecalc > now) {
                continue;
            }
            double delta = 0;

            v.Next()->TrackedBefore = Impl->Records[i]->TrackedMicroSeconds.load();
            v.Next()->MaxLimitDeviation = Impl->SmoothPeriod.MicroSeconds() * v.Next()->Weight;
            v.Next()->LastNowRecalc = now;
            v.Next()->TrackedBefore = Min<ssize_t>(group.get()->Limit(now) - group.get()->MaxLimitDeviation, v.Next()->TrackedBefore);

            if (!group.get()->Disabled && group.get()->EntitiesWeight > MinEntitiesWeight) {
                delta = FromDuration(now - group.get()->LastNowRecalc) * group.get()->Weight / group.get()->EntitiesWeight;
                v.Next()->MaxDeviation = (FromDuration(Impl->SmoothPeriod) * v.Next()->Weight) / v.Next()->EntitiesWeight;
            }

            if (Impl->VtimeCounters.size() > i && Impl->VtimeCounters[i]) {
                Impl->SchedulerLimitUs[i]->Set(group.get()->Limit(now));
                Impl->SchedulerTrackedUs[i]->Set(Impl->Records[i]->TrackedMicroSeconds.load());
                Impl->SchedulerClock[i]->Add(now.MicroSeconds() - group.get()->LastNowRecalc.MicroSeconds());
                Impl->VtimeCounters[i]->Add(delta);
                Impl->EntitiesWeightCounters[i]->Set(v.Next()->EntitiesWeight);
                Impl->LimitCounters[i]->Add(FromDuration(now - group.get()->LastNowRecalc) * group.get()->Weight);
                Impl->WeightCounters[i]->Set(group.get()->Weight);
            }
        }
        v.Publish();
    }
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

bool TComputeScheduler::Disabled(TString group) {
    auto ptr = Impl->PoolId.FindPtr(group);
    return !ptr || Impl->Records[*ptr]->MutableStats.Current().get()->Disabled;
}


::NMonitoring::TDynamicCounters::TCounterPtr TComputeScheduler::GetGroupUsageCounter(TString group) const {
    return Impl->Counters
        ->GetKqpCounters()
        ->GetSubgroup("NodeScheduler/Group", group)
        ->GetCounter("Usage", true);
}

} // namespace NKqp
} // namespace NKikimr

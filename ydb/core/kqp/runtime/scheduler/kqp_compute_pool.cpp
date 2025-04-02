#include "kqp_compute_pool.h"

#include "kqp_compute_scheduler.h"

namespace NKikimr::NKqp::NScheduler {

namespace {
    constexpr ui64 FromDuration(TDuration d) {
        return d.MicroSeconds();
    }
} // namespace

double TPool::TMutableStats::Limit(TMonotonic now) const {
    return FromDuration(now - LastNowRecalc) * Capacity + MaxLimitDeviation + TrackedBefore;
}

// TPool

TPool::TPool(const TString& name, THolder<IObservableValue<double>> share, TIntrusivePtr<TKqpCounters> counters)
    : Name(name)
    , Share(std::move(share))
{
    InitCounters(counters);
}

void TPool::AddEntity(THolder<TSchedulerEntity>& entity) {
    MutableStats.Next()->EntitiesWeight += entity->Weight;
}

void TPool::RemoveEntity(THolder<TSchedulerEntity>& entity) {
    MutableStats.Next()->EntitiesWeight -= entity->Weight;
}

void TPool::Enable() {

}

void TPool::Disable() {
    MutableStats.Next()->Disabled = true;
}

bool TPool::IsDisabled() const {
    return MutableStats.Current().get()->Disabled;
}

void TPool::AdvanceTime(TMonotonic now, TDuration smoothPeriod, TDuration forgetInterval) {
    MutableStats.Next()->Capacity = Share->GetValue();

    // auto& v = MutableStats;

    auto* current = MutableStats.Current().get();
    auto* next = MutableStats.Next();

    if (current->LastNowRecalc > now) {
        return;
    }

    auto delta = now - current->LastNowRecalc;
    ThrottledMicroSeconds.fetch_add(delta.MicroSeconds() * DelayedCount.load());

    auto tracked = TrackedMicroSeconds.load();
    next->MaxLimitDeviation = smoothPeriod.MicroSeconds() * next->Capacity;
    next->LastNowRecalc = now;
    next->TrackedBefore =
        Max<ssize_t>(
            tracked - FromDuration(forgetInterval) * current->Capacity,
            Min<ssize_t>(current->Limit(now) - current->MaxLimitDeviation, tracked));

    SchedulerLimitUs->Set(current->Limit(now));
    SchedulerClock->Add(now.MicroSeconds() - current->LastNowRecalc.MicroSeconds());
    Vtime->Add(delta.MicroSeconds());
    EntitiesWeight->Set(next->EntitiesWeight);
    OldLimit->Add(FromDuration(now - current->LastNowRecalc) * current->Capacity);
    Weight->Set(current->Capacity);

    // TODO: set limit once - somewhere else.
    // auto* shareValue = WeightsUpdater.FindValue<TParameter<double>>({Name, TImpl::TotalShare});
    // Limit->Set(shareValue->GetValue() * SumCores.GetValue() * 1'000'000);
    Usage->Set(TrackedMicroSeconds.load());
    Demand->Set(EntitiesCount.load() * 1'000'000);
    Throttle->Set(ThrottledMicroSeconds.load());

    MutableStats.Publish();
}

void TPool::UpdateGuarantee(ui64 value) {
    Guarantee->Set(value);
}

const TString& TPool::GetName() const {
    return Name;
}

bool TPool::IsActive() const {
    return MutableStats.Current().get()->EntitiesWeight > 0 || Share->HasDependents();
}

void TPool::InitCounters(const TIntrusivePtr<TKqpCounters>& counters) {
    auto group = counters->GetKqpCounters()->GetSubgroup("NodeScheduler/Group", Name);
    Vtime = group->GetCounter("VTime", true);
    EntitiesWeight = group->GetCounter("Entities", false);
    OldLimit = group->GetCounter("OldLimit", true);
    Weight = group->GetCounter("Weight", false);
    SchedulerClock = group->GetCounter("Clock", false);
    SchedulerLimitUs = group->GetCounter("AbsoluteLimit", true);

    // TODO: since counters don't support float-point values, then use CPU * 1'000'000 to account the microseconds precision
    Limit     = group->GetCounter("Limit",     false); // done
    Guarantee = group->GetCounter("Guarantee", false); // done
    Demand    = group->GetCounter("Demand",    false); // done
    Usage     = group->GetCounter("Usage",     true);  // done: also includes actor's system overhead (waiting on mailbox?)
    Throttle  = group->GetCounter("Throttle",  true);  // done
    FairShare = group->GetCounter("FairShare", false); // TODO
}

} // namespace NKikimr::NKqp::NScheduler

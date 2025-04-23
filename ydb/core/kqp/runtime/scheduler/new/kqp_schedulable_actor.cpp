#include "kqp_schedulable_actor.h"

#include "kqp_compute_tree.h"

namespace NKikimr::NKqp::NScheduler {

TSchedulableTask::TSchedulableTask(const NHdrf::TQueryPtr& query)
    : Query(query)
{
    Y_ENSURE(query);
    ++Query->Demand;
}

TSchedulableTask::~TSchedulableTask() {
    --Query->Demand;
}

void TSchedulableTask::IncreaseUsage(const TDuration& burstThrottle) {
    Y_ENSURE(Query);
    for (NHdrf::TTreeElementBase* parent = Query.get(); parent; parent = parent->Parent) {
        ++parent->Usage;
        parent->BurstThrottle += burstThrottle.MicroSeconds();
    }
}

void TSchedulableTask::DecreaseUsage(const TDuration& burstUsage) {
    for (NHdrf::TTreeElementBase* parent = Query.get(); parent; parent = parent->Parent) {
        --parent->Usage;
        parent->BurstUsage += burstUsage.MicroSeconds();
    }
}

TSchedulableActorHelper::TSchedulableActorHelper(TOptions&& options)
    : SchedulableTask(std::move(options.SchedulableTask))
{
}

// static
TMonotonic TSchedulableActorHelper::Now() {
    return TMonotonic::Now();
}

bool TSchedulableActorHelper::IsSchedulable() const {
    return !!SchedulableTask;
}

void TSchedulableActorHelper::StartExecution(const TDuration& burstThrottle) {
    Y_ASSERT(SchedulableTask);

    Timer.Reset();
    SchedulableTask->IncreaseUsage(burstThrottle);
}

void TSchedulableActorHelper::StopExecution() {
    Y_ASSERT(SchedulableTask);

    SchedulableTask->DecreaseUsage(TDuration::MicroSeconds(Timer.Passed() * 1'000'000));
}

std::optional<TDuration> TSchedulableActorHelper::CalculateDelay(TMonotonic) const {
    // TODO: no delays for now
    return {};
}

void TSchedulableActorHelper::AccountThrottledTime(TMonotonic) {
    // TODO: no throttled time for now
}

} // namespace NKikimr::NKqp::NScheduler

#include "kqp_schedulable_task.h"

#include <ydb/core/kqp/runtime/scheduler/tree/dynamic.h>

namespace NKikimr::NKqp::NScheduler {

using namespace NHdrf::NDynamic;

TSchedulableTask::TSchedulableTask(const TQueryPtr& query)
    : Query(query)
{
    Y_ENSURE(query);
    ++Query->Demand;
}

TSchedulableTask::~TSchedulableTask() {
    if (Iterator) {
        Query->RemoveTask(*Iterator);
    }
    --Query->Demand;
}

void TSchedulableTask::RegisterForResume(const TActorId& actorId) {
    Y_ENSURE(!Iterator);
    Iterator = Query->AddTask(shared_from_this());
    ActorId = actorId;
}

void TSchedulableTask::Resume() {
    NActors::TActivationContext::Send(ActorId, GetResumeEvent());
}

// TODO: referring to the pool's fair-share and usage - query's fair-share is ignored.
bool TSchedulableTask::TryIncreaseUsage() {
    const auto snapshot = Query->GetSnapshot();
    auto pool = Query->GetParent();
    ui64 newUsage = pool->Usage.load();
    bool increased = false;

    while (!increased && newUsage < snapshot->GetParent()->FairShare) {
        increased = pool->Usage.compare_exchange_weak(newUsage, newUsage + 1);
    }

    if (!increased) {
        return false;
    }

    ++Query->Usage;
    for (TTreeElement* parent = pool->GetParent(); parent; parent = parent->GetParent()) {
        ++parent->Usage;
    }

    return true;
}

// TODO: referring to the pool's fair-share and usage - query's fair-share is ignored.
void TSchedulableTask::IncreaseUsage() {
    for (TTreeElement* parent = Query.get(); parent; parent = parent->GetParent()) {
        ++parent->Usage;
    }
}

// TODO: referring to the pool's fair-share and usage - query's fair-share is ignored.
void TSchedulableTask::DecreaseUsage(const TDuration& burstUsage, bool forcedResume) {
    for (TTreeElement* parent = Query.get(); parent; parent = parent->GetParent()) {
        --parent->Usage;
        if (forcedResume) {
            parent->BurstUsageResume += burstUsage.MicroSeconds();
        } else {
            parent->BurstUsage += burstUsage.MicroSeconds();
        }
    }
}

size_t TSchedulableTask::GetSpareUsage() const {
    if (const auto snapshot = Query->GetSnapshot()) {
        auto usage = Query->GetParent()->Usage.load();
        auto fairShare = snapshot->GetParent()->FairShare;
        return fairShare >= usage ? (fairShare - usage) : 0;
    }

    return 0;
}

void TSchedulableTask::IncreaseExtraUsage() {
    for (TTreeElement* parent = Query.get(); parent; parent = parent->GetParent()) {
        ++parent->UsageExtra;
    }
}

void TSchedulableTask::DecreaseExtraUsage(const TDuration& burstUsageExtra) {
    for (TTreeElement* parent = Query.get(); parent; parent = parent->GetParent()) {
        --parent->UsageExtra;
        parent->BurstUsageExtra += burstUsageExtra.MicroSeconds();
    }
}

void TSchedulableTask::IncreaseBurstThrottle(const TDuration& burstThrottle) {
    for (TTreeElement* parent = Query.get(); parent; parent = parent->GetParent()) {
        parent->BurstThrottle += burstThrottle.MicroSeconds();
    }
}

void TSchedulableTask::IncreaseThrottle() {
    if (Iterator) {
        (*Iterator)->second = true;
    }
    for (TTreeElement* parent = Query.get(); parent; parent = parent->GetParent()) {
        ++parent->Throttle;
    }
}

void TSchedulableTask::DecreaseThrottle() {
    if (Iterator) {
        (*Iterator)->second = false;
    }
    for (TTreeElement* parent = Query.get(); parent; parent = parent->GetParent()) {
        --parent->Throttle;
    }
}

} // namespace NKikimr::NKqp::NScheduler

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

// TODO: referring to the pool's usage - to support all-equal fair-share query mode.
bool TSchedulableTask::TryIncreaseUsage() {
    bool increased = false;
    ui64 fairShare = 0;
    NHdrf::NDynamic::TTreeElement* poolOrQuery = nullptr;

    if (const auto snapshot = Query->GetSnapshot()) {
        fairShare = snapshot->FairShare;
        poolOrQuery = Query->GetParent();
    } else { // TODO: check directly for the pool snapshot - even if there is no query snapshot yet.
        fairShare = Query->AllowMinFairShare;
        poolOrQuery = Query.get();
    }

    ui64 newUsage = poolOrQuery->Usage.load();

    while (!increased && newUsage < fairShare) {
        increased = poolOrQuery->Usage.compare_exchange_weak(newUsage, newUsage + 1);
    }

    if (!increased) {
        return false;
    }

    for (TTreeElement* parent = poolOrQuery; parent; parent = parent->GetParent()) {
        if (parent != poolOrQuery) {
            ++parent->Usage;
        }
    }

    return true;
}

void TSchedulableTask::IncreaseUsage() {
    for (TTreeElement* parent = Query.get(); parent; parent = parent->GetParent()) {
        ++parent->Usage;
    }
}

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
        auto usage = Query->GetParent()->Usage.load(std::memory_order_relaxed);
        auto fairShare = snapshot->FairShare;
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

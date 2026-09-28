#include "kqp_schedulable_task.h"

#include <ydb/core/kqp/runtime/scheduler/tree/dynamic.h>

namespace NKikimr::NKqp::NScheduler {

using namespace NHdrf::NDynamic;

TSchedulableTask::TSchedulableTask(const TQueryPtr& query)
    : Query(query)
{
    Y_ENSURE(query);
    ++Query->CpuMaxDemand;
}

TSchedulableTask::~TSchedulableTask() {
    --Query->CpuMaxDemand;
}

void TSchedulableTask::RegisterForResume(const TActorId& actorId) {
    Y_ENSURE(!Iterator);
    Iterator = Query->AddTask(shared_from_this());
    ActorId = actorId;
}

void TSchedulableTask::Resume() {
    NActors::TActivationContext::Send(ActorId, GetResumeEvent());
}

bool TSchedulableTask::TryIncreaseUsage() {
    bool increased = false;
    ui64 fairShare = 0;
    NHdrf::NDynamic::TTreeElement* poolOrQuery = nullptr;

    if (const auto snapshot = Query->GetSnapshot()) {
        fairShare = snapshot->FairShare;
        poolOrQuery = Query->GetParent();

        // Special case for zero max demand and zero fair-share - there are pending tasks but snapshot is not updated yet.
        if (fairShare == 0 && snapshot->CpuMaxDemand == 0) {
            auto prevMaxDemand = snapshot->CpuMaxDemand.fetch_add(1);
            if (prevMaxDemand == 0) {
                fairShare = Query->AllowMinFairShare;
            }
        }
    } else { // TODO: check directly for the pool snapshot - even if there is no query snapshot yet.
        fairShare = Query->AllowMinFairShare;
        poolOrQuery = Query.get();
    }

    ui64 newUsage = poolOrQuery->CpuUsage.load();

    while (!increased && newUsage < fairShare) {
        increased = poolOrQuery->CpuUsage.compare_exchange_weak(newUsage, newUsage + 1);
    }

    if (!increased) {
        return false;
    }

    // Always start from the query itself to keep usage symmetric with DecreaseUsage().
    for (TTreeElement* parent = Query.get(); parent; parent = parent->GetParent()) {
        if (parent != poolOrQuery) {
            ++parent->CpuUsage;
        }
    }

    return true;
}

void TSchedulableTask::IncreaseUsage() {
    for (TTreeElement* parent = Query.get(); parent; parent = parent->GetParent()) {
        ++parent->CpuUsage;
    }
}

void TSchedulableTask::DecreaseUsage(TDuration burstUsage, EUsageType usageType) {
    for (TTreeElement* parent = Query.get(); parent; parent = parent->GetParent()) {
        --parent->CpuUsage;
        switch(usageType) {
            case CPU_DEFAULT:
                parent->CpuBurstUsage += burstUsage.MicroSeconds();
                break;
            case CPU_RESUMED:
                parent->CpuBurstUsageResume += burstUsage.MicroSeconds();
                break;
            case READ_DEFAULT:
                parent->ReadBurstUsage += burstUsage.MicroSeconds();
                break;
        }
    }
}

size_t TSchedulableTask::GetSpareUsage() const {
    if (const auto snapshot = Query->GetSnapshot()) {
        auto usage = Query->GetParent()->CpuUsage.load(std::memory_order_relaxed);
        auto fairShare = snapshot->FairShare;
        return fairShare >= usage ? (fairShare - usage) : 0;
    }

    return 0;
}

void TSchedulableTask::IncreaseBurstThrottle(TDuration burstThrottle) {
    for (TTreeElement* parent = Query.get(); parent; parent = parent->GetParent()) {
        parent->CpuBurstThrottle += burstThrottle.MicroSeconds();
    }
}

void TSchedulableTask::IncreaseThrottle() {
    if (Iterator) {
        (*Iterator)->second = true;
    }

    for (TTreeElement* parent = Query.get(); parent; parent = parent->GetParent()) {
        ++parent->CpuThrottle;
    }
}

void TSchedulableTask::DecreaseThrottle() {
    if (Iterator) {
        (*Iterator)->second = false;
    }
    for (TTreeElement* parent = Query.get(); parent; parent = parent->GetParent()) {
        --parent->CpuThrottle;
    }
}

} // namespace NKikimr::NKqp::NScheduler

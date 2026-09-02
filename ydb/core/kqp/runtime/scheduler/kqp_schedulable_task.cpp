#include "kqp_schedulable_task.h"

#include <ydb/core/kqp/runtime/scheduler/tree/dynamic.h>

namespace NKikimr::NKqp::NScheduler {

using namespace NHdrf::NDynamic;

TSchedulableTask::TSchedulableTask(const TQueryPtr& query)
    : Query(query)
{
    Y_ENSURE(query);
    ++Query->CpuDemand;
}

TSchedulableTask::~TSchedulableTask() {
    --Query->CpuDemand;
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
    ui64 queryFairShare = 0;
    ui64 poolFairShare = 0;
    NHdrf::NDynamic::TTreeElement* query = Query.get();
    NHdrf::NDynamic::TTreeElement* pool = Query->GetParent();

    if (const auto snapshot = Query->GetSnapshot()) {
        queryFairShare = Min(snapshot->FairShare, Query->GetCpuLimit());
        poolFairShare = snapshot->ParentFairShare;

        // Special case for zero demand and zero fair-share - there are pending tasks but snapshot is not updated yet.
        if (queryFairShare == 0 && snapshot->CpuDemand == 0 && Query->GetCpuLimit() > 0) {
            auto prevDemand = snapshot->CpuDemand.fetch_add(1);
            if (prevDemand == 0) {
                queryFairShare = Query->AllowMinFairShare;
                poolFairShare = Max(poolFairShare, queryFairShare);
            }
        }
    } else { // TODO: check directly for the pool snapshot - even if there is no query snapshot yet.
        queryFairShare = Query->GetCpuLimit() > 0 ? Query->AllowMinFairShare : 0;
        poolFairShare = queryFairShare;
    }

    const auto tryIncrease = [](TTreeElement* element, ui64 fairShare) {
        ui64 newUsage = element->CpuUsage.load();
        while (newUsage < fairShare) {
            if (element->CpuUsage.compare_exchange_weak(newUsage, newUsage + 1)) {
                return true;
            }
        }
        return false;
    };

    if (!tryIncrease(query, queryFairShare)) {
        return false;
    }
    if (!tryIncrease(pool, poolFairShare)) {
        --query->CpuUsage;
        return false;
    }
    for (TTreeElement* parent = pool->GetParent(); parent; parent = parent->GetParent()) {
        ++parent->CpuUsage;
    }

    Query->UpdateActualDemand();

    return true;
}

void TSchedulableTask::IncreaseUsage() {
    for (TTreeElement* parent = Query.get(); parent; parent = parent->GetParent()) {
        ++parent->CpuUsage;
    }
}

void TSchedulableTask::DecreaseUsage(const TDuration& burstUsage, EUsageType usageType) {
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
        const auto queryUsage = Query->CpuUsage.load(std::memory_order_relaxed);
        const auto queryFairShare = Min(snapshot->FairShare, Query->GetCpuLimit());
        const auto querySpare = queryFairShare >= queryUsage ? queryFairShare - queryUsage : 0;
        const auto poolUsage = Query->GetParent()->CpuUsage.load(std::memory_order_relaxed);
        const auto poolFairShare = snapshot->ParentFairShare;
        const auto poolSpare = poolFairShare >= poolUsage ? poolFairShare - poolUsage : 0;
        return Min(querySpare, poolSpare);
    }

    return 0;
}

void TSchedulableTask::IncreaseBurstThrottle(const TDuration& burstThrottle) {
    for (TTreeElement* parent = Query.get(); parent; parent = parent->GetParent()) {
        parent->CpuBurstThrottle += burstThrottle.MicroSeconds();
    }
}

void TSchedulableTask::IncreaseThrottle() {
    if (Iterator) {
        (*Iterator)->second = true;
    }

    Query->UpdateActualDemand();

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

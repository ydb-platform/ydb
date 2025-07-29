#include "actor.h"

#include <ydb/core/tx/columnshard/common/limits.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

void TMemoryLimiterActor::Bootstrap() {
    for (ui64 i = 0; i < Config.GetCountBuckets(); i++) {
        LoadQueue.Add(i);
        Managers.push_back(std::make_shared<TManager>(SelfId(), Config, Name, Signals, DefaultStage));
    }

    Send(NMemory::MakeMemoryControllerId(), new NMemory::TEvConsumerRegister(ConsumerKind));
    Become(&TThis::StateWait);
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvStartTask::TPtr& ev) {
    const size_t index = AcquireManager(ev->Get()->GetExternalProcessId());
    for (auto&& i : ev->Get()->GetAllocations()) {
        Managers[index]->RegisterAllocation(ev->Get()->GetExternalProcessId(), ev->Get()->GetExternalScopeId(), ev->Get()->GetExternalGroupId(), i,
            ev->Get()->GetStageFeaturesIdx());
    }
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvFinishTask::TPtr& ev) {
    const size_t index = ReleaseManager(ev->Get()->GetExternalProcessId());
    Managers[index]->UnregisterAllocation(ev->Get()->GetExternalProcessId(), ev->Get()->GetExternalScopeId(), ev->Get()->GetAllocationId());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvUpdateTask::TPtr& ev) {
    const size_t index = GetManager(ev->Get()->GetExternalProcessId());
    Managers[index]->UpdateAllocation(
        ev->Get()->GetExternalProcessId(), ev->Get()->GetExternalScopeId(), ev->Get()->GetAllocationId(), ev->Get()->GetVolume());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvFinishGroup::TPtr& ev) {
    const size_t index = ReleaseManager(ev->Get()->GetExternalProcessId());
    Managers[index]->UnregisterGroup(ev->Get()->GetExternalProcessId(), ev->Get()->GetExternalScopeId(), ev->Get()->GetExternalGroupId());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvStartGroup::TPtr& ev) {
    const size_t index = AcquireManager(ev->Get()->GetExternalProcessId());
    Managers[index]->RegisterGroup(ev->Get()->GetExternalProcessId(), ev->Get()->GetExternalScopeId(), ev->Get()->GetExternalGroupId());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvFinishProcess::TPtr& ev) {
    const size_t index = ReleaseManager(ev->Get()->GetExternalProcessId());
    Managers[index]->UnregisterProcess(ev->Get()->GetExternalProcessId());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvStartProcess::TPtr& ev) {
    const size_t index = AcquireManager(ev->Get()->GetExternalProcessId());
    Managers[index]->RegisterProcess(ev->Get()->GetExternalProcessId(), ev->Get()->GetStages());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvFinishProcessScope::TPtr& ev) {
    const size_t index = ReleaseManager(ev->Get()->GetExternalProcessId());
    Managers[index]->UnregisterProcessScope(ev->Get()->GetExternalProcessId(), ev->Get()->GetExternalScopeId());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvStartProcessScope::TPtr& ev) {
    const size_t index = AcquireManager(ev->Get()->GetExternalProcessId());
    Managers[index]->RegisterProcessScope(ev->Get()->GetExternalProcessId(), ev->Get()->GetExternalScopeId());
}

void TMemoryLimiterActor::Handle(NMemory::TEvConsumerRegistered::TPtr& ev) {
    MemoryConsumptionAggregator->SetConsumer(std::move(ev->Get()->Consumer));
    for (size_t i = 0; i < Managers.size(); ++i) {
        Managers[i]->SetMemoryConsumptionUpdateFunction([aggregator = this->MemoryConsumptionAggregator, i](ui64 consumption) {
            aggregator->SetConsumption(i, consumption);
        });
    }
}

void TMemoryLimiterActor::Handle(NMemory::TEvConsumerLimit::TPtr& ev) {
    const ui64 countBuckets = Config.GetCountBuckets() ? Config.GetCountBuckets() : 1;
    const ui64 limitBytes = ev->Get()->LimitBytes * NKikimr::NOlap::TGlobalLimits::GroupedMemoryLimiterSoftLimitCoefficient / countBuckets;
    const ui64 hardLimitBytes = ev->Get()->LimitBytes / countBuckets;
    for (auto& manager: Managers) {
        manager->UpdateMemoryLimits(limitBytes, hardLimitBytes);
    }
}

size_t TMemoryLimiterActor::AcquireManager(ui64 externalProcessId) {
    auto it = ProcessMapping.find(externalProcessId);
    if (it == ProcessMapping.end()) {
        size_t index = LoadQueue.Top();
        LoadQueue.ChangeLoad(index, +1);
        auto& stats = ProcessMapping[externalProcessId];
        stats.Counter++;
        stats.ManagerIndex = index;
        return index;
    }

    auto& stats = it->second;
    stats.Counter++;
    return stats.ManagerIndex;
}

size_t TMemoryLimiterActor::ReleaseManager(ui64 externalProcessId) {
    auto it = ProcessMapping.find(externalProcessId);
    AFL_VERIFY(it != ProcessMapping.end());
    size_t id = it->second.ManagerIndex;
    it->second.Counter--;
    if (it->second.Counter == 0) {
        LoadQueue.ChangeLoad(id, -1);
        ProcessMapping.erase(it);
    }
    return id;
}

size_t TMemoryLimiterActor::GetManager(ui64 externalProcessId) {
    auto it = ProcessMapping.find(externalProcessId);
    AFL_VERIFY(it != ProcessMapping.end());
    return it->second.ManagerIndex;
}



}   // namespace NKikimr::NOlap::NGroupedMemoryManager

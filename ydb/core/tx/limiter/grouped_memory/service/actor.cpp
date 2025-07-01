#include "actor.h"

namespace NKikimr::NOlap::NGroupedMemoryManager {

void TMemoryLimiterActor::Bootstrap() {
    for (ui64 i = 0; i < Config.GetCountBuckets(); i++) {
        LoadQueue.Add(i);
        Managers.push_back(std::make_shared<TManager>(SelfId(), Config, Name, Signals, std::make_shared<TStageFeatures>(*DefaultStage)));
    }

    Send(NMemory::MakeMemoryControllerId(), new NMemory::TEvConsumerRegister(ConsumerKind));
    Become(&TThis::StateWait);
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvStartTask::TPtr& ev) {
    auto it = ProcessMapping.find(ev->Get()->GetExternalProcessId());
    AFL_VERIFY(it != ProcessMapping.end());
    for (auto&& i : ev->Get()->GetAllocations()) {
        Managers[it->second.ManagerIndex]->RegisterAllocation(ev->Get()->GetExternalProcessId(), ev->Get()->GetExternalScopeId(), ev->Get()->GetExternalGroupId(), i,
            ev->Get()->GetStageFeaturesIdx());
    }
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvFinishTask::TPtr& ev) {
    auto it = ProcessMapping.find(ev->Get()->GetExternalProcessId());
    AFL_VERIFY(it != ProcessMapping.end());
    Managers[it->second.ManagerIndex]->UnregisterAllocation(ev->Get()->GetExternalProcessId(), ev->Get()->GetExternalScopeId(), ev->Get()->GetAllocationId());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvUpdateTask::TPtr& ev) {
    auto it = ProcessMapping.find(ev->Get()->GetExternalProcessId());
    AFL_VERIFY(it != ProcessMapping.end());
    Managers[it->second.ManagerIndex]->UpdateAllocation(
        ev->Get()->GetExternalProcessId(), ev->Get()->GetExternalScopeId(), ev->Get()->GetAllocationId(), ev->Get()->GetVolume());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvFinishGroup::TPtr& ev) {
    auto it = ProcessMapping.find(ev->Get()->GetExternalProcessId());
    AFL_VERIFY(it != ProcessMapping.end());
    Managers[it->second.ManagerIndex]->UnregisterGroup(ev->Get()->GetExternalProcessId(), ev->Get()->GetExternalScopeId(), ev->Get()->GetExternalGroupId());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvStartGroup::TPtr& ev) {
    auto it = ProcessMapping.find(ev->Get()->GetExternalProcessId());
    AFL_VERIFY(it != ProcessMapping.end());
    Managers[it->second.ManagerIndex]->RegisterGroup(ev->Get()->GetExternalProcessId(), ev->Get()->GetExternalScopeId(), ev->Get()->GetExternalGroupId());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvFinishProcess::TPtr& ev) {
    auto it = ProcessMapping.find(ev->Get()->GetExternalProcessId());
    AFL_VERIFY(it != ProcessMapping.end());
    size_t id = it->second.ManagerIndex;
    it->second.Counter--;
    if (it->second.Counter == 0) {
        LoadQueue.ChangeLoad(id, -1);
        ProcessMapping.erase(it);
    }
    Managers[id]->UnregisterProcess(ev->Get()->GetExternalProcessId());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvStartProcess::TPtr& ev) {
    auto it = ProcessMapping.find(ev->Get()->GetExternalProcessId());
    int index = 0;
    if (it == ProcessMapping.end()) {
        index = LoadQueue.Top();
        LoadQueue.ChangeLoad(index, +1);
        auto& stats = ProcessMapping[ev->Get()->GetExternalProcessId()];
        stats.ManagerIndex = index;
        stats.Counter++;
    } else {
        auto& stats = it->second;
        index = stats.ManagerIndex;
        stats.Counter++;
    }

    Managers[index]->RegisterProcess(ev->Get()->GetExternalProcessId(), ev->Get()->GetStages());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvFinishProcessScope::TPtr& ev) {
    auto it = ProcessMapping.find(ev->Get()->GetExternalProcessId());
    AFL_VERIFY(it != ProcessMapping.end());
    Managers[it->second.ManagerIndex]->UnregisterProcessScope(ev->Get()->GetExternalProcessId(), ev->Get()->GetExternalScopeId());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvStartProcessScope::TPtr& ev) {
    auto it = ProcessMapping.find(ev->Get()->GetExternalProcessId());
    AFL_VERIFY(it != ProcessMapping.end());
    Managers[it->second.ManagerIndex]->RegisterProcessScope(ev->Get()->GetExternalProcessId(), ev->Get()->GetExternalScopeId());
}

void TMemoryLimiterActor::Handle(NMemory::TEvConsumerRegistered::TPtr& ev) {
    for (auto& manager: Managers) {
        manager->SetMemoryConsumer(std::move(ev->Get()->Consumer));
    }
}

void TMemoryLimiterActor::Handle(NMemory::TEvConsumerLimit::TPtr& ev) {
    for (auto& manager: Managers) {
        manager->UpdateMemoryLimits(ev->Get()->LimitBytes / Config.GetCountBuckets(), ev->Get()->HardLimitBytes ? *ev->Get()->HardLimitBytes / Config.GetCountBuckets() : ev->Get()->HardLimitBytes);
    }
}

}   // namespace NKikimr::NOlap::NGroupedMemoryManager

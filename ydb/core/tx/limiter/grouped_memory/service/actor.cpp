#include "actor.h"

#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/limiter/grouped_memory/tracing/probes.h>

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

LWTRACE_USING(YDB_GROUPED_MEMORY_PROVIDER);

void TMemoryLimiterActor::Bootstrap() {
    NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(YDB_GROUPED_MEMORY_PROVIDER));
    for (ui64 i = 0; i < Config.GetCountBuckets(); i++) {
        LoadQueue.Add(i);
        Counters.push_back(std::make_shared<TCounters>(Signals, Name + "_" + ToString(i)));
        DefaultStages.push_back(std::make_shared<TStageFeatures>("GLOBAL", Config.GetMemoryLimit(), Config.GetHardMemoryLimit(), nullptr, Counters.back()->BuildStageCounters("general")));
        Managers.push_back(std::make_shared<TManager>(SelfId(), Config, Name, Counters.back(), DefaultStages.back()));
    }

    Send(NMemory::MakeMemoryControllerId(), new NMemory::TEvConsumerRegister(ConsumerKind));
    Become(&TThis::StateWait);
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvStartTask::TPtr& ev) {
    auto& event = *ev->Get();
    const size_t index = AcquireManager(event.GetExternalProcessId(), event.GetAllocations().size());
    for (auto&& i : event.GetAllocations()) {
        LWPROBE(StartTask, index, event.GetExternalProcessId(), event.GetExternalScopeId(), event.GetExternalGroupId(), event.GetStageFeaturesIdx().value_or(std::numeric_limits<ui32>::max()), i->GetIdentifier(), i->GetMemory(), event.GetAllocations().size(), LoadQueue.GetLoad(index));
        Managers[index]->RegisterAllocation(event.GetExternalProcessId(), event.GetExternalScopeId(), event.GetExternalGroupId(), i,
            event.GetStageFeaturesIdx());
    }
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvFinishTask::TPtr& ev) {
    auto& event = *ev->Get();
    const size_t index = ReleaseManager(event.GetExternalProcessId());
    LWPROBE(FinishTask, index, event.GetExternalProcessId(), event.GetExternalScopeId(), event.GetAllocationId(), LoadQueue.GetLoad(index));
    Managers[index]->UnregisterAllocation(event.GetExternalProcessId(), event.GetExternalScopeId(), event.GetAllocationId());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvTaskUpdated::TPtr& ev) {
    auto& event = *ev->Get();
    const size_t index = GetManager(event.GetExternalProcessId());
    LWPROBE(TaskUpdated, index, event.GetExternalProcessId(), event.GetExternalScopeId(), event.GetAllocationId(), LoadQueue.GetLoad(index));
    Managers[index]->AllocationUpdated(
        event.GetExternalProcessId(), event.GetExternalScopeId(), event.GetAllocationId());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvFinishGroup::TPtr& ev) {
    auto& event = *ev->Get();
    const size_t index = ReleaseManager(event.GetExternalProcessId());
    LWPROBE(FinishGroup, index, event.GetExternalProcessId(), event.GetExternalScopeId(), ev->Get()->GetExternalGroupId(), LoadQueue.GetLoad(index));
    Managers[index]->UnregisterGroup(event.GetExternalProcessId(), event.GetExternalScopeId(), event.GetExternalGroupId());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvStartGroup::TPtr& ev) {
    auto& event = *ev->Get();
    const size_t index = AcquireManager(event.GetExternalProcessId());
    LWPROBE(StartGroup, index, event.GetExternalProcessId(), event.GetExternalScopeId(), ev->Get()->GetExternalGroupId(), LoadQueue.GetLoad(index));
    Managers[index]->RegisterGroup(event.GetExternalProcessId(), event.GetExternalScopeId(), event.GetExternalGroupId());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvFinishProcess::TPtr& ev) {
    auto& event = *ev->Get();
    const size_t index = ReleaseManager(event.GetExternalProcessId());
    LWPROBE(FinishProcess, index, event.GetExternalProcessId(), LoadQueue.GetLoad(index));
    Managers[index]->UnregisterProcess(event.GetExternalProcessId());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvStartProcess::TPtr& ev) {
    auto& event = *ev->Get();
    const size_t index = AcquireManager(ev->Get()->GetExternalProcessId());
    LWPROBE(StartProcess, index, event.GetExternalProcessId(), LoadQueue.GetLoad(index));
    for (auto& stage : event.GetStages()) {
        stage->AttachOwner(DefaultStages[index]);
        stage->AttachCounters(Counters[index]->BuildStageCounters(stage->GetName()));
    }
    Managers[index]->RegisterProcess(event.GetExternalProcessId(), event.GetStages());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvFinishProcessScope::TPtr& ev) {
    auto& event = *ev->Get();
    const size_t index = ReleaseManager(event.GetExternalProcessId());
    LWPROBE(FinishProcessScope, index, event.GetExternalProcessId(), event.GetExternalScopeId(), LoadQueue.GetLoad(index));
    Managers[index]->UnregisterProcessScope(event.GetExternalProcessId(), event.GetExternalScopeId());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvStartProcessScope::TPtr& ev) {
    auto& event = *ev->Get();
    const size_t index = AcquireManager(event.GetExternalProcessId());
    LWPROBE(StartProcessScope, index, event.GetExternalProcessId(), event.GetExternalScopeId(), LoadQueue.GetLoad(index));
    Managers[index]->RegisterProcessScope(event.GetExternalProcessId(), event.GetExternalScopeId());
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

size_t TMemoryLimiterActor::AcquireManager(ui64 externalProcessId, int delta) {
    auto it = ProcessMapping.find(externalProcessId);
    if (it == ProcessMapping.end()) {
        size_t index = LoadQueue.Top();
        LoadQueue.ChangeLoad(index, +1);
        auto& stats = ProcessMapping[externalProcessId];
        stats.Counter += delta;
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

#pragma once
#include "counters.h"
#include "manager.h"

#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/config.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/events.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

namespace {

template<class T>
class TLoadQueue {
    using TLoad = i64;
    std::map<TLoad, std::set<T>> Load;
    std::map<T, TLoad> Items;

public:
    void Add(const T& item) {
        Load[0].emplace(item);
        Items[item] = 0;
    }

    const T& Top() {
        auto loadLevelIt = Load.begin();
        AFL_VERIFY(loadLevelIt != Load.end())("error", "Load queue is empty. Please add at least one element to load queue");
        return *loadLevelIt->second.begin();
    }

    void ChangeLoad(const T& item, i64 delta) {
        if (delta == 0) {
            return;
        }
        auto it = Items.find(item);
        AFL_VERIFY(it != Items.end())("error", "Load item is not found");
        TLoad& load = it->second;
        auto loadLevelIt = Load.find(load);
        loadLevelIt->second.erase(item);
        if (loadLevelIt->second.empty()) {
            Load.erase(loadLevelIt);
        }
        load += delta;
        Load[load].emplace(item);
    }
};

class TMemoryConsumptionAggregator: public TThrRefBase {
public:
    TMemoryConsumptionAggregator(size_t managersCount)
        : ManagerConsumption(managersCount) {
    }

    void SetConsumer(TIntrusivePtr<NKikimr::NMemory::IMemoryConsumer> consumer) {
        std::lock_guard lock(Mutex);
        Consumer = std::move(consumer);
    }

    void SetConsumption(size_t managerIndex, ui64 newConsumption) {
        std::lock_guard lock(Mutex);
        AFL_VERIFY(managerIndex < ManagerConsumption.size())("error", "Manager index is out of range")("index", managerIndex)("managersCount", ManagerConsumption.size());

        auto& managerConsumption = ManagerConsumption[managerIndex];
        AFL_VERIFY(managerConsumption <= TotalConsumption)("error", "Manager consumption is more than total consumption")("managerConsumption", managerConsumption)("totalConsumption", TotalConsumption);

        TotalConsumption -= managerConsumption;
        TotalConsumption += newConsumption;

        managerConsumption = newConsumption;

        if (Consumer) {
            Consumer->SetConsumption(TotalConsumption);
        }
    }

private:
    std::mutex Mutex;
    ui64 TotalConsumption = 0;
    TVector<ui64> ManagerConsumption;
    TIntrusivePtr<NKikimr::NMemory::IMemoryConsumer> Consumer;
};
}

class TManager;
class TMemoryLimiterActor: public NActors::TActorBootstrapped<TMemoryLimiterActor> {
private:
    TVector<std::shared_ptr<TManager>> Managers;
    TLoadQueue<size_t> LoadQueue;
    struct TProcessStats {
        size_t ManagerIndex = 0;
        int Counter = 0;
    };
    TMap<ui64, TProcessStats> ProcessMapping;

    const TConfig Config;
    const TString Name;
    const std::shared_ptr<TCounters> Signals;
    const std::shared_ptr<TStageFeatures> DefaultStage;
    const NMemory::EMemoryConsumerKind ConsumerKind;
    TIntrusivePtr<TMemoryConsumptionAggregator> MemoryConsumptionAggregator;

public:
    TMemoryLimiterActor(const TConfig& config, const TString& name, const std::shared_ptr<TCounters>& signals,
        const std::shared_ptr<TStageFeatures>& defaultStage, const NMemory::EMemoryConsumerKind consumerKind)
        : Config(config)
        , Name(name)
        , Signals(signals)
        , DefaultStage(defaultStage)
        , ConsumerKind(consumerKind)
        , MemoryConsumptionAggregator(new TMemoryConsumptionAggregator(Config.GetCountBuckets())) {
    }

    void Handle(NEvents::TEvExternal::TEvStartTask::TPtr& ev);
    void Handle(NEvents::TEvExternal::TEvFinishTask::TPtr& ev);
    void Handle(NEvents::TEvExternal::TEvUpdateTask::TPtr& ev);
    void Handle(NEvents::TEvExternal::TEvStartGroup::TPtr& ev);
    void Handle(NEvents::TEvExternal::TEvFinishGroup::TPtr& ev);
    void Handle(NEvents::TEvExternal::TEvStartProcess::TPtr& ev);
    void Handle(NEvents::TEvExternal::TEvFinishProcess::TPtr& ev);
    void Handle(NEvents::TEvExternal::TEvStartProcessScope::TPtr& ev);
    void Handle(NEvents::TEvExternal::TEvFinishProcessScope::TPtr& ev);
    void Handle(NMemory::TEvConsumerRegistered::TPtr& ev);
    void Handle(NMemory::TEvConsumerLimit::TPtr& ev);

    void Bootstrap();

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NEvents::TEvExternal::TEvStartTask, Handle);
            hFunc(NEvents::TEvExternal::TEvFinishTask, Handle);
            hFunc(NEvents::TEvExternal::TEvUpdateTask, Handle);
            hFunc(NEvents::TEvExternal::TEvStartGroup, Handle);
            hFunc(NEvents::TEvExternal::TEvFinishGroup, Handle);
            hFunc(NEvents::TEvExternal::TEvStartProcess, Handle);
            hFunc(NEvents::TEvExternal::TEvFinishProcess, Handle);
            hFunc(NEvents::TEvExternal::TEvStartProcessScope, Handle);
            hFunc(NEvents::TEvExternal::TEvFinishProcessScope, Handle);
            hFunc(NMemory::TEvConsumerRegistered, Handle);
            hFunc(NMemory::TEvConsumerLimit, Handle);
            default:
                AFL_VERIFY(false)("ev_type", ev->GetTypeName());
        }
    }
private:
    size_t AcquireManager(ui64 externalProcessId);
    size_t ReleaseManager(ui64 externalProcessId);
    size_t GetManager(ui64 externalProcessId);
};

}   // namespace NKikimr::NOlap::NGroupedMemoryManager

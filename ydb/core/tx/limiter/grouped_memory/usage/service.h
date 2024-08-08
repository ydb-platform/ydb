#pragma once
#include "abstract.h"
#include "config.h"
#include "events.h"

#include <ydb/core/tx/limiter/grouped_memory/service/actor.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

template <class TMemoryLimiterPolicy>
class TServiceOperatorImpl {
private:
    TConfig ServiceConfig = TConfig::BuildDisabledConfig();
    std::shared_ptr<TCounters> Counters;
    std::shared_ptr<TStageFeatures> DefaultStageFeatures = std::make_shared<TStageFeatures>("DEFAULT", ((ui64)3) << 30, nullptr, nullptr);
    using TSelf = TServiceOperatorImpl<TMemoryLimiterPolicy>;
    static void Register(const TConfig& serviceConfig, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters) {
        Singleton<TSelf>()->Counters = std::make_shared<TCounters>(counters, TMemoryLimiterPolicy::Name);
        Singleton<TSelf>()->ServiceConfig = serviceConfig;
        Singleton<TSelf>()->DefaultStageFeatures = std::make_shared<TStageFeatures>(
            "GLOBAL", serviceConfig.GetMemoryLimit(), nullptr, Singleton<TSelf>()->Counters->BuildStageCounters("general"));
    }
    static const TString& GetMemoryLimiterName() {
        Y_ABORT_UNLESS(TMemoryLimiterPolicy::Name.size() == 4);
        return TMemoryLimiterPolicy::Name;
    }

public:
    static std::shared_ptr<TStageFeatures> BuildStageFeatures(const TString& name, const ui64 limit) {
        if (!IsEnabled()) {
            return Singleton<TSelf>()->DefaultStageFeatures;
        } else {
            AFL_VERIFY(Singleton<TSelf>()->DefaultStageFeatures);
            return std::make_shared<TStageFeatures>(
                name, limit, Singleton<TSelf>()->DefaultStageFeatures, Singleton<TSelf>()->Counters->BuildStageCounters(name));
        }
    }

    static std::shared_ptr<TStageFeatures> GetDefaultStageFeatures() {
        AFL_VERIFY(Singleton<TSelf>()->DefaultStageFeatures);
        return Singleton<TSelf>()->DefaultStageFeatures;
    }

    static std::shared_ptr<TGroupGuard> BuildGroupGuard() {
        static TAtomicCounter counter = 0;
        auto& context = NActors::TActorContext::AsActorContext();
        const NActors::TActorId& selfId = context.SelfID;
        return std::make_shared<TGroupGuard>(MakeServiceId(selfId.NodeId()), counter.Inc());
    }

    static bool SendToAllocation(const std::vector<std::shared_ptr<IAllocation>>& tasks, const std::shared_ptr<TStageFeatures>& stage, const ui64 groupId) {
        auto& context = NActors::TActorContext::AsActorContext();
        const NActors::TActorId& selfId = context.SelfID;
        if (TSelf::IsEnabled()) {
            context.Send(MakeServiceId(selfId.NodeId()), new NEvents::TEvExternal::TEvStartTask(tasks, stage, groupId));
            return true;
        } else {
            for (auto&& i : tasks) {
                if (!i->IsAllocated()) {
                    AFL_VERIFY(i->OnAllocated(std::make_shared<TAllocationGuard>(NActors::TActorId(), 0, i->GetMemory()), i));
                }
            }
            return false;
        }
    }
    static bool IsEnabled() {
        return Singleton<TSelf>()->ServiceConfig.IsEnabled();
    }
    static NActors::TActorId MakeServiceId(const ui32 nodeId) {
        return NActors::TActorId(nodeId, "SrvcMlmt" + GetMemoryLimiterName());
    }
    static NActors::IActor* CreateService(const TConfig& config, TIntrusivePtr<::NMonitoring::TDynamicCounters> signals) {
        Register(config, signals);
        return new TMemoryLimiterActor(config, GetMemoryLimiterName(), Singleton<TSelf>()->Counters);
    }
};

class TScanMemoryLimiterPolicy {
public:
    static const inline TString Name = "Scan";
};

using TScanMemoryLimiterOperator = TServiceOperatorImpl<TScanMemoryLimiterPolicy>;

}   // namespace NKikimr::NOlap::NGroupedMemoryManager

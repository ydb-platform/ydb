#pragma once
#include "abstract.h"
#include "config.h"
#include "events.h"

#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/tx/limiter/grouped_memory/service/actor.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

template <class TMemoryLimiterPolicy>
class TServiceOperatorImpl {
private:
    TAtomicCounter LastProcessId = 0;
    TConfig ServiceConfig = TConfig::BuildDisabledConfig();
    using TSelf = TServiceOperatorImpl<TMemoryLimiterPolicy>;
    static void Register(const TConfig& serviceConfig) {
        Singleton<TSelf>()->ServiceConfig = serviceConfig;
    }
    static const TString& GetMemoryLimiterName() {
        Y_ABORT_UNLESS(TMemoryLimiterPolicy::Name.size() == 4);
        return TMemoryLimiterPolicy::Name;
    }

    static NMemory::EMemoryConsumerKind GetConsumerKind() {
        return TMemoryLimiterPolicy::ConsumerKind;
    }

    static ui64 GetHardLimitMultiplier() {
        return TMemoryLimiterPolicy::HardLimitMultiplier;
    }

public:
    static std::shared_ptr<TStageFeatures> BuildStageFeatures(const TString& name, const ui64 limit) {
        if (!IsEnabled()) {
            return nullptr;
        } else {
            return std::make_shared<TStageFeatures>(
                name, limit / (GetCountBuckets() ? GetCountBuckets() : 1), std::nullopt, nullptr, nullptr);
        }
    }

    static std::shared_ptr<TProcessGuard> BuildProcessGuard(const std::vector<std::shared_ptr<TStageFeatures>>& stages)
        requires(!TMemoryLimiterPolicy::ExternalProcessIdAllocation)
    {
        ui64 processId = Singleton<TSelf>()->LastProcessId.Inc();
        auto& context = NActors::TActorContext::AsActorContext();
        const NActors::TActorId& selfId = context.SelfID;
        return std::make_shared<TProcessGuard>(MakeServiceId(selfId.NodeId()), processId, stages);
    }

    static std::shared_ptr<TProcessGuard> BuildProcessGuard(const ui64 processId, const std::vector<std::shared_ptr<TStageFeatures>>& stages)
        requires(TMemoryLimiterPolicy::ExternalProcessIdAllocation)
    {
        auto& context = NActors::TActorContext::AsActorContext();
        const NActors::TActorId& selfId = context.SelfID;
        return std::make_shared<TProcessGuard>(MakeServiceId(selfId.NodeId()), processId, stages);
    }

    static bool SendToAllocation(const ui64 processId, const ui64 scopeId, const ui64 groupId,
        const std::vector<std::shared_ptr<IAllocation>>& tasks,
        const std::optional<ui32>& stageIdx) {
        auto& context = NActors::TActorContext::AsActorContext();
        const NActors::TActorId& selfId = context.SelfID;
        if (TSelf::IsEnabled()) {
            context.Send(MakeServiceId(selfId.NodeId()), new NEvents::TEvExternal::TEvStartTask(processId, scopeId, groupId, tasks, stageIdx));
            return true;
        } else {
            for (auto&& i : tasks) {
                if (!i->IsAllocated()) {
                    LWPROBE(Allocated, "disabled", i->GetIdentifier(), "", std::numeric_limits<ui64>::max(), std::numeric_limits<ui64>::max(), 0, 0, TDuration::Zero(), false, true);
                    AFL_VERIFY(i->OnAllocated(std::make_shared<TAllocationGuard>(0, 0, 0, NActors::TActorId(), i->GetMemory(), nullptr), i));
                }
            }
            return false;
        }
    }
    static bool IsEnabled() {
        return Singleton<TSelf>()->ServiceConfig.IsEnabled();
    }

    static ui64 GetCountBuckets() {
        return Singleton<TSelf>()->ServiceConfig.GetCountBuckets();
    }
    static NActors::TActorId MakeServiceId(const ui32 nodeId) {
        return NActors::TActorId(nodeId, "SrvcMlmt" + GetMemoryLimiterName());
    }
    static NActors::IActor* CreateService(const TConfig& config, TIntrusivePtr<::NMonitoring::TDynamicCounters> signals) {
        Register(config);
        return new TMemoryLimiterActor(config, GetMemoryLimiterName(), signals, GetConsumerKind(), GetHardLimitMultiplier());
    }
};

class TScanMemoryLimiterPolicy {
public:
    static const inline TString Name = "Scan";
    static const inline NMemory::EMemoryConsumerKind ConsumerKind = NMemory::EMemoryConsumerKind::ColumnTablesScanGroupedMemory;
    static constexpr bool ExternalProcessIdAllocation = true;
    static constexpr ui64 HardLimitMultiplier = 1;
};

using TScanMemoryLimiterOperator = TServiceOperatorImpl<TScanMemoryLimiterPolicy>;

class TCompMemoryLimiterPolicy {
public:
    static const inline TString Name = "Comp";
    static const inline NMemory::EMemoryConsumerKind ConsumerKind = NMemory::EMemoryConsumerKind::ColumnTablesCompGroupedMemory;
    static constexpr bool ExternalProcessIdAllocation = false;
    static constexpr ui64 HardLimitMultiplier = 4;
};

using TCompMemoryLimiterOperator = TServiceOperatorImpl<TCompMemoryLimiterPolicy>;

class TDeduplicationMemoryLimiterPolicy {
public:
    static const inline TString Name = "Dedu";
    static const inline NMemory::EMemoryConsumerKind ConsumerKind = NMemory::EMemoryConsumerKind::ColumnTablesDeduplicationGroupedMemory;
    static constexpr bool ExternalProcessIdAllocation = false;
    static constexpr ui64 HardLimitMultiplier = 1;
};

using TDeduplicationMemoryLimiterOperator = TServiceOperatorImpl<TDeduplicationMemoryLimiterPolicy>;

}   // namespace NKikimr::NOlap::NGroupedMemoryManager

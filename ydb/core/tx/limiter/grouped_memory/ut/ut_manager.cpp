#include <ydb/core/tx/limiter/grouped_memory/service/counters.h>
#include <ydb/core/tx/limiter/grouped_memory/service/manager.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/config.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

#include <ydb/library/actors/core/log.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/object_counter.h>

Y_UNIT_TEST_SUITE(GroupedMemoryLimiter) {
    using namespace NKikimr;

    class TAllocation: public NOlap::NGroupedMemoryManager::IAllocation, public TObjectCounter<TAllocation> {
    private:
        using TBase = NOlap::NGroupedMemoryManager::IAllocation;
        virtual bool DoOnAllocated(std::shared_ptr<NOlap::NGroupedMemoryManager::TAllocationGuard>&& /*guard*/,
            const std::shared_ptr<NOlap::NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
            return true;
        }

    public:
        TAllocation(const ui64 mem)
            : TBase(mem) {
        }
    };

    Y_UNIT_TEST(Simplest) {
        auto counters = std::make_shared<NOlap::NGroupedMemoryManager::TCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>(), "test");
        NOlap::NGroupedMemoryManager::TConfig config;
        {
            NKikimrConfig::TGroupedMemoryLimiterConfig protoConfig;
            protoConfig.SetMemoryLimit(100);
            AFL_VERIFY(config.DeserializeFromProto(protoConfig));
        }
        std::unique_ptr<NActors::IActor> actor(
            NOlap::NGroupedMemoryManager::TScanMemoryLimiterOperator::CreateService(config, MakeIntrusive<NMonitoring::TDynamicCounters>()));
        auto stage = NOlap::NGroupedMemoryManager::TScanMemoryLimiterOperator::GetDefaultStageFeatures();
        auto manager = std::make_shared<NOlap::NGroupedMemoryManager::TManager>(NActors::TActorId(), config, "test", counters, stage);
        {
            auto alloc1 = std::make_shared<TAllocation>(50);
            manager->RegisterProcess(0, {});
            manager->RegisterGroup(0, 1);
            manager->RegisterAllocation(0, 1, alloc1, {});
            AFL_VERIFY(alloc1->IsAllocated());
            auto alloc1_1 = std::make_shared<TAllocation>(50);
            manager->RegisterAllocation(0, 1, alloc1_1, {});
            AFL_VERIFY(alloc1_1->IsAllocated());

            manager->RegisterGroup(0, 2);
            auto alloc2 = std::make_shared<TAllocation>(50);
            manager->RegisterAllocation(0, 2, alloc2, {});
            AFL_VERIFY(!alloc2->IsAllocated());

            manager->UnregisterAllocation(0, alloc1->GetIdentifier());
            AFL_VERIFY(alloc2->IsAllocated());
            manager->UnregisterAllocation(0, alloc2->GetIdentifier());
            manager->UnregisterAllocation(0, alloc1_1->GetIdentifier());
            manager->UnregisterGroup(0, 1);
            manager->UnregisterGroup(0, 2);
            manager->UnregisterProcess(0);
        }
        AFL_VERIFY(!stage->GetUsage().Val());
        AFL_VERIFY(manager->IsEmpty());
        AFL_VERIFY(!TObjectCounter<TAllocation>::ObjectCount());
    }

    Y_UNIT_TEST(Simple) {
        auto counters = std::make_shared<NOlap::NGroupedMemoryManager::TCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>(), "test");
        NOlap::NGroupedMemoryManager::TConfig config;
        {
            NKikimrConfig::TGroupedMemoryLimiterConfig protoConfig;
            protoConfig.SetMemoryLimit(100);
            AFL_VERIFY(config.DeserializeFromProto(protoConfig));
        }
        std::unique_ptr<NActors::IActor> actor(NOlap::NGroupedMemoryManager::TScanMemoryLimiterOperator::CreateService(config, MakeIntrusive<NMonitoring::TDynamicCounters>()));
        auto stage = NOlap::NGroupedMemoryManager::TScanMemoryLimiterOperator::GetDefaultStageFeatures();
        auto manager = std::make_shared<NOlap::NGroupedMemoryManager::TManager>(NActors::TActorId(), config, "test", counters, stage);
        {
            manager->RegisterProcess(0, {});
            auto alloc1 = std::make_shared<TAllocation>(10);
            manager->RegisterGroup(0, 1);
            manager->RegisterAllocation(0, 1, alloc1, {});
            AFL_VERIFY(alloc1->IsAllocated());
            auto alloc2 = std::make_shared<TAllocation>(1000);
            manager->RegisterGroup(0, 2);
            manager->RegisterAllocation(0, 2, alloc2, {});
            AFL_VERIFY(!alloc2->IsAllocated());
            auto alloc3 = std::make_shared<TAllocation>(1000);
            manager->RegisterGroup(0, 3);
            manager->RegisterAllocation(0, 3, alloc3, {});
            AFL_VERIFY(alloc1->IsAllocated());
            AFL_VERIFY(!alloc2->IsAllocated());
            AFL_VERIFY(!alloc3->IsAllocated());
            auto alloc1_1 = std::make_shared<TAllocation>(1000);
            manager->RegisterAllocation(0, 1, alloc1_1, {});
            AFL_VERIFY(alloc1_1->IsAllocated());
            AFL_VERIFY(!alloc2->IsAllocated());
            manager->UnregisterAllocation(0, alloc1_1->GetIdentifier());
            AFL_VERIFY(!alloc2->IsAllocated());
            manager->UnregisterGroup(0, 1);
            AFL_VERIFY(alloc2->IsAllocated());

            manager->UnregisterAllocation(0, alloc1->GetIdentifier());
            AFL_VERIFY(!alloc3->IsAllocated());
            manager->UnregisterGroup(0, 2);
            manager->UnregisterAllocation(0, alloc2->GetIdentifier());
            AFL_VERIFY(alloc3->IsAllocated());
            manager->UnregisterGroup(0, 3);
            manager->UnregisterAllocation(0, alloc3->GetIdentifier());
            manager->UnregisterProcess(0);
        }
        AFL_VERIFY(!stage->GetUsage().Val());
        AFL_VERIFY(manager->IsEmpty());
        AFL_VERIFY(!TObjectCounter<TAllocation>::ObjectCount());
    }

    Y_UNIT_TEST(CommonUsage) {
        auto counters = std::make_shared<NOlap::NGroupedMemoryManager::TCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>(), "test");
        NOlap::NGroupedMemoryManager::TConfig config;
        {
            NKikimrConfig::TGroupedMemoryLimiterConfig protoConfig;
            protoConfig.SetMemoryLimit(100);
            AFL_VERIFY(config.DeserializeFromProto(protoConfig));
        }
        std::unique_ptr<NActors::IActor> actor(
            NOlap::NGroupedMemoryManager::TScanMemoryLimiterOperator::CreateService(config, MakeIntrusive<NMonitoring::TDynamicCounters>()));
        auto stage = NOlap::NGroupedMemoryManager::TScanMemoryLimiterOperator::GetDefaultStageFeatures();
        auto manager = std::make_shared<NOlap::NGroupedMemoryManager::TManager>(NActors::TActorId(), config, "test", counters, stage);
        {
            manager->RegisterProcess(0, {});
            manager->RegisterGroup(0, 1);
            auto alloc0 = std::make_shared<TAllocation>(1000);
            manager->RegisterAllocation(0, 1, alloc0, {});
            auto alloc1 = std::make_shared<TAllocation>(1000);
            manager->RegisterAllocation(0, 1, alloc1, {});
            AFL_VERIFY(alloc0->IsAllocated());
            AFL_VERIFY(alloc1->IsAllocated());

            manager->RegisterGroup(0, 2);
            auto alloc2 = std::make_shared<TAllocation>(1000);
            manager->RegisterAllocation(0, 2, alloc0, {});
            manager->RegisterAllocation(0, 2, alloc2, {});
            AFL_VERIFY(alloc0->IsAllocated());
            AFL_VERIFY(!alloc2->IsAllocated());

            auto alloc3 = std::make_shared<TAllocation>(1000);
            manager->RegisterGroup(0, 3);
            manager->RegisterAllocation(0, 3, alloc0, {});
            manager->RegisterAllocation(0, 3, alloc3, {});
            AFL_VERIFY(alloc0->IsAllocated());
            AFL_VERIFY(alloc1->IsAllocated());
            AFL_VERIFY(!alloc2->IsAllocated());
            AFL_VERIFY(!alloc3->IsAllocated());

            manager->UnregisterGroup(0, 1);
            manager->UnregisterAllocation(0, alloc1->GetIdentifier());

            AFL_VERIFY(alloc0->IsAllocated());
            AFL_VERIFY(alloc2->IsAllocated());
            AFL_VERIFY(!alloc3->IsAllocated());
            manager->UnregisterGroup(0, 2);
            manager->UnregisterAllocation(0, alloc2->GetIdentifier());
            AFL_VERIFY(alloc0->IsAllocated());
            AFL_VERIFY(alloc3->IsAllocated());

            manager->UnregisterGroup(0, 3);
            manager->UnregisterAllocation(0, alloc3->GetIdentifier());
            manager->UnregisterAllocation(0, alloc0->GetIdentifier());
            manager->UnregisterProcess(0);
        }
        AFL_VERIFY(!stage->GetUsage().Val());
        AFL_VERIFY(manager->IsEmpty());
        AFL_VERIFY(!TObjectCounter<TAllocation>::ObjectCount());
    }

    Y_UNIT_TEST(Update) {
        auto counters = std::make_shared<NOlap::NGroupedMemoryManager::TCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>(), "test");
        NOlap::NGroupedMemoryManager::TConfig config;
        {
            NKikimrConfig::TGroupedMemoryLimiterConfig protoConfig;
            protoConfig.SetMemoryLimit(100);
            AFL_VERIFY(config.DeserializeFromProto(protoConfig));
        }
        std::unique_ptr<NActors::IActor> actor(
            NOlap::NGroupedMemoryManager::TScanMemoryLimiterOperator::CreateService(config, MakeIntrusive<NMonitoring::TDynamicCounters>()));
        auto stage = NOlap::NGroupedMemoryManager::TScanMemoryLimiterOperator::GetDefaultStageFeatures();
        auto manager = std::make_shared<NOlap::NGroupedMemoryManager::TManager>(NActors::TActorId(), config, "test", counters, stage);
        {
            manager->RegisterProcess(0, {});
            auto alloc1 = std::make_shared<TAllocation>(1000);
            manager->RegisterGroup(0, 1);
            manager->RegisterAllocation(0, 1, alloc1, {});
            AFL_VERIFY(alloc1->IsAllocated());
            auto alloc2 = std::make_shared<TAllocation>(10);
            manager->RegisterGroup(0, 3);
            manager->RegisterAllocation(0, 3, alloc2, {});
            AFL_VERIFY(!alloc2->IsAllocated());

            manager->UpdateAllocation(0, alloc1->GetIdentifier(), 10);
            AFL_VERIFY(alloc2->IsAllocated());

            manager->UnregisterGroup(0, 3);
            manager->UnregisterAllocation(0, alloc2->GetIdentifier());

            manager->UnregisterGroup(0, 1);
            manager->UnregisterAllocation(0, alloc1->GetIdentifier());
            manager->UnregisterProcess(0);
        }
        AFL_VERIFY(!stage->GetUsage().Val());
        AFL_VERIFY(manager->IsEmpty());
        AFL_VERIFY(!TObjectCounter<TAllocation>::ObjectCount());
    }
};

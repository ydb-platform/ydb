#include <ydb/core/tx/limiter/grouped_memory/service/counters.h>
#include <ydb/core/tx/limiter/grouped_memory/service/manager.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/config.h>

#include <ydb/library/actors/core/log.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/object_counter.h>

Y_UNIT_TEST_SUITE(GroupedMemoryLimiter) {
    using namespace NKikimr;

    class TAllocation: public NOlap::NGroupedMemoryManager::IAllocation, public TObjectCounter<TAllocation> {
    private:
        using TBase = NOlap::NGroupedMemoryManager::IAllocation;
        virtual void DoOnAllocated(std::shared_ptr<NOlap::NGroupedMemoryManager::TAllocationGuard>&& /*guard*/,
            const std::shared_ptr<NOlap::NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
        }

    public:
        TAllocation(const ui64 mem)
            : TBase(mem) {
        }
    };

    Y_UNIT_TEST(Simple) {
        NOlap::NGroupedMemoryManager::TCounters counters("test", MakeIntrusive<NMonitoring::TDynamicCounters>());
        NOlap::NGroupedMemoryManager::TConfig config;
        {
            NKikimrConfig::TGroupedMemoryLimiterConfig protoConfig;
            protoConfig.SetMemoryLimit(100);
            AFL_VERIFY(config.DeserializeFromProto(protoConfig));
        }
        auto manager = std::make_shared<NOlap::NGroupedMemoryManager::TManager>(NActors::TActorId(), config, "test", counters);
        {
            auto alloc1 = std::make_shared<TAllocation>(10);
            manager->RegisterAllocation(alloc1, 1);
            AFL_VERIFY(alloc1->IsAllocated());
            auto alloc2 = std::make_shared<TAllocation>(1000);
            manager->RegisterAllocation(alloc2, 3);
            AFL_VERIFY(!alloc2->IsAllocated());
            auto alloc3 = std::make_shared<TAllocation>(1000);
            manager->RegisterAllocation(alloc3, 2);
            AFL_VERIFY(alloc1->IsAllocated());
            AFL_VERIFY(!alloc2->IsAllocated());
            AFL_VERIFY(!alloc3->IsAllocated());
            auto alloc1_1 = std::make_shared<TAllocation>(1000);
            manager->RegisterAllocation(alloc1_1, 1);
            AFL_VERIFY(alloc1_1->IsAllocated());
            AFL_VERIFY(!alloc2->IsAllocated());
            manager->UnregisterAllocation(alloc1_1->GetIdentifier());
            AFL_VERIFY(!alloc2->IsAllocated());
            manager->UnregisterGroup(1);
            AFL_VERIFY(!alloc2->IsAllocated());

            manager->UnregisterAllocation(alloc1->GetIdentifier());
            AFL_VERIFY(alloc2->IsAllocated());
            AFL_VERIFY(!alloc3->IsAllocated());
            manager->UnregisterGroup(3);
            manager->UnregisterAllocation(alloc2->GetIdentifier());
            AFL_VERIFY(alloc3->IsAllocated());
            manager->UnregisterGroup(2);
            manager->UnregisterAllocation(alloc3->GetIdentifier());
        }
        AFL_VERIFY(manager->IsEmpty());
        AFL_VERIFY(!TObjectCounter<TAllocation>::ObjectCount());
    }

    Y_UNIT_TEST(CommonUsage) {
        NOlap::NGroupedMemoryManager::TCounters counters("test", MakeIntrusive<NMonitoring::TDynamicCounters>());
        NOlap::NGroupedMemoryManager::TConfig config;
        {
            NKikimrConfig::TGroupedMemoryLimiterConfig protoConfig;
            protoConfig.SetMemoryLimit(100);
            AFL_VERIFY(config.DeserializeFromProto(protoConfig));
        }
        auto manager = std::make_shared<NOlap::NGroupedMemoryManager::TManager>(NActors::TActorId(), config, "test", counters);
        {
            auto alloc0 = std::make_shared<TAllocation>(1000);
            manager->RegisterAllocation(alloc0, 1);
            auto alloc1 = std::make_shared<TAllocation>(1000);
            manager->RegisterAllocation(alloc1, 1);
            AFL_VERIFY(alloc0->IsAllocated());
            AFL_VERIFY(alloc1->IsAllocated());

            auto alloc2 = std::make_shared<TAllocation>(1000);
            manager->RegisterAllocation(alloc0, 3);
            manager->RegisterAllocation(alloc2, 3);
            AFL_VERIFY(alloc0->IsAllocated());
            AFL_VERIFY(!alloc2->IsAllocated());

            auto alloc3 = std::make_shared<TAllocation>(1000);
            manager->RegisterAllocation(alloc0, 2);
            manager->RegisterAllocation(alloc3, 2);
            AFL_VERIFY(alloc0->IsAllocated());
            AFL_VERIFY(alloc1->IsAllocated());
            AFL_VERIFY(!alloc2->IsAllocated());
            AFL_VERIFY(!alloc3->IsAllocated());

            manager->UnregisterGroup(1);
            manager->UnregisterAllocation(alloc1->GetIdentifier());

            AFL_VERIFY(alloc0->IsAllocated());
            AFL_VERIFY(alloc2->IsAllocated());
            AFL_VERIFY(!alloc3->IsAllocated());
            manager->UnregisterGroup(3);
            manager->UnregisterAllocation(alloc2->GetIdentifier());
            AFL_VERIFY(alloc0->IsAllocated());
            AFL_VERIFY(alloc3->IsAllocated());

            manager->UnregisterGroup(2);
            manager->UnregisterAllocation(alloc3->GetIdentifier());
            manager->UnregisterAllocation(alloc0->GetIdentifier());
        }
        AFL_VERIFY(manager->IsEmpty());
        AFL_VERIFY(!TObjectCounter<TAllocation>::ObjectCount());
    }

    Y_UNIT_TEST(Update) {
        NOlap::NGroupedMemoryManager::TCounters counters("test", MakeIntrusive<NMonitoring::TDynamicCounters>());
        NOlap::NGroupedMemoryManager::TConfig config;
        {
            NKikimrConfig::TGroupedMemoryLimiterConfig protoConfig;
            protoConfig.SetMemoryLimit(100);
            AFL_VERIFY(config.DeserializeFromProto(protoConfig));
        }
        auto manager = std::make_shared<NOlap::NGroupedMemoryManager::TManager>(NActors::TActorId(), config, "test", counters);
        {
            auto alloc1 = std::make_shared<TAllocation>(1000);
            manager->RegisterAllocation(alloc1, 1);
            AFL_VERIFY(alloc1->IsAllocated());
            auto alloc2 = std::make_shared<TAllocation>(10);
            manager->RegisterAllocation(alloc2, 3);
            AFL_VERIFY(!alloc2->IsAllocated());

            manager->UpdateAllocation(alloc1->GetIdentifier(), 10);
            AFL_VERIFY(alloc2->IsAllocated());

            manager->UnregisterGroup(3);
            manager->UnregisterAllocation(alloc2->GetIdentifier());

            manager->UnregisterGroup(1);
            manager->UnregisterAllocation(alloc1->GetIdentifier());
        }
        AFL_VERIFY(manager->IsEmpty());
        AFL_VERIFY(!TObjectCounter<TAllocation>::ObjectCount());
    }
};

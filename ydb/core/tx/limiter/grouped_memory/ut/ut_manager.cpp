#include <ydb/core/tx/limiter/grouped_memory/service/counters.h>
#include <ydb/core/tx/limiter/grouped_memory/service/manager.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/config.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>
#include <ydb/core/protos/config.pb.h>

#include <ydb/library/actors/core/log.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/object_counter.h>

#include <limits>
#include <optional>
#include <vector>

Y_UNIT_TEST_SUITE(GroupedMemoryLimiter) {
    using namespace NKikimr;

    class TAllocation: public NOlap::NGroupedMemoryManager::IAllocation, public TObjectCounter<TAllocation> {
    public:
        std::shared_ptr<NOlap::NGroupedMemoryManager::TAllocationGuard> Guard;
    private:
        using TBase = NOlap::NGroupedMemoryManager::IAllocation;
        virtual void DoOnAllocationImpossible(const TString& errorMessage) override {
            AFL_VERIFY(false)("error", errorMessage);
        }

        virtual bool DoOnAllocated(std::shared_ptr<NOlap::NGroupedMemoryManager::TAllocationGuard>&& guard,
            const std::shared_ptr<NOlap::NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
            Guard = std::move(guard);
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
            UNIT_ASSERT(config.DeserializeFromProto(protoConfig));
        }
        std::unique_ptr<NActors::IActor> actor(
            NOlap::NGroupedMemoryManager::TScanMemoryLimiterOperator::CreateService(config, MakeIntrusive<NMonitoring::TDynamicCounters>()));
        auto groupedMemoryLimiterCounters = std::make_shared<NKikimr::NOlap::NGroupedMemoryManager::TCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>(), "Scan");
        auto stage = std::make_shared<NKikimr::NOlap::NGroupedMemoryManager::TStageFeatures>("GLOBAL", config.GetMemoryLimit(), config.GetHardMemoryLimit(), nullptr, groupedMemoryLimiterCounters->BuildStageCounters("general"));
        auto manager = std::make_shared<NOlap::NGroupedMemoryManager::TManager>(NActors::TActorId(), config, "test", counters, stage);
        {
            auto alloc1 = std::make_shared<TAllocation>(50);
            manager->RegisterProcess(0, {});
            manager->RegisterProcessScope(0, 0);
            manager->RegisterGroup(0, 0, 1);
            manager->RegisterAllocation(0, 0, 1, alloc1, {});
            UNIT_ASSERT(alloc1->IsAllocated());
            auto alloc1_1 = std::make_shared<TAllocation>(50);
            manager->RegisterAllocation(0, 0, 1, alloc1_1, {});
            UNIT_ASSERT(alloc1_1->IsAllocated());

            manager->RegisterGroup(0, 0, 2);
            auto alloc2 = std::make_shared<TAllocation>(50);
            manager->RegisterAllocation(0, 0, 2, alloc2, {});
            UNIT_ASSERT(!alloc2->IsAllocated());
            alloc1->Guard.reset();
            manager->UnregisterAllocation(0, 0, alloc1->GetIdentifier());

            UNIT_ASSERT(alloc2->IsAllocated());
            manager->UnregisterAllocation(0, 0, alloc2->GetIdentifier());
            manager->UnregisterAllocation(0, 0, alloc1_1->GetIdentifier());
            manager->UnregisterGroup(0, 0, 1);
            manager->UnregisterGroup(0, 0, 2);
            manager->UnregisterProcessScope(0, 0);
            manager->UnregisterProcess(0);
        }
        UNIT_ASSERT_VALUES_EQUAL(stage->GetUsage().Val(), 0);
        UNIT_ASSERT(manager->IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(TObjectCounter<TAllocation>::ObjectCount(), 0);
    }

    Y_UNIT_TEST(Simple) {
        auto counters = std::make_shared<NOlap::NGroupedMemoryManager::TCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>(), "test");
        NOlap::NGroupedMemoryManager::TConfig config;
        {
            NKikimrConfig::TGroupedMemoryLimiterConfig protoConfig;
            protoConfig.SetMemoryLimit(100);
            UNIT_ASSERT(config.DeserializeFromProto(protoConfig));
        }
        std::unique_ptr<NActors::IActor> actor(NOlap::NGroupedMemoryManager::TScanMemoryLimiterOperator::CreateService(config, MakeIntrusive<NMonitoring::TDynamicCounters>()));
        auto groupedMemoryLimiterCounters = std::make_shared<NKikimr::NOlap::NGroupedMemoryManager::TCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>(), "Scan");
        auto stage = std::make_shared<NKikimr::NOlap::NGroupedMemoryManager::TStageFeatures>("GLOBAL", config.GetMemoryLimit(), config.GetHardMemoryLimit(), nullptr, groupedMemoryLimiterCounters->BuildStageCounters("general"));
        auto manager = std::make_shared<NOlap::NGroupedMemoryManager::TManager>(NActors::TActorId(), config, "test", counters, stage);
        {
            manager->RegisterProcess(0, {});
            manager->RegisterProcessScope(0, 0);
            auto alloc1 = std::make_shared<TAllocation>(10);
            manager->RegisterGroup(0, 0, 1);
            manager->RegisterAllocation(0, 0, 1, alloc1, {});
            UNIT_ASSERT(alloc1->IsAllocated());
            auto alloc2 = std::make_shared<TAllocation>(1000);
            manager->RegisterGroup(0, 0, 2);
            manager->RegisterAllocation(0, 0, 2, alloc2, {});
            UNIT_ASSERT(!alloc2->IsAllocated());
            auto alloc3 = std::make_shared<TAllocation>(1000);
            manager->RegisterGroup(0, 0, 3);
            manager->RegisterAllocation(0, 0, 3, alloc3, {});
            UNIT_ASSERT(alloc1->IsAllocated());
            UNIT_ASSERT(!alloc2->IsAllocated());
            UNIT_ASSERT(!alloc3->IsAllocated());
            auto alloc1_1 = std::make_shared<TAllocation>(1000);
            manager->RegisterAllocation(0, 0, 1, alloc1_1, {});
            UNIT_ASSERT(alloc1_1->IsAllocated());
            UNIT_ASSERT(!alloc2->IsAllocated());
            alloc1_1->ResetAllocation();
            manager->UnregisterAllocation(0, 0, alloc1_1->GetIdentifier());
            UNIT_ASSERT(!alloc2->IsAllocated());
            manager->UnregisterGroup(0, 0, 1);
            UNIT_ASSERT(alloc2->IsAllocated());

            manager->UnregisterAllocation(0, 0, alloc1->GetIdentifier());
            UNIT_ASSERT(!alloc3->IsAllocated());
            manager->UnregisterGroup(0, 0, 2);
            manager->UnregisterAllocation(0, 0, alloc2->GetIdentifier());
            UNIT_ASSERT(alloc3->IsAllocated());
            manager->UnregisterGroup(0, 0, 3);
            manager->UnregisterAllocation(0, 0, alloc3->GetIdentifier());
            manager->UnregisterProcessScope(0, 0);
            manager->UnregisterProcess(0);
        }
        UNIT_ASSERT_VALUES_EQUAL(stage->GetUsage().Val(), 0);
        UNIT_ASSERT(manager->IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(TObjectCounter<TAllocation>::ObjectCount(), 0);
    }

    Y_UNIT_TEST(CommonUsage) {
        auto counters = std::make_shared<NOlap::NGroupedMemoryManager::TCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>(), "test");
        NOlap::NGroupedMemoryManager::TConfig config;
        {
            NKikimrConfig::TGroupedMemoryLimiterConfig protoConfig;
            protoConfig.SetMemoryLimit(100);
            UNIT_ASSERT(config.DeserializeFromProto(protoConfig));
        }
        std::unique_ptr<NActors::IActor> actor(
            NOlap::NGroupedMemoryManager::TScanMemoryLimiterOperator::CreateService(config, MakeIntrusive<NMonitoring::TDynamicCounters>()));
        auto groupedMemoryLimiterCounters = std::make_shared<NKikimr::NOlap::NGroupedMemoryManager::TCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>(), "Scan");
        auto stage = std::make_shared<NKikimr::NOlap::NGroupedMemoryManager::TStageFeatures>("GLOBAL", config.GetMemoryLimit(), config.GetHardMemoryLimit(), nullptr, groupedMemoryLimiterCounters->BuildStageCounters("general"));
        auto manager = std::make_shared<NOlap::NGroupedMemoryManager::TManager>(NActors::TActorId(), config, "test", counters, stage);
        {
            manager->RegisterProcess(0, {});
            manager->RegisterProcessScope(0, 0);
            manager->RegisterGroup(0, 0, 1);
            auto alloc0 = std::make_shared<TAllocation>(1000);
            manager->RegisterAllocation(0, 0, 1, alloc0, {});
            auto alloc1 = std::make_shared<TAllocation>(1000);
            manager->RegisterAllocation(0, 0, 1, alloc1, {});
            UNIT_ASSERT(alloc0->IsAllocated());
            UNIT_ASSERT(alloc1->IsAllocated());

            manager->RegisterGroup(0, 0, 2);
            auto alloc2 = std::make_shared<TAllocation>(1000);
            manager->RegisterAllocation(0, 0, 2, alloc0, {});
            manager->RegisterAllocation(0, 0, 2, alloc2, {});
            UNIT_ASSERT(alloc0->IsAllocated());
            UNIT_ASSERT(!alloc2->IsAllocated());

            auto alloc3 = std::make_shared<TAllocation>(1000);
            manager->RegisterGroup(0, 0, 3);
            manager->RegisterAllocation(0, 0, 3, alloc0, {});
            manager->RegisterAllocation(0, 0, 3, alloc3, {});
            UNIT_ASSERT(alloc0->IsAllocated());
            UNIT_ASSERT(alloc1->IsAllocated());
            UNIT_ASSERT(!alloc2->IsAllocated());
            UNIT_ASSERT(!alloc3->IsAllocated());

            manager->UnregisterGroup(0, 0, 1);
            manager->UnregisterAllocation(0, 0, alloc1->GetIdentifier());

            UNIT_ASSERT(alloc0->IsAllocated());
            UNIT_ASSERT(alloc2->IsAllocated());
            UNIT_ASSERT(!alloc3->IsAllocated());
            manager->UnregisterGroup(0, 0, 2);
            manager->UnregisterAllocation(0, 0, alloc2->GetIdentifier());
            UNIT_ASSERT(alloc0->IsAllocated());
            UNIT_ASSERT(alloc3->IsAllocated());

            manager->UnregisterGroup(0, 0, 3);
            manager->UnregisterAllocation(0, 0, alloc3->GetIdentifier());
            manager->UnregisterAllocation(0, 0, alloc0->GetIdentifier());
            manager->UnregisterProcess(0);
        }
        UNIT_ASSERT_VALUES_EQUAL(stage->GetUsage().Val(), 0);
        UNIT_ASSERT(manager->IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(TObjectCounter<TAllocation>::ObjectCount(), 0);
    }

    Y_UNIT_TEST(Update) {
        auto counters = std::make_shared<NOlap::NGroupedMemoryManager::TCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>(), "test");
        NOlap::NGroupedMemoryManager::TConfig config;
        {
            NKikimrConfig::TGroupedMemoryLimiterConfig protoConfig;
            protoConfig.SetMemoryLimit(100);
            UNIT_ASSERT(config.DeserializeFromProto(protoConfig));
        }
        std::unique_ptr<NActors::IActor> actor(
            NOlap::NGroupedMemoryManager::TScanMemoryLimiterOperator::CreateService(config, MakeIntrusive<NMonitoring::TDynamicCounters>()));
        auto groupedMemoryLimiterCounters = std::make_shared<NKikimr::NOlap::NGroupedMemoryManager::TCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>(), "Scan");
        auto stage = std::make_shared<NKikimr::NOlap::NGroupedMemoryManager::TStageFeatures>("GLOBAL", config.GetMemoryLimit(), config.GetHardMemoryLimit(), nullptr, groupedMemoryLimiterCounters->BuildStageCounters("general"));
        auto manager = std::make_shared<NOlap::NGroupedMemoryManager::TManager>(NActors::TActorId(), config, "test", counters, stage);
        {
            manager->RegisterProcess(0, {});
            manager->RegisterProcessScope(0, 0);
            auto alloc1 = std::make_shared<TAllocation>(1000);
            manager->RegisterGroup(0, 0, 1);
            manager->RegisterAllocation(0, 0, 1, alloc1, {});
            UNIT_ASSERT(alloc1->IsAllocated());
            auto alloc2 = std::make_shared<TAllocation>(10);
            manager->RegisterGroup(0, 0, 3);
            manager->RegisterAllocation(0, 0, 3, alloc2, {});
            UNIT_ASSERT(!alloc2->IsAllocated());

            alloc1->Guard->Update(10);
            manager->AllocationUpdated(0, 0, alloc1->GetIdentifier(), 10);
            UNIT_ASSERT(alloc2->IsAllocated());

            manager->UnregisterGroup(0, 0, 3);
            manager->UnregisterAllocation(0, 0, alloc2->GetIdentifier());

            manager->UnregisterGroup(0, 0, 1);
            manager->UnregisterAllocation(0, 0, alloc1->GetIdentifier());
            manager->UnregisterProcess(0);
        }
        UNIT_ASSERT_VALUES_EQUAL(stage->GetUsage().Val(), 0);
        UNIT_ASSERT(manager->IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(TObjectCounter<TAllocation>::ObjectCount(), 0);
    }

    Y_UNIT_TEST(UnregisterScopeKeepsWaitingWhenScopeHasLinks) {
        auto groupedMemoryLimiterCounters = std::make_shared<NOlap::NGroupedMemoryManager::TCounters>(
            MakeIntrusive<NMonitoring::TDynamicCounters>(), "Scan");
        auto stage = std::make_shared<NOlap::NGroupedMemoryManager::TStageFeatures>("GLOBAL", 100, std::nullopt, nullptr,
            groupedMemoryLimiterCounters->BuildStageCounters("general"));
        NOlap::NGroupedMemoryManager::TProcessMemory process(0, 1, NActors::TActorId(), true, {}, stage);

        process.RegisterScope(0);
        process.RegisterScope(0);

        process.RegisterGroup(0, 1);
        auto alloc1 = std::make_shared<TAllocation>(100);
        process.RegisterAllocation(0, 1, alloc1, {});
        UNIT_ASSERT(alloc1->IsAllocated());

        process.RegisterGroup(0, 2);
        auto alloc2 = std::make_shared<TAllocation>(50);
        process.RegisterAllocation(0, 2, alloc2, {});
        UNIT_ASSERT(!alloc2->IsAllocated());
        UNIT_ASSERT(process.HasWaitingAllocations());

        process.UnregisterScope(0);
        UNIT_ASSERT(process.HasWaitingAllocations());

        alloc1->Guard.reset();
        UNIT_ASSERT(process.TryAllocateWaiting(1));
        UNIT_ASSERT(alloc2->IsAllocated());
        
        alloc2->Guard.reset();
        process.UnregisterAllocation(0, alloc1->GetIdentifier());
        process.UnregisterAllocation(0, alloc2->GetIdentifier());
        process.UnregisterGroup(0, 1);
        process.UnregisterGroup(0, 2);
        process.UnregisterScope(0);

        alloc1.reset();
        alloc2.reset();

        UNIT_ASSERT_VALUES_EQUAL(stage->GetUsage().Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(TObjectCounter<TAllocation>::ObjectCount(), 0);
    }

    class TFallibleAllocation: public NOlap::NGroupedMemoryManager::IAllocation {
    public:
        std::shared_ptr<NOlap::NGroupedMemoryManager::TAllocationGuard> Guard;
        TString Error;

    private:
        using TBase = NOlap::NGroupedMemoryManager::IAllocation;
        void DoOnAllocationImpossible(const TString& errorMessage) override {
            Error = errorMessage;
        }

        bool DoOnAllocated(std::shared_ptr<NOlap::NGroupedMemoryManager::TAllocationGuard>&& guard,
            const std::shared_ptr<NOlap::NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
            Guard = std::move(guard);
            return true;
        }

    public:
        explicit TFallibleAllocation(const ui64 mem)
            : TBase(mem) {
        }
    };

    class TOrderedAllocation: public NOlap::NGroupedMemoryManager::IAllocation {
    public:
        std::shared_ptr<NOlap::NGroupedMemoryManager::TAllocationGuard> Guard;
        std::vector<int>* Order = nullptr;
        int Tag = 0;

    private:
        using TBase = NOlap::NGroupedMemoryManager::IAllocation;
        void DoOnAllocationImpossible(const TString& /*errorMessage*/) override {
            UNIT_FAIL("allocation must wait or succeed");
        }

        bool DoOnAllocated(std::shared_ptr<NOlap::NGroupedMemoryManager::TAllocationGuard>&& guard,
            const std::shared_ptr<NOlap::NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
            if (Order) {
                Order->push_back(Tag);
            }
            Guard = std::move(guard);
            return true;
        }

    public:
        explicit TOrderedAllocation(const ui64 mem)
            : TBase(mem) {
        }
    };

    struct TBandLimiter {
        std::shared_ptr<NOlap::NGroupedMemoryManager::TStageFeatures> Stage;
        std::shared_ptr<NOlap::NGroupedMemoryManager::TManager> Manager;
        std::shared_ptr<NOlap::NGroupedMemoryManager::TCounters> Counters;
    };

    TBandLimiter MakeBandLimiter(const ui64 soft, const std::optional<ui64> hard, const std::optional<double> coefficient,
        const std::optional<ui64> unrestrictedOverride = {}, const std::optional<ui64> hardOverride = {}, const ui32 maxGroups = 1) {
        NOlap::NGroupedMemoryManager::TConfig config;
        NKikimrConfig::TGroupedMemoryLimiterConfig protoConfig;
        protoConfig.SetMemoryLimit(soft);
        if (hard) {
            protoConfig.SetHardMemoryLimit(*hard);
        }
        if (coefficient) {
            protoConfig.SetUnrestrictedSoftLimitCoefficient(*coefficient);
        }
        protoConfig.SetMaxUnrestrictedGroupsPerScope(maxGroups);
        UNIT_ASSERT(config.DeserializeFromProto(protoConfig));
        auto counters = std::make_shared<NOlap::NGroupedMemoryManager::TCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>(), "test");
        const std::optional<ui64> unrestricted = unrestrictedOverride ? unrestrictedOverride : config.MakeUnrestrictedSoftBytes(config.GetHardMemoryLimit());
        const std::optional<ui64> stageHard = hardOverride ? hardOverride : config.GetHardMemoryLimit();
        auto stage = std::make_shared<NOlap::NGroupedMemoryManager::TStageFeatures>(
            "GLOBAL", config.GetMemoryLimit(), stageHard, nullptr, counters->BuildStageCounters("general"), unrestricted);
        auto manager = std::make_shared<NOlap::NGroupedMemoryManager::TManager>(NActors::TActorId(), config, "test", counters, stage);
        return {stage, manager, counters};
    }

    Y_UNIT_TEST(UnrestrictedCoefficientRejectsOutOfRange) {
        NOlap::NGroupedMemoryManager::TConfig config;
        NKikimrConfig::TGroupedMemoryLimiterConfig protoConfig;
        protoConfig.SetUnrestrictedSoftLimitCoefficient(0.1);
        UNIT_ASSERT(!config.DeserializeFromProto(protoConfig));
        protoConfig.SetUnrestrictedSoftLimitCoefficient(1.5);
        UNIT_ASSERT(!config.DeserializeFromProto(protoConfig));
        protoConfig.SetUnrestrictedSoftLimitCoefficient(std::numeric_limits<double>::quiet_NaN());
        UNIT_ASSERT(!config.DeserializeFromProto(protoConfig));
        UNIT_ASSERT(!config.IsUnrestrictedEnabled());
        protoConfig.SetUnrestrictedSoftLimitCoefficient(0.5);
        protoConfig.SetMaxUnrestrictedGroupsPerScope(0);
        UNIT_ASSERT(!config.DeserializeFromProto(protoConfig));
        UNIT_ASSERT(!config.IsUnrestrictedEnabled());
        protoConfig.ClearMaxUnrestrictedGroupsPerScope();
        UNIT_ASSERT(config.DeserializeFromProto(protoConfig));
        UNIT_ASSERT(config.IsUnrestrictedEnabled());
        UNIT_ASSERT_VALUES_EQUAL(config.GetMaxUnrestrictedGroupsPerScope(), 1u);
    }

    Y_UNIT_TEST(FeatureOffPriorityHeadStillPassesSoft) {
        auto limiter = MakeBandLimiter(100, std::nullopt, std::nullopt);
        limiter.Manager->RegisterProcess(0, {});
        limiter.Manager->RegisterProcessScope(0, 0);
        limiter.Manager->RegisterGroup(0, 0, 1);
        auto head = std::make_shared<TAllocation>(150);
        limiter.Manager->RegisterAllocation(0, 0, 1, head, {});
        UNIT_ASSERT(head->IsAllocated());
        limiter.Manager->RegisterGroup(0, 0, 2);
        auto tail = std::make_shared<TAllocation>(10);
        limiter.Manager->RegisterAllocation(0, 0, 2, tail, {});
        UNIT_ASSERT(!tail->IsAllocated());

        head->Guard.reset();
        limiter.Manager->UnregisterAllocation(0, 0, head->GetIdentifier());
        limiter.Manager->UnregisterGroup(0, 0, 1);
        tail->Guard.reset();
        limiter.Manager->UnregisterAllocation(0, 0, tail->GetIdentifier());
        limiter.Manager->UnregisterGroup(0, 0, 2);
        limiter.Manager->UnregisterProcessScope(0, 0);
        limiter.Manager->UnregisterProcess(0);
        head.reset();
        tail.reset();
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(TObjectCounter<TAllocation>::ObjectCount(), 0);
    }

    Y_UNIT_TEST(FeatureOffHardLimitFailsPriorityHead) {
        auto limiter = MakeBandLimiter(100, 50, std::nullopt);
        limiter.Manager->RegisterProcess(0, {});
        limiter.Manager->RegisterProcessScope(0, 0);
        limiter.Manager->RegisterGroup(0, 0, 1);
        auto alloc = std::make_shared<TFallibleAllocation>(80);
        limiter.Manager->RegisterAllocation(0, 0, 1, alloc, {});
        UNIT_ASSERT(!alloc->IsAllocated());
        UNIT_ASSERT(!alloc->Error.empty());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 0);

        limiter.Manager->UnregisterGroup(0, 0, 1);
        limiter.Manager->UnregisterProcessScope(0, 0);
        limiter.Manager->UnregisterProcess(0);
        UNIT_ASSERT(limiter.Manager->IsEmpty());
    }

    Y_UNIT_TEST(AdmittedHeadsOfTwoScopesShareUnrestrictedSoft) {
        auto limiter = MakeBandLimiter(100, 1000, 0.5);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUnrestrictedSoft().value_or(0), 500u);

        limiter.Manager->RegisterProcess(0, {});
        limiter.Manager->RegisterProcessScope(0, 0);
        limiter.Manager->RegisterGroup(0, 0, 1);
        auto firstHead = std::make_shared<TAllocation>(120);
        limiter.Manager->RegisterAllocation(0, 0, 1, firstHead, {});
        UNIT_ASSERT(firstHead->IsAllocated());

        limiter.Manager->RegisterGroup(0, 0, 2);
        auto firstTail = std::make_shared<TAllocation>(80);
        limiter.Manager->RegisterAllocation(0, 0, 2, firstTail, {});
        UNIT_ASSERT(!firstTail->IsAllocated());

        limiter.Manager->RegisterProcess(1, {});
        limiter.Manager->RegisterProcessScope(1, 0);
        limiter.Manager->RegisterGroup(1, 0, 1);
        auto secondHead = std::make_shared<TAllocation>(120);
        limiter.Manager->RegisterAllocation(1, 0, 1, secondHead, {});
        UNIT_ASSERT(secondHead->IsAllocated());

        limiter.Manager->RegisterGroup(1, 0, 2);
        auto secondTail = std::make_shared<TAllocation>(80);
        limiter.Manager->RegisterAllocation(1, 0, 2, secondTail, {});
        UNIT_ASSERT(!secondTail->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->UnrestrictedAdmittedGroupsCount->Val(), 2u);

        firstHead->Guard.reset();
        limiter.Manager->UnregisterAllocation(0, 0, firstHead->GetIdentifier());
        UNIT_ASSERT(!firstTail->IsAllocated());
        limiter.Manager->UnregisterGroup(0, 0, 1);
        UNIT_ASSERT(firstTail->IsAllocated());
        UNIT_ASSERT(!secondTail->IsAllocated());

        firstTail->Guard.reset();
        secondHead->Guard.reset();
        limiter.Manager->UnregisterAllocation(0, 0, firstTail->GetIdentifier());
        limiter.Manager->UnregisterAllocation(1, 0, secondHead->GetIdentifier());
        limiter.Manager->UnregisterAllocation(1, 0, secondTail->GetIdentifier());
        limiter.Manager->UnregisterGroup(0, 0, 2);
        limiter.Manager->UnregisterGroup(1, 0, 1);
        limiter.Manager->UnregisterGroup(1, 0, 2);
        limiter.Manager->UnregisterProcessScope(0, 0);
        limiter.Manager->UnregisterProcess(0);
        limiter.Manager->UnregisterProcessScope(1, 0);
        limiter.Manager->UnregisterProcess(1);
        firstHead.reset();
        firstTail.reset();
        secondHead.reset();
        secondTail.reset();
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 0);
        UNIT_ASSERT(limiter.Manager->IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(TObjectCounter<TAllocation>::ObjectCount(), 0);
    }

    Y_UNIT_TEST(RequestAboveUnrestrictedSoftFails) {
        auto limiter = MakeBandLimiter(100, 1000, 0.5);
        limiter.Manager->RegisterProcess(0, {});
        limiter.Manager->RegisterProcessScope(0, 0);
        limiter.Manager->RegisterGroup(0, 0, 1);
        limiter.Manager->RegisterGroup(0, 0, 2);
        auto alloc = std::make_shared<TFallibleAllocation>(600);
        limiter.Manager->RegisterAllocation(0, 0, 1, alloc, {});
        UNIT_ASSERT(!alloc->IsAllocated());
        UNIT_ASSERT(!alloc->Error.empty());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetWaiting().Val(), 0);

        // The failed head does not hold the scope: the next group still gets the band.
        auto next = std::make_shared<TAllocation>(150);
        limiter.Manager->RegisterAllocation(0, 0, 2, next, {});
        UNIT_ASSERT(next->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->UnrestrictedAdmittedGroupsCount->Val(), 1u);

        next->Guard.reset();
        limiter.Manager->UnregisterAllocation(0, 0, next->GetIdentifier());
        limiter.Manager->UnregisterGroup(0, 0, 1);
        limiter.Manager->UnregisterGroup(0, 0, 2);
        limiter.Manager->UnregisterProcessScope(0, 0);
        limiter.Manager->UnregisterProcess(0);
        next.reset();
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 0);
        UNIT_ASSERT(limiter.Manager->IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(TObjectCounter<TAllocation>::ObjectCount(), 0);
    }

    Y_UNIT_TEST(ShrunkBandFailsWaitingRequest) {
        NOlap::NGroupedMemoryManager::TConfig config;
        NKikimrConfig::TGroupedMemoryLimiterConfig protoConfig;
        protoConfig.SetUnrestrictedSoftLimitCoefficient(0.5);
        UNIT_ASSERT(config.DeserializeFromProto(protoConfig));
        auto counters = std::make_shared<NOlap::NGroupedMemoryManager::TCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>(), "test");
        auto stage = std::make_shared<NOlap::NGroupedMemoryManager::TStageFeatures>(
            "GLOBAL", std::nullopt, std::nullopt, nullptr, counters->BuildStageCounters("general"));
        auto manager = std::make_shared<NOlap::NGroupedMemoryManager::TManager>(NActors::TActorId(), config, "test", counters, stage);
        manager->UpdateMemoryLimits(100, 1000, config.MakeUnrestrictedSoftBytes(1000));
        UNIT_ASSERT_VALUES_EQUAL(stage->GetUnrestrictedSoft().value_or(0), 500u);

        manager->RegisterProcess(0, {});
        manager->RegisterProcessScope(0, 0);
        manager->RegisterGroup(0, 0, 1);
        manager->RegisterGroup(0, 0, 2);
        auto head = std::make_shared<TAllocation>(300);
        manager->RegisterAllocation(0, 0, 1, head, {});
        UNIT_ASSERT(head->IsAllocated());
        auto tail = std::make_shared<TFallibleAllocation>(250);
        manager->RegisterAllocation(0, 0, 2, tail, {});
        UNIT_ASSERT(!tail->IsAllocated());
        UNIT_ASSERT(tail->Error.empty());

        manager->UpdateMemoryLimits(60, 400, config.MakeUnrestrictedSoftBytes(400));
        UNIT_ASSERT_VALUES_EQUAL(stage->GetUnrestrictedSoft().value_or(0), 200u);
        UNIT_ASSERT(!tail->IsAllocated());
        UNIT_ASSERT(!tail->Error.empty());
        UNIT_ASSERT_VALUES_EQUAL(stage->GetUsage().Val(), 300u);
        UNIT_ASSERT_VALUES_EQUAL(stage->GetWaiting().Val(), 0u);

        head->Guard.reset();
        manager->UnregisterAllocation(0, 0, head->GetIdentifier());
        manager->UnregisterGroup(0, 0, 1);
        manager->UnregisterGroup(0, 0, 2);
        manager->UnregisterProcessScope(0, 0);
        manager->UnregisterProcess(0);
        head.reset();
        UNIT_ASSERT_VALUES_EQUAL(stage->GetUsage().Val(), 0);
        UNIT_ASSERT(manager->IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(TObjectCounter<TAllocation>::ObjectCount(), 0);
    }

    Y_UNIT_TEST(ConfiguredHardLimitKeepsItsBand) {
        NOlap::NGroupedMemoryManager::TConfig config;
        NKikimrConfig::TGroupedMemoryLimiterConfig protoConfig;
        protoConfig.SetMemoryLimit(100);
        protoConfig.SetHardMemoryLimit(200);
        protoConfig.SetUnrestrictedSoftLimitCoefficient(0.5);
        UNIT_ASSERT(config.DeserializeFromProto(protoConfig));
        auto counters = std::make_shared<NOlap::NGroupedMemoryManager::TCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>(), "test");
        auto stage = std::make_shared<NOlap::NGroupedMemoryManager::TStageFeatures>(
            "GLOBAL", config.GetMemoryLimit(), config.GetHardMemoryLimit(), nullptr, counters->BuildStageCounters("general"),
            config.MakeUnrestrictedSoftBytes(config.GetHardMemoryLimit()));
        auto manager = std::make_shared<NOlap::NGroupedMemoryManager::TManager>(NActors::TActorId(), config, "test", counters, stage);
        UNIT_ASSERT_VALUES_EQUAL(stage->GetHardLimit().value_or(0), 200u);
        UNIT_ASSERT_VALUES_EQUAL(stage->GetUnrestrictedSoft().value_or(0), 100u);

        manager->UpdateMemoryLimits(300, 1000, config.MakeUnrestrictedSoftBytes(1000));
        UNIT_ASSERT_VALUES_EQUAL(stage->GetLimit(), 100u);
        UNIT_ASSERT_VALUES_EQUAL(stage->GetHardLimit().value_or(0), 200u);
        UNIT_ASSERT_VALUES_EQUAL(stage->GetUnrestrictedSoft().value_or(0), 100u);
    }

    Y_UNIT_TEST(TwoAdmissionSlotsShareTheBand) {
        auto limiter = MakeBandLimiter(100, 1000, 0.5, {}, {}, 2);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUnrestrictedSoft().value_or(0), 500u);

        limiter.Manager->RegisterProcess(0, {});
        limiter.Manager->RegisterProcessScope(0, 0);
        limiter.Manager->RegisterGroup(0, 0, 1);
        limiter.Manager->RegisterGroup(0, 0, 2);
        limiter.Manager->RegisterGroup(0, 0, 3);

        auto g1 = std::make_shared<TAllocation>(150);
        limiter.Manager->RegisterAllocation(0, 0, 1, g1, {});
        UNIT_ASSERT(g1->IsAllocated());
        auto g2 = std::make_shared<TAllocation>(150);
        limiter.Manager->RegisterAllocation(0, 0, 2, g2, {});
        UNIT_ASSERT(g2->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->UnrestrictedAdmittedGroupsCount->Val(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->AdmittedBytes->Val(), 300u);

        // Fits the band, but both slots are taken and neither holder is stuck.
        auto g3 = std::make_shared<TAllocation>(100);
        limiter.Manager->RegisterAllocation(0, 0, 3, g3, {});
        UNIT_ASSERT(!g3->IsAllocated());

        // G1 asks for more than the band can give. Its slot is released to G3 while G2 keeps its slot.
        auto g1Next = std::make_shared<TAllocation>(300);
        limiter.Manager->RegisterAllocation(0, 0, 1, g1Next, {});
        UNIT_ASSERT(!g1Next->IsAllocated());
        UNIT_ASSERT(g3->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 400u);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->UnrestrictedAdmittedGroupsCount->Val(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->AdmittedBytes->Val(), 250u);

        g3->Guard.reset();
        limiter.Manager->UnregisterAllocation(0, 0, g3->GetIdentifier());
        limiter.Manager->UnregisterGroup(0, 0, 3);
        UNIT_ASSERT(!g1Next->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->UnrestrictedAdmittedGroupsCount->Val(), 1u);

        g2->Guard.reset();
        limiter.Manager->UnregisterAllocation(0, 0, g2->GetIdentifier());
        limiter.Manager->UnregisterGroup(0, 0, 2);
        UNIT_ASSERT(g1Next->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 450u);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->UnrestrictedAdmittedGroupsCount->Val(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->AdmittedBytes->Val(), 450u);

        g1->Guard.reset();
        g1Next->Guard.reset();
        limiter.Manager->UnregisterAllocation(0, 0, g1->GetIdentifier());
        limiter.Manager->UnregisterAllocation(0, 0, g1Next->GetIdentifier());
        limiter.Manager->UnregisterGroup(0, 0, 1);
        limiter.Manager->UnregisterProcessScope(0, 0);
        limiter.Manager->UnregisterProcess(0);
        g1.reset();
        g1Next.reset();
        g2.reset();
        g3.reset();
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 0);
        UNIT_ASSERT(limiter.Manager->IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(TObjectCounter<TAllocation>::ObjectCount(), 0);
    }

    Y_UNIT_TEST(ProcessStageLimitCapsTheBand) {
        auto limiter = MakeBandLimiter(100, 1000, 0.5);
        auto stage = std::make_shared<NOlap::NGroupedMemoryManager::TStageFeatures>("STAGE", 200, std::nullopt, nullptr, nullptr);
        stage->AttachOwner(limiter.Stage);
        UNIT_ASSERT_VALUES_EQUAL(stage->GetEffectiveUnrestrictedLimit(), 200u);

        limiter.Manager->RegisterProcess(0, {stage});
        limiter.Manager->RegisterProcessScope(0, 0);
        limiter.Manager->RegisterGroup(0, 0, 1);
        limiter.Manager->RegisterGroup(0, 0, 2);

        // Above the global soft limit, inside the stage limit and the band.
        auto head = std::make_shared<TAllocation>(150);
        limiter.Manager->RegisterAllocation(0, 0, 1, head, 0);
        UNIT_ASSERT(head->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(stage->GetUsage().Val(), 150u);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 150u);

        // Inside the band, above the stage limit: can never fit.
        auto tooBig = std::make_shared<TFallibleAllocation>(250);
        limiter.Manager->RegisterAllocation(0, 0, 2, tooBig, 0);
        UNIT_ASSERT(!tooBig->IsAllocated());
        UNIT_ASSERT(!tooBig->Error.empty());
        UNIT_ASSERT_VALUES_EQUAL(stage->GetWaiting().Val(), 0u);

        // Fits the stage limit alone but not next to the head. The head is the only holder and it is
        // the one waiting, so nothing would ever be released: the request is forced above the stage limit.
        auto next = std::make_shared<TAllocation>(100);
        limiter.Manager->RegisterAllocation(0, 0, 1, next, 0);
        UNIT_ASSERT(next->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(stage->GetUsage().Val(), 250u);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 250u);

        head->Guard.reset();
        next->Guard.reset();
        limiter.Manager->UnregisterAllocation(0, 0, head->GetIdentifier());
        limiter.Manager->UnregisterAllocation(0, 0, next->GetIdentifier());
        limiter.Manager->UnregisterGroup(0, 0, 1);
        limiter.Manager->UnregisterGroup(0, 0, 2);
        limiter.Manager->UnregisterProcessScope(0, 0);
        limiter.Manager->UnregisterProcess(0);
        head.reset();
        next.reset();
        UNIT_ASSERT_VALUES_EQUAL(stage->GetUsage().Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 0);
        UNIT_ASSERT(limiter.Manager->IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(TObjectCounter<TAllocation>::ObjectCount(), 0);
    }

    Y_UNIT_TEST(RequestInsideUnrestrictedSoftAboveHardFails) {
        auto limiter = MakeBandLimiter(100, 1000, 0.5, 200, 150);
        limiter.Manager->RegisterProcess(0, {});
        limiter.Manager->RegisterProcessScope(0, 0);
        limiter.Manager->RegisterGroup(0, 0, 1);
        auto alloc = std::make_shared<TFallibleAllocation>(180);
        limiter.Manager->RegisterAllocation(0, 0, 1, alloc, {});
        UNIT_ASSERT(!alloc->IsAllocated());
        UNIT_ASSERT(!alloc->Error.empty());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 0);

        limiter.Manager->UnregisterGroup(0, 0, 1);
        limiter.Manager->UnregisterProcessScope(0, 0);
        limiter.Manager->UnregisterProcess(0);
        UNIT_ASSERT(limiter.Manager->IsEmpty());
    }

    Y_UNIT_TEST(ScopeWithoutAdmissionIsServedFirst) {
        auto limiter = MakeBandLimiter(100, 400, 0.5);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUnrestrictedSoft().value_or(0), 200u);
        std::vector<int> order;

        limiter.Manager->RegisterProcess(0, {});
        limiter.Manager->RegisterProcessScope(0, 0);
        limiter.Manager->RegisterGroup(0, 0, 1);
        limiter.Manager->RegisterGroup(0, 0, 2);
        // A group that holds memory under soft and asks for nothing more: it is still working,
        // so the waits below are not a deadlock.
        auto worker = std::make_shared<TAllocation>(10);
        limiter.Manager->RegisterAllocation(0, 0, 2, worker, {});
        UNIT_ASSERT(worker->IsAllocated());
        auto head = std::make_shared<TAllocation>(180);
        limiter.Manager->RegisterAllocation(0, 0, 1, head, {});
        UNIT_ASSERT(head->IsAllocated());

        auto held = std::make_shared<TOrderedAllocation>(30);
        held->Order = &order;
        held->Tag = 1;
        limiter.Manager->RegisterAllocation(0, 0, 1, held, {});
        UNIT_ASSERT(!held->IsAllocated());

        limiter.Manager->RegisterProcess(1, {});
        limiter.Manager->RegisterProcessScope(1, 0);
        limiter.Manager->RegisterGroup(1, 0, 1);
        auto fresh = std::make_shared<TOrderedAllocation>(30);
        fresh->Order = &order;
        fresh->Tag = 2;
        limiter.Manager->RegisterAllocation(1, 0, 1, fresh, {});
        UNIT_ASSERT(!fresh->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->AdmittedBytes->Val(), 180u);

        head->Guard->Update(130);
        limiter.Manager->AllocationUpdated(0, 0, head->GetIdentifier(), 130);
        UNIT_ASSERT(fresh->IsAllocated());
        UNIT_ASSERT(held->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->AdmittedBytes->Val(), 190u);
        UNIT_ASSERT_VALUES_EQUAL(order.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(order[0], 2);
        UNIT_ASSERT_VALUES_EQUAL(order[1], 1);

        head->Guard.reset();
        held->Guard.reset();
        fresh->Guard.reset();
        worker->Guard.reset();
        limiter.Manager->UnregisterAllocation(0, 0, head->GetIdentifier());
        limiter.Manager->UnregisterAllocation(0, 0, held->GetIdentifier());
        limiter.Manager->UnregisterAllocation(1, 0, fresh->GetIdentifier());
        limiter.Manager->UnregisterAllocation(0, 0, worker->GetIdentifier());
        limiter.Manager->UnregisterGroup(0, 0, 1);
        limiter.Manager->UnregisterGroup(0, 0, 2);
        limiter.Manager->UnregisterGroup(1, 0, 1);
        limiter.Manager->UnregisterProcessScope(0, 0);
        limiter.Manager->UnregisterProcess(0);
        limiter.Manager->UnregisterProcessScope(1, 0);
        limiter.Manager->UnregisterProcess(1);
        head.reset();
        held.reset();
        fresh.reset();
        worker.reset();
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 0);
        UNIT_ASSERT(limiter.Manager->IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(TObjectCounter<TAllocation>::ObjectCount(), 0);
    }

    Y_UNIT_TEST(StaticSoftLimitTakesDynamicUnrestrictedBand) {
        NOlap::NGroupedMemoryManager::TConfig config;
        NKikimrConfig::TGroupedMemoryLimiterConfig protoConfig;
        protoConfig.SetMemoryLimit(100);
        protoConfig.SetUnrestrictedSoftLimitCoefficient(0.5);
        UNIT_ASSERT(config.DeserializeFromProto(protoConfig));
        auto counters = std::make_shared<NOlap::NGroupedMemoryManager::TCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>(), "test");
        auto stage = std::make_shared<NOlap::NGroupedMemoryManager::TStageFeatures>(
            "GLOBAL", config.GetMemoryLimit(), config.GetHardMemoryLimit(), nullptr, counters->BuildStageCounters("general"),
            config.MakeUnrestrictedSoftBytes(config.GetHardMemoryLimit()));
        auto manager = std::make_shared<NOlap::NGroupedMemoryManager::TManager>(NActors::TActorId(), config, "test", counters, stage);
        UNIT_ASSERT(!stage->GetUnrestrictedSoft().has_value());
        UNIT_ASSERT_VALUES_EQUAL(stage->GetLimit(), 100u);

        const ui64 hard = 1000;
        manager->UpdateMemoryLimits(300, hard, config.MakeUnrestrictedSoftBytes(hard));
        UNIT_ASSERT_VALUES_EQUAL(stage->GetLimit(), 100u);
        UNIT_ASSERT_VALUES_EQUAL(stage->GetHardLimit().value_or(0), hard);
        UNIT_ASSERT_VALUES_EQUAL(stage->GetUnrestrictedSoft().value_or(0), 500u);

        manager->RegisterProcess(0, {});
        manager->RegisterProcessScope(0, 0);
        manager->RegisterGroup(0, 0, 1);
        auto alloc = std::make_shared<TAllocation>(120);
        manager->RegisterAllocation(0, 0, 1, alloc, {});
        UNIT_ASSERT(alloc->IsAllocated());

        alloc->Guard.reset();
        manager->UnregisterAllocation(0, 0, alloc->GetIdentifier());
        manager->UnregisterGroup(0, 0, 1);
        manager->UnregisterProcessScope(0, 0);
        manager->UnregisterProcess(0);
        alloc.reset();
        UNIT_ASSERT_VALUES_EQUAL(stage->GetUsage().Val(), 0);
        UNIT_ASSERT(manager->IsEmpty());
    }

    Y_UNIT_TEST(StuckAdmissionYieldsToSmallerGroup) {
        auto limiter = MakeBandLimiter(100, 1000, 0.5);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUnrestrictedSoft().value_or(0), 500u);

        limiter.Manager->RegisterProcess(0, {});
        limiter.Manager->RegisterProcessScope(0, 0);
        limiter.Manager->RegisterGroup(0, 0, 1);
        limiter.Manager->RegisterGroup(0, 0, 2);

        auto g1 = std::make_shared<TAllocation>(80);
        limiter.Manager->RegisterAllocation(0, 0, 1, g1, {});
        UNIT_ASSERT(g1->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->UnrestrictedAdmittedGroupsCount->Val(), 0u);

        auto g2 = std::make_shared<TAllocation>(40);
        limiter.Manager->RegisterAllocation(0, 0, 2, g2, {});
        UNIT_ASSERT(g2->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 120u);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->UnrestrictedAdmittedGroupsCount->Val(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->AdmittedBytes->Val(), 40u);

        auto g1Next = std::make_shared<TAllocation>(30);
        limiter.Manager->RegisterAllocation(0, 0, 1, g1Next, {});
        UNIT_ASSERT(!g1Next->IsAllocated());

        auto g2Next = std::make_shared<TAllocation>(400);
        limiter.Manager->RegisterAllocation(0, 0, 2, g2Next, {});
        UNIT_ASSERT(g1Next->IsAllocated());
        UNIT_ASSERT(!g2Next->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 150u);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->UnrestrictedAdmittedGroupsCount->Val(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->AdmittedBytes->Val(), 110u);

        g1->Guard.reset();
        g1Next->Guard.reset();
        limiter.Manager->UnregisterAllocation(0, 0, g1->GetIdentifier());
        limiter.Manager->UnregisterAllocation(0, 0, g1Next->GetIdentifier());
        limiter.Manager->UnregisterGroup(0, 0, 1);
        UNIT_ASSERT(g2Next->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->UnrestrictedAdmittedGroupsCount->Val(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->AdmittedBytes->Val(), 440u);

        g2->Guard.reset();
        g2Next->Guard.reset();
        limiter.Manager->UnregisterAllocation(0, 0, g2->GetIdentifier());
        limiter.Manager->UnregisterAllocation(0, 0, g2Next->GetIdentifier());
        limiter.Manager->UnregisterGroup(0, 0, 2);
        limiter.Manager->UnregisterProcessScope(0, 0);
        limiter.Manager->UnregisterProcess(0);
        g1.reset();
        g1Next.reset();
        g2.reset();
        g2Next.reset();
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 0);
        UNIT_ASSERT(limiter.Manager->IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(TObjectCounter<TAllocation>::ObjectCount(), 0);
    }

    Y_UNIT_TEST(StuckAdmissionYieldsPastBlockedMiddleGroup) {
        auto limiter = MakeBandLimiter(100, 1000, 0.5);
        limiter.Manager->RegisterProcess(0, {});
        limiter.Manager->RegisterProcessScope(0, 0);
        limiter.Manager->RegisterGroup(0, 0, 1);
        limiter.Manager->RegisterGroup(0, 0, 2);
        limiter.Manager->RegisterGroup(0, 0, 3);

        auto g3 = std::make_shared<TAllocation>(80);
        limiter.Manager->RegisterAllocation(0, 0, 3, g3, {});
        UNIT_ASSERT(g3->IsAllocated());
        auto g1 = std::make_shared<TAllocation>(40);
        limiter.Manager->RegisterAllocation(0, 0, 1, g1, {});
        UNIT_ASSERT(g1->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->UnrestrictedAdmittedGroupsCount->Val(), 1u);

        // G2 holds nothing and its request does not fit. G3 holds 80 and its request does fit.
        auto g2 = std::make_shared<TAllocation>(400);
        limiter.Manager->RegisterAllocation(0, 0, 2, g2, {});
        UNIT_ASSERT(!g2->IsAllocated());
        auto g3Next = std::make_shared<TAllocation>(30);
        limiter.Manager->RegisterAllocation(0, 0, 3, g3Next, {});
        UNIT_ASSERT(!g3Next->IsAllocated());

        // G1 gets stuck on 400. Its slot goes past G2 to G3, whose 30 fits.
        auto g1Next = std::make_shared<TAllocation>(400);
        limiter.Manager->RegisterAllocation(0, 0, 1, g1Next, {});
        UNIT_ASSERT(g3Next->IsAllocated());
        UNIT_ASSERT(!g1Next->IsAllocated());
        UNIT_ASSERT(!g2->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 150u);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->UnrestrictedAdmittedGroupsCount->Val(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->AdmittedBytes->Val(), 110u);

        // G3 finishes. G1 is the smallest waiting group and now fits: 40 + 400.
        g3->Guard.reset();
        g3Next->Guard.reset();
        limiter.Manager->UnregisterAllocation(0, 0, g3->GetIdentifier());
        limiter.Manager->UnregisterAllocation(0, 0, g3Next->GetIdentifier());
        limiter.Manager->UnregisterGroup(0, 0, 3);
        UNIT_ASSERT(g1Next->IsAllocated());
        UNIT_ASSERT(!g2->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 440u);

        g1->Guard.reset();
        g1Next->Guard.reset();
        limiter.Manager->UnregisterAllocation(0, 0, g1->GetIdentifier());
        limiter.Manager->UnregisterAllocation(0, 0, g1Next->GetIdentifier());
        limiter.Manager->UnregisterGroup(0, 0, 1);
        UNIT_ASSERT(g2->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 400u);

        g2->Guard.reset();
        limiter.Manager->UnregisterAllocation(0, 0, g2->GetIdentifier());
        limiter.Manager->UnregisterGroup(0, 0, 2);
        limiter.Manager->UnregisterProcessScope(0, 0);
        limiter.Manager->UnregisterProcess(0);
        g1.reset();
        g1Next.reset();
        g2.reset();
        g3.reset();
        g3Next.reset();
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 0);
        UNIT_ASSERT(limiter.Manager->IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(TObjectCounter<TAllocation>::ObjectCount(), 0);
    }

    Y_UNIT_TEST(DeadlockedHoldersForceOneAboveBand) {
        auto limiter = MakeBandLimiter(100, 1000, 0.5);
        limiter.Manager->RegisterProcess(0, {});
        limiter.Manager->RegisterProcessScope(0, 0);
        limiter.Manager->RegisterGroup(0, 0, 1);
        limiter.Manager->RegisterGroup(0, 0, 2);

        auto g1 = std::make_shared<TAllocation>(80);
        limiter.Manager->RegisterAllocation(0, 0, 1, g1, {});
        UNIT_ASSERT(g1->IsAllocated());
        auto g2 = std::make_shared<TAllocation>(40);
        limiter.Manager->RegisterAllocation(0, 0, 2, g2, {});
        UNIT_ASSERT(g2->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->UnrestrictedAdmittedGroupsCount->Val(), 1u);

        // G2 still holds nothing pending, so it may release: G1 waits.
        auto g1Next = std::make_shared<TAllocation>(400);
        limiter.Manager->RegisterAllocation(0, 0, 1, g1Next, {});
        UNIT_ASSERT(!g1Next->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 120u);

        // Now both holders wait for their own next request. The admitted G2 is forced above the band.
        auto g2Next = std::make_shared<TAllocation>(400);
        limiter.Manager->RegisterAllocation(0, 0, 2, g2Next, {});
        UNIT_ASSERT(g2Next->IsAllocated());
        UNIT_ASSERT(!g1Next->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 520u);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->AdmittedBytes->Val(), 440u);

        g2->Guard.reset();
        g2Next->Guard.reset();
        limiter.Manager->UnregisterAllocation(0, 0, g2->GetIdentifier());
        limiter.Manager->UnregisterAllocation(0, 0, g2Next->GetIdentifier());
        limiter.Manager->UnregisterGroup(0, 0, 2);
        UNIT_ASSERT(g1Next->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 480u);

        g1->Guard.reset();
        g1Next->Guard.reset();
        limiter.Manager->UnregisterAllocation(0, 0, g1->GetIdentifier());
        limiter.Manager->UnregisterAllocation(0, 0, g1Next->GetIdentifier());
        limiter.Manager->UnregisterGroup(0, 0, 1);
        limiter.Manager->UnregisterProcessScope(0, 0);
        limiter.Manager->UnregisterProcess(0);
        g1.reset();
        g1Next.reset();
        g2.reset();
        g2Next.reset();
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 0);
        UNIT_ASSERT(limiter.Manager->IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(TObjectCounter<TAllocation>::ObjectCount(), 0);
    }

    Y_UNIT_TEST(DeadlockForcingServesAllRequestsOfOneHolder) {
        auto limiter = MakeBandLimiter(100, 1000, 0.5);
        limiter.Manager->RegisterProcess(0, {});
        limiter.Manager->RegisterProcessScope(0, 0);
        limiter.Manager->RegisterGroup(0, 0, 1);
        limiter.Manager->RegisterGroup(0, 0, 2);

        auto g1 = std::make_shared<TAllocation>(80);
        limiter.Manager->RegisterAllocation(0, 0, 1, g1, {});
        UNIT_ASSERT(g1->IsAllocated());
        auto g2 = std::make_shared<TAllocation>(400);
        limiter.Manager->RegisterAllocation(0, 0, 2, g2, {});
        UNIT_ASSERT(g2->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 480u);

        // G2 has two pending requests; G1 holds and waits for nothing yet, so nothing is forced.
        auto g2A = std::make_shared<TAllocation>(30);
        auto g2B = std::make_shared<TAllocation>(30);
        limiter.Manager->RegisterAllocation(0, 0, 2, g2A, {});
        limiter.Manager->RegisterAllocation(0, 0, 2, g2B, {});
        UNIT_ASSERT(!g2A->IsAllocated());
        UNIT_ASSERT(!g2B->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 480u);

        // Now every holder waits. Forcing must serve both of G2's requests, not just one: G2 can release only after both.
        auto g1Next = std::make_shared<TAllocation>(30);
        limiter.Manager->RegisterAllocation(0, 0, 1, g1Next, {});
        UNIT_ASSERT(g2A->IsAllocated());
        UNIT_ASSERT(g2B->IsAllocated());
        UNIT_ASSERT(!g1Next->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 540u);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->AdmittedBytes->Val(), 460u);

        g2->Guard.reset();
        g2A->Guard.reset();
        g2B->Guard.reset();
        limiter.Manager->UnregisterAllocation(0, 0, g2->GetIdentifier());
        limiter.Manager->UnregisterAllocation(0, 0, g2A->GetIdentifier());
        limiter.Manager->UnregisterAllocation(0, 0, g2B->GetIdentifier());
        limiter.Manager->UnregisterGroup(0, 0, 2);
        UNIT_ASSERT(g1Next->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 110u);

        g1->Guard.reset();
        g1Next->Guard.reset();
        limiter.Manager->UnregisterAllocation(0, 0, g1->GetIdentifier());
        limiter.Manager->UnregisterAllocation(0, 0, g1Next->GetIdentifier());
        limiter.Manager->UnregisterGroup(0, 0, 1);
        limiter.Manager->UnregisterProcessScope(0, 0);
        limiter.Manager->UnregisterProcess(0);
        g1.reset();
        g1Next.reset();
        g2.reset();
        g2A.reset();
        g2B.reset();
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 0);
        UNIT_ASSERT(limiter.Manager->IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(TObjectCounter<TAllocation>::ObjectCount(), 0);
    }

    Y_UNIT_TEST(ZeroByteHolderDoesNotBlockDeadlockRecovery) {
        auto limiter = MakeBandLimiter(100, 1000, 0.5);
        limiter.Manager->RegisterProcess(0, {});
        limiter.Manager->RegisterProcessScope(0, 0);
        limiter.Manager->RegisterGroup(0, 0, 1);
        limiter.Manager->RegisterGroup(0, 0, 2);

        // G2 keeps an empty guard. It contributes no usage and has nothing to release.
        auto empty = std::make_shared<TAllocation>(0);
        limiter.Manager->RegisterAllocation(0, 0, 2, empty, {});
        UNIT_ASSERT(empty->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 0u);

        auto g1 = std::make_shared<TAllocation>(300);
        limiter.Manager->RegisterAllocation(0, 0, 1, g1, {});
        UNIT_ASSERT(g1->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 300u);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->UnrestrictedAdmittedGroupsCount->Val(), 1u);

        // 300 + 300 exceeds the band and stays under the hard limit. The empty guard must not block the force.
        auto g1Next = std::make_shared<TAllocation>(300);
        limiter.Manager->RegisterAllocation(0, 0, 1, g1Next, {});
        UNIT_ASSERT(g1Next->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 600u);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->AdmittedBytes->Val(), 600u);

        empty->Guard.reset();
        g1->Guard.reset();
        g1Next->Guard.reset();
        limiter.Manager->UnregisterAllocation(0, 0, empty->GetIdentifier());
        limiter.Manager->UnregisterAllocation(0, 0, g1->GetIdentifier());
        limiter.Manager->UnregisterAllocation(0, 0, g1Next->GetIdentifier());
        limiter.Manager->UnregisterGroup(0, 0, 1);
        limiter.Manager->UnregisterGroup(0, 0, 2);
        limiter.Manager->UnregisterProcessScope(0, 0);
        limiter.Manager->UnregisterProcess(0);
        empty.reset();
        g1.reset();
        g1Next.reset();
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 0);
        UNIT_ASSERT(limiter.Manager->IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(TObjectCounter<TAllocation>::ObjectCount(), 0);
    }

    Y_UNIT_TEST(StuckAdmissionYieldsToLaterGroup) {
        auto limiter = MakeBandLimiter(100, 1000, 0.5);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUnrestrictedSoft().value_or(0), 500u);

        limiter.Manager->RegisterProcess(0, {});
        limiter.Manager->RegisterProcessScope(0, 0);
        limiter.Manager->RegisterGroup(0, 0, 1);
        limiter.Manager->RegisterGroup(0, 0, 2);

        auto g2 = std::make_shared<TAllocation>(80);
        limiter.Manager->RegisterAllocation(0, 0, 2, g2, {});
        UNIT_ASSERT(g2->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->UnrestrictedAdmittedGroupsCount->Val(), 0u);

        auto g1 = std::make_shared<TAllocation>(40);
        limiter.Manager->RegisterAllocation(0, 0, 1, g1, {});
        UNIT_ASSERT(g1->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 120u);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->UnrestrictedAdmittedGroupsCount->Val(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->AdmittedBytes->Val(), 40u);

        auto g2Next = std::make_shared<TAllocation>(30);
        limiter.Manager->RegisterAllocation(0, 0, 2, g2Next, {});
        UNIT_ASSERT(!g2Next->IsAllocated());

        auto g1Next = std::make_shared<TAllocation>(400);
        limiter.Manager->RegisterAllocation(0, 0, 1, g1Next, {});
        UNIT_ASSERT(g2Next->IsAllocated());
        UNIT_ASSERT(!g1Next->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 150u);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->UnrestrictedAdmittedGroupsCount->Val(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->AdmittedBytes->Val(), 110u);

        g2->Guard.reset();
        g2Next->Guard.reset();
        limiter.Manager->UnregisterAllocation(0, 0, g2->GetIdentifier());
        limiter.Manager->UnregisterAllocation(0, 0, g2Next->GetIdentifier());
        limiter.Manager->UnregisterGroup(0, 0, 2);
        UNIT_ASSERT(g1Next->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->UnrestrictedAdmittedGroupsCount->Val(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Counters->AdmittedBytes->Val(), 440u);

        g1->Guard.reset();
        g1Next->Guard.reset();
        limiter.Manager->UnregisterAllocation(0, 0, g1->GetIdentifier());
        limiter.Manager->UnregisterAllocation(0, 0, g1Next->GetIdentifier());
        limiter.Manager->UnregisterGroup(0, 0, 1);
        limiter.Manager->UnregisterProcessScope(0, 0);
        limiter.Manager->UnregisterProcess(0);
        g1.reset();
        g1Next.reset();
        g2.reset();
        g2Next.reset();
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 0);
        UNIT_ASSERT(limiter.Manager->IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(TObjectCounter<TAllocation>::ObjectCount(), 0);
    }
};

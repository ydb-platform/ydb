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
            manager->AllocationUpdated(0, 0, alloc1->GetIdentifier());
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
        const std::optional<ui64> unrestrictedOverride = {}, const std::optional<ui64> hardOverride = {}) {
        NOlap::NGroupedMemoryManager::TConfig config;
        NKikimrConfig::TGroupedMemoryLimiterConfig protoConfig;
        protoConfig.SetMemoryLimit(soft);
        if (hard) {
            protoConfig.SetHardMemoryLimit(*hard);
        }
        if (coefficient) {
            protoConfig.SetUnrestrictedSoftLimitCoefficient(*coefficient);
        }
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

    Y_UNIT_TEST(RequestAboveUnrestrictedSoftWaits) {
        auto limiter = MakeBandLimiter(100, 1000, 0.5);
        limiter.Manager->RegisterProcess(0, {});
        limiter.Manager->RegisterProcessScope(0, 0);
        limiter.Manager->RegisterGroup(0, 0, 1);
        auto alloc = std::make_shared<TFallibleAllocation>(600);
        limiter.Manager->RegisterAllocation(0, 0, 1, alloc, {});
        UNIT_ASSERT(!alloc->IsAllocated());
        UNIT_ASSERT(alloc->Error.empty());

        limiter.Manager->UnregisterAllocation(0, 0, alloc->GetIdentifier());
        limiter.Manager->UnregisterGroup(0, 0, 1);
        limiter.Manager->UnregisterProcessScope(0, 0);
        limiter.Manager->UnregisterProcess(0);
        UNIT_ASSERT_VALUES_EQUAL(limiter.Stage->GetUsage().Val(), 0);
        UNIT_ASSERT(limiter.Manager->IsEmpty());
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

        head->Guard->Update(140);
        limiter.Manager->AllocationUpdated(0, 0, head->GetIdentifier());
        UNIT_ASSERT(fresh->IsAllocated());
        UNIT_ASSERT(held->IsAllocated());
        UNIT_ASSERT_VALUES_EQUAL(order.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(order[0], 2);
        UNIT_ASSERT_VALUES_EQUAL(order[1], 1);

        head->Guard.reset();
        held->Guard.reset();
        fresh->Guard.reset();
        limiter.Manager->UnregisterAllocation(0, 0, head->GetIdentifier());
        limiter.Manager->UnregisterAllocation(0, 0, held->GetIdentifier());
        limiter.Manager->UnregisterAllocation(1, 0, fresh->GetIdentifier());
        limiter.Manager->UnregisterGroup(0, 0, 1);
        limiter.Manager->UnregisterGroup(1, 0, 1);
        limiter.Manager->UnregisterProcessScope(0, 0);
        limiter.Manager->UnregisterProcess(0);
        limiter.Manager->UnregisterProcessScope(1, 0);
        limiter.Manager->UnregisterProcess(1);
        head.reset();
        held.reset();
        fresh.reset();
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

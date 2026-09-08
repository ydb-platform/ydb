#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_memory_quota.h>
#include <ydb/library/yql/dq/comp_nodes/operator_memory_quota/dq_operator_memory_quota.h>

#include <yql/essentials/minikql/mkql_alloc.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/ptr.h>
#include <util/generic/size_literals.h>

#include <limits>
#include <memory>
#include <utility>
#include <vector>

namespace NYql::NDq {

namespace {

using namespace NKikimr::NMiniKQL;

// Scripted IMemoryQuotaManager: counts requests, refuses on demand, reports a scripted availability
struct TStubQuotaManager : public IMemoryQuotaManager {
    struct TRequest {
        ui64 Size;
        bool Optional;
    };

    bool AllocateQuota(ui64 memorySize, bool isOptional) override {
        Requests.push_back({memorySize, isOptional});
        if (RefuseNext > 0) {
            RefuseNext--;
            return false;
        }
        if (RefuseAll || (isOptional && RefuseOptional)) {
            return false;
        }
        Quota += memorySize;
        return true;
    }

    void FreeQuota(ui64 memorySize) override {
        Freed += memorySize;
        Quota -= memorySize;
    }

    ui64 GetCurrentQuota() const override {
        return Quota;
    }

    ui64 GetMaxMemorySize() const override {
        return MaxMemorySize;
    }

    i64 GetMemoryAvailability() const override {
        return Availability;
    }

    TString MemoryConsumptionDetails() const override {
        return TString();
    }

    ui64 Quota = 0;
    ui64 Freed = 0;
    bool RefuseAll = false;
    bool RefuseOptional = false;
    ui32 RefuseNext = 0; // refuse this many requests, whatever they are, then behave as scripted
    ui64 MaxMemorySize = 1_GB;
    i64 Availability = 1_GB;
    std::vector<TRequest> Requests;
};

TComputeMemoryLimits MakeLimits(IMemoryQuotaManager::TPtr manager, ui64 hardLimit = 0, bool enableOperatorQuota = true) {
    TComputeMemoryLimits limits;
    limits.MkqlLightProgramMemoryLimit = 40_MB;
    limits.MkqlHeavyProgramMemoryLimit = 60_MB;
    limits.MkqlProgramHardMemoryLimit = hardLimit;
    limits.MinMemAllocSize = 1_MB;
    limits.MinMemFreeSize = 32_MB;
    limits.MemoryQuotaManager = std::move(manager);
    limits.EnableOperatorMemoryQuota = enableOperatorQuota;
    return limits;
}

// the compute actor allocator: sized allocators on, so blocks larger than a page are malloc-backed
struct TQuotaEnv {
    TQuotaEnv(ui64 hardLimit = 0, bool enableOperatorQuota = true)
        : Alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(), /* supportsSizedAllocators = */ true) // acquired by this thread
        , Manager(std::make_shared<TStubQuotaManager>())
        , Quota(Counter, 40_MB, MakeLimits(Manager, hardLimit, enableOperatorQuota), TTxId{ui64(1)}, 1, /* profileStats = */ true, /* actorSystem = */ nullptr)
    {
        Alloc.SetLimit(Quota.GetMkqlMemoryLimit());
    }

    void Bind() {
        Quota.TrySetIncreaseMemoryLimitCallback(&Alloc);
    }

    TScopedAlloc Alloc;
    std::shared_ptr<TStubQuotaManager> Manager;
    ::NMonitoring::TDynamicCounters::TCounterPtr Counter;
    TDqMemoryQuota Quota;
};

} // namespace

Y_UNIT_TEST_SUITE(TDqMemoryQuotaTest) {

    Y_UNIT_TEST(InitialLimit) {
        TQuotaEnv env;
        UNIT_ASSERT_VALUES_EQUAL(env.Quota.GetMkqlMemoryLimit(), 40_MB);
        UNIT_ASSERT_VALUES_EQUAL(env.Manager->Quota, 40_MB);
        UNIT_ASSERT_VALUES_EQUAL(env.Manager->Requests.size(), 1);
        UNIT_ASSERT(!env.Manager->Requests[0].Optional);
        UNIT_ASSERT_VALUES_EQUAL(env.Quota.GetMemoryAvailability(), 1_GB); // forwarded from the manager
    }

    // The initial allocation is mandatory; when the manager refuses it, the quota falls back to the manager's
    // GetMaxMemorySize() (the guaranteed part of a fresh TGuaranteeQuotaManager), capped by the initial limit,
    // and starts with 0 when that is refused too. The MkqlMemoryQuota counter follows the granted limit.
    Y_UNIT_TEST(InitialLimitFallsBackToMaxMemorySize) {
        struct TObserved {
            ui64 Limit;
            ui64 QuotaAfterConstruction;
            i64 CounterAfterConstruction;
            i64 CounterAfterRelease;
            std::shared_ptr<TStubQuotaManager> Manager;
        };
        auto construct = [](ui64 maxMemorySize, bool refuseAll) {
            auto manager = std::make_shared<TStubQuotaManager>();
            manager->MaxMemorySize = maxMemorySize;
            manager->RefuseNext = 1; // the initial request is refused once
            manager->RefuseAll = refuseAll;
            auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
            auto counter = counters->GetCounter("MkqlMemoryQuota");
            TDqMemoryQuota quota(counter, 40_MB, MakeLimits(manager), TTxId{ui64(1)}, 1, /* profileStats = */ false, /* actorSystem = */ nullptr);
            TObserved observed{quota.GetMkqlMemoryLimit(), manager->Quota, counter->Val(), 0, manager};
            quota.TryReleaseQuota(); // gives the granted limit back to the manager and takes it off the counter
            observed.CounterAfterRelease = counter->Val();
            return observed;
        };
        {
            const auto o = construct(10_MB, /* refuseAll = */ false); // the fallback is below the initial limit
            UNIT_ASSERT_VALUES_EQUAL(o.Limit, 10_MB);
            UNIT_ASSERT_VALUES_EQUAL(o.Manager->Requests.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(o.Manager->Requests[0].Size, 40_MB);
            UNIT_ASSERT(!o.Manager->Requests[0].Optional);
            UNIT_ASSERT_VALUES_EQUAL(o.Manager->Requests[1].Size, 10_MB);
            UNIT_ASSERT(!o.Manager->Requests[1].Optional);
            UNIT_ASSERT_VALUES_EQUAL(o.QuotaAfterConstruction, 10_MB);
            UNIT_ASSERT_VALUES_EQUAL(o.CounterAfterConstruction, 10_MB);
            UNIT_ASSERT_VALUES_EQUAL(o.CounterAfterRelease, 0);
            UNIT_ASSERT_VALUES_EQUAL(o.Manager->Freed, 10_MB);
            UNIT_ASSERT_VALUES_EQUAL(o.Manager->Quota, 0);
        }
        {
            const auto o = construct(1_GB, /* refuseAll = */ false); // capped by the initial limit
            UNIT_ASSERT_VALUES_EQUAL(o.Limit, 40_MB);
            UNIT_ASSERT_VALUES_EQUAL(o.Manager->Requests.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(o.Manager->Requests[1].Size, 40_MB);
            UNIT_ASSERT_VALUES_EQUAL(o.QuotaAfterConstruction, 40_MB);
            UNIT_ASSERT_VALUES_EQUAL(o.CounterAfterConstruction, 40_MB);
            UNIT_ASSERT_VALUES_EQUAL(o.CounterAfterRelease, 0);
            UNIT_ASSERT_VALUES_EQUAL(o.Manager->Freed, 40_MB);
        }
        {
            const auto o = construct(10_MB, /* refuseAll = */ true); // the fallback is refused too: start with 0
            UNIT_ASSERT_VALUES_EQUAL(o.Limit, 0);
            UNIT_ASSERT_VALUES_EQUAL(o.Manager->Requests.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(o.Manager->Requests[1].Size, 10_MB);
            UNIT_ASSERT_VALUES_EQUAL(o.QuotaAfterConstruction, 0);
            UNIT_ASSERT_VALUES_EQUAL(o.CounterAfterConstruction, 0);
            UNIT_ASSERT_VALUES_EQUAL(o.CounterAfterRelease, 0);
            UNIT_ASSERT_VALUES_EQUAL(o.Manager->Freed, 0);
        }
    }

    Y_UNIT_TEST(OptionalGrantedAndRefused) {
        TQuotaEnv env;
        env.Bind();

        UNIT_ASSERT(env.Quota.RequestExtraMemory(10_MB, /* isOptional = */ true, &env.Alloc));
        UNIT_ASSERT_VALUES_EQUAL(env.Quota.GetMkqlMemoryLimit(), 50_MB);
        UNIT_ASSERT_VALUES_EQUAL(env.Alloc.GetLimit(), 50_MB);
        UNIT_ASSERT(env.Manager->Requests.back().Optional);

        env.Manager->RefuseOptional = true;
        UNIT_ASSERT(!env.Quota.RequestExtraMemory(10_MB, /* isOptional = */ true, &env.Alloc)); // no throw
        UNIT_ASSERT_VALUES_EQUAL(env.Quota.GetMkqlMemoryLimit(), 50_MB);
        UNIT_ASSERT_VALUES_EQUAL(env.Alloc.GetLimit(), 50_MB);

        // a mandatory refusal does not throw here either: the allocator throws when the limit is not raised
        env.Manager->RefuseAll = true;
        UNIT_ASSERT(!env.Quota.RequestExtraMemory(10_MB, /* isOptional = */ false, &env.Alloc));
        UNIT_ASSERT_VALUES_EQUAL(env.Quota.GetMkqlMemoryLimit(), 50_MB);

        // every request counts, granted or not: two optional and one mandatory, 10 MB each
        const auto* stats = env.Quota.GetProfileStats();
        UNIT_ASSERT(stats);
        UNIT_ASSERT_VALUES_EQUAL(stats->MkqlExtraMemoryRequests, 3);
        UNIT_ASSERT_VALUES_EQUAL(stats->MkqlExtraMemoryBytes, 30_MB);
    }

    Y_UNIT_TEST(HardLimit) {
        TQuotaEnv env(/* hardLimit = */ 45_MB);
        env.Bind();
        UNIT_ASSERT(!env.Quota.RequestExtraMemory(10_MB, /* isOptional = */ true, &env.Alloc)); // refused, no throw
        UNIT_ASSERT_VALUES_EQUAL(env.Quota.GetMkqlMemoryLimit(), 40_MB);
        UNIT_ASSERT_EXCEPTION(env.Quota.RequestExtraMemory(10_MB, /* isOptional = */ false, &env.Alloc), THardMemoryLimitException);
        UNIT_ASSERT(env.Quota.RequestExtraMemory(4_MB, /* isOptional = */ true, &env.Alloc)); // still fits
        UNIT_ASSERT_VALUES_EQUAL(env.Quota.GetMkqlMemoryLimit(), 44_MB);
    }

    Y_UNIT_TEST(MaximumLimitFlagFollowsAvailability) {
        TQuotaEnv env;
        env.Bind();
        env.Manager->Availability = -1;
        UNIT_ASSERT(env.Quota.RequestExtraMemory(1_MB, /* isOptional = */ true, &env.Alloc));
        UNIT_ASSERT(env.Alloc.Ref().GetMaximumLimitValueReached()); // the old IsReasonableToUseSpilling signal
        env.Manager->Availability = 1;
        UNIT_ASSERT(env.Quota.RequestExtraMemory(1_MB, /* isOptional = */ true, &env.Alloc));
        UNIT_ASSERT(!env.Alloc.Ref().GetMaximumLimitValueReached());
        env.Manager->Availability = 0; // zero is not pressure, just "do not ask for optional quota"
        UNIT_ASSERT(env.Quota.RequestExtraMemory(1_MB, /* isOptional = */ false, &env.Alloc));
        UNIT_ASSERT(!env.Alloc.Ref().GetMaximumLimitValueReached());
    }

    Y_UNIT_TEST(OperatorQuotaBinding) {
        TQuotaEnv env;
        UNIT_ASSERT(env.Quota.GetOperatorQuota() == nullptr); // no allocator attached yet
        env.Bind();
        IDqOperatorMemoryQuota* operatorQuota = env.Quota.GetOperatorQuota();
        UNIT_ASSERT(operatorQuota == &env.Quota);

        // the operator-facing methods work on the attached allocator (TScopedAlloc binds itself to the thread)
        UNIT_ASSERT(operatorQuota->RequestExtraMemory(10_MB, /* isOptional = */ true));
        UNIT_ASSERT_VALUES_EQUAL(env.Alloc.GetLimit(), 50_MB);
        UNIT_ASSERT_VALUES_EQUAL(operatorQuota->GetMemoryAvailability(), 1_GB);
        operatorQuota->TryShrinkMemory(); // the granted memory is unused: it goes back, down to the initial limit
        UNIT_ASSERT_VALUES_EQUAL(env.Quota.GetMkqlMemoryLimit(), 40_MB);
        UNIT_ASSERT_VALUES_EQUAL(env.Manager->Freed, 10_MB);
        UNIT_ASSERT(operatorQuota->RequestExtraMemory(10_MB, /* isOptional = */ true));
        UNIT_ASSERT_VALUES_EQUAL(env.Quota.GetMkqlMemoryLimit(), 50_MB);

        // and are no-ops when another allocator is bound to the thread
        {
            TScopedAlloc other(__LOCATION__);
            UNIT_ASSERT(!operatorQuota->RequestExtraMemory(10_MB, /* isOptional = */ true));
            operatorQuota->TryShrinkMemory();
            UNIT_ASSERT_VALUES_EQUAL(env.Quota.GetMkqlMemoryLimit(), 50_MB);
        }
    }

    Y_UNIT_TEST(OperatorQuotaDisabled) {
        TQuotaEnv env(/* hardLimit = */ 0, /* enableOperatorQuota = */ false);
        env.Bind();
        UNIT_ASSERT(env.Quota.GetOperatorQuota() == nullptr);
    }

    Y_UNIT_TEST(ShrinkReturnsMallocBackedBlocks) {
        TQuotaEnv env;
        env.Bind();

        // a block larger than a page is malloc-backed: its release produces no free pages, only a lower
        // TotalAllocated, so the shrink gate must look at the unused part of the limit
        const size_t blockSize = 64_MB;
        void* block = MKQLAllocWithSize(blockSize, EMemorySubPool::Default);
        UNIT_ASSERT(block);
        // grown through the mandatory callback, which asks for the missing part of the block only
        const ui64 grownLimit = env.Quota.GetMkqlMemoryLimit();
        UNIT_ASSERT_GE(grownLimit, blockSize);
        UNIT_ASSERT_VALUES_EQUAL(env.Manager->Freed, 0);

        MKQLFreeWithSize(block, blockSize, EMemorySubPool::Default);
        UNIT_ASSERT_VALUES_EQUAL(env.Alloc.GetAllocated() - env.Alloc.GetUsed(), 0); // no free pages appeared

        env.Quota.TryShrinkMemory(&env.Alloc);
        UNIT_ASSERT_VALUES_EQUAL(env.Quota.GetMkqlMemoryLimit(), 40_MB); // back to the initial limit
        UNIT_ASSERT_VALUES_EQUAL(env.Alloc.GetLimit(), 40_MB);
        UNIT_ASSERT_VALUES_EQUAL(env.Manager->Freed, grownLimit - 40_MB);
    }

    // Page-backed blocks (at most a page each): every one takes its own 64 KB page and freeing it leaves the page
    // cached in the allocator, so GetAllocated() - GetUsed() is the cache size
    std::vector<void*> AllocatePageBlocks(size_t count) {
        std::vector<void*> blocks;
        blocks.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            blocks.push_back(MKQLAllocWithSize(60_KB, EMemorySubPool::Default));
        }
        return blocks;
    }

    void FreePageBlocks(const std::vector<void*>& blocks) {
        for (void* block : blocks) {
            MKQLFreeWithSize(block, 60_KB, EMemorySubPool::Default);
        }
    }

    // The limit is still the initial one and the page cache is small: nothing could be given back (the shrink
    // never goes below the initial limit), so the cached pages must stay with the task instead of being pushed
    // to the global pool on every execution
    Y_UNIT_TEST(ShrinkKeepsSmallPageCacheUnderInitialLimit) {
        TQuotaEnv env;
        env.Bind();

        FreePageBlocks(AllocatePageBlocks(16));
        const ui64 cached = env.Alloc.GetAllocated() - env.Alloc.GetUsed();
        UNIT_ASSERT_GE(cached, 15 * 64_KB);
        UNIT_ASSERT_LT(cached, 32_MB);
        UNIT_ASSERT_VALUES_EQUAL(env.Quota.GetMkqlMemoryLimit(), 40_MB); // never grew
        const ui64 allocatedBefore = env.Alloc.GetAllocated();

        env.Quota.TryShrinkMemory(&env.Alloc);
        UNIT_ASSERT_VALUES_EQUAL(env.Alloc.GetAllocated(), allocatedBefore); // the cache is kept
        UNIT_ASSERT_VALUES_EQUAL(env.Alloc.GetAllocated() - env.Alloc.GetUsed(), cached);
        UNIT_ASSERT_VALUES_EQUAL(env.Quota.GetMkqlMemoryLimit(), 40_MB);
        UNIT_ASSERT_VALUES_EQUAL(env.Manager->Freed, 0);
    }

    // A page cache above MinMemFreeSize is released even under the initial limit (the classic trigger),
    // the limit itself stays at the initial value
    Y_UNIT_TEST(ShrinkReleasesLargePageCacheUnderInitialLimit) {
        TQuotaEnv env;
        env.Bind();

        FreePageBlocks(AllocatePageBlocks(560)); // 35 MB of pages, within the 40 MB initial limit
        UNIT_ASSERT_VALUES_EQUAL(env.Quota.GetMkqlMemoryLimit(), 40_MB);
        UNIT_ASSERT_GT(env.Alloc.GetAllocated() - env.Alloc.GetUsed(), 32_MB);

        env.Quota.TryShrinkMemory(&env.Alloc);
        UNIT_ASSERT_VALUES_EQUAL(env.Alloc.GetAllocated() - env.Alloc.GetUsed(), 0); // the cache went to the global pool
        UNIT_ASSERT_LT(env.Alloc.GetAllocated(), 1_MB);
        UNIT_ASSERT_VALUES_EQUAL(env.Quota.GetMkqlMemoryLimit(), 40_MB); // never below the initial limit
        UNIT_ASSERT_VALUES_EQUAL(env.Manager->Freed, 0);
    }

    // The quota detaches from the allocator when it dies: the allocator must not call a dead quota to raise its
    // limit, exceeding the limit throws instead
    Y_UNIT_TEST(DestructorDetachesFromAllocator) {
        TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(), /* supportsSizedAllocators = */ true);
        auto manager = std::make_shared<TStubQuotaManager>();
        ::NMonitoring::TDynamicCounters::TCounterPtr counter;
        {
            TDqMemoryQuota quota(counter, 40_MB, MakeLimits(manager), TTxId{ui64(1)}, 1, /* profileStats = */ false, /* actorSystem = */ nullptr);
            alloc.SetLimit(quota.GetMkqlMemoryLimit());
            quota.TrySetIncreaseMemoryLimitCallback(&alloc);
            // attached: a block beyond the limit grows it through the quota
            void* block = MKQLAllocWithSize(64_MB, EMemorySubPool::Default);
            UNIT_ASSERT(block);
            MKQLFreeWithSize(block, 64_MB, EMemorySubPool::Default);
            UNIT_ASSERT_GE(quota.GetMkqlMemoryLimit(), 64_MB);
            quota.TryShrinkMemory(&alloc);
            UNIT_ASSERT_VALUES_EQUAL(quota.GetMkqlMemoryLimit(), 40_MB);
        }
        // detached: the same block hits the allocator limit and nothing raises it
        UNIT_ASSERT_VALUES_EQUAL(alloc.GetLimit(), 40_MB);
        UNIT_ASSERT_EXCEPTION(MKQLAllocWithSize(64_MB, EMemorySubPool::Default), NKikimr::TMemoryLimitExceededException);
    }

    Y_UNIT_TEST(GuaranteeManagerNegativeParentDominates) {
        struct TParentedManager : public TGuaranteeQuotaManager {
            TParentedManager()
                : TGuaranteeQuotaManager(30_MB, 30_MB)
            {
            }

            bool AllocateExtraQuota(ui64 size) override {
                ExtraRequests++;
                return ExtraGranted && (Extra -= size, true);
            }

            i64 GetExtraMemoryAvailability() const override {
                return Extra;
            }

            i64 Extra = -1;
            bool ExtraGranted = true;
            size_t ExtraRequests = 0;
        };

        TParentedManager manager;
        UNIT_ASSERT(manager.AllocateQuota(1_MB, /* isOptional = */ false)); // fits in the guarantee
        UNIT_ASSERT_VALUES_EQUAL(manager.GetMemoryAvailability(), -1); // the local leftover does not mask node pressure

        manager.Extra = 5_MB;
        UNIT_ASSERT_VALUES_EQUAL(manager.GetMemoryAvailability(), i64(29_MB + 5_MB));

        // an optional request beyond the limit is refused in advance when the parent cannot cover the delta
        manager.Extra = 0;
        UNIT_ASSERT(!manager.AllocateQuota(40_MB, /* isOptional = */ true));
        UNIT_ASSERT_VALUES_EQUAL(manager.ExtraRequests, 0);
        // a mandatory one still asks the parent
        manager.Extra = 100_MB;
        UNIT_ASSERT(manager.AllocateQuota(40_MB, /* isOptional = */ false));
        UNIT_ASSERT_VALUES_EQUAL(manager.ExtraRequests, 1);
        // unlimited parents saturate instead of overflowing
        manager.Extra = std::numeric_limits<i64>::max();
        UNIT_ASSERT_VALUES_EQUAL(manager.GetMemoryAvailability(), std::numeric_limits<i64>::max());
    }
}

} // namespace NYql::NDq

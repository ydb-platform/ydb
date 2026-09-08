#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_memory_quota.h>
#include <ydb/library/yql/dq/comp_nodes/operator_memory_quota/dq_operator_memory_quota.h>

#include <yql/essentials/minikql/mkql_alloc.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

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
        return 1_GB;
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

        const auto* stats = env.Quota.GetProfileStats();
        UNIT_ASSERT(stats);
        UNIT_ASSERT_VALUES_EQUAL(stats->MkqlOptionalMemoryRequests, 2);
        UNIT_ASSERT_VALUES_EQUAL(stats->MkqlOptionalMemoryRefusals, 1);
        UNIT_ASSERT_VALUES_EQUAL(stats->MkqlExtraMemoryRequests, 1);
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

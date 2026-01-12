#include <ydb/library/actors/interconnect/rdma/mem_pool.h>
#include <ydb/library/actors/interconnect/rdma/ut/utils/utils.h>

#include <library/cpp/testing/gtest/gtest.h>
#include <ydb/library/testlib/unittest_gtest_macro_subst.h>

#include <util/random/fast.h>
#include <util/random/random.h>

#include <thread>

namespace NMonitoring {
    struct TDynamicCounters;
}

static void GTestSkip() {
    GTEST_SKIP() << "Skipping all rdma tests for suit, set \""
                 << NRdmaTest::RdmaTestEnvSwitchName << "\" env if it is RDMA compatible";
}

class TAllocatorSuite32 : public ::testing::Test {
protected:
    void SetUp() override {
        using namespace NRdmaTest;
        if (IsRdmaTestDisabled()) {
            GTestSkip();
        }
    }
};

TEST_F(TAllocatorSuite32, SlotPoolLimit) {
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 32
    };
    static auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    const size_t sz = 4 << 20;
    std::vector<NInterconnect::NRdma::TMemRegionPtr> regions;
    regions.reserve(8);
    size_t i = 0;
    for (;;i++) {
        auto reg = pool->Alloc(sz, 0);
        if (!reg) {
            UNIT_ASSERT(i == 8); // 32 / 4
            break;
        }
        ASSERT_TRUE(reg->GetAddr()) << "invalid address";
        ASSERT_TRUE(reg->GetSize() == sz) << "invalid size of allocated chunk";
        regions.push_back(reg);
    }

    regions.erase(regions.begin()); // free one region

    {
        auto reg = pool->Alloc(sz, 0); // allocate one
        ASSERT_TRUE(reg->GetAddr()) << "invalid address";
        ASSERT_TRUE(reg->GetSize() == sz) << "invalid size of allocated chunk";
        UNIT_ASSERT(!pool->Alloc(sz, 0)); // pool is full
    }

    regions.clear();
}

TEST_F(TAllocatorSuite32, SlotPoolHugeAlloc) {
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 32
    };

    static auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    std::vector<NInterconnect::NRdma::TMemRegionPtr> regions;
    const size_t sz = 8 << 20;
    for (size_t i = 0; i < 4; i++) {
        auto reg = pool->Alloc(sz, 0);
        ASSERT_TRUE(reg->GetAddr()) << "invalid address";
        regions.push_back(reg);
    }
    regions.clear();
}

TEST_F(TAllocatorSuite32, SlotPoolHugeAllocAfterSmall) {
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 32
    };

    const size_t smallSz = 1 << 20;
    const size_t hugeSz = 4 << 20;

   static auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    std::vector<NInterconnect::NRdma::TMemRegionPtr> regions;
    regions.reserve(32);
    for (size_t i = 0; i < 32;i++) {
        auto reg = pool->Alloc(smallSz, 0);
        ASSERT_TRUE(reg->GetAddr()) << "invalid address";
        regions.push_back(reg);
    }
    regions.clear();

    auto reg = pool->Alloc(hugeSz, 0);
    ASSERT_TRUE(reg) << "allocation failed";
    ASSERT_TRUE(reg->GetAddr()) << "invalid address";
}

TEST_F(TAllocatorSuite32, SlotPoolHugeAllocOtherThreadAfterSmall) {
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 32
    };

    const size_t smallSz = 1 << 20;
    const size_t hugeSz = 4 << 20;

    static auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    std::vector<NInterconnect::NRdma::TMemRegionPtr> regions;
    regions.reserve(32);
    for (size_t i = 0; i < 32;i++) {
        auto reg = pool->Alloc(smallSz, 0);
        ASSERT_TRUE(reg->GetAddr()) << "invalid address";
        regions.push_back(reg);
    }
    regions.clear();

    auto fn = [&]() {
        auto reg = pool->Alloc(hugeSz, 0);
        ASSERT_TRUE(reg) << "allocation failed";
        ASSERT_TRUE(reg->GetAddr()) << "invalid address";
    };

    std::thread thread(fn);
    thread.join();

    // And try to alloc small again
    for (size_t i = 0; i < 32;i++) {
        auto reg = pool->Alloc(smallSz, 0);
        ASSERT_TRUE(reg->GetAddr()) << "invalid address";
        regions.push_back(reg);
    }
    regions.clear();
}

TEST_F(TAllocatorSuite32, AllocationRandSizeWithReclaimOneThread) {
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 32
    };

    static auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    constexpr ui32 NUM_ALLOC = 40000;
    constexpr ui32 MAX_REG_SZ = 2u << 20;

    TReallyFastRng32 rng(RandomNumber<ui64>());

    auto now = TInstant::Now();
    for (ui32 j = 0; j < NUM_ALLOC; ++j) {
        auto memRegion = pool->Alloc(((rng() % MAX_REG_SZ) | 1u), 0);
        ASSERT_TRUE(memRegion) << "allocation failed";
        ASSERT_TRUE(memRegion->GetAddr()) << "invalid address";
    }

    float s = (TInstant::Now() - now).MicroSeconds();

    s = s / float(NUM_ALLOC);
    Cerr << "Average time per allocation: " << s << " us" << Endl;
}

TEST_F(TAllocatorSuite32, AllocationWithReclaimTwoThreads) {
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 32
    };

    auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    auto allocFn = [&](size_t sz, size_t num, float& s, bool holdAllocations) {
        size_t j = 0;

        auto now = TInstant::Now();
        std::vector<NInterconnect::NRdma::TMemRegionPtr> alls;
        alls.reserve(num);
        while (j < num) {
            auto memRegion = pool->Alloc(sz, 0);
            if (!memRegion) {
                continue;
            }
            ASSERT_TRUE(memRegion) << "allocation failed";
            ASSERT_TRUE(memRegion->GetAddr()) << "invalid address";
            if (holdAllocations) {
                alls.push_back(memRegion);
            }
            j++;
        }
        alls.clear();

        s = (TInstant::Now() - now).MicroSeconds();
    };

    Cerr << "===" << Endl;

    for (size_t i = 0; i < 10; i++) {
        float s0 = 0.0;
        float s1 = 0.0;
        size_t numAlloc0 = 1000;
        size_t numAlloc1 = 1000;
        std::thread thread0(allocFn, 512, numAlloc0, std::ref(s0), false);
        std::thread thread1(allocFn, 512, numAlloc1, std::ref(s1), false);

        thread0.join();
        thread1.join();

        s0 = s0 / float(numAlloc0);
        s1 = s1 / float(numAlloc1);
        Cerr << "Average time per allocation t0: " << s0 << " us, t1: " << s1 << " us" << Endl;
    }

    Cerr << "===" << Endl;

    for (size_t i = 0; i < 10; i++) {
        float s0 = 0.0;
        float s1 = 0.0;
        size_t numAlloc0 = 100000;
        size_t numAlloc1 = 100000;
        std::thread thread0(allocFn, 512, numAlloc0, std::ref(s0), false);
        std::thread thread1(allocFn, 32768, numAlloc1, std::ref(s1), false);

        thread0.join();
        thread1.join();

        s0 = s0 / float(numAlloc0);
        s1 = s1 / float(numAlloc1);
        Cerr << "Average time per allocation t0: " << s0 << " us, t1: " << s1 << " us" << Endl;
    }
    Cerr << "===" << Endl;

    for (size_t i = 0; i < 10; i++) {
        float s0 = 0.0;
        float s1 = 0.0;
        size_t numAlloc0 = 10000;
        size_t numAlloc1 = 10000;
        std::thread thread0(allocFn, 512, numAlloc0, std::ref(s0), true);
        std::thread thread1(allocFn, 512, numAlloc1, std::ref(s1), true);

        thread0.join();
        thread1.join();

        s0 = s0 / float(numAlloc0);
        s1 = s1 / float(numAlloc1);
        Cerr << "Average time per allocation t0: " << s0 << " us, t1: " << s1 << " us" << Endl;
    }

    Cerr << "===" << Endl;

    for (size_t i = 0; i < 10; i++) {
        float s0 = 0.0;
        float s1 = 0.0;
        size_t numAlloc0 = 10000;
        size_t numAlloc1 = 10000;
        std::thread thread0(allocFn, 512, numAlloc0, std::ref(s0), true);
        std::thread thread1(allocFn, 1024, numAlloc1, std::ref(s1), true);

        thread0.join();
        thread1.join();

        s0 = s0 / float(numAlloc0);
        s1 = s1 / float(numAlloc1);
        Cerr << "Average time per allocation t0: " << s0 << " us, t1: " << s1 << " us" << Endl;
    }

    Cerr << "===" << Endl;

    for (size_t i = 0; i < 10; i++) {
        float s0 = 0.0;
        float s1 = 0.0;
        size_t numAlloc0 = 10000;
        size_t numAlloc1 = 10000;
        std::thread thread0(allocFn, 512, numAlloc0, std::ref(s0), true);
        std::thread thread1(allocFn, 512, numAlloc1, std::ref(s1), false);

        thread0.join();
        thread1.join();

        s0 = s0 / float(numAlloc0);
        s1 = s1 / float(numAlloc1);
        Cerr << "Average time per allocation t0: " << s0 << " us, t1: " << s1 << " us" << Endl;
    }

    Cerr << "===" << Endl;

    for (size_t i = 0; i < 10; i++) {
        float s0 = 0.0;
        float s1 = 0.0;
        size_t numAlloc0 = 10000;
        size_t numAlloc1 = 10000;
        std::thread thread0(allocFn, 512, numAlloc0, std::ref(s0), true);
        std::thread thread1(allocFn, 1024, numAlloc1, std::ref(s1), false);

        thread0.join();
        thread1.join();

        s0 = s0 / float(numAlloc0);
        s1 = s1 / float(numAlloc1);
        Cerr << "Average time per allocation t0: " << s0 << " us, t1: " << s1 << " us" << Endl;
    }

}

TEST_F(TAllocatorSuite32, AllocationWithReclaimThreeThreads) {
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 32
    };

    auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    auto allocFn = [&](size_t sz, size_t num, float& s, bool holdAllocations) {
        size_t j = 0;

        auto now = TInstant::Now();
        std::vector<NInterconnect::NRdma::TMemRegionPtr> alls;
        alls.reserve(num);
        while (j < num) {
            auto memRegion = pool->Alloc(sz, 0);
            if (!memRegion) {
                continue;
            }
            ASSERT_TRUE(memRegion) << "allocation failed";
            ASSERT_TRUE(memRegion->GetAddr()) << "invalid address";
            if (holdAllocations) {
                alls.push_back(memRegion);
            }
            j++;
        }
        alls.clear();

        s = (TInstant::Now() - now).MicroSeconds();
    };

    Cerr << "===" << Endl;

    for (size_t i = 0; i < 10; i++) {
        float s0 = 0.0;
        float s1 = 0.0;
        float s2 = 0.0;
        size_t numAlloc0 = 100;
        size_t numAlloc1 = 100;
        size_t numAlloc2 = 100;
        std::thread thread0(allocFn, 512, numAlloc0, std::ref(s0), false);
        std::thread thread1(allocFn, 512, numAlloc1, std::ref(s1), false);
        std::thread thread2(allocFn, 512, numAlloc2, std::ref(s2), false);

        thread0.join();
        thread1.join();
        thread2.join();

        s0 = s0 / float(numAlloc0);
        s1 = s1 / float(numAlloc1);
        s2 = s2 / float(numAlloc2);
        Cerr << "Average time per allocation t0: " << s0 << " us, t1: " << s1 << " us, t2: " << s2 << " us" << Endl;
    }

    Cerr << "===" << Endl;

    for (size_t i = 0; i < 10; i++) {
        float s0 = 0.0;
        float s1 = 0.0;
        float s2 = 0.0;
        size_t numAlloc0 = 10000;
        size_t numAlloc1 = 10000;
        size_t numAlloc2 = 10000;
        std::thread thread0(allocFn, 512, numAlloc0, std::ref(s0), false);
        std::thread thread1(allocFn, 4096, numAlloc1, std::ref(s1), false);
        std::thread thread2(allocFn, 32768, numAlloc2, std::ref(s2), false);

        thread0.join();
        thread1.join();
        thread2.join();

        s0 = s0 / float(numAlloc0);
        s1 = s1 / float(numAlloc1);
        s2 = s2 / float(numAlloc2);
        Cerr << "Average time per allocation t0: " << s0 << " us, t1: " << s1 << " us, t2: " << s2 << " us" << Endl;
    }

    Cerr << "===" << Endl;

    for (size_t i = 0; i < 10; i++) {
        float s0 = 0.0;
        float s1 = 0.0;
        float s2 = 0.0;
        size_t numAlloc0 = 10000;
        size_t numAlloc1 = 10000;
        size_t numAlloc2 = 10000;
        std::thread thread0(allocFn, 512, numAlloc0, std::ref(s0), true);
        std::thread thread1(allocFn, 512, numAlloc1, std::ref(s1), true);
        std::thread thread2(allocFn, 512, numAlloc2, std::ref(s2), true);

        thread0.join();
        thread1.join();
        thread2.join();

        s0 = s0 / float(numAlloc0);
        s1 = s1 / float(numAlloc1);
        s2 = s2 / float(numAlloc2);
        Cerr << "Average time per allocation t0: " << s0 << " us, t1: " << s1 << " us, t2: " << s2 << " us" << Endl;
    }

    Cerr << "===" << Endl;

    for (size_t i = 0; i < 10; i++) {
        float s0 = 0.0;
        float s1 = 0.0;
        float s2 = 0.0;
        size_t numAlloc0 = 10000;
        size_t numAlloc1 = 10000;
        size_t numAlloc2 = 8192;
        std::thread thread0(allocFn, 512, numAlloc0, std::ref(s0), true);
        std::thread thread1(allocFn, 1024, numAlloc1, std::ref(s1), true);
        std::thread thread2(allocFn, 4096, numAlloc2, std::ref(s2), true);

        thread0.join();
        thread1.join();
        thread2.join();

        s0 = s0 / float(numAlloc0);
        s1 = s1 / float(numAlloc1);
        s2 = s2 / float(numAlloc2);
        Cerr << "Average time per allocation t0: " << s0 << " us, t1: " << s1 << " us, t2: " << s2 << " us" << Endl;
    }
}

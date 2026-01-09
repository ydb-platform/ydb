#include <ydb/library/actors/interconnect/rdma/mem_pool.h>
#include <ydb/library/actors/interconnect/rdma/ut/utils/utils.h>

#include <library/cpp/testing/gtest/gtest.h>
#include <library/cpp/threading/future/future.h>

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

class TAllocatorSuite128 : public ::testing::Test {
protected:
    void SetUp() override {
        using namespace NRdmaTest;
        if (IsRdmaTestDisabled()) {
            GTestSkip();
        }
    }
};

TEST_F(TAllocatorSuite128, SlotPoolLimit) {
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 128
    };
    static auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    const size_t sz = 4 << 20;
    std::vector<NInterconnect::NRdma::TMemRegionPtr> regions;
    regions.reserve(32);
    size_t i = 0;
    for (;;i++) {
        auto reg = pool->Alloc(sz, 0);
        if (!reg) {
            UNIT_ASSERT(i == 32);
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

TEST_F(TAllocatorSuite128, SlotPoolHugeAlloc) {
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 128
    };

    static auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    std::vector<NInterconnect::NRdma::TMemRegionPtr> regions;
    const size_t sz = 32 << 20;
    for (size_t i = 0; i < 4; i++) {
        auto reg = pool->Alloc(sz, 0);
        ASSERT_TRUE(reg->GetAddr()) << "invalid address";
        regions.push_back(reg);
    }
    regions.clear();
}

TEST_F(TAllocatorSuite128, SlotPoolTinyAlloc) {
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 128
    };

    static auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    std::vector<NInterconnect::NRdma::TMemRegionPtr> regions;
    const size_t sz = 512;
    regions.reserve(262144);
    for (size_t i = 0;; i++) {
        auto reg = pool->Alloc(sz, 0);
        if (!reg) {
            UNIT_ASSERT(i == 262144);
            break;
        }
        ASSERT_TRUE(reg->GetAddr()) << "invalid address";
        regions.push_back(reg);
    }
    regions.clear();

    {
        auto reg = pool->Alloc(sz, 0); // allocate one
        ASSERT_TRUE(reg->GetAddr()) << "invalid address";
        ASSERT_TRUE(reg->GetSize() == sz) << "invalid size of allocated chunk";
    }
}

TEST_F(TAllocatorSuite128, SlotPoolHugeAfterTinyAlloc) {
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 128
    };

    static auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    std::vector<NInterconnect::NRdma::TMemRegionPtr> regions;
    const size_t sz = 512;

    regions.reserve(262144);

    for (size_t i = 0;; i++) {
        auto reg = pool->Alloc(sz, 0);
        if (!reg) {
            UNIT_ASSERT(i == 262144);
            break;
        }
        ASSERT_TRUE(reg->GetAddr()) << "invalid address";
        regions.push_back(reg);
    }

    regions.clear();

    {
        for (size_t i = 0; i < 4; i++) {
            auto reg = pool->Alloc(sz, 0);
            ASSERT_TRUE(reg->GetAddr()) << "invalid address";
            regions.push_back(reg);
        }
        regions.clear();
    }
}


TEST_F(TAllocatorSuite128, AllocationFourThreads) {
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 128
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
        float s3 = 0.0;
        size_t numAlloc0 = 100000;
        size_t numAlloc1 = 100000;
        size_t numAlloc2 = 100000;
        size_t numAlloc3 = 100000;
        std::thread thread0(allocFn, 512, numAlloc0, std::ref(s0), false);
        std::thread thread1(allocFn, 512, numAlloc1, std::ref(s1), false);
        std::thread thread2(allocFn, 512, numAlloc2, std::ref(s2), false);
        std::thread thread3(allocFn, 512, numAlloc3, std::ref(s3), false);

        thread0.join();
        thread1.join();
        thread2.join();
        thread3.join();

        s0 = s0 / float(numAlloc0);
        s1 = s1 / float(numAlloc1);
        s2 = s2 / float(numAlloc2);
        s3 = s3 / float(numAlloc3);
        Cerr << "Average time per allocation t0: " << s0 << " us, t1: " << s1 << " us, t2: " << s2 << " us, t3: " << s3 <<" us" << Endl;
    }

    Cerr << "===" << Endl;

    for (size_t i = 0; i < 10; i++) {
        float s0 = 0.0;
        float s1 = 0.0;
        float s2 = 0.0;
        float s3 = 0.0;
        size_t numAlloc0 = 10000;
        size_t numAlloc1 = 10000;
        size_t numAlloc2 = 10000;
        size_t numAlloc3 = 10000;
        std::thread thread0(allocFn, 512, numAlloc0, std::ref(s0), false);
        std::thread thread1(allocFn, 4096, numAlloc1, std::ref(s1), false);
        std::thread thread2(allocFn, 32768, numAlloc2, std::ref(s2), false);
        std::thread thread3(allocFn, 65536, numAlloc3, std::ref(s3), false);

        thread0.join();
        thread1.join();
        thread2.join();
        thread3.join();

        s0 = s0 / float(numAlloc0);
        s1 = s1 / float(numAlloc1);
        s2 = s2 / float(numAlloc2);
        s3 = s3 / float(numAlloc3);
        Cerr << "Average time per allocation t0: " << s0 << " us, t1: " << s1 << " us, t2: " << s2 << " us, t3: " << s3 <<" us" << Endl;
    }

    Cerr << "===" << Endl;

    for (size_t i = 0; i < 10; i++) {
        float s0 = 0.0;
        float s1 = 0.0;
        float s2 = 0.0;
        float s3 = 0.0;
        size_t numAlloc0 = 10000;
        size_t numAlloc1 = 10000;
        size_t numAlloc2 = 10000;
        size_t numAlloc3 = 10000;
        std::thread thread0(allocFn, 512, numAlloc0, std::ref(s0), true);
        std::thread thread1(allocFn, 512, numAlloc1, std::ref(s1), true);
        std::thread thread2(allocFn, 512, numAlloc2, std::ref(s2), true);
        std::thread thread3(allocFn, 512, numAlloc2, std::ref(s3), true);

        thread0.join();
        thread1.join();
        thread2.join();
        thread3.join();

        s0 = s0 / float(numAlloc0);
        s1 = s1 / float(numAlloc1);
        s2 = s2 / float(numAlloc2);
        s3 = s3 / float(numAlloc3);
        Cerr << "Average time per allocation t0: " << s0 << " us, t1: " << s1 << " us, t2: " << s2 << " us, t3: " << s3 <<" us" << Endl;
    }

    Cerr << "===" << Endl;

    for (size_t i = 0; i < 10; i++) {
        float s0 = 0.0;
        float s1 = 0.0;
        float s2 = 0.0;
        float s3 = 0.0;
        size_t numAlloc0 = 65536;
        size_t numAlloc1 = 32768;
        size_t numAlloc2 = 8192;
        size_t numAlloc3 = 1024;
        std::thread thread0(allocFn, 512, numAlloc0, std::ref(s0), true);
        std::thread thread1(allocFn, 1024, numAlloc1, std::ref(s1), true);
        std::thread thread2(allocFn, 4096, numAlloc2, std::ref(s2), true);
        std::thread thread3(allocFn, 32768, numAlloc3, std::ref(s3), true);

        thread0.join();
        thread1.join();
        thread2.join();
        thread3.join();

        s0 = s0 / float(numAlloc0);
        s1 = s1 / float(numAlloc1);
        s2 = s2 / float(numAlloc2);
        s3 = s3 / float(numAlloc3);
        Cerr << "Average time per allocation t0: " << s0 << " us, t1: " << s1 << " us, t2: " << s2 << " us, t3: " << s3 <<" us" << Endl;
    }
}

TEST_F(TAllocatorSuite128, AllocationWithReclaimSixThreads) {
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 128
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
        float s3 = 0.0;
        float s4 = 0.0;
        float s5 = 0.0;
        size_t numAlloc0 = 100000;
        size_t numAlloc1 = 100000;
        size_t numAlloc2 = 100000;
        size_t numAlloc3 = 100000;
        size_t numAlloc4 = 100000;
        size_t numAlloc5 = 100000;
        std::thread thread0(allocFn, 512, numAlloc0, std::ref(s0), false);
        std::thread thread1(allocFn, 512, numAlloc1, std::ref(s1), false);
        std::thread thread2(allocFn, 512, numAlloc2, std::ref(s2), false);
        std::thread thread3(allocFn, 512, numAlloc3, std::ref(s3), false);
        std::thread thread4(allocFn, 512, numAlloc4, std::ref(s4), false);
        std::thread thread5(allocFn, 512, numAlloc5, std::ref(s5), false);

        thread0.join();
        thread1.join();
        thread2.join();
        thread3.join();
        thread4.join();
        thread5.join();

        s0 = s0 / float(numAlloc0);
        s1 = s1 / float(numAlloc1);
        s2 = s2 / float(numAlloc2);
        s3 = s3 / float(numAlloc3);
        s4 = s4 / float(numAlloc4);
        s5 = s5 / float(numAlloc5);
        Cerr << "Average time per allocation t0: " << s0 << " us, t1: " << s1 << " us, t2: " << s2 << " us, t3: " << s3 <<" us, t4: " << s4 << " us, t5: " << s5 << " us" << Endl;
    }

    Cerr << "===" << Endl;

    for (size_t i = 0; i < 10; i++) {
        float s0 = 0.0;
        float s1 = 0.0;
        float s2 = 0.0;
        float s3 = 0.0;
        float s4 = 0.0;
        float s5 = 0.0;
        size_t numAlloc0 = 100000;
        size_t numAlloc1 = 100000;
        size_t numAlloc2 = 100000;
        size_t numAlloc3 = 100000;
        size_t numAlloc4 = 100000;
        size_t numAlloc5 = 100000;
        std::thread thread0(allocFn, 512, numAlloc0, std::ref(s0), false);
        std::thread thread1(allocFn, 4096, numAlloc1, std::ref(s1), false);
        std::thread thread2(allocFn, 32768, numAlloc2, std::ref(s2), false);
        std::thread thread3(allocFn, 65536, numAlloc3, std::ref(s3), false);
        std::thread thread4(allocFn, 131072, numAlloc4, std::ref(s4), false);
        std::thread thread5(allocFn, 262144, numAlloc5, std::ref(s5), false);

        thread0.join();
        thread1.join();
        thread2.join();
        thread3.join();
        thread4.join();
        thread5.join();

        s0 = s0 / float(numAlloc0);
        s1 = s1 / float(numAlloc1);
        s2 = s2 / float(numAlloc2);
        s3 = s3 / float(numAlloc3);
        s4 = s4 / float(numAlloc4);
        s5 = s5 / float(numAlloc5);
        Cerr << "Average time per allocation t0: " << s0 << " us, t1: " << s1 << " us, t2: " << s2 << " us, t3: " << s3 <<" us, t4: " << s4 << " us, t5: " << s5 << " us" << Endl;
    }

    Cerr << "===" << Endl;

    for (size_t i = 0; i < 10; i++) {
        float s0 = 0.0;
        float s1 = 0.0;
        float s2 = 0.0;
        float s3 = 0.0;
        float s4 = 0.0;
        float s5 = 0.0;
        size_t numAlloc0 = 43690; // total chunks we have 4 * 65536, so per thread: 4 * 65536 / 6
        size_t numAlloc1 = 43690;
        size_t numAlloc2 = 43690;
        size_t numAlloc3 = 43690;
        size_t numAlloc4 = 43690;
        size_t numAlloc5 = 43690;
        std::thread thread0(allocFn, 512, numAlloc0, std::ref(s0), true);
        std::thread thread1(allocFn, 512, numAlloc1, std::ref(s1), true);
        std::thread thread2(allocFn, 512, numAlloc2, std::ref(s2), true);
        std::thread thread3(allocFn, 512, numAlloc3, std::ref(s3), true);
        std::thread thread4(allocFn, 512, numAlloc4, std::ref(s4), true);
        std::thread thread5(allocFn, 512, numAlloc5, std::ref(s5), true);

        thread0.join();
        thread1.join();
        thread2.join();
        thread3.join();
        thread4.join();
        thread5.join();

        s0 = s0 / float(numAlloc0);
        s1 = s1 / float(numAlloc1);
        s2 = s2 / float(numAlloc2);
        s3 = s3 / float(numAlloc3);
        s4 = s4 / float(numAlloc4);
        s5 = s5 / float(numAlloc5);
        Cerr << "Average time per allocation t0: " << s0 << " us, t1: " << s1 << " us, t2: " << s2 << " us, t3: " << s3 <<" us, t4: " << s4 << " us, t5: " << s5 << " us" << Endl;
    }

    Cerr << "===" << Endl;

    for (size_t i = 0; i < 10; i++) {
        float s0 = 0.0;
        float s1 = 0.0;
        float s2 = 0.0;
        float s3 = 0.0;
        float s4 = 0.0;
        float s5 = 0.0;
        size_t numAlloc0 = 65536;
        size_t numAlloc1 = 32768;
        size_t numAlloc2 = 8192;
        size_t numAlloc3 = 4096;
        size_t numAlloc4 = 1024;
        size_t numAlloc5 = 512;
        std::thread thread0(allocFn, 512, numAlloc0, std::ref(s0), true);
        std::thread thread1(allocFn, 1024, numAlloc1, std::ref(s1), true);
        std::thread thread2(allocFn, 4096, numAlloc2, std::ref(s2), true);
        std::thread thread3(allocFn, 8192, numAlloc3, std::ref(s3), true);
        std::thread thread4(allocFn, 32768, numAlloc4, std::ref(s4), true);
        std::thread thread5(allocFn, 65536, numAlloc5, std::ref(s5), true);

        thread0.join();
        thread1.join();
        thread2.join();
        thread3.join();
        thread4.join();
        thread5.join();

        s0 = s0 / float(numAlloc0);
        s1 = s1 / float(numAlloc1);
        s2 = s2 / float(numAlloc2);
        s3 = s3 / float(numAlloc3);
        s4 = s4 / float(numAlloc4);
        s5 = s5 / float(numAlloc5);
        Cerr << "Average time per allocation t0: " << s0 << " us, t1: " << s1 << " us, t2: " << s2 << " us, t3: " << s3 <<" us, t4: " << s4 << " us, t5: " << s5 << " us" << Endl;
    }
}

TEST_F(TAllocatorSuite128, AllocationMultipleThreads) {
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 128
    };

    auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);
    std::atomic<size_t> threadsNum = 100;

    auto allocFn = [&](size_t maxSz, size_t num, NThreading::TFuture<void> barrier) {
        size_t j = 0;

        std::vector<NInterconnect::NRdma::TMemRegionPtr> alls;
        alls.reserve(num);
        TReallyFastRng32 rng(RandomNumber<ui64>());
        while (j < num) {
            auto memRegion = pool->Alloc((rng() % maxSz) | 1u, 0);
            if (!memRegion) {
                continue;
            }
            ASSERT_TRUE(memRegion) << "allocation failed";
            ASSERT_TRUE(memRegion->GetAddr()) << "invalid address";
            alls.push_back(memRegion);
            j++;
        }
        alls.clear();

        threadsNum.fetch_sub(1);

        barrier.GetValueSync();
    };

    NThreading::TPromise<void> promise = NThreading::NewPromise<void>();

    std::vector<std::unique_ptr<std::thread>> threads;
    threads.reserve(threadsNum.load());


    // Launch threads
    for (size_t i = threadsNum.load(); i > 0; i--) {
        threads.emplace_back(std::make_unique<std::thread>(allocFn, 32768, 35, promise.GetFuture()));
    }

    // whait the work to be done
    while (threadsNum.load()) {
        Sleep(TDuration::MilliSeconds(100));
    }

    // Signal to finish therads
    promise.SetValue();

    // Join
    for (const auto& t : threads) {
        t->join();
    }
}

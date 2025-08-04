#include <ydb/library/actors/interconnect/rdma/link_manager.h>
#include <ydb/library/actors/interconnect/rdma/ctx.h>
#include <ydb/library/actors/interconnect/rdma/mem_pool.h>

#include <library/cpp/testing/gtest/gtest.h>
#include <ydb/library/testlib/unittest_gtest_macro_subst.h>

#include <ydb/library/actors/interconnect/rdma/ibdrv/include/infiniband/verbs.h>

#include <ydb/library/actors/util/rope.h>

#include <thread>

const size_t BUF_SIZE = 1 * 1024 * 1024;

bool IsFastPool(std::shared_ptr<NInterconnect::NRdma::IMemPool> memPool) {
    return memPool->GetName() == "SlotMemPool" || memPool->GetName() == "IncrementalMemPool";
}

    TEST(Allocator, AllocMemoryManually) {
        auto ctxs = NInterconnect::NRdma::NLinkMgr::GetAllCtxs();
        ASSERT_TRUE(ctxs.size() > 0);
        auto [gidEntry, ctx] = ctxs[0];

        void *buf;
        buf = malloc(BUF_SIZE);
        ASSERT_TRUE(buf) << "unable to allocate memory";

        ibv_mr* mr = ibv_reg_mr(
            ctx->GetProtDomain(), buf, BUF_SIZE,
            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE
        );

        ASSERT_TRUE(mr) << "unable to register memory region";
        ASSERT_NE(mr->lkey, 0u) << "invalid lkey";
        ASSERT_NE(mr->rkey, 0u) << "invalid rkey";
        Cerr << "lkey: " << mr->lkey << " rkey: " << mr->rkey << Endl;
        ibv_dereg_mr(mr);
        free(buf);
    }

    class WithAllPools : public ::testing::TestWithParam<std::shared_ptr<NInterconnect::NRdma::IMemPool>> {};

    TEST_P(WithAllPools, AllocMemoryWithMemPool) {
        Cerr << NInterconnect::NRdma::NLinkMgr::GetAllCtxs().size() << " devices found" << Endl;

        auto memPool = GetParam();

        {
            auto memRegion = memPool->Alloc(BUF_SIZE, 0);
            ASSERT_TRUE(memRegion->GetAddr()) << "invalid address";
            ASSERT_EQ(memRegion->GetSize(), BUF_SIZE) << "invalid address";
            for (ui32 i = 0; i < NInterconnect::NRdma::NLinkMgr::GetAllCtxs().size(); ++i) {
                ASSERT_NE(memRegion->GetLKey(i), 0u) << "invalid lkey";
                ASSERT_NE(memRegion->GetRKey(i), 0u) << "invalid rkey";
                Cerr << "lkey: " << memRegion->GetLKey(i) << " rkey: " << memRegion->GetRKey(i) << Endl;
            }
        }
        for (ui32 i = 0; i < 10; ++i) {
            auto m1 = memPool->Alloc(BUF_SIZE, 0);
            ASSERT_NE(m1->GetAddr(), nullptr) << "invalid address";
            auto m2 = memPool->Alloc(BUF_SIZE, 0);
            ASSERT_NE(m2->GetAddr(), nullptr) << "invalid address";
            auto m3 = memPool->Alloc(BUF_SIZE, 0);
            ASSERT_NE(m3->GetAddr(), nullptr) << "invalid address";
        }
    }

    TEST_P(WithAllPools, AllocMemoryWithMemPoolAsync) {
        const ui32 NUM_THREADS = 20;
        const ui32 NUM_ALLOC = 10000;
        const ui32 BUF_SIZE = 4 * 1024;

        auto memPool = GetParam();

        std::vector<std::thread> threads;
        std::vector<ui64> times(NUM_THREADS);

        for (ui32 i = 0; i < NUM_THREADS; ++i) {
            threads.emplace_back([memPool, &t=times[i]]() {
                auto now = TInstant::Now();
                for (ui32 j = 0; j < NUM_ALLOC; ++j) {
                    auto memRegion = memPool->Alloc(BUF_SIZE, NInterconnect::NRdma::IMemPool::BLOCK_MODE);
                    ASSERT_TRUE(memRegion->GetAddr()) << "invalid address";
                }
                t = (TInstant::Now() - now).MicroSeconds();
            });
        }
        for (auto& t : threads) {
            t.join();
        }

        double s = 0;
        for (const auto& t : times) {
            s += t / 1000.0 / NUM_ALLOC;
        }
        Cerr << "Average time per allocation for " << memPool->GetName() << ": " << s / NUM_THREADS << " ms" << Endl;
    }

    TEST_P(WithAllPools, AllocMemoryWithMemPoolAsyncRandomOrderDealloc) {
        const ui32 NUM_THREADS = 16;
        const ui32 NUM_ALLOC = 1000000;
        const ui32 BUF_SIZE = 4 * 1024;

        auto memPool = GetParam();
        if (!IsFastPool(memPool)) {
            GTEST_SKIP() << "Skipping test for slow pool: " << memPool->GetName() << Endl;
        }

        std::vector<std::thread> threads;
        std::vector<ui64> times(NUM_THREADS);

        std::vector<NInterconnect::NRdma::TMemRegionPtr> regions(NUM_THREADS);
        std::vector<std::mutex> mtxs(NUM_THREADS);

        for (ui32 i = 0; i < NUM_THREADS; ++i) {
            threads.emplace_back([memPool, &t=times[i], &mtxs, &regions]() {
                auto now = TInstant::Now();
                for (ui32 j = 0; j < NUM_ALLOC; ++j) {
                    auto memRegion = memPool->Alloc(BUF_SIZE, 0);
                    ASSERT_TRUE(memRegion->GetAddr()) << "invalid address";
                    size_t pos = (size_t)rand() % NUM_THREADS;
                    mtxs[pos].lock();
                    regions[pos] = std::move(memRegion);
                    mtxs[pos].unlock();
                }
                t = (TInstant::Now() - now).MicroSeconds();
            });
        }
        for (auto& t : threads) {
            t.join();
        }

        double s = 0;
        for (const auto& t : times) {
            s += t / float(NUM_ALLOC);
        }
        Cerr << "Average time per allocation for " << memPool->GetName() << ": " << s / NUM_THREADS << " us" << Endl;
    }

    TEST_P(WithAllPools, MemRegRcBuf) {
        auto memPool = GetParam();
        TRcBuf data = memPool->AllocRcBuf(BUF_SIZE, 0).value();
        ASSERT_TRUE(data.GetData()) << "invalid data";
        ASSERT_EQ(data.GetSize(), BUF_SIZE) << "invalid size";
        data.GetDataMut()[0] = 'a';
        ASSERT_EQ(data.GetData()[0], 'a') << "data mismatch";
        IContiguousChunk::TPtr x = *data.ExtractFullUnderlyingContainer<IContiguousChunk::TPtr>();
        ASSERT_NE(x, nullptr) << "unable to extract underlying container";
        ASSERT_EQ(x->GetData().data(), data.GetData()) << "data mismatch";
        ASSERT_EQ(x->GetData().data()[0], 'a') << "data mismatch";
        ASSERT_EQ(x->GetInnerType(), IContiguousChunk::EInnerType::RDMA_MEM_REG) << "invalid inner type";

        auto memReg = dynamic_cast<NInterconnect::NRdma::TMemRegion*>(x.Get());
        ASSERT_TRUE(memReg) << "unable to cast to TMemRegion";
        ASSERT_EQ(memReg->GetSize(), BUF_SIZE) << "invalid size";
        for (ui32 i = 0; i < NInterconnect::NRdma::NLinkMgr::GetAllCtxs().size(); ++i) {
            ASSERT_NE(memReg->GetLKey(i), 0u) << "invalid lkey";
            ASSERT_NE(memReg->GetRKey(i), 0u) << "invalid rkey";
            Cerr << "lkey: " << memReg->GetLKey(i) << " rkey: " << memReg->GetRKey(i) << Endl;
        }
    }

    TEST_P(WithAllPools, MemRegRcBufSubstr) {
        auto memPool = GetParam();
        TRcBuf data = memPool->AllocRcBuf(BUF_SIZE, 0).value();
        data.GetDataMut()[0] = 'a';
        data.GetDataMut()[1] = 'b';
        data.GetDataMut()[2] = 'c';
        data.GetDataMut()[3] = 'd';
        IContiguousChunk::TPtr x = *data.ExtractFullUnderlyingContainer<IContiguousChunk::TPtr>();
        ASSERT_NE(x, nullptr) << "unable to extract underlying container";
        ASSERT_EQ(x->GetInnerType(), IContiguousChunk::EInnerType::RDMA_MEM_REG) << "invalid inner type for data";
        auto memReg = dynamic_cast<NInterconnect::NRdma::TMemRegion*>(x.Get());
        ASSERT_TRUE(memReg) << "unable to cast to TMemRegion for data";

        TRcBuf s1 = TRcBuf(TRcBuf::Piece, data.data(), 2, data);
        TRcBuf s2 = TRcBuf(TRcBuf::Piece, data.data() + 2, data.Size() - 2, data);
        ASSERT_EQ(s1.GetData()[0], 'a') << "data mismatch";
        ASSERT_EQ(s1.GetData()[1], 'b') << "data mismatch";
        ASSERT_EQ(s2.GetData()[0], 'c') << "data mismatch";
        ASSERT_EQ(s2.GetData()[1], 'd') << "data mismatch";

        auto memReg1 = NInterconnect::NRdma::TryExtractFromRcBuf(s1);
        auto memReg2 = NInterconnect::NRdma::TryExtractFromRcBuf(s2);
        ASSERT_FALSE(memReg1.Empty()) << "unable to extract mem region from s1";
        ASSERT_FALSE(memReg2.Empty()) << "unable to extract mem region from s2";
        ASSERT_EQ(memReg1.GetSize(), 2ULL) << "invalid size for memReg1";
        ASSERT_EQ(memReg2.GetSize(), data.GetSize() - 2) << "invalid size for memReg2";
    }

    TVector<NInterconnect::NRdma::TMemRegionSlice> ExtractMemRegions(const TRope& rope) {
        TVector<NInterconnect::NRdma::TMemRegionSlice> regions;
        for (auto it = rope.Begin(); it != rope.End(); ++it) {
            const TRcBuf& chunk = it.GetChunk();
            regions.emplace_back(NInterconnect::NRdma::TryExtractFromRcBuf(chunk));
        }
        return regions;
    }

    TEST_P(WithAllPools, MemRegRope) {
        auto memPool = GetParam();
        TRope rope;
        rope.Insert(rope.End(), TRope(TRcBuf(memPool->AllocRcBuf(BUF_SIZE, 0).value())));
        rope.Insert(rope.End(), TRope("AAAAAAABBBBBBBCCCCCC"));
        rope.Insert(rope.End(), TRope(TRcBuf(memPool->AllocRcBuf(2 * BUF_SIZE, 0).value())));
        auto regions = ExtractMemRegions(rope);
        ASSERT_EQ(regions.size(), 3ULL) << "invalid number of regions";
        ASSERT_FALSE(regions[0].Empty()) << "invalid region";
        ASSERT_EQ(regions[0].GetSize(), BUF_SIZE) << "invalid size";
        ASSERT_TRUE(regions[1].Empty()) << "invalid region";
        ASSERT_FALSE(regions[2].Empty()) << "invalid region";
        ASSERT_EQ(regions[2].GetSize(), 2 * BUF_SIZE) << "invalid size";
    }

/*
    Y_UNIT_TEST(FullChunkAllocationSingleThread) {
        std::shared_ptr<NInterconnect::NRdma::IMemPool> memPool = NInterconnect::NRdma::CreateIncrementalMemPool();
        size_t sz = memPool->GetMaxAllocSz();
        void* addr = nullptr;
        {
            NInterconnect::NRdma::TMemRegionPtr region = memPool->Alloc(sz);
            UNIT_ASSERT(region);
            addr = region->GetAddr();
            UNIT_ASSERT(addr); 
            UNIT_ASSERT_VALUES_EQUAL(sz, region->GetSize());
        }
        std::vector<void*> dummy;
        for (size_t i = 4096; i <= sz; i <<= 1) {
            dummy.push_back(std::aligned_alloc(4096, i));
        }
        {
            NInterconnect::NRdma::TMemRegionPtr region = memPool->Alloc(sz);
            UNIT_ASSERT(region);
            UNIT_ASSERT(addr == region->GetAddr()); 
            UNIT_ASSERT_VALUES_EQUAL(sz, region->GetSize());
        }
        for (auto& m : dummy) {
            std::free(m);
        }
    } */

    TEST_P(WithAllPools, FullChunkAllocationMultiThread) {
        const ui32 NUM_THREADS = 8;
        const ui32 NUM_ALLOC = 4000000;
        std::vector<std::thread> threads;
        std::vector<ui64> times(NUM_THREADS);

        auto memPool = GetParam();
        if (!IsFastPool(memPool)) {
            GTEST_SKIP() << "Skipping test for slow pool: " << memPool->GetName() << Endl;
        }
        size_t sz = memPool->GetMaxAllocSz();

        for (ui32 i = 0; i < NUM_THREADS; ++i) {
            threads.emplace_back([memPool, &t=times[i], sz]() {
                auto now = TInstant::Now();
                for (ui32 j = 0; j < NUM_ALLOC; ++j) {
                    NInterconnect::NRdma::TMemRegionPtr memRegion = memPool->Alloc(sz, NInterconnect::NRdma::IMemPool::BLOCK_MODE);
                    ASSERT_TRUE(memRegion->GetAddr()) << "invalid address";
                }
                t = (TInstant::Now() - now).MicroSeconds();
            });
        }
        for (auto& t : threads) {
            t.join();
        }
        threads.clear();

        double s = 0;
        for (const auto& t : times) {
            s += t / float(NUM_ALLOC);
        }
        Cerr << "Average time per allocation: " << s / NUM_THREADS << " us" << Endl;
    }

    TEST_P(WithAllPools, SmallAllocationMultiThread) {
        const ui32 NUM_THREADS = 8;
        const ui32 NUM_ALLOC = 4000000;
        std::vector<std::thread> threads;
        std::vector<ui64> times(NUM_THREADS);

        auto memPool = GetParam();
        if (!IsFastPool(memPool)) {
            GTEST_SKIP() << "Skipping test for slow pool: " << memPool->GetName() << Endl;
        }
        size_t sz = 4096;

        for (ui32 i = 0; i < NUM_THREADS; ++i) {
            threads.emplace_back([memPool, &t=times[i], sz]() {
                auto now = TInstant::Now();
                for (ui32 j = 0; j < NUM_ALLOC; ++j) {
                    auto memRegion = memPool->Alloc(sz, NInterconnect::NRdma::IMemPool::BLOCK_MODE);
                    ASSERT_TRUE(memRegion->GetAddr()) << "invalid address";
                }
                t = (TInstant::Now() - now).MicroSeconds();
            });
        }
        for (auto& t : threads) {
            t.join();
        }

        double s = 0;
        for (const auto& t : times) {
            s += t / float(NUM_ALLOC);
        }
        Cerr << "Average time per allocation: " << s / NUM_THREADS << " us" << Endl;
    }

INSTANTIATE_TEST_SUITE_P(
    Allocator,
    WithAllPools,
    ::testing::Values(
        NInterconnect::NRdma::CreateIncrementalMemPool(),
        NInterconnect::NRdma::CreateSlotMemPool(),
        NInterconnect::NRdma::CreateDummyMemPool()
    ),
    [](const testing::TestParamInfo<std::shared_ptr<NInterconnect::NRdma::IMemPool>>& info) {
        return info.param->GetName();
    }
);

#include <ydb/library/actors/interconnect/rdma/link_manager.h>
#include <ydb/library/actors/interconnect/rdma/ctx.h>
#include <ydb/library/actors/interconnect/rdma/mem_pool.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/actors/interconnect/rdma/ibdrv/include/infiniband/verbs.h>

#include <ydb/library/actors/util/rope.h>

#include <thread>

const size_t BUF_SIZE = 1 * 1024 * 1024;

// Copy paste from KQP
#define Y_UNIT_TEST_TWIN(N, OPT)                                                                                   \
    template <bool OPT>                                                                                            \
    struct TTestCase##N : public TCurrentTestCase {                                                                \
        TTestCase##N() : TCurrentTestCase() {                                                                      \
            if constexpr (OPT) { Name_ = #N "+" #OPT; } else { Name_ = #N "-" #OPT; }                              \
        }                                                                                                          \
        static THolder<NUnitTest::TBaseTestCase> CreateOn()  { return ::MakeHolder<TTestCase##N<true>>();  }       \
        static THolder<NUnitTest::TBaseTestCase> CreateOff() { return ::MakeHolder<TTestCase##N<false>>(); }       \
        void Execute_(NUnitTest::TTestContext&) override;                                                          \
    };                                                                                                             \
    struct TTestRegistration##N {                                                                                  \
        TTestRegistration##N() {                                                                                   \
            TCurrentTest::AddTest(TTestCase##N<true>::CreateOn);                                                   \
            TCurrentTest::AddTest(TTestCase##N<false>::CreateOff);                                                 \
        }                                                                                                          \
    };                                                                                                             \
    static TTestRegistration##N testRegistration##N;                                                               \
    template <bool OPT>                                                                                            \
    void TTestCase##N<OPT>::Execute_(NUnitTest::TTestContext& ut_context Y_DECLARE_UNUSED)                         \

Y_UNIT_TEST_SUITE(Allocator) {

    Y_UNIT_TEST(AllocMemoryManually) {
        auto ctxs = NInterconnect::NRdma::NLinkMgr::GetAllCtxs();
        UNIT_ASSERT(ctxs.size() > 0);
        auto [gidEntry, ctx] = ctxs[0];

        void *buf;
        buf = malloc(BUF_SIZE);
        UNIT_ASSERT_C(buf, "unable to allocate memory");

        ibv_mr* mr = ibv_reg_mr(
            ctx->GetProtDomain(), buf, BUF_SIZE,
            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE
        );

        UNIT_ASSERT_C(mr, "unable to register memory region");
        UNIT_ASSERT_C(mr->lkey != 0, "invalid lkey");
        UNIT_ASSERT_C(mr->rkey != 0, "invalid rkey");
        Cerr << "lkey: " << mr->lkey << " rkey: " << mr->rkey << Endl;
        ibv_dereg_mr(mr);
        free(buf);
    }

    template <bool IncrementalPool>
    std::shared_ptr<NInterconnect::NRdma::IMemPool> CreateMemPool() {
        return IncrementalPool ? NInterconnect::NRdma::CreateIncrementalMemPool() : NInterconnect::NRdma::CreateSlotMemPool();
    }


    Y_UNIT_TEST_TWIN(AllocMemoryWithMemPool, IncrementalPool) {
        Cerr << NInterconnect::NRdma::NLinkMgr::GetAllCtxs().size() << " devices found" << Endl;

        auto memPool = CreateMemPool<IncrementalPool>();

        {
            auto memRegion = memPool->Alloc(BUF_SIZE, 0);
            UNIT_ASSERT_C(memRegion->GetAddr(), "invalid address");
            UNIT_ASSERT_VALUES_EQUAL_C(memRegion->GetSize(), BUF_SIZE, "invalid address");
            for (ui32 i = 0; i < NInterconnect::NRdma::NLinkMgr::GetAllCtxs().size(); ++i) {
                // auto mr = memRegion->GetMr(i);
                UNIT_ASSERT_C(memRegion->GetLKey(i) != 0, "invalid lkey");
                UNIT_ASSERT_C(memRegion->GetRKey(i) != 0, "invalid rkey");
                Cerr << "lkey: " << memRegion->GetLKey(i) << " rkey: " << memRegion->GetRKey(i) << Endl;
            }
        }
        for (ui32 i = 0; i < 10; ++i) {
            auto m1 = memPool->Alloc(BUF_SIZE, 0);
            UNIT_ASSERT_C(m1->GetAddr() != nullptr, "invalid address");
            auto m2 = memPool->Alloc(BUF_SIZE, 0);
            UNIT_ASSERT_C(m2->GetAddr() != nullptr, "invalid address");
            auto m3 = memPool->Alloc(BUF_SIZE, 0);
            UNIT_ASSERT_C(m2->GetAddr() != nullptr, "invalid address");
        }
    }

    Y_UNIT_TEST_TWIN(AllocMemoryWithMemPoolAsync, IncrementalPool) {
        const ui32 NUM_THREADS = 20;
        const ui32 NUM_ALLOC = 10000;
        const ui32 BUF_SIZE = 4 * 1024;

        auto memPool = CreateMemPool<IncrementalPool>();

        std::vector<std::thread> threads;
        std::vector<ui64> times(NUM_THREADS);

        for (ui32 i = 0; i < NUM_THREADS; ++i) {
            threads.emplace_back([memPool, &t=times[i]]() {
                auto now = TInstant::Now();
                for (ui32 j = 0; j < NUM_ALLOC; ++j) {
                    auto memRegion = memPool->Alloc(BUF_SIZE, NInterconnect::NRdma::IMemPool::BLOCK_MODE);
                    UNIT_ASSERT_C(memRegion->GetAddr(), "invalid address");
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
        Cerr << "Average time per allocation: " << s / NUM_THREADS << " ms" << Endl;
    }

    Y_UNIT_TEST_TWIN(AllocMemoryWithMemPoolAsyncRandomOrderDealloc, IncrementalPool) {
        const ui32 NUM_THREADS = 4;
        const ui32 NUM_ALLOC = 10000000;
        const ui32 BUF_SIZE = 1 * 1024 * 1024;

        auto memPool = CreateMemPool<IncrementalPool>();
        // auto memPool = NInterconnect::NRdma::CreateIncrementalMemPool();
        // auto memPool = NInterconnect::NRdma::CreateSlotMemPool();

        std::vector<std::thread> threads;
        std::vector<ui64> times(NUM_THREADS);

        std::vector<NInterconnect::NRdma::TMemRegionPtr> regions(NUM_THREADS);
        std::mutex mtx;

        for (ui32 i = 0; i < NUM_THREADS; ++i) {
            threads.emplace_back([memPool, &t=times[i], &mtx, &regions]() {
                auto now = TInstant::Now();
                for (ui32 j = 0; j < NUM_ALLOC; ++j) {
                    auto memRegion = memPool->Alloc(BUF_SIZE, 0);
                    UNIT_ASSERT_C(memRegion->GetAddr(), "invalid address");
                    size_t pos = (size_t)rand() % NUM_THREADS;
                    mtx.lock();
                    regions[pos] = std::move(memRegion);
                    mtx.unlock();
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
        Cerr << "Average time per allocation: " << s / NUM_THREADS << " ms" << Endl;
    }


    Y_UNIT_TEST_TWIN(MemRegRcBuf, IncrementalPool) {
        auto memPool = CreateMemPool<IncrementalPool>();
        TRcBuf data = memPool->AllocRcBuf(BUF_SIZE, 0).value();
        UNIT_ASSERT_C(data.GetData(), "invalid data");
        UNIT_ASSERT_VALUES_EQUAL_C(data.GetSize(), BUF_SIZE, "invalid size");
        data.GetDataMut()[0] = 'a';
        UNIT_ASSERT_VALUES_EQUAL_C(data.GetData()[0], 'a', "data mismatch");
        IContiguousChunk::TPtr x = *data.ExtractFullUnderlyingContainer<IContiguousChunk::TPtr>();
        UNIT_ASSERT_C(x != nullptr, "unable to extract underlying container");
        UNIT_ASSERT_VALUES_EQUAL_C(x->GetData().data(), data.GetData(), "data mismatch");
        UNIT_ASSERT_VALUES_EQUAL_C(x->GetData().data()[0], 'a', "data mismatch");
        UNIT_ASSERT_C(x->GetInnerType() == IContiguousChunk::EInnerType::RDMA_MEM_REG, "invalid inner type");

        auto memReg = dynamic_cast<NInterconnect::NRdma::TMemRegion*>(x.Get());
        UNIT_ASSERT_C(memReg, "unable to cast to TMemRegion");
        UNIT_ASSERT_VALUES_EQUAL_C(memReg->GetSize(), BUF_SIZE, "invalid size");
        for (ui32 i = 0; i < NInterconnect::NRdma::NLinkMgr::GetAllCtxs().size(); ++i) {
            UNIT_ASSERT_C(memReg->GetLKey(i) != 0, "invalid lkey");
            UNIT_ASSERT_C(memReg->GetRKey(i) != 0, "invalid rkey");
            Cerr << "lkey: " << memReg->GetLKey(i) << " rkey: " << memReg->GetRKey(i) << Endl;
        }
    }

    Y_UNIT_TEST_TWIN(MemRegRcBufSubstr, IncrementalPool) {
        auto memPool = CreateMemPool<IncrementalPool>();
        TRcBuf data = memPool->AllocRcBuf(BUF_SIZE, 0).value();
        data.GetDataMut()[0] = 'a';
        data.GetDataMut()[1] = 'b';
        data.GetDataMut()[2] = 'c';
        data.GetDataMut()[3] = 'd';
        IContiguousChunk::TPtr x = *data.ExtractFullUnderlyingContainer<IContiguousChunk::TPtr>();
        UNIT_ASSERT_C(x != nullptr, "unable to extract underlying container");
        UNIT_ASSERT_C(x->GetInnerType() == IContiguousChunk::EInnerType::RDMA_MEM_REG, "invalid inner type for data");
        auto memReg = dynamic_cast<NInterconnect::NRdma::TMemRegion*>(x.Get());
        UNIT_ASSERT_C(memReg, "unable to cast to TMemRegion for data");

        TRcBuf s1 = TRcBuf(TRcBuf::Piece, data.data(), 2, data);
        TRcBuf s2 = TRcBuf(TRcBuf::Piece, data.data() + 2, data.Size() - 2, data);
        UNIT_ASSERT_VALUES_EQUAL_C(s1.GetData()[0], 'a', "data mismatch");
        UNIT_ASSERT_VALUES_EQUAL_C(s1.GetData()[1], 'b', "data mismatch");
        UNIT_ASSERT_VALUES_EQUAL_C(s2.GetData()[0], 'c', "data mismatch");
        UNIT_ASSERT_VALUES_EQUAL_C(s2.GetData()[1], 'd', "data mismatch");

        auto memReg1 = NInterconnect::NRdma::TryExtractFromRcBuf(s1);
        auto memReg2 = NInterconnect::NRdma::TryExtractFromRcBuf(s2);
        UNIT_ASSERT_C(!memReg1.Empty(), "unable to extract mem region from s1");
        UNIT_ASSERT_C(!memReg2.Empty(), "unable to extract mem region from s2");
        UNIT_ASSERT_VALUES_EQUAL_C(memReg1.GetSize(), 2, "invalid size for memReg1");
        UNIT_ASSERT_VALUES_EQUAL_C(memReg2.GetSize(), data.GetSize() - 2, "invalid size for memReg2");
    }

    TVector<NInterconnect::NRdma::TMemRegionSlice> ExtractMemRegions(const TRope& rope) {
        TVector<NInterconnect::NRdma::TMemRegionSlice> regions;
        for (auto it = rope.Begin(); it != rope.End(); ++it) {
            const TRcBuf& chunk = it.GetChunk();
            regions.emplace_back(NInterconnect::NRdma::TryExtractFromRcBuf(chunk));
        }
        return regions;
    }


    Y_UNIT_TEST_TWIN(MemRegRope, IncrementalPool) {
        auto memPool = CreateMemPool<IncrementalPool>();
        TRope rope;
        rope.Insert(rope.End(), TRope(TRcBuf(memPool->AllocRcBuf(BUF_SIZE, 0).value())));
        rope.Insert(rope.End(), TRope("AAAAAAABBBBBBBCCCCCC"));
        rope.Insert(rope.End(), TRope(TRcBuf(memPool->AllocRcBuf(2 * BUF_SIZE, 0).value())));
        auto regions = ExtractMemRegions(rope);
        UNIT_ASSERT_VALUES_EQUAL_C(regions.size(), 3, "invalid number of regions");
        UNIT_ASSERT_C(!regions[0].Empty(), "invalid region");
        UNIT_ASSERT_VALUES_EQUAL_C(regions[0].GetSize(), BUF_SIZE, "invalid size");
        UNIT_ASSERT_C(regions[1].Empty(), "invalid region");
        UNIT_ASSERT_C(!regions[2].Empty(), "invalid region");
        UNIT_ASSERT_VALUES_EQUAL_C(regions[2].GetSize(), 2 * BUF_SIZE, "invalid size");
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

    Y_UNIT_TEST(FullChunkAllocationMultiThread) {
        const ui32 NUM_THREADS = 8;
        const ui32 NUM_ALLOC = 400000;
        std::vector<std::thread> threads;
        std::vector<ui64> times(NUM_THREADS);

        auto memPool = NInterconnect::NRdma::CreateIncrementalMemPool();
        size_t sz = memPool->GetMaxAllocSz();

        for (ui32 i = 0; i < NUM_THREADS; ++i) {
            threads.emplace_back([memPool, &t=times[i], sz]() {
                auto now = TInstant::Now();
                for (ui32 j = 0; j < NUM_ALLOC; ++j) {
                    NInterconnect::NRdma::TMemRegionPtr memRegion = memPool->Alloc(sz, NInterconnect::NRdma::IMemPool::BLOCK_MODE);
                    UNIT_ASSERT_C(memRegion->GetAddr(), "invalid address");
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
            s += t / 1000.0 / NUM_ALLOC;
        }
        Cerr << "Average time per allocation: " << s / NUM_THREADS << " ms" << Endl;
    }

    Y_UNIT_TEST(SmallAllocationMultiThread) {
        const ui32 NUM_THREADS = 8;
        //const ui32 NUM_ALLOC = 4000000000;
        const ui32 NUM_ALLOC = 400000;
        std::vector<std::thread> threads;
        std::vector<ui64> times(NUM_THREADS);

        auto memPool = NInterconnect::NRdma::CreateIncrementalMemPool();
        size_t sz = 4096;//memPool->GetMaxAllocSz();

        for (ui32 i = 0; i < NUM_THREADS; ++i) {
            threads.emplace_back([memPool, &t=times[i], sz]() {
                auto now = TInstant::Now();
                for (ui32 j = 0; j < NUM_ALLOC; ++j) {
                    auto memRegion = memPool->Alloc(sz, NInterconnect::NRdma::IMemPool::BLOCK_MODE);
                    UNIT_ASSERT_C(memRegion->GetAddr(), "invalid address");
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
        Cerr << "Average time per allocation: " << s / NUM_THREADS << " ms" << Endl;
    }

}

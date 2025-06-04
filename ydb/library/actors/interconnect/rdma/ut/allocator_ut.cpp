#include <ydb/library/actors/interconnect/rdma/link_manager.h>
#include <ydb/library/actors/interconnect/rdma/ctx.h>
#include <ydb/library/actors/interconnect/rdma/mem_pool.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/actors/interconnect/rdma/ibdrv/include/infiniband/verbs.h>

#include <ydb/library/actors/util/rope.h>

#include <thread>

const size_t BUF_SIZE = 1 * 1024 * 1024;

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

    Y_UNIT_TEST(AllocMemoryWithMemPool) {
        Cerr << NInterconnect::NRdma::NLinkMgr::GetAllCtxs().size() << " devices found" << Endl;

        auto memPool = NInterconnect::NRdma::CreateDummyMemPool();

        {
            auto memRegion = memPool->Alloc(BUF_SIZE);
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
            auto m1 = memPool->Alloc(BUF_SIZE);
            UNIT_ASSERT_C(m1->GetAddr() != nullptr, "invalid address");
            auto m2 = memPool->Alloc(BUF_SIZE);
            UNIT_ASSERT_C(m2->GetAddr() != nullptr, "invalid address");
            auto m3 = memPool->Alloc(BUF_SIZE);
            UNIT_ASSERT_C(m2->GetAddr() != nullptr, "invalid address");
        }
    }

    Y_UNIT_TEST(AllocMemoryWithMemPoolAsync) {
        const ui32 NUM_THREADS = 20;
        const ui32 NUM_ALLOC = 10000;
        const ui32 BUF_SIZE = 4 * 1024;

        auto memPool = NInterconnect::NRdma::CreateDummyMemPool();

        std::vector<std::thread> threads;
        std::vector<ui64> times(NUM_THREADS);

        for (ui32 i = 0; i < NUM_THREADS; ++i) {
            threads.emplace_back([memPool, &t=times[i]]() {
                auto now = TInstant::Now();
                for (ui32 j = 0; j < NUM_ALLOC; ++j) {
                    auto memRegion = memPool->Alloc(BUF_SIZE);
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

    Y_UNIT_TEST(MemRegRcBuf) {
        auto memPool = NInterconnect::NRdma::CreateDummyMemPool();
        TRcBuf data = memPool->AllocRcBuf(BUF_SIZE);
        UNIT_ASSERT_C(data.GetData(), "invalid data");
        UNIT_ASSERT_C(data.GetSize() == BUF_SIZE, "invalid size");
        data.GetDataMut()[0] = 'a';
        UNIT_ASSERT_C(data.GetData()[0] == 'a', "data mismatch");
        IContiguousChunk::TPtr x = data.ExtractUnderlyingContainerOrCopy<IContiguousChunk::TPtr>();
        UNIT_ASSERT_C(x != nullptr, "unable to extract underlying container");
        UNIT_ASSERT_C(x->GetData().data() == data.GetData(), "data mismatch");
        UNIT_ASSERT_C(x->GetData().data()[0] == 'a', "data mismatch");
        UNIT_ASSERT_C(x->GetInnerType() == IContiguousChunk::EInnerType::RDMA_MEM_REG, "invalid inner type");

        auto memReg = dynamic_cast<NInterconnect::NRdma::TMemRegion*>(x.Get());
        UNIT_ASSERT_C(memReg, "unable to cast to TMemRegion");
        UNIT_ASSERT_C(memReg->GetSize() == BUF_SIZE, "invalid size");
        for (ui32 i = 0; i < NInterconnect::NRdma::NLinkMgr::GetAllCtxs().size(); ++i) {
            UNIT_ASSERT_C(memReg->GetLKey(i) != 0, "invalid lkey");
            UNIT_ASSERT_C(memReg->GetRKey(i) != 0, "invalid rkey");
            Cerr << "lkey: " << memReg->GetLKey(i) << " rkey: " << memReg->GetRKey(i) << Endl;
        }
    }

    TVector<NInterconnect::NRdma::TMemRegion*> ExtractMemRegions(const TRope& rope) {
        TVector<NInterconnect::NRdma::TMemRegion*> regions;
        for (auto it = rope.Begin(); it != rope.End(); ++it) {
            NInterconnect::NRdma::TMemRegion* memReg = nullptr;
            const TRcBuf& chunk = it.GetChunk();
            IContiguousChunk::TPtr underlying = chunk.ExtractUnderlyingContainerOrCopy<IContiguousChunk::TPtr>();
            if (underlying && underlying->GetInnerType() == IContiguousChunk::EInnerType::RDMA_MEM_REG) {
                memReg = dynamic_cast<NInterconnect::NRdma::TMemRegion*>(underlying.Get());
            }
            regions.push_back(memReg);
        }
        return regions;
    }

    Y_UNIT_TEST(MemRegRope) {
        auto memPool = NInterconnect::NRdma::CreateDummyMemPool();
        TRope rope;
        rope.Insert(rope.End(), TRope(TRcBuf(memPool->AllocRcBuf(BUF_SIZE))));
        rope.Insert(rope.End(), TRope("AAAAAAABBBBBBBCCCCCC"));
        rope.Insert(rope.End(), TRope(TRcBuf(memPool->AllocRcBuf(2 * BUF_SIZE))));
        auto regions = ExtractMemRegions(rope);
        UNIT_ASSERT_C(regions.size() == 3, "invalid number of regions");
        UNIT_ASSERT_C(regions[0] != nullptr, "invalid region");
        UNIT_ASSERT_C(regions[0]->GetSize() == BUF_SIZE, "invalid size");
        UNIT_ASSERT_C(regions[1] == nullptr, "invalid region");
        UNIT_ASSERT_C(regions[2] != nullptr, "invalid region");
        UNIT_ASSERT_C(regions[2]->GetSize() == 2 * BUF_SIZE, "invalid size");
    }
}

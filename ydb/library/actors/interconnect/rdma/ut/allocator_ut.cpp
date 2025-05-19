#include <ydb/library/actors/interconnect/rdma/rdma_link_manager.h>
#include <ydb/library/actors/interconnect/rdma/rdma_ctx.h>
#include <ydb/library/actors/interconnect/rdma/mem_pool.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/actors/interconnect/rdma/ibdrv/include/infiniband/verbs.h>

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
            UNIT_ASSERT_C(!memRegion->IsEmpty(), "unable to allocate memory region");
            UNIT_ASSERT_C(memRegion->GetAddr(), "invalid address");
            for (ui32 i = 0; i < NInterconnect::NRdma::NLinkMgr::GetAllCtxs().size(); ++i) {
                auto mr = memRegion->GetMr(i);
                UNIT_ASSERT_C(mr, "unable to register memory region");
                UNIT_ASSERT_C(mr->lkey != 0, "invalid lkey");
                UNIT_ASSERT_C(mr->rkey != 0, "invalid rkey");
                UNIT_ASSERT_C(mr->addr == memRegion->GetAddr(), "invalid address");
                UNIT_ASSERT_C(mr->length == BUF_SIZE, "invalid length");
                Cerr << "lkey: " << mr->lkey << " rkey: " << mr->rkey << Endl;
            }
        }
        for (ui32 i = 0; i < 10; ++i) {
            auto m1 = memPool->Alloc(BUF_SIZE);
            UNIT_ASSERT_C(!m1->IsEmpty(), "unable to allocate memory region");
            auto m2 = memPool->Alloc(BUF_SIZE);
            UNIT_ASSERT_C(!m2->IsEmpty(), "unable to allocate memory region");
            auto m3 = memPool->Alloc(BUF_SIZE);
            UNIT_ASSERT_C(!m2->IsEmpty(), "unable to allocate memory region");
        }
    }

    Y_UNIT_TEST(AllocMemoryWithMemPoolAsync) {
        const ui32 NUM_THREADS = 20;
        const ui32 NUM_ALLOC = 10000;
        const ui32 BUF_SIZE = 1 * 1024 * 1024;

        auto memPool = NInterconnect::NRdma::CreateDummyMemPool();

        std::vector<std::thread> threads;
        std::vector<ui64> times(NUM_THREADS);

        for (ui32 i = 0; i < NUM_THREADS; ++i) {
            threads.emplace_back([memPool, &t=times[i]]() {
                auto now = TInstant::Now();
                for (ui32 j = 0; j < NUM_ALLOC; ++j) {
                    auto memRegion = memPool->Alloc(BUF_SIZE);
                    UNIT_ASSERT_C(!memRegion->IsEmpty(), "unable to allocate memory region");
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

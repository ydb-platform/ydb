#include <util/thread/pool.h>

#include <string.h>

#include <ydb/library/actors/interconnect/rdma/ctx.h>
#include <ydb/library/actors/interconnect/rdma/events.h>
#include <ydb/library/actors/interconnect/rdma/rdma.h>
#include <ydb/library/actors/interconnect/rdma/mem_pool.h>
#include <ydb/library/actors/interconnect/interconnect_address.h>
#include <ydb/library/actors/interconnect/poller/poller_actor.h>

#include <library/cpp/testing/gtest/gtest.h>
#include <ydb/library/testlib/unittest_gtest_macro_subst.h>

#include "utils.h"

using namespace NRdmaTest;
using namespace NInterconnect::NRdma;
using namespace NActors;

static const size_t MEM_REG_SZ = 4096;

class TRdmaLow : public TSkipFixture {};

static NInterconnect::NRdma::TMemRegionPtr AllocSourceRegion(std::shared_ptr<IMemPool> memPool) {
    auto reg = memPool->Alloc(MEM_REG_SZ, IMemPool::EMPTY);
    memset(reg->GetAddr(), 0, MEM_REG_SZ);
    const char* testString = "-_RMDA_YDB_INTERCONNRCT_-";
    strncpy((char*)reg->GetAddr(), testString, MEM_REG_SZ);
    return reg;
}

void DoReadInOneProcess(TString bindTo) {
    auto rdma = InitLocalRdmaStuff(bindTo);

    auto reg1 = AllocSourceRegion(rdma->MemPool);
    auto reg2 = rdma->MemPool->Alloc(MEM_REG_SZ, 0);

    ReadOneMemRegion(rdma, rdma->Qp2, reg1->GetAddr(), reg1->GetRKey(rdma->Ctx->GetDeviceIndex()), MEM_REG_SZ, reg2);

    ASSERT_TRUE(strncmp((char*)reg1->GetAddr(), (char*)reg2->GetAddr(), MEM_REG_SZ) == 0);
}

TEST_F(TRdmaLow, ReadInOneProcessIpV4) {
    DoReadInOneProcess("127.0.0.1");
}

TEST_F(TRdmaLow, ReadInOneProcessIpV6) {
    DoReadInOneProcess("::1");
}

/*
 * This test cover the sutuation when sender going to reuse memory but has no
 * information about remote reading in progress.
 * In this case we change QP on the sender to the 'Reset' state and expect reader will fail with read error
 */
TEST_F(TRdmaLow, ReadInOneProcessWithQpInterruption) {
    TString addr = "127.0.0.1";

    auto rdma = InitLocalRdmaStuff(addr);

    THolder<IThreadPool> pool = CreateThreadPool(2, 2);
    const int intialAttempts = 50000;

    // Use attempt as timeout to delay to run mem corrupter 
    int attempt = intialAttempts;

    // bin search works unstable here due to small ammount of time to trigger race
    while (attempt--) {
        auto reg1 = AllocSourceRegion(rdma->MemPool);
        auto reg2 = rdma->MemPool->Alloc(reg1->GetSize(), 0);
        std::vector<char> expected(reg1->GetSize());
        memcpy(expected.data(), (char*)reg1->GetAddr(), reg1->GetSize());

        NThreading::TPromise<void> promise = NThreading::NewPromise<void>();
        NThreading::TFuture<void> done = promise.GetFuture();

        class TMemCorrupter : public IObjectInQueue {
        public:
            TMemCorrupter(char* mem, size_t sz, TQueuePair* qp, int attempt, NThreading::TPromise<void> promise)
                : Mem(mem)
                , Sz(sz)
                , Qp(qp)
                , Attempt(attempt)
                , Promise(std::move(promise))
            {}
            virtual void Process(void*) override {
                // Delay to get a chanse to triger memset just during the RDMA read.
                Sleep(TDuration::MicroSeconds(Attempt / 128));
                Qp->ToErrorState();
                memset(Mem, 'Q', Sz);
                Promise.SetValue();
                delete this;
            }
        private:
            char* Mem;
            size_t Sz;
            TQueuePair* Qp;
            const int Attempt;
            NThreading::TPromise<void> Promise;
        };

        std::function<void()> srcInterruptHook = [&]() noexcept {
            bool added = pool->Add(
                new TMemCorrupter((char*)reg1->GetAddr(), reg1->GetSize(), rdma->Qp1.get(), attempt, std::move(promise))
            );
            Y_ABORT_UNLESS(added);
        };

        auto readResult = ReadOneMemRegion(rdma, rdma->Qp2, reg1->GetAddr(), reg1->GetRKey(rdma->Ctx->GetDeviceIndex()), MEM_REG_SZ, reg2, std::move(srcInterruptHook));

        // Whait until corrupter finished
        done.Wait();

        switch (readResult) {
            case EReadResult::OK: // corrupter fired too late, just check data is ok
                {
                    ASSERT_TRUE(strncmp(expected.data(), (char*)reg2->GetAddr(), MEM_REG_SZ) == 0);
                    // Additional check cq is empty after all this stuff
                    ICq::TWrStats stats = rdma->CqPtr->GetWrStats();
                    EXPECT_TRUE(stats.Total > 0);
                    EXPECT_TRUE(stats.Ready == stats.Total);
                }
                break;
            case EReadResult::WRPOST_ERR: // currupter fired too early, increase timeout
                attempt = std::min(intialAttempts, attempt *= 2);
                break;
            case EReadResult::READ_ERR:
                Cerr << "passed at " << attempt << Endl;
                return;
        }
        if (attempt == 0) {
            Cerr << "race was not triggered, restart..." << Endl;
            attempt = intialAttempts;
        }

        {
            rdma->Qp1->ToResetState();
            rdma->Qp2->ToResetState();

            auto qp1num = rdma->Qp1->GetQpNum();

            {
                int err = rdma->Qp2->ToRtsState(qp1num, rdma->Ctx->GetGid(), rdma->Ctx->GetPortAttr().active_mtu);
                EXPECT_TRUE(err == 0);
            }

            {
                int err = rdma->Qp1->ToRtsState(rdma->Qp2->GetQpNum(), rdma->Ctx->GetGid(), rdma->Ctx->GetPortAttr().active_mtu);
                EXPECT_TRUE(err == 0);
            }
        }
    }
}

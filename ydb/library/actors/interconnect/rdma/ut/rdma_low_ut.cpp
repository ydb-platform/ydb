#include <util/thread/pool.h>

#include <string.h>

#include <contrib/libs/ibdrv/include/infiniband/verbs.h>
#include <ydb/library/actors/interconnect/rdma/ctx.h>
#include <ydb/library/actors/interconnect/rdma/events.h>
#include <ydb/library/actors/interconnect/rdma/rdma.h>
#include <ydb/library/actors/interconnect/rdma/mem_pool.h>
#include <ydb/library/actors/interconnect/address/interconnect_address.h>
#include <ydb/library/actors/interconnect/poller/poller_actor.h>

#include <library/cpp/testing/gtest/gtest.h>
#include <ydb/library/testlib/unittest_gtest_macro_subst.h>

#include "utils.h"

using namespace NRdmaTest;
using namespace NInterconnect::NRdma;
using namespace NActors;

static const size_t MEM_REG_SZ = 4096;

class TRdmaLow : public TSkipFixture {};
class TCqMode : public TSkipFixtureWithParams<NInterconnect::NRdma::ECqMode> {};

static NInterconnect::NRdma::TMemRegionPtr AllocSourceRegion(std::shared_ptr<IMemPool> memPool) {
    auto reg = memPool->Alloc(MEM_REG_SZ, IMemPool::EMPTY);
    memset(reg->GetAddr(), 0, MEM_REG_SZ);
    const char* testString = "-_RDMA_YDB_INTERCONNECT_-";
    strncpy((char*)reg->GetAddr(), testString, MEM_REG_SZ);
    return reg;
}

void DoReadInOneProcess(TString bindTo, NInterconnect::NRdma::ECqMode mode) {
    auto rdma = InitLocalRdmaStuff(bindTo, mode);

    auto reg1 = AllocSourceRegion(rdma->MemPool);
    auto reg2 = rdma->MemPool->Alloc(MEM_REG_SZ, 0);

    ReadOneMemRegion(rdma, rdma->Qp2, reg1->GetAddr(), reg1->GetRKey(rdma->Ctx->GetDeviceIndex()), MEM_REG_SZ, reg2);

    ASSERT_TRUE(strncmp((char*)reg1->GetAddr(), (char*)reg2->GetAddr(), MEM_REG_SZ) == 0);
}

TEST_P(TCqMode, ReadInOneProcessIpV4) {
    DoReadInOneProcess("127.0.0.1", GetParam());
}

TEST_P(TCqMode, ReadInOneProcessIpV6) {
    DoReadInOneProcess("::1", GetParam());
}

/*
 * This test covers the situation when sender is going to reuse memory but has no
 * information about remote reading in progress.
 * In this case we change QP on the sender to the 'Reset' state and expect reader will fail with read error.
 */
TEST_P(TCqMode, ReadInOneProcessWithQpInterruption) {
    TString addr = "127.0.0.1";

    auto rdma = InitLocalRdmaStuff(addr, GetParam());

    THolder<IThreadPool> pool = CreateThreadPool(2, 2);
    const int initialAttempts = 50000;

    // Use attempt as timeout to delay the memory corruptor.
    int attempt = initialAttempts;

    // Binary search is unstable here due to the small amount of time to trigger the race.
    while (attempt--) {
        auto reg1 = AllocSourceRegion(rdma->MemPool);
        auto reg2 = rdma->MemPool->Alloc(reg1->GetSize(), 0);
        std::vector<char> expected(reg1->GetSize());
        memcpy(expected.data(), (char*)reg1->GetAddr(), reg1->GetSize());

        NThreading::TPromise<void> promise = NThreading::NewPromise<void>();
        NThreading::TFuture<void> done = promise.GetFuture();

        class TMemCorruptor : public IObjectInQueue {
        public:
            TMemCorruptor(char* mem, size_t sz, TQueuePair* qp, int attempt, NThreading::TPromise<void> promise)
                : Mem(mem)
                , Sz(sz)
                , Qp(qp)
                , Attempt(attempt)
                , Promise(std::move(promise))
            {}
            virtual void Process(void*) override {
                // Delay to get a chance to trigger memset just during the RDMA read.
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
                new TMemCorruptor((char*)reg1->GetAddr(), reg1->GetSize(), rdma->Qp1.get(), attempt, std::move(promise))
            );
            Y_ABORT_UNLESS(added);
        };

        auto readResult = ReadOneMemRegion(rdma, rdma->Qp2, reg1->GetAddr(), reg1->GetRKey(rdma->Ctx->GetDeviceIndex()), MEM_REG_SZ, reg2, std::move(srcInterruptHook));

        // Wait until corruptor finished.
        done.Wait();

        switch (readResult) {
            case EReadResult::OK: // corruptor fired too late, just check data is ok
                {
                    ASSERT_TRUE(strncmp(expected.data(), (char*)reg2->GetAddr(), MEM_REG_SZ) == 0);
                    // Additional check cq is empty after all this stuff
                    ICq::TWrStats stats = rdma->CqPtr->GetWrStats();
                    EXPECT_TRUE(stats.Total > 0);
                    EXPECT_TRUE(stats.Ready == stats.Total);
                }
                break;
            case EReadResult::WRPOST_ERR: // corruptor fired too early, increase timeout
                attempt = std::min(initialAttempts, attempt *= 2);
                break;
            case EReadResult::READ_ERR:
                Cerr << "passed at " << attempt << Endl;
                return;
        }
        if (attempt == 0) {
            Cerr << "race was not triggered, restart..." << Endl;
            attempt = initialAttempts;
        }

        {
            rdma->Qp1->ToResetState();
            rdma->Qp2->ToResetState();

            auto qp1num = rdma->Qp1->GetQpNum();

            {
                int err = rdma->Qp2->ToRtsState(NInterconnect::NRdma::THandshakeData {
                    .QpNum = qp1num,
                    .SubnetPrefix = rdma->Ctx->GetGid().global.subnet_prefix,
                    .InterfaceId = rdma->Ctx->GetGid().global.interface_id,
                    .MtuIndex = rdma->Ctx->GetPortAttr().active_mtu
                 });
                EXPECT_TRUE(err == 0);
            }

            {
                int err = rdma->Qp1->ToRtsState(NInterconnect::NRdma::THandshakeData {
                    .QpNum = rdma->Qp2->GetQpNum(),
                    .SubnetPrefix = rdma->Ctx->GetGid().global.subnet_prefix,
                    .InterfaceId = rdma->Ctx->GetGid().global.interface_id,
                    .MtuIndex = rdma->Ctx->GetPortAttr().active_mtu
                });
                EXPECT_TRUE(err == 0);
            }
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
    TRdmaLow,
    TCqMode,
    ::testing::Values(
        NInterconnect::NRdma::ECqMode::POLLING,
        NInterconnect::NRdma::ECqMode::EVENT
    ),
    [](const testing::TestParamInfo<NInterconnect::NRdma::ECqMode>& info) {
        switch (info.param) {
            case NInterconnect::NRdma::ECqMode::POLLING:
                return "POLLING";
            case NInterconnect::NRdma::ECqMode::EVENT:
                return "EVENT";
        }
    }
);

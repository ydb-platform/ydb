#include <util/thread/pool.h>

#include <string.h>

#include <ydb/library/actors/interconnect/rdma/rdma.h>
#include <ydb/library/actors/interconnect/rdma/events.h>
#include <ydb/library/actors/interconnect/rdma/mem_pool.h>
#include <ydb/library/actors/interconnect/interconnect_address.h>
#include <ydb/library/actors/interconnect/poller_actor.h>

#include <library/cpp/testing/gtest/gtest.h>
#include <ydb/library/testlib/unittest_gtest_macro_subst.h>

#include "utils.h"

using namespace NInterconnect::NRdma;
using namespace NActors;

static const size_t MEM_REG_SZ = 4096;


static NInterconnect::NRdma::TMemRegionPtr AllocSourceRegion(std::shared_ptr<IMemPool> memPool) {
    auto reg = memPool->Alloc(MEM_REG_SZ, IMemPool::BLOCK_MODE);
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

TEST(RdmaLow, ReadInOneProcessIpV4) {
    DoReadInOneProcess("127.0.0.1");
}

TEST(RdmaLow, ReadInOneProcessIpV6) {
    DoReadInOneProcess("::1");
}

/*
 * This test cover the sutuation when sender going to reuse memory but has no
 * information about remote reading in progress.
 * In this case we change QP on the sender to the 'Reset' state and expect reader will fail with read error
 */
TEST(RdmaLow, ReadInOneProcessWithQpInterruption) {
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
            }
        private:
            char* Mem;
            size_t Sz;
            TQueuePair* Qp;
            const int Attempt;
            NThreading::TPromise<void> Promise;
        } corrupter((char*)reg1->GetAddr(), reg1->GetSize(), &rdma->Qp1, attempt, std::move(promise));

        std::function<void()> srcInterruptHook = [&pool, &corrupter]() noexcept {
            bool added = pool->Add(&corrupter);
            Y_ABORT_UNLESS(added);
        };

        auto readResult = ReadOneMemRegion(rdma, rdma->Qp2, reg1->GetAddr(), reg1->GetRKey(rdma->Ctx->GetDeviceIndex()), MEM_REG_SZ, reg2, std::move(srcInterruptHook));

        // Whait until corrupter finished
        done.Wait();

        switch (readResult) {
            case EReadResult::OK: // corrupter fired too late, just check data is ok
                ASSERT_TRUE(strncmp(expected.data(), (char*)reg2->GetAddr(), MEM_REG_SZ) == 0);
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
            rdma->Qp1.ToResetState(); 
            rdma->Qp2.ToResetState(); 

            auto qp1num = rdma->Qp1.GetQpNum();

            {
                int err = rdma->Qp2.ToRtsState(rdma->Ctx, qp1num, rdma->Ctx->GetGid(), rdma->Ctx->GetPortAttr().active_mtu);
                EXPECT_TRUE(err == 0);
            }

            {
                int err = rdma->Qp1.ToRtsState(rdma->Ctx, rdma->Qp2.GetQpNum(), rdma->Ctx->GetGid(), rdma->Ctx->GetPortAttr().active_mtu);
                EXPECT_TRUE(err == 0);
            }
        }
    }
}

TEST(RdmaLow, CqOverflow) {
    auto [actorSystem, ctx] = PrepareTestRuntime("::1");
    auto cqActorId = actorSystem->Register(CreateCqActor(1));

    auto memPool = NInterconnect::NRdma::CreateIncrementalMemPool();
    
    // Number of attempt to trigger overflow
    int attempt = 10000;

    std::atomic<bool> wasOverflow = false;
    while (attempt--) {
        auto cqPtr = GetCqHandle(actorSystem.Get(), ctx, cqActorId);
        std::atomic<int> postedNum = 0;
        std::atomic<int> completedNum = 0;

        TQueuePair qp1;
        {
            int err = qp1.Init(ctx, cqPtr.get(), 8);
            ASSERT_EQ(err, 0);
        }

        auto reg1 = AllocSourceRegion(memPool);

        auto qp1num = qp1.GetQpNum();
        TQueuePair qp2;
        {
            int err = qp2.Init(ctx, cqPtr.get(), 8);
            ASSERT_EQ(err, 0);
            err = qp2.ToRtsState(ctx, qp1num, ctx->GetGid(), ctx->GetPortAttr().active_mtu);
            ASSERT_EQ(err, 0);
        }

        {
            int err = qp1.ToRtsState(ctx, qp2.GetQpNum(), ctx->GetGid(), ctx->GetPortAttr().active_mtu);
            ASSERT_EQ(err, 0);
        }

        const size_t inflight = 40;
        std::vector<NThreading::TFuture<bool>> completed;
        completed.reserve(inflight);
        auto reg2 = memPool->Alloc(MEM_REG_SZ, 0);

        bool wasAlloc = false;

        for (size_t i = 0; i < inflight; i++) {
            ICq::IWr* wr = nullptr;

            auto asptr = actorSystem->GetActorSystem(0);
            NThreading::TPromise<bool> promise = NThreading::NewPromise<bool>();
            auto future = promise.GetFuture();
            auto cb = [promise, asptr, &completedNum, &wasOverflow](NActors::TActorSystem* as, TEvRdmaIoDone* ioDone) mutable {
                Y_ABORT_UNLESS(as == asptr);
                completedNum.fetch_add(1);
                promise.SetValue(ioDone->IsSuccess());
                if (ioDone->IsCqError()) {
                    wasOverflow.store(true, std::memory_order_relaxed);
                }
                delete ioDone; // Clean up the event
            };
            while (wr == nullptr) {
                auto allocResult = cqPtr->AllocWr(cb);
                if (ICq::IsWrSuccess(allocResult)) {
                    wasAlloc = true;
                    wr = std::get<0>(allocResult);
                } else if (ICq::IsWrErr(allocResult)) {
                    wasOverflow.store(true, std::memory_order_relaxed);
                    break;
                } else {
                    ASSERT_TRUE(ICq::IsWrBusy(allocResult));
                }
            }

            if (!wr) {
                break;
            }

            ASSERT_TRUE(wr);
            postedNum.fetch_add(1);

            int err = qp2.SendRdmaReadWr(wr->GetId(), reg2->GetAddr(), reg2->GetLKey(ctx->GetDeviceIndex()), reg1->GetAddr(), reg1->GetRKey(ctx->GetDeviceIndex()), MEM_REG_SZ);
            if (err) {
                Cerr << "got post err: " << err << Endl;
                wr->Release();
            } else {
                completed.emplace_back(future);
            }
        }

        ASSERT_TRUE(wasAlloc); // Check it was at least one sucess wr allocation
        if (wasOverflow && attempt) {
            attempt = 1;
        }

        auto stats = cqPtr->GetWrStats();
        Cerr << "Whait for futures... " << "total: " << stats.Total << "ready: " << stats.Ready << Endl;

        auto all = NThreading::WaitAll(completed);
        while (!all.HasValue()) {
            Sleep(TDuration::Seconds(1));
            Cerr << "... " << postedNum.load() << "  " << completedNum.load() << Endl;
        }

        if (!wasOverflow) {
            ASSERT_TRUE(strncmp((char*)reg1->GetAddr(), (char*)reg2->GetAddr(), MEM_REG_SZ) == 0);
        }
    }

    ASSERT_TRUE(wasOverflow.load(std::memory_order_relaxed)); // Check it was at least one sucess wr allocation
}


#include <string.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <ydb/library/actors/interconnect/rdma/ctx.h>
#include <ydb/library/actors/interconnect/rdma/events.h>
#include <ydb/library/actors/interconnect/cq_actor.h>
#include <ydb/library/actors/interconnect/rdma/rdma.h>
#include <ydb/library/actors/interconnect/rdma/mem_pool.h>
#include <ydb/library/actors/interconnect/rdma/link_manager.h>
#include <ydb/library/actors/interconnect/interconnect_address.h>
#include <ydb/library/actors/interconnect/poller_actor.h>

using namespace NInterconnect::NRdma;
using namespace NActors;

static const size_t MEM_REG_SZ = 4096;

static ICq::TPtr GetCqHandle(NActors::TTestActorRuntimeBase* actorSystem, TRdmaCtx* ctx, TActorId cqActorId) {

    const TActorId edge = actorSystem->AllocateEdgeActor(0);
    auto ev = std::make_unique<TEvGetCqHandle>(ctx);

    actorSystem->Send(new IEventHandle(cqActorId, edge, ev.release()), 0);

    TAutoPtr<IEventHandle> handle;
    actorSystem->GrabEdgeEvent<TEvGetCqHandle>(handle);

    TEvGetCqHandle* cqHandle = handle->Get<TEvGetCqHandle>();
    UNIT_ASSERT(cqHandle->CqPtr);
    return cqHandle->CqPtr;
}

static std::tuple<THolder<NActors::TTestActorRuntimeBase>, TRdmaCtx*> PrepareTestRuntime(TString defIp) {
    auto actorSystem = MakeHolder<NActors::TTestActorRuntimeBase>(1, 1, true);
    actorSystem->Initialize();

    TDispatchOptions opts;
    opts.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
    actorSystem->DispatchEvents(opts);

    auto env = std::getenv("IP_TO_BIND_RDMA_TEST");

    TString ip = env ?: defIp;

    NInterconnect::TAddress address(ip, 7777);
    auto ctx = NInterconnect::NRdma::NLinkMgr::GetCtx(address.GetV6CompatAddr());
    UNIT_ASSERT(ctx);
    Cerr << "Using verbs context: " << *ctx << ", on addr: " << ip << Endl;

    return {std::move(actorSystem), ctx};
}

static NInterconnect::NRdma::TMemRegionPtr AllocSourceRegion(std::shared_ptr<IMemPool> memPool) {
    auto reg = memPool->Alloc(MEM_REG_SZ);
    memset(reg->GetAddr(), 0, MEM_REG_SZ);
    const char* testString = "-_RMDA_YDB_INTERCONNRCT_-";
    strncpy((char*)reg->GetAddr(), testString, MEM_REG_SZ);
    return reg;
}

Y_UNIT_TEST_SUITE(RdmaLow) {
    void DoReadInOneProcess(TString bindTo) {
        auto [actorSystem, ctx] = PrepareTestRuntime(bindTo);
        auto cqActorId = actorSystem->Register(CreateCqActor(1));
        auto cqPtr = GetCqHandle(actorSystem.Get(), ctx, cqActorId);

        auto memPool = NInterconnect::NRdma::CreateDummyMemPool();

        TQueuePair qp1;
        {
            int err = qp1.Init(ctx, cqPtr.get(), 16);
            UNIT_ASSERT_C(err == 0, strerror(err));
        }

        auto reg1 = AllocSourceRegion(memPool);

        auto qp1num = qp1.GetQpNum();

        TQueuePair qp2;
        {
            int err = qp2.Init(ctx, cqPtr.get(), 16);
            UNIT_ASSERT(err == 0);
            err = qp2.ToRtsState(ctx, qp1num, ctx->GetGid(), ctx->GetPortAttr().active_mtu);
            UNIT_ASSERT(err == 0);
        }

        {
            int err = qp1.ToRtsState(ctx, qp2.GetQpNum(), ctx->GetGid(), ctx->GetPortAttr().active_mtu);
            UNIT_ASSERT(err == 0);
        }

        auto reg2 = memPool->Alloc(MEM_REG_SZ);

        NThreading::TPromise<bool> promise = NThreading::NewPromise<bool>();
        auto future = promise.GetFuture();

        auto asptr = actorSystem->GetActorSystem(0);
        auto cb = [promise, asptr](NActors::TActorSystem* as, TEvRdmaIoDone* ioDone) mutable {
            Y_ABORT_UNLESS(as == asptr);
            promise.SetValue(ioDone->IsSuccess());
        };

        auto allocResult = cqPtr->AllocWr(cb);
        ICq::IWr* wr = (allocResult.index() == 0) ? std::get<0>(allocResult) : nullptr;

        UNIT_ASSERT(wr);

        qp2.SendRdmaReadWr(wr->GetId(), reg2->GetAddr(), reg2->GetLKey(ctx->GetDeviceIndex()), reg1->GetAddr(), reg1->GetRKey(ctx->GetDeviceIndex()), MEM_REG_SZ);

        UNIT_ASSERT(future.GetValueSync());
        UNIT_ASSERT(strncmp((char*)reg1->GetAddr(), (char*)reg2->GetAddr(), MEM_REG_SZ) == 0);
    }

    Y_UNIT_TEST(ReadInOneProcessIpV4) {
        DoReadInOneProcess("127.0.0.1");
    }

    Y_UNIT_TEST(ReadInOneProcessIpV6) {
        DoReadInOneProcess("::1");
    }

    Y_UNIT_TEST(CqOverflow) {
        auto [actorSystem, ctx] = PrepareTestRuntime("::1");
        auto cqActorId = actorSystem->Register(CreateCqActor(1));

        auto memPool = NInterconnect::NRdma::CreateDummyMemPool();
        
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
                UNIT_ASSERT(err == 0);
            }

            auto reg1 = AllocSourceRegion(memPool);

            auto qp1num = qp1.GetQpNum();
            TQueuePair qp2;
            {
                int err = qp2.Init(ctx, cqPtr.get(), 8);
                UNIT_ASSERT(err == 0);
                err = qp2.ToRtsState(ctx, qp1num, ctx->GetGid(), ctx->GetPortAttr().active_mtu);
                UNIT_ASSERT(err == 0);
            }

            {
                int err = qp1.ToRtsState(ctx, qp2.GetQpNum(), ctx->GetGid(), ctx->GetPortAttr().active_mtu);
                UNIT_ASSERT(err == 0);
            }

            const size_t inflight = 40;
            std::vector<NThreading::TFuture<bool>> completed;
            completed.reserve(inflight);
            auto reg2 = memPool->Alloc(MEM_REG_SZ);

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
                        UNIT_ASSERT(ICq::IsWrBusy(allocResult));
                    }
                }

                if (!wr) {
                    break;
                }

                UNIT_ASSERT(wr);
                postedNum.fetch_add(1);

                int err = qp2.SendRdmaReadWr(wr->GetId(), reg2->GetAddr(), reg2->GetLKey(ctx->GetDeviceIndex()), reg1->GetAddr(), reg1->GetRKey(ctx->GetDeviceIndex()), MEM_REG_SZ);
                if (err) {
                    Cerr << "get post err: " << err << Endl;
                    wr->Release();
                } else {
                    completed.emplace_back(future);
                }
            }

            UNIT_ASSERT(wasAlloc); // Check it was at least one sucess wr allocation
            if (wasOverflow && attempt) {
                attempt = 1;
            }

            Cerr << "Whait for futures" << Endl;

            auto all = NThreading::WaitAll(completed);
            while (!all.HasValue()) {
                Sleep(TDuration::Seconds(1));
                Cerr << "... " << postedNum.load() << "  " << completedNum.load() << Endl;
            }
            UNIT_ASSERT(strncmp((char*)reg1->GetAddr(), (char*)reg2->GetAddr(), MEM_REG_SZ) == 0);
        }

        UNIT_ASSERT(wasOverflow.load(std::memory_order_relaxed)); // Check it was at least one sucess wr allocation
    }
}
 

#pragma once

#include <ydb/library/actors/interconnect/cq_actor.h>
#include <ydb/library/actors/interconnect/rdma/mem_pool.h>
#include <ydb/library/actors/interconnect/rdma/ctx.h>
#include <ydb/library/actors/interconnect/rdma/events.h>
#include <ydb/library/actors/interconnect/rdma/link_manager.h>

#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/gtest/gtest.h>
#include <ydb/library/testlib/unittest_gtest_macro_subst.h>


using namespace NInterconnect::NRdma;
using namespace NActors;


ICq::TPtr GetCqHandle(NActors::TTestActorRuntimeBase* actorSystem, TRdmaCtx* ctx, TActorId cqActorId) {
    const TActorId edge = actorSystem->AllocateEdgeActor(0);
    auto ev = std::make_unique<TEvGetCqHandle>(ctx);

    actorSystem->Send(new IEventHandle(cqActorId, edge, ev.release()), 0);

    TAutoPtr<IEventHandle> handle;
    actorSystem->GrabEdgeEvent<TEvGetCqHandle>(handle);

    TEvGetCqHandle* cqHandle = handle->Get<TEvGetCqHandle>();
    EXPECT_TRUE(cqHandle->CqPtr);
    return cqHandle->CqPtr;
}

std::tuple<THolder<NActors::TTestActorRuntimeBase>, TRdmaCtx*> PrepareTestRuntime(TString defIp) {
    auto actorSystem = MakeHolder<NActors::TTestActorRuntimeBase>(1, 1, true);
    actorSystem->Initialize();

    TDispatchOptions opts;
    opts.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
    actorSystem->DispatchEvents(opts);

    auto env = std::getenv("IP_TO_BIND_RDMA_TEST");

    TString ip = env ?: defIp;

    NInterconnect::TAddress address(ip, 7777);
    auto ctx = NInterconnect::NRdma::NLinkMgr::GetCtx(address.GetV6CompatAddr());
    EXPECT_TRUE(ctx);
    Cerr << "Using verbs context: " << *ctx << ", on addr: " << ip << Endl;

    return {std::move(actorSystem), ctx};
}

struct TLocalRdmaStuff {
    std::shared_ptr<NInterconnect::NRdma::IMemPool> MemPool;
    THolder<NActors::TTestActorRuntimeBase> ActorSystem;
    TActorId CqActorId;
    ICq::TPtr CqPtr;
    TRdmaCtx* Ctx;
    TQueuePair Qp1;
    TQueuePair Qp2;
};

std::shared_ptr<TLocalRdmaStuff> InitLocalRdmaStuff(TString bindTo="::1") {
    auto rdma = std::make_shared<TLocalRdmaStuff>();

    rdma->MemPool = NInterconnect::NRdma::CreateDummyMemPool();

    {
        auto [actorSystem, ctx] = PrepareTestRuntime(bindTo);
        rdma->ActorSystem = std::move(actorSystem);
        rdma->Ctx = ctx;
    }
    rdma->CqActorId = rdma->ActorSystem->Register(CreateCqMockActor(1));
    rdma->CqPtr = GetCqHandle(rdma->ActorSystem.get(), rdma->Ctx, rdma->CqActorId);

    {
        int err = rdma->Qp1.Init(rdma->Ctx, rdma->CqPtr.get(), 16);
        EXPECT_TRUE(err == 0) << strerror(err);
    }

    auto qp1num = rdma->Qp1.GetQpNum();

    {
        int err = rdma->Qp2.Init(rdma->Ctx, rdma->CqPtr.get(), 16);
        EXPECT_TRUE(err == 0);
        err = rdma->Qp2.ToRtsState(rdma->Ctx, qp1num, rdma->Ctx->GetGid(), rdma->Ctx->GetPortAttr().active_mtu);
        EXPECT_TRUE(err == 0);
    }

    {
        int err = rdma->Qp1.ToRtsState(rdma->Ctx, rdma->Qp2.GetQpNum(), rdma->Ctx->GetGid(), rdma->Ctx->GetPortAttr().active_mtu);
        EXPECT_TRUE(err == 0);
    }

    return rdma;
}

enum class EReadResult {
    OK,
    WRPOST_ERR,
    READ_ERR
};

EReadResult ReadOneMemRegion(std::shared_ptr<TLocalRdmaStuff> rdma, TQueuePair& qp, void* dstAddr, ui32 dstRkey, int dstSize, TMemRegionPtr& src, std::function<void()> hook = {}) {
    auto asptr = rdma->ActorSystem->GetActorSystem(0);
    NThreading::TPromise<bool> promise = NThreading::NewPromise<bool>();
    auto future = promise.GetFuture();
    auto cb = [promise, asptr](NActors::TActorSystem* as, TEvRdmaIoDone* ioDone) mutable {
        Y_ABORT_UNLESS(as == asptr);
        promise.SetValue(ioDone->IsSuccess());
        delete ioDone; // Clean up the event
    };

    auto allocResult = rdma->CqPtr->AllocWr(cb);
    ICq::IWr* wr = (allocResult.index() == 0) ? std::get<0>(allocResult) : nullptr;

    EXPECT_TRUE(wr);

    if (hook)
        hook();

    int err = qp.SendRdmaReadWr(wr->GetId(), src->GetAddr(), src->GetLKey(rdma->Ctx->GetDeviceIndex()), dstAddr, dstRkey, dstSize);
    if (err) {
        wr->Release();
        Cerr << "Unable to post wr" << Endl;
        return EReadResult::WRPOST_ERR;
    }

    if (!future.GetValueSync()) {
        return EReadResult::READ_ERR;
    } else {
        return EReadResult::OK;
    }
}



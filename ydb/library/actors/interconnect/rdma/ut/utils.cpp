#include "utils.h"

#include <ydb/library/actors/interconnect/rdma/cq_actor/cq_actor.h>
#include <ydb/library/actors/interconnect/rdma/ctx.h>
#include <ydb/library/actors/interconnect/rdma/events.h>
#include <ydb/library/actors/interconnect/rdma/link_manager.h>
#include <ydb/library/actors/interconnect/interconnect_stream.h>

#include <util/system/env.h>

#define RDMA_UT_EXPECT_TRUE(val) EXPECT_TRUE(val)

namespace NRdmaTest {

using namespace NActors;
using namespace NInterconnect::NRdma;

ICq::TPtr GetCqHandle(NActors::TTestActorRuntimeBase* actorSystem, TRdmaCtx* ctx, TActorId cqActorId) {
    const TActorId edge = actorSystem->AllocateEdgeActor(0);
    auto ev = std::make_unique<TEvGetCqHandle>(ctx);

    actorSystem->Send(new IEventHandle(cqActorId, edge, ev.release()), 0);

    TAutoPtr<IEventHandle> handle;
    actorSystem->GrabEdgeEvent<TEvGetCqHandle>(handle);

    TEvGetCqHandle* cqHandle = handle->Get<TEvGetCqHandle>();
    RDMA_UT_EXPECT_TRUE(cqHandle->CqPtr);
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
    auto ctx = NInterconnect::NRdma::NLinkMgr::GetCtx(address);
    RDMA_UT_EXPECT_TRUE(ctx);
    Cerr << "Using verbs context: " << *ctx << ", on addr: " << ip << Endl;

    return {std::move(actorSystem), ctx};
}

std::shared_ptr<TLocalRdmaStuff> InitLocalRdmaStuff(TString bindTo) {
    auto rdma = std::make_shared<TLocalRdmaStuff>();

    rdma->MemPool = NInterconnect::NRdma::CreateDummyMemPool();

    {
        auto [actorSystem, ctx] = PrepareTestRuntime(bindTo);
        rdma->ActorSystem = std::move(actorSystem);
        rdma->Ctx = ctx;
    }

    rdma->Qp1 = std::make_shared<TQueuePair>();
    rdma->Qp2 = std::make_shared<TQueuePair>();

    rdma->CqActorId = rdma->ActorSystem->Register(CreateCqActor(1, 1));
    rdma->CqPtr = GetCqHandle(rdma->ActorSystem.get(), rdma->Ctx, rdma->CqActorId);

    {
        int err = rdma->Qp1->Init(rdma->Ctx, rdma->CqPtr.get(), 16);
        RDMA_UT_EXPECT_TRUE(err == 0);
    }

    auto qp1num = rdma->Qp1->GetQpNum();

    {
        int err = rdma->Qp2->Init(rdma->Ctx, rdma->CqPtr.get(), 16);
        RDMA_UT_EXPECT_TRUE(err == 0);
        err = rdma->Qp2->ToRtsState(NInterconnect::NRdma::THandshakeData{
            .QpNum = qp1num,
            .SubnetPrefix = rdma->Ctx->GetGid().global.subnet_prefix,
            .InterfaceId = rdma->Ctx->GetGid().global.interface_id,
            .MtuIndex = rdma->Ctx->GetPortAttr().active_mtu
        });
        RDMA_UT_EXPECT_TRUE(err == 0);
    }

    {
        int err = rdma->Qp1->ToRtsState(NInterconnect::NRdma::THandshakeData{
            .QpNum = rdma->Qp2->GetQpNum(),
            .SubnetPrefix = rdma->Ctx->GetGid().global.subnet_prefix,
            .InterfaceId = rdma->Ctx->GetGid().global.interface_id,
            .MtuIndex = rdma->Ctx->GetPortAttr().active_mtu
        });
        RDMA_UT_EXPECT_TRUE(err == 0);
    }

    return rdma;
}

EReadResult ReadOneMemRegion(std::shared_ptr<TLocalRdmaStuff> rdma, std::shared_ptr<TQueuePair> qp, void* dstAddr, ui32 dstRkey, int dstSize, TMemRegionPtr& src, std::function<void()> hook) {
    auto asptr = rdma->ActorSystem->GetActorSystem(0);
    NThreading::TPromise<TEvRdmaIoDone* > promise = NThreading::NewPromise<TEvRdmaIoDone* >();
    auto future = promise.GetFuture();
    auto cb = [promise, asptr](NActors::TActorSystem* as, TEvRdmaIoDone* ioDone) mutable {
        Y_ABORT_UNLESS(as == asptr);
        promise.SetValue(ioDone);
    };

    if (hook)
        hook();

    std::unique_ptr<IIbVerbsBuilder> verbsBuilder = CreateIbVerbsBuilder(1);

    verbsBuilder->AddReadVerb(
        reinterpret_cast<char*>(src->GetAddr()),
        src->GetLKey(rdma->Ctx->GetDeviceIndex()),
        reinterpret_cast<void*>(dstAddr),
        dstRkey,
        dstSize,
        std::move(cb)
    );

    auto err = rdma->CqPtr->DoWrBatchAsync(qp, std::move(verbsBuilder));

    RDMA_UT_EXPECT_TRUE(!err);

    TEvRdmaIoDone* event = future.GetValueSync();

    EReadResult res;

    if (event->IsSuccess()) {
        res = EReadResult::OK;
    } else if (event->IsWrError()) {
        res = EReadResult::WRPOST_ERR;
    } else if (event->IsWcError()) {
        res = EReadResult::READ_ERR;
    } else {
        Y_ABORT_UNLESS(false, "Unexpected err in rdma test");
    }

    delete event;

    return res;
}

}

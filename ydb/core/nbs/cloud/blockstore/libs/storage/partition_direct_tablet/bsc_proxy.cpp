#include "bsc_proxy.h"

#include <ydb/core/mind/bscontroller/types.h>
#include <ydb/core/protos/base.pb.h>

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#include <util/string/builder.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;

namespace {

IEventBase* MakePipeFailureResult(const TString& reason)
{
    auto result = std::make_unique<
        TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult>();
    result->Record.SetStatus(TBscProxy::PipeFailureStatus);
    result->Record.SetErrorReason(reason);
    return result.release();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TBscProxy::TEvSend::TEvSend(THolder<IEventBase> request)
    : Request(std::move(request))
{}

TBscProxy::TBscProxy(TActorId owner, TLogTitle logTitle)
    : TActor(&TThis::StateWork)
    , Owner(owner)
    , LogTitle(std::move(logTitle))
{}

STFUNC(TBscProxy::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvSend, HandleSend);
        HFunc(
            TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult,
            HandleAllocateResult);
        HFunc(TEvTabletPipe::TEvClientConnected, HandleConnect);
        HFunc(TEvTabletPipe::TEvClientDestroyed, HandleDisconnect);
        cFunc(TEvents::TEvPoison::EventType, PassAway);
        default:
            LOG_ERROR(
                TActivationContext::AsActorContext(),
                NKikimrServices::NBS_PARTITION,
                "%s Unhandled event type: %u event %s",
                LogTitle.GetWithTime().c_str(),
                ev->GetTypeRewrite(),
                ev->ToString().c_str());
            break;
    }
}

void TBscProxy::HandleSend(TEvSend::TPtr& ev, const TActorContext& ctx)
{
    THolder<IEventBase> request = std::move(ev->Get()->Request);
    Y_ABORT_UNLESS(request);
    Y_ABORT_UNLESS(
        request->Type() ==
        TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup::EventType);
    const ui64 clientRequestCookie = ev->Cookie;

    if (InFlight) {
        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s BSC request rejected cookie=%lu already inflight cookie=%lu",
            LogTitle.GetWithTime().c_str(),
            clientRequestCookie,
            InFlight->ClientRequestCookie);
        ctx.Send(
            Owner,
            MakePipeFailureResult("BSC request already in flight"),
            0,
            clientRequestCookie);
        return;
    }

    Y_ABORT_UNLESS(!PipeClient);
    const ui64 bscRequestCookie = NextBscRequestCookie++;
    InFlight = TInFlight{
        .ClientRequestCookie = clientRequestCookie,
        .BscRequestCookie = bscRequestCookie,
    };
    NTabletPipe::TClientConfig clientConfig;
    clientConfig.RetryPolicy = {.RetryLimitCount = 3};
    PipeClient = ctx.Register(NTabletPipe::CreateClient(
        ctx.SelfID,
        MakeBSControllerID(),
        clientConfig));
    NTabletPipe::SendData(ctx, PipeClient, request.Release(), bscRequestCookie);

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s BSC request sent clientRequestCookie=%lu bscRequestCookie=%lu",
        LogTitle.GetWithTime().c_str(),
        clientRequestCookie,
        bscRequestCookie);
}

void TBscProxy::HandleAllocateResult(
    TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::TPtr& ev,
    const TActorContext& ctx)
{
    if (!InFlight || InFlight->BscRequestCookie != ev->Cookie) {
        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s Ignore BSC result for untracked cookie=%lu",
            LogTitle.GetWithTime().c_str(),
            ev->Cookie);
        return;
    }

    const ui64 clientRequestCookie = InFlight->ClientRequestCookie;
    InFlight.reset();
    ClosePipe(ctx);
    ctx.Send(Owner, ev->Release().Release(), 0, clientRequestCookie);
}

void TBscProxy::HandleConnect(
    TEvTabletPipe::TEvClientConnected::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (msg->ClientId != PipeClient) {
        return;
    }
    if (msg->Status == NKikimrProto::OK) {
        return;
    }

    LOG_ERROR(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s BSController pipe connect failed: %s",
        LogTitle.GetWithTime().c_str(),
        NKikimrProto::EReplyStatus_Name(msg->Status).c_str());

    FailInFlight(
        ctx,
        TStringBuilder() << "BSController pipe connect failed: "
                         << NKikimrProto::EReplyStatus_Name(msg->Status));
}

void TBscProxy::HandleDisconnect(
    TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
    const TActorContext& ctx)
{
    if (ev->Get()->ClientId != PipeClient) {
        return;
    }

    LOG_ERROR(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s BSController pipe destroyed",
        LogTitle.GetWithTime().c_str());

    FailInFlight(ctx, "BSController pipe destroyed");
}

void TBscProxy::FailInFlight(const TActorContext& ctx, const TString& reason)
{
    const std::optional<TInFlight> inflight = InFlight;
    InFlight.reset();
    ClosePipe(ctx);
    if (inflight) {
        ctx.Send(
            Owner,
            MakePipeFailureResult(reason),
            0,
            inflight->ClientRequestCookie);
    }
}

void TBscProxy::ClosePipe(const TActorContext& ctx)
{
    if (!PipeClient) {
        return;
    }
    NTabletPipe::CloseClient(ctx, PipeClient);
    PipeClient = {};
}

void TBscProxy::PassAway()
{
    InFlight.reset();
    ClosePipe(TActivationContext::AsActorContext());
    TActor::PassAway();
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

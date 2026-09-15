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

IEventBase* MakePipeFailureResult(ui32 requestType, const TString& reason)
{
    Y_ABORT_UNLESS(
        requestType ==
        TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup::EventType);

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
    const ui32 requestType = request->Type();
    Y_ABORT_UNLESS(
        requestType ==
        TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup::EventType);
    const ui64 cookie = ev->Cookie;
    Y_ABORT_UNLESS(InFlight.find(cookie) == InFlight.end());

    InFlight[cookie] = requestType;
    if (!PipeClient) {
        PipeClient = ctx.Register(
            NTabletPipe::CreateClient(ctx.SelfID, MakeBSControllerID()));
    }
    NTabletPipe::SendData(ctx, PipeClient, request.Release(), cookie);

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s BSC request sent cookie=%lu",
        LogTitle.GetWithTime().c_str(),
        cookie);
}

void TBscProxy::HandleAllocateResult(
    TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::TPtr& ev,
    const TActorContext& ctx)
{
    InFlight.erase(ev->Cookie);
    ctx.Send(ev->Forward(Owner));
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

    if (InFlight.empty()) {
        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s BSController pipe destroyed (idle)",
            LogTitle.GetWithTime().c_str());
        ClosePipe(ctx);
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
    auto inflight = std::move(InFlight);
    ClosePipe(ctx);
    for (const auto& [cookie, requestType]: inflight) {
        ctx.Send(Owner, MakePipeFailureResult(requestType, reason), 0, cookie);
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
    InFlight.clear();
    ClosePipe(TActivationContext::AsActorContext());
    TActor::PassAway();
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

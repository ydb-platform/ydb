#include "pqtablet_mock.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPersQueueTests {

TMaybe<ui64> TPQTabletMock::GetPartitionRequestCookie() const
{
    Y_ABORT_UNLESS(Request);
    Y_ABORT_UNLESS(Request->Record.HasPartitionRequest());

    if (Request->Record.GetPartitionRequest().HasCookie()) {
        return Request->Record.GetPartitionRequest().GetCookie();
    }

    return {};
}

void TPQTabletMock::PrepareGetOwnershipResponse()
{
    Response = std::make_unique<TEvPersQueue::TEvResponse>();
    Response->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
    Response->Record.SetErrorCode(NPersQueue::NErrorCode::OK);

    auto* partition = Response->Record.MutablePartitionResponse();

    if (auto cookie = GetPartitionRequestCookie()) {
        partition->SetCookie(*cookie);
    }

    auto* result = partition->MutableCmdGetOwnershipResult();
    result->SetOwnerCookie(OwnerCookie);
    result->SetSupportivePartition(1'000'000); // fictitious number of the supportive partition
}

void TPQTabletMock::PrepareGetMaxSeqNoResponse()
{
    Response = std::make_unique<TEvPersQueue::TEvResponse>();
    Response->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
    Response->Record.SetErrorCode(NPersQueue::NErrorCode::OK);

    auto* partition = Response->Record.MutablePartitionResponse();

    if (auto cookie = GetPartitionRequestCookie()) {
        partition->SetCookie(*cookie);
    }

    auto* sourceIdInfo = partition->MutableCmdGetMaxSeqNoResult()->AddSourceIdInfo();
    sourceIdInfo->SetState(NKikimrPQ::TMessageGroupInfo::STATE_REGISTERED);
    sourceIdInfo->SetSeqNo(MaxSeqNo);
}

auto TPQTabletMock::MakeReserveBytesResponse(ui64 cookie) -> TEvResponsePtr
{
    auto event = std::make_unique<TEvPersQueue::TEvResponse>();
    event->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
    event->Record.SetErrorCode(NPersQueue::NErrorCode::OK);

    auto* partition = event->Record.MutablePartitionResponse();
    partition->SetCookie(cookie);

    //
    // CmdWriteResultSize == 0
    //

    return event;
}

auto TPQTabletMock::MakeWriteResponse(ui64 cookie) -> TEvResponsePtr
{
    auto event = std::make_unique<TEvPersQueue::TEvResponse>();
    event->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
    event->Record.SetErrorCode(NPersQueue::NErrorCode::OK);

    auto* partition = event->Record.MutablePartitionResponse();
    partition->SetCookie(cookie);

    //
    // CmdWriteResultSize == 1
    //
    auto* w = partition->AddCmdWriteResult();
    Y_UNUSED(w);

    return event;
}

void TPQTabletMock::PrepareReserveBytesResponse(const TActorId& recipient)
{
    if (auto cookie = GetPartitionRequestCookie(); cookie) {
        TReply reply;
        reply.Response = MakeReserveBytesResponse(*cookie);
        reply.Recipient = recipient;

        CmdReserve.Replies[*cookie] = std::move(reply);
    }
}

void TPQTabletMock::PrepareWriteResponse(const TActorId& recipient)
{
    if (auto cookie = GetPartitionRequestCookie(); cookie) {
        TReply reply;
        reply.Response = MakeWriteResponse(*cookie);
        reply.Recipient = recipient;

        CmdWrite.Replies[*cookie] = std::move(reply);
    }
}

void TPQTabletMock::TrySendResponses(const TActorContext& ctx, TCommandReplies& cmd)
{
    size_t count = 0;
    for (auto cookie : cmd.ExpectedRequests) {
        count += cmd.Replies.contains(cookie);
    }

    if (count != cmd.ExpectedRequests.size()) {
        return;
    }

    for (auto cookie : cmd.ExpectedRequests) {
        auto p = cmd.Replies.find(cookie);
        auto& reply = p->second;

        ctx.Send(reply.Recipient, std::move(reply.Response));
        cmd.Replies.erase(p);
    }

    for (auto& [_, reply] : cmd.Replies) {
        ctx.Send(reply.Recipient, std::move(reply.Response));
    }
    cmd.Replies.clear();
}

TPQTabletMock::TPQTabletMock(const TActorId& tablet, TTabletStorageInfo* info) :
    TActor(&TThis::StateBoot),
    TTabletExecutedFlat(info, tablet, nullptr)
{
}

void TPQTabletMock::OnDetach(const TActorContext &ctx)
{
    Die(ctx);
}

void TPQTabletMock::OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx)
{
    Y_UNUSED(ev);

    Die(ctx);
}

void TPQTabletMock::DefaultSignalTabletActive(const TActorContext &ctx)
{
    Y_UNUSED(ctx);
}

void TPQTabletMock::OnActivateExecutor(const TActorContext &ctx)
{
    Become(&TThis::StateWork);
    SignalTabletActive(ctx);
}

STFUNC(TPQTabletMock::StateBoot)
{
    StateInitImpl(ev, SelfId());
}

STFUNC(TPQTabletMock::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvTabletPipe::TEvClientConnected, Handle);
        HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);

        HFunc(TEvPersQueue::TEvRequest, Handle);
    default:
        HandleDefaultEvents(ev, SelfId());
    }
}

void TPQTabletMock::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx)
{
    Y_UNUSED(ev);
    Y_UNUSED(ctx);
}

void TPQTabletMock::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx)
{
    Y_UNUSED(ev);
    Y_UNUSED(ctx);
}

void TPQTabletMock::Handle(TEvPersQueue::TEvRequest::TPtr& ev, const TActorContext& ctx)
{
    Request.reset(ev->Release().Release());

    Y_ABORT_UNLESS(Request->Record.HasPartitionRequest());
    auto& partition = Request->Record.GetPartitionRequest();

    if (partition.HasCmdGetOwnership()) {
        PrepareGetOwnershipResponse();
        UNIT_ASSERT(Response.get());
        ctx.Send(ev->Sender, std::move(Response));
    } else if (partition.HasCmdGetMaxSeqNo()) {
        PrepareGetMaxSeqNoResponse();
        UNIT_ASSERT(Response.get());
        ctx.Send(ev->Sender, std::move(Response));
    } else if (partition.HasCmdReserveBytes()) {
        PrepareReserveBytesResponse(ev->Sender);
        TrySendResponses(ctx, CmdReserve);
    } else if (partition.CmdWriteSize()) {
        PrepareWriteResponse(ev->Sender);
        TrySendResponses(ctx, CmdWrite);
    } else {
        Y_ABORT_UNLESS(false);
    }
}

void TPQTabletMock::AppendWriteReply(ui64 cookie)
{
    CmdReserve.ExpectedRequests.push_back(cookie);
    CmdWrite.ExpectedRequests.push_back(cookie);
}

}

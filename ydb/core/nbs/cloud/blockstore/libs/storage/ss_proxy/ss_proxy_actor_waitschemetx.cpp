#include "ss_proxy_actor.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::NBS_SS_PROXY

namespace NYdb::NBS::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NSchemeShard;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TReplyProxyActor final: public TActor<TReplyProxyActor>
{
private:
    const TActorId Owner;
    const ui64 TabletId;

public:
    TReplyProxyActor(const TActorId& owner, const ui64 tabletId)
        : TActor(&TThis::StateWork)
        , Owner(owner)
        , TabletId(tabletId)
    {}

private:
    STFUNC(StateWork);

    void Handle(
        const TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr& ev,
        const TActorContext& ctx);

    void Handle(
        const TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

STFUNC(TReplyProxyActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvSchemeShard::TEvNotifyTxCompletionRegistered, Handle);
        HFunc(TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);

        default:
            HandleUnexpectedEvent(
                ev,
                NKikimrServices::NBS_SS_PROXY,
                __PRETTY_FUNCTION__);
            break;
    }
}

void TReplyProxyActor::Handle(
    const TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr& ev,
    const TActorContext& ctx)
{
    // Send response to owner with the correct cookie
    ctx.Send(Owner, ev->Release().Release(), 0, TabletId);
}

void TReplyProxyActor::Handle(
    const TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev,
    const TActorContext& ctx)
{
    // Send response to owner with the correct cookie
    ctx.Send(Owner, ev->Release().Release(), 0, TabletId);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TSSProxyActor::HandleWaitSchemeTx(
    const TEvSSProxy::TEvWaitSchemeTxRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto& state = SchemeShardStates[msg->SchemeShardTabletId];
    auto& requests = state.TxToRequests[msg->TxId];
    requests.emplace_back(
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext));

    if (requests.size() == 1) {
        // This is the first request for this tabletId/txId
        SendWaitTxRequest(ctx, msg->SchemeShardTabletId, msg->TxId);
    }
}

void TSSProxyActor::SendWaitTxRequest(
    const TActorContext& ctx,
    ui64 schemeShard,
    ui64 txId)
{
    auto& state = SchemeShardStates[schemeShard];
    if (!state.ReplyProxy) {
        YDB_LOG_DEBUG_CTX(ctx, "Creating reply proxy actor for schemeshard",
            {"schemeShard", schemeShard});

        state.ReplyProxy =
            NYdb::NBS::Register<TReplyProxyActor>(ctx, ctx.SelfID, schemeShard);
    }

    YDB_LOG_DEBUG_CTX(ctx, "Sending NotifyTxCompletion to",
        {"schemeShard", schemeShard},
        {"txId", txId});

    TActorId clientId = ClientCache->Prepare(ctx, schemeShard);
    NTabletPipe::SendData(
        ctx.MakeFor(state.ReplyProxy),
        clientId,
        new TEvSchemeShard::TEvNotifyTxCompletion(txId));
}

void TSSProxyActor::HandleTxRegistered(
    const TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr& ev,
    const TActorContext& ctx)
{
    ui64 schemeShard = ev->Cookie;

    const auto* msg = ev->Get();
    ui64 txId = msg->Record.GetTxId();

    YDB_LOG_DEBUG_CTX(ctx, "Received NotifyTxCompletionRegistered from",
        {"schemeShard", schemeShard},
        {"txId", txId});
}

void TSSProxyActor::HandleTxResult(
    const TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev,
    const TActorContext& ctx)
{
    ui64 schemeShard = ev->Cookie;
    auto& state = SchemeShardStates[schemeShard];

    const auto* msg = ev->Get();
    ui64 txId = msg->Record.GetTxId();

    YDB_LOG_DEBUG_CTX(ctx, "Received NotifyTxCompletionResult from",
        {"schemeShard", schemeShard},
        {"txId", txId});

    auto it = state.TxToRequests.find(txId);
    if (it != state.TxToRequests.end()) {
        for (const auto& request: it->second) {
            NYdb::NBS::Reply(
                ctx,
                *request,
                std::make_unique<TEvSSProxy::TEvWaitSchemeTxResponse>());
        }
        state.TxToRequests.erase(it);
    }
}

}   // namespace NYdb::NBS::NStorage

#include "partition_writer_cache_actor.h"
#include <ydb/core/persqueue/writer/writer.h>

namespace NKikimr::NGRpcProxy::V1 {

TPartitionWriterCacheActor::TPartitionWriterCacheActor(const TActorId& owner,
                                                       ui32 partition,
                                                       ui64 tabletId,
                                                       const NPQ::TPartitionWriterOpts& opts) :
    Owner(owner),
    Partition(partition),
    TabletId(tabletId),
    Opts(opts)
{
}

void TPartitionWriterCacheActor::Bootstrap(const TActorContext& ctx)
{
    RegisterDefaultPartitionWriter(ctx);

    this->Become(&TPartitionWriterCacheActor::StateWork);
}

void TPartitionWriterCacheActor::RegisterPartitionWriter(const TString& sessionId, const TString& txId,
                                                         const TActorContext& ctx)
{
    std::pair<TString, TString> key(sessionId, txId);

    auto writer = std::make_unique<TPartitionWriter>();
    writer->Actor = CreatePartitionWriter(sessionId, txId, ctx);
    writer->LastActivity = ctx.Now();

    Writers.emplace(key, std::move(writer));
}

void TPartitionWriterCacheActor::RegisterDefaultPartitionWriter(const TActorContext& ctx)
{
    RegisterPartitionWriter("", "", ctx);
}

STFUNC(TPartitionWriterCacheActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(NPQ::TEvPartitionWriter::TEvTxWriteRequest, Handle);
        HFunc(NPQ::TEvPartitionWriter::TEvInitResult, Handle);
        HFunc(NPQ::TEvPartitionWriter::TEvWriteAccepted, Handle);
        HFunc(NPQ::TEvPartitionWriter::TEvWriteResponse, Handle);
        HFunc(NPQ::TEvPartitionWriter::TEvDisconnected, Handle);
        HFunc(TEvents::TEvPoison, Handle);
    }
}

void TPartitionWriterCacheActor::ReplyError(const TString& sessionId, const TString& txId,
                                            EErrorCode code, const TString& reason,
                                            ui64 cookie,
                                            const TActorContext& ctx)
{
    NKikimrClient::TResponse response;
    response.MutablePartitionResponse()->SetCookie(cookie);

    ctx.Send(Owner, new NPQ::TEvPartitionWriter::TEvWriteResponse(sessionId, txId,
                                                                  code, reason,
                                                                  std::move(response)));
}

void TPartitionWriterCacheActor::Handle(NPQ::TEvPartitionWriter::TEvTxWriteRequest::TPtr& ev, const TActorContext& ctx)
{
    auto& event = *ev->Get();

    if (auto* writer = GetPartitionWriter(event.SessionId, event.TxId, ctx); writer) {
        if (PendingWriteAccepted.Expected == Max<ui64>()) {
            Y_ABORT_UNLESS(PendingWriteResponse.Expected == Max<ui64>());

            PendingWriteAccepted.Expected = event.Request->GetCookie();
            PendingWriteResponse.Expected = event.Request->GetCookie();
        }

        writer->LastActivity = ctx.Now();
        writer->OnWriteRequest(std::move(event.Request), ctx);
    } else {
        ReplyError(event.SessionId, event.TxId,
                   EErrorCode::OverloadError, "limit of active transactions has been exceeded",
                   event.Request->GetCookie(),
                   ctx);
        this->Become(&TPartitionWriterCacheActor::StateBroken);
    }
}

void TPartitionWriterCacheActor::HandleOnBroken(NPQ::TEvPartitionWriter::TEvTxWriteRequest::TPtr& ev, const TActorContext& ctx)
{
    auto& event = *ev->Get();

    ReplyError(event.SessionId, event.TxId,
               EErrorCode::OverloadError, "limit of active transactions has been exceeded",
               event.Request->GetCookie(),
               ctx);
}

void TPartitionWriterCacheActor::Handle(NPQ::TEvPartitionWriter::TEvInitResult::TPtr& ev, const TActorContext& ctx)
{
    auto& result = *ev->Get();

    auto key = std::make_pair(result.SessionId, result.TxId);
    auto p = Writers.find(key);
    Y_ABORT_UNLESS(p != Writers.end());

    if (result.IsSuccess()) {
        p->second->OnEvInitResult(ev);
    } else {
        auto response = result.GetError().Response;
        ctx.Send(Owner, new NPQ::TEvPartitionWriter::TEvWriteResponse(result.SessionId, result.TxId,
                                                                      EErrorCode::InternalError, result.GetError().Reason,
                                                                      std::move(response)));
    }

    if (!result.SessionId && !result.TxId) {
        ctx.Send(Owner, ev->Release().Release());
    }
}

template <class TEvent>
void TPartitionWriterCacheActor::TryForwardToOwner(TEvent* event, TEventQueue<TEvent>& queue,
                                                   ui64 cookie,
                                                   const TActorContext& ctx)
{
    Y_ABORT_UNLESS(queue.Expected != Max<ui64>());

    if (queue.Expected == cookie) {
        ctx.Send(Owner, event);

        ++queue.Expected;
        for (auto p = queue.Events.find(queue.Expected); p != queue.Events.end(); ) {
            ctx.Send(Owner, p->second.release());
            queue.Events.erase(queue.Expected);

            ++queue.Expected;
            p = queue.Events.find(queue.Expected);
        }
    } else {
        queue.Events.emplace(cookie, event);
    }
}

void TPartitionWriterCacheActor::Handle(NPQ::TEvPartitionWriter::TEvWriteAccepted::TPtr& ev, const TActorContext& ctx)
{
    const auto& result = *ev->Get();

    auto key = std::make_pair(result.SessionId, result.TxId);
    auto p = Writers.find(key);
    Y_ABORT_UNLESS(p != Writers.end());

    if (result.Cookie == p->second->SentRequests.front()) {
        p->second->OnWriteAccepted(result, ctx);

        TryForwardToOwner(ev->Release().Release(), PendingWriteAccepted,
                          result.Cookie,
                          ctx);
    } else {
        ReplyError(result.SessionId, result.TxId,
                   EErrorCode::InternalError, "out of order reserve bytes response from server, may be previous is lost",
                   p->second->SentRequests.front(),
                   ctx);
        this->Become(&TPartitionWriterCacheActor::StateBroken);
    }
}

void TPartitionWriterCacheActor::Handle(NPQ::TEvPartitionWriter::TEvWriteResponse::TPtr& ev, const TActorContext& ctx)
{
    const auto& result = *ev->Get();

    auto key = std::make_pair(result.SessionId, result.TxId);
    auto p = Writers.find(key);
    Y_ABORT_UNLESS(p != Writers.end());

    if (result.IsSuccess()) {
        ui64 cookie = result.Record.GetPartitionResponse().GetCookie();
        if (cookie == p->second->AcceptedRequests.front()) {
            p->second->OnWriteResponse(result);

            TryForwardToOwner(ev->Release().Release(), PendingWriteResponse,
                              cookie,
                              ctx);
        } else {
            ReplyError(result.SessionId, result.TxId,
                       EErrorCode::InternalError, "out of order write response from server, may be previous is lost",
                       p->second->AcceptedRequests.front(),
                       ctx);
            this->Become(&TPartitionWriterCacheActor::StateBroken);
        }
    } else {
        ctx.Send(Owner, ev->Release().Release());
    }
}

void TPartitionWriterCacheActor::Handle(NPQ::TEvPartitionWriter::TEvDisconnected::TPtr& ev, const TActorContext& ctx)
{
    ctx.Send(Owner, ev->Release().Release());
}

void TPartitionWriterCacheActor::Handle(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx)
{
    Y_UNUSED(ev);

    for (auto& [_, writer] : Writers) {
        ctx.Send(writer->Actor, new TEvents::TEvPoisonPill());
    }

    Die(ctx);
}

auto TPartitionWriterCacheActor::GetPartitionWriter(const TString& sessionId, const TString& txId,
                                                    const TActorContext& ctx) -> TPartitionWriter*
{
    auto key = std::make_pair(sessionId, txId);

    auto p = Writers.find(key);
    if (p != Writers.end()) {
        return p->second.get();
    }

    if (Writers.size() >= (1 + MAX_TRANSACTIONS_COUNT)) {
        if (!TryDeleteOldestWriter(ctx)) {
            return nullptr;
        }
    }

    RegisterPartitionWriter(sessionId, txId, ctx);

    p = Writers.find(key);
    Y_ABORT_UNLESS(p != Writers.end());

    return p->second.get();
}

bool TPartitionWriterCacheActor::TryDeleteOldestWriter(const TActorContext& ctx)
{
    Y_ABORT_UNLESS(!Writers.empty());

    auto minLastActivity = TInstant::Max();
    auto oldest = Writers.end();

    for (auto p = Writers.begin(); p != Writers.end(); ++p) {
        auto& tx = p->first;
        auto& writer = *p->second;

        if ((tx.first == "") && (tx.second == "")) {
            continue;
        }

        if ((writer.LastActivity < minLastActivity) && !writer.HasPendingRequests()) {
            minLastActivity = writer.LastActivity;
            oldest = p;
        }
    }

    if (minLastActivity == TInstant::Max()) {
        return false;
    }

    ctx.Send(oldest->second->Actor, new TEvents::TEvPoisonPill());
    Writers.erase(oldest);

    return true;
}

TActorId TPartitionWriterCacheActor::CreatePartitionWriter(const TString& sessionId, const TString& txId,
                                                           const TActorContext& ctx)
{
    NPQ::TPartitionWriterOpts opts = Opts;
    if (sessionId && txId) {
        opts.WithSessionId(sessionId);
        opts.WithTxId(txId);
    }

    return ctx.RegisterWithSameMailbox(NPQ::CreatePartitionWriter(
        ctx.SelfID, TabletId, Partition, opts
    ));
}

STFUNC(TPartitionWriterCacheActor::StateBroken)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(NPQ::TEvPartitionWriter::TEvTxWriteRequest, HandleOnBroken);
        HFunc(NPQ::TEvPartitionWriter::TEvInitResult, Handle);
        HFunc(NPQ::TEvPartitionWriter::TEvWriteAccepted, Handle);
        HFunc(NPQ::TEvPartitionWriter::TEvWriteResponse, Handle);
        HFunc(NPQ::TEvPartitionWriter::TEvDisconnected, Handle);
        HFunc(TEvents::TEvPoison, Handle);
    }
}

}

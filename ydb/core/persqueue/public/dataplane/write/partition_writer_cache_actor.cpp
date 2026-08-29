#include "partition_writer_cache_actor.h"
#include "deferred_destination_upsert_actor.h"
#include "actors.h"

#include <ydb/core/persqueue/writer/writer.h>

namespace NKikimr::NPQ::NDataplane::NWrite {

using namespace NActors;

TPartitionWriterCacheActor::TPartitionWriterCacheActor(const TActorId& owner,
                                                       ui32 partition,
                                                       ui64 tabletId,
                                                       const TPartitionWriterOpts& opts)
    : TBase(NKikimrServices::PQ_WRITE_PROXY)
    , Owner(owner)
    , Partition(partition)
    , TabletId(tabletId)
    , Opts(opts)
{
}

void TPartitionWriterCacheActor::Bootstrap(const TActorContext& ctx)
{
    RegisterDefaultPartitionWriter(ctx);

    this->Become(&TPartitionWriterCacheActor::StateWork);
}

TString TPartitionWriterCacheActor::BuildLogPrefix() const {
    return TStringBuilder() << " (TabletId=" << TabletId << ", Partition=" << Partition << ") ";
}

void TPartitionWriterCacheActor::PoisonWriters() {
    for (auto& [_, writer] : std::exchange(Writers, {})) {
        Send(writer->Actor, new TEvents::TEvPoisonPill());
    }
}

void TPartitionWriterCacheActor::OnException(const std::exception& exc) {
    // Do not Die here: TBaseActor::OnUnhandledException will PassAway.
    const TString reason = TStringBuilder() << "Unhandled exception: " << exc.what();
    for (auto& [k, _] : Writers) {
        ReplyError(k.first, k.second, EErrorCode::InternalError, reason, 0);
    }
    PoisonWriters();
}

void TPartitionWriterCacheActor::PassAway() {
    PoisonWriters();
    TBase::PassAway();
}

void TPartitionWriterCacheActor::RegisterPartitionWriter(
    const TString& sessionId,
    const TString& txId,
    const std::optional<TDeferredPublishWriterOpts>& deferredPublish,
    const TActorContext& ctx)
{
    std::pair<TString, TString> key(sessionId, txId);

    auto writer = std::make_unique<TCachedPartitionWriter>();
    writer->Actor = CreatePartitionWriter(sessionId, txId, deferredPublish, ctx);
    writer->LastActivity = ctx.Now();

    Writers.emplace(key, std::move(writer));
}

void TPartitionWriterCacheActor::RegisterDefaultPartitionWriter(const TActorContext& ctx)
{
    RegisterPartitionWriter("", "", std::nullopt, ctx);
}

STFUNC(TPartitionWriterCacheActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvPartitionWriter::TEvTxWriteRequest, Handle);
        HFunc(TEvPartitionWriter::TEvRequestDeferredDestinationUpsert, HandleDeferredDestinationUpsertRequest);
        HFunc(TEvPartitionWriter::TEvInitResult, Handle);
        HFunc(TEvPartitionWriter::TEvWriteAccepted, Handle);
        HFunc(TEvPartitionWriter::TEvWriteResponse, Handle);
        HFunc(TEvPartitionWriter::TEvDisconnected, Handle);
        HFunc(TEvents::TEvPoison, Handle);
    }
}

void TPartitionWriterCacheActor::ReplyError(const TString& sessionId, const TString& txId,
                                            EErrorCode code, const TString& reason,
                                            ui64 cookie)
{
    NKikimrClient::TResponse response;
    response.MutablePartitionResponse()->SetCookie(cookie);

    Send(Owner, new TEvPartitionWriter::TEvWriteResponse(sessionId, txId,
                                                             code, reason,
                                                             std::move(response)));
}

void TPartitionWriterCacheActor::Handle(TEvPartitionWriter::TEvTxWriteRequest::TPtr& ev, const TActorContext& ctx)
{
    auto& event = *ev->Get();

    std::optional<TDeferredPublishWriterOpts> deferredPublish;
    if (event.DeferredPublish) {
        deferredPublish = *event.DeferredPublish;
    }

    if (auto* writer = GetPartitionWriter(event.SessionId, event.TxId, deferredPublish, ctx); writer) {
        if (PendingWriteAccepted.Expected == Max<ui64>()) {
            AFL_ENSURE(PendingWriteResponse.Expected == Max<ui64>());

            PendingWriteAccepted.Expected = event.Request->GetCookie();
            PendingWriteResponse.Expected = event.Request->GetCookie();
        }

        writer->LastActivity = ctx.Now();
        writer->OnWriteRequest(std::move(event.Request), std::move(ev->TraceId), ctx);
    } else {
        ReplyError(event.SessionId, event.TxId,
                   EErrorCode::OverloadError, "limit of active transactions has been exceeded",
                   event.Request->GetCookie());
        this->Become(&TPartitionWriterCacheActor::StateBroken);
    }
}

void TPartitionWriterCacheActor::HandleOnBroken(TEvPartitionWriter::TEvTxWriteRequest::TPtr& ev, const TActorContext& ctx)
{
    Y_UNUSED(ctx);
    auto& event = *ev->Get();

    ReplyError(event.SessionId, event.TxId,
               EErrorCode::OverloadError, "limit of active transactions has been exceeded",
               event.Request->GetCookie());
}

void TPartitionWriterCacheActor::HandleDeferredDestinationUpsertRequest(
    TEvPartitionWriter::TEvRequestDeferredDestinationUpsert::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& request = *ev->Get();
    ctx.Register(CreateDeferredDestinationUpsertActor(ev->Sender, {
        .IntPublicationId = request.IntPublicationId,
        .TopicPath = request.TopicPath,
        .Database = request.Database,
        .PartitionId = request.PartitionId,
        .TabletId = request.TabletId,
    }));
}

void TPartitionWriterCacheActor::ReplyTxWriterInitError(TCachedPartitionWriter& writer,
                                                       const TEvPartitionWriter::TEvInitResult& result,
                                                       const TActorContext& ctx)
{
    auto response = result.GetError().Response;
    if (const ui64 cookie = writer.FrontPendingCookie(); cookie != 0) {
        response.MutablePartitionResponse()->SetCookie(cookie);
    }

    ctx.Send(Owner, new TEvPartitionWriter::TEvWriteResponse(result.SessionId, result.TxId,
                                                             EErrorCode::InternalError, result.GetError().Reason,
                                                             std::move(response)));
    writer.InitErrorReported = true;
}

void TPartitionWriterCacheActor::Handle(TEvPartitionWriter::TEvInitResult::TPtr& ev, const TActorContext& ctx)
{
    auto& result = *ev->Get();

    auto key = std::make_pair(result.SessionId, result.TxId);
    auto p = Writers.find(key);
    AFL_ENSURE(p != Writers.end());

    if (result.IsSuccess()) {
        p->second->OnEvInitResult(ev);
    } else if (result.SessionId || result.TxId) {
        // InitResult is not forwarded for tx-writers; keep the original error
        // (UNKNOWN_TXID / INITIALIZING) so the client does not retry UNAVAILABLE.
        ReplyTxWriterInitError(*p->second, result, ctx);
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
    AFL_ENSURE(queue.Expected != Max<ui64>());

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
        queue.Events.try_emplace(cookie, std::unique_ptr<TEvent>(event));
    }
}

void TPartitionWriterCacheActor::Handle(TEvPartitionWriter::TEvWriteAccepted::TPtr& ev, const TActorContext& ctx)
{
    const auto& result = *ev->Get();

    auto key = std::make_pair(result.SessionId, result.TxId);
    auto p = Writers.find(key);
    AFL_ENSURE(p != Writers.end());

    if (result.Cookie == p->second->SentRequests.front().Cookie) {
        p->second->OnWriteAccepted(result, ctx);

        TryForwardToOwner(ev->Release().Release(), PendingWriteAccepted,
                          result.Cookie,
                          ctx);
    } else {
        ReplyError(result.SessionId, result.TxId,
                   EErrorCode::InternalError, "out of order reserve bytes response from server, may be previous is lost",
                   p->second->SentRequests.front().Cookie);
        this->Become(&TPartitionWriterCacheActor::StateBroken);
    }
}

void TPartitionWriterCacheActor::Handle(TEvPartitionWriter::TEvWriteResponse::TPtr& ev, const TActorContext& ctx)
{
    auto& result = *ev->Get();

    auto key = std::make_pair(result.SessionId, result.TxId);
    auto p = Writers.find(key);
    AFL_ENSURE(p != Writers.end());

    if (result.IsSuccess()) {
        ui64 cookie = result.Record.GetPartitionResponse().GetCookie();
        if (cookie == p->second->AcceptedRequests.front().Cookie) {
            p->second->OnWriteResponse(result);

            TryForwardToOwner(ev->Release().Release(), PendingWriteResponse,
                              cookie,
                              ctx);
        } else {
            ReplyError(result.SessionId, result.TxId,
                       EErrorCode::InternalError, "out of order write response from server, may be previous is lost",
                       p->second->AcceptedRequests.front().Cookie);
            this->Become(&TPartitionWriterCacheActor::StateBroken);
        }
    } else if (!p->second->InitErrorReported) {
        ctx.Send(Owner, ev->Release().Release());
    }
}

void TPartitionWriterCacheActor::Handle(TEvPartitionWriter::TEvDisconnected::TPtr& ev, const TActorContext& ctx)
{
    ctx.Send(Owner, ev->Release().Release());
}

void TPartitionWriterCacheActor::Handle(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx)
{
    Y_UNUSED(ev);
    Die(ctx);
}

auto TPartitionWriterCacheActor::GetPartitionWriter(
    const TString& sessionId,
    const TString& txId,
    const std::optional<TDeferredPublishWriterOpts>& deferredPublish,
    const TActorContext& ctx) -> TCachedPartitionWriter*
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

    RegisterPartitionWriter(sessionId, txId, deferredPublish, ctx);

    p = Writers.find(key);
    AFL_ENSURE(p != Writers.end());

    return p->second.get();
}

bool TPartitionWriterCacheActor::TryDeleteOldestWriter(const TActorContext& ctx)
{
    AFL_ENSURE(!Writers.empty());

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

TActorId TPartitionWriterCacheActor::CreatePartitionWriter(
    const TString& sessionId,
    const TString& txId,
    const std::optional<TDeferredPublishWriterOpts>& deferredPublish,
    const TActorContext& ctx)
{
    TPartitionWriterOpts opts = Opts;
    if (deferredPublish) {
        opts.WithDeferredPublish(deferredPublish->IntPublicationId, deferredPublish->ExtPublicationId);
        opts.WithTxId(txId);
    } else if (sessionId && txId) {
        opts.WithSessionId(sessionId);
        opts.WithTxId(txId);
    }

    return ctx.RegisterWithSameMailbox(::NKikimr::NPQ::CreatePartitionWriter(
        ctx.SelfID, TabletId, Partition, opts
    ));
}

STFUNC(TPartitionWriterCacheActor::StateBroken)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvPartitionWriter::TEvTxWriteRequest, HandleOnBroken);
        IgnoreFunc(TEvPartitionWriter::TEvRequestDeferredDestinationUpsert);
        IgnoreFunc(TEvPartitionWriter::TEvInitResult);
        IgnoreFunc(TEvPartitionWriter::TEvWriteAccepted);
        IgnoreFunc(TEvPartitionWriter::TEvWriteResponse);
        IgnoreFunc(TEvPartitionWriter::TEvDisconnected);
        HFunc(TEvents::TEvPoison, Handle);
    }
}

NActors::IActor* CreatePartitionWriterCacheActor(
    const NActors::TActorId& owner,
    ui32 partition,
    ui64 tabletId,
    const TPartitionWriterOpts& opts)
{
    return new TPartitionWriterCacheActor(owner, partition, tabletId, opts);
}

} // namespace NKikimr::NPQ::NDataplane::NWrite

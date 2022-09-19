#include "add_offsets_to_transaction_actor.h"

namespace NKikimr::NGRpcService {

TAddOffsetsToTransactionActor::TAddOffsetsToTransactionActor(IRequestOpCtx* request)
    : TBase{request}
{
}

void TAddOffsetsToTransactionActor::Bootstrap(const NActors::TActorContext& ctx)
{
    TBase::Bootstrap(ctx);
    Become(&TAddOffsetsToTransactionActor::StateWork);
    Proceed(ctx);
}

void TAddOffsetsToTransactionActor::Proceed(const NActors::TActorContext& ctx)
{
    const auto req = GetProtoRequest();

    auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
    SetAuthToken(ev, *Request_);
    SetDatabase(ev, *Request_);

    NYql::TIssues issues;
    if (CheckSession(req->session_id(), issues)) {
        ev->Record.MutableRequest()->SetSessionId(req->session_id());
    } else {
        return Reply(Ydb::StatusIds::BAD_REQUEST, issues, ctx);
    }

    ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_UNDEFINED);
    ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_TOPIC);

    if (const auto traceId = Request_->GetTraceId(); traceId) {
        ev->Record.SetTraceId(traceId.GetRef());
    }

    if (const auto requestType = Request_->GetRequestType(); requestType) {
        ev->Record.SetRequestType(requestType.GetRef());
    }

    ev->Record.MutableRequest()->SetCancelAfterMs(GetCancelAfter().MilliSeconds());
    ev->Record.MutableRequest()->SetTimeoutMs(GetOperationTimeout().MilliSeconds());

    if (!req->has_tx_control()) {
        NYql::TIssues issues;
        issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Empty tx_control."));
        return Reply(Ydb::StatusIds::BAD_REQUEST, issues, ctx);
    }

    //
    // TODO: проверить комбинацию значений атрибутов tx_control
    //

    ev->Record.MutableRequest()->MutableTxControl()->CopyFrom(req->tx_control());
    ev->Record.MutableRequest()->MutableTopicOperations()->SetConsumer(req->consumer());
    *ev->Record.MutableRequest()->MutableTopicOperations()->MutableTopics() = req->topics();

    ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());
}

void TAddOffsetsToTransactionActor::Handle(const NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const NActors::TActorContext& ctx)
{
    const auto& record = ev->Get()->Record.GetRef();
    SetCost(record.GetConsumedRu());
    AddServerHintsIfAny(record);

    if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
        const auto& kqpResponse = record.GetResponse();
        const auto& issueMessage = kqpResponse.GetQueryIssues();
        auto queryResult = TEvAddOffsetsToTransactionRequest::AllocateResult<Ydb::Topic::AddOffsetsToTransactionResult>(Request_);

        //
        // TODO: сохранить результат
        //

        //
        // TODO: статистика
        //

        //
        // TODO: tx_meta
        //

        ReplyWithResult(Ydb::StatusIds::SUCCESS, issueMessage, *queryResult, ctx);
    } else {
        OnGenericQueryResponseError(record, ctx);
    }
}

void DoAddOffsetsToTransaction(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider &)
{
    TActivationContext::AsActorContext().Register(new TAddOffsetsToTransactionActor(p.release()));
}

}

#include "update_offsets_in_transaction_actor.h"
#include <ydb/core/base/feature_flags.h>


namespace NKikimr::NGRpcService {

TUpdateOffsetsInTransactionActor::TUpdateOffsetsInTransactionActor(IRequestOpCtx* request)
    : TBase{request}
{
}

void TUpdateOffsetsInTransactionActor::Bootstrap(const NActors::TActorContext& ctx)
{
    TBase::Bootstrap(ctx);
    Become(&TUpdateOffsetsInTransactionActor::StateWork);
    Proceed(ctx);
}

void TUpdateOffsetsInTransactionActor::Proceed(const NActors::TActorContext& ctx)
{
    if (!AppData(ctx)->FeatureFlags.GetEnableTopicServiceTx()) {
        return Reply(Ydb::StatusIds::UNSUPPORTED,
                     "Disabled transaction support for TopicService.",
                     NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                     ctx);
    }

    const auto req = GetProtoRequest();

    if (!req->has_tx()) {
        return Reply(Ydb::StatusIds::BAD_REQUEST,
                     "Empty tx.",
                     NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                     ctx);
    }

    auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
    SetAuthToken(ev, *Request_);
    SetDatabase(ev, *Request_);

    if (CheckSession(req->tx().session(), Request_.get())) {
        ev->Record.MutableRequest()->SetSessionId(req->tx().session());
    } else {
        return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
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

    ev->Record.MutableRequest()->MutableTxControl()->set_tx_id(req->tx().id());

    auto* topicOperations = ev->Record.MutableRequest()->MutableTopicOperations();
    if (!req->consumer().empty()) {
        topicOperations->SetConsumer(req->consumer());
    }
    *topicOperations->MutableTopics() = req->topics();

    ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());
}

void TUpdateOffsetsInTransactionActor::Handle(const NKqp::TEvKqp::TEvQueryResponse::TPtr& ev,
                                              const NActors::TActorContext& ctx)
{
    const auto& record = ev->Get()->Record.GetRef();
    SetCost(record.GetConsumedRu());
    AddServerHintsIfAny(record);

    if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
        const auto& kqpResponse = record.GetResponse();
        const auto& issueMessage = kqpResponse.GetQueryIssues();
        auto queryResult = TEvUpdateOffsetsInTransactionRequest::AllocateResult<Ydb::Topic::UpdateOffsetsInTransactionResult>(Request_);

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

}

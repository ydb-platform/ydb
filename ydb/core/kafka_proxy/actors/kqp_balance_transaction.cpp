#include "kqp_balance_transaction.h"
#include "ydb/core/kqp/common/simple/services.h"
#include "kafka_consumer_groups_metadata_initializers.h"
#include "kafka_consumer_members_metadata_initializers.h"


namespace NKikimr::NGRpcProxy::V1 {

TKqpTxHelper::TKqpTxHelper(TString database)
    : DataBase(database)
{}

void TKqpTxHelper::SendCreateSessionRequest(const TActorContext& ctx) {
    auto ev = MakeCreateSessionRequest();
    ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release(), 0, 0);
}

void TKqpTxHelper::BeginTransaction(ui64 cookie, const NActors::TActorContext& ctx) {
    auto begin = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();

    begin->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_BEGIN_TX);
    begin->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
    begin->Record.MutableRequest()->SetSessionId(KqpSessionId);
    begin->Record.MutableRequest()->SetDatabase(DataBase);

    ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), begin.Release(), 0, cookie);
}

bool TKqpTxHelper::HandleCreateSessionResponse(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext&) {
    const auto& record = ev->Get()->Record;

    if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
        return false;
    }

    KqpSessionId = record.GetResponse().GetSessionId();
    Y_ABORT_UNLESS(!KqpSessionId.empty());

    return true;
}

void TKqpTxHelper::CloseKqpSession(const TActorContext& ctx) {
    if (KqpSessionId) {
        auto ev = MakeCloseSessionRequest();
        ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release(), 0, 0);
        KqpSessionId = "";
    }
}

THolder<NKqp::TEvKqp::TEvCreateSessionRequest> TKqpTxHelper::MakeCreateSessionRequest() {
    auto ev = MakeHolder<NKqp::TEvKqp::TEvCreateSessionRequest>();
    ev->Record.MutableRequest()->SetDatabase(DataBase);
    return ev;
}

THolder<NKqp::TEvKqp::TEvCloseSessionRequest> TKqpTxHelper::MakeCloseSessionRequest() {
    auto ev = MakeHolder<NKqp::TEvKqp::TEvCloseSessionRequest>();
    ev->Record.MutableRequest()->SetSessionId(KqpSessionId);
    return ev;
}

void TKqpTxHelper::SendRequest(THolder<NKqp::TEvKqp::TEvQueryRequest> request, ui64 cookie, const NActors::TActorContext& ctx) {
    ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), request.Release(), 0, cookie);
}

void TKqpTxHelper::SendYqlRequest(const TString& yqlRequest, NYdb::TParams sqlParams, ui64 cookie, const NActors::TActorContext& ctx, bool commit) {
    auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();

    ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
    ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
    ev->Record.MutableRequest()->SetQuery(yqlRequest);
    ev->Record.MutableRequest()->SetDatabase(DataBase);
    ev->Record.MutableRequest()->SetSessionId(KqpSessionId);
    ev->Record.MutableRequest()->MutableTxControl()->set_commit_tx(commit);
    if (!TxId.empty()) {
        ev->Record.MutableRequest()->MutableTxControl()->set_tx_id(TxId);
    } else {
        ev->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
    }
    ev->Record.MutableRequest()->SetUsePublicResponseDataFormat(true);
    ev->Record.MutableRequest()->MutableQueryCachePolicy()->set_keep_in_cache(true);

    ev->Record.MutableRequest()->MutableYdbParameters()->swap(*(NYdb::TProtoAccessor::GetProtoMapPtr(sqlParams)));
    ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release(), 0, cookie);
}

void TKqpTxHelper::CommitTx(ui64 cookie, const NActors::TActorContext& ctx) {
    auto commit = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();

    commit->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_COMMIT_TX);
    commit->Record.MutableRequest()->MutableTxControl()->set_tx_id(TxId);
    commit->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);
    commit->Record.MutableRequest()->SetSessionId(KqpSessionId);
    commit->Record.MutableRequest()->SetDatabase(DataBase);

    ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), commit.Release(), 0, cookie);
}

void TKqpTxHelper::SendInitTablesRequest(const TActorContext& ctx) {
    ctx.Send(
        NMetadata::NProvider::MakeServiceId(ctx.SelfID.NodeId()),
        new NMetadata::NProvider::TEvPrepareManager(NGRpcProxy::V1::TKafkaConsumerMembersMetaInitManager::GetInstant())
    );

    ctx.Send(
        NMetadata::NProvider::MakeServiceId(ctx.SelfID.NodeId()),
        new NMetadata::NProvider::TEvPrepareManager(NGRpcProxy::V1::TKafkaConsumerGroupsMetaInitManager::GetInstant())
    );
}

void TKqpTxHelper::SetTxId(const TString& txId) {
    TxId = txId;
}

void TKqpTxHelper::ResetTxId() {
    TxId = "";
}

}  // namespace NKikimr::NGRpcProxy::V1

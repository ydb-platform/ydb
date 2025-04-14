#include "distributed_commit_helper.h"
#include "ydb/core/kqp/common/simple/services.h"

namespace NKikimr::NGRpcProxy::V1 {

TDistributedCommitHelper::TDistributedCommitHelper(TString database, TString consumer, TString path, std::vector<TCommitInfo> commits, ui64 cookie)
    : DataBase(database)
    , Consumer(consumer)
    , Path(path)
    , Commits(std::move(commits))
    , Step(BEGIN_TRANSACTION_SENDED)
    , Cookie(cookie)
{}

TDistributedCommitHelper::ECurrentStep TDistributedCommitHelper::Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
    switch (Step) {
        case BEGIN_TRANSACTION_SENDED:
            Step = OFFSETS_SENDED;
            SendCommits(ev, ctx);
            break;
        case OFFSETS_SENDED:
            Step = COMMIT_SENDED;
            CommitTx(ctx);
            break;
        case COMMIT_SENDED:
            Step = DONE;
            CloseKqpSession(ctx);
            break;
        case DONE:
            break;
    }
    return Step;
}

void TDistributedCommitHelper::SendCreateSessionRequest(const TActorContext& ctx) {
    auto ev = MakeCreateSessionRequest();
    ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release(), 0, Cookie);
}

void TDistributedCommitHelper::BeginTransaction(const NActors::TActorContext& ctx) {
    auto begin = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();

    begin->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_BEGIN_TX);
    begin->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
    begin->Record.MutableRequest()->SetSessionId(KqpSessionId);
    begin->Record.MutableRequest()->SetDatabase(DataBase);

    ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), begin.Release(), 0, Cookie);
}

bool TDistributedCommitHelper::Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;

    if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
        return false;
    }

    KqpSessionId = record.GetResponse().GetSessionId();
    Y_ABORT_UNLESS(!KqpSessionId.empty());
    BeginTransaction(ctx);
    return true;
}

void TDistributedCommitHelper::CloseKqpSession(const TActorContext& ctx) {
    if (KqpSessionId) {
        auto ev = MakeCloseSessionRequest();
        ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release(), 0, Cookie);
        KqpSessionId = "";
    }
}

THolder<NKqp::TEvKqp::TEvCreateSessionRequest> TDistributedCommitHelper::MakeCreateSessionRequest() {
    auto ev = MakeHolder<NKqp::TEvKqp::TEvCreateSessionRequest>();
    ev->Record.MutableRequest()->SetDatabase(DataBase);
    return ev;
}

THolder<NKqp::TEvKqp::TEvCloseSessionRequest> TDistributedCommitHelper::MakeCloseSessionRequest() {
    auto ev = MakeHolder<NKqp::TEvKqp::TEvCloseSessionRequest>();
    ev->Record.MutableRequest()->SetSessionId(KqpSessionId);
    return ev;
}

void TDistributedCommitHelper::SendCommits(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const NActors::TActorContext& ctx) {
    auto& record = ev->Get()->Record;
    TxId = record.GetResponse().GetTxMeta().id();
    Y_ABORT_UNLESS(!TxId.empty());

    auto offsets = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
    offsets->Record.MutableRequest()->SetDatabase(DataBase);
    offsets->Record.MutableRequest()->SetSessionId(KqpSessionId);
    offsets->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_UNDEFINED);
    offsets->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_TOPIC);
    offsets->Record.MutableRequest()->MutableTxControl()->set_tx_id(TxId);
    offsets->Record.MutableRequest()->MutableTopicOperations()->SetConsumer(Consumer);

    auto* topic = offsets->Record.MutableRequest()->MutableTopicOperations()->AddTopics();
    topic->set_path(Path);

    for(auto &commit: Commits) {
        auto* partition = topic->add_partitions();
        partition->set_partition_id(commit.PartitionId);
        partition->set_force_commit(true);
        partition->set_kill_read_session(commit.KillReadSession);
        partition->set_only_check_commited_to_finish(commit.OnlyCheckCommitedToFinish);
        partition->set_read_session_id(commit.ReadSessionId);
        auto* offset = partition->add_partition_offsets();
        offset->set_end(commit.Offset);
    }

    ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), offsets.Release(), 0, Cookie);
}

void TDistributedCommitHelper::CommitTx(const NActors::TActorContext& ctx) {
    auto commit = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();

    commit->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_COMMIT_TX);
    commit->Record.MutableRequest()->MutableTxControl()->set_tx_id(TxId);
    commit->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);
    commit->Record.MutableRequest()->SetSessionId(KqpSessionId);
    commit->Record.MutableRequest()->SetDatabase(DataBase);

    ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), commit.Release(), 0, Cookie);
}

}  // namespace NKikimr::NGRpcProxy::V1

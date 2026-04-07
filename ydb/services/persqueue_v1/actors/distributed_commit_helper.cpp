#include "distributed_commit_helper.h"
#include "ydb/core/kqp/common/simple/services.h"
#include "ydb/services/lib/actors/pq_schema_actor.h"

namespace NKikimr::NGRpcProxy::V1 {

TDistributedCommitHelper::TDistributedCommitHelper(TString database, TString consumer, std::vector<TCommitInfo> commits, ui64 cookie, std::optional<GenerationIdCheckerSettings> generationCheckerSettings)
    : DataBase(database)
    , Consumer(consumer)
    , Commits(std::move(commits))
    , Step(BEGIN_TRANSACTION_SENDED)
    , Cookie(cookie)
    , CheckerSettings(generationCheckerSettings)
{}

TDistributedCommitHelper::ECurrentStep TDistributedCommitHelper::Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
    switch (Step) {
        case BEGIN_TRANSACTION_SENDED:
            if (CheckerSettings.has_value()) {
                Step = CHECK_GENERATION;
                RetrieveGeneration(ev, ctx);
            } else {
                Step = OFFSETS_SENDED;
                SendCommits(ev, ctx);
            }
            break;
        case CHECK_GENERATION:
            CompareGenerations(ev, ctx);
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

void TDistributedCommitHelper::CompareGenerations(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const NActors::TActorContext& ctx) {
    auto& record = ev->Get()->Record;
    auto& resp = record.GetResponse();

    auto createErrorResponse = [](Ydb::StatusIds_StatusCode status, TString errorMessage) {
        auto queryResponse = MakeHolder<NKqp::TEvKqp::TEvQueryResponse>();
        auto* response = queryResponse->Record.MutableResponse();
        queryResponse->Record.SetYdbStatus(status);
        NYql::TIssues issues;
        issues.AddIssue(FillIssue(errorMessage, Ydb::PersQueue::ErrorCode::ErrorCode::GENERATION_MISMATCH));
        NYql::IssuesToMessage(issues, response->MutableQueryIssues());
        return queryResponse;
    };

    AFL_ENSURE(record.GetYdbStatus() == Ydb::StatusIds::SUCCESS)("Status", record.GetYdbStatus());

    if (resp.GetYdbResults().empty()) {
        TString errorMessage = "Incorrect consumer group generation";
        ctx.Send(ctx.SelfID, createErrorResponse(Ydb::StatusIds_StatusCode_PRECONDITION_FAILED, errorMessage).Release());
        return;
    }

    NYdb::TResultSetParser parser(resp.GetYdbResults(0));
    if (!parser.TryNextRow()) {
        TString errorMessage = "Incorrect consumer group generation";
        ctx.Send(ctx.SelfID, createErrorResponse(Ydb::StatusIds_StatusCode_PRECONDITION_FAILED, errorMessage).Release());
        return;
    }

    ui64 Generation = parser.ColumnParser("generation").GetUint64();
    if (Generation != CheckerSettings->GenerationId) {
        TString errorMessage = TStringBuilder() << "Consumer group generation is outdated. Group generation=" << Generation << ", but consumer has generation=" << CheckerSettings->GenerationId;
        ctx.Send(ctx.SelfID, createErrorResponse(Ydb::StatusIds_StatusCode_PRECONDITION_FAILED, errorMessage).Release());
        return;
    }
    Step = OFFSETS_SENDED;
    SendCommits(ev, ctx);
}

void TDistributedCommitHelper::RetrieveGeneration(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const NActors::TActorContext& ctx) {
    auto& record = ev->Get()->Record;
    TxId = record.GetResponse().GetTxMeta().id();
    Y_ABORT_UNLESS(!TxId.empty());

    NYdb::TParamsBuilder params;
    params.AddParam("$ConsumerGroup").Utf8(Consumer).Build();
    params.AddParam("$Database").Utf8(CheckerSettings->TopicDatabasePath).Build();
    NYdb::TParams sqlParams = params.Build();

    auto check = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
    check->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
    check->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
    check->Record.MutableRequest()->SetSessionId(KqpSessionId);
    check->Record.MutableRequest()->SetDatabase(DataBase);
    check->Record.MutableRequest()->MutableTxControl()->set_tx_id(TxId);
    check->Record.MutableRequest()->SetQuery(Sprintf(CHECK_GROUP_GENERATION_ID.c_str(),
                        CheckerSettings->ConsumerMetadataTablePath.c_str()));
    check->Record.MutableRequest()->MutableYdbParameters()->swap(*(NYdb::TProtoAccessor::GetProtoMapPtr(sqlParams)));
    Step = CHECK_GENERATION;
    ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), check.Release(), 0, Cookie);
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

    THashMap<TString, TVector<TCommitInfo*>> commitsByTopic;
    for (auto& commit : Commits) {
        commitsByTopic[commit.TopicPath].push_back(&commit);
    }

    for (const auto& [topicPath, topicCommits] : commitsByTopic) {
        auto* topic = offsets->Record.MutableRequest()->MutableTopicOperations()->AddTopics();
        topic->set_path(topicPath);

        for (auto* commit : topicCommits) {
            auto* partition = topic->add_partitions();
            partition->set_partition_id(commit->PartitionId);
            partition->set_force_commit(true);
            partition->set_kill_read_session(commit->KillReadSession);
            partition->set_only_check_commited_to_finish(commit->OnlyCheckCommitedToFinish);
            partition->set_read_session_id(commit->ReadSessionId);
            auto* offset = partition->add_partition_offsets();
            offset->set_end(commit->Offset);
        }
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

#include "finalize_publication_actor.h"

#include "delete_publication_query.h"
#include "destination_blob.h"
#include "list_destinations_query.h"
#include "tables_creator.h"

#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <yql/essentials/public/issue/yql_issue.h>

namespace NKikimr::NPQ::NDeferredPublish {

namespace {

using namespace NKikimr::NKqp;

TString MakeDeletePublicationSql() {
    return TStringBuilder() << R"(
        -- TFinalizePublicationActor::MakeDeletePublicationSql
        DECLARE $int_publication_id AS Uint64;

        DELETE FROM `)" << DestinationsTablePath() << R"(`
        WHERE `int_publication_id` = $int_publication_id;

        DELETE FROM `)" << PublicationsTablePath() << R"(` 
        WHERE `int_publication_id` = $int_publication_id
        RETURNING `int_publication_id`;
    )";
}

NKikimrKqp::TTopicDeferredPublicationRequest::EOp MapFinalizeOp(EFinalizePublicationOp op) {
    switch (op) {
        case EFinalizePublicationOp::Publish:
            return NKikimrKqp::TTopicDeferredPublicationRequest::Publish;
        case EFinalizePublicationOp::Cancel:
            return NKikimrKqp::TTopicDeferredPublicationRequest::Cancel;
    }
}

bool BuildDeferredPublicationRequest(
    const TListDestinationsData& data,
    EFinalizePublicationOp op,
    ui64 intPublicationId,
    NKikimrKqp::TTopicDeferredPublicationRequest* request,
    NYql::TIssues* issues)
{
    Y_ABORT_UNLESS(request != nullptr);
    Y_ABORT_UNLESS(issues != nullptr);

    request->Clear();
    request->SetOp(MapFinalizeOp(op));
    request->SetIntPublicationId(intPublicationId);
    request->SetExtPublicationId(data.ExtPublicationId);

    for (const auto& row : data.Destinations) {
        NKikimrPQ::TDeferredPublishDestinationBlob blob;
        if (!row.DestinationBlob.empty() && !ParseDestinationBlob(row.DestinationBlob, &blob)) {
            issues->AddIssue("Invalid destination blob");
            return false;
        }

        if (blob.PartitionsSize() == 0) {
            issues->AddIssue(TStringBuilder() << "Destination blob for path '" << row.Path << "' has no partitions");
            return false;
        }

        for (const auto& partition : blob.GetPartitions()) {
            if (!partition.HasPartitionId() || !partition.HasTabletId()) {
                issues->AddIssue(TStringBuilder() << "Destination blob for path '" << row.Path << "' is incomplete");
                return false;
            }

            auto* destination = request->AddDestinations();
            destination->SetPath(row.Path);
            destination->SetPartitionId(partition.GetPartitionId());
            destination->SetTabletId(partition.GetTabletId());
        }
    }

    if (request->GetDestinations().empty()) {
        issues->AddIssue("Deferred publication has no partition destinations");
        return false;
    }

    return true;
}

class TFinalizePublicationActor : public NActors::TActorBootstrapped<TFinalizePublicationActor> {
    enum class EStep {
        ListDestinations,
        DeleteOnly,
        KqpCreateSession,
        KqpBeginTx,
        KqpDelete,
        KqpTopicOp,
        KqpCommit,
    };

public:
    TFinalizePublicationActor(
        const NActors::TActorId& replyTo,
        TString database,
        ui64 intPublicationId,
        EFinalizePublicationOp op,
        TString userToken)
        : ReplyTo(replyTo)
        , Database(std::move(database))
        , IntPublicationId(intPublicationId)
        , Op(op)
        , UserToken(std::move(userToken))
    {}

    void StartListDestinations() {
        Register(CreateListDestinationsQueryActor(SelfId(), Database, IntPublicationId));
        Step = EStep::ListDestinations;
        Become(&TFinalizePublicationActor::StateListDestinations);
    }

    void Bootstrap() {
        StartListDestinations();
    }

private:
    void ReplyAndPassAway(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues = {}) {
        CloseKqpSession();
        auto* response = new TEvFinalizePublicationResponse;
        response->Status = status;
        response->Issues = issues;
        Send(ReplyTo, response);
        PassAway();
    }

    void CloseKqpSession() {
        if (KqpSessionId.empty()) {
            return;
        }

        auto ev = MakeHolder<TEvKqp::TEvCloseSessionRequest>();
        ev->Record.MutableRequest()->SetSessionId(KqpSessionId);
        Send(MakeKqpProxyID(SelfId().NodeId()), ev.Release());
        KqpSessionId.clear();
    }

    void StartDeleteOnly() {
        Register(CreateDeletePublicationQueryActor(SelfId(), Database, IntPublicationId));
        Step = EStep::DeleteOnly;
        Become(&TFinalizePublicationActor::StateDeleteOnly);
    }

    void SetCreateSessionIdentity(TEvKqp::TEvCreateSessionRequest& ev) {
        if (!UserToken.empty()) {
            NACLibProto::TUserToken token;
            if (token.ParseFromString(UserToken)) {
                ev.Record.SetUserSID(token.GetUserSID());
            }
        }
    }

    void StartKqpSession() {
        auto ev = MakeHolder<TEvKqp::TEvCreateSessionRequest>();
        ev->Record.MutableRequest()->SetDatabase(Database);
        SetCreateSessionIdentity(*ev);
        Send(MakeKqpProxyID(SelfId().NodeId()), ev.Release());
        Step = EStep::KqpCreateSession;
        Become(&TFinalizePublicationActor::StateKqp);
    }

    void SendKqpBeginTx() {
        auto ev = MakeHolder<TEvKqp::TEvQueryRequest>();
        if (!UserToken.empty()) {
            ev->Record.SetUserToken(UserToken);
        }
        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_BEGIN_TX);
        ev->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
        ev->Record.MutableRequest()->SetSessionId(KqpSessionId);
        ev->Record.MutableRequest()->SetDatabase(Database);
        Send(MakeKqpProxyID(SelfId().NodeId()), ev.Release());
        Step = EStep::KqpBeginTx;
    }

    void SendKqpDelete() {
        NYdb::TParamsBuilder params;
        params
            .AddParam("$int_publication_id")
                .Uint64(IntPublicationId)
                .Build();

        auto ev = MakeHolder<TEvKqp::TEvQueryRequest>();
        if (!UserToken.empty()) {
            ev->Record.SetUserToken(UserToken);
        }
        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        ev->Record.MutableRequest()->SetQuery(MakeDeletePublicationSql());
        ev->Record.MutableRequest()->SetDatabase(Database);
        ev->Record.MutableRequest()->SetSessionId(KqpSessionId);
        ev->Record.MutableRequest()->MutableTxControl()->set_commit_tx(false);
        ev->Record.MutableRequest()->MutableTxControl()->set_tx_id(TxId);
        ev->Record.MutableRequest()->SetUsePublicResponseDataFormat(true);
        NYdb::TParams builtParams = params.Build();
        ev->Record.MutableRequest()->MutableYdbParameters()->swap(
            *(NYdb::TProtoAccessor::GetProtoMapPtr(builtParams)));

        Send(MakeKqpProxyID(SelfId().NodeId()), ev.Release());
        Step = EStep::KqpDelete;
    }

    void SendKqpDeferredPublication() {
        auto ev = MakeHolder<TEvKqp::TEvQueryRequest>();
        if (!UserToken.empty()) {
            ev->Record.SetUserToken(UserToken);
        }
        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_UNDEFINED);
        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_TOPIC);
        ev->Record.MutableRequest()->SetDatabase(Database);
        ev->Record.MutableRequest()->SetSessionId(KqpSessionId);
        ev->Record.MutableRequest()->MutableTxControl()->set_tx_id(TxId);
        ev->Record.MutableRequest()->SetUsePublicResponseDataFormat(true);
        *ev->Record.MutableRequest()->MutableDeferredPublication() = DeferredPublicationRequest;

        Send(MakeKqpProxyID(SelfId().NodeId()), ev.Release());
        Step = EStep::KqpTopicOp;
    }

    void SendKqpCommit() {
        auto ev = MakeHolder<TEvKqp::TEvQueryRequest>();
        if (!UserToken.empty()) {
            ev->Record.SetUserToken(UserToken);
        }
        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_COMMIT_TX);
        ev->Record.MutableRequest()->MutableTxControl()->set_tx_id(TxId);
        ev->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);
        ev->Record.MutableRequest()->SetSessionId(KqpSessionId);
        ev->Record.MutableRequest()->SetDatabase(Database);
        Send(MakeKqpProxyID(SelfId().NodeId()), ev.Release());
        Step = EStep::KqpCommit;
    }

    static NYql::TIssues IssuesFromKqpResponse(const NKikimrKqp::TEvQueryResponse& record) {
        NYql::TIssues issues;
        for (const auto& issue : record.GetResponse().GetQueryIssues()) {
            issues.AddIssue(NYql::IssueFromMessage(issue));
        }
        if (issues.Empty()) {
            issues.AddIssue("Finalize publication failed");
        }
        return issues;
    }

    void HandleListDestinationsResponse(TEvListDestinationsResponse::TPtr& ev) {
        const auto& response = *ev->Get();
        if (response.Status != Ydb::StatusIds::SUCCESS) {
            return ReplyAndPassAway(response.Status, response.Issues);
        }

        Y_ABORT_UNLESS(response.Data.Defined());
        ListDestinationsData = *response.Data;

        if (ListDestinationsData->Destinations.empty()) {
            if (Op == EFinalizePublicationOp::Cancel) {
                return StartDeleteOnly();
            }

            NYql::TIssues issues;
            issues.AddIssue("Publication has no destinations");
            return ReplyAndPassAway(Ydb::StatusIds::ABORTED, issues);
        }

        NYql::TIssues issues;
        if (!BuildDeferredPublicationRequest(
                *ListDestinationsData,
                Op,
                IntPublicationId,
                &DeferredPublicationRequest,
                &issues)) {
            return ReplyAndPassAway(Ydb::StatusIds::BAD_REQUEST, issues);
        }

        StartKqpSession();
    }

    void HandleDeleteOnlyResponse(TEvDeletePublicationResponse::TPtr& ev) {
        ReplyAndPassAway(ev->Get()->Status, ev->Get()->Issues);
    }

    void HandleKqpCreateSessionResponse(TEvKqp::TEvCreateSessionResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
            NYql::TIssues issues;
            issues.AddIssue("Failed to create KQP session");
            return ReplyAndPassAway(record.GetYdbStatus(), issues);
        }

        KqpSessionId = record.GetResponse().GetSessionId();
        Y_ABORT_UNLESS(!KqpSessionId.empty());
        SendKqpBeginTx();
    }

    void HandleKqpQueryResponse(TEvKqp::TEvQueryResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
            return ReplyAndPassAway(record.GetYdbStatus(), IssuesFromKqpResponse(record));
        }

        switch (Step) {
            case EStep::KqpBeginTx: {
                TxId = record.GetResponse().GetTxMeta().id();
                Y_ABORT_UNLESS(!TxId.empty());
                SendKqpDelete();
                return;
            }
            case EStep::KqpDelete:
                SendKqpDeferredPublication();
                return;
            case EStep::KqpTopicOp:
                SendKqpCommit();
                return;
            case EStep::KqpCommit:
                ReplyAndPassAway(Ydb::StatusIds::SUCCESS);
                return;
            default:
                return ReplyAndPassAway(Ydb::StatusIds::INTERNAL_ERROR);
        }
    }

    STFUNC(StateListDestinations) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvListDestinationsResponse, HandleListDestinationsResponse);
            default:
                break;
        }
    }

    STFUNC(StateDeleteOnly) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDeletePublicationResponse, HandleDeleteOnlyResponse);
            default:
                break;
        }
    }

    STFUNC(StateKqp) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKqp::TEvCreateSessionResponse, HandleKqpCreateSessionResponse);
            hFunc(TEvKqp::TEvQueryResponse, HandleKqpQueryResponse);
            default:
                break;
        }
    }

    const NActors::TActorId ReplyTo;
    const TString Database;
    const ui64 IntPublicationId;
    const EFinalizePublicationOp Op;
    const TString UserToken;

    TMaybe<TListDestinationsData> ListDestinationsData;
    NKikimrKqp::TTopicDeferredPublicationRequest DeferredPublicationRequest;
    TString KqpSessionId;
    TString TxId;
    EStep Step = EStep::ListDestinations;
};

} // namespace

NActors::IActor* CreateFinalizePublicationActor(
    const NActors::TActorId& replyTo,
    const TString& database,
    ui64 intPublicationId,
    EFinalizePublicationOp op,
    const TString& userToken)
{
    return new TFinalizePublicationActor(replyTo, database, intPublicationId, op, userToken);
}

} // namespace NKikimr::NPQ::NDeferredPublish

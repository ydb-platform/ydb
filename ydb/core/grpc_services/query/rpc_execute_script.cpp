#include "service_query.h"

#include <ydb/core/base/appdata.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/rpc_kqp_base.h>
#include <ydb/core/grpc_services/audit_dml_operations.h>
#include <ydb/core/grpc_services/grpc_integrity_trails.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/public/api/protos/ydb_query.pb.h>
#include <ydb/public/lib/operation_id/operation_id.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NGRpcService {

namespace {

using namespace NActors;

using TEvExecuteScriptRequest = TGrpcRequestNoOperationCall<Ydb::Query::ExecuteScriptRequest,
    Ydb::Operations::Operation>;

bool FillQueryContent(const Ydb::Query::ExecuteScriptRequest& req, NKikimrKqp::TEvQueryRequest& kqpRequest,
    NYql::TIssues& issues)
{
    if (!CheckQuery(req.script_content().text(), issues)) {
        return false;
    }

    kqpRequest.MutableRequest()->SetQuery(req.script_content().text());
    return true;
}

std::tuple<Ydb::StatusIds::StatusCode, NYql::TIssues> FillKqpRequest(
    const Ydb::Query::ExecuteScriptRequest& req, NKikimrKqp::TEvQueryRequest& kqpRequest)
{
    kqpRequest.MutableRequest()->MutableYdbParameters()->insert(req.parameters().begin(), req.parameters().end());

    switch (req.exec_mode()) {
        case Ydb::Query::EXEC_MODE_EXECUTE:
            kqpRequest.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
            break;
        case Ydb::Query::EXEC_MODE_EXPLAIN:
            kqpRequest.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXPLAIN);
            break;
        // TODO: other modes
        default: {
            NYql::TIssues issues;
            issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                            req.exec_mode() == Ydb::Query::EXEC_MODE_UNSPECIFIED ? "Query mode is not specified" : "Query mode is not supported yet"));
            return {Ydb::StatusIds::BAD_REQUEST, std::move(issues)};
        }
    }

    kqpRequest.MutableRequest()->SetCollectStats(GetCollectStatsMode(req.stats_mode()));
    kqpRequest.MutableRequest()->SetSyntax(req.script_content().syntax());

    kqpRequest.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT);
    kqpRequest.MutableRequest()->SetKeepSession(false);
    kqpRequest.MutableRequest()->SetPoolId(req.pool_id());

    kqpRequest.MutableRequest()->SetCancelAfterMs(GetDuration(req.operation_params().cancel_after()).MilliSeconds());
    kqpRequest.MutableRequest()->SetTimeoutMs(GetDuration(req.operation_params().operation_timeout()).MilliSeconds());

    NYql::TIssues issues;
    if (!FillQueryContent(req, kqpRequest, issues)) {
        return {Ydb::StatusIds::BAD_REQUEST, std::move(issues)};
    }

    return {Ydb::StatusIds::SUCCESS, {}};
}

class TExecuteScriptRPC : public TActorBootstrapped<TExecuteScriptRPC> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    TExecuteScriptRPC(TEvExecuteScriptRequest* request)
        : Request_(request)
    {}

    void Bootstrap() {
        NYql::TIssues issues;
        const auto& request = *Request_->GetProtoRequest();

        if (request.operation_params().operation_mode() == Ydb::Operations::OperationParams::SYNC) {
            issues.AddIssue("ExecuteScript must be asyncronous operation");
            return Reply(Ydb::StatusIds::BAD_REQUEST, issues);
        }

        AuditContextAppend(Request_.get(), request);
        NDataIntegrity::LogIntegrityTrails(Request_->GetTraceId(), request, TlsActivationContext->AsActorContext());

        Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS;
        if (auto scriptRequest = MakeScriptRequest(issues, status)) {
            if (Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), scriptRequest.Release())) {
                Become(&TExecuteScriptRPC::StateFunc);
            } else {
                issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Internal error"));
                Reply(Ydb::StatusIds::INTERNAL_ERROR, issues);
            }
        } else {
            Reply(status, issues);
        }
    }

private:
    STRICT_STFUNC(StateFunc,
        HFunc(NKqp::TEvKqp::TEvScriptResponse, Handle)
    )

    void Handle(NKqp::TEvKqp::TEvScriptResponse::TPtr& ev, const TActorContext& ctx) {
        NDataIntegrity::LogIntegrityTrails(Request_->GetTraceId(), *Request_->GetProtoRequest(), ev, ctx);

        Ydb::Operations::Operation operation;
        operation.set_id(ev->Get()->OperationId);
        Ydb::Query::ExecuteScriptMetadata metadata;
        metadata.set_execution_id(ev->Get()->ExecutionId);
        metadata.set_exec_status(ev->Get()->ExecStatus);
        metadata.set_exec_mode(ev->Get()->ExecMode);
        operation.mutable_metadata()->PackFrom(metadata);
        Reply(ev->Get()->Status, std::move(operation), ev->Get()->Issues);
    }

    THolder<NKqp::TEvKqp::TEvScriptRequest> MakeScriptRequest(NYql::TIssues& issues, Ydb::StatusIds::StatusCode& status) const {
        const auto* req = Request_->GetProtoRequest();
        const auto traceId = Request_->GetTraceId();

        auto ev = MakeHolder<NKqp::TEvKqp::TEvScriptRequest>();

        SetAuthToken(ev, *Request_);
        SetDatabase(ev, *Request_);
        SetRlPath(ev, *Request_);

        if (traceId) {
            ev->Record.SetTraceId(traceId.GetRef());
        }

        if (req->operation_params().has_forget_after()) {
            ev->ForgetAfter = GetDuration(req->operation_params().forget_after());
        }

        if (req->has_results_ttl()) {
            ev->ResultsTtl = GetDuration(req->results_ttl());
        }

        std::tie(status, issues) = FillKqpRequest(*req, ev->Record);
        if (status != Ydb::StatusIds::SUCCESS) {
            return nullptr;
        }
        return ev;
    }

    void Reply(Ydb::StatusIds::StatusCode status, Ydb::Operations::Operation&& result, const NYql::TIssues& issues = {}) {
        LOG_INFO_S(TActivationContext::AsActorContext(), NKikimrServices::RPC_REQUEST, "Execute script, status: "
            << Ydb::StatusIds::StatusCode_Name(status) << (issues ? ". Issues: " : "") << issues.ToOneLineString());

        google::protobuf::RepeatedPtrField<TYdbIssueMessageType> issuesMessage;
        for (const auto& issue : issues) {
            auto item = result.add_issues();
            NYql::IssueToMessage(issue, item);
        }

        result.set_status(status);

        AuditContextAppend(Request_.get(), *Request_->GetProtoRequest(), result);

        TString serializedResult;
        Y_PROTOBUF_SUPPRESS_NODISCARD result.SerializeToString(&serializedResult);

        Request_->SendSerializedResult(std::move(serializedResult), status);

        PassAway();
    }

    void Reply(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues) {
        Ydb::Operations::Operation result;
        result.set_ready(true);
        Reply(status, std::move(result), issues);
    }

private:
    std::unique_ptr<TEvExecuteScriptRequest> Request_;
};

} // namespace

namespace NQuery {

void DoExecuteScript(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    Y_UNUSED(f);
    auto* req = dynamic_cast<TEvExecuteScriptRequest*>(p.release());
    Y_ABORT_UNLESS(req != nullptr, "Wrong using of TGRpcRequestWrapper");
    f.RegisterActor(new TExecuteScriptRPC(req));
}

}

} // namespace NKikimr::NGRpcService

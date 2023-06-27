#include "service_query.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/kikimr_issue.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/rpc_kqp_base.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/public/api/protos/draft/ydb_query.pb.h>
#include <ydb/public/lib/operation_id/operation_id.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>

namespace NKikimr::NGRpcService {

namespace {

using namespace NActors;

using TEvExecuteScriptRequest = TGrpcRequestNoOperationCall<Ydb::Query::ExecuteScriptRequest,
    Ydb::Operations::Operation>;

bool FillQueryContent(const Ydb::Query::ExecuteScriptRequest& req, NKikimrKqp::TEvQueryRequest& kqpRequest,
    NYql::TIssues& issues)
{
    switch (req.script_case()) {
        case Ydb::Query::ExecuteScriptRequest::kScriptContent:
            if (!CheckQuery(req.script_content().text(), issues)) {
                return false;
            }

            kqpRequest.MutableRequest()->SetQuery(req.script_content().text());
            return true;

        case Ydb::Query::ExecuteScriptRequest::kScriptId:
            issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                "Execution by script id is not supported yet"));
            return false;

        default:
            issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                "Unexpected query option"));
            return false;
    }
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



    kqpRequest.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT);
    kqpRequest.MutableRequest()->SetKeepSession(false);

    // TODO: Avoid explicit tx_control for script queries.
    kqpRequest.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
    kqpRequest.MutableRequest()->MutableTxControl()->set_commit_tx(true);

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
        if (request.operation_params().has_forget_after() && request.operation_params().operation_mode() != Ydb::Operations::OperationParams::SYNC) {
            issues.AddIssue("forget_after is not supported for this type of operation");
            return Reply(Ydb::StatusIds::UNSUPPORTED, issues);
        }

        if (request.operation_params().operation_mode() == Ydb::Operations::OperationParams::SYNC) {
            issues.AddIssue("ExecuteScript must be asyncronous operation");
            return Reply(Ydb::StatusIds::BAD_REQUEST, issues);
        }

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
        hFunc(NKqp::TEvKqp::TEvScriptResponse, Handle)
    )

    void Handle(NKqp::TEvKqp::TEvScriptResponse::TPtr& ev) {
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
    Y_VERIFY(req != nullptr, "Wrong using of TGRpcRequestWrapper");
    f.RegisterActor(new TExecuteScriptRPC(req));
}

}

} // namespace NKikimr::NGRpcService

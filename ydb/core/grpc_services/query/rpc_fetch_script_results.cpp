#include "service_query.h"

#include <ydb/core/base/appdata.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/rpc_request_base.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/kqp_script_executions.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/proxy_service/kqp_script_executions.h>
#include <ydb/public/api/protos/ydb_query.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>

namespace NKikimr::NGRpcService {

namespace {

using namespace NActors;

using TEvFetchScriptResultsRequest = TGrpcRequestNoOperationCall<Ydb::Query::FetchScriptResultsRequest,
    Ydb::Query::FetchScriptResultsResponse>;

constexpr i64 MAX_ROWS_LIMIT = 1000;
constexpr i64 MAX_SIZE_LIMIT = 50_MB;

class TFetchScriptResultsRPC : public TRpcRequestActor<TFetchScriptResultsRPC, TEvFetchScriptResultsRequest, false> {
public:
    using TRpcRequestActorBase = TRpcRequestActor<TFetchScriptResultsRPC, TEvFetchScriptResultsRequest, false>;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    TFetchScriptResultsRPC(TEvFetchScriptResultsRequest* request)
        : TRpcRequestActorBase(request)
    {}

    void Bootstrap() {
        const auto* req = GetProtoRequest();
        if (!req) {
            Reply(Ydb::StatusIds::INTERNAL_ERROR, "Internal error");
            return;
        }

        if (req->rows_limit() < 0) {
            Reply(Ydb::StatusIds::BAD_REQUEST, "Invalid rows limit");
            return;
        }

        if (req->rows_limit() > MAX_ROWS_LIMIT) {
            Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Rows limit is too large. Values <= " << MAX_ROWS_LIMIT << " are allowed");
            return;
        }

        RowsOffset = 0;
        if (!req->fetch_token().Empty()) {
            auto fetch_token = TryFromString<ui64>(req->fetch_token());
            if (fetch_token) {
                RowsOffset = *fetch_token;
            } else {
                Reply(Ydb::StatusIds::BAD_REQUEST, "Invalid fetch token");
                return;
            }
        }

        if (!GetExecutionIdFromRequest()) {
            return;
        }

        Register(NKqp::CreateGetScriptExecutionResultActor(SelfId(), GetDatabaseName(), ExecutionId, req->result_set_index(), RowsOffset, req->rows_limit(), req->rows_limit() ? 0 : MAX_SIZE_LIMIT, Request->GetDeadline()));

        Become(&TFetchScriptResultsRPC::StateFunc);
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(NKqp::TEvFetchScriptResultsResponse, Handle);
    )

    void Handle(NKqp::TEvFetchScriptResultsResponse::TPtr& ev) {
        Ydb::Query::FetchScriptResultsResponse resp;
        resp.set_status(ev->Get()->Status);
        resp.set_result_set_index(static_cast<i64>(GetProtoRequest()->result_set_index()));
        if (ev->Get()->Issues) {
            NYql::IssuesToMessage(ev->Get()->Issues, resp.mutable_issues());
        }
        if (ev->Get()->ResultSet) {
            resp.mutable_result_set()->Swap(&(*ev->Get()->ResultSet));
            if (ev->Get()->HasMoreResults) {
                resp.set_next_fetch_token(ToString(RowsOffset + resp.result_set().rows_size()));
            }
        }
        Reply(resp.status(), std::move(resp));
    }

    void Reply(Ydb::StatusIds::StatusCode status, Ydb::Query::FetchScriptResultsResponse&& result, const NYql::TIssues& issues = {}) {
        LOG_INFO_S(TActivationContext::AsActorContext(), NKikimrServices::RPC_REQUEST, "Fetch script results, status: "
            << Ydb::StatusIds::StatusCode_Name(status) << (issues ? ". Issues: " : "") << issues.ToOneLineString());

        for (const auto& issue : issues) {
            auto item = result.add_issues();
            NYql::IssueToMessage(issue, item);
        }

        result.set_status(status);

        TString serializedResult;
        Y_PROTOBUF_SUPPRESS_NODISCARD result.SerializeToString(&serializedResult);

        Request->SendSerializedResult(std::move(serializedResult), status);

        PassAway();
    }

    void Reply(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues) {
        Ydb::Query::FetchScriptResultsResponse result;
        Reply(status, std::move(result), issues);
    }

    void Reply(Ydb::StatusIds::StatusCode status, const TString& errorText) {
        NYql::TIssues issues;
        issues.AddIssue(errorText);
        Reply(status, issues);
    }

    bool GetExecutionIdFromRequest() {
        TMaybe<TString> executionId = NKqp::ScriptExecutionIdFromOperation(GetProtoRequest()->operation_id());
        if (!executionId) {
            Reply(Ydb::StatusIds::BAD_REQUEST, "Invalid operation id");
            return false;
        }
        ExecutionId = *executionId;
        return true;
    }

private:
    TString ExecutionId;
    ui64 RowsOffset = 0;
};

} // namespace

namespace NQuery {

void DoFetchScriptResults(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    auto* req = dynamic_cast<TEvFetchScriptResultsRequest*>(p.release());
    Y_ABORT_UNLESS(req != nullptr, "Wrong using of TGRpcRequestWrapper");
    f.RegisterActor(new TFetchScriptResultsRPC(req));
}

}

} // namespace NKikimr::NGRpcService

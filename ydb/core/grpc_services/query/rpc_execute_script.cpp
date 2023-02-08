#include "service_query.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/kikimr_issue.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/rpc_kqp_base.h>
#include <ydb/public/api/protos/draft/ydb_query.pb.h>
#include <ydb/public/lib/operation_id/operation_id.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>

namespace NKikimr::NGRpcService {

namespace {

using namespace NActors;

using TEvExecuteScriptRequest = TGrpcRequestNoOperationCall<Ydb::Query::ExecuteScriptRequest,
    Ydb::Operations::Operation>;

class TExecuteScriptRPC : public TActorBootstrapped<TExecuteScriptRPC> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    TExecuteScriptRPC(TEvExecuteScriptRequest* request)
        : Request_(request)
    {}

    void Bootstrap() {
        Ydb::Operations::Operation operation;
        operation.set_id(GetOperationId());
        NYql::TIssues issues;
        issues.AddIssue("Execute script started");
        Reply(Ydb::StatusIds::SUCCESS, std::move(operation), issues);
    }

    TString GetOperationId() const {
        Ydb::TOperationId operationId;
        operationId.SetKind(Ydb::TOperationId::SCRIPT);
        NOperationId::AddOptionalValue(operationId, "id", "fake_execute_script_id");
        return NOperationId::ProtoToString(operationId);
    }

private:
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

private:
    std::unique_ptr<TEvExecuteScriptRequest> Request_;
};

} // namespace

void DoExecuteScriptRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    Y_UNUSED(f);
    auto* req = dynamic_cast<TEvExecuteScriptRequest*>(p.release());
    Y_VERIFY(req != nullptr, "Wrong using of TGRpcRequestWrapper");
    TActivationContext::AsActorContext().Register(new TExecuteScriptRPC(req));
}

} // namespace NKikimr::NGRpcService

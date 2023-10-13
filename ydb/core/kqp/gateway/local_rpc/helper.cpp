#include "helper.h"

#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/ydb_issue/issue_helpers.h>

namespace NKikimr {

namespace NGRpcService {

IActor* CreateExtAlterTableRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg, ui64 flags);

class TExtendedAlterTableRequest : public Ydb::Table::AlterTableRequest {
public:
    TExtendedAlterTableRequest(NKqpProto::TKqpSchemeOperation::TAlterTable&& alter)
        : Ydb::Table::AlterTableRequest(std::move(*alter.MutableReq()))
        , ExtendedFlags(alter.GetFlags())
    {}

    ui64 GetExtendedFlags() const {
        return ExtendedFlags;
    }
private:
    const ui64 ExtendedFlags;
};

using TEvAlterTableRequest = NGRpcService::TGrpcRequestOperationCall<TExtendedAlterTableRequest,
    Ydb::Table::AlterTableResponse>;

struct TEvPgAlterTableRequest : public TEvAlterTableRequest {
    static IActor* CreateRpcActor(IRequestOpCtx* ctx) {
        auto request = static_cast<const TExtendedAlterTableRequest*>(ctx->GetRequest());
        return CreateExtAlterTableRpcActor(ctx, request->GetExtendedFlags());
    }
};

} // NGRpcService

namespace NKqp {

using namespace NYql;
using namespace Ydb;

IKikimrGateway::TGenericResult GenericResultFromSyncOperation(const Operations::Operation& op) {
    using NYql::NCommon::ResultFromIssues;

    NYql::TIssues issues;
    NYql::IssuesFromMessage(op.issues(), issues);

    if (op.ready() != true) {
        issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, TStringBuilder()
            << "Unexpected operation for \"sync\" mode"));
        return ResultFromIssues<IKikimrGateway::TGenericResult>(TIssuesIds::DEFAULT_ERROR, issues);
    } else {
        const auto& yqlStatus = NYql::YqlStatusFromYdbStatus(op.status());
        return ResultFromIssues<IKikimrGateway::TGenericResult>(yqlStatus, issues);
    }
}

void DoAlterTableSameMailbox(NKqpProto::TKqpSchemeOperation::TAlterTable&& req, TAlterTableRespHandler&& cb,
    const TString& database, const TMaybe<TString>& token, const TMaybe<TString>& type)
{
    NRpcService::DoLocalRpcSameMailbox<NGRpcService::TEvPgAlterTableRequest>(std::move(req), std::move(cb),
        database, token, type, TlsActivationContext->AsActorContext());
}

}
}

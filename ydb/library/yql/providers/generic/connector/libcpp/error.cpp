#include "error.h"

#include <grpcpp/impl/codegen/status_code_enum.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h> // for NKikimr::NRpcService::GrpcStatusToYdbStatus
#include <ydb/library/yql/dq/actors/dq.h>
#include <yql/essentials/public/issue/yql_issue_message.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NConnector {
    NApi::TError NewSuccess() {
        NApi::TError error;
        error.set_status(Ydb::StatusIds::SUCCESS);
        return error;
    }

    TIssues ErrorToIssues(const NApi::TError& error, TString prefix) {
        TIssues issues;
        issues.Reserve(error.get_arr_issues().size() + 1);

        // add high-level error
        issues.AddIssue(TIssue(TStringBuilder() << prefix << error.message()));

        // convert detailed errors
        for (auto& subIssue : error.get_arr_issues()) {
            issues.AddIssue(IssueFromMessage(subIssue));
        }

        return issues;
    }

    NDqProto::StatusIds::StatusCode ErrorToDqStatus(const NApi::TError& error) {
        return NYql::NDq::YdbStatusToDqStatus(error.status(), NYql::NDq::EStatusCompatibilityLevel::WithUnauthorized);
    }

    NApi::TError ErrorFromGRPCStatus(const NYdbGrpc::TGrpcStatus& status) {
        NApi::TError result;

        if (status.Ok()) {
            result.set_status(Ydb::StatusIds::SUCCESS);
        } else {
            result.set_status(status.InternalError ? Ydb::StatusIds::INTERNAL_ERROR : NKikimr::NRpcService::GrpcStatusToYdbStatus(static_cast<grpc::StatusCode>(status.GRpcStatusCode)));
            result.set_message(TString{status.Msg});
        }

        return result;
    }
} // namespace NYql::NConnector

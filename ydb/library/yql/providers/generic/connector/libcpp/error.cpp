#include "error.h"

#include <grpcpp/impl/codegen/status_code_enum.h>
#include <yql/essentials/public/issue/yql_issue_message.h>
#include <yql/essentials/utils/yql_panic.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NYql::NConnector {
    NApi::TError NewSuccess() {
        NApi::TError error;
        error.set_status(Ydb::StatusIds_StatusCode::StatusIds_StatusCode_SUCCESS);
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
        switch (error.status()) {
            case ::Ydb::StatusIds::StatusCode::StatusIds_StatusCode_BAD_REQUEST:
                return NDqProto::StatusIds::StatusCode::StatusIds_StatusCode_BAD_REQUEST;
            case ::Ydb::StatusIds::StatusCode::StatusIds_StatusCode_INTERNAL_ERROR:
                return NDqProto::StatusIds::StatusCode::StatusIds_StatusCode_INTERNAL_ERROR;
            case ::Ydb::StatusIds::StatusCode::StatusIds_StatusCode_UNSUPPORTED:
                return NDqProto::StatusIds::StatusCode::StatusIds_StatusCode_UNSUPPORTED;
            case ::Ydb::StatusIds::StatusCode::StatusIds_StatusCode_NOT_FOUND:
                return NDqProto::StatusIds::StatusCode::StatusIds_StatusCode_BAD_REQUEST;
            case ::Ydb::StatusIds::StatusCode::StatusIds_StatusCode_SCHEME_ERROR:
                return NDqProto::StatusIds::StatusCode::StatusIds_StatusCode_SCHEME_ERROR;
            default:
                ythrow yexception() << "Unexpected YDB status code: " << ::Ydb::StatusIds::StatusCode_Name(error.status());
        }
    }

    NApi::TError ErrorFromGRPCStatus(const NYdbGrpc::TGrpcStatus& status) {
        NApi::TError result;

        if (status.GRpcStatusCode == grpc::OK) {
            result.set_status(Ydb::StatusIds_StatusCode::StatusIds_StatusCode_SUCCESS);
        } else {
            // FIXME: more appropriate error code for network error
            result.set_status(Ydb::StatusIds_StatusCode::StatusIds_StatusCode_INTERNAL_ERROR);
            result.set_message(TString{status.Msg});
        }

        return result;
    }
} // namespace NYql::NConnector

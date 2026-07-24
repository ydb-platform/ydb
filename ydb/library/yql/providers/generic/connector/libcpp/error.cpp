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
            case ::Ydb::StatusIds::StatusCode::StatusIds_StatusCode_UNAUTHORIZED:
                return NDqProto::StatusIds::StatusCode::StatusIds_StatusCode_UNAUTHORIZED;
            case ::Ydb::StatusIds::StatusCode::StatusIds_StatusCode_STATUS_CODE_UNSPECIFIED:
                return NDqProto::StatusIds::StatusCode::StatusIds_StatusCode_UNSPECIFIED;
            case ::Ydb::StatusIds::StatusCode::StatusIds_StatusCode_SUCCESS:
                return NDqProto::StatusIds::StatusCode::StatusIds_StatusCode_SUCCESS;
            case ::Ydb::StatusIds::StatusCode::StatusIds_StatusCode_ABORTED:
                return NDqProto::StatusIds::StatusCode::StatusIds_StatusCode_ABORTED;
            case ::Ydb::StatusIds::StatusCode::StatusIds_StatusCode_UNAVAILABLE:
                return NDqProto::StatusIds::StatusCode::StatusIds_StatusCode_UNAVAILABLE;
            case ::Ydb::StatusIds::StatusCode::StatusIds_StatusCode_OVERLOADED:
                return NDqProto::StatusIds::StatusCode::StatusIds_StatusCode_OVERLOADED;
            case ::Ydb::StatusIds::StatusCode::StatusIds_StatusCode_GENERIC_ERROR:
                return NDqProto::StatusIds::StatusCode::StatusIds_StatusCode_GENERIC_ERROR;
            case ::Ydb::StatusIds::StatusCode::StatusIds_StatusCode_TIMEOUT:
                return NDqProto::StatusIds::StatusCode::StatusIds_StatusCode_TIMEOUT;
            case ::Ydb::StatusIds::StatusCode::StatusIds_StatusCode_PRECONDITION_FAILED:
                return NDqProto::StatusIds::StatusCode::StatusIds_StatusCode_PRECONDITION_FAILED;
            case ::Ydb::StatusIds::StatusCode::StatusIds_StatusCode_CANCELLED:
                return NDqProto::StatusIds::StatusCode::StatusIds_StatusCode_CANCELLED;
            case ::Ydb::StatusIds::StatusCode::StatusIds_StatusCode_UNDETERMINED:
                return NDqProto::StatusIds::StatusCode::StatusIds_StatusCode_UNDETERMINED;
            case ::Ydb::StatusIds::StatusCode::StatusIds_StatusCode_EXTERNAL_ERROR:
                return NDqProto::StatusIds::StatusCode::StatusIds_StatusCode_EXTERNAL_ERROR;

            case ::Ydb::StatusIds::StatusCode::StatusIds_StatusCode_ALREADY_EXISTS:
            case ::Ydb::StatusIds::StatusCode::StatusIds_StatusCode_SESSION_EXPIRED:
            case ::Ydb::StatusIds::StatusCode::StatusIds_StatusCode_SESSION_BUSY:
            case ::Ydb::StatusIds::StatusCode::StatusIds_StatusCode_BAD_SESSION:
            default:
                return NDqProto::StatusIds::StatusCode::StatusIds_StatusCode_INTERNAL_ERROR;
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

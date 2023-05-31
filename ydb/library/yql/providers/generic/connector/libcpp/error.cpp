#include "error.h"

#include <grpcpp/impl/codegen/status_code_enum.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NYql::Connector {
    bool ErrorIsUnitialized(const API::Error& error) noexcept {
        return error.status() == Ydb::StatusIds_StatusCode::StatusIds_StatusCode_SUCCESS && error.message().empty();
    }

    bool ErrorIsSuccess(const API::Error& error) {
        YQL_ENSURE(error.status() != Ydb::StatusIds_StatusCode::StatusIds_StatusCode_STATUS_CODE_UNSPECIFIED,
                   "error status code is not initialized");

        auto ok = error.status() == Ydb::StatusIds_StatusCode::StatusIds_StatusCode_SUCCESS;
        if (ok) {
            YQL_ENSURE(error.issues_size() == 0, "request succeeded, but issues are not empty");
        }

        return ok;
    }

    TIssues ErrorToIssues(const API::Error& error) {
        TIssues issues;
        issues.Reserve(error.get_arr_issues().size() + 1);

        // add high-level error
        issues.AddIssue(TIssue(error.message()));

        // convert detailed errors
        IssuesFromMessage(error.get_arr_issues(), issues);

        return issues;
    }

    void ErrorToExprCtx(const API::Error& error, TExprContext& ctx, const TPosition& position, const TString& summary) {
        YQL_ENSURE(!ErrorIsSuccess(error));

        // add high-level error
        TStringBuilder ss;
        ss << summary << ": status=" << Ydb::StatusIds_StatusCode_Name(error.status()) << ", message=" << error.message();
        ctx.AddError(TIssue(position, ss));

        // convert detailed errors
        TIssues issues;
        IssuesFromMessage(error.get_arr_issues(), issues);
        for (const auto& issue : issues) {
            ctx.AddError(issue);
        }
    }

    API::Error ErrorFromGRPCStatus(const grpc::Status& status) {
        API::Error result;

        if (status.error_code() == grpc::StatusCode::OK) {
            result.set_status(Ydb::StatusIds_StatusCode::StatusIds_StatusCode_SUCCESS);
        } else {
            // FIXME: more appropriate error code for network error
            result.set_status(Ydb::StatusIds_StatusCode::StatusIds_StatusCode_INTERNAL_ERROR);
            result.set_message(status.error_message());
        }

        return result;
    }
}

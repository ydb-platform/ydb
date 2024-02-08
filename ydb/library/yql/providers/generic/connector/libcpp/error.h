#pragma once

#include <grpcpp/support/status.h>

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/dq/actors/protos/dq_status_codes.pb.h>
#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>
#include <ydb/library/grpc/client/grpc_client_low.h>

namespace NYql::NConnector {
    NApi::TError NewSuccess();

    template <typename TResponse>
    bool IsSuccess(const TResponse& response) {
        if (!response.has_error()) {
            return true;
        }

        const NApi::TError& error = response.error();

        YQL_ENSURE(error.status() != Ydb::StatusIds_StatusCode::StatusIds_StatusCode_STATUS_CODE_UNSPECIFIED,
                   "error status code is not initialized");

        auto ok = error.status() == Ydb::StatusIds_StatusCode::StatusIds_StatusCode_SUCCESS;
        if (ok) {
            YQL_ENSURE(error.issues_size() == 0, "request succeeded, but issues are not empty");
        }

        return ok;
    }

    TIssues ErrorToIssues(const NApi::TError& error);

    NDqProto::StatusIds::StatusCode ErrorToDqStatus(const NApi::TError& error);

    void ErrorToExprCtx(const NApi::TError& error, TExprContext& ctx, const TPosition& position, const TString& summary);

    NApi::TError ErrorFromGRPCStatus(const NYdbGrpc::TGrpcStatus& status);

    inline bool GrpcStatusEndOfStream(const NYdbGrpc::TGrpcStatus& status) noexcept {
        return status.GRpcStatusCode == grpc::OUT_OF_RANGE && status.Msg == "Read EOF";
    }

    inline bool GrpcStatusNeedsRetry(const NYdbGrpc::TGrpcStatus& status) noexcept {
        return status.GRpcStatusCode == grpc::UNAVAILABLE;
    }
}

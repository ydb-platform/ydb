#pragma once

#include <grpcpp/support/status.h>

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/dq/actors/protos/dq_status_codes.pb.h>
#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>
#include <library/cpp/grpc/client/grpc_client_low.h>

namespace NYql::NConnector {
    NApi::TError NewSuccess();

    bool IsSuccess(const NApi::TError& error);

    TIssues ErrorToIssues(const NApi::TError& error);

    NDqProto::StatusIds::StatusCode ErrorToDqStatus(const NApi::TError& error);

    void ErrorToExprCtx(const NApi::TError& error, TExprContext& ctx, const TPosition& position, const TString& summary);

    NApi::TError ErrorFromGRPCStatus(const NGrpc::TGrpcStatus& status);

    inline bool GrpcStatusEndOfStream(const NGrpc::TGrpcStatus& status) noexcept {
        return status.GRpcStatusCode == grpc::OUT_OF_RANGE && status.Msg == "Read EOF";
    }

    inline bool GrpcStatusNeedsRetry(const NGrpc::TGrpcStatus& status) noexcept {
        return status.GRpcStatusCode == grpc::UNAVAILABLE;
    }
}

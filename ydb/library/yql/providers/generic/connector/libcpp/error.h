#pragma once

#include <grpcpp/support/status.h>

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/dq/actors/protos/dq_status_codes.pb.h>
#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>

namespace NYql::NConnector {
    bool ErrorIsUninitialized(const NApi::TError& error) noexcept;
    bool ErrorIsSuccess(const NApi::TError& error);
    TIssues ErrorToIssues(const NApi::TError& error);
    NDqProto::StatusIds::StatusCode ErrorToDqStatus(const NApi::TError& error);
    void ErrorToExprCtx(const NApi::TError& error, TExprContext& ctx, const TPosition& position, const TString& summary);
    NApi::TError ErrorFromGRPCStatus(const grpc::Status& status);
}

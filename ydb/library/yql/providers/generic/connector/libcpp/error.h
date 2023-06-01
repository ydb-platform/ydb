#pragma once

#include <grpcpp/support/status.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/providers/generic/connector/api/protos/connector.pb.h>

namespace NYql::Connector {

    bool ErrorIsUnitialized(const API::Error& error) noexcept;
    bool ErrorIsSuccess(const API::Error& error);
    TIssues ErrorToIssues(const API::Error& error);
    void ErrorToExprCtx(const API::Error& error, TExprContext& ctx, const TPosition& position, const TString& summary);
    API::Error ErrorFromGRPCStatus(const grpc::Status& status);

}

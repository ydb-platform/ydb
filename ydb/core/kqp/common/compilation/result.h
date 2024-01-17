#pragma once
#include <memory>
#include <ydb/core/kqp/common/simple/query_id.h>
#include <ydb/core/kqp/common/simple/helpers.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/ast/yql_ast.h>

namespace NKikimr::NKqp {

class TPreparedQueryHolder;
struct TQueryAst;

struct TKqpCompileResult {
    using TConstPtr = std::shared_ptr<const TKqpCompileResult>;

    TKqpCompileResult(const TString& uid, const Ydb::StatusIds::StatusCode& status, const NYql::TIssues& issues,
            ETableReadType maxReadType, TMaybe<TKqpQueryId> query = {}, std::shared_ptr<NYql::TAstParseResult> ast = {})
        : Status(status)
        , Issues(issues)
        , Query(std::move(query))
        , Uid(uid)
        , MaxReadType(maxReadType)
        , Ast(std::move(ast)) {}

    static std::shared_ptr<TKqpCompileResult> Make(const TString& uid, const Ydb::StatusIds::StatusCode& status,
        const NYql::TIssues& issues, ETableReadType maxReadType, TMaybe<TKqpQueryId> query = {},
        std::shared_ptr<NYql::TAstParseResult> ast = {})
    {
        return std::make_shared<TKqpCompileResult>(uid, status, issues, maxReadType, std::move(query), std::move(ast));
    }

    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;

    TMaybe<TKqpQueryId> Query;
    TString Uid;

    ETableReadType MaxReadType;
    bool AllowCache = true;
    std::shared_ptr<NYql::TAstParseResult> Ast;

    std::shared_ptr<const TPreparedQueryHolder> PreparedQuery;
};
} // namespace NKikimr::NKqp

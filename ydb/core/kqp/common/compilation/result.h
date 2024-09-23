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
            ETableReadType maxReadType, TMaybe<TKqpQueryId> query = {}, std::shared_ptr<NYql::TAstParseResult> ast = {},
            bool needToSplit = false, const TMaybe<TString>& commandTagName = {})
        : Status(status)
        , Issues(issues)
        , Query(std::move(query))
        , Uid(uid)
        , MaxReadType(maxReadType)
        , Ast(std::move(ast))
        , NeedToSplit(needToSplit)
        , CommandTagName(commandTagName) {}

    static std::shared_ptr<TKqpCompileResult> Make(const TString& uid, const Ydb::StatusIds::StatusCode& status,
        const NYql::TIssues& issues, ETableReadType maxReadType, TMaybe<TKqpQueryId> query = {},
        std::shared_ptr<NYql::TAstParseResult> ast = {}, bool needToSplit = false, const TMaybe<TString>& commandTagName = {})
    {
        return std::make_shared<TKqpCompileResult>(uid, status, issues, maxReadType, std::move(query), std::move(ast), needToSplit, commandTagName);
    }

    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;

    TMaybe<TKqpQueryId> Query;
    TString Uid;

    ETableReadType MaxReadType;
    bool AllowCache = true;
    std::shared_ptr<NYql::TAstParseResult> Ast;
    bool NeedToSplit = false;
    TMaybe<TString> CommandTagName = {};

    std::shared_ptr<const TPreparedQueryHolder> PreparedQuery;
};

struct TKqpStatsCompile {
    bool FromCache = false;
    ui64 DurationUs = 0;
    ui64 CpuTimeUs = 0;
};
} // namespace NKikimr::NKqp

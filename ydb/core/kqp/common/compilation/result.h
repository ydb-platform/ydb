#pragma once
#include <memory>
#include <ydb/core/kqp/common/simple/query_ast.h>
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
            ETableReadType maxReadType, TMaybe<TKqpQueryId> query = {}, TMaybe<TQueryAst> queryAst = {},
            bool needToSplit = false, const TMaybe<TString>& commandTagName = {})
        : Status(status)
        , Issues(issues)
        , Query(std::move(query))
        , Uid(uid)
        , MaxReadType(maxReadType)
        , QueryAst(std::move(queryAst))
        , NeedToSplit(needToSplit)
        , CommandTagName(commandTagName) {}

    static std::shared_ptr<TKqpCompileResult> Make(const TString& uid, const Ydb::StatusIds::StatusCode& status,
        const NYql::TIssues& issues, ETableReadType maxReadType, TMaybe<TKqpQueryId> query = {},
        TMaybe<TQueryAst> queryAst = {}, bool needToSplit = false, const TMaybe<TString>& commandTagName = {})
    {
        return std::make_shared<TKqpCompileResult>(uid, status, issues, maxReadType, std::move(query), std::move(queryAst), needToSplit, commandTagName);
    }

    std::shared_ptr<NYql::TAstParseResult> GetAst() const;

    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;

    TMaybe<TKqpQueryId> Query;
    TString Uid;

    ETableReadType MaxReadType;
    bool AllowCache = true;
    TMaybe<TQueryAst> QueryAst;
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

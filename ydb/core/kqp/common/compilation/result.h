#pragma once
#include <memory>
#include <ydb/core/kqp/common/simple/query_id.h>
#include <ydb/core/kqp/common/simple/helpers.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

namespace NKikimr::NKqp {

class TPreparedQueryHolder;

struct TKqpCompileResult {
    using TConstPtr = std::shared_ptr<const TKqpCompileResult>;

    TKqpCompileResult(const TString& uid, TKqpQueryId&& query, const Ydb::StatusIds::StatusCode& status,
        const NYql::TIssues& issues, ETableReadType maxReadType)
        : Status(status)
        , Issues(issues)
        , Query(std::move(query))
        , Uid(uid)
        , MaxReadType(maxReadType) {}

    TKqpCompileResult(const TString& uid, const Ydb::StatusIds::StatusCode& status, const NYql::TIssues& issues,
        ETableReadType maxReadType)
        : Status(status)
        , Issues(issues)
        , Uid(uid)
        , MaxReadType(maxReadType) {}

    static std::shared_ptr<TKqpCompileResult> Make(const TString& uid, TKqpQueryId&& query,
        const Ydb::StatusIds::StatusCode& status, const NYql::TIssues& issues, ETableReadType maxReadType)
    {
        return std::make_shared<TKqpCompileResult>(uid, std::move(query), status, issues, maxReadType);
    }

    static std::shared_ptr<TKqpCompileResult> Make(const TString& uid, const Ydb::StatusIds::StatusCode& status,
        const NYql::TIssues& issues, ETableReadType maxReadType)
    {
        return std::make_shared<TKqpCompileResult>(uid, status, issues, maxReadType);
    }

    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;

    TMaybe<TKqpQueryId> Query;
    TString Uid;

    ETableReadType MaxReadType;
    bool AllowCache = true;

    std::shared_ptr<const TPreparedQueryHolder> PreparedQuery;
};
} // namespace NKikimr::NKqp

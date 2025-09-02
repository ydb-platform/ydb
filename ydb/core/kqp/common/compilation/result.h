#pragma once
#include <memory>
#include <util/datetime/base.h>
#include <util/digest/multi.h>
#include <ydb/core/kqp/common/simple/query_ast.h>
#include <ydb/core/kqp/common/simple/query_id.h>
#include <ydb/core/kqp/common/simple/helpers.h>
#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/ast/yql_ast.h>

namespace NKikimr::NKqp {

class TPreparedQueryHolder;
struct TQueryAst;

struct TKqpCompileResult {
    using TConstPtr = std::shared_ptr<const TKqpCompileResult>;

    TKqpCompileResult(const TString& uid, const Ydb::StatusIds::StatusCode& status, const NYql::TIssues& issues,
            ETableReadType maxReadType, TMaybe<TKqpQueryId> query = {}, TMaybe<TQueryAst> queryAst = {},
            bool needToSplit = false, const TMaybe<TString>& commandTagName = {}, const TMaybe<TString>& replayMessageUserView = {})
        : Status(status)
        , Issues(issues)
        , Query(std::move(query))
        , Uid(uid)
        , MaxReadType(maxReadType)
        , QueryAst(std::move(queryAst))
        , NeedToSplit(needToSplit)
        , CommandTagName(commandTagName)
        , ReplayMessageUserView(replayMessageUserView) {}

    static std::shared_ptr<TKqpCompileResult> Make(const TString& uid, const Ydb::StatusIds::StatusCode& status,
        const NYql::TIssues& issues, ETableReadType maxReadType, TMaybe<TKqpQueryId> query = {},
        TMaybe<TQueryAst> queryAst = {}, bool needToSplit = false, const TMaybe<TString>& commandTagName = {}, const TMaybe<TString>& replayMessageUserView = {})
    {
        return std::make_shared<TKqpCompileResult>(uid, status, issues, maxReadType, std::move(query), std::move(queryAst), needToSplit, commandTagName, replayMessageUserView);
    }

    std::shared_ptr<NYql::TAstParseResult> GetAst() const;

    void IncUsage() const { UsageFrequency++; }
    ui64 GetAccessCount() const { return UsageFrequency.load(); }

    void SerializeTo(NKikimrKqp::TCompileCacheQueryInfo* to) const;

    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;

    TMaybe<TKqpQueryId> Query;
    TString Uid;

    ETableReadType MaxReadType;
    bool AllowCache = true;
    TMaybe<TQueryAst> QueryAst;
    bool NeedToSplit = false;
    TMaybe<TString> CommandTagName = {};

    TMaybe<TString> ReplayMessageUserView;

    std::shared_ptr<const TPreparedQueryHolder> PreparedQuery;
    mutable std::atomic<ui64> UsageFrequency;
    TInstant CompiledAt = TInstant::Now();
};

struct TKqpStatsCompile {
    bool FromCache = false;
    ui64 DurationUs = 0;
    ui64 CpuTimeUs = 0;
};
} // namespace NKikimr::NKqp

template<>
struct THash<NKikimr::NKqp::TKqpCompileResult::TConstPtr> {
    size_t operator ()(const NKikimr::NKqp::TKqpCompileResult::TConstPtr& x) const { return std::hash<NKikimr::NKqp::TKqpCompileResult::TConstPtr>()(x); }
};
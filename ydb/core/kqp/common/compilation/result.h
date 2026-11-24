#pragma once
#include <memory>
#include <library/cpp/json/json_value.h>
#include <util/datetime/base.h>
#include <util/digest/multi.h>
#include <ydb/core/kqp/common/simple/query_id.h>
#include <ydb/core/kqp/common/simple/helpers.h>
#include <yql/essentials/public/issue/yql_issue.h>

namespace NKikimr::NKqp {

class TPreparedQueryHolder;

struct TKqpCompileResult {
    using TConstPtr = std::shared_ptr<const TKqpCompileResult>;

    TKqpCompileResult(const TString& uid, const Ydb::StatusIds::StatusCode& status, const NYql::TIssues& issues,
            ETableReadType maxReadType, const TDuration& compilationDuration = {}, TMaybe<TKqpQueryId> query = {},
            const TMaybe<NJson::TJsonValue>& compileMeta = {}, bool needToSplit = false, const TMaybe<TString>& commandTagName = {},
            const TMaybe<TString>& replayMessageUserView = {})
        : Status(status)
        , Issues(issues)
        , Query(std::move(query))
        , Uid(uid)
        , MaxReadType(maxReadType)
        , NeedToSplit(needToSplit)
        , CommandTagName(commandTagName)
        , ReplayMessageUserView(replayMessageUserView)
        , CompilationDuration(compilationDuration)
        , CompileMeta(compileMeta) {}

    static std::shared_ptr<TKqpCompileResult> Make(const TString& uid, const Ydb::StatusIds::StatusCode& status,
        const NYql::TIssues& issues, ETableReadType maxReadType, const TDuration& compileDuration = {}, TMaybe<TKqpQueryId> query = {},
        const TMaybe<NJson::TJsonValue>& compileMeta = {}, bool needToSplit = false, const TMaybe<TString>& commandTagName = {},
        const TMaybe<TString>& replayMessageUserView = {})
    {
        return std::make_shared<TKqpCompileResult>(uid, status, issues, maxReadType, compileDuration, std::move(query),
                compileMeta, needToSplit, commandTagName, replayMessageUserView);
    }

    void IncUsage() const { UsageFrequency++; }
    ui64 GetAccessCount() const { return UsageFrequency.load(); }
    TString SerializeIssues() const {return Issues.ToOneLineString(); }

    void SerializeTo(NKikimrKqp::TCompileCacheQueryInfo* to, std::optional<ui64> lastAccessedAt = std::nullopt) const;

    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;

    TMaybe<TKqpQueryId> Query;
    TString Uid;

    ETableReadType MaxReadType;
    bool AllowCache = true;
    bool UsedNewRbo = false;
    bool NeedToSplit = false;
    TMaybe<TString> CommandTagName = {};

    TMaybe<TString> ReplayMessageUserView;

    std::shared_ptr<const TPreparedQueryHolder> PreparedQuery;
    mutable std::atomic<ui64> UsageFrequency;
    TInstant CompiledAt = TInstant::Now();
    TDuration CompilationDuration;
    TMaybe<NJson::TJsonValue> CompileMeta; // consist of parameters of query
};

struct TKqpStatsCompile {
    bool FromCache = false;
    ui64 DurationUs = 0;
    ui64 CpuTimeUs = 0;
    bool UsedNewRbo = false;
};
} // namespace NKikimr::NKqp

template<>
struct THash<NKikimr::NKqp::TKqpCompileResult::TConstPtr> {
    size_t operator ()(const NKikimr::NKqp::TKqpCompileResult::TConstPtr& x) const { return std::hash<NKikimr::NKqp::TKqpCompileResult::TConstPtr>()(x); }
};
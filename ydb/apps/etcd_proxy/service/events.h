#pragma once

#include <ydb/core/base/events.h>

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/public/issue/yql_issue_message.h>
#include <ydb/public/sdk/cpp/client/ydb_query/client.h>

namespace NEtcd {

enum EEv : ui32 {
    EvBegin = 5000,
    EvQueryResult,
    EvQueryError,
    EvEnd
};

struct TEvQueryResult : public NActors::TEventLocal<TEvQueryResult, EvQueryResult> {
    TEvQueryResult(const NYdb::TResultSets& result): Results(std::move(result)) {}

    const NYdb::TResultSets Results;
};

struct TEvQueryError : public NActors::TEventLocal<TEvQueryError, EvQueryError> {
    TEvQueryError(const NYql::TIssues& issues): Issues(issues) {}

    const NYql::TIssues Issues;
};

} // namespace NEtcd


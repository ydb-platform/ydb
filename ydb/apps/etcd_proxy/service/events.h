#pragma once

#include <ydb/core/base/events.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

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
    TEvQueryError(const NYdb::NIssue::TIssues& issues): Issues(issues) {}

    const NYdb::NIssue::TIssues Issues;
};

} // namespace NEtcd


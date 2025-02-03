#pragma once

#include <ydb/core/base/events.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

namespace NEtcd {

struct TData {
    TString Value;
    i64 Created = 0LL, Modified = 0LL, Version = 0LL, Lease = 0LL;
};

enum class EWatchKind : ui8 {
    Unsubscribe = 0,
    OnUpdates,
    OnDeletions,
    OnChanges = OnUpdates | OnDeletions
};

enum EEv : ui32 {
    EvBegin = 5000,
    EvQueryResult,
    EvQueryError,
    EvWatch,
    EvSubscribe,
    EvChange,
    EvEnd
};

struct TEvQueryResult : public NActors::TEventLocal<TEvQueryResult, EvQueryResult> {
    TEvQueryResult(const NYdb::TResultSets& result): Results(result) {}

    const NYdb::TResultSets Results;
};

struct TEvQueryError : public NActors::TEventLocal<TEvQueryError, EvQueryError> {
    TEvQueryError(const NYdb::NIssue::TIssues& issues): Issues(issues) {}

    const NYdb::NIssue::TIssues Issues;
};

struct TEvSubscribe : public NActors::TEventLocal<TEvSubscribe, EvSubscribe> {
    const TString Key, RangeEnd;
    const EWatchKind Kind;
    bool WithPrevious = false;

    TEvSubscribe(std::string_view key, std::string_view rangeEnd, EWatchKind kind, bool withPrevious = false)
        : Key(key), RangeEnd(rangeEnd), Kind(kind), WithPrevious(withPrevious)
    {}
};

struct TEvChange : public NActors::TEventLocal<TEvChange, EvChange> {
    TEvChange(TString&& key, TData&& oldData, TData&& newData)
        : Key(std::move(key)), OldData(std::move(oldData)), NewData(std::move(newData))
    {}

    TEvChange(const TEvChange& put)
        : Key(put.Key), OldData(put.OldData), NewData(put.NewData)
    {}

    const TString Key;
    const TData OldData, NewData;
};

} // namespace NEtcd

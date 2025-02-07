#pragma once

#include <ydb/core/base/events.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <ydb/apps/etcd_proxy/proto/rpc.grpc.pb.h>

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>

namespace NEtcd {

struct TData {
    std::string Value;
    i64 Created = 0LL, Modified = 0LL, Version = 0LL, Lease = 0LL;
};

enum class EWatchKind : ui8 {
    Unsubscribe = 0,
    OnUpdates,
    OnDeletions,
    OnChanges = OnUpdates | OnDeletions
};

enum Ev : ui32 {
    Begin = 5000,
    QueryResult,
    QueryError,

    Watch,
    LeaseKeepAlive,

    Subscribe,
    Change,
    End
};

struct TEvQueryResult : public NActors::TEventLocal<TEvQueryResult, Ev::QueryResult> {
    TEvQueryResult(const NYdb::TResultSets& result): Results(result) {}

    const NYdb::TResultSets Results;
};

struct TEvQueryError : public NActors::TEventLocal<TEvQueryError, Ev::QueryError> {
    TEvQueryError(const NYdb::NIssue::TIssues& issues): Issues(issues) {}

    const NYdb::NIssue::TIssues Issues;
};

struct TEvSubscribe : public NActors::TEventLocal<TEvSubscribe, Ev::Subscribe> {
    const std::string Key, RangeEnd;
    const EWatchKind Kind;
    bool WithPrevious = false;

    TEvSubscribe(std::string_view key = {}, std::string_view rangeEnd = {}, EWatchKind kind = EWatchKind::Unsubscribe, bool withPrevious = false)
        : Key(key), RangeEnd(rangeEnd), Kind(kind), WithPrevious(withPrevious)
    {}
};

struct TEvChange : public NActors::TEventLocal<TEvChange, Ev::Change> {
    TEvChange(std::string&& key, TData&& oldData, TData&& newData = {})
        : Key(std::move(key)), OldData(std::move(oldData)), NewData(std::move(newData))
    {}

    TEvChange(const TEvChange& put)
        : Key(put.Key), OldData(put.OldData), NewData(put.NewData)
    {}

    const std::string Key;
    const TData OldData, NewData;
};

template <Ev TRpcId, typename TReq, typename TRes>
class TEtcdRequestStreamWrapper
    : public NActors::TEventLocal<TEtcdRequestStreamWrapper<TRpcId, TReq, TRes>, TRpcId>
{
public:
    using IStreamCtx = NKikimr::NGRpcServer::IGRpcStreamingContext<TReq, TRes>;

    TEtcdRequestStreamWrapper(TIntrusivePtr<IStreamCtx> ctx)
        : Ctx_(ctx)
    {}

    const TIntrusivePtr<IStreamCtx>& GetStreamCtx() const {
        return Ctx_;
    }
private:
    const TIntrusivePtr<IStreamCtx> Ctx_;
};

using TEvWatchRequest = TEtcdRequestStreamWrapper<Ev::Watch, etcdserverpb::WatchRequest, etcdserverpb::WatchResponse>;
using TEvLeaseKeepAliveRequest = TEtcdRequestStreamWrapper<Ev::LeaseKeepAlive, etcdserverpb::LeaseKeepAliveRequest, etcdserverpb::LeaseKeepAliveResponse>;

} // namespace NEtcd

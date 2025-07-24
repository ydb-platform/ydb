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

    SimpleRequest,
    Watch,
    LeaseKeepAlive,

    QueryResult,
    QueryError,

    Subscribe,
    Change,
    Changes,
    Cancel,

    RequestRevision,
    ReturnRevision,

    End
};

struct TEvQueryResult : public NActors::TEventLocal<TEvQueryResult, Ev::QueryResult> {
    TEvQueryResult(const NYdb::TResultSets& result = {}): Results(result) {}

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

struct TChange {
    TChange(std::string&& key, TData&& oldData = {}, TData&& newData = {})
        : Key(std::move(key)), OldData(std::move(oldData)), NewData(std::move(newData))
    {}

    struct TOrder {
        bool operator()(const TChange& lhs, const TChange& rhs) const {
            return lhs.NewData.Modified < rhs.NewData.Modified;
        }
    };

    std::string Key;
    TData OldData, NewData;
};

struct TEvChange : public TChange, public NActors::TEventLocal<TEvChange, Ev::Change> {
    using TChange::TChange;
    TEvChange(const TEvChange& put) : TChange(put) {}
};

struct TEvChanges : public NActors::TEventLocal<TEvChanges, Ev::Changes> {
    TEvChanges(i64 id, std::vector<TChange>&& changes = {}) : Id(id), Changes(std::move(changes)) {}

    const i64 Id = 0LL;
    std::vector<TChange> Changes;
};

struct TEvCancel : public NActors::TEventLocal<TEvCancel, Ev::Cancel> {};

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

using TKeysSet = std::set<std::pair<std::string, std::string>>;

struct TEvRequestRevision : public NActors::TEventLocal<TEvRequestRevision, Ev::RequestRevision> {
    explicit TEvRequestRevision(TKeysSet&& keysSet = {})
        : KeysSet(std::move(keysSet))
    {}

    TKeysSet KeysSet;
};

using TGuard = std::shared_ptr<void>;

struct TEvReturnRevision : public NActors::TEventLocal<TEvReturnRevision, Ev::ReturnRevision> {
    explicit TEvReturnRevision(const i64 revision, const TGuard& guard = {}) : Revision(revision), Guard(std::move(guard)) {}

    const i64 Revision;
    const TGuard Guard;
};

using TEvWatchRequest = TEtcdRequestStreamWrapper<Ev::Watch, etcdserverpb::WatchRequest, etcdserverpb::WatchResponse>;
using TEvLeaseKeepAliveRequest = TEtcdRequestStreamWrapper<Ev::LeaseKeepAlive, etcdserverpb::LeaseKeepAliveRequest, etcdserverpb::LeaseKeepAliveResponse>;

} // namespace NEtcd

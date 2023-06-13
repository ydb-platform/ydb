#pragma once

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_endpoints/endpoints.h>
#include <ydb/public/lib/operation_id/operation_id.h>

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <library/cpp/cache/cache.h>

#include <util/datetime/base.h>

#include <functional>

namespace NYdb {
namespace NTable {

////////////////////////////////////////////////////////////////////////////////

using TSessionInspectorFn = std::function<void(TAsyncCreateSessionResult future)>;


class TSession::TImpl : public TEndpointObj {
    friend class TTableClient;
    friend class TSession;

#ifdef YDB_IMPL_TABLE_CLIENT_SESSION_UT
public:
#endif
    TImpl(const TString& sessionId, const TString& endpoint, bool useQueryCache, ui32 queryCacheSize, bool isOwnedBySessionPool);
public:
    enum EState {
        S_STANDALONE,
        S_IDLE,
        S_BROKEN,
        S_ACTIVE,
        S_CLOSING
    };

    struct TDataQueryInfo {
        TString QueryId;
        ::google::protobuf::Map<TString, Ydb::Type> ParameterTypes;

        TDataQueryInfo() {}

        TDataQueryInfo(const TString& queryId,
            const ::google::protobuf::Map<TString, Ydb::Type>& parameterTypes)
            : QueryId(queryId)
            , ParameterTypes(parameterTypes) {}
    };
public:
    ~TImpl();

    const TString& GetId() const;
    const TString& GetEndpoint() const;
    const TEndpointKey& GetEndpointKey() const;
    void MarkBroken();
    void MarkAsClosing();
    void MarkStandalone();
    void MarkActive();
    void MarkIdle();
    bool IsOwnedBySessionPool() const;
    EState GetState() const;
    void SetNeedUpdateActiveCounter(bool flag);
    bool NeedUpdateActiveCounter() const;
    void InvalidateQueryInCache(const TString& key);
    void InvalidateQueryCache();
    TMaybe<TDataQueryInfo> GetQueryFromCache(const TString& query, bool allowMigration);
    void AddQueryToCache(const TDataQuery& query);
    void ScheduleTimeToTouch(TDuration interval, bool updateTimeInPast);
    void ScheduleTimeToTouchFast(TDuration interval, bool updateTimeInPast);
    TInstant GetTimeToTouchFast() const;
    TInstant GetTimeInPastFast() const;

    // SetTimeInterval/GetTimeInterval, are not atomic!
    void SetTimeInterval(TDuration interval);
    TDuration GetTimeInterval() const;

    const TLRUCache<TString, TDataQueryInfo>& GetQueryCacheUnsafe() const;

    static std::function<void(TSession::TImpl*)> GetSmartDeleter(std::shared_ptr<TTableClient::TImpl> client);

    static TSessionInspectorFn GetSessionInspector(
        NThreading::TPromise<TCreateSessionResult>& promise,
        std::shared_ptr<TTableClient::TImpl> client,
        const TCreateSessionSettings& settings,
        ui32 counter, bool needUpdateActiveSessionCounter);

private:
    const TString SessionId_;
    const TEndpointKey EndpointKey_;
    EState State_;
    bool UseQueryCache_;
    TLRUCache<TString, TDataQueryInfo> QueryCache_;
    TAdaptiveLock Lock_;
    TInstant TimeToTouch_;
    TInstant TimeInPast_;
    // Is used to implement progressive timeout for settler keep alive call
    TDuration TimeInterval_;
    // Indicate session was in active state, but state was changed (need to decrement active session counter)
    // TODO: suboptimal because need lock for atomic change from interceptor
    // Rewrite with bit field
    bool NeedUpdateActiveCounter_;
    const bool IsOwnedBySessionPool_;
};

} // namespace NTable
} // namespace NYdb

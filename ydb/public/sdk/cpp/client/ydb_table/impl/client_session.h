#pragma once

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/kqp_session_common/kqp_session_common.h>
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


class TSession::TImpl : public TKqpSessionCommon {
    friend class TTableClient;
    friend class TSession;

#ifdef YDB_IMPL_TABLE_CLIENT_SESSION_UT
public:
#endif
    TImpl(const TString& sessionId, const TString& endpoint, bool useQueryCache, ui32 queryCacheSize, bool isOwnedBySessionPool);
public:
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
    ~TImpl() = default;

    void InvalidateQueryInCache(const TString& key);
    void InvalidateQueryCache();
    TMaybe<TDataQueryInfo> GetQueryFromCache(const TString& query, bool allowMigration);
    void AddQueryToCache(const TDataQuery& query);

    const TLRUCache<TString, TDataQueryInfo>& GetQueryCacheUnsafe() const;

    static TSessionInspectorFn GetSessionInspector(
        NThreading::TPromise<TCreateSessionResult>& promise,
        std::shared_ptr<TTableClient::TImpl> client,
        const TCreateSessionSettings& settings,
        ui32 counter, bool needUpdateActiveSessionCounter);

private:
    bool UseQueryCache_;
    TLRUCache<TString, TDataQueryInfo> QueryCache_;
};

} // namespace NTable
} // namespace NYdb

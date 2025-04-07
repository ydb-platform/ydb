#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/src/client/impl/ydb_internal/kqp_session_common/kqp_session_common.h>
#include <ydb/public/sdk/cpp/src/client/impl/ydb_endpoints/endpoints.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/operation_id/operation_id.h>

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <library/cpp/cache/cache.h>

#include <util/datetime/base.h>

#include <functional>

namespace NYdb::inline Dev {
namespace NTable {

////////////////////////////////////////////////////////////////////////////////

using TSessionInspectorFn = std::function<void(TAsyncCreateSessionResult future)>;

class TSession::TImpl : public TKqpSessionCommon {
    friend class TTableClient;
    friend class TSession;

#ifdef YDB_IMPL_TABLE_CLIENT_SESSION_UT
public:
#endif
    TImpl(const std::string& sessionId, const std::string& endpoint, bool useQueryCache, ui32 queryCacheSize, bool isOwnedBySessionPool);
public:
    struct TDataQueryInfo {
        std::string QueryId;
        ::google::protobuf::Map<TStringType, Ydb::Type> ParameterTypes;

        TDataQueryInfo() {}

        TDataQueryInfo(const std::string& queryId,
            const ::google::protobuf::Map<TStringType, Ydb::Type>& parameterTypes)
            : QueryId(queryId)
            , ParameterTypes(parameterTypes) {}
    };
public:
    ~TImpl() = default;

    void InvalidateQueryInCache(const std::string& key);
    void InvalidateQueryCache();
    std::optional<TDataQueryInfo> GetQueryFromCache(const std::string& query, bool allowMigration);
    void AddQueryToCache(const TDataQuery& query);

    const TLRUCache<std::string, TDataQueryInfo>& GetQueryCacheUnsafe() const;

    static TSessionInspectorFn GetSessionInspector(
        NThreading::TPromise<TCreateSessionResult>& promise,
        std::shared_ptr<TTableClient::TImpl> client,
        const TCreateSessionSettings& settings,
        ui32 counter, bool needUpdateActiveSessionCounter);

private:
    bool UseQueryCache_;
    TLRUCache<std::string, TDataQueryInfo> QueryCache_;
};

} // namespace NTable
} // namespace NYdb

#pragma once

#include <ydb/public/sdk/cpp/client/impl/ydb_internal/internal_header.h>

#include <ydb/public/sdk/cpp/client/draft/ydb_query/query.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/grpc_connections/grpc_connections.h>

namespace NYdb::NQuery {

class TExecQueryImpl {
public:
    static TAsyncExecuteQueryIterator StreamExecuteQuery(const std::shared_ptr<TGRpcConnectionsImpl>& connections,
        const TDbDriverStatePtr& driverState, const TString& query, const TExecuteQuerySettings& settings);

    static TAsyncExecuteQueryResult ExecuteQuery(const std::shared_ptr<TGRpcConnectionsImpl>& connections,
        const TDbDriverStatePtr& driverState, const TString& query, const TExecuteQuerySettings& settings);
};

} // namespace NYdb::NQuery::NImpl

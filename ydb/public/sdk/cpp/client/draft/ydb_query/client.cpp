#include "client.h"

#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>
#include <ydb/public/sdk/cpp/client/draft/ydb_query/impl/exec_query.h>

namespace NYdb::NQuery {

class TQueryClient::TImpl: public TClientImplCommon<TQueryClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
        , Settings_(settings)
    {
    }

    ~TImpl() {
        // TODO: Drain sessions.
    }

    TAsyncExecuteQueryIterator StreamExecuteQuery(const TString& query, const TExecuteQuerySettings& settings) {
        return TExecQueryImpl::StreamExecuteQuery(Connections_, DbDriverState_, query, settings);
    }

    TAsyncExecuteQueryResult ExecuteQuery(const TString& query, const TExecuteQuerySettings& settings) {
        return TExecQueryImpl::ExecuteQuery(Connections_, DbDriverState_, query, settings);
    }

private:
    TClientSettings Settings_;
};

TQueryClient::TQueryClient(const TDriver& driver, const TClientSettings& settings)
    : Impl_(new TQueryClient::TImpl(CreateInternalInterface(driver), settings))
{
}

TAsyncExecuteQueryResult TQueryClient::ExecuteQuery(const TString& query,
    const TExecuteQuerySettings& settings)
{
    return Impl_->ExecuteQuery(query, settings);
}

TAsyncExecuteQueryIterator TQueryClient::StreamExecuteQuery(const TString& query,
    const TExecuteQuerySettings& settings)
{
    return Impl_->StreamExecuteQuery(query, settings);
}

} // namespace NYdb::NQuery

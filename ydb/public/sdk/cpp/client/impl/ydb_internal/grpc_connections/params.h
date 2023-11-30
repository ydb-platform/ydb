#pragma once

#include <ydb/public/sdk/cpp/client/impl/ydb_internal/internal_header.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/common/ssl_credentials.h>

namespace NYdb {

class IConnectionsParams {
public:
    virtual ~IConnectionsParams() = default;
    virtual TStringType GetEndpoint() const = 0;
    virtual size_t GetNetworkThreadsNum() const = 0;
    virtual size_t GetClientThreadsNum() const = 0;
    virtual size_t GetMaxQueuedResponses() const = 0;
    virtual TSslCredentials GetSslCredentials() const = 0;
    virtual TStringType GetDatabase() const = 0;
    virtual std::shared_ptr<ICredentialsProviderFactory> GetCredentialsProviderFactory() const = 0;
    virtual EDiscoveryMode GetDiscoveryMode() const = 0;
    virtual size_t GetMaxQueuedRequests() const = 0;
    virtual NYdbGrpc::TTcpKeepAliveSettings GetTcpKeepAliveSettings() const = 0;
    virtual bool GetDrinOnDtors() const = 0;
    virtual TBalancingSettings GetBalancingSettings() const = 0;
    virtual TDuration GetGRpcKeepAliveTimeout() const = 0;
    virtual bool GetGRpcKeepAlivePermitWithoutCalls() const = 0;
    virtual TDuration GetSocketIdleTimeout() const = 0;
    virtual const TLog& GetLog() const = 0;
    virtual ui64 GetMemoryQuota() const = 0;
    virtual ui64 GetMaxInboundMessageSize() const = 0;
    virtual ui64 GetMaxOutboundMessageSize() const = 0;
    virtual ui64 GetMaxMessageSize() const = 0;
};

} // namespace NYdb

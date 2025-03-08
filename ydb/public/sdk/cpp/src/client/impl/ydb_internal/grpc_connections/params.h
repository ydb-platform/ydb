#pragma once

#include <library/cpp/logger/log.h>

#include <src/client/impl/ydb_internal/internal_header.h>
#include <src/client/impl/ydb_internal/common/types.h>
#include <ydb-cpp-sdk/client/common_client/ssl_credentials.h>
#include <ydb-cpp-sdk/client/types/credentials/credentials.h>

namespace NYdb::inline Dev {

class IConnectionsParams {
public:
    virtual ~IConnectionsParams() = default;
    virtual std::string GetEndpoint() const = 0;
    virtual size_t GetNetworkThreadsNum() const = 0;
    virtual size_t GetClientThreadsNum() const = 0;
    virtual size_t GetMaxQueuedResponses() const = 0;
    virtual TSslCredentials GetSslCredentials() const = 0;
    virtual std::string GetDatabase() const = 0;
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
    virtual uint64_t GetMemoryQuota() const = 0;
    virtual uint64_t GetMaxInboundMessageSize() const = 0;
    virtual uint64_t GetMaxOutboundMessageSize() const = 0;
    virtual uint64_t GetMaxMessageSize() const = 0;
};

} // namespace NYdb

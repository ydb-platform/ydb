#include <ydb-cpp-sdk/client/driver/driver.h>

#define INCLUDE_YDB_INTERNAL_H
#include <src/client/impl/ydb_internal/driver/constants.h>
#include <src/client/impl/ydb_internal/grpc_connections/grpc_connections.h>
#include <src/client/impl/ydb_internal/logger/log.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <library/cpp/logger/log.h>
#include <src/client/impl/ydb_internal/common/parser.h>
#include <src/client/impl/ydb_internal/common/getenv.h>
#include <ydb-cpp-sdk/client/common_client/ssl_credentials.h>
#include <util/stream/file.h>
#include <ydb-cpp-sdk/client/resources/ydb_ca.h>

namespace NYdb::inline Dev {

using NYdbGrpc::TGRpcClientLow;
using NYdbGrpc::TServiceConnection;
using NYdbGrpc::TSimpleRequestProcessor;
using NYdbGrpc::TGRpcClientConfig;
using NYdbGrpc::TResponseCallback;
using NYdbGrpc::TGrpcStatus;
using NYdbGrpc::TTcpKeepAliveSettings;

using Ydb::StatusIds;

using namespace NThreading;

class TDriverConfig::TImpl : public IConnectionsParams {
public:
    std::string GetEndpoint() const override { return Endpoint; }
    size_t GetNetworkThreadsNum() const override { return NetworkThreadsNum; }
    size_t GetClientThreadsNum() const override { return ClientThreadsNum; }
    size_t GetMaxQueuedResponses() const override { return MaxQueuedResponses; }
    TSslCredentials GetSslCredentials() const override { return SslCredentials; }
    std::string GetDatabase() const override { return Database; }
    std::shared_ptr<ICredentialsProviderFactory> GetCredentialsProviderFactory() const override { return CredentialsProviderFactory; }
    EDiscoveryMode GetDiscoveryMode() const override { return DiscoveryMode; }
    size_t GetMaxQueuedRequests() const override { return MaxQueuedRequests; }
    TTcpKeepAliveSettings GetTcpKeepAliveSettings() const override { return TcpKeepAliveSettings; }
    bool GetDrinOnDtors() const override { return DrainOnDtors; }
    TBalancingSettings GetBalancingSettings() const override { return BalancingSettings; }
    TDuration GetGRpcKeepAliveTimeout() const override { return GRpcKeepAliveTimeout; }
    bool GetGRpcKeepAlivePermitWithoutCalls() const override { return GRpcKeepAlivePermitWithoutCalls; }
    TDuration GetSocketIdleTimeout() const override { return SocketIdleTimeout; }
    uint64_t GetMemoryQuota() const override { return MemoryQuota; }
    uint64_t GetMaxInboundMessageSize() const override { return MaxInboundMessageSize; }
    uint64_t GetMaxOutboundMessageSize() const override { return MaxOutboundMessageSize; }
    uint64_t GetMaxMessageSize() const override { return MaxMessageSize; }
    const TLog& GetLog() const override { return Log; }

    std::string Endpoint;
    size_t NetworkThreadsNum = 2;
    size_t ClientThreadsNum = 0;
    size_t MaxQueuedResponses = 0;
    TSslCredentials SslCredentials;
    std::string Database;
    std::shared_ptr<ICredentialsProviderFactory> CredentialsProviderFactory = CreateInsecureCredentialsProviderFactory();
    EDiscoveryMode DiscoveryMode = EDiscoveryMode::Sync;
    size_t MaxQueuedRequests = 100;
    NYdbGrpc::TTcpKeepAliveSettings TcpKeepAliveSettings =
        {
            true,
            TCP_KEEPALIVE_IDLE,
            TCP_KEEPALIVE_COUNT,
            TCP_KEEPALIVE_INTERVAL
        };
    bool DrainOnDtors = true;
    TBalancingSettings BalancingSettings = TBalancingSettings{EBalancingPolicy::UsePreferableLocation, std::string()};
    TDuration GRpcKeepAliveTimeout;
    bool GRpcKeepAlivePermitWithoutCalls = false;
    TDuration SocketIdleTimeout = TDuration::Minutes(6);
    uint64_t MemoryQuota = 0;
    uint64_t MaxInboundMessageSize = 0;
    uint64_t MaxOutboundMessageSize = 0;
    uint64_t MaxMessageSize = 0;
    TLog Log; // Null by default.
};

TDriverConfig::TDriverConfig(const std::string& connectionString)
    : Impl_(new TImpl) {
        if (connectionString != ""){
            auto connectionInfo = ParseConnectionString(connectionString);
            SetEndpoint(connectionInfo.Endpoint);
            SetDatabase(connectionInfo.Database);
            Impl_->SslCredentials.IsEnabled = connectionInfo.EnableSsl;
        }
}

TDriverConfig& TDriverConfig::SetEndpoint(const std::string& endpoint) {
    Impl_->Endpoint = endpoint;
    return *this;
}

TDriverConfig& TDriverConfig::SetNetworkThreadsNum(size_t sz) {
    Impl_->NetworkThreadsNum = sz;
    return *this;
}

TDriverConfig& TDriverConfig::SetClientThreadsNum(size_t sz) {
    Impl_->ClientThreadsNum = sz;
    return *this;
}

TDriverConfig& TDriverConfig::SetMaxClientQueueSize(size_t sz) {
    Impl_->MaxQueuedResponses = sz;
    return *this;
}

TDriverConfig& TDriverConfig::UseSecureConnection(const std::string& cert) {
    Impl_->SslCredentials.IsEnabled = true;
    Impl_->SslCredentials.CaCert = cert;
    return *this;
}

TDriverConfig& TDriverConfig::UseClientCertificate(const std::string& clientCert, const std::string& clientPrivateKey) {
    Impl_->SslCredentials.Cert = clientCert;
    Impl_->SslCredentials.PrivateKey = clientPrivateKey;
    return *this;
}

TDriverConfig& TDriverConfig::SetAuthToken(const std::string& token) {
    return SetCredentialsProviderFactory(CreateOAuthCredentialsProviderFactory(token));
}

TDriverConfig& TDriverConfig::SetDatabase(const std::string& database) {
    Impl_->Database = database;
    Impl_->Log.SetFormatter(GetPrefixLogFormatter(GetDatabaseLogPrefix(Impl_->Database)));
    return *this;
}

TDriverConfig& TDriverConfig::SetCredentialsProviderFactory(std::shared_ptr<ICredentialsProviderFactory> credentialsProviderFactory) {
    Impl_->CredentialsProviderFactory = credentialsProviderFactory;
    return *this;
}

TDriverConfig& TDriverConfig::SetDiscoveryMode(EDiscoveryMode discoveryMode) {
    Impl_->DiscoveryMode = discoveryMode;
    return *this;
}

TDriverConfig& TDriverConfig::SetMaxQueuedRequests(size_t sz) {
    Impl_->MaxQueuedRequests = sz;
    return *this;
}

TDriverConfig& TDriverConfig::SetTcpKeepAliveSettings(bool enable, size_t idle, size_t count, size_t interval) {
    Impl_->TcpKeepAliveSettings.Enabled = enable;
    Impl_->TcpKeepAliveSettings.Idle = idle;
    Impl_->TcpKeepAliveSettings.Count = count;
    Impl_->TcpKeepAliveSettings.Interval = interval;
    return *this;
}

TDriverConfig& TDriverConfig::SetGrpcMemoryQuota(uint64_t bytes) {
    Impl_->MemoryQuota = bytes;
    return *this;
}

TDriverConfig& TDriverConfig::SetDrainOnDtors(bool allowed) {
    Impl_->DrainOnDtors = allowed;
    return *this;
}

TDriverConfig& TDriverConfig::SetBalancingPolicy(EBalancingPolicy policy, const std::string& params) {
    Impl_->BalancingSettings = TBalancingSettings{policy, params};
    return *this;
}

TDriverConfig& TDriverConfig::SetGRpcKeepAliveTimeout(TDuration timeout) {
    Impl_->GRpcKeepAliveTimeout = timeout;
    return *this;
}

TDriverConfig& TDriverConfig::SetGRpcKeepAlivePermitWithoutCalls(bool permitWithoutCalls) {
    Impl_->GRpcKeepAlivePermitWithoutCalls = permitWithoutCalls;
    return *this;
}

TDriverConfig& TDriverConfig::SetSocketIdleTimeout(TDuration timeout) {
    Impl_->SocketIdleTimeout = timeout;
    return *this;
}

TDriverConfig& TDriverConfig::SetMaxInboundMessageSize(uint64_t maxInboundMessageSize) {
    Impl_->MaxInboundMessageSize = maxInboundMessageSize;
    return *this;
}

TDriverConfig& TDriverConfig::SetMaxOutboundMessageSize(uint64_t maxOutboundMessageSize) {
    Impl_->MaxOutboundMessageSize = maxOutboundMessageSize;
    return *this;
}

TDriverConfig& TDriverConfig::SetMaxMessageSize(uint64_t maxMessageSize) {
    Impl_->MaxMessageSize = maxMessageSize;
    return *this;
}

TDriverConfig& TDriverConfig::SetLog(std::unique_ptr<TLogBackend>&& log) {
    Impl_->Log.ResetBackend(THolder(log.release()));
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<TGRpcConnectionsImpl> CreateInternalInterface(const TDriver connection) {
    return connection.Impl_;
}

////////////////////////////////////////////////////////////////////////////////

TDriver::TDriver(const TDriverConfig& config) {
    if (!config.Impl_) {
        ythrow yexception() << "Invalid config object";
    }

    Impl_.reset(new TGRpcConnectionsImpl(config.Impl_));
}

void TDriver::Stop(bool wait) {
    Impl_->Stop(wait);
}

TDriverConfig TDriver::GetConfig() const {
    TDriverConfig config;

    config.SetEndpoint(Impl_->DefaultDiscoveryEndpoint_);
    config.SetNetworkThreadsNum(Impl_->NetworkThreadsNum_);
    config.SetClientThreadsNum(Impl_->ClientThreadsNum_);
    config.SetMaxClientQueueSize(Impl_->MaxQueuedResponses_);
    if (Impl_->SslCredentials_.IsEnabled) {
        config.UseSecureConnection(Impl_->SslCredentials_.CaCert);
    }
    config.UseClientCertificate(Impl_->SslCredentials_.Cert, Impl_->SslCredentials_.PrivateKey);
    config.SetCredentialsProviderFactory(Impl_->DefaultCredentialsProviderFactory_);
    config.SetDatabase(Impl_->DefaultDatabase_);
    config.SetDiscoveryMode(Impl_->DefaultDiscoveryMode_);
    config.SetMaxQueuedRequests(Impl_->MaxQueuedRequests_);
    config.SetGrpcMemoryQuota(Impl_->MemoryQuota_);
    config.SetTcpKeepAliveSettings(
        Impl_->TcpKeepAliveSettings_.Enabled,
        Impl_->TcpKeepAliveSettings_.Idle,
        Impl_->TcpKeepAliveSettings_.Count,
        Impl_->TcpKeepAliveSettings_.Interval
    );
    config.SetDrainOnDtors(Impl_->DrainOnDtors_);
    config.SetBalancingPolicy(Impl_->BalancingSettings_.Policy, Impl_->BalancingSettings_.PolicyParams);
    config.SetGRpcKeepAliveTimeout(Impl_->GRpcKeepAliveTimeout_);
    config.SetGRpcKeepAlivePermitWithoutCalls(Impl_->GRpcKeepAlivePermitWithoutCalls_);
    config.SetSocketIdleTimeout(Impl_->SocketIdleTimeout_);
    config.SetMaxInboundMessageSize(Impl_->MaxInboundMessageSize_);
    config.SetMaxOutboundMessageSize(Impl_->MaxOutboundMessageSize_);
    config.SetMaxMessageSize(Impl_->MaxMessageSize_);
    config.Impl_->Log = Impl_->Log;
    
    return config;
}

} // namespace NYdb

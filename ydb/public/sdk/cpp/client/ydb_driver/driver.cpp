#include "driver.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/driver/constants.h> 
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/grpc_connections/grpc_connections.h> 
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/logger/log.h> 
#undef INCLUDE_YDB_INTERNAL_H

#include <library/cpp/logger/log.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/common/parser.h> 
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/common/getenv.h> 
#include <util/stream/file.h>
#include <ydb/public/sdk/cpp/client/resources/ydb_ca.h> 

namespace NYdb {

using NGrpc::TGRpcClientLow;
using NGrpc::TServiceConnection;
using NGrpc::TSimpleRequestProcessor;
using NGrpc::TGRpcClientConfig;
using NGrpc::TResponseCallback;
using NGrpc::TGrpcStatus;
using NGrpc::TTcpKeepAliveSettings;

using Ydb::StatusIds;

using namespace NThreading;

class TDriverConfig::TImpl : public IConnectionsParams {
public:
    TStringType GetEndpoint() const override { return Endpoint; }
    size_t GetNetworkThreadsNum() const override { return NetworkThreadsNum; }
    size_t GetClientThreadsNum() const override { return ClientThreadsNum; }
    size_t GetMaxQueuedResponses() const override { return MaxQueuedResponses; }
    bool IsSslEnabled() const override { return EnableSsl; }
    TStringType GetCaCert() const override { return CaCert; }
    TStringType GetDatabase() const override { return Database; }
    std::shared_ptr<ICredentialsProviderFactory> GetCredentialsProviderFactory() const override { return CredentialsProviderFactory; }
    EDiscoveryMode GetDiscoveryMode() const override { return DiscoveryMode; }
    size_t GetMaxQueuedRequests() const override { return MaxQueuedRequests; }
    TTcpKeepAliveSettings GetTcpKeepAliveSettings() const override { return TcpKeepAliveSettings; }
    bool GetDrinOnDtors() const override { return DrainOnDtors; }
    TBalancingSettings GetBalancingSettings() const override { return BalancingSettings; }
    TDuration GetGRpcKeepAliveTimeout() const override { return GRpcKeepAliveTimeout; }
    bool GetGRpcKeepAlivePermitWithoutCalls() const override { return GRpcKeepAlivePermitWithoutCalls; }
    TDuration GetSocketIdleTimeout() const override { return SocketIdleTimeout; }
    ui64 GetMemoryQuota() const override { return MemoryQuota; }
    const TLog& GetLog() const override { return Log; }

    TStringType Endpoint;
    size_t NetworkThreadsNum = 2;
    size_t ClientThreadsNum = 0;
    size_t MaxQueuedResponses = 0;
    bool EnableSsl = false;
    TStringType CaCert;
    TStringType Database;
    std::shared_ptr<ICredentialsProviderFactory> CredentialsProviderFactory = CreateInsecureCredentialsProviderFactory();
    EDiscoveryMode DiscoveryMode = EDiscoveryMode::Sync;
    size_t MaxQueuedRequests = 100;
    NGrpc::TTcpKeepAliveSettings TcpKeepAliveSettings =
        {
            true,
            TCP_KEEPALIVE_IDLE,
            TCP_KEEPALIVE_COUNT,
            TCP_KEEPALIVE_INTERVAL
        };
    bool DrainOnDtors = true;
    TBalancingSettings BalancingSettings = TBalancingSettings{EBalancingPolicy::UsePreferableLocation, TStringType()};
    TDuration GRpcKeepAliveTimeout;
    bool GRpcKeepAlivePermitWithoutCalls = false;
    TDuration SocketIdleTimeout = TDuration::Minutes(6);
    ui64 MemoryQuota = 0;
    TLog Log; // Null by default.
};

TDriverConfig::TDriverConfig(const TStringType& connectionString)
    : Impl_(new TImpl) {
        if (connectionString != ""){
            auto connectionInfo = ParseConnectionString(connectionString);
            SetEndpoint(connectionInfo.Endpoint);
            SetDatabase(connectionInfo.Database);
            Impl_->EnableSsl = connectionInfo.EnableSsl;
        }
}

TDriverConfig& TDriverConfig::SetEndpoint(const TStringType& endpoint) {
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

TDriverConfig& TDriverConfig::UseSecureConnection(const TStringType& cert) {
    Impl_->EnableSsl = true;
    Impl_->CaCert = cert;
    return *this;
}

TDriverConfig& TDriverConfig::SetAuthToken(const TStringType& token) {
    return SetCredentialsProviderFactory(CreateOAuthCredentialsProviderFactory(token));
}

TDriverConfig& TDriverConfig::SetDatabase(const TStringType& database) {
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

TDriverConfig& TDriverConfig::SetGrpcMemoryQuota(ui64 bytes) {
    Impl_->MemoryQuota = bytes;
    return *this;
}

TDriverConfig& TDriverConfig::SetDrainOnDtors(bool allowed) {
    Impl_->DrainOnDtors = allowed;
    return *this;
}

TDriverConfig& TDriverConfig::SetBalancingPolicy(EBalancingPolicy policy, const TStringType& params) {
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

TDriverConfig& TDriverConfig::SetLog(THolder<TLogBackend> log) {
    Impl_->Log.ResetBackend(std::move(log));
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

} // namespace NYdb

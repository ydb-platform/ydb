#pragma once

#include <ydb/public/sdk/cpp/client/ydb_common_client/settings.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status_codes.h>
#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>
#include <ydb/public/sdk/cpp/client/ydb_types/fatal_error_handlers/handlers.h>
#include <ydb/public/sdk/cpp/client/ydb_types/request_settings.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status/status.h>

#include <library/cpp/logger/backend.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYdb {

class TDriver;
class TGRpcConnectionsImpl;

////////////////////////////////////////////////////////////////////////////////

//! Represents configuration of YDB driver
class TDriverConfig {
    friend class TDriver;

public:
    //! Connection string format: "<protocol>://<hostname:port>/?database=<database-path>",
    //! where "<protocol>://" can be "grpc://" or "grpcs://" or be absent, "<hostname:port>" is endpoint,
    //! "/?database=<database-path>" is optional
    TDriverConfig(const TStringType& connectionString = "");
    //! Endpoint to initiate connections with Ydb cluster,
    //! client will connect to others nodes according to client loadbalancing
    TDriverConfig& SetEndpoint(const TStringType& endpoint);
    //! Set number of network threads, default: 2
    TDriverConfig& SetNetworkThreadsNum(size_t sz);
    //! Set number of client pool threads, if 0 adaptive thread pool will be used.
    //! NOTE: in case of no zero value it is possible to get deadlock if all threads
    //! of this pool is blocked somewhere in user code.
    //! default: 0
    TDriverConfig& SetClientThreadsNum(size_t sz);
    //! Warning: not recommended to change
    //! Set max number of queued responses. 0 - no limit
    //! There is a queue to perform async calls to user code,
    //! if this queue is full, attempts to enqueue responses inside sdk will be blocked
    //! Size of this queue must be greater than max size of all requests inflight
    //! Note: if this limit is reached network threads will be blocked.
    //! Note: set of this limit can cause deadlock in some case of using async interface
    //! This value doesn't make sense if SetClientThreadsNum is 0
    //! default: 0
    TDriverConfig& SetMaxClientQueueSize(size_t sz);
    //! Enable Ssl.
    //! caCerts  - The buffer containing the PEM encoding of the server root certificates.
    //!            If this parameter is empty, the default roots will be used.
    TDriverConfig& UseSecureConnection(const TStringType& caCerts = TStringType());
    TDriverConfig& UseClientCertificate(const TStringType& clientCert, const TStringType& clientPrivateKey);
    //! Set token, this option can be overridden for client by ClientSettings
    TDriverConfig& SetAuthToken(const TStringType& token);
    //! Set database, this option can be overridden for client by ClientSettings
    TDriverConfig& SetDatabase(const TStringType& database);
    //! Set credentials data, this option can be overridden for client by ClientSettings
    TDriverConfig& SetCredentialsProviderFactory(std::shared_ptr<ICredentialsProviderFactory> credentialsProviderFactory);
    //! Set behaviour of discovery routine
    //! See EDiscoveryMode enum comments
    //! default: EDiscoveryMode::Sync
    TDriverConfig& SetDiscoveryMode(EDiscoveryMode discoveryMode);
    //! Max number of requests in queue waiting for discovery if "Async" mode chosen
    //! default: 100
    TDriverConfig& SetMaxQueuedRequests(size_t sz);
    //! Limit using of memory for grpc buffer pool. 0 means disabled.
    //! If enabled the size must be greater than size of recieved message.
    //! default: 0
    TDriverConfig& SetGrpcMemoryQuota(ui64 bytes);
    //! Specify tcp keep alive settings
    //! This option allows to adjust tcp keep alive settings, useful to work
    //! with balancers or to detect unexpected connectivity problem.
    //! enable   - if true enable tcp keep alive and use following settings
    //!          - if false disable tcp keep alive
    //! idle     - (Linux only) the interval between the last data packet sent and the first keepalive probe, sec
    //!            if zero use OS default
    //! count    - (Linux only) the number of unacknowledged probes to send before considering the connection dead
    //!            if zero use OS default
    //! interval - (Linux only) the interval between subsequential keepalive probes, sec
    //!            if zero use OS default
    //! NOTE: Please read OS documentation and investigate your network topology before touching this option.
    //! default: true, 30, 5, 10 for linux, and true and OS default for others POSIX
    TDriverConfig& SetTcpKeepAliveSettings(bool enable, size_t idle, size_t count, size_t interval);
    //! Enable or disable drain of client logic (e.g. session pool drain) during dtor call
    TDriverConfig& SetDrainOnDtors(bool allowed);
    //! Set policy for balancing
    //! Params is a optionally field to set policy settings
    //! default: EBalancingPolicy::UsePreferableLocation
    TDriverConfig& SetBalancingPolicy(EBalancingPolicy policy, const TStringType& params = TStringType());
    //! !!! EXPERIMENTAL !!!
    //! Set grpc level keep alive. If keepalive ping was delayed more than given timeout
    //! internal grpc routine fails request with TRANSIENT_FAILURE or TRANSPORT_UNAVAILABLE error
    //! Note: this timeout should not be too small to prevent fail due to
    //! network buffers delay. I.e. values less than 5 seconds may cause request failure
    //! even with fast network
    //! default: disabled
    TDriverConfig& SetGRpcKeepAliveTimeout(TDuration timeout);
    TDriverConfig& SetGRpcKeepAlivePermitWithoutCalls(bool permitWithoutCalls);
    //! Set inactive socket timeout.
    //! Used to close connections, that were inactive for given time.
    //! Closes unused connections every 1/10 of timeout, so deletion time is approximate.
    //! Use TDuration::Max() to disable.
    //! default: 6 minutes
    TDriverConfig& SetSocketIdleTimeout(TDuration timeout);

    //! Set maximum incoming message size.
    //! Note: this option overrides MaxMessageSize for incoming messages.
    //! default: 0
    TDriverConfig& SetMaxInboundMessageSize(ui64 maxInboundMessageSize);
    //! Set maximum outgoing message size.
    //! Note: this option overrides MaxMessageSize for outgoing messages.
    //! default: 0
    TDriverConfig& SetMaxOutboundMessageSize(ui64 maxOutboundMessageSize);
    //! Note: if this option is unset, default 64_MB message size will be used.
    //! default: 0
    TDriverConfig& SetMaxMessageSize(ui64 maxMessageSize);

    //! Log backend.
    TDriverConfig& SetLog(THolder<TLogBackend> log);
private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

//! Represents connection pool to the database
class TDriver {
    friend std::shared_ptr<TGRpcConnectionsImpl> CreateInternalInterface(const TDriver);

public:
    TDriver(const TDriverConfig& config);

    //! Cancel all currently running and future requests
    //! This method is useful to make sure there are no new asynchronous
    //! callbacks and it is safe to destroy the driver
    //! When wait is true this method will not return until the underlying
    //! client thread pool is stopped completely
    void Stop(bool wait = false);

    template<typename TExtension>
    void AddExtension(typename TExtension::TParams params = typename TExtension::TParams());

private:
    std::shared_ptr<TGRpcConnectionsImpl> Impl_;
};

} // namespace NYdb

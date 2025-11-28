#pragma once

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/test_framework/test_key.h>
#include <yt/yt/core/test_framework/test_memory_tracker.h>
#include <yt/yt/core/test_framework/test_server_host.h>

#include <yt/yt/core/bus/bus.h>
#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/client.h>
#include <yt/yt/core/bus/tcp/dispatcher.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/crypto/config.h>

#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <yt/yt/core/bus/public.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/bus/server.h>

#include <yt/yt/core/rpc/client.h>
#include <yt/yt/core/rpc/retrying_channel.h>
#include <yt/yt/core/rpc/caching_channel_factory.h>
#include <yt/yt/core/rpc/static_channel_factory.h>
#include <yt/yt/core/rpc/server.h>
#include <yt/yt/core/rpc/local_server.h>
#include <yt/yt/core/rpc/local_channel.h>
#include <yt/yt/core/rpc/service_detail.h>
#include <yt/yt/core/rpc/stream.h>

#include <yt/yt/core/rpc/unittests/lib/test_service.h>
#include <yt/yt/core/rpc/unittests/lib/no_baggage_service.h>

#include <yt/yt/core/rpc/grpc/config.h>
#include <yt/yt/core/rpc/grpc/channel.h>
#include <yt/yt/core/rpc/grpc/server.h>
#include <yt/yt/core/rpc/grpc/proto/grpc.pb.h>

#include <yt/yt/core/http/server.h>
#include <yt/yt/core/https/config.h>
#include <yt/yt/core/https/server.h>
#include <yt/yt/core/rpc/http/server.h>
#include <yt/yt/core/rpc/http/channel.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/memory_usage_tracker.h>
#include <yt/yt/core/misc/shutdown.h>

#include <yt/yt/core/tracing/public.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/helpers.h>

#include <yt/yt/build/ya_version.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/common/network.h>

namespace NYT::NRpc {

using namespace NCrypto;

////////////////////////////////////////////////////////////////////////////////

extern TPemBlobConfigPtr RpcCACert;
extern TPemBlobConfigPtr RpcServerCert;
extern TPemBlobConfigPtr RpcServerKey;
extern TPemBlobConfigPtr RpcClientCert;
extern TPemBlobConfigPtr RpcClientKey;

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class TRpcTestBase
    : public ::testing::Test
{
public:
    void SetUp() final
    {
        WorkerPool_ = NConcurrency::CreateThreadPool(4, "Worker");
        MemoryUsageTracker_ = New<TTestNodeMemoryTracker>(32_MB);
        TestService_ = CreateTestService(WorkerPool_->GetInvoker(), TImpl::Secure, {}, MemoryUsageTracker_);

        auto serverConfig = ConvertTo<TServerConfigPtr>(NYson::TYsonString(TStringBuf(R"({
            services = {
                TestService = {
                    methods = {
                        DelayedCall = {
                            testing = {
                                random_delay = 1000;
                            }
                        }
                    }
                }
            }
        })")));

        auto services = std::vector<IServicePtr>{
            TestService_,
            CreateNoBaggageService(WorkerPool_->GetInvoker()),
        };

        Host_ = TImpl::CreateTestServerHost(
            NTesting::GetFreePort(),
            std::move(services),
            std::move(serverConfig),
            MemoryUsageTracker_);

        // Make sure local bypass is globally enabled.
        // Individual tests will toggle per-connection flag to actually enable this feature.
        auto config = New<NYT::NBus::TTcpDispatcherConfig>();
        config->EnableLocalBypass = true;
        NYT::NBus::TTcpDispatcher::Get()->Configure(config);
    }

    void TearDown() final
    {
        Host_->TearDown();
    }

    IChannelPtr CreateChannel(
        const std::optional<std::string>& address = {},
        THashMap<std::string, NYTree::INodePtr> grpcArguments = {})
    {
        return TImpl::CreateChannel(
            address.value_or(Host_->GetAddress()),
            Host_->GetAddress(),
            std::move(grpcArguments));
    }

    TTestNodeMemoryTrackerPtr GetMemoryUsageTracker() const
    {
        return Host_->GetMemoryUsageTracker();
    }

    ITestServicePtr GetTestService() const
    {
        return TestService_;
    }

    IServerPtr GetServer() const
    {
        return Host_->GetServer();
    }

    static bool CheckCancelCode(TErrorCode code)
    {
        if (code == NYT::EErrorCode::Canceled) {
            return true;
        }
        if (code == NYT::NRpc::EErrorCode::TransportError && TImpl::AllowTransportErrors) {
            return true;
        }
        return false;
    }

    static bool CheckTimeoutCode(TErrorCode code)
    {
        if (code == NYT::EErrorCode::Timeout) {
            return true;
        }
        if (code == NYT::NRpc::EErrorCode::TransportError && TImpl::AllowTransportErrors) {
            return true;
        }
        return false;
    }

    static int GetMaxSimultaneousRequestCount()
    {
        return TImpl::MaxSimultaneousRequestCount;
    }

private:
    NConcurrency::IThreadPoolPtr WorkerPool_;
    TTestNodeMemoryTrackerPtr MemoryUsageTracker_;
    TTestServerHostPtr Host_;
    ITestServicePtr TestService_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class TRpcOverBus
{
public:
    static constexpr bool AllowTransportErrors = false;
    static constexpr bool Secure = TImpl::Secure;
    static constexpr int MaxSimultaneousRequestCount = 1000;
    static constexpr bool MemoryUsageTrackingEnabled = TImpl::MemoryUsageTrackingEnabled;

    static TTestServerHostPtr CreateTestServerHost(
        NTesting::TPortHolder port,
        std::vector<IServicePtr> services,
        TServerConfigPtr serverConfig,
        TTestNodeMemoryTrackerPtr memoryUsageTracker)
    {
        auto busServer = CreateBusServer(port, memoryUsageTracker);
        auto server = NRpc::NBus::CreateBusServer(busServer);
        server->Configure(serverConfig);

        return New<TTestServerHost>(
            std::move(port),
            server,
            services,
            memoryUsageTracker);
    }

    static IChannelPtr CreateChannel(
        const std::string& address,
        const std::string& serverAddress,
        THashMap<std::string, NYTree::INodePtr> grpcArguments)
    {
        return TImpl::CreateChannel(address, serverAddress, std::move(grpcArguments));
    }

    static NYT::NBus::IBusServerPtr CreateBusServer(ui16 port, IMemoryUsageTrackerPtr memoryUsageTracker)
    {
        return TImpl::CreateBusServer(port, memoryUsageTracker);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <bool EnableSsl, bool EnableLocalBypass>
class TRpcOverBusImpl
{
public:
    // Bus RPC does not provide extension TSslCredentialsExt yet.
    static constexpr bool Secure = false;
    static constexpr bool MemoryUsageTrackingEnabled = !EnableLocalBypass;

    static IChannelPtr CreateChannel(
        const std::string& address,
        const std::string& /*serverAddress*/,
        THashMap<std::string, NYTree::INodePtr> /*grpcArguments*/)
    {
        auto config = NYT::NBus::TBusClientConfig::CreateTcp(address);
        if (EnableSsl) {
            config->EncryptionMode = NYT::NBus::EEncryptionMode::Required;
            config->VerificationMode = NYT::NBus::EVerificationMode::Full;
            config->CertificateAuthority = RpcCACert;
            config->CertificateChain = RpcClientCert;
            config->PrivateKey = RpcClientKey;
        }
        config->EnableLocalBypass = EnableLocalBypass;
        auto client = CreateBusClient(std::move(config));
        return NRpc::NBus::CreateBusChannel(std::move(client));
    }

    static NYT::NBus::IBusServerPtr CreateBusServer(ui16 port, IMemoryUsageTrackerPtr memoryUsageTracker)
    {
        auto config = NYT::NBus::TBusServerConfig::CreateTcp(port);
        if (EnableSsl) {
            config->EncryptionMode = NYT::NBus::EEncryptionMode::Required;
            config->VerificationMode = NYT::NBus::EVerificationMode::Full;
            config->CertificateAuthority = RpcCACert;
            config->CertificateChain = RpcServerCert;
            config->PrivateKey = RpcServerKey;
        }
        config->EnableLocalBypass = EnableLocalBypass;
        return NYT::NBus::CreateBusServer(
            std::move(config),
            NYT::NBus::GetYTPacketTranscoderFactory(),
            std::move(memoryUsageTracker));
    }
};

////////////////////////////////////////////////////////////////////////////////

template <bool EnableSsl, bool EnableUds>
class TRpcOverGrpcImpl
{
public:
    static constexpr bool AllowTransportErrors = true;
    static constexpr bool Secure = EnableSsl;
    static constexpr int MaxSimultaneousRequestCount = 1000;

    static IChannelPtr CreateChannel(
        const std::string& address,
        const std::string& /*serverAddress*/,
        THashMap<std::string, NYTree::INodePtr> grpcArguments)
    {
        auto channelConfig = New<NGrpc::TChannelConfig>();
        if (EnableSsl) {
            channelConfig->Credentials = New<NGrpc::TChannelCredentialsConfig>();
            channelConfig->Credentials->PemRootCerts = RpcCACert;
            channelConfig->Credentials->PemKeyCertPair = New<NGrpc::TSslPemKeyCertPairConfig>();
            channelConfig->Credentials->PemKeyCertPair->PrivateKey = RpcClientKey;
            channelConfig->Credentials->PemKeyCertPair->CertChain = RpcClientCert;
        }

        if (EnableUds) {
            channelConfig->Address = Format("unix:%v", address);
        } else {
            channelConfig->Address = address;
        }

        channelConfig->GrpcArguments = std::move(grpcArguments);
        return NGrpc::CreateGrpcChannel(channelConfig);
    }

    static TTestServerHostPtr CreateTestServerHost(
        NTesting::TPortHolder port,
        std::vector<IServicePtr> services,
        TServerConfigPtr serverConfig,
        TTestNodeMemoryTrackerPtr memoryUsageTracker)
    {
        auto serverAddressConfig = New<NGrpc::TServerAddressConfig>();
        if (EnableSsl) {
            serverAddressConfig->Credentials = New<NGrpc::TServerCredentialsConfig>();
            serverAddressConfig->Credentials->PemRootCerts = RpcCACert;
            serverAddressConfig->Credentials->PemKeyCertPairs.push_back(New<NGrpc::TSslPemKeyCertPairConfig>());
            serverAddressConfig->Credentials->PemKeyCertPairs[0]->PrivateKey = RpcServerKey;
            serverAddressConfig->Credentials->PemKeyCertPairs[0]->CertChain = RpcServerCert;
        }

        if (EnableUds) {
            serverAddressConfig->Address = Format("unix:localhost:%v", port);
        } else {
            serverAddressConfig->Address = Format("localhost:%v", port);
        }

        auto grpcServerConfig = New<NGrpc::TServerConfig>();
        grpcServerConfig->Addresses.push_back(serverAddressConfig);

        auto server = NGrpc::CreateServer(grpcServerConfig);
        server->Configure(serverConfig);
        return New<TTestServerHost>(
            std::move(port),
            std::move(server),
            std::move(services),
            std::move(memoryUsageTracker));
    }
};

////////////////////////////////////////////////////////////////////////////////

// TRpcOverUdsImpl creates unix domain sockets, supported only on Linux.
class TRpcOverUdsImpl
{
public:
    static constexpr bool Secure = false;
    static constexpr bool MemoryUsageTrackingEnabled = true;

    static NYT::NBus::IBusServerPtr CreateBusServer(ui16 port, IMemoryUsageTrackerPtr memoryUsageTracker)
    {
        SocketPath_ = GetWorkPath() + "/socket_" + ToString(port);
        auto config = NYT::NBus::TBusServerConfig::CreateUds(SocketPath_);
        return NYT::NBus::CreateBusServer(
            config,
            NYT::NBus::GetYTPacketTranscoderFactory(),
            memoryUsageTracker);
    }

    static IChannelPtr CreateChannel(
        const std::string& address,
        const std::string& serverAddress,
        THashMap<std::string, NYTree::INodePtr> /*grpcArguments*/)
    {
        auto config = NYT::NBus::TBusClientConfig::CreateUds(
            address == serverAddress ? SocketPath_ : address);
        auto client = CreateBusClient(config);
        return NRpc::NBus::CreateBusChannel(client);
    }

private:
    static inline std::string SocketPath_;
};

////////////////////////////////////////////////////////////////////////////////

template <bool EnableSsl>
class TRpcOverHttpImpl
{
public:
    static constexpr bool AllowTransportErrors = true;

    // NOTE: Some minor functionality is still missing from the HTTPs server.
    // TODO(melkov): Fill ssl_credentials_ext in server code and enable the Secure flag.
    static constexpr bool Secure = false;

    // HTTP will use at least two file descriptors per test connection.
    // Allow tests to run when the limit for the file descriptors is low.
    static constexpr int MaxSimultaneousRequestCount = 400;

    static IChannelPtr CreateChannel(
        const std::string& address,
        const std::string& /*serverAddress*/,
        THashMap<std::string, NYTree::INodePtr> /*grpcArguments*/)
    {
        static auto poller = NConcurrency::CreateThreadPoolPoller(4, "HttpChannelTest");
        auto credentials = New<NHttps::TClientCredentialsConfig>();
        credentials->CertificateAuthority = RpcCACert;
        credentials->PrivateKey = RpcClientKey;
        credentials->CertificateChain = RpcClientCert;
        return NHttp::CreateHttpChannel(address, poller, EnableSsl, credentials);
    }

    static TTestServerHostPtr CreateTestServerHost(
        NTesting::TPortHolder port,
        std::vector<IServicePtr> services,
        TServerConfigPtr serverConfig,
        TTestNodeMemoryTrackerPtr memoryUsageTracker)
    {
        auto config = New<NHttps::TServerConfig>();
        config->Port = port;
        config->CancelFiberOnConnectionClose = true;
        config->ServerName = "HttpServerTest";

        NYT::NHttp::IServerPtr httpServer;
        if (EnableSsl) {
            config->Credentials = New<NHttps::TServerCredentialsConfig>();
            config->Credentials->PrivateKey = RpcServerKey;
            config->Credentials->CertificateChain = RpcServerCert;
            httpServer = NYT::NHttps::CreateServer(config, 4);
        } else {
            httpServer = NYT::NHttp::CreateServer(config, 4);
        }

        auto httpRpcServer = NYT::NRpc::NHttp::CreateServer(httpServer);
        httpRpcServer->Configure(serverConfig);
        return New<TTestServerHost>(
            std::move(port),
            httpRpcServer,
            services,
            memoryUsageTracker);
    }
};

////////////////////////////////////////////////////////////////////////////////

using TAllTransports = ::testing::Types<
#ifdef _linux_
    TRpcOverBus<TRpcOverUdsImpl>,
#endif
    TRpcOverBus<TRpcOverBusImpl<false, false>>,
    TRpcOverBus<TRpcOverBusImpl<false, true>>,
    TRpcOverBus<TRpcOverBusImpl<true, false>>,
    TRpcOverBus<TRpcOverBusImpl<true, true>>,
    TRpcOverGrpcImpl<false, false>,
    TRpcOverGrpcImpl<false, true>,
    TRpcOverGrpcImpl<true, false>,
    TRpcOverGrpcImpl<true, true>,
    TRpcOverHttpImpl<false>,
    TRpcOverHttpImpl<true>
>;

using TWithAttachments = ::testing::Types<
#ifdef _linux_
    TRpcOverBus<TRpcOverUdsImpl>,
#endif
    TRpcOverBus<TRpcOverBusImpl<false, false>>,
    TRpcOverBus<TRpcOverBusImpl<false, true>>,
    TRpcOverBus<TRpcOverBusImpl<true, false>>,
    TRpcOverBus<TRpcOverBusImpl<true, true>>,
    TRpcOverGrpcImpl<false, false>,
    TRpcOverGrpcImpl<false, true>,
    TRpcOverGrpcImpl<true, false>,
    TRpcOverGrpcImpl<true, true>
>;

using TWithoutUds = ::testing::Types<
    TRpcOverBus<TRpcOverBusImpl<false, false>>,
    TRpcOverBus<TRpcOverBusImpl<false, true>>,
    TRpcOverBus<TRpcOverBusImpl<true, false>>,
    TRpcOverBus<TRpcOverBusImpl<true, true>>,
    TRpcOverGrpcImpl<false, false>,
    TRpcOverGrpcImpl<true, false>,
    TRpcOverHttpImpl<false>,
    TRpcOverHttpImpl<true>
>;

using TWithoutGrpc = ::testing::Types<
#ifdef _linux_
    TRpcOverBus<TRpcOverUdsImpl>,
#endif
    TRpcOverBus<TRpcOverBusImpl<false, false>>,
    TRpcOverBus<TRpcOverBusImpl<false, true>>,
    TRpcOverBus<TRpcOverBusImpl<true, false>>,
    TRpcOverBus<TRpcOverBusImpl<true, true>>
>;

using TGrpcOnly = ::testing::Types<
    TRpcOverGrpcImpl<false, false>,
    TRpcOverGrpcImpl<false, true>,
    TRpcOverGrpcImpl<true, false>,
    TRpcOverGrpcImpl<true, true>
>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

#include "server.h"
#include "config.h"
#include "server.h"
#include "connection.h"
#include "dispatcher_impl.h"

#include <yt/yt/core/bus/bus.h>
#include <yt/yt/core/bus/server.h>
#include <yt/yt/core/bus/private.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/socket.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/concurrency/pollable_detail.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>
#include <library/cpp/yt/threading/spin_lock.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <cerrno>

namespace NYT::NBus {

using namespace NYTree;
using namespace NConcurrency;
using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

class TTcpBusServerBase
    : public TPollableBase
{
public:
    TTcpBusServerBase(
        TBusServerConfigPtr config,
        IPollerPtr poller,
        IMessageHandlerPtr handler,
        IPacketTranscoderFactory* packetTranscoderFactory,
        IMemoryUsageTrackerPtr memoryUsageTracker)
        : Config_(std::move(config))
        , Poller_(std::move(poller))
        , Handler_(std::move(handler))
        , PacketTranscoderFactory_(std::move(packetTranscoderFactory))
        , MemoryUsageTracker_(std::move(memoryUsageTracker))
    {
        YT_VERIFY(Config_);
        YT_VERIFY(Poller_);
        YT_VERIFY(Handler_);
        YT_VERIFY(MemoryUsageTracker_);

        if (Config_->Port) {
            Logger.AddTag("ServerPort: %v", *Config_->Port);
        }
        if (Config_->UnixDomainSocketPath) {
            Logger.AddTag("UnixDomainSocketPath: %v", *Config_->UnixDomainSocketPath);
        }
    }

    ~TTcpBusServerBase()
    {
        CloseServerSocket();
    }

    void Start()
    {
        OpenServerSocket();
        if (!Poller_->TryRegister(this)) {
            CloseServerSocket();
            THROW_ERROR_EXCEPTION("Cannot register server pollable");
        }
        ArmPoller();
    }

    TFuture<void> Stop()
    {
        YT_LOG_INFO("Stopping Bus server");
        UnarmPoller();
        return Poller_->Unregister(this).Apply(BIND([this, this_ = MakeStrong(this)] {
            YT_LOG_INFO("Bus server stopped");
        }));
    }

    // IPollable implementation.
    const TString& GetLoggingTag() const override
    {
        return Logger.GetTag();
    }

    void OnEvent(EPollControl /*control*/) override
    {
        OnAccept();
    }

    void OnShutdown() override
    {
        {
            auto guard = Guard(ControlSpinLock_);
            CloseServerSocket();
        }

        decltype(Connections_) connections;
        {
            auto guard = WriterGuard(ConnectionsSpinLock_);
            std::swap(connections, Connections_);
        }

        for (const auto& connection : connections) {
            connection->Terminate(TError(
                NRpc::EErrorCode::TransportError,
                "Bus server terminated"));
        }
    }

protected:
    const TBusServerConfigPtr Config_;
    const IPollerPtr Poller_;
    const IMessageHandlerPtr Handler_;

    IPacketTranscoderFactory* const PacketTranscoderFactory_;

    const IMemoryUsageTrackerPtr MemoryUsageTracker_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ControlSpinLock_);
    SOCKET ServerSocket_ = INVALID_SOCKET;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ConnectionsSpinLock_);
    THashSet<TTcpConnectionPtr> Connections_;

    NLogging::TLogger Logger = BusLogger();

    virtual void CreateServerSocket() = 0;

    virtual void InitClientSocket(SOCKET clientSocket) = 0;

    void OnConnectionTerminated(const TTcpConnectionPtr& connection, const TError& /*error*/)
    {
        auto guard = WriterGuard(ConnectionsSpinLock_);
        // NB: Connection could be missing, see OnShutdown.
        Connections_.erase(connection);
    }

    void OpenServerSocket()
    {
        auto guard = Guard(ControlSpinLock_);

        YT_LOG_DEBUG("Opening server socket");

        CreateServerSocket();

        try {
            ListenSocket(ServerSocket_, Config_->MaxBacklogSize);
        } catch (const std::exception& ex) {
            CloseServerSocket();
            throw;
        }

        YT_LOG_DEBUG("Server socket opened");
    }

    void CloseServerSocket()
    {
        if (ServerSocket_ != INVALID_SOCKET) {
            CloseSocket(ServerSocket_);
            if (Config_->UnixDomainSocketPath) {
                unlink(Config_->UnixDomainSocketPath->c_str());
            }
            ServerSocket_ = INVALID_SOCKET;
            YT_LOG_DEBUG("Server socket closed");
        }
    }

    int GetTotalServerConnectionCount(const TString& clientNetwork)
    {
        const auto& dispatcher = TTcpDispatcher::TImpl::Get();
        int result = 0;
        for (auto encrypted : { false, true }) {
            const auto& counters = dispatcher->GetCounters(clientNetwork, encrypted);
            for (auto band : TEnumTraits<EMultiplexingBand>::GetDomainValues()) {
                result += counters->PerBandCounters[band].ServerConnections.load(std::memory_order::relaxed);
            }
        }
        return result;
    }

    void OnAccept()
    {
        while (true) {
            TNetworkAddress clientAddress;
            SOCKET clientSocket;
            try {
                clientSocket = AcceptSocket(ServerSocket_, &clientAddress);
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Error accepting client connection");
                break;
            }

            if (clientSocket == INVALID_SOCKET) {
                break;
            }

            auto rejectConnection = [&] {
                CloseSocket(clientSocket);
            };

            auto connectionId = TConnectionId::Create();

            const auto& dispatcher = TTcpDispatcher::TImpl::Get();
            auto clientNetwork = dispatcher->GetNetworkNameForAddress(clientAddress);
            auto connectionCount = GetTotalServerConnectionCount(clientNetwork);
            auto connectionLimit = Config_->MaxSimultaneousConnections;
            auto formattedClientAddress = ToString(clientAddress, NNet::TNetworkAddressFormatOptions{.IncludePort = false});
            if (connectionCount >= connectionLimit) {
                YT_LOG_WARNING("Connection dropped (Address: %v, ConnectionCount: %v, ConnectionLimit: %v)",
                    formattedClientAddress,
                    connectionCount,
                    connectionLimit);
                rejectConnection();
                continue;
            }

            YT_LOG_DEBUG("Connection accepted (ConnectionId: %v, Address: %v, Network: %v, ConnectionCount: %v, ConnectionLimit: %v)",
                connectionId,
                formattedClientAddress,
                clientNetwork,
                connectionCount,
                connectionLimit);

            InitClientSocket(clientSocket);

            auto address = ToString(clientAddress);
            auto endpointDescription = address;
            auto endpointAttributes = ConvertToAttributes(BuildYsonStringFluently()
                .BeginMap()
                    .Item("address").Value(address)
                    .Item("network").Value(clientNetwork)
                .EndMap());

            auto poller = TTcpDispatcher::TImpl::Get()->GetXferPoller();

            auto connection = New<TTcpConnection>(
                Config_,
                EConnectionType::Server,
                connectionId,
                clientSocket,
                EMultiplexingBand::Default,
                endpointDescription,
                *endpointAttributes,
                clientAddress,
                address,
                std::nullopt,
                Handler_,
                std::move(poller),
                PacketTranscoderFactory_,
                MemoryUsageTracker_);

            {
                auto guard = WriterGuard(ConnectionsSpinLock_);
                YT_VERIFY(Connections_.insert(connection).second);
            }

            connection->SubscribeTerminated(BIND_NO_PROPAGATE(
                &TTcpBusServerBase::OnConnectionTerminated,
                MakeWeak(this),
                connection));

            connection->Start();
        }
    }

    void BindSocket(const TNetworkAddress& address, const TString& errorMessage)
    {
        for (int attempt = 1; attempt <= Config_->BindRetryCount; ++attempt) {
            try {
                NNet::BindSocket(ServerSocket_, address);
                return;
            } catch (const std::exception& ex) {
                if (attempt == Config_->BindRetryCount) {
                    CloseServerSocket();

                    THROW_ERROR_EXCEPTION(NRpc::EErrorCode::TransportError, errorMessage)
                        << ex;
                } else {
                    YT_LOG_WARNING(ex, "Error binding socket, starting %v retry", attempt + 1);
                    Sleep(Config_->BindRetryBackoff);
                }
            }
        }
    }

    void ArmPoller()
    {
        auto guard = Guard(ControlSpinLock_);
        if (ServerSocket_ != INVALID_SOCKET) {
            Poller_->Arm(ServerSocket_, this, EPollControl::Read | EPollControl::EdgeTriggered);
        }
    }

    void UnarmPoller()
    {
        auto guard = Guard(ControlSpinLock_);
        if (ServerSocket_ != INVALID_SOCKET) {
            Poller_->Unarm(ServerSocket_, this);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRemoteTcpBusServer
    : public TTcpBusServerBase
{
public:
    using TTcpBusServerBase::TTcpBusServerBase;

private:
    void CreateServerSocket() override
    {
        ServerSocket_ = CreateTcpServerSocket();

        auto serverAddress = TNetworkAddress::CreateIPv6Any(*Config_->Port);
        BindSocket(serverAddress, Format("Failed to bind a server socket to port %v", Config_->Port));
    }

    void InitClientSocket(SOCKET clientSocket) override
    {
        if (Config_->EnableNoDelay) {
            if (!TrySetSocketNoDelay(clientSocket)) {
                YT_LOG_DEBUG("Failed to set socket no delay option");
            }
        }

        if (!TrySetSocketKeepAlive(clientSocket)) {
            YT_LOG_DEBUG("Failed to set socket keep alive option");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TLocalTcpBusServer
    : public TTcpBusServerBase
{
public:
    TLocalTcpBusServer(
        TBusServerConfigPtr config,
        IPollerPtr poller,
        IMessageHandlerPtr handler,
        IPacketTranscoderFactory* packetTranscoderFactory,
        IMemoryUsageTrackerPtr memoryUsageTracker)
        : TTcpBusServerBase(
            std::move(config),
            std::move(poller),
            std::move(handler),
            packetTranscoderFactory,
            std::move(memoryUsageTracker))
    { }

private:
    void CreateServerSocket() override
    {
        ServerSocket_ = CreateUnixServerSocket();

        {
            TNetworkAddress netAddress;
            if (Config_->UnixDomainSocketPath) {
                // NB(gritukan): Unix domain socket path cannot be longer than 108 symbols, so let's try to shorten it.
                netAddress = TNetworkAddress::CreateUnixDomainSocketAddress(NFS::GetShortestPath(*Config_->UnixDomainSocketPath));
            } else {
                netAddress = GetLocalBusAddress(*Config_->Port);
            }
            BindSocket(netAddress, "Failed to bind a local server socket");
        }
    }

    void InitClientSocket(SOCKET /*clientSocket*/) override
    { }
};

////////////////////////////////////////////////////////////////////////////////

//! A lightweight proxy controlling the lifetime of a TCP bus server.
/*!
 *  When the last strong reference vanishes, it unregisters the underlying
 *  server instance.
 */
template <class TServer>
class TTcpBusServerProxy
    : public IBusServer
{
public:
    explicit TTcpBusServerProxy(
        TBusServerConfigPtr config,
        IPacketTranscoderFactory* packetTranscoderFactory,
        IMemoryUsageTrackerPtr memoryUsageTracker)
        : Config_(std::move(config))
        , PacketTranscoderFactory_(packetTranscoderFactory)
        , MemoryUsageTracker_(std::move(memoryUsageTracker))
    {
        YT_VERIFY(Config_);
        YT_VERIFY(MemoryUsageTracker_);
    }

    ~TTcpBusServerProxy()
    {
        YT_UNUSED_FUTURE(Stop());
    }

    void Start(IMessageHandlerPtr handler) override
    {
        auto server = New<TServer>(
            Config_,
            TTcpDispatcher::TImpl::Get()->GetAcceptorPoller(),
            std::move(handler),
            PacketTranscoderFactory_,
            MemoryUsageTracker_);

        Server_.Store(server);
        server->Start();
    }

    TFuture<void> Stop() override
    {
        if (auto server = Server_.Exchange(nullptr)) {
            return server->Stop();
        } else {
            return VoidFuture;
        }
    }

private:
    const TBusServerConfigPtr Config_;

    IPacketTranscoderFactory* const PacketTranscoderFactory_;

    const IMemoryUsageTrackerPtr MemoryUsageTracker_;

    TAtomicIntrusivePtr<TServer> Server_;
};

////////////////////////////////////////////////////////////////////////////////

class TCompositeBusServer
    : public IBusServer
{
public:
    explicit TCompositeBusServer(std::vector<IBusServerPtr> servers)
        : Servers_(std::move(servers))
    { }

    // IBusServer implementation.

    void Start(IMessageHandlerPtr handler) override
    {
        for (const auto& server : Servers_) {
            server->Start(handler);
        }
    }

    TFuture<void> Stop() override
    {
        std::vector<TFuture<void>> asyncResults;
        for (const auto& server : Servers_) {
            asyncResults.push_back(server->Stop());
        }
        return AllSucceeded(asyncResults);
    }

private:
    const std::vector<IBusServerPtr> Servers_;
};

////////////////////////////////////////////////////////////////////////////////

IBusServerPtr CreateBusServer(
    TBusServerConfigPtr config,
    IPacketTranscoderFactory* packetTranscoderFactory,
    IMemoryUsageTrackerPtr memoryUsageTracker)
{
    std::vector<IBusServerPtr> servers;

    if (config->Port) {
        servers.push_back(
            New<TTcpBusServerProxy<TRemoteTcpBusServer>>(
                config,
                packetTranscoderFactory,
                memoryUsageTracker));
    }
#ifdef _linux_
    // Abstract unix sockets are supported only on Linux.
    servers.push_back(
        New<TTcpBusServerProxy<TLocalTcpBusServer>>(
            config,
            packetTranscoderFactory,
            memoryUsageTracker));
#endif

    return New<TCompositeBusServer>(std::move(servers));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus


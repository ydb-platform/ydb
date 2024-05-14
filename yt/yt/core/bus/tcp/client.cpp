#include "client.h"
#include "private.h"
#include "client.h"
#include "config.h"
#include "connection.h"

#include <yt/yt/core/bus/bus.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <errno.h>

#ifdef _unix_
    #include <netinet/tcp.h>
    #include <sys/socket.h>
#endif

namespace NYT::NBus {

using namespace NNet;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = BusLogger;

////////////////////////////////////////////////////////////////////////////////

//! A lightweight proxy controlling the lifetime of client #TTcpConnection.
/*!
 *  When the last strong reference vanishes, it calls IBus::Terminate
 *  for the underlying connection.
 */
class TTcpClientBusProxy
    : public IBus
{
public:
    explicit TTcpClientBusProxy(TTcpConnectionPtr connection)
        : Connection_(std::move(connection))
    { }

    ~TTcpClientBusProxy()
    {
        VERIFY_THREAD_AFFINITY_ANY();
        Connection_->Terminate(TError(NBus::EErrorCode::TransportError, "Bus terminated"));
    }

    const TString& GetEndpointDescription() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Connection_->GetEndpointDescription();
    }

    const IAttributeDictionary& GetEndpointAttributes() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Connection_->GetEndpointAttributes();
    }

    const TString& GetEndpointAddress() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Connection_->GetEndpointAddress();
    }

    const NNet::TNetworkAddress& GetEndpointNetworkAddress() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Connection_->GetEndpointNetworkAddress();
    }

    bool IsEndpointLocal() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Connection_->IsEndpointLocal();
    }

    bool IsEncrypted() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Connection_->IsEncrypted();
    }

    TBusNetworkStatistics GetNetworkStatistics() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Connection_->GetNetworkStatistics();
    }

    TFuture<void> GetReadyFuture() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Connection_->GetReadyFuture();
    }

    TFuture<void> Send(TSharedRefArray message, const TSendOptions& options) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Connection_->Send(std::move(message), options);
    }

    void SetTosLevel(TTosLevel tosLevel) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Connection_->SetTosLevel(tosLevel);
    }

    void Terminate(const TError& error) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        Connection_->Terminate(error);
    }

    void SubscribeTerminated(const TCallback<void(const TError&)>& callback) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        Connection_->SubscribeTerminated(callback);
    }

    void UnsubscribeTerminated(const TCallback<void(const TError&)>& callback) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        Connection_->UnsubscribeTerminated(callback);
    }

private:
    const TTcpConnectionPtr Connection_;
};

////////////////////////////////////////////////////////////////////////////////

class TTcpBusClient
    : public IBusClient
{
public:
    TTcpBusClient(
        TBusClientConfigPtr config,
        IPacketTranscoderFactory* packetTranscoderFactory,
        IMemoryUsageTrackerPtr memoryUsageTracker)
        : Config_(std::move(config))
        , PacketTranscoderFactory_(packetTranscoderFactory)
        , MemoryUsageTracker_(std::move(memoryUsageTracker))
    {
        if (Config_->Address) {
            EndpointDescription_ = *Config_->Address;
        } else if (Config_->UnixDomainSocketPath) {
            EndpointDescription_ = Format("unix://%v", *Config_->UnixDomainSocketPath);
        }

        EndpointAttributes_ = ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Item("address").Value(EndpointDescription_)
                .Item("encryption_mode").Value(Config_->EncryptionMode)
                .Item("verification_mode").Value(Config_->VerificationMode)
            .EndMap());
    }

    const TString& GetEndpointDescription() const override
    {
        return EndpointDescription_;
    }

    const IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes_;
    }

    IBusPtr CreateBus(IMessageHandlerPtr handler, const TCreateBusOptions& options) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto id = TConnectionId::Create();

        YT_LOG_DEBUG("Connecting to server (Address: %v, ConnectionId: %v, MultiplexingBand: %v, EncryptionMode: %v, VerificationMode: %v)",
            EndpointDescription_,
            id,
            options.MultiplexingBand,
            Config_->EncryptionMode,
            Config_->VerificationMode);

        auto endpointAttributes = ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Items(*EndpointAttributes_)
                .Item("connection_id").Value(id)
                .Item("connection_type").Value(EConnectionType::Client)
            .EndMap());

        auto poller = TTcpDispatcher::TImpl::Get()->GetXferPoller();

        auto connection = New<TTcpConnection>(
            Config_,
            EConnectionType::Client,
            id,
            INVALID_SOCKET,
            options.MultiplexingBand,
            EndpointDescription_,
            *endpointAttributes,
            TNetworkAddress(),
            Config_->Address,
            Config_->UnixDomainSocketPath,
            std::move(handler),
            std::move(poller),
            PacketTranscoderFactory_,
            MemoryUsageTracker_);
        connection->Start();

        return New<TTcpClientBusProxy>(std::move(connection));
    }

private:
    const TBusClientConfigPtr Config_;

    IPacketTranscoderFactory* const PacketTranscoderFactory_;

    const IMemoryUsageTrackerPtr MemoryUsageTracker_;

    TString EndpointDescription_;
    IAttributeDictionaryPtr EndpointAttributes_;
};

////////////////////////////////////////////////////////////////////////////////

IBusClientPtr CreateBusClient(
    TBusClientConfigPtr config,
    IPacketTranscoderFactory* packetTranscoderFactory,
    IMemoryUsageTrackerPtr memoryUsageTracker)
{
    return New<TTcpBusClient>(
        std::move(config),
        packetTranscoderFactory,
        std::move(memoryUsageTracker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus

#include "local_bypass.h"

#include <yt/yt/core/bus/bus.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/net/address.h>

namespace NYT::NBus {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TLocalBypassReplyBus
    : public IBus
{
public:
    TLocalBypassReplyBus(
        const NNet::TNetworkAddress& localAddress,
        ILocalMessageHandlerPtr serverHandler,
        IMessageHandlerPtr clientHandler)
        : LocalAddress_(localAddress)
        , ServerHandler_(std::move(serverHandler))
        , ClientHandler_(std::move(clientHandler))
        , EndpointDescription_(Format("local-bypass:%v", LocalAddress_))
        , EndpointAttributes_(ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Item("local_bypass").Value(true)
                .Item("address").Value(EndpointDescription_)
            .EndMap()))
        , ServerHandlerTerminatedCallback_(BIND(&TLocalBypassReplyBus::OnServerHandlerTerminated, MakeWeak(this)))
    {
        ServerHandler_->SubscribeTerminated(ServerHandlerTerminatedCallback_);
    }

    ~TLocalBypassReplyBus()
    {
        ServerHandler_->UnsubscribeTerminated(ServerHandlerTerminatedCallback_);
    }

    // IBus overrides.
    const std::string& GetEndpointDescription() const override
    {
        return EndpointDescription_;
    }

    const NYTree::IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes_;
    }

    TBusNetworkStatistics GetNetworkStatistics() const override
    {
        return {};
    }

    const std::string& GetEndpointAddress() const override
    {
        return EndpointDescription_;
    }

    const NNet::TNetworkAddress& GetEndpointNetworkAddress() const override
    {
        return LocalAddress_;
    }

    bool IsEndpointLocal() const override
    {
        return true;
    }

    bool IsEncrypted() const override
    {
        return false;
    }

    TFuture<void> GetReadyFuture() const override
    {
        return VoidFuture;
    }

    TFuture<void> Send(TSharedRefArray message, const NBus::TSendOptions& /*options*/) override
    {
        ClientHandler_->HandleMessage(std::move(message), /*replyBus*/ nullptr);
        return VoidFuture;
    }

    void SetTosLevel(TTosLevel /*tosLevel*/) override
    { }

    void Terminate(const TError& error) override
    {
        TerminatedList_.Fire(error);
    }

    void SubscribeTerminated(const TCallback<void(const TError&)>& callback) override
    {
        TerminatedList_.Subscribe(callback);
    }

    void UnsubscribeTerminated(const TCallback<void(const TError&)>& callback) override
    {
        TerminatedList_.Unsubscribe(callback);
    }

private:
    const NNet::TNetworkAddress LocalAddress_;
    const ILocalMessageHandlerPtr ServerHandler_;
    const IMessageHandlerPtr ClientHandler_;
    const std::string EndpointDescription_;
    const IAttributeDictionaryPtr EndpointAttributes_;
    const TCallback<void(const TError&)> ServerHandlerTerminatedCallback_;

    TSingleShotCallbackList<void(const TError&)> TerminatedList_;

    void OnServerHandlerTerminated(const TError& error)
    {
        TerminatedList_.Fire(error);
    }
};

////////////////////////////////////////////////////////////////////////////////

IBusPtr CreateLocalBypassReplyBus(
    const NNet::TNetworkAddress& localAddress,
    ILocalMessageHandlerPtr serverHandler,
    IMessageHandlerPtr clientHandler)
{
    return New<TLocalBypassReplyBus>(
        localAddress,
        std::move(serverHandler),
        std::move(clientHandler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus


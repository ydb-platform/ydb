#include "credentials_injecting_channel.h"

#include "authentication_options.h"

#include <yt/yt/core/rpc/client.h>
#include <yt/yt/core/rpc/channel_detail.h>

#include <yt/yt/library/tvm/tvm_base.h>

#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

namespace NYT::NAuth {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

NRpc::IChannelPtr CreateCredentialsInjectingChannel(
    NRpc::IChannelPtr underlyingChannel,
    const TAuthenticationOptions& options)
{
    if (options.Token) {
        return CreateTokenInjectingChannel(
            underlyingChannel,
            options);
    } else if (options.SessionId || options.SslSessionId) {
        return CreateCookieInjectingChannel(
            underlyingChannel,
            options);
    } else if (options.ServiceTicketAuth) {
        return CreateServiceTicketInjectingChannel(
            underlyingChannel,
            options);
    } else if (options.UserTicket) {
        return CreateUserTicketInjectingChannel(
            underlyingChannel,
            options);
    } else {
        return CreateUserInjectingChannel(underlyingChannel, options);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TUserInjectingChannel
    : public TChannelWrapper
{
public:
    TUserInjectingChannel(
        IChannelPtr underlyingChannel,
        const TAuthenticationOptions& options)
        : TChannelWrapper(std::move(underlyingChannel))
        , User_(options.User)
        , UserTag_(options.UserTag)
    { }

    IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override
    {
        try {
            DoInject(request);
        } catch (const std::exception& ex) {
            responseHandler->HandleError(TError(ex));
            return nullptr;
        }

        return TChannelWrapper::Send(
            std::move(request),
            std::move(responseHandler),
            options);
    }

protected:
    virtual void DoInject(const IClientRequestPtr& request)
    {
        if (User_) {
            request->SetUser(*User_);
        }
        if (UserTag_ && UserTag_ != User_) {
            request->SetUserTag(*UserTag_);
        }
    }

private:
    const std::optional<TString> User_;
    const std::optional<TString> UserTag_;
};

IChannelPtr CreateUserInjectingChannel(
    IChannelPtr underlyingChannel,
    const TAuthenticationOptions& options)
{
    YT_VERIFY(underlyingChannel);
    return New<TUserInjectingChannel>(std::move(underlyingChannel), options);
}

////////////////////////////////////////////////////////////////////////////////

class TTokenInjectingChannel
    : public TUserInjectingChannel
{
public:
    TTokenInjectingChannel(
        IChannelPtr underlyingChannel,
        const TAuthenticationOptions& options)
        : TUserInjectingChannel(std::move(underlyingChannel), options)
        , Token_(*options.Token)
    { }

protected:
    void DoInject(const IClientRequestPtr& request) override
    {
        TUserInjectingChannel::DoInject(request);

        auto* ext = request->Header().MutableExtension(NRpc::NProto::TCredentialsExt::credentials_ext);
        ext->set_token(Token_);
    }

private:
    const TString Token_;
};

IChannelPtr CreateTokenInjectingChannel(
    IChannelPtr underlyingChannel,
    const TAuthenticationOptions& options)
{
    YT_VERIFY(underlyingChannel);
    YT_VERIFY(options.Token);
    return New<TTokenInjectingChannel>(
        std::move(underlyingChannel),
        options);
}

////////////////////////////////////////////////////////////////////////////////

class TCookieInjectingChannel
    : public TUserInjectingChannel
{
public:
    TCookieInjectingChannel(
        IChannelPtr underlyingChannel,
        const TAuthenticationOptions& options)
        : TUserInjectingChannel(std::move(underlyingChannel), options)
        , SessionId_(options.SessionId.value_or(TString()))
        , SslSessionId_(options.SslSessionId.value_or(TString()))
    { }

protected:
    void DoInject(const IClientRequestPtr& request) override
    {
        TUserInjectingChannel::DoInject(request);

        auto* ext = request->Header().MutableExtension(NRpc::NProto::TCredentialsExt::credentials_ext);
        ext->set_session_id(SessionId_);
        ext->set_ssl_session_id(SslSessionId_);
    }

private:
    const TString SessionId_;
    const TString SslSessionId_;
};

IChannelPtr CreateCookieInjectingChannel(
    IChannelPtr underlyingChannel,
    const TAuthenticationOptions& options)
{
    YT_VERIFY(underlyingChannel);
    YT_VERIFY(options.SessionId.has_value() || options.SslSessionId.has_value());
    return New<TCookieInjectingChannel>(
        std::move(underlyingChannel),
        options);
}

////////////////////////////////////////////////////////////////////////////////

class TServiceTicketInjectingChannel
    : public TUserInjectingChannel
{
public:
    TServiceTicketInjectingChannel(
        IChannelPtr underlyingChannel,
        const TAuthenticationOptions& options)
        : TUserInjectingChannel(std::move(underlyingChannel), options)
        , TicketAuth_(*options.ServiceTicketAuth)
    { }

protected:
    void DoInject(const IClientRequestPtr& request) override
    {
        TUserInjectingChannel::DoInject(request);

        auto* ext = request->Header().MutableExtension(NRpc::NProto::TCredentialsExt::credentials_ext);
        ext->set_service_ticket(TicketAuth_->IssueServiceTicket());
    }

private:
    NAuth::IServiceTicketAuthPtr TicketAuth_;
};

NRpc::IChannelPtr CreateServiceTicketInjectingChannel(
    NRpc::IChannelPtr underlyingChannel,
    const TAuthenticationOptions& options)
{
    YT_VERIFY(underlyingChannel);
    YT_VERIFY(options.ServiceTicketAuth && *options.ServiceTicketAuth);
    return New<TServiceTicketInjectingChannel>(
        std::move(underlyingChannel),
        options);
}

////////////////////////////////////////////////////////////////////////////////

class TUserTicketInjectingChannel
    : public TUserInjectingChannel
{
public:
    TUserTicketInjectingChannel(
        IChannelPtr underlyingChannel,
        const TAuthenticationOptions& options)
        : TUserInjectingChannel(std::move(underlyingChannel), options)
        , UserTicket_(*options.UserTicket)
    { }

protected:
    void DoInject(const IClientRequestPtr& request) override
    {
        TUserInjectingChannel::DoInject(request);

        auto* ext = request->Header().MutableExtension(NRpc::NProto::TCredentialsExt::credentials_ext);
        ext->set_user_ticket(UserTicket_);
    }

private:
    const TString UserTicket_;
};

NRpc::IChannelPtr CreateUserTicketInjectingChannel(
    NRpc::IChannelPtr underlyingChannel,
    const TAuthenticationOptions& options)
{
    YT_VERIFY(underlyingChannel);
    YT_VERIFY(options.UserTicket && *options.UserTicket);
    return New<TUserTicketInjectingChannel>(
        std::move(underlyingChannel),
        options);
}
////////////////////////////////////////////////////////////////////////////////

class TServiceTicketInjectingChannelFactory
    : public IChannelFactory
{
public:
    TServiceTicketInjectingChannelFactory(
        IChannelFactoryPtr underlyingFactory,
        IServiceTicketAuthPtr serviceTicketAuth)
        : UnderlyingFactory_(std::move(underlyingFactory))
        , ServiceTicketAuth_(std::move(serviceTicketAuth))
    { }

    IChannelPtr CreateChannel(const std::string& address) override
    {
        auto channel = UnderlyingFactory_->CreateChannel(address);
        if (!ServiceTicketAuth_) {
            return channel;
        }
        return CreateServiceTicketInjectingChannel(
            std::move(channel),
            TAuthenticationOptions::FromServiceTicketAuth(ServiceTicketAuth_));
    }

private:
    IChannelFactoryPtr UnderlyingFactory_;
    IServiceTicketAuthPtr ServiceTicketAuth_;
};

////////////////////////////////////////////////////////////////////////////////

IChannelFactoryPtr CreateServiceTicketInjectingChannelFactory(
    IChannelFactoryPtr underlyingFactory,
    IServiceTicketAuthPtr serviceTicketAuth)
{
    return New<TServiceTicketInjectingChannelFactory>(
        std::move(underlyingFactory),
        std::move(serviceTicketAuth));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth

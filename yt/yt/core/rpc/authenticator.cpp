#include "authenticator.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

class TCompositeAuthenticator
    : public IAuthenticator
{
public:
    explicit TCompositeAuthenticator(std::vector<IAuthenticatorPtr> authenticators)
        : Authenticators_(std::move(authenticators))
    { }

    bool CanAuthenticate(const TAuthenticationContext& context) override
    {
        for (const auto& authenticator : Authenticators_) {
            if (authenticator->CanAuthenticate(context)) {
                return true;
            }
        }
        return false;
    }

    TFuture<TAuthenticationResult> AsyncAuthenticate(
        const TAuthenticationContext& context) override
    {
        for (const auto& authenticator : Authenticators_) {
            if (authenticator->CanAuthenticate(context)) {
                return authenticator->AsyncAuthenticate(context);
            }
        }
        // Hypothetically some authenticator may change its opinion on whether it can authenticate request (e.g.
        // due to dynamic configuration change), so we report an error instead of crashing.
        return MakeFuture<TAuthenticationResult>(TError(
            NYT::NRpc::EErrorCode::AuthenticationError,
            "Request is missing credentials"));
    }

private:
    const std::vector<IAuthenticatorPtr> Authenticators_;
};

////////////////////////////////////////////////////////////////////////////////

IAuthenticatorPtr CreateCompositeAuthenticator(
    std::vector<IAuthenticatorPtr> authenticators)
{
    return New<TCompositeAuthenticator>(std::move(authenticators));
}

////////////////////////////////////////////////////////////////////////////////

class TNoopAuthenticator
    : public IAuthenticator
{
public:
    bool CanAuthenticate(const TAuthenticationContext& /*context*/) override
    {
        return true;
    }

    TFuture<TAuthenticationResult> AsyncAuthenticate(
        const TAuthenticationContext& context) override
    {
        static const auto Realm = TString("noop");
        static const auto UserTicket = TString();
        TAuthenticationResult result{
            context.Header->has_user() ? FromProto<std::string>(context.Header->user()) : RootUserName,
            Realm,
            UserTicket,
        };
        return MakeFuture<TAuthenticationResult>(result);
    }
};

////////////////////////////////////////////////////////////////////////////////

IAuthenticatorPtr CreateNoopAuthenticator()
{
    return New<TNoopAuthenticator>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc


#pragma once

#include "public.h"

#include <yt/yt/library/tvm/public.h>

#include <yt/yt/core/rpc/authentication_identity.h>

#include <optional>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

struct TAuthenticationOptions
{
    static TAuthenticationOptions FromUser(const TString& user, const std::optional<TString>& userTag = {});
    static TAuthenticationOptions FromAuthenticationIdentity(const NRpc::TAuthenticationIdentity& identity);
    static TAuthenticationOptions FromToken(const TString& token);
    static TAuthenticationOptions FromServiceTicketAuth(const IServiceTicketAuthPtr& ticketAuth);
    static TAuthenticationOptions FromUserTicket(const TString& userTicket);

    const TString& GetAuthenticatedUser() const;
    NRpc::TAuthenticationIdentity GetAuthenticationIdentity() const;

    //! This field is not required for authentication.
    //! When not specified, user is derived from credentials. When
    //! specified, server additionally checks that #User is
    //! matching user derived from credentials.
    std::optional<TString> User;

    //! Provides an additional annotation to differentiate between
    //! various clients that authenticate via the same effective user.
    std::optional<TString> UserTag;

    std::optional<TString> Token;
    std::optional<TString> SessionId;
    std::optional<TString> SslSessionId;
    std::optional<IServiceTicketAuthPtr> ServiceTicketAuth;
    std::optional<TString> UserTicket;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth

#include "authentication_options.h"

#include <yt/yt/core/rpc/authentication_identity.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

TAuthenticationOptions TAuthenticationOptions::FromUser(const TString& user, const std::optional<TString>& userTag)
{
    return {
        .User = user,
        .UserTag = userTag.value_or(user),
    };
}

TAuthenticationOptions TAuthenticationOptions::FromAuthenticationIdentity(const NRpc::TAuthenticationIdentity& identity)
{
    return FromUser(identity.User, identity.UserTag);
}

TAuthenticationOptions TAuthenticationOptions::FromToken(const TString& token)
{
    return {
        .Token = token
    };
}

TAuthenticationOptions TAuthenticationOptions::FromServiceTicketAuth(const IServiceTicketAuthPtr& ticketAuth)
{
    return {
        .ServiceTicketAuth = ticketAuth
    };
}

TAuthenticationOptions TAuthenticationOptions::FromUserTicket(const TString& userTicket)
{
    return {
        .UserTicket = userTicket
    };
}

const TString& TAuthenticationOptions::GetAuthenticatedUser() const
{
    static const TString UnknownUser("<unknown>");
    return User ? *User : UnknownUser;
}

NRpc::TAuthenticationIdentity TAuthenticationOptions::GetAuthenticationIdentity() const
{
    if (!User) {
        THROW_ERROR_EXCEPTION("Authenticated user is not specified in client options");
    }
    return NRpc::TAuthenticationIdentity(*User, UserTag.value_or(*User));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth

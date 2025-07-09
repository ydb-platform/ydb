#include "options.h"

#include <yt/yt/library/auth/auth.h>

#include <util/string/strip.h>

#include <util/system/env.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

TClientOptions TClientOptions::FromToken(std::string token)
{
    TClientOptions options;
    options.Token = std::move(token);
    return options;
}

TClientOptions TClientOptions::FromUserAndToken(std::string user, std::string token)
{
    TClientOptions options;
    options.User = std::move(user);
    options.Token = std::move(token);
    return options;
}

TClientOptions TClientOptions::FromUser(std::string user, std::optional<std::string> userTag)
{
    TClientOptions options;
    options.User = std::move(user);
    options.UserTag = std::move(userTag);
    return options;
}

TClientOptions TClientOptions::Root()
{
    return FromUser(NRpc::RootUserName);
}

TClientOptions TClientOptions::FromAuthenticationIdentity(const NRpc::TAuthenticationIdentity& identity)
{
    return FromUser(identity.User, identity.UserTag);
}

TClientOptions TClientOptions::FromServiceTicketAuth(NAuth::IServiceTicketAuthPtr ticketAuth)
{
    TClientOptions options;
    options.ServiceTicketAuth = std::move(ticketAuth);
    return options;
}

TClientOptions TClientOptions::FromUserTicket(std::string userTicket)
{
    TClientOptions options;
    options.UserTicket = std::move(userTicket);
    return options;
}

////////////////////////////////////////////////////////////////////////////////

TClientOptions GetClientOptionsFromEnv()
{
    NApi::TClientOptions options;
    options.Token = NAuth::LoadToken();

    auto user = Strip(GetEnv("YT_USER"));
    if (!user.empty()) {
        options.User = user;
    }

    return options;
}

const TClientOptions& GetClientOptionsFromEnvStatic()
{
    static const NApi::TClientOptions options = GetClientOptionsFromEnv();
    return options;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

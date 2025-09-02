#pragma once

#include "public.h"

#include <yt/yt/library/tvm/public.h>

#include <yt/yt/core/rpc/authentication_identity.h>

#include <optional>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

struct TAuthenticationOptions
{
    const std::string& GetAuthenticatedUser() const;
    NRpc::TAuthenticationIdentity GetAuthenticationIdentity() const;

    //! This field is not required for authentication.
    //! When not specified, user is derived from credentials. When
    //! specified, server additionally checks that #User is
    //! matching user derived from credentials.
    std::optional<std::string> User;

    //! Provides an additional annotation to differentiate between
    //! various clients that authenticate via the same effective user.
    std::optional<std::string> UserTag;

    std::optional<std::string> Token;
    std::optional<std::string> SessionId;
    std::optional<std::string> SslSessionId;
    std::optional<IServiceTicketAuthPtr> ServiceTicketAuth;
    std::optional<std::string> UserTicket;

    //! Controls whether authentication commands (SetUserPassword, IssueToken, ListUserTokens, etc.) require a correct password to be used.
    bool RequirePasswordInAuthenticationCommands = true;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth

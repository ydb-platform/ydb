#pragma once

#include "public.h"

#include <yt/yt/library/auth/authentication_options.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TClientOptions
    : public NAuth::TAuthenticationOptions
{
    //! Create options for authenticating with token.
    /*!
     *  User is determined automatically.
    */
    static TClientOptions FromToken(std::string token);

    //! Create options for authenticating with token.
    /*!
     *  Usually you can use @ref NYT::NApi::TClientOptions::FromToken
    */
    static TClientOptions FromUserAndToken(std::string user, std::string token);

    //! Create options for authenticating as user.
    /*!
     *  Such options works in internal YT code and can work in tests.
     *  Production clusters reject such options.
    */
    static TClientOptions FromUser(std::string user, std::optional<std::string> userTag = {});

    //! Creates options for authenticating as root.
    /*!
     *  Such options works in internal YT code and can work in tests.
     *  Production clusters reject such options.
     */
    static TClientOptions Root();

    static TClientOptions FromAuthenticationIdentity(const NRpc::TAuthenticationIdentity& identity);

    static TClientOptions FromServiceTicketAuth(NAuth::IServiceTicketAuthPtr ticketAuth);
    static TClientOptions FromUserTicket(std::string userTicket);

    //! The cluster to which requests should be routed.
    /*!
     *  Multiproxy mode should be enabled on server side to use this option.
     *
     *  RPC proxies support multiproxy mode, which allows using a proxy from one cluster to send requests to another cluster.
     *
     *  Depending on the configuration on server side this mode may apply only to certain requests or be disabled completely.
     *  Consult your cluster administrators for details.
     */
    std::optional<std::string> MultiproxyTargetCluster;

    //! Use a separate connection for lightweight control requests.
    /**
    *  If this option is set to true, a separate connection is opened for lightweight requests (for example, ping_transaction).
    *  This is needed so that important lightweight requests do not wait for heavy requests, such as file writes, to complete.
    *  However, using this option increases the number of open TCP connections.
    */
    bool EnableControlMultiplexingBand = false;
};

////////////////////////////////////////////////////////////////////////////////

//! Fills client options from environment variable (client options is permanent for whole lifecycle of program).
/*!
 *  UserName is extracted from YT_USER env variable or uses current system username.
 *  Token is extracted from YT_TOKEN env variable or from file `~/.yt/token`.
 */
TClientOptions GetClientOptionsFromEnv();

//! Resolves options only once per launch and then returns the cached result.
const TClientOptions& GetClientOptionsFromEnvStatic();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

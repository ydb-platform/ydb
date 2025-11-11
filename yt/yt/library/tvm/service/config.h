#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

struct TTvmServiceConfig
    : public virtual NYTree::TYsonStruct
{
    bool UseTvmTool;

    // TvmClient settings
    TTvmId ClientSelfId = 0;
    std::optional<std::string> ClientDiskCacheDir;

    std::optional<std::string> TvmHost;
    std::optional<ui16> TvmPort;

    bool ClientEnableUserTicketChecking = false;
    std::string ClientBlackboxEnv;

    bool ClientEnableServiceTicketFetching = false;

    //! Do not use this option as the plaintext value of secret may be exposed via service orchid or somehow else.
    std::optional<std::string> ClientSelfSecret;

    //! Name of env variable with TVM secret. Used if ClientSelfSecret is unset.
    std::optional<std::string> ClientSelfSecretEnv;

    //! Path to TVM secret. Used if ClientSelfSecret is unset.
    std::optional<std::string> ClientSelfSecretPath;

    THashMap<std::string, TTvmId> ClientDstMap;

    bool ClientEnableServiceTicketChecking = false;

    //! If true, then checked tickets are cached, allowing us to speed up checking.
    bool EnableTicketParseCache = false;
    TDuration TicketCheckingCacheTimeout;

    std::string TvmToolSelfAlias;
    //! If not specified, get port from env variable `DEPLOY_TVM_TOOL_URL`.
    int TvmToolPort = 0;
    //! Do not use this option in production.
    //! If not specified, get token from env variable `TVMTOOL_LOCAL_AUTHTOKEN`.
    std::optional<std::string> TvmToolAuthToken;

    //! For testing only. If enabled, then a mock instead of a real TVM service will be used.
    bool EnableMock = false;

    //! If EnableMock and RequireMockSecret is true, then ensures that ClientSelfSecret is equal to
    //! "SecretPrefix-" + ToString(ClientSelfId).
    bool RequireMockSecret = true;

    REGISTER_YSON_STRUCT(TTvmServiceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTvmServiceConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth

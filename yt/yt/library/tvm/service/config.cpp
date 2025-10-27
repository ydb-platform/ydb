#include "config.h"

#include <util/system/env.h>
#include <util/stream/file.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

TTvmId TTvmServiceConfig::GetClientSelfId() const
{
    if (ClientSelfId.has_value()) {
        return *ClientSelfId;
    } else if (ClientSelfIdEnv.has_value()) {
        try {
            return FromString<TTvmId>(GetEnv(TString(*ClientSelfIdEnv)));
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Can not parse client self id from env %Qv", *ClientSelfIdEnv) << TError(ex);
        }
    } else {
        return 0;
    }
}

std::optional<std::string> TTvmServiceConfig::GetClientSelfSecret() const
{
    if (ClientSelfSecret.has_value()) {
        return ClientSelfSecret;
    } else if (ClientSelfSecretEnv.has_value()) {
        return GetEnv(TString(*ClientSelfSecretEnv));
    } else if (ClientSelfSecretPath.has_value()) {
        TFileInput input(*ClientSelfSecretPath);
        return input.ReadLine();
    } else {
        return std::nullopt;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TTvmServiceConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("use_tvm_tool", &TThis::UseTvmTool)
        .Default(false);
    registrar.Parameter("client_self_id", &TThis::ClientSelfId)
        .Optional();
    registrar.Parameter("client_self_id_env", &TThis::ClientSelfIdEnv)
        .Optional();
    registrar.Parameter("client_disk_cache_dir", &TThis::ClientDiskCacheDir)
        .Optional();
    registrar.Parameter("tvm_host", &TThis::TvmHost)
        .Optional();
    registrar.Parameter("tvm_port", &TThis::TvmPort)
        .Optional();
    registrar.Parameter("client_enable_user_ticket_checking", &TThis::ClientEnableUserTicketChecking)
        .Default(false);
    registrar.Parameter("client_blackbox_env", &TThis::ClientBlackboxEnv)
        .Default("ProdYateam");
    registrar.Parameter("client_enable_service_ticket_fetching", &TThis::ClientEnableServiceTicketFetching)
        .Default(false);
    registrar.Parameter("client_self_secret", &TThis::ClientSelfSecret)
        .Optional();
    registrar.Parameter("client_self_secret_path", &TThis::ClientSelfSecretPath)
        .Optional();
    registrar.Parameter("client_self_secret_env", &TThis::ClientSelfSecretEnv)
        .Optional();
    registrar.Parameter("client_dst_map", &TThis::ClientDstMap)
        .Optional();
    registrar.Parameter("client_enable_service_ticket_checking", &TThis::ClientEnableServiceTicketChecking)
        .Default(false);

    registrar.Parameter("enable_ticket_parse_cache", &TThis::EnableTicketParseCache)
        .Default(false);
    registrar.Parameter("ticket_checking_cache_timeout", &TThis::TicketCheckingCacheTimeout)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("tvm_tool_self_alias", &TThis::TvmToolSelfAlias)
        .Optional();
    registrar.Parameter("tvm_tool_port", &TThis::TvmToolPort)
        .Optional();
    registrar.Parameter("tvm_tool_auth_token", &TThis::TvmToolAuthToken)
        .Optional();

    registrar.Parameter("enable_mock", &TThis::EnableMock)
        .Default(false);
    registrar.Parameter("require_mock_secret", &TThis::RequireMockSecret)
        .Default(true);

    registrar.Postprocessor([] (TThis* config) {
        if (config->ClientSelfSecretEnv && config->ClientSelfSecretPath) {
            THROW_ERROR_EXCEPTION("Options \"client_self_secret_env\", \"client_self_secret_path\" "
                "cannot be used together");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth

#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_sdk_core_access.h>
#include <ydb/core/driver_lib/cli_config_base/config_base.h>
#include "cli_kicli.h"

namespace NKikimr {
namespace NDriverClient {

template <typename RequestType>
void SetToken(TClientCommand::TConfig& config, TAutoPtr<RequestType>& request) {
    if (!config.SecurityToken.empty()) {
        request->Record.SetSecurityToken(config.SecurityToken);
    }
}

void PrepareRequest(TClientCommand::TConfig& config, TAutoPtr<NMsgBusProxy::TBusRequest>& request) {
    SetToken(config, request);
}

void PrepareRequest(TClientCommand::TConfig& config, TAutoPtr<NMsgBusProxy::TBusSchemeInitRoot>& request) {
    SetToken(config, request);
}

void PrepareRequest(TClientCommand::TConfig& config, TAutoPtr<NMsgBusProxy::TBusSchemeOperation>& request) {
    SetToken(config, request);
}

void PrepareRequest(TClientCommand::TConfig& config, TAutoPtr<NMsgBusProxy::TBusSchemeDescribe>& request) {
    SetToken(config, request);
}

void PrepareRequest(TClientCommand::TConfig& config, TAutoPtr<NMsgBusProxy::TBusCmsRequest>& request) {
    SetToken(config, request);
}

void PrepareRequest(TClientCommand::TConfig& config, TAutoPtr<NMsgBusProxy::TBusConsoleRequest>& request) {
    SetToken(config, request);
}

void PrepareRequest(TClientCommand::TConfig& config, TAutoPtr<NMsgBusProxy::TBusFillNode>& request) {
    SetToken(config, request);
}

void PrepareRequest(TClientCommand::TConfig& config, TAutoPtr<NMsgBusProxy::TBusDrainNode>& request) {
    SetToken(config, request);
}

template <>
int OnMessageBus(const TClientCommand::TConfig& config, const NMsgBusProxy::TBusResponse& response) {
    const NKikimrClient::TResponse& resp(response.Record);
    if (resp.HasExecutionEngineEvaluatedResponse()) {
        NClient::TValue value = NClient::TValue::Create(resp.GetExecutionEngineEvaluatedResponse().GetValue(), resp.GetExecutionEngineEvaluatedResponse().GetType());
        Cout << value.GetValueText<NClient::TFormatJSON>({config.JsonUi64AsText, config.JsonBinaryAsBase64});
    }
    return 0;
}

std::optional<TString> AcquireSecurityToken(TClientCommand::TConfig& config) {
    if (config.SecurityToken.empty() && !config.StaticCredentials.User.empty()) {
        NYdb::TDriverConfig driverConfig;
        driverConfig.SetEndpoint(TCommandConfig::ParseServerAddress(config.Address).Address);
        NYdb::TDriver connection(driverConfig);
        NYdb::NConsoleClient::TDummyClient client(connection);

        auto credentialsProviderFactory = NYdb::CreateLoginCredentialsProviderFactory(config.StaticCredentials);
        auto loginProvider = credentialsProviderFactory->CreateProvider(client.GetCoreFacility());
        try {
            // Note: may throw exceptions
            config.SecurityToken = loginProvider->GetAuthInfo();
        } catch (...) {
            connection.Stop();
            throw;
        }
        connection.Stop();
    }

    if (!config.SecurityToken.empty()) {
        return config.SecurityToken;
    } else {
        return std::nullopt;
    }
}

int InvokeThroughKikimr(TClientCommand::TConfig& config, std::function<int(NClient::TKikimr&)> handler) {
    NClient::TKikimr kikimr(CommandConfig.ClientConfig);

    try {
        if (auto token = AcquireSecurityToken(config)) {
            kikimr.SetSecurityToken(*token);
        }
    } catch (const std::exception& e) {
        Cerr << e.what() << Endl;
        return 1;
    }

    return handler(kikimr);
}

}
}

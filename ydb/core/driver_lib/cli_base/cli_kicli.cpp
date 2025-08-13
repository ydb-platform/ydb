#include "cli_kicli.h"

namespace NKikimr {
namespace NDriverClient {

template <>
int OnMessageBus(const TClientCommand::TConfig& config, const NMsgBusProxy::TBusResponse& response) {
    const NKikimrClient::TResponse& resp(response.Record);
    if (resp.HasExecutionEngineEvaluatedResponse()) {
        NClient::TValue value = NClient::TValue::Create(resp.GetExecutionEngineEvaluatedResponse().GetValue(), resp.GetExecutionEngineEvaluatedResponse().GetType());
        Cout << value.GetValueText<NClient::TFormatJSON>({config.JsonUi64AsText, config.JsonBinaryAsBase64});
    }
    return 0;
}

int InvokeThroughKikimr(TClientCommand::TConfig& config, std::function<int(NClient::TKikimr&)> handler) {
    NClient::TKikimr kikimr(CommandConfig.ClientConfig);
    if (!config.SecurityToken.empty()) {
        kikimr.SetSecurityToken(config.SecurityToken);
    }

    if (!config.StaticCredentials.User.empty()) {
        TAutoPtr<NMsgBusProxy::TBusLoginRequest> request = new NMsgBusProxy::TBusLoginRequest();
        request.Get()->Record.SetUser(config.StaticCredentials.User);
        request.Get()->Record.SetPassword(config.StaticCredentials.Password);
        NClient::TResult result = kikimr.ExecuteRequest(request.Release()).GetValueSync();
        if (result.GetStatus() == NMsgBusProxy::MSTATUS_OK) {
            kikimr.SetSecurityToken(result.GetResponse<NMsgBusProxy::TBusResponse>().Record.GetUserToken());
            config.SecurityToken = result.GetResponse<NMsgBusProxy::TBusResponse>().Record.GetUserToken();
        } else {
            Cerr << result.GetError().GetMessage() << Endl;
            return 1;
        }
    }

    return handler(kikimr);
}

}
}

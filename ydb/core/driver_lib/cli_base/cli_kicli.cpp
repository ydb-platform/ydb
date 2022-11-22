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

void PrepareRequest(TClientCommand::TConfig& config, TAutoPtr<NMsgBusProxy::TBusWhoAmI>& request) {
    SetToken(config, request);
}

void PrepareRequest(TClientCommand::TConfig& config, TAutoPtr<NMsgBusProxy::TBusBSAdm>& request) {
    SetToken(config, request);
}

void PrepareRequest(TClientCommand::TConfig& config, TAutoPtr<NMsgBusProxy::TBusCmsRequest>& request) {
    SetToken(config, request);
}

void PrepareRequest(TClientCommand::TConfig& config, TAutoPtr<NMsgBusProxy::TBusConsoleRequest>& request) {
    SetToken(config, request);
}

void PrepareRequest(TClientCommand::TConfig& config, TAutoPtr<NMsgBusProxy::TBusTabletLocalMKQL>& request) {
    SetToken(config, request);
}

void PrepareRequest(TClientCommand::TConfig& config, TAutoPtr<NMsgBusProxy::TBusTabletLocalSchemeTx>& request) {
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

int InvokeThroughKikimr(TClientCommand::TConfig& config, std::function<int(NClient::TKikimr&)> handler) {
    NClient::TKikimr kikimr(CommandConfig.ClientConfig);
    if (!config.SecurityToken.empty()) {
        kikimr.SetSecurityToken(config.SecurityToken);
    }

    if (config.UseStaticCredentials) {
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

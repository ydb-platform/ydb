#pragma once

#include <ydb/core/tx/tx_proxy/proxy.h>
#include "msgbus_server.h"
#include "msgbus_server_proxy.h"

namespace NKikimr {
namespace NMsgBusProxy {

template <typename TDerived>
class TMessageBusServerRequestBase : public TActorBootstrapped<TDerived>, public TMessageBusSessionIdentHolder {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::MSGBUS_PROXY_ACTOR; }

    TMessageBusServerRequestBase(TBusMessageContext &msg) {
        InitSession(msg);
    }

    void HandleError(EResponseStatus status,  TEvTxUserProxy::TResultStatus::EStatus proxyStatus, const TActorContext &ctx) {
        HandleError(status, proxyStatus, TEvTxUserProxy::TResultStatus::Str(proxyStatus), ctx);
    }

    void HandleError(EResponseStatus status,  TEvTxUserProxy::TResultStatus::EStatus proxyStatus, const TString& message, const TActorContext &ctx) {
        TAutoPtr<TBusResponse> response(new TBusResponseStatus(status, message));

        if (proxyStatus != TEvTxUserProxy::TResultStatus::Unknown)
            response->Record.SetProxyErrorCode(proxyStatus);

        SendReplyAutoPtr(response);

        this->Die(ctx);
    }
};

}
}

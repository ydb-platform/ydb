#pragma once
#include "msgbus_server.h"
#include "msgbus_server_request.h"
#include "msgbus_tabletreq.h"
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/security/secure_request.h>

namespace NKikimr {
namespace NMsgBusProxy {

template <typename TBase>
class TMessageBusSecureRequest;

template <typename TDerived>
class TMessageBusSecureRequest<TMessageBusServerRequestBase<TDerived>> : public
        TSecureRequestActor<TMessageBusServerRequestBase<TMessageBusSecureRequest<TMessageBusServerRequestBase<TDerived>>>, TDerived> {
public:
    void OnAccessDenied(const TEvTicketParser::TError& error, const TActorContext& ctx) {
        TMessageBusServerRequestBase<TMessageBusSecureRequest<TMessageBusServerRequestBase<TDerived>>>::HandleError(
                    MSTATUS_ERROR,
                    TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::AccessDenied,
                    error.Message,
                    ctx);
    }

    template <typename... Args>
    TMessageBusSecureRequest(Args&&... args)
        : TSecureRequestActor<TMessageBusServerRequestBase<TMessageBusSecureRequest<TMessageBusServerRequestBase<TDerived>>>, TDerived>(std::forward<Args>(args)...)
    {
        this->SetInternalToken(this->GetInternalToken()); // No effect if token is nullptr
    }
};

template <typename TDerived, typename TTabletReplyEvent, NKikimrServices::TActivity::EType Activity>
class TMessageBusSecureRequest<TMessageBusSimpleTabletRequest<TDerived, TTabletReplyEvent, Activity>> :
        public TSecureRequestActor<
        TMessageBusSimpleTabletRequest<TDerived, TTabletReplyEvent, Activity>,
        TMessageBusSecureRequest<TMessageBusSimpleTabletRequest<TDerived, TTabletReplyEvent, Activity>>,
        TMessageBusSimpleTabletRequest<TDerived, TTabletReplyEvent, Activity>> {
public:
    void HandleError(EResponseStatus status,  TEvTxUserProxy::TResultStatus::EStatus proxyStatus, const TActorContext &ctx) {
        HandleError(status, proxyStatus, TEvTxUserProxy::TResultStatus::Str(proxyStatus), ctx);
    }

    void HandleError(EResponseStatus status,  TEvTxUserProxy::TResultStatus::EStatus proxyStatus, const TString& message, const TActorContext &ctx) {
        TAutoPtr<TBusResponse> response(new TBusResponseStatus(status, message));

        if (proxyStatus != TEvTxUserProxy::TResultStatus::Unknown)
            response->Record.SetProxyErrorCode(proxyStatus);

        TMessageBusSimpleTabletRequest<TDerived, TTabletReplyEvent, Activity>::SendReplyAutoPtr(response);

        TMessageBusSimpleTabletRequest<TDerived, TTabletReplyEvent, Activity>::Die(ctx);
    }

    void OnAccessDenied(const TEvTicketParser::TError& error, const TActorContext& ctx) {
        HandleError(
                    MSTATUS_ERROR,
                    TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::AccessDenied,
                    error.Message,
                    ctx);
    }

    template <typename... Args>
    TMessageBusSecureRequest(Args&&... args)
        : TSecureRequestActor<
          TMessageBusSimpleTabletRequest<TDerived, TTabletReplyEvent, Activity>,
          TMessageBusSecureRequest<TMessageBusSimpleTabletRequest<TDerived, TTabletReplyEvent, Activity>>,
          TMessageBusSimpleTabletRequest<TDerived, TTabletReplyEvent, Activity>>(std::forward<Args>(args)...)
    {
        this->SetInternalToken(this->GetInternalToken()); // No effect if token is nullptr
    }
};

template <typename TDerived, typename TTabletReplyEvent>
class TMessageBusSecureRequest<TMessageBusTabletRequest<TDerived, TTabletReplyEvent>> :
        public TSecureRequestActor<
        TMessageBusTabletRequest<TDerived, TTabletReplyEvent>,
        TMessageBusSecureRequest<TMessageBusTabletRequest<TDerived, TTabletReplyEvent>>,
        TMessageBusTabletRequest<TDerived, TTabletReplyEvent>> {
public:
    void HandleError(EResponseStatus status,  TEvTxUserProxy::TResultStatus::EStatus proxyStatus, const TActorContext &ctx) {
        HandleError(status, proxyStatus, TEvTxUserProxy::TResultStatus::Str(proxyStatus), ctx);
    }

    void HandleError(EResponseStatus status,  TEvTxUserProxy::TResultStatus::EStatus proxyStatus, const TString& message, const TActorContext &ctx) {
        TAutoPtr<TBusResponse> response(new TBusResponseStatus(status, message));

        if (proxyStatus != TEvTxUserProxy::TResultStatus::Unknown)
            response->Record.SetProxyErrorCode(proxyStatus);

        TMessageBusTabletRequest<TDerived, TTabletReplyEvent>::SendReplyAutoPtr(response);

        TMessageBusTabletRequest<TDerived, TTabletReplyEvent>::Die(ctx);
    }

    void OnAccessDenied(const TEvTicketParser::TError& error, const TActorContext& ctx) {
        HandleError(
                    MSTATUS_ERROR,
                    TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::AccessDenied,
                    error.Message,
                    ctx);
    }

    template <typename... Args>
    TMessageBusSecureRequest(Args&&... args)
        : TSecureRequestActor<
          TMessageBusTabletRequest<TDerived, TTabletReplyEvent>,
          TMessageBusSecureRequest<TMessageBusTabletRequest<TDerived, TTabletReplyEvent>>,
          TMessageBusTabletRequest<TDerived, TTabletReplyEvent>>(std::forward<Args>(args)...)
    {
        this->SetInternalToken(this->GetInternalToken()); // No effect if token is nullptr
    }
};

}
}

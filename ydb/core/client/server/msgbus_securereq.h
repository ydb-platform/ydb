#pragma once
#include "msgbus_server.h"
#include "msgbus_server_request.h"
#include "msgbus_tabletreq.h"
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/security/secure_request.h>
#include <ydb/core/grpc_services/grpc_request_check_actor.h>
#include <ydb/core/grpc_services/base/base.h>

namespace NKikimr {
namespace NMsgBusProxy {

template <typename TBase>
class TMessageBusSecureRequest;

template <class TBase, class TDerived>
class TMessageBusDatabaseAttributesLoader : public TBase {
public:
    template <typename... Args>
    TMessageBusDatabaseAttributesLoader(Args&&... args)
        : TBase(std::forward<Args>(args)...)
    {}

    void Bootstrap(const TActorContext& ctx) {
        if (AttributesAreLoaded) {
            TBase::Bootstrap(ctx);
        } else {
            LoadClusterAttributes();
            this->Become(&TMessageBusDatabaseAttributesLoader::LoadClusterAttributesStateFunc);
        }
    }

    void LoadClusterAttributes() {
        this->Send(NGRpcService::CreateGRpcRequestProxyId(), new NGRpcService::TEvTmpGetClusterAttributes());
    }

    void HandleClusterAttributes(NGRpcService::TEvTmpGetClusterAttributesResponse::TPtr& ev, const TActorContext& ctx) {
        AttributesAreLoaded = true;
        if (!ev->Get()->Success) {
            TEvTicketParser::TError err;
            err.Message = ev->Get()->ErrorMessage;
            err.LogMessage = ev->Get()->ErrorMessage;
            TDerived* self = static_cast<TDerived*>(this);
            self->OnAccessDenied(err, ctx);
            return;
        }

        const auto& rootAttributes = ev->Get()->RootAttributes;
        TVector<TEvTicketParser::TEvAuthorizeTicket::TEntry> entries;
        TVector<TEvTicketParser::TEvAuthorizeTicket::TEntry> clusterAccessCheckEntries = NGRpcService::GetEntriesForClusterAccessCheck(rootAttributes);
        entries.insert(entries.end(), clusterAccessCheckEntries.begin(), clusterAccessCheckEntries.end());
        if (!entries.empty()) {
            TBase::SetEntries(entries);
        }

        Bootstrap(ctx);
    }

    STRICT_STFUNC(LoadClusterAttributesStateFunc,
        HFunc(NGRpcService::TEvTmpGetClusterAttributesResponse, HandleClusterAttributes);
    )

private:
    bool AttributesAreLoaded = false;
    TString RootDatabase;
};

template <typename TDerived>
class TMessageBusSecureRequest<TMessageBusServerRequestBase<TDerived>> : public
        TMessageBusDatabaseAttributesLoader<TSecureRequestActor<TMessageBusServerRequestBase<TMessageBusSecureRequest<TMessageBusServerRequestBase<TDerived>>>, TDerived>, TDerived> {

    using TSecureRequestBase = TMessageBusDatabaseAttributesLoader<TSecureRequestActor<TMessageBusServerRequestBase<TMessageBusSecureRequest<TMessageBusServerRequestBase<TDerived>>>, TDerived>, TDerived>;
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
        : TSecureRequestBase(std::forward<Args>(args)...)
    {}

    template<typename T>
    void Become(T stateFunc) {
        IActorCallback::Become(stateFunc);
    }
};

template <typename TDerived, typename TTabletReplyEvent, NKikimrServices::TActivity::EType Activity>
class TMessageBusSecureRequest<TMessageBusSimpleTabletRequest<TDerived, TTabletReplyEvent, Activity>> :
        public TMessageBusDatabaseAttributesLoader<TSecureRequestActor<
        TMessageBusSimpleTabletRequest<TDerived, TTabletReplyEvent, Activity>,
        TMessageBusSecureRequest<TMessageBusSimpleTabletRequest<TDerived, TTabletReplyEvent, Activity>>,
        TMessageBusSimpleTabletRequest<TDerived, TTabletReplyEvent, Activity>>, TDerived> {
    using TSecureRequestBase = TMessageBusDatabaseAttributesLoader<TSecureRequestActor<
        TMessageBusSimpleTabletRequest<TDerived, TTabletReplyEvent, Activity>,
        TMessageBusSecureRequest<TMessageBusSimpleTabletRequest<TDerived, TTabletReplyEvent, Activity>>,
        TMessageBusSimpleTabletRequest<TDerived, TTabletReplyEvent, Activity>>, TDerived>;
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
        : TSecureRequestBase(std::forward<Args>(args)...)
    {}
};

template <typename TDerived, typename TTabletReplyEvent>
class TMessageBusSecureRequest<TMessageBusTabletRequest<TDerived, TTabletReplyEvent>> :
        public TMessageBusDatabaseAttributesLoader<TSecureRequestActor<
            TMessageBusTabletRequest<TDerived, TTabletReplyEvent>,
            TMessageBusSecureRequest<TMessageBusTabletRequest<TDerived, TTabletReplyEvent>>,
        TMessageBusTabletRequest<TDerived, TTabletReplyEvent>>, TDerived> {

    using TSecureRequestBase = TMessageBusDatabaseAttributesLoader<TSecureRequestActor<
            TMessageBusTabletRequest<TDerived, TTabletReplyEvent>,
            TMessageBusSecureRequest<TMessageBusTabletRequest<TDerived, TTabletReplyEvent>>,
        TMessageBusTabletRequest<TDerived, TTabletReplyEvent>>, TDerived>;
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
        : TSecureRequestBase(std::forward<Args>(args)...)
    {}
};

}
}

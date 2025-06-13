#pragma once

#include <ydb/public/api/client/nc_private/iam/profile_service.grpc.pb.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/http/http_proxy.h>
#include "oidc_settings.h"

namespace NMVP::NOIDC {

using namespace NActors;

class THandlerWhoamiExtendNebius : public NActors::TActorBootstrapped<THandlerWhoamiExtendNebius> {
private:
    using TProfileService = nebius::iam::v1::ProfileService;


protected:
    const NActors::TActorId Sender;
    const NHttp::THttpIncomingRequestPtr Request;
    const NActors::TActorId HttpProxyId;
    const TOpenIdConnectSettings Settings;
    const TCrackedPage ProtectedPage;
    TString RequestedPageScheme;
    TString AuthHeader;
    bool Secure;
    std::optional<NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr> WhoamiResponse;
    std::optional<TEvPrivate::TEvGetProfileResponse::TPtr> WhoamiExtendedInfoResponse;
    ui32 DataRequests;

public:
    THandlerWhoamiExtendNebius(const NActors::TActorId& sender,
                               const NHttp::THttpIncomingRequestPtr& request,
                               const NActors::TActorId& httpProxyId,
                               const TOpenIdConnectSettings& settings,
                               TStringBuf authHeader);
    void Bootstrap(const NActors::TActorContext& ctx);
    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event);
    void Handle(TEvPrivate::TEvGetProfileResponse::TPtr event);
    void Handle(TEvPrivate::TEvErrorResponse::TPtr event);

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            hFunc(TEvPrivate::TEvGetProfileResponse, Handle);
            hFunc(TEvPrivate::TEvErrorResponse, Handle);
            cFunc(TEvents::TEvWakeup::EventType, ReplyAndPassAway);
        }
    }


private:
    void ForwardUserRequest(TStringBuf authHeader, bool secure = false);
    void RequestWhoamiExtendedInfo();
    bool NeedSendSecureHttpRequest(const NHttp::THttpIncomingResponsePtr& response) const;
    void ComputeResponseStatus(const NJson::TJsonValue& json, TProxiedResponseParams& params) const;
    void RequestDone();
    void SetResponseStatus(const NJson::TJsonValue& json, TProxiedResponseParams& params) const;
    void ReplyAndPassAway(NHttp::THttpOutgoingResponsePtr httpResponse);
    void ReplyAndPassAway(TProxiedResponseParams& params);
    void ReplyAndPassAway();
};

} // NMVP::NOIDC

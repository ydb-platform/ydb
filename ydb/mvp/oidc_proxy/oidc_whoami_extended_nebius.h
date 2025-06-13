#pragma once

#include "openid_connect.h"
#include "oidc_settings.h"

#include <ydb/public/api/client/nc_private/iam/profile_service.grpc.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NMVP::NOIDC {

class THandlerWhoamiExtendNebius : public NActors::TActorBootstrapped<THandlerWhoamiExtendNebius> {
private:
    using TProfileService = nebius::iam::v1::ProfileService;


protected:
    const NActors::TActorId Sender;
    const NHttp::THttpIncomingRequestPtr Request;
    const NActors::TActorId HttpProxyId;
    const TOpenIdConnectSettings Settings;
    const TCrackedPage ProtectedPage;
    const TString AuthHeader;

    std::optional<NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr> YdbResponse;
    std::optional<TEvPrivate::TEvGetProfileResponse::TPtr> IamResponse;
    std::optional<TEvPrivate::TEvErrorResponse::TPtr> IamError;
    ui32 DataRequests = 0;

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
    void HandleTimeout();

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            hFunc(TEvPrivate::TEvGetProfileResponse, Handle);
            hFunc(TEvPrivate::TEvErrorResponse, Handle);
            cFunc(NActors::TEvents::TEvWakeup::EventType, HandleTimeout);
        }
    }

private:
    static void SetExtendedError(NJson::TJsonValue& root, const TStringBuf section, const TStringBuf key, const TStringBuf value);
    void ForwardUserRequest(TStringBuf authHeader, bool secure = false);
    void RequestWhoamiExtendedInfo();
    bool NeedSendSecureHttpRequest(const NHttp::THttpIncomingResponsePtr& response) const;
    void RequestDone();
    TProxiedResponseParams CreateResponseParams(NHttp::THttpIncomingResponsePtr response,
                                                NJson::TJsonValue& json,
                                                NJson::TJsonValue& errorJson) const;
    void ReplyAndPassAway(NHttp::THttpOutgoingResponsePtr httpResponse);
    void ReplyAndPassAway(TProxiedResponseParams params);
    void ReplyAndPassAway(); // construct response based on collected state
};

} // NMVP::NOIDC

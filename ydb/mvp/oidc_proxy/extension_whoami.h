#pragma once

#include "extension.h"

#include <ydb/public/api/client/nc_private/iam/v1/profile_service.grpc.pb.h>

namespace NMVP::NOIDC {

class TExtensionWhoamiWorker : public NActors::TActorBootstrapped<TExtensionWhoamiWorker> {
    using TBase = IExtension;
    using TProfileService = nebius::iam::v1::ProfileService;

    const TString AuthHeader;

    const TOpenIdConnectSettings Settings;
    TIntrusivePtr<TExtensionContext> Context;

    std::optional<TEvPrivate::TEvGetProfileResponse::TPtr> IamResponse;
    std::optional<TEvPrivate::TEvErrorResponse::TPtr> IamError;
    TDuration Timeout;

public:
    TExtensionWhoamiWorker(const TOpenIdConnectSettings& settings, const TString& authHeader, const TDuration timeout)
        : AuthHeader(authHeader)
        , Settings(settings)
        , Timeout(timeout)
    {}
    void Bootstrap();
    void Handle(TEvPrivate::TEvExtensionRequest::TPtr event);
    void Handle(TEvPrivate::TEvGetProfileResponse::TPtr event);
    void Handle(TEvPrivate::TEvErrorResponse::TPtr event);

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvGetProfileResponse, Handle);
            hFunc(TEvPrivate::TEvErrorResponse, Handle);
            hFunc(TEvPrivate::TEvExtensionRequest, Handle);
        }
    }

private:
    void PatchResponse(NJson::TJsonValue& json, NJson::TJsonValue& errorJson);
    void ApplyIfReady();
    void ApplyExtension();
    void SetExtendedError(NJson::TJsonValue& root, const TStringBuf section, const TStringBuf key, const TStringBuf value);
    void ContinueAndPassAway();
};

class TExtensionWhoami : public IExtension {
private:
    TActorId WhoamiHandlerId;

public:
    TExtensionWhoami(const TOpenIdConnectSettings& settings, const TString& authHeader, const TDuration timeout);
    void Execute(TIntrusivePtr<TExtensionContext> ctx) override;
};

} // NMVP::NOIDC

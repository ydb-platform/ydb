#pragma once

#include "extension.h"

#include <ydb/public/api/client/nc_private/iam/v1/profile_service.grpc.pb.h>

namespace NMVP::NOIDC {

class TExtensionWhoami : public TExtension {
private:
    using TBase = TExtension;
    using TProfileService = nebius::iam::v1::ProfileService;

protected:
    const TString AuthHeader;
    bool Timeout = false;

    std::optional<TEvPrivate::TEvGetProfileResponse::TPtr> IamResponse;
    std::optional<TEvPrivate::TEvErrorResponse::TPtr> IamError;

public:
    TExtensionWhoami(const TOpenIdConnectSettings& settings, const TString& authHeader)
        : TBase(settings)
        , AuthHeader(authHeader)
    {}
    void Bootstrap() override;
    void Handle(TEvPrivate::TEvExtensionRequest::TPtr event) override;
    void Handle(TEvPrivate::TEvGetProfileResponse::TPtr event);
    void Handle(TEvPrivate::TEvErrorResponse::TPtr event);

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvGetProfileResponse, Handle);
            hFunc(TEvPrivate::TEvErrorResponse, Handle);
            default:
                TBase::StateWork(ev);
                break;
        }
    }

private:
    void PatchResponse(NJson::TJsonValue& json, NJson::TJsonValue& errorJson);
    void ApplyIfReady();
    void ApplyExtension();
    void SetExtendedError(NJson::TJsonValue& root, const TStringBuf section, const TStringBuf key, const TStringBuf value);
};

} // NMVP::NOIDC

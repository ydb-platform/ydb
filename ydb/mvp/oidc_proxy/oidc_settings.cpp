#include "openid_connect.h"
#include "oidc_settings.h"
#include <library/cpp/string_utils/base64/base64.h>
#include <util/generic/string.h>

namespace NMVP::NOIDC {

TString TOpenIdConnectSettings::GetAuthorizationString() const {
    return "Basic " + Base64Encode(ClientId + ":" + ClientSecret);
}

TString TOpenIdConnectSettings::GetAuthEndpointURL() const {
    return AuthorizationServerAddress + AuthUrlPath;
}

TString TOpenIdConnectSettings::GetTokenEndpointURL() const {
    return AuthorizationServerAddress + TokenUrlPath;
}

TString TOpenIdConnectSettings::GetExchangeEndpointURL() const {
    return AuthorizationServerAddress + ExchangeUrlPath;
}

TString TOpenIdConnectSettings::GetImpersonateEndpointURL() const {
    return AuthorizationServerAddress + ImpersonateUrlPath;
}

TString TOpenIdConnectSettings::CreateNameSessionCookie() const {
    return "__Host_" + SESSION_COOKIE + "_" + HmacSHA256(ClientSecret, ClientId);
}

TString TOpenIdConnectSettings::CreateNameImpersonatedCookie() const {
    return "__Host_" + IMPERSONATED_COOKIE + "_" + HmacSHA256(ClientSecret, ClientId);
}

} // NMVP::NOIDC

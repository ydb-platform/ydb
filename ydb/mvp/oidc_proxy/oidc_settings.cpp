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

} // NMVP::NOIDC

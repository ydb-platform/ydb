#include <util/generic/string.h>
#include <library/cpp/string_utils/base64/base64.h>
#include "oidc_settings.h"

namespace NMVP {
namespace NOIDC {

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

} // NOIDC
} // NMVP

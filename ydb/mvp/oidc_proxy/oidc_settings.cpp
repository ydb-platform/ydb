#include "oidc_settings.h"
#include <library/cpp/string_utils/base64/base64.h>
#include <util/generic/string.h>

namespace NMVP::NOIDC {

const TVector<TStringBuf> TOpenIdConnectSettings::REQUEST_HEADERS_WHITE_LIST = {
    "Connection",
    "Accept",
    "Accept-Language",
    "Cache-Control",
    "Sec-Fetch-Dest",
    "Sec-Fetch-Mode",
    "Sec-Fetch-Site",
    "Sec-Fetch-User",
    "Upgrade-Insecure-Requests",
    "Content-Type",
    "Origin",
    "X-Trace-Verbosity",
    "X-Want-Trace",
    "traceparent"
};

const TVector<TStringBuf> TOpenIdConnectSettings::RESPONSE_HEADERS_WHITE_LIST = {
    "Content-Type",
    "Connection",
    "X-Worker-Name",
    "Set-Cookie",
    "Access-Control-Allow-Origin",
    "Access-Control-Allow-Credentials",
    "Access-Control-Allow-Headers",
    "Access-Control-Allow-Methods",
    "traceresponse"
};

bool TOpenIdConnectSettings::EnabledExtensionWhoami() const {
    return AccessServiceType == NMvp::nebius_v1 && !WhoamiExtendedInfoEndpoint.empty();
}

void TOpenIdConnectSettings::InitRequestTimeoutsByPath() {
    RequestTimeoutsByPath.clear();

    // For requests with enrichment (e.g. extended whoami), set an explicit timeout
    // to avoid long waits in case YDB is slow or unresponsive
    if (EnabledExtensionWhoami()) {
        for (auto path : WHOAMI_PATHS) {
            RequestTimeoutsByPath[path] = TDuration::Seconds(10);
        }
    }
}

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

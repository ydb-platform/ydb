#include "extension_final.h"

namespace NMVP::NOIDC {

// Rewrites HTML references starting with findStr + '/' to include host,
// e.g. src='/js → src='/host/js
// If the next char is a quote, adds /internal:
// e.g. src='/' → src='/host/internal/'
TString TExtensionFinal::FixReferenceInHtml(TStringBuf html, TStringBuf host, TStringBuf findStr) {
    TStringBuilder result;
    size_t n = html.find(findStr);
    if (n == TStringBuf::npos) {
        return TString(html);
    }
    size_t len = findStr.length() + 1;
    size_t pos = 0;
    while (n != TStringBuf::npos) {
        result << html.SubStr(pos, n + len - pos);
        if (html[n + len] == '/') {
            result << "/" << host;
            if (html[n + len + 1] == '\'' || html[n + len + 1] == '\"') {
                result << "/internal";
                n++;
            }
        }
        pos = n + len;
        n = html.find(findStr, pos);
    }
    result << html.SubStr(pos);
    return result;
}

TString TExtensionFinal::FixReferenceInHtml(TStringBuf html, TStringBuf host) {
    TStringBuf findString = "href=";
    auto result = FixReferenceInHtml(html, host, findString);
    findString = "src=";
    return FixReferenceInHtml(result, host, findString);
}

void TExtensionFinal::SetProxyResponseHeaders() {
    auto& params = Context->Params;

    THolder<NHttp::THeadersBuilder> headers = std::move(params->HeadersOverride);

    params->HeadersOverride = MakeHolder<NHttp::THeadersBuilder>();
    for (const auto& header : Settings.RESPONSE_HEADERS_WHITE_LIST) {
        if (headers->Has(header)) {
            params->HeadersOverride->Set(header, headers->Get(header));
        }
    }

    if (headers->Has(LOCATION_HEADER)) {
        params->HeadersOverride->Set(LOCATION_HEADER, GetFixedLocationHeader(headers->Get(LOCATION_HEADER)));
    }
}

void TExtensionFinal::SetProxyResponseBody() {
    auto& params = Context->Params;

    TStringBuf contentType = params->HeadersOverride->Get("Content-Type").NextTok(';');
    if (contentType == "text/html") {
        params->BodyOverride = FixReferenceInHtml(params->BodyOverride, params->ProtectedPage->Host);
    }
}

TString TExtensionFinal::GetFixedLocationHeader(TStringBuf location) {
    auto& page = Context->Params->ProtectedPage;
    if (location.StartsWith("//")) {
        return TStringBuilder() << '/' << (page->Scheme.empty() ? "" : TString(page->Scheme) + "://") << location.SubStr(2);
    } else if (location.StartsWith('/')) {
        return TStringBuilder() << '/'
                                << (page->Scheme.empty() ? "" : TString(page->Scheme) + "://")
                                << page->Host << location;
    } else {
        TStringBuf locScheme, locHost, locUri;
        NHttp::CrackURL(location, locScheme, locHost, locUri);
        if (!locScheme.empty()) {
            return TStringBuilder() << '/' << location;
        }
    }
    return TString(location);
}

void TExtensionFinal::Handle(TEvPrivate::TEvExtensionRequest::TPtr ev) {
    Context = std::move(ev->Get()->Context);
    if (Context->Params->StatusOverride) {
        SetProxyResponseHeaders();
        SetProxyResponseBody();
    }

    ContinueAndPassAway();
}

} // NMVP::NOIDC

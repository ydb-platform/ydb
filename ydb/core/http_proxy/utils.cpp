#include "utils.h"

#include "http_req.h"

#include <util/string/ascii.h>
#include <util/string/builder.h>
#include <util/string/strip.h>

namespace NKikimr::NHttpProxy {

namespace {

TStringBuf FirstForwardedValue(TStringBuf value) {
    return StripString(value.Before(','));
}

TString NormalizeForwardedProto(TStringBuf proto) {
    TStringBuf first = FirstForwardedValue(proto);
    TString scheme;
    scheme.reserve(first.size());
    for (char c : first) {
        scheme.push_back(AsciiToLower(c));
    }
    if (scheme != "http" && scheme != "https") {
        return {};
    }
    return scheme;
}

// Host / X-Forwarded-Host must be host[:port] (or [ipv6]:port). Reject path, query and fragment
// so they cannot leak into QueueUrl as https://evil.com/phishing#/v1/...
bool IsValidRequestHost(TStringBuf host) {
    return !host.empty() && host.find_first_of("/?#") == TStringBuf::npos;
}

} // namespace

TString MakeSqsRequestEndpoint(TStringBuf host, TStringBuf headersBlob, bool tlsSecure) {
    const NHttp::THeaders headers(headersBlob);
    if (TStringBuf forwardedHost = FirstForwardedValue(headers.Get("x-forwarded-host"));
        IsValidRequestHost(forwardedHost))
    {
        host = forwardedHost;
    }
    if (!IsValidRequestHost(host)) {
        return {};
    }
    TString scheme = tlsSecure ? "https" : "http";
    if (TStringBuf proto = headers.Get("x-forwarded-proto"); !proto.empty()) {
        if (TString normalized = NormalizeForwardedProto(proto); !normalized.empty()) {
            scheme = std::move(normalized);
        }
    }
    return TStringBuilder() << scheme << "://" << host;
}

TString MakeSqsRequestEndpoint(const THttpRequestContext& httpContext) {
    if (!httpContext.Request) {
        return {};
    }
    const auto& request = *httpContext.Request;
    const bool tlsSecure = request.Endpoint && request.Endpoint->Secure;
    return MakeSqsRequestEndpoint(request.Host, request.Headers, tlsSecure);
}

} // namespace NKikimr::NHttpProxy

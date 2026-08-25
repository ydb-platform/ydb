#include "utils.h"

#include "http_req.h"

#include <library/cpp/string_utils/url/url.h>
#include <ydb/library/http/rfc7239_forwarded.h>

#include <util/generic/maybe.h>
#include <util/string/ascii.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/strip.h>

namespace NKikimr::NHttpProxy {

TException MapToException(NYdb::EStatus status, const TString& method, size_t issueCode) {
    auto IssueCode = static_cast<NYds::EErrorCodes>(issueCode);

    switch(status) {
    case NYdb::EStatus::SUCCESS:
        return TException("", HTTP_OK);
    case NYdb::EStatus::BAD_REQUEST:
        return BadRequestExceptions(method, IssueCode);
    case NYdb::EStatus::UNAUTHORIZED:
        return UnauthorizedExceptions(method, IssueCode);
    case NYdb::EStatus::INTERNAL_ERROR:
        return InternalErrorExceptions(method, IssueCode);
    case NYdb::EStatus::OVERLOADED:
        return OverloadedExceptions(method, IssueCode);
    case NYdb::EStatus::GENERIC_ERROR:
        return GenericErrorExceptions(method, IssueCode);
    case NYdb::EStatus::PRECONDITION_FAILED:
        return PreconditionFailedExceptions(method, IssueCode);
    case NYdb::EStatus::ALREADY_EXISTS:
        return AlreadyExistsExceptions(method, IssueCode);
    case NYdb::EStatus::SCHEME_ERROR:
        return SchemeErrorExceptions(method, IssueCode);
    case NYdb::EStatus::NOT_FOUND:
        return NotFoundExceptions(method, IssueCode);
    case NYdb::EStatus::UNSUPPORTED:
        return UnsupportedExceptions(method, IssueCode);
    case NYdb::EStatus::CLIENT_UNAUTHENTICATED:
        return TException("Unauthenticated", HTTP_BAD_REQUEST);
    case NYdb::EStatus::ABORTED:
        return TException("Aborted", HTTP_BAD_REQUEST);
    case NYdb::EStatus::UNAVAILABLE:
        return TException("Unavailable", HTTP_SERVICE_UNAVAILABLE);
    case NYdb::EStatus::TIMEOUT:
        return TException("RequestExpired", HTTP_BAD_REQUEST);
    case NYdb::EStatus::BAD_SESSION:
        return TException("BadSession", HTTP_BAD_REQUEST);
    case NYdb::EStatus::SESSION_EXPIRED:
        return TException("SessionExpired", HTTP_BAD_REQUEST);
    default:
        return TException("InternalException", HTTP_INTERNAL_SERVER_ERROR);
    }
}

TString LogHttpRequestResponseCommonInfoString(const THttpRequestContext& httpContext, TInstant startTime, TStringBuf api, TStringBuf topicPath, TStringBuf method, TStringBuf userSid, int httpCode, TStringBuf httpResponseMessage) {
    const TDuration duration = TInstant::Now() - startTime;
    TStringBuilder logString;
    logString << "Request done.";
    if (!api.empty()) {
        logString << " Api [" << api << "]";
    }
    if (!method.empty()) {
        logString << " Action [" << method << "]";
    }
    if (!httpContext.UserName.empty()) {
        logString << " User [" << httpContext.UserName << "]";
    }
    if (!httpContext.DatabasePath.empty()) {
        logString << " Database [" << httpContext.DatabasePath << "]";
    }
    if (!topicPath.empty()) {
        logString << " Queue [" << topicPath << "]";
    }
    logString << " IP [" << httpContext.SourceAddress << "] Duration [" << duration.MilliSeconds() << "ms]";
    if (!userSid.empty()) {
        logString << " Subject [" << userSid << "]";
    }
    logString << " Code [" << httpCode << "]";
    if (httpCode != 200) {
        logString << " Response [" << httpResponseMessage << "]";
    }
    return logString;
}

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

// Host / X-Forwarded-Host / Forwarded host= must be host[:port] (or [ipv6]:port).
// Reject path, query and fragment so they cannot leak into QueueUrl as https://evil.com/phishing#/v1/...
bool IsValidRequestHost(TStringBuf host) {
    return !host.empty() && host.find_first_of("/?#") == TStringBuf::npos;
}

TString JoinForwardedHeaderValues(const ::NHttp::THeaders& headers) {
    TStringBuilder out;
    const auto range = headers.Headers.equal_range("forwarded");
    for (auto it = range.first; it != range.second; ++it) {
        if (!out.empty()) {
            out << ", ";
        }
        out << it->second;
    }
    return out;
}

TMaybe<ui16> TryParseTcpPort(TStringBuf value) {
    ui16 parsed = 0;
    if (!TryFromString(value, parsed) || parsed == 0) {
        return Nothing();
    }
    return parsed;
}

std::pair<TStringBuf, TMaybe<ui16>> SplitHostAndPort(TStringBuf host) {
    TStringBuf scheme;
    TStringBuf hostname;
    ui16 port = 0;
    if (TryGetSchemeHostAndPort(host, scheme, hostname, port)) {
        return {hostname, port != 0 ? TMaybe<ui16>(port) : Nothing()};
    }

    TStringBuf hostAndPort = GetHostAndPort(host);
    TStringBuf hostOnly;
    TStringBuf portStr;
    if (hostAndPort && hostAndPort.back() != ']' && hostAndPort.TryRSplit(':', hostOnly, portStr)) {
        return {hostOnly, Nothing()};
    }
    return {hostAndPort ? hostAndPort : host, Nothing()};
}

TString FormatSqsEndpoint(TStringBuf scheme, TStringBuf hostname, TMaybe<ui16> port) {
    const ui16 defaultPort = scheme == "https" ? 443 : 80;
    TStringBuilder result;
    result << scheme << "://" << hostname;
    if (port && *port != defaultPort) {
        result << ':' << *port;
    }
    return result;
}

} // namespace

TString MakeSqsRequestEndpoint(TStringBuf host, TStringBuf headersBlob, bool tlsSecure) {
    const ::NHttp::THeaders headers(headersBlob);
    const auto forwarded = ::NKikimr::NHttp::ParseRfc7239Forwarded(JoinForwardedHeaderValues(headers));

    TStringBuf hostname;
    TMaybe<ui16> hostPort;
    if (!forwarded.Host.empty()) {
        hostname = forwarded.Host;
        hostPort = forwarded.Port;
    } else {
        TStringBuf chosen = host;
        if (TStringBuf forwardedHost = FirstForwardedValue(headers.Get("x-forwarded-host"));
            IsValidRequestHost(forwardedHost))
        {
            chosen = forwardedHost;
        }
        if (!IsValidRequestHost(chosen)) {
            return {};
        }
        const auto split = SplitHostAndPort(chosen);
        hostname = split.first;
        hostPort = split.second;
    }
    if (!IsValidRequestHost(hostname)) {
        return {};
    }

    TString scheme = tlsSecure ? "https" : "http";
    if (forwarded.Proto == "http" || forwarded.Proto == "https") {
        scheme = forwarded.Proto;
    } else if (TStringBuf proto = headers.Get("x-forwarded-proto"); !proto.empty()) {
        if (TString normalized = NormalizeForwardedProto(proto); !normalized.empty()) {
            scheme = std::move(normalized);
        }
    }

    TMaybe<ui16> port = TryParseTcpPort(FirstForwardedValue(headers.Get("x-forwarded-port")));
    if (!port) {
        port = hostPort;
    }

    return FormatSqsEndpoint(scheme, hostname, port);
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

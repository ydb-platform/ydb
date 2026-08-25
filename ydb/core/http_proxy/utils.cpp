#include "utils.h"

#include "http_req.h"

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

void SkipOws(TStringBuf& s) {
    while (!s.empty() && (s[0] == ' ' || s[0] == '\t')) {
        s = s.SubStr(1);
    }
}

bool IsHttpTchar(char c) {
    return IsAsciiAlnum(c)
        || c == '!' || c == '#' || c == '$' || c == '%' || c == '&' || c == '\''
        || c == '*' || c == '+' || c == '-' || c == '.' || c == '^' || c == '_'
        || c == '`' || c == '|' || c == '~';
}

bool ParseHttpToken(TStringBuf& s, TStringBuf& token) {
    size_t n = 0;
    while (n < s.size() && IsHttpTchar(s[n])) {
        ++n;
    }
    if (n == 0) {
        return false;
    }
    token = s.SubStr(0, n);
    s = s.SubStr(n);
    return true;
}

bool ParseHttpQuotedString(TStringBuf& s, TString& value) {
    if (s.empty() || s[0] != '"') {
        return false;
    }
    s = s.SubStr(1);
    value.clear();
    while (!s.empty()) {
        const char c = s[0];
        s = s.SubStr(1);
        if (c == '"') {
            return true;
        }
        if (c == '\\') {
            if (s.empty()) {
                return false;
            }
            value.push_back(s[0]);
            s = s.SubStr(1);
            continue;
        }
        value.push_back(c);
    }
    return false;
}

bool ParseForwardedPairValue(TStringBuf& s, TString& value) {
    SkipOws(s);
    if (!s.empty() && s[0] == '"') {
        return ParseHttpQuotedString(s, value);
    }
    size_t n = 0;
    while (n < s.size() && s[n] != ';' && s[n] != ',') {
        ++n;
    }
    value = TString{StripString(s.SubStr(0, n))};
    s = s.SubStr(n);
    return !value.empty();
}

// RFC 7239: first valid host= / proto= left to right. Port is part of host (Host ABNF).
void ParseRfc7239Forwarded(TStringBuf header, TString& host, TString& proto) {
    host.clear();
    proto.clear();
    TStringBuf s = header;
    while (!s.empty()) {
        SkipOws(s);
        if (s.empty()) {
            break;
        }
        if (s[0] == ',' || s[0] == ';') {
            s = s.SubStr(1);
            continue;
        }
        TStringBuf name;
        if (!ParseHttpToken(s, name)) {
            while (!s.empty() && s[0] != ';' && s[0] != ',') {
                s = s.SubStr(1);
            }
            continue;
        }
        SkipOws(s);
        if (s.empty() || s[0] != '=') {
            continue;
        }
        s = s.SubStr(1);
        TString value;
        if (!ParseForwardedPairValue(s, value)) {
            continue;
        }
        if (host.empty() && AsciiEqualsIgnoreCase(name, "host") && IsValidRequestHost(value)) {
            host = std::move(value);
        } else if (proto.empty() && AsciiEqualsIgnoreCase(name, "proto")) {
            if (TString normalized = NormalizeForwardedProto(value); !normalized.empty()) {
                proto = std::move(normalized);
            }
        }
    }
}

TString JoinForwardedHeaderValues(const NHttp::THeaders& headers) {
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

bool TryParseTcpPort(TStringBuf value, ui16& port) {
    ui16 parsed = 0;
    if (!TryFromString(value, parsed) || parsed == 0) {
        return false;
    }
    port = parsed;
    return true;
}

void SplitHostAndPort(TStringBuf host, TStringBuf& hostname, bool& hasPort, ui16& port) {
    hostname = host;
    hasPort = false;
    port = 0;
    if (host.empty()) {
        return;
    }
    if (host[0] == '[') {
        const size_t close = host.find(']');
        if (close == TStringBuf::npos) {
            return;
        }
        hostname = host.SubStr(0, close + 1);
        const TStringBuf rest = host.SubStr(close + 1);
        if (!rest.empty() && rest[0] == ':') {
            ui16 parsed = 0;
            if (TryParseTcpPort(rest.SubStr(1), parsed)) {
                hasPort = true;
                port = parsed;
            }
        }
        return;
    }
    const size_t colon = host.find(':');
    if (colon == TStringBuf::npos || host.find(':', colon + 1) != TStringBuf::npos) {
        return;
    }
    hostname = host.SubStr(0, colon);
    ui16 parsed = 0;
    if (TryParseTcpPort(host.SubStr(colon + 1), parsed)) {
        hasPort = true;
        port = parsed;
    }
}

TString FormatSqsEndpoint(TStringBuf scheme, TStringBuf hostname, bool hasPort, ui16 port) {
    const ui16 defaultPort = scheme == "https" ? 443 : 80;
    TStringBuilder result;
    result << scheme << "://" << hostname;
    if (hasPort && port != defaultPort) {
        result << ':' << port;
    }
    return result;
}

} // namespace

TString MakeSqsRequestEndpoint(TStringBuf host, TStringBuf headersBlob, bool tlsSecure) {
    const NHttp::THeaders headers(headersBlob);
    TString rfcHost;
    TString rfcProto;
    ParseRfc7239Forwarded(JoinForwardedHeaderValues(headers), rfcHost, rfcProto);

    if (IsValidRequestHost(rfcHost)) {
        host = rfcHost;
    } else if (TStringBuf forwardedHost = FirstForwardedValue(headers.Get("x-forwarded-host"));
               IsValidRequestHost(forwardedHost))
    {
        host = forwardedHost;
    }
    if (!IsValidRequestHost(host)) {
        return {};
    }

    TStringBuf hostname;
    bool hostHasPort = false;
    ui16 hostPort = 0;
    SplitHostAndPort(host, hostname, hostHasPort, hostPort);
    if (!IsValidRequestHost(hostname)) {
        return {};
    }

    TString scheme = tlsSecure ? "https" : "http";
    if (!rfcProto.empty()) {
        scheme = rfcProto;
    } else if (TStringBuf proto = headers.Get("x-forwarded-proto"); !proto.empty()) {
        if (TString normalized = NormalizeForwardedProto(proto); !normalized.empty()) {
            scheme = std::move(normalized);
        }
    }

    bool hasPort = false;
    ui16 port = 0;
    if (ui16 forwardedPort = 0; TryParseTcpPort(FirstForwardedValue(headers.Get("x-forwarded-port")), forwardedPort)) {
        hasPort = true;
        port = forwardedPort;
    } else if (hostHasPort) {
        hasPort = true;
        port = hostPort;
    }

    return FormatSqsEndpoint(scheme, hostname, hasPort, port);
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

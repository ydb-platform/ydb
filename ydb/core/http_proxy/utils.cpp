#include "utils.h"

#include "http_req.h"

#include <ydb/core/base/path.h>

#include <util/string/ascii.h>
#include <util/string/builder.h>
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

TString ParseDatabasePathFromRequestUrl(TStringBuf url) {
    return CanonizePath(TString{url.Before('?')});
}

} // namespace NKikimr::NHttpProxy

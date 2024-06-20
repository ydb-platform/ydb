#include "helpers.h"

#include "http.h"
#include "private.h"
#include "config.h"

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/json/json_writer.h>
#include <yt/yt/core/json/json_parser.h>
#include <yt/yt/core/json/config.h>

#include <yt/yt/core/ytree/fluent.h>

#include <util/stream/buffer.h>

#include <util/generic/buffer.h>

#include <util/string/strip.h>
#include <util/string/join.h>
#include <util/string/cast.h>
#include <util/string/split.h>

namespace NYT::NHttp {

static constexpr auto& Logger = HttpLogger;

using namespace NJson;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;
using namespace NHeaders;

////////////////////////////////////////////////////////////////////////////////

void FillYTError(const THeadersPtr& headers, const TError& error)
{
    TString errorJson;
    TStringOutput errorJsonOutput(errorJson);
    auto jsonWriter = CreateJsonConsumer(&errorJsonOutput);
    Serialize(error, jsonWriter.get());
    jsonWriter->Flush();

    headers->Add(XYTErrorHeaderName, errorJson);
    headers->Add(XYTResponseCodeHeaderName, ToString(static_cast<int>(error.GetCode())));
    headers->Add(XYTResponseMessageHeaderName, EscapeHeaderValue(error.GetMessage()));
}

void FillYTErrorHeaders(const IResponseWriterPtr& rsp, const TError& error)
{
    FillYTError(rsp->GetHeaders(), error);
}

void FillYTErrorTrailers(const IResponseWriterPtr& rsp, const TError& error)
{
    FillYTError(rsp->GetTrailers(), error);
}

TError ParseYTError(const IResponsePtr& rsp, bool fromTrailers)
{
    TString source;
    const TString* errorHeader;
    if (fromTrailers) {
        static const TString TrailerSource("trailer");
        source = TrailerSource;
        errorHeader = rsp->GetTrailers()->Find(XYTErrorHeaderName);
    } else {
        static const TString HeaderSource("header");
        source = HeaderSource;
        errorHeader = rsp->GetHeaders()->Find(XYTErrorHeaderName);
    }

    TString errorJson;
    if (errorHeader) {
        errorJson = *errorHeader;
    } else {
        static const TString BodySource("body");
        source = BodySource;
        errorJson = ToString(rsp->ReadAll());
    }

    TStringInput errorJsonInput(errorJson);
    std::unique_ptr<IBuildingYsonConsumer<TError>> buildingConsumer;
    CreateBuildingYsonConsumer(&buildingConsumer, EYsonType::Node);
    try {
        ParseJson(&errorJsonInput, buildingConsumer.get());
    } catch (const std::exception& ex) {
        return TError("Failed to parse error from response")
            << TErrorAttribute("source", source)
            << ex;
    }
    return buildingConsumer->Finish();
}

class TErrorWrappingHttpHandler
    : public virtual IHttpHandler
{
public:
    explicit TErrorWrappingHttpHandler(IHttpHandlerPtr underlying)
        : Underlying_(std::move(underlying))
    { }

    void HandleRequest(
        const IRequestPtr& req,
        const IResponseWriterPtr& rsp) override
    {
        try {
            Underlying_->HandleRequest(req, rsp);
        } catch(const std::exception& ex) {
            TError error(ex);

            YT_LOG_DEBUG(error, "Error handling HTTP request (Path: %v)",
                req->GetUrl().Path);

            FillYTErrorHeaders(rsp, error);
            rsp->SetStatus(EStatusCode::InternalServerError);

            WaitFor(rsp->Close())
                .ThrowOnError();
        }
    }

private:
    const IHttpHandlerPtr Underlying_;
};

IHttpHandlerPtr WrapYTException(IHttpHandlerPtr underlying)
{
    return New<TErrorWrappingHttpHandler>(std::move(underlying));
}

static const auto HeadersWhitelist = JoinSeq(", ", std::vector<TString>{
    "Authorization",
    "Origin",
    "Content-Type",
    "Accept",
    "Cache-Control",
    "Request-Timeout",
    "X-Csrf-Token",
    "X-YT-Parameters",
    "X-YT-Parameters0",
    "X-YT-Parameters-0",
    "X-YT-Parameters1",
    "X-YT-Parameters-1",
    "X-YT-Response-Parameters",
    "X-YT-Input-Format",
    "X-YT-Input-Format0",
    "X-YT-Input-Format-0",
    "X-YT-Output-Format",
    "X-YT-Output-Format0",
    "X-YT-Output-Format-0",
    "X-YT-Header-Format",
    "X-YT-Suppress-Redirect",
    "X-YT-Omit-Trailers",
    "X-YT-Request-Format-Options",
    "X-YT-Response-Format-Options",
    "X-YT-Request-Id",
    "X-YT-Error",
    "X-YT-Response-Code",
    "X-YT-Response-Message",
    "X-YT-Trace-Id",
    "X-YT-User-Tag",
});

static const std::vector<TString> KnownHeaders = {
    AcceptHeaderName,
    AccessControlAllowCredentialsHeaderName,
    AccessControlAllowHeadersHeaderName,
    AccessControlAllowMethodsHeaderName,
    AccessControlAllowOriginHeaderName,
    AccessControlExposeHeadersHeaderName,
    AccessControlMaxAgeHeaderName,
    AuthorizationHeaderName,
    CacheControlHeaderName,
    ContentRangeHeaderName,
    ContentTypeHeaderName,
    CookieHeaderName,
    ExpiresHeaderName,
    PragmaHeaderName,
    RangeHeaderName,
    RequestTimeoutHeaderName,
    UserAgentHeaderName,
    XContentTypeOptionsHeaderName,
    XRequestTimeoutHeaderName,

    UserTicketHeaderName,
    XDnsPrefetchControlHeaderName,
    XForwardedForYHeaderName,
    XFrameOptionsHeaderName,
    XSourcePortYHeaderName,

    ProtocolVersionMajor,
    ProtocolVersionMinor,
    RequestFormatOptionsHeaderName,
    RequestIdHeaderName,
    ResponseFormatOptionsHeaderName,
    UserNameHeaderName,
    UserTagHeaderName,
    XYTErrorHeaderName,
    XYTResponseCodeHeaderName,
    XYTResponseMessageHeaderName,
    XYTSpanIdHeaderName,
    XYTTraceIdHeaderName,
};

bool MaybeHandleCors(
    const IRequestPtr& req,
    const IResponseWriterPtr& rsp,
    const TCorsConfigPtr& config)
{
    auto origin = req->GetHeaders()->Find("Origin");
    if (origin) {
        auto url = ParseUrl(*origin);

        bool allow = false;
        if (config->DisableCorsCheck) {
            allow = true;
        }

        for (const auto& host : config->HostAllowList) {
            if (host == url.Host) {
                allow = true;
            }
        }

        for (const auto& suffix : config->HostSuffixAllowList) {
            if (url.Host.EndsWith(suffix)) {
                allow = true;
            }
        }

        if (allow) {
            rsp->GetHeaders()->Add(AccessControlAllowCredentialsHeaderName, "true");
            rsp->GetHeaders()->Add(AccessControlAllowOriginHeaderName, *origin);
            rsp->GetHeaders()->Add(AccessControlAllowMethodsHeaderName, "POST, PUT, GET, OPTIONS");
            rsp->GetHeaders()->Add(AccessControlMaxAgeHeaderName, "3600");

            if (req->GetMethod() == EMethod::Options) {
                rsp->GetHeaders()->Add(AccessControlAllowHeadersHeaderName, HeadersWhitelist);
                rsp->SetStatus(EStatusCode::OK);
                WaitFor(rsp->Close())
                    .ThrowOnError();
                return true;
            } else {
                rsp->GetHeaders()->Add(AccessControlExposeHeadersHeaderName, HeadersWhitelist);
            }
        }
    }

    return false;
}

THashMap<TString, TString> ParseCookies(TStringBuf cookies)
{
    THashMap<TString, TString> map;
    size_t index = 0;
    while (index < cookies.size()) {
        auto nameStartIndex = index;
        auto nameEndIndex = cookies.find('=', index);
        if (nameEndIndex == TString::npos) {
            THROW_ERROR_EXCEPTION("Malformed cookies");
        }
        auto name = StripString(cookies.substr(nameStartIndex, nameEndIndex - nameStartIndex));

        auto valueStartIndex = nameEndIndex + 1;
        auto valueEndIndex = cookies.find(';', valueStartIndex);
        if (valueEndIndex == TString::npos) {
            valueEndIndex = cookies.size();
        }
        auto value = StripString(cookies.substr(valueStartIndex, valueEndIndex - valueStartIndex));

        map.emplace(TString(name), TString(value));

        index = valueEndIndex + 1;
    }
    return map;
}

void ProtectCsrfToken(const IResponseWriterPtr& rsp)
{
    const auto& headers = rsp->GetHeaders();

    headers->Set(PragmaHeaderName, "nocache");
    headers->Set(ExpiresHeaderName, "Thu, 01 Jan 1970 00:00:01 GMT");
    headers->Set(CacheControlHeaderName, "max-age=0, must-revalidate, proxy-revalidate, no-cache, no-store, private");
    headers->Set(XContentTypeOptionsHeaderName, "nosniff");
    headers->Set(XFrameOptionsHeaderName, "SAMEORIGIN");
    headers->Set(XDnsPrefetchControlHeaderName, "off");
}

std::optional<TString> FindHeader(const IRequestPtr& req, const TString& headerName)
{
    auto header = req->GetHeaders()->Find(headerName);
    return header ? std::make_optional(*header) : std::nullopt;
}

std::optional<TString> FindBalancerRequestId(const IRequestPtr& req)
{
    return FindHeader(req, "X-Req-Id");
}

std::optional<TString> FindBalancerRealIP(const IRequestPtr& req)
{
    const auto& headers = req->GetHeaders();

    auto forwardedFor = headers->Find(XForwardedForYHeaderName);
    auto sourcePort = headers->Find(XSourcePortYHeaderName);

    if (forwardedFor && sourcePort) {
        return Format("[%v]:%v", *forwardedFor, *sourcePort);
    }

    return {};
}

std::optional<TString> FindUserAgent(const IRequestPtr& req)
{
    return FindHeader(req, "User-Agent");
}

void SetUserAgent(const THeadersPtr& headers, const TString& value)
{
    headers->Set("User-Agent", value);
}

void ReplyJson(const IResponseWriterPtr& rsp, std::function<void(NYson::IYsonConsumer*)> producer)
{
    rsp->GetHeaders()->Set(ContentTypeHeaderName, "application/json");

    TBufferOutput out;

    auto json = NJson::CreateJsonConsumer(&out);
    producer(json.get());
    json->Flush();

    TString body;
    out.Buffer().AsString(body);
    WaitFor(rsp->WriteBody(TSharedRef::FromString(body)))
        .ThrowOnError();
}

void ReplyError(const IResponseWriterPtr& response, const TError& error)
{
    FillYTErrorHeaders(response, error);
    ReplyJson(response, [&] (NYson::IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .Value(error);
    });
}

NTracing::TTraceId GetTraceId(const IRequestPtr& req)
{
    const auto& headers = req->GetHeaders();
    auto id = headers->Find(XYTTraceIdHeaderName);
    if (!id) {
        return NTracing::InvalidTraceId;
    }

    NTracing::TTraceId traceId;
    if (!NTracing::TTraceId::FromString(*id, &traceId)) {
        return NTracing::InvalidTraceId;
    }
    return traceId;
}

void SetTraceId(const IResponseWriterPtr& rsp, NTracing::TTraceId traceId)
{
    if (traceId != NTracing::InvalidTraceId) {
        rsp->GetHeaders()->Set(XYTTraceIdHeaderName, ToString(traceId));
    }
}

void SetRequestId(const IResponseWriterPtr& rsp, NRpc::TRequestId requestId)
{
    if (requestId) {
        rsp->GetHeaders()->Set(RequestIdHeaderName, ToString(requestId));
    }
}

NTracing::TSpanId GetSpanId(const IRequestPtr& req)
{
    const auto& headers = req->GetHeaders();
    auto id = headers->Find(XYTSpanIdHeaderName);
    if (!id) {
        return NTracing::InvalidSpanId;
    }
    return IntFromString<NTracing::TSpanId, 16>(*id);
}

bool TryParseTraceParent(const TString& traceParent, NTracing::TSpanContext& spanContext)
{
    // An adaptation of https://github.com/census-instrumentation/opencensus-go/blob/ae11cd04b/plugin/ochttp/propagation/tracecontext/propagation.go#L49-L106

    auto parts = StringSplitter(traceParent).Split('-').ToList<TString>();
    if (parts.size() < 3 || parts.size() > 4) {
        return false;
    }

    // NB: we support three-part form in which version is assumed to be zero.
    ui8 version = 0;
    if (parts.size() == 4) {
        if (parts[0].size() != 2) {
            return false;
        }
        if (!TryIntFromString<10>(parts[0], version)) {
            return false;
        }
        parts.erase(parts.begin());
    }

    // Now we have exactly three parts: traceId-spanId-options.

    // Parse trace context.
    if (!TGuid::FromStringHex32(parts[0], &spanContext.TraceId)) {
        return false;
    }

    if (parts[1].size() != 16) {
        return false;
    }
    if (!TryIntFromString<16>(parts[1], spanContext.SpanId)) {
        return false;
    }

    ui8 options = 0;
    if (!TryIntFromString<16>(parts[2], options)) {
        return false;
    }
    spanContext.Sampled = static_cast<bool>(options & 1u);
    spanContext.Debug = static_cast<bool>(options & 2u);

    return true;
}

NTracing::TTraceContextPtr GetOrCreateTraceContext(const IRequestPtr& req)
{
    const auto& headers = req->GetHeaders();
    NTracing::TTraceContextPtr traceContext;
    if (auto* traceParent = headers->Find("traceparent")) {
        NTracing::TSpanContext parentSpan;
        if (TryParseTraceParent(*traceParent, parentSpan)) {
            traceContext = NTracing::TTraceContext::NewChildFromSpan(parentSpan, "HttpServer");
        }
    }

    if (!traceContext) {
        // Generate new trace context from scratch.
        traceContext = NTracing::TTraceContext::NewRoot("HttpServer");
    }

    traceContext->SetRecorded();
    return traceContext;
}

std::optional<std::pair<i64, i64>> FindBytesRange(const THeadersPtr& headers)
{
    auto range = headers->Find(RangeHeaderName);
    if (!range) {
        return {};
    }

    const TString bytesPrefix = "bytes=";
    if (!range->StartsWith(bytesPrefix)) {
        THROW_ERROR_EXCEPTION("Invalid range header format")
            << TErrorAttribute("range", *range);
    }

    auto indices = range->substr(bytesPrefix.size());
    std::pair<i64, i64> rangeValue;
    StringSplitter(indices).Split('-').CollectInto(&rangeValue.first, &rangeValue.second);
    return rangeValue;
}

void SetBytesRange(const THeadersPtr& headers, std::pair<i64, i64> range)
{
    headers->Set(ContentRangeHeaderName, Format("bytes %v-%v/*", range.first, range.second));
}

TString SanitizeUrl(const TString& url)
{
    // Do not expose URL parameters in error attributes.
    auto urlRef = ParseUrl(url);
    if (urlRef.PortStr.empty()) {
        return TString(urlRef.Host) + urlRef.Path;
    } else {
        return Format("%v:%v%v", urlRef.Host, urlRef.PortStr, urlRef.Path);
    }
}

std::vector<std::pair<TString, TString>> DumpUnknownHeaders(const THeadersPtr& headers)
{
    static const THeaders::THeaderNames known(KnownHeaders.begin(), KnownHeaders.end());
    return headers->Dump(&known);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp

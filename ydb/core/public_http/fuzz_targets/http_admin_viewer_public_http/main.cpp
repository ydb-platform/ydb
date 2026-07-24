#include <ydb/core/public_http/fq_handlers.h>
#include <ydb/core/public_http/http_req.h>
#include <ydb/core/public_http/http_router.h>
#include <ydb/core/viewer/viewer.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <library/cpp/monlib/service/mon_service_http_request.h>
#include <library/cpp/protobuf/json/json2proto.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>
#include <util/string/ascii.h>
#include <util/string/builder.h>

namespace {

class TFuzzHttpRequest : public NMonitoring::IHttpRequest {
public:
    HTTP_METHOD Method = HTTP_METHOD_GET;
    TString Uri;
    TString Path;
    TString PostContent;
    TString RemoteAddr;
    TCgiParameters Params;
    TCgiParameters PostParams;
    THttpHeaders Headers;

    const char* GetURI() const override {
        return Uri.c_str();
    }

    const char* GetPath() const override {
        return Path.c_str();
    }

    const TCgiParameters& GetParams() const override {
        return Params;
    }

    const TCgiParameters& GetPostParams() const override {
        return PostParams;
    }

    TStringBuf GetPostContent() const override {
        return PostContent;
    }

    HTTP_METHOD GetMethod() const override {
        return Method;
    }

    const THttpHeaders& GetHeaders() const override {
        return Headers;
    }

    TString GetRemoteAddr() const override {
        return RemoteAddr;
    }
};

template <typename THttpType>
void FeedInChunks(const TIntrusivePtr<THttpType>& message, TStringBuf payload, size_t maxChunk) {
    size_t cursor = 0;
    maxChunk = Max<size_t>(1, maxChunk);

    while (!payload.empty()) {
        const size_t wantedChunk = 1 + (static_cast<unsigned char>(payload[cursor % payload.size()]) % maxChunk);
        const size_t chunk = Min(payload.size(), Min(maxChunk, wantedChunk));
        message->EnsureEnoughSpaceAvailable(chunk);
        std::memcpy(message->Pos(), payload.data(), chunk);
        message->Advance(chunk);
        payload.Skip(chunk);
        ++cursor;
    }
}

TString SanitizePathSegment(TString value) {
    if (value.empty()) {
        return "0";
    }

    for (char& ch : value) {
        if (!IsAsciiAlnum(ch) && ch != '-' && ch != '_' && ch != '.') {
            ch = 'x';
        }
    }
    return value;
}

TString HttpMethodToString(HTTP_METHOD method) {
    switch (method) {
        case HTTP_METHOD_GET:
            return "GET";
        case HTTP_METHOD_POST:
            return "POST";
        case HTTP_METHOD_PUT:
            return "PUT";
        case HTTP_METHOD_DELETE:
            return "DELETE";
        case HTTP_METHOD_OPTIONS:
            return "OPTIONS";
        case HTTP_METHOD_HEAD:
            return "HEAD";
        default:
            return "GET";
    }
}

void RegisterRoutes(NKikimr::NPublicHttp::THttpRequestRouter& router) {
    using namespace NKikimr::NPublicHttp;

    IActor* const createQuery = reinterpret_cast<IActor*>(1);
    IActor* const getQuery = reinterpret_cast<IActor*>(2);
    IActor* const getQueryStatus = reinterpret_cast<IActor*>(3);
    IActor* const getResultData = reinterpret_cast<IActor*>(4);
    IActor* const stopQuery = reinterpret_cast<IActor*>(5);
    IActor* const startQuery = reinterpret_cast<IActor*>(6);

    router.RegisterHandler(HTTP_METHOD_POST, "/api/fq/v1/queries", [createQuery](const THttpRequestContext&) { return createQuery; });
    router.RegisterHandler(HTTP_METHOD_GET, "/api/fq/v1/queries/{query_id}", [getQuery](const THttpRequestContext&) { return getQuery; });
    router.RegisterHandler(HTTP_METHOD_GET, "/api/fq/v1/queries/{query_id}/status", [getQueryStatus](const THttpRequestContext&) { return getQueryStatus; });
    router.RegisterHandler(HTTP_METHOD_GET, "/api/fq/v1/queries/{query_id}/results/{result_set_index}", [getResultData](const THttpRequestContext&) { return getResultData; });
    router.RegisterHandler(HTTP_METHOD_POST, "/api/fq/v1/queries/{query_id}/stop", [stopQuery](const THttpRequestContext&) { return stopQuery; });
    router.RegisterHandler(HTTP_METHOD_POST, "/api/fq/v1/queries/{query_id}/start", [startQuery](const THttpRequestContext&) { return startQuery; });
}

template <typename THttpProto, typename TGrpcProto>
void ParseAndConvertFqRequest(TStringBuf body, const std::map<TString, TString>& pathParams, const TString& idempotencyKey) {
    THttpProto httpRequest;
    if (!body.empty()) {
        try {
            NProtobufJson::Json2Proto(TString(body), httpRequest);
        } catch (...) {
            return;
        }
    }

    if constexpr (std::is_same_v<THttpProto, FQHttp::GetQueryRequest> ||
                  std::is_same_v<THttpProto, FQHttp::GetQueryStatusRequest> ||
                  std::is_same_v<THttpProto, FQHttp::StopQueryRequest> ||
                  std::is_same_v<THttpProto, FQHttp::GetResultDataRequest>) {
        const auto it = pathParams.find("query_id");
        if (it != pathParams.end()) {
            httpRequest.set_query_id(it->second);
        }
    }

    if constexpr (std::is_same_v<THttpProto, FQHttp::GetResultDataRequest>) {
        const auto it = pathParams.find("result_set_index");
        if (it != pathParams.end()) {
            httpRequest.set_result_set_index(FromString<ui64>(it->second));
        }
    }

    TGrpcProto grpcRequest;
    try {
        NKikimr::NPublicHttp::FqConvert(httpRequest, grpcRequest);
        NKikimr::NPublicHttp::SetIdempotencyKey(grpcRequest, idempotencyKey);
    } catch (...) {
    }

    (void)grpcRequest.ByteSizeLong();
}

void ExerciseViewerRequestState(FuzzedDataProvider& fdp) {
    TFuzzHttpRequest request;
    request.Method = fdp.ConsumeBool() ? HTTP_METHOD_POST : HTTP_METHOD_GET;
    request.Path = fdp.ConsumeBool() ? "/viewer/query" : "/viewer/healthcheck";
    request.Uri = TStringBuilder() << request.Path << "?database=/Root&schema=" << SanitizePathSegment(fdp.ConsumeRandomLengthString(16));
    request.PostContent = fdp.ConsumeRandomLengthString(4096);
    request.RemoteAddr = "127.0.0.1";
    request.Headers.AddHeader("Accept", fdp.ConsumeBool() ? "application/json,text/plain" : "text/plain");
    request.Headers.AddHeader("Authorization", TStringBuilder() << "Bearer " << fdp.ConsumeRandomLengthString(48));

    NMonitoring::TMonService2HttpRequest monRequest(nullptr, &request, nullptr, nullptr, request.Path, nullptr);
    NActors::NMon::TEvHttpInfo event(monRequest, fdp.ConsumeRandomLengthString(96));
    NKikimr::NViewer::TRequestState state(&event);

    (void)state.HasHeader("Accept");
    (void)state.GetHeader("Authorization");
    (void)state.GetRemoteAddr();
    (void)state.GetUri();
    (void)state.GetUserTokenObject();
    (void)state.Accepts("application/json");
    (void)state.Accepts("text/plain");
}

void ExercisePublicHttp(FuzzedDataProvider& fdp) {
    using namespace NKikimr::NPublicHttp;

    static constexpr TStringBuf PathPatterns[] = {
        "/api/fq/v1/queries",
        "/api/fq/v1/queries/{query_id}",
        "/api/fq/v1/queries/{query_id}/status",
        "/api/fq/v1/queries/{query_id}/results/{result_set_index}",
        "/api/fq/v1/queries/{query_id}/stop",
        "/api/fq/v1/queries/{query_id}/start",
    };

    const TString queryId = SanitizePathSegment(fdp.ConsumeRandomLengthString(24));
    const ui32 resultSetIndex = fdp.ConsumeIntegralInRange<ui32>(0, 1000);
    const TString queryTail = fdp.ConsumeRandomLengthString(64);
    const TString body = fdp.ConsumeRandomLengthString(16 * 1024);
    const TString idempotencyKey = fdp.ConsumeRandomLengthString(64);

    const TStringBuf pattern = PathPatterns[fdp.ConsumeIntegralInRange<size_t>(0, Y_ARRAY_SIZE(PathPatterns) - 1)];
    HTTP_METHOD method = fdp.ConsumeBool() ? HTTP_METHOD_GET : HTTP_METHOD_POST;
    if (pattern == "/api/fq/v1/queries") {
        method = HTTP_METHOD_POST;
    }

    TString path(pattern);
    path = SubstGlobalCopy(path, TString("{query_id}"), queryId);
    const TString resultSetIndexString = ToString(resultSetIndex);
    path = SubstGlobalCopy(path, TString("{result_set_index}"), resultSetIndexString);

    const TString queryString = queryTail.empty() ? TString() : TStringBuilder() << "?database=/Root&trace=" << queryTail;
    const TString rawRequest = TStringBuilder()
        << HttpMethodToString(method) << " " << path << queryString << " HTTP/1.1\r\n"
        << "Host: fuzz.example\r\n"
        << "Accept: application/json\r\n"
        << "Authorization: Bearer " << fdp.ConsumeRandomLengthString(48) << "\r\n"
        << "X-Forwarded-For: 192.0.2.1\r\n"
        << "X-Request-Id: " << fdp.ConsumeRandomLengthString(32) << "\r\n"
        << "Idempotency-Key: " << idempotencyKey << "\r\n"
        << "Content-Type: application/json\r\n"
        << "Content-Length: " << body.size() << "\r\n"
        << "\r\n"
        << body;

    NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
    FeedInChunks(request, rawRequest, fdp.ConsumeIntegralInRange<size_t>(1, 128));

    THolder<TActorSystemSetup> setup(new TActorSystemSetup());
    TActorSystem actorSystem(setup);
    THttpRequestContext requestContext(&actorSystem, request, {}, TInstant::Now(), {});
    requestContext.SetProject(fdp.ConsumeRandomLengthString(32));
    requestContext.SetDb(fdp.ConsumeRandomLengthString(32));

    THttpRequestRouter router;
    RegisterRoutes(router);

    const TStringBuf pathOnly = request->URL.Before('?');
    auto resolved = router.ResolveHandler(request->Method, pathOnly);
    if (!resolved) {
        return;
    }

    requestContext.SetPathParams(resolved->PathParams);
    requestContext.SetPathPattern(resolved->PathPattern);

    (void)router.GetSize();
    (void)resolved->Handler(requestContext);
    (void)requestContext.GetPathParams();
    (void)requestContext.GetProject();
    (void)requestContext.GetDb();
    (void)requestContext.GetToken();
    (void)requestContext.GetContentType();
    (void)requestContext.GetIdempotencyKey();
    (void)requestContext.GetPeer();
    (void)requestContext.GetHttpRequest();

    if (resolved->PathPattern == "/api/fq/v1/queries") {
        ParseAndConvertFqRequest<FQHttp::CreateQueryRequest, FederatedQuery::CreateQueryRequest>(request->Body, resolved->PathParams, requestContext.GetIdempotencyKey());
    } else if (resolved->PathPattern == "/api/fq/v1/queries/{query_id}") {
        ParseAndConvertFqRequest<FQHttp::GetQueryRequest, FederatedQuery::DescribeQueryRequest>(request->Body, resolved->PathParams, requestContext.GetIdempotencyKey());
    } else if (resolved->PathPattern == "/api/fq/v1/queries/{query_id}/status") {
        ParseAndConvertFqRequest<FQHttp::GetQueryStatusRequest, FederatedQuery::GetQueryStatusRequest>(request->Body, resolved->PathParams, requestContext.GetIdempotencyKey());
    } else if (resolved->PathPattern == "/api/fq/v1/queries/{query_id}/results/{result_set_index}") {
        ParseAndConvertFqRequest<FQHttp::GetResultDataRequest, FederatedQuery::GetResultDataRequest>(request->Body, resolved->PathParams, requestContext.GetIdempotencyKey());
    } else if (resolved->PathPattern == "/api/fq/v1/queries/{query_id}/stop") {
        ParseAndConvertFqRequest<FQHttp::StopQueryRequest, FederatedQuery::ControlQueryRequest>(request->Body, resolved->PathParams, requestContext.GetIdempotencyKey());
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 64 * 1024) {
        return 0;
    }

    FuzzedDataProvider fdp(data, size);

    try {
        ExerciseViewerRequestState(fdp);
    } catch (...) {
    }

    try {
        ExercisePublicHttp(fdp);
    } catch (...) {
    }

    return 0;
}

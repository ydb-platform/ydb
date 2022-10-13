#include "http_req.h"

#include <library/cpp/actors/http/http_proxy.h>
#include <ydb/core/http_proxy/http_req.h>

#include <util/generic/guid.h>
#include <util/string/ascii.h>

namespace NKikimr::NPublicHttp {
    constexpr TStringBuf AUTHORIZATION_HEADER = "authorization";
    constexpr TStringBuf REQUEST_ID_HEADER = "x-request-id";
    constexpr TStringBuf REQUEST_CONTENT_TYPE_HEADER = "content-type";
    constexpr TStringBuf REQUEST_FORWARDED_FOR = "x-forwarded-for";
    constexpr TStringBuf IDEMPOTENCY_KEY_HEADER = "idempotency-key";
    
    constexpr TStringBuf APPLICATION_JSON = "application/json";

    TString GenerateRequestId(const TString& sourceReqId) {
        if (sourceReqId.empty()) {
            return CreateGuidAsString();
        }

        return TStringBuilder() << CreateGuidAsString() << "-" << sourceReqId;
    }

    TString HttpCodeFamily(TStringBuf code) {
        if (code.Size() != 3) {
            return "unknown";
        }

        return TStringBuilder() << code[0] << "xx";
    }

    THttpRequestContext::THttpRequestContext(TActorSystem* actorSystem, NHttp::THttpIncomingRequestPtr request, NActors::TActorId sender, TInstant startedAt, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters)
        : ActorSystem(actorSystem)
        , Request(request)
        , Sender(sender)
        , StartedAt(startedAt)
        , Counters(counters)
    {
        Y_VERIFY(ActorSystem);
        Y_VERIFY(Request);
        ParseHeaders(Request->Headers);
    }

    void THttpRequestContext::SetPathParams(std::map<TString, TString> pathParams) {
        PathParams = std::move(pathParams);
    }

    const std::map<TString, TString>& THttpRequestContext::GetPathParams() const {
        return PathParams;
    }

    void THttpRequestContext::SetPathPattern(const TString& pathPattern) {
        PathPattern = pathPattern;
    }

    void THttpRequestContext::SetProject(const TString& project) {
        Project = project;
    }

    TString THttpRequestContext::GetProject() const {
        return Project;
    }

    void THttpRequestContext::SetDb(const TString& db) {
        Db = db;
    }

    TString THttpRequestContext::GetDb() const {
        return Db;
    }

    TString THttpRequestContext::GetToken() const {
        return Token;
    }

    NHttp::THttpIncomingRequestPtr THttpRequestContext::GetHttpRequest() const {
        return Request;
    }

    TString THttpRequestContext::GetContentType() const {
        return ContentType;
    }

    TString THttpRequestContext::GetIdempotencyKey() const {
        return IdempotencyKey;
    }

    void THttpRequestContext::ResponseBadRequest(Ydb::StatusIds::StatusCode status, const TString& errorText) const {
        DoResponseBadRequest(status, errorText);
    }

    void THttpRequestContext::ResponseBadRequestJson(Ydb::StatusIds::StatusCode status, const TString& json) const {
        DoResponseBadRequest(status, json, APPLICATION_JSON);
    }

    void THttpRequestContext::DoResponseBadRequest(Ydb::StatusIds::StatusCode status, const TString& errorText, TStringBuf contentType) const {
        const NYdb::EStatus ydbStatus = static_cast<NYdb::EStatus>(status);
        const TString httpCodeStr = ToString((int)NKikimr::NHttpProxy::StatusToHttpCode(ydbStatus));
        DoResponse(httpCodeStr, NKikimr::NHttpProxy::StatusToErrorType(ydbStatus), errorText, contentType);
    }

    void THttpRequestContext::ResponseOK() const {
        DoResponse("200", "OK");
    }

    void THttpRequestContext::ResponseOKJson(const TString& json) const {
        DoResponse("200", "OK", json, APPLICATION_JSON);
    }

    void THttpRequestContext::ResponseNotFound() const {
        DoResponse("404", "Not Found");
    }

    void THttpRequestContext::ResponseNoContent() const {
        DoResponse("204", "No Content");
    }

    void THttpRequestContext::ResponseUnauthenticated(const TString& message) const {
        DoResponse("401", "Unauthorized", message);
    }

    void THttpRequestContext::DoResponse(TStringBuf status, TStringBuf message, TStringBuf body, TStringBuf contentType) const {
        auto res = Request->CreateResponse(status, message, contentType, body);
        ActorSystem->Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(res));

        const TDuration elapsed = TInstant::Now() - StartedAt;
        LOG_INFO_S(*ActorSystem, NKikimrServices::PUBLIC_HTTP,
                "HTTP response -> code: " << status <<
                ", method: " << Request->Method <<
                ", url: " << Request->URL <<
                ", content type: " << ContentType <<
                ", request body size: " << Request->Body.Size() <<
                ", response body size: " << body.Size() <<
                ", elapsed: " << elapsed <<
                ", from: " << Request->Address <<
                ", forwarded for: " << ForwardedFor <<
                ", request id: " << RequestId <<
                ", idempotency key: " << IdempotencyKey
                );

        auto group = Counters;
        if (Db) {
            group = group->GetSubgroup("target_db", Db);
        }
        if (Project) {
            group = group->GetSubgroup("target_project", Project);
        }
        group = group->GetSubgroup("path_pattern", PathPattern)->GetSubgroup("method", TString(Request->Method));
        group->GetSubgroup("code", TString(status))->GetCounter("count", true)->Inc();
        group->GetSubgroup("code", HttpCodeFamily(status))->GetCounter("count", true)->Inc();
    }

    void THttpRequestContext::ParseHeaders(TStringBuf str) {
        TString sourceReqId;
        NHttp::THeaders headers(str);
        for (const auto& header : headers.Headers) {
            if (AsciiEqualsIgnoreCase(header.first, AUTHORIZATION_HEADER)) {
                Token = header.second;
            } else if (AsciiEqualsIgnoreCase(header.first, REQUEST_ID_HEADER)) {
                sourceReqId = header.second;
            } else if (AsciiEqualsIgnoreCase(header.first, REQUEST_CONTENT_TYPE_HEADER)) {
                ContentType = header.second;
            } else if (AsciiEqualsIgnoreCase(header.first, IDEMPOTENCY_KEY_HEADER)) {
                IdempotencyKey = header.second;
            } else if (AsciiEqualsIgnoreCase(header.first, REQUEST_FORWARDED_FOR)) {
                ForwardedFor = header.second;
            }
        }
        RequestId = GenerateRequestId(sourceReqId);
    }

} // namespace NKikimr::NPublicHttp

#pragma once

#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/http/http.h>

namespace NKikimr::NPublicHttp {

using namespace NActors;

class THttpRequestContext {
public:
    THttpRequestContext(TActorSystem* actorSystem, NHttp::THttpIncomingRequestPtr request, NActors::TActorId sender, TInstant startedAt, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters);
    THttpRequestContext(const THttpRequestContext&) = default;
    THttpRequestContext(const THttpRequestContext&&) = default;

    void ResponseBadRequest(Ydb::StatusIds::StatusCode status, const TString& errorText) const;
    void ResponseBadRequestJson(Ydb::StatusIds::StatusCode status, const TString& json) const;
    void ResponseOK() const;
    void ResponseOKJson(const TString& json) const;
    void ResponseOKUtf8Text(const TString& text) const;
    void ResponseNotFound() const;
    void ResponseNoContent() const;
    void ResponseUnauthenticated(const TString& message) const;
    void ResponseInternalServerError(const TString& message) const;

    void SetPathParams(std::map<TString, TString> pathParams);
    const std::map<TString, TString>& GetPathParams() const;
    void SetPathPattern(const TString& pathPattern);
    void SetProject(const TString& project);
    TString GetProject() const;
    void SetDb(const TString& db);
    TString GetDb() const;
    TString GetToken() const;
    NHttp::THttpIncomingRequestPtr GetHttpRequest() const;
    TString GetContentType() const;
    TString GetIdempotencyKey() const;
    TString GetPeer() const;

private:
    void ParseHeaders(TStringBuf headers);
    void DoResponse(TStringBuf status, TStringBuf message, TStringBuf body = "", TStringBuf contentType = "text/plain") const;
    void DoResponseBadRequest(Ydb::StatusIds::StatusCode status, const TString& body, TStringBuf contentType = "text/plain") const;

private:
    TActorSystem* ActorSystem;
    NHttp::THttpIncomingRequestPtr Request;
    NActors::TActorId Sender;
    TInstant StartedAt;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;

    TString ContentType;
    TString ForwardedFor;
    TString RequestId;
    TString IdempotencyKey;
    TString Token;
    TString PathPattern;
    std::map<TString, TString> PathParams;
    TString Project;
    TString Db;
};

} // namespace NKikimr::NPublicHttp

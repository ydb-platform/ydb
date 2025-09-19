#include "auth.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/auth.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/mon/audit/audit.h>
#include <ydb/core/util/wildcard.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <library/cpp/json/json_writer.h>

namespace NActors {

namespace {

using namespace NKikimr;

class THttpMonAuthRequest : public TActorBootstrapped<THttpMonAuthRequest> {
public:
    using TBase = TActorBootstrapped<THttpMonAuthRequest>;

    NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr Event;
    THandlerInfo Handler;
    NMonitoring::NAudit::TAuditCtx AuditCtx;
    NHttp::TEvHttpProxy::TEvSubscribeForCancel::TPtr CancelSubscriber;

    THttpMonAuthRequest(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event,
                        const THandlerInfo* handler)
        : Event(std::move(event))
        , Handler(*handler)
    {}

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HTTP_MON_AUTHORIZED_ACTOR_REQUEST;
    }

    void Bootstrap() {
        if (Event->Get()->Request->Method == "OPTIONS") {
            return ReplyOptionsAndPassAway();
        }

        AuditCtx.InitAudit(Event);

        Send(Event->Sender, new NHttp::TEvHttpProxy::TEvSubscribeForCancel(), IEventHandle::FlagTrackDelivery);

        if (Handler.Authorizer) {
            if (NActors::IEventHandle* handle = Handler.Authorizer(SelfId(), Event->Get()->Request.Get())) {
                Send(handle);
                Become(&THttpMonAuthRequest::StateWork);
                return;
            }
        }

        SendRequest();
        Become(&THttpMonAuthRequest::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvUndelivered, HandleUndelivered);
            hFunc(NKikimr::NGRpcService::TEvRequestAuthAndCheckResult, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvHttpOutgoingResponse, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvSubscribeForCancel, Handle);
            cFunc(NHttp::TEvHttpProxy::EvRequestCancelled, Cancelled);
        }
    }

private:
    void ReplyWith(NHttp::THttpOutgoingResponsePtr response) {
        AuditCtx.LogOnCompleted(response);
        Send(Event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    void ReplyOptionsAndPassAway() {
        NHttp::THttpIncomingRequestPtr request = Event->Get()->Request;
        TString url(request->URL.Before('?'));
        TString type = mimetypeByExt(url.data());
        if (type.empty()) {
            type = "application/json";
        }
        NHttp::THeaders headers(request->Headers);
        TString allowOrigin = AppData()->Mon->GetConfig().AllowOrigin;
        TString requestOrigin = TString(headers["Origin"]);
        TString origin;
        if (allowOrigin) {
            if (IsMatchesWildcards(requestOrigin, allowOrigin)) {
                origin = requestOrigin;
            } else {
                Send(Event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(request->CreateResponseBadRequest("Invalid CORS origin")));
                return PassAway();
            }
        } else if (requestOrigin) {
            origin = requestOrigin;
        }
        if (origin.empty()) {
            origin = "*";
        }
        TStringBuilder response;
        response << "HTTP/1.1 204 No Content\r\n"
                    "Access-Control-Allow-Origin: " << origin << "\r\n"
                    "Access-Control-Allow-Credentials: true\r\n"
                    "Access-Control-Allow-Headers: Content-Type,Authorization,Origin,Accept,X-Trace-Verbosity,X-Want-Trace,traceparent\r\n"
                    "Access-Control-Expose-Headers: traceresponse,X-Worker-Name\r\n"
                    "Access-Control-Allow-Methods: OPTIONS,GET,POST,PUT,DELETE\r\n"
                    "Content-Type: " << type << "\r\n"
                    "Connection: keep-alive\r\n\r\n";
        Send(Event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(request->CreateResponseString(response)));
        PassAway();
    }

    bool CredentialsProvided() {
        NHttp::THttpIncomingRequestPtr request = Event->Get()->Request;
        NHttp::THeaders headers(request->Headers);
        if (headers.Has("Authorization")) {
            return true;
        }
        NHttp::TCookies cookies(headers["Cookie"]);
        return cookies.Has("ydb_session_id");
    }

    TString YdbToHttpError(Ydb::StatusIds::StatusCode status) {
        switch (status) {
        case Ydb::StatusIds::UNAUTHORIZED:
            return CredentialsProvided() ? "403 Forbidden" : "401 Unauthorized";
        case Ydb::StatusIds::INTERNAL_ERROR:
            return "500 Internal Server Error";
        case Ydb::StatusIds::UNAVAILABLE:
            return "503 Service Unavailable";
        case Ydb::StatusIds::OVERLOADED:
            return "429 Too Many Requests";
        case Ydb::StatusIds::TIMEOUT:
            return "408 Request Timeout";
        case Ydb::StatusIds::PRECONDITION_FAILED:
            return "412 Precondition Failed";
        default:
            return "400 Bad Request";
        }
    }

    bool AcceptsJson() const {
        NHttp::THttpIncomingRequestPtr request = Event->Get()->Request;
        TStringBuf acceptHeader = NHttp::THeaders(request->Headers)["Accept"];
        return acceptHeader.find("application/json") != TStringBuf::npos;
    }

    void ReplyErrorAndPassAway(const NKikimr::NGRpcService::TEvRequestAuthAndCheckResult& result) {
        ReplyErrorAndPassAway(result.Status, result.Issues, true);
    }

    void ReplyErrorAndPassAway(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues, bool addAccessControlHeaders) {
        NHttp::THttpIncomingRequestPtr request = Event->Get()->Request;
        TStringBuilder response;
        TStringBuilder body;
        TStringBuf contentType;
        const TString httpError = YdbToHttpError(status);

        if (AcceptsJson()) {
            contentType = "application/json";
            NJson::TJsonValue json;
            TString message;
            MakeJsonErrorReply(json, message, issues, NYdb::EStatus(status));
            NJson::WriteJson(&body.Out, &json);
        } else {
            contentType = "text/html";
            body << "<html><body><h1>" << httpError << "</h1>";
            if (issues) {
                body << "<p>" << issues.ToString() << "</p>";
            }
            body << "</body></html>";
        }

        response << "HTTP/1.1 " << httpError << "\r\n";
        if (addAccessControlHeaders) {
            NHttp::THeaders headers(request->Headers);
            TString origin = TString(headers["Origin"]);
            if (origin.empty()) {
                origin = "*";
            }
            response << "Access-Control-Allow-Origin: " << origin << "\r\n";
            response << "Access-Control-Allow-Credentials: true\r\n";
            response << "Access-Control-Allow-Headers: Content-Type,Authorization,Origin,Accept\r\n";
            response << "Access-Control-Allow-Methods: OPTIONS, GET, POST, PUT, DELETE\r\n";
            response << "Access-Control-Expose-Headers: traceresponse,X-Worker-Name\r\n";
        }

        response << "Content-Type: " << contentType << "\r\n";
        response << "Content-Length: " << body.size() << "\r\n";
        response << "\r\n";
        response << body;
        ReplyWith(request->CreateResponseString(response));
        PassAway();
    }

    void ReplyForbiddenAndPassAway(const TString& error = {}) {
        NYql::TIssues issues;
        issues.AddIssue(error);
        ReplyErrorAndPassAway(Ydb::StatusIds::UNAUTHORIZED, issues, true);
    }

    void SendRequest(const NKikimr::NGRpcService::TEvRequestAuthAndCheckResult* result = nullptr) {
        NHttp::THttpIncomingRequestPtr request = Event->Get()->Request;
        if (Handler.Authorizer) {
            TString user = (result && result->UserToken) ? result->UserToken->GetUserSID() : "anonymous";
            ALOG_NOTICE(NActorsServices::HTTP,
                (request->Address ? request->Address->ToString() : "")
                << " " << user
                << " " << request->Method
                << " " << request->URL);
        }

        TString serializedToken;
        if (result) {
            AuditCtx.AddAuditLogParts(result->AuditLogParts);
            if (result->UserToken) {
                AuditCtx.SetSubjectType(result->UserToken->GetSubjectType());
                serializedToken = result->UserToken->GetSerializedToken();
            }
        }

        AuditCtx.LogOnReceived();
        Event->Get()->UserToken = serializedToken;
        Send(new IEventHandle(Handler.Handler, SelfId(), Event->ReleaseBase().Release(), IEventHandle::FlagTrackDelivery, Event->Cookie));
    }

    void Cancelled() {
        if (CancelSubscriber) {
            Send(CancelSubscriber->Sender, new NHttp::TEvHttpProxy::TEvRequestCancelled(), 0, CancelSubscriber->Cookie);
        }
        PassAway();
    }

    void HandleUndelivered(TEvents::TEvUndelivered::TPtr& ev) {
        if (ev->Get()->SourceType == NHttp::TEvHttpProxy::EvSubscribeForCancel) {
            return Cancelled();
        }
        NHttp::THttpIncomingRequestPtr request = Event->Get()->Request;
        ReplyWith(request->CreateResponseServiceUnavailable("Actor is not available"));
        PassAway();
    }

    void Handle(NKikimr::NGRpcService::TEvRequestAuthAndCheckResult::TPtr& ev) {
        const auto& result = *ev->Get();
        if (result.Status != Ydb::StatusIds::SUCCESS) {
            return ReplyErrorAndPassAway(result);
        }
        if (IsTokenAllowed(result.UserToken.Get(), Handler.AllowedSIDs)) {
            SendRequest(&result);
        } else {
            return ReplyForbiddenAndPassAway("SID is not allowed");
        }
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpOutgoingResponse::TPtr& ev) {
        bool endOfData = ev->Get()->Response->IsDone();
        AuditCtx.LogOnCompleted(ev->Get()->Response);
        Forward(ev, Event->Sender);
        if (endOfData) {
            return PassAway();
        }
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk::TPtr& ev) {
        bool endOfData = ev->Get()->DataChunk && ev->Get()->DataChunk->IsEndOfData() || ev->Get()->Error;
        Forward(ev, Event->Sender);
        if (endOfData) {
            PassAway();
        }
    }

    void Handle(NHttp::TEvHttpProxy::TEvSubscribeForCancel::TPtr& ev) {
        CancelSubscriber = std::move(ev);
    }
};

} // namespace

void THttpMonAuth::Handle(NHttp::TEvHttpProxy::TEvRegisterHandler::TPtr& ev) {
    const TString& path = ev->Get()->Path;
    auto& info = Handlers[path];
    info.Handler = ev->Get()->Handler;
    info.Authorizer = TMon::TRequestAuthorizer();
    Send(HttpProxyActorId, new NHttp::TEvHttpProxy::TEvRegisterHandler(path, SelfId()));
}

void THttpMonAuth::Handle(TEvMon::TEvRegisterHandler::TPtr& ev) {
    const auto& fields = ev->Get()->Fields;
    auto& info = Handlers[fields.Path];
    info.Handler = fields.Handler;
    info.AllowedSIDs = fields.AllowedSIDs;
    info.Authorizer = fields.UseAuth ? Authorizer : TMon::TRequestAuthorizer();
    Send(HttpProxyActorId, new NHttp::TEvHttpProxy::TEvRegisterHandler(fields.Path, SelfId()));
}

void THttpMonAuth::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev) {
    TString path(ev->Get()->Request->URL.Before('?'));
    if (path.empty()) {
        path = "/";
    } else if (path[0] != '/') {
        path = TString("/") + path;
    }

    TString matchedPath;
    if (THandlerInfo* handler = FindHandler(path, matchedPath)) {
        if (handler->Handler) {
            Register(new THttpMonAuthRequest(std::move(ev), handler));
        } else {
            ALOG_ERROR(NActorsServices::HTTP,
                "HttpMonAuth: no handler actor for URL " << matchedPath);
            Send(ev->Sender,
                new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(ev->Get()->Request->CreateResponseServiceUnavailable(
                    TStringBuilder() << "No handler for path " << matchedPath)),
                0,
                ev->Cookie);
        }
        return;
    }

    ALOG_WARN(NActorsServices::HTTP,
        "HttpMonAuth: no handler for URL " << ev->Get()->Request->URL);
    Send(ev->Sender,
        new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(ev->Get()->Request->CreateResponseNotFound()),
        0,
        ev->Cookie);
}

THandlerInfo* THttpMonAuth::FindHandler(const TString& originalPath, TString& matchedPath) {
    TStringBuf url = originalPath;
    while (!url.empty()) {
        auto it = Handlers.find(TString(url));
        if (it != Handlers.end() && it->second.Handler) {
            matchedPath = TString(url);
            return &it->second;
        }
        if (url.EndsWith('/')) {
            url.Chop(1);
        } else {
            size_t pos = url.rfind('/');
            if (pos == TStringBuf::npos) {
                break;
            } else {
                url = url.substr(0, pos + 1);
            }
        }
    }

    auto it = Handlers.find("/");
    if (it != Handlers.end() && it->second.Handler) {
        matchedPath = "/";
        return &it->second;
    }

    return nullptr;
}

STATEFN(THttpMonAuth::StateWork) {
    switch (ev->GetTypeRewrite()) {
        hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        hFunc(NHttp::TEvHttpProxy::TEvRegisterHandler, Handle);
        hFunc(TEvMon::TEvRegisterHandler, Handle);
        cFunc(TEvents::TSystem::Poison, PassAway);
    }
}

} // namespace NActors

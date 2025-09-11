#include "login_page.h"
#include "login_shared_func.h"

#include <ydb/library/actors/http/http_proxy.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <ydb/core/util/address_classifier.h>
#include <ydb/core/audit/audit_log.h>

#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>

#include <ydb/library/login/login.h>
#include <ydb/library/security/util.h>

#include <util/datetime/base.h>  // for ToInstant


namespace {

using namespace NKikimr;

void AuditLogWebUILogout(const NHttp::THttpIncomingRequest& request, const TString& userSID, const TString& sanitizedToken) {
    static const TString WebLoginComponentName = "web-login";
    static const TString LogoutOperationName = "LOGOUT";
    static const TString EmptyValue = "{none}";

    auto remoteAddress = NAddressClassifier::ExtractAddress(request.Address->ToString());

    // NOTE: audit field set here must be in sync with ydb/core/grpc_services/audit_logins.cpp, AuditLogLogin()
    AUDIT_LOG(
        AUDIT_PART("component", WebLoginComponentName)
        AUDIT_PART("remote_address", (!remoteAddress.empty() ? remoteAddress : EmptyValue))
        AUDIT_PART("subject", (!userSID.empty() ? userSID : EmptyValue))
        AUDIT_PART("sanitized_token", (!sanitizedToken.empty() ? sanitizedToken : EmptyValue))
        //NOTE: no database specified as web logout considered cluster-wide
        AUDIT_PART("operation", LogoutOperationName)
        AUDIT_PART("status", "SUCCESS")
    );
}

struct TEvPrivate {
    enum EEv {
        EvLoginResponse = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    struct TEvLoginResponse : NActors::TEventLocal<TEvLoginResponse, EvLoginResponse> {
        Ydb::Auth::LoginResponse LoginResponse;

        TEvLoginResponse(const Ydb::Auth::LoginResponse& loginResponse)
            : LoginResponse(loginResponse)
        {}
    };
};

void SendLoginRequest(const TString& database, const TString& user, const TString& password, const TActorContext& recipientCtx) {
    Ydb::Auth::LoginRequest request;
    request.set_user(user);
    request.set_password(password);

    auto* actorSystem = recipientCtx.ActorSystem();
    auto recipientId = recipientCtx.SelfID;

    using TEvLoginRequest = NGRpcService::TGRpcRequestWrapperNoAuth<NGRpcService::TRpcServices::EvLogin, Ydb::Auth::LoginRequest, Ydb::Auth::LoginResponse>;

    auto rpcFuture = NRpcService::DoLocalRpc<TEvLoginRequest>(std::move(request), database, {}, actorSystem);
    rpcFuture.Subscribe([actorSystem, recipientId](const auto& future) {
        actorSystem->Send(recipientId, new TEvPrivate::TEvLoginResponse(future.GetValueSync()));
    });
}

class TLoginRequest : public NActors::TActorBootstrapped<TLoginRequest> {
public:
    using TBase = NActors::TActorBootstrapped<TLoginRequest>;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::ACTORLIB_COMMON;
    }

    TLoginRequest(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev)
        : Sender(ev->Sender)
        , Request(ev->Get()->Request)
    {
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
            hFunc(TEvPrivate::TEvLoginResponse, HandleLoginResponse);
        }
    }

    void Bootstrap() {
        ALOG_WARN(NActorsServices::HTTP, Request->Address << " " << Request->Method << " " << Request->URL);

        if (Request->Method == "OPTIONS") {
            return ReplyOptionsAndPassAway();
        }

        if (Request->Method != "POST") {
            return ReplyErrorAndPassAway("400", "Bad Request", "Invalid method");
        }

        NHttp::THeaders headers(Request->Headers);

        if (headers.Get("Content-Type").Before(';') != "application/json") {
            return ReplyErrorAndPassAway("400", "Bad Request", "Invalid Content-Type");
        }

        NJson::TJsonValue postData;

        if (!NJson::ReadJsonTree(Request->Body, &postData)) {
            return ReplyErrorAndPassAway("400", "Bad Request", "Invalid JSON data");
        }

        TString password;
        NJson::TJsonValue* jsonUser;
        if (postData.GetValuePointer("user", &jsonUser)) {
            User = jsonUser->GetStringRobust();
        } else {
            return ReplyErrorAndPassAway("400", "Bad Request", "User must be specified");
        }

        NJson::TJsonValue* jsonPassword;
        if (postData.GetValuePointer("password", &jsonPassword)) {
            password = jsonPassword->GetStringRobust();
        } else {
            return ReplyErrorAndPassAway("400", "Bad Request", "Password must be specified");
        }

        if (postData.Has("database")) {
            Database = postData["database"].GetStringRobust();
        }

        SendLoginRequest(Database, User, password, ActorContext());

        Become(&TThis::StateWork, Timeout, new TEvents::TEvWakeup());
    }


    void HandleLoginResponse(TEvPrivate::TEvLoginResponse::TPtr& ev) {
        const auto& operation = ev->Get()->LoginResponse.operation();

        if (operation.status() == Ydb::StatusIds::SUCCESS) {
            Ydb::Auth::LoginResult loginResult;
            operation.result().UnpackTo(&loginResult);

            ReplyCookieAndPassAway(loginResult.token());

        } else {
            TString error;
            if (operation.issues_size() > 0) {
                error = operation.issues(0).message();
            } else {
                error = Ydb::StatusIds_StatusCode_Name(operation.status());
            }

            ReplyErrorAndPassAway("403", "Forbidden", error);
        }
    }

    void HandlePoisonPill(TEvents::TEvPoisonPill::TPtr&) {
        PassAway();
    }

    void HandleTimeout() {
        ReplyErrorAndPassAway("504", "Gateway Timeout", "Timeout");
    }

    void ReplyOptionsAndPassAway() {
        NHttp::THeadersBuilder headers;
        SetCORS(headers);
        headers.Set("Allow", "OPTIONS, POST");
        Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request->CreateResponse("204", "No Content", headers)));
        PassAway();
    }

    void SetCORS(NHttp::THeadersBuilder& headers) {
        TStringBuilder res;
        TString origin = TString(NHttp::THeaders(Request->Headers)["Origin"]);
        if (origin.empty()) {
            origin = "*";
        }
        headers.Set("Access-Control-Allow-Origin", origin);
        headers.Set("Access-Control-Allow-Credentials", "true");
        headers.Set("Access-Control-Allow-Headers", "Content-Type,Authorization,Origin,Accept");
        headers.Set("Access-Control-Allow-Methods", "OPTIONS, GET, POST");
    }

    void ReplyCookieAndPassAway(const TString& cookie) {
        ALOG_DEBUG(NActorsServices::HTTP, "Login success for " << User);
        NHttp::THeadersBuilder headers;
        SetCORS(headers);
        TDuration maxAge = (ToInstant(NLogin::TLoginProvider::GetTokenExpiresAt(cookie)) - TInstant::Now());
        headers.Set("Set-Cookie", TStringBuilder() << "ydb_session_id=" << cookie << "; Max-Age=" << maxAge.Seconds());
        Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request->CreateResponse("200", "OK", headers)));
        PassAway();
    }

    void ReplyErrorAndPassAway(const TString& status, const TString& message, const TString& error) {
        ALOG_ERROR(NActorsServices::HTTP, "Login fail for " << User << ": " << error);
        NHttp::THeadersBuilder headers;
        SetCORS(headers);
        headers.Set("Content-Type", "application/json");
        NJson::TJsonValue body;
        body["error"] = error;
        Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request->CreateResponse(status, message, headers, NJson::WriteJson(body, false))));
        PassAway();
    }

protected:
    TActorId Sender;
    NHttp::THttpIncomingRequestPtr Request;
    TDuration Timeout = TDuration::Seconds(60);
    TString User;
    TString Database;
};

class TLogoutRequest : public NActors::TActorBootstrapped<TLogoutRequest> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::ACTORLIB_COMMON;
    }

    TLogoutRequest(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev)
        : Sender(ev->Sender)
        , Request(ev->Get()->Request)
    {
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
            hFunc(TEvTicketParser::TEvAuthorizeTicketResult, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Bootstrap() {
        ALOG_WARN(NActorsServices::HTTP, Request->Address << " " << Request->Method << " " << Request->URL);

        if (Request->Method == "OPTIONS") {
            return ReplyOptionsAndPassAway();
        }

        if (Request->Method != "POST") {
            return ReplyErrorAndPassAway("400", "Bad Request", "Invalid method");
        }

        NHttp::TCookies cookies(NHttp::THeaders(Request->Headers)["Cookie"]);
        TStringBuf ydbSessionId = cookies["ydb_session_id"];
        if (ydbSessionId.empty()) {
            return ReplyErrorAndPassAway("401", "Unauthorized", "No ydb_session_id cookie");
        }

        Send(MakeTicketParserID(), new TEvTicketParser::TEvAuthorizeTicket({
            .Database = TString(),
            .Ticket = TString("Login ") + ydbSessionId,
            .PeerName = Request->Address->ToString(),
        }));

        Become(&TThis::StateWork, Timeout, new TEvents::TEvWakeup());
    }

    void Handle(TEvTicketParser::TEvAuthorizeTicketResult::TPtr& ev) {
        const TEvTicketParser::TEvAuthorizeTicketResult& result = *ev->Get();
        if (result.Error) {
            return ReplyErrorAndPassAway("403", "Forbidden", result.Error.Message);
        }
        if (result.Token == nullptr) {
            return ReplyErrorAndPassAway("403", "Forbidden", "Empty token");
        }

        ReplyDeleteCookieAndPassAway(result.Token->GetUserSID(), result.Token->GetSanitizedToken());
    }

    void HandlePoisonPill(TEvents::TEvPoisonPill::TPtr&) {
        PassAway();
    }

    void HandleTimeout() {
        ALOG_ERROR(NActorsServices::HTTP, Request->Address << " " << Request->Method << " " << Request->URL << " timeout");
        ReplyErrorAndPassAway("504", "Gateway Timeout", "Timeout");
    }

    void ReplyOptionsAndPassAway() {
        NHttp::THeadersBuilder headers;
        headers.Set("Allow", "OPTIONS, POST");
        Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request->CreateResponse("204", "No Content", headers)));
        PassAway();
    }

    void SetCORS(NHttp::THeadersBuilder& headers) {
        TStringBuilder res;
        TString origin = TString(NHttp::THeaders(Request->Headers)["Origin"]);
        if (origin.empty()) {
            origin = "*";
        }
        headers.Set("Access-Control-Allow-Origin", origin);
        headers.Set("Access-Control-Allow-Credentials", "true");
        headers.Set("Access-Control-Allow-Headers", "Content-Type,Authorization,Origin,Accept");
        headers.Set("Access-Control-Allow-Methods", "OPTIONS, GET, POST");
    }

    void ReplyDeleteCookieAndPassAway(const TString& userSID, const TString& sanitizedToken) {
        ALOG_DEBUG(NActorsServices::HTTP, "Logout success");
        NHttp::THeadersBuilder headers;
        SetCORS(headers);
        headers.Set("Set-Cookie", "ydb_session_id=; Max-Age=0");
        Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request->CreateResponse("200", "OK", headers)));

        AuditLogWebUILogout(*Request, userSID, sanitizedToken);

        PassAway();
    }

    void ReplyErrorAndPassAway(const TString& status, const TString& message, const TString& error) {
        ALOG_ERROR(NActorsServices::HTTP, "Logout: " << error);
        NHttp::THeadersBuilder headers;
        SetCORS(headers);
        headers.Set("Content-Type", "application/json");
        NJson::TJsonValue body;
        body["error"] = error;
        Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request->CreateResponse(status, message, headers, NJson::WriteJson(body, false))));
        PassAway();
    }

protected:
    TActorId Sender;
    NHttp::THttpIncomingRequestPtr Request;
    TDuration Timeout = TDuration::Seconds(5);
};

class TLoginService : public TActor<TLoginService> {
public:
    TLoginService()
        : TActor(&TLoginService::Work)
    {}

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::ACTORLIB_COMMON;
    }

    STATEFN(Work) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, HandleRequest);
        }
    }

    void HandlePoisonPill(TEvents::TEvPoisonPill::TPtr&) {
        PassAway();
    }

    void HandleRequest(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev) {
        if (ev->Get()->Request->URL == "/login") {
            Register(new TLoginRequest(ev));
        } else if (ev->Get()->Request->URL == "/logout") {
            Register(new TLogoutRequest(ev));
        } else {
            Send(ev->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(ev->Get()->Request->CreateResponseNotFound()));
        }
    }
};

}

namespace NKikimr {

NActors::IActor* CreateWebLoginService() {
    return new TLoginService();
}

}

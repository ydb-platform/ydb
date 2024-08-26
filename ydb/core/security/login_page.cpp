#include "login_page.h"
#include "login_shared_func.h"

#include <ydb/library/actors/http/http_proxy.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/security/ldap_auth_provider/ldap_auth_provider.h>

#include <ydb/library/login/login.h>
#include <ydb/library/security/util.h>

namespace {

using namespace NActors;
using namespace NKikimr;
using namespace NSchemeShard;
using namespace NMonitoring;

using THttpResponsePtr = THolder<NMon::IEvHttpInfoRes>;

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
            hFunc(TEvTabletPipe::TEvClientConnected, HandleConnect);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNavigate);
            hFunc(TEvSchemeShard::TEvLoginResult, HandleResult);
            hFunc(TEvLdapAuthProvider::TEvAuthenticateResponse, Handle);
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

        NHttp::THeaders headers(Request->Headers);

        if (headers.Get("Content-Type").Before(';') != "application/json") {
            return ReplyErrorAndPassAway("400", "Bad Request", "Invalid Content-Type");
        }

        NJson::TJsonValue postData;

        if (!NJson::ReadJsonTree(Request->Body, &postData)) {
            return ReplyErrorAndPassAway("400", "Bad Request", "Invalid JSON data");
        }

        TString login;
        TString password;
        NJson::TJsonValue* jsonUser;
        if (postData.GetValuePointer("user", &jsonUser)) {
            login = jsonUser->GetStringRobust();
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

        AuthCredentials = PrepareCredentials(login, password, AppData()->AuthConfig);
        if (AuthCredentials.AuthType == TAuthCredentials::EAuthType::Ldap) {
            ALOG_DEBUG(NActorsServices::HTTP, "Login: Requesting LDAP provider for user " << AuthCredentials.Login);
            Send(MakeLdapAuthProviderID(), new TEvLdapAuthProvider::TEvAuthenticateRequest(AuthCredentials.Login, AuthCredentials.Password));
        } else {
            RequestLoginProvider();
        }
        Become(&TThis::StateWork, Timeout, new TEvents::TEvWakeup());
    }

    static NTabletPipe::TClientConfig GetPipeClientConfig() {
        NTabletPipe::TClientConfig clientConfig;
        clientConfig.RetryPolicy = {.RetryLimitCount = 3};
        return clientConfig;
    }

    void RequestSchemeShard(ui64 schemeShardTabletId) {
        ALOG_DEBUG(NActorsServices::HTTP, "Login: Requesting schemeshard " << schemeShardTabletId << " for user " << AuthCredentials.Login);
        IActor* pipe = NTabletPipe::CreateClient(SelfId(), schemeShardTabletId, GetPipeClientConfig());
        PipeClient = RegisterWithSameMailbox(pipe);
        THolder<TEvSchemeShard::TEvLogin> request = MakeHolder<TEvSchemeShard::TEvLogin>();
        request.Get()->Record = CreateLoginRequest(AuthCredentials, AppData()->AuthConfig);
        request.Get()->Record.SetPeerName(Request->Address->ToString());
        NTabletPipe::SendData(SelfId(), PipeClient, request.Release());
    }

    void HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const NSchemeCache::TSchemeCacheNavigate* response = ev->Get()->Request.Get();
        if (response->ResultSet.size() == 1) {
            if (response->ResultSet.front().Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
                const NSchemeCache::TSchemeCacheNavigate::TEntry& entry = response->ResultSet.front();
                ui64 schemeShardTabletId = entry.DomainInfo->ExtractSchemeShard();
                RequestSchemeShard(schemeShardTabletId);
                return;
            } else {
                ReplyErrorAndPassAway("503", "Service Unavailable", TStringBuilder()
                    << "Status " << static_cast<int>(response->ResultSet.front().Status));
            }
        } else {
            ReplyErrorAndPassAway("503", "Service Unavailable", "Scheme error");
        }
    }

    void Handle(TEvLdapAuthProvider::TEvAuthenticateResponse::TPtr& ev) {
        TEvLdapAuthProvider::TEvAuthenticateResponse* response = ev->Get();
        if (response->Status == TEvLdapAuthProvider::EStatus::SUCCESS) {
            RequestLoginProvider();
        } else {
            ReplyErrorAndPassAway("403", "Forbidden", response->Error.Message);
        }
    }

    void RequestLoginProvider() {
        auto *domain = AppData()->DomainsInfo->GetDomain();
        TString rootDatabase = "/" + domain->Name;
        ui64 rootSchemeShardTabletId = domain->SchemeRoot;
        if (!Database.empty() && Database != rootDatabase) {
            Database = rootDatabase;
            ALOG_DEBUG(NActorsServices::HTTP, "Login: Requesting schemecache for database " << Database);
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(CreateNavigateKeySetRequest(Database).Release()));
        } else {
            Database = rootDatabase;
            RequestSchemeShard(rootSchemeShardTabletId);
        }
    }

    void HandleResult(TEvSchemeShard::TEvLoginResult::TPtr& ev) {
        if (ev->Get()->Record.GetError()) {
            ReplyErrorAndPassAway("403", "Forbidden", ev->Get()->Record.GetError());
        } else {
            ReplyCookieAndPassAway(ev->Get()->Record.GetToken());
        }
    }

    void HandleConnect(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            ReplyErrorAndPassAway("503", "Service Unavailable", "SchemeShard is not available");
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
        ALOG_DEBUG(NActorsServices::HTTP, "Login success for " << AuthCredentials.Login);
        NHttp::THeadersBuilder headers;
        SetCORS(headers);
        TDuration maxAge = (ToInstant(NLogin::TLoginProvider::GetTokenExpiresAt(cookie)) - TInstant::Now());
        headers.Set("Set-Cookie", TStringBuilder() << "ydb_session_id=" << cookie << "; Max-Age=" << maxAge.Seconds());
        Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request->CreateResponse("200", "OK", headers)));
        PassAway();
    }

    void ReplyErrorAndPassAway(const TString& status, const TString& message, const TString& error) {
        ALOG_ERROR(NActorsServices::HTTP, "Login: " << error);
        NHttp::THeadersBuilder headers;
        SetCORS(headers);
        headers.Set("Content-Type", "application/json");
        NJson::TJsonValue body;
        body["error"] = error;
        Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request->CreateResponse(status, message, headers, NJson::WriteJson(body, false))));
        PassAway();
    }

    void PassAway() override {
        if (PipeClient) {
            NTabletPipe::CloseClient(TBase::SelfId(), PipeClient);
        }
        TBase::PassAway();
    }

protected:
    TActorId Sender;
    NHttp::THttpIncomingRequestPtr Request;
    TDuration Timeout = TDuration::Seconds(60);
    TString Database;
    TActorId PipeClient;
    TAuthCredentials AuthCredentials;
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

        ReplyDeleteCookieAndPassAway();
    }

    void HandlePoisonPill(TEvents::TEvPoisonPill::TPtr&) {
        PassAway();
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

    void ReplyDeleteCookieAndPassAway() {
        ALOG_DEBUG(NActorsServices::HTTP, "Logout success");
        NHttp::THeadersBuilder headers;
        SetCORS(headers);
        headers.Set("Set-Cookie", "ydb_session_id=; Max-Age=0");
        Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request->CreateResponse("200", "OK", headers)));
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

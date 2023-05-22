#include "login_page.h"

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

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
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::ACTORLIB_COMMON;
    }

    TLoginRequest(IMonHttpRequest& request, NThreading::TPromise<THttpResponsePtr> result)
        : Request(request)
        , Result(result)
    {
    }

    ~TLoginRequest() {
        if (!Result.HasValue()) {
            Result.SetValue(nullptr);
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
            hFunc(TEvTabletPipe::TEvClientConnected, HandleConnect);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNavigate);
            hFunc(TEvSchemeShard::TEvLoginResult, HandleResult);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    static TString GetMethod(HTTP_METHOD method) {
        switch (method) {
            case HTTP_METHOD_UNDEFINED: return "UNDEFINED";
            case HTTP_METHOD_OPTIONS: return "OPTIONS";
            case HTTP_METHOD_GET: return "GET";
            case HTTP_METHOD_HEAD: return "HEAD";
            case HTTP_METHOD_POST: return "POST";
            case HTTP_METHOD_PUT: return "PUT";
            case HTTP_METHOD_DELETE: return "DELETE";
            case HTTP_METHOD_TRACE: return "TRACE";
            case HTTP_METHOD_CONNECT: return "CONNECT";
            case HTTP_METHOD_EXTENSION: return "EXTENSION";
            default: return "UNKNOWN";
        }
    }

    void Bootstrap() {
        LOG_WARN_S(*TlsActivationContext, NActorsServices::HTTP,
                        Request.GetRemoteAddr()
                        << " " << GetMethod(Request.GetMethod())
                        << " " << Request.GetUri());

        if (Request.GetMethod() == HTTP_METHOD_OPTIONS) {
            return ReplyOptionsAndPassAway();
        }

        if (Request.GetMethod() != HTTP_METHOD_POST) {
            return ReplyErrorAndPassAway("400 Bad Request", "Invalid method");
        }

        if (Request.GetHeader("Content-Type").Before(';') != "application/json") {
            return ReplyErrorAndPassAway("400 Bad Request", "Invalid Content-Type");
        }

        NJson::TJsonValue postData;

        if (!NJson::ReadJsonTree(Request.GetPostContent(), &postData)) {
            return ReplyErrorAndPassAway("400 Bad Request", "Invalid JSON data");
        }

        if (postData.Has("database")) {
            Database = postData["database"].GetStringRobust();
        } else {
            TDomainsInfo* domainsInfo = AppData()->DomainsInfo.Get();
            const TDomainsInfo::TDomain& domain = *domainsInfo->Domains.begin()->second.Get();
            Database = "/" + domain.Name;
        }

        NJson::TJsonValue* jsonUser;
        if (postData.GetValuePointer("user", &jsonUser)) {
            User = jsonUser->GetStringRobust();
        } else {
            return ReplyErrorAndPassAway("400 Bad Request", "User must be specified");
        }

        NJson::TJsonValue* jsonPassword;
        if (postData.GetValuePointer("password", &jsonPassword)) {
            Password = jsonPassword->GetStringRobust();
        } else {
            return ReplyErrorAndPassAway("400 Bad Request", "Password must be specified");
        }

        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = Database;
        auto& entry = request->ResultSet.emplace_back();
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        entry.Path = ::NKikimr::SplitPath(Database);
        entry.RedirectRequired = false;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));

        Become(&TThis::StateWork, Timeout, new TEvents::TEvWakeup());
    }

    static NTabletPipe::TClientConfig GetPipeClientConfig() {
        NTabletPipe::TClientConfig clientConfig;
        clientConfig.RetryPolicy = {.RetryLimitCount = 3};
        return clientConfig;
    }

    void HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const NSchemeCache::TSchemeCacheNavigate* response = ev->Get()->Request.Get();
        if (response->ResultSet.size() == 1) {
            if (response->ResultSet.front().Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
                const NSchemeCache::TSchemeCacheNavigate::TEntry& entry = response->ResultSet.front();
                ui64 schemeShardTabletId = entry.DomainInfo->ExtractSchemeShard();
                IActor* pipe = NTabletPipe::CreateClient(SelfId(), schemeShardTabletId, GetPipeClientConfig());
                TActorId pipeClient = RegisterWithSameMailbox(pipe);
                THolder<TEvSchemeShard::TEvLogin> request = MakeHolder<TEvSchemeShard::TEvLogin>();
                request.Get()->Record.SetUser(User);
                request.Get()->Record.SetPassword(Password);
                NTabletPipe::SendData(SelfId(), pipeClient, request.Release());
                return;
            } else {
                ReplyErrorAndPassAway("503 Service Unavailable", TStringBuilder()
                    << "Status " << static_cast<int>(response->ResultSet.front().Status));
            }
        } else {
            ReplyErrorAndPassAway("503 Service Unavailable", "Scheme error");
        }
    }

    void HandleResult(TEvSchemeShard::TEvLoginResult::TPtr& ev) {
        if (ev->Get()->Record.GetError()) {
            ReplyErrorAndPassAway("403 Forbidden", ev->Get()->Record.GetError());
        } else {
            ReplyCookieAndPassAway(ev->Get()->Record.GetToken());
        }
    }

    void HandleConnect(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            ReplyErrorAndPassAway("503 Service Unavailable", "SchemeShard is not available");
        }
    }

    void HandlePoisonPill(TEvents::TEvPoisonPill::TPtr&) {
        PassAway();
    }

    void HandleTimeout() {
        ReplyErrorAndPassAway("504 Gateway Timeout", "Timeout");
    }

    void ReplyOptionsAndPassAway() {
        Result.SetValue(MakeHolder<NMon::TEvHttpInfoRes>(
            "HTTP/1.1 204 No Content\r\n"
            "Allow: OPTIONS, POST\r\n"
            "Connection: Keep-Alive\r\n\r\n", 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

    TString GetCORS() {
        TStringBuilder res;
        TString origin = TString(Request.GetHeader("Origin"));
        if (origin.empty()) {
            origin = "*";
        }
        res << "Access-Control-Allow-Origin: " << origin << "\r\n";
        res << "Access-Control-Allow-Credentials: true\r\n";
        res << "Access-Control-Allow-Headers: Content-Type,Authorization,Origin,Accept\r\n";
        res << "Access-Control-Allow-Methods: OPTIONS, GET, POST\r\n";
        return res;
    }

    void ReplyCookieAndPassAway(const TString& cookie) {
        TStringStream response;
        TDuration maxAge = (ToInstant(NLogin::TLoginProvider::GetTokenExpiresAt(cookie)) - TInstant::Now());
        response << "HTTP/1.1 200 OK\r\n";
        response << "Set-Cookie: ydb_session_id=" << cookie << "; Max-Age=" << maxAge.Seconds() << "\r\n";
        response << GetCORS();
        response << "\r\n";
        Result.SetValue(MakeHolder<NMon::TEvHttpInfoRes>(response.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

    void ReplyErrorAndPassAway(const TString& status, const TString& error) {
        NJson::TJsonValue body;
        body["error"] = error;
        TStringStream response;
        TString responseBody = NJson::WriteJson(body, false);
        response << "HTTP/1.1 " << status << "\r\n";
        response << "Content-Type: application/json\r\n";
        response << "Content-Length: " << responseBody.Size() << "\r\n";
        response << GetCORS();
        response << "\r\n";
        response << responseBody;
        Result.SetValue(MakeHolder<NMon::TEvHttpInfoRes>(response.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

protected:
    IMonHttpRequest& Request;
    NThreading::TPromise<THttpResponsePtr> Result;
    TDuration Timeout = TDuration::Seconds(60);
    TString Database;
    TString User;
    TString Password;
};

class TLoginMonPage: public IMonPage {
public:
    TLoginMonPage(TActorSystem* actorSystem, const TString& path)
        : IMonPage(path, {})
        , ActorSystem(actorSystem)
    {
    }

    void Output(IMonHttpRequest &request) override {
        auto promise = NThreading::NewPromise<THttpResponsePtr>();
        auto future = promise.GetFuture();

        ActorSystem->Register(new TLoginRequest(request, promise));

        THttpResponsePtr result = future.ExtractValue(TDuration::Max());

        if (result) {
            Output(request, *result);
        }
    }

private:
    void Output(IMonHttpRequest& request, const NMon::IEvHttpInfoRes& result) const {
        result.Output(request.Output());
    }

private:
    TActorSystem* ActorSystem;
};

class TLogoutRequest : public NActors::TActorBootstrapped<TLogoutRequest> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::ACTORLIB_COMMON;
    }

    TLogoutRequest(IMonHttpRequest& request, NThreading::TPromise<THttpResponsePtr> result)
        : Request(request)
        , Result(result)
    {
    }

    ~TLogoutRequest() {
        if (!Result.HasValue()) {
            Result.SetValue(nullptr);
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    static TString GetMethod(HTTP_METHOD method) {
        switch (method) {
            case HTTP_METHOD_UNDEFINED: return "UNDEFINED";
            case HTTP_METHOD_OPTIONS: return "OPTIONS";
            case HTTP_METHOD_GET: return "GET";
            case HTTP_METHOD_HEAD: return "HEAD";
            case HTTP_METHOD_POST: return "POST";
            case HTTP_METHOD_PUT: return "PUT";
            case HTTP_METHOD_DELETE: return "DELETE";
            case HTTP_METHOD_TRACE: return "TRACE";
            case HTTP_METHOD_CONNECT: return "CONNECT";
            case HTTP_METHOD_EXTENSION: return "EXTENSION";
            default: return "UNKNOWN";
        }
    }

    void Bootstrap() {
        LOG_WARN_S(*TlsActivationContext, NActorsServices::HTTP,
                        Request.GetRemoteAddr()
                        << " " << GetMethod(Request.GetMethod())
                        << " " << Request.GetUri());

        if (Request.GetMethod() == HTTP_METHOD_OPTIONS) {
            return ReplyOptionsAndPassAway();
        }

        if (Request.GetMethod() != HTTP_METHOD_POST) {
            return ReplyErrorAndPassAway("400 Bad Request", "Invalid method");
        }

        if (Request.GetHeader("Content-Type").Before(';') != "application/json") {
            return ReplyErrorAndPassAway("400 Bad Request", "Invalid Content-Type");
        }

        NJson::TJsonValue postData;

        if (!NJson::ReadJsonTree(Request.GetPostContent(), &postData)) {
            return ReplyErrorAndPassAway("400 Bad Request", "Invalid JSON data");
        }

        ReplyDeleteCookieAndPassAway();
    }

    void HandlePoisonPill(TEvents::TEvPoisonPill::TPtr&) {
        PassAway();
    }

    void HandleTimeout() {
        ReplyErrorAndPassAway("504 Gateway Timeout", "Timeout");
    }

    void ReplyOptionsAndPassAway() {
        Result.SetValue(MakeHolder<NMon::TEvHttpInfoRes>(
            "HTTP/1.1 204 No Content\r\n"
            "Allow: OPTIONS, POST\r\n"
            "Connection: Keep-Alive\r\n\r\n", 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

    void ReplyDeleteCookieAndPassAway() {
        TStringStream response;
        response << "HTTP/1.1 200 OK\r\n";
        response << "Set-Cookie: ydb_session_id=; Max-Age=0\r\n";
        response << "\r\n";
        Result.SetValue(MakeHolder<NMon::TEvHttpInfoRes>(response.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

    void ReplyErrorAndPassAway(const TString& status, const TString& error) {
        NJson::TJsonValue body;
        body["error"] = error;
        TStringStream response;
        TString responseBody = NJson::WriteJson(body, false);
        response << "HTTP/1.1 " << status << "\r\n";
        response << "Content-Type: application/json\r\n";
        response << "Content-Length: " << responseBody.Size() << "\r\n";
        response << "\r\n";
        response << responseBody;
        Result.SetValue(MakeHolder<NMon::TEvHttpInfoRes>(response.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

protected:
    IMonHttpRequest& Request;
    NThreading::TPromise<THttpResponsePtr> Result;
    TDuration Timeout = TDuration::Seconds(60);
};

class TLogoutMonPage: public IMonPage {
public:
    TLogoutMonPage(TActorSystem* actorSystem, const TString& path)
        : IMonPage(path, {})
        , ActorSystem(actorSystem)
    {
    }

    void Output(IMonHttpRequest &request) override {
        auto promise = NThreading::NewPromise<THttpResponsePtr>();
        auto future = promise.GetFuture();

        ActorSystem->Register(new TLogoutRequest(request, promise));

        THttpResponsePtr result = future.ExtractValue(TDuration::Max());

        if (result) {
            Output(request, *result);
        }
    }

private:
    void Output(IMonHttpRequest& request, const NMon::IEvHttpInfoRes& result) const {
        result.Output(request.Output());
    }

private:
    TActorSystem* ActorSystem;
};

}

namespace NKikimr {

IMonPage* CreateLoginPage(TActorSystem* actorSystem, const TString& path) {
    return new TLoginMonPage(actorSystem, path);
}

IMonPage* CreateLogoutPage(TActorSystem* actorSystem, const TString& path) {
    return new TLogoutMonPage(actorSystem, path);
}

}

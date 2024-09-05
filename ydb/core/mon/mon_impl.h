#pragma once

#include "mon.h"
#include <ydb/library/services/services.pb.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/page.h>

namespace NActors {

using namespace NMonitoring;
using THttpResponsePtr = THolder<NMon::IEvHttpInfoRes>;

////////////////////////////////////////////////////////////////////////////////
// MON REQUEST
////////////////////////////////////////////////////////////////////////////////
class TMonRequest : public NActors::TActor<TMonRequest> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::ACTORLIB_COMMON;
    }

    TMonRequest(const TActorId &targetActorId, IMonHttpRequest& request,
                NThreading::TPromise<THttpResponsePtr> result, const TVector<TString> &sids, TMon::TRequestAuthorizer authorizer)
        : TActor(&TMonRequest::StateFunc)
        , TargetActorId(targetActorId)
        , Request(request)
        , Result(result)
        , AllowedSIDs(sids)
        , Authorizer(authorizer)
    {
    }

    ~TMonRequest() {
        if (!Result.HasValue()) {
            Result.SetValue(nullptr);
        }
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvBootstrap, HandleBootstrap);
            HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
            HFunc(TEvents::TEvWakeup, HandleWakeup);
            HFunc(TEvents::TEvUndelivered, HandleUndelivered);
            HFunc(NMon::IEvHttpInfoRes, HandleInfoRes);
            HFunc(NKikimr::TEvTicketParser::TEvAuthorizeTicketResult, Handle);
        }
    }

    void HandleBootstrap(TEvents::TEvBootstrap::TPtr &, const TActorContext &ctx) {
        if (Request.GetMethod() == HTTP_METHOD_OPTIONS) {
            return ReplyOptionsResultAndDie(ctx);
        }
        ctx.Schedule(TDuration::Seconds(600), new TEvents::TEvWakeup());
        if (Authorizer) {
            NActors::IEventHandle* handle = Authorizer(SelfId(), Request);
            if (handle) {
                ctx.Send(handle);
                return;
            }
        }
        SendRequest(ctx);
    }

    void HandlePoisonPill(TEvents::TEvPoisonPill::TPtr &, const TActorContext &ctx) {
        Die(ctx);
    }

    void HandleWakeup(TEvents::TEvWakeup::TPtr &, const TActorContext &ctx) {
        Result.SetValue(nullptr);
        Die(ctx);
    }

    void HandleUndelivered(TEvents::TEvUndelivered::TPtr &, const TActorContext &ctx) {
        ReplyActorUnavailableAndDie(ctx);
    }

    void HandleInfoRes(NMon::IEvHttpInfoRes::TPtr &ev, const NActors::TActorContext &ctx) {
        Result.SetValue(THolder<NMon::IEvHttpInfoRes>(ev->Release().Release()));
        Die(ctx);
    }

    void Handle(NKikimr::TEvTicketParser::TEvAuthorizeTicketResult::TPtr &ev, const TActorContext &ctx) {
        const NKikimr::TEvTicketParser::TEvAuthorizeTicketResult &result(*ev->Get());
        if (result.Error) {
            ReplyUnathorizedAndDie(ctx, result.Error.Message);
            return;
        }
        bool found = false;
        for (const TString& sid : AllowedSIDs) {
            if (result.Token->IsExist(sid)) {
                found = true;
                break;;
            }
        }
        if (found || AllowedSIDs.empty()) {
            User = result.Token->GetUserSID();
            SendRequest(ctx, result.SerializedToken);
        } else {
            ReplyForbiddenAndDie(ctx, TStringBuilder() << "SID is not allowed");
        }
    }

    TString GetUser() const {
        return User ? User : "anonymous";
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

    void SendRequest(const TActorContext &ctx, const TString& serializedToken = TString()) {
        if (Authorizer) {
            LOG_WARN_S(ctx, NActorsServices::HTTP,
                        Request.GetRemoteAddr()
                        << " " << GetUser()
                        << " " << GetMethod(Request.GetMethod())
                        << " " << Request.GetUri());
        }
        ctx.Send(TargetActorId, new NMon::TEvHttpInfo(Request, serializedToken), IEventHandle::FlagTrackDelivery);
    }

    void ReplyOptionsResultAndDie(const TActorContext &ctx) {
        TString url(Request.GetPathInfo());
        TString type = mimetypeByExt(url.data());
        if (type.empty()) {
            type = "application/json";
        }
        TString origin = TString(Request.GetHeader("Origin"));
        if (origin.empty()) {
            origin = "*";
        }
        TStringBuilder response;
        response << "HTTP/1.1 204 No Content\r\n"
                    "Access-Control-Allow-Origin: " << origin << "\r\n"
                    "Access-Control-Allow-Credentials: true\r\n"
                    "Access-Control-Allow-Headers: Content-Type,Authorization,Origin,Accept\r\n"
                    "Access-Control-Allow-Methods: OPTIONS, GET, POST, PUT, DELETE\r\n"
                    "Content-Type: " + type + "\r\n"
                    "Connection: Keep-Alive\r\n\r\n";
        Result.SetValue(MakeHolder<NMon::TEvHttpInfoRes>(response, 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }

    void ReplyUnathorizedAndDie(const TActorContext &ctx, const TString& error = TString()) {
        TStringStream response;
        TStringStream body;
        body << "<html><body><h1>401 Unauthorized</h1>";
        if (!error.empty()) {
            body << "<p>" << error << "</p>";
        }
        body << "</body></html>";
        TString origin = TString(Request.GetHeader("Origin"));
        if (origin.empty()) {
            origin = "*";
        }
        response << "HTTP/1.1 401 Unauthorized\r\n";
        response << "Access-Control-Allow-Origin: " << origin << "\r\n";
        response << "Access-Control-Allow-Credentials: true\r\n";
        response << "Access-Control-Allow-Headers: Content-Type,Authorization,Origin,Accept\r\n";
        response << "Access-Control-Allow-Methods: OPTIONS, GET, POST, PUT, DELETE\r\n";
        response << "Content-Type: text/html\r\n";
        response << "Content-Length: " << body.Size() << "\r\n";
        response << "\r\n";
        response << body.Str();
        Result.SetValue(MakeHolder<NMon::TEvHttpInfoRes>(response.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }

    void ReplyForbiddenAndDie(const TActorContext &ctx, const TString& error = TString()) {
        TStringStream response;
        TStringStream body;
        body << "<html><body><h1>403 Forbidden</h1>";
        if (!error.empty()) {
            body << "<p>" << error << "</p>";
        }
        body << "</body></html>";
        response << "HTTP/1.1 403 Forbidden\r\n";
        response << "Content-Type: text/html\r\n";
        response << "Content-Length: " << body.Size() << "\r\n";
        response << "\r\n";
        response << body.Str();
        Result.SetValue(MakeHolder<NMon::TEvHttpInfoRes>(response.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }

    void ReplyActorUnavailableAndDie(const TActorContext &ctx, const TString& error = TString()) {
        TStringStream response;
        TStringStream body;
        body << "<html><body><h1>503 Actor Unavailable</h1>";
        if (!error.empty()) {
            body << "<p>" << error << "</p>";
        }
        body << "</body></html>";
        response << "HTTP/1.1 503 Actor Unavailable\r\n";
        response << "Content-Type: text/html\r\n";
        response << "Content-Length: " << body.Size() << "\r\n";
        response << "\r\n";
        response << body.Str();
        Result.SetValue(MakeHolder<NMon::TEvHttpInfoRes>(response.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }

    virtual TAutoPtr<NActors::IEventHandle> AfterRegister(const NActors::TActorId &self, const TActorId& parentId) override {
        Y_UNUSED(parentId);
        return new NActors::IEventHandle(self, self, new TEvents::TEvBootstrap(), 0);
    }

protected:
    TActorId TargetActorId;
    IMonHttpRequest& Request;
    NThreading::TPromise<THttpResponsePtr> Result;
    const TVector<TString> &AllowedSIDs;
    TMon::TRequestAuthorizer Authorizer;
    TString User;
};


////////////////////////////////////////////////////////////////////////////////
// HTML results page
////////////////////////////////////////////////////////////////////////////////
class THtmlResultMonPage: public THtmlMonPage {
public:
    THtmlResultMonPage(const TString &path, const TString &title, const TString &host, bool preTag,
                        const NMon::IEvHttpInfoRes &result)
        : THtmlMonPage(path, title, true)
        , Host(host)
        , PreTag(preTag)
        , Result(result)
    {
    }

    void Output(NMonitoring::IMonHttpRequest& request) override {
        IOutputStream& out = request.Output();

        out << "HTTP/1.1 200 Ok\r\n"
            << "Content-Type: text/html\r\n"
            << "Connection: Close\r\n";
        TString origin = TString(request.GetHeader("Origin"));
        if (origin.empty()) {
            origin = "*";
        }
        out << "Access-Control-Allow-Origin: " << origin << "\r\n"
            << "Access-Control-Allow-Credentials: true\r\n"
            << "Access-Control-Allow-Headers: Content-Type,Authorization,Origin,Accept\r\n"
            << "Access-Control-Allow-Methods: OPTIONS, GET, POST\r\n";
        out << "\r\n";

        out << "<!DOCTYPE html>\n";
        out << "<html>";
        out << "<head>";
        if (Title) {
            if (Host) {
                out << "<title>" << Title << " - " << Host << "</title>\n";
            } else {
                out << "<title>" << Title << "</title>\n";
            }
        }

        out << "<link rel='stylesheet' href='/static/css/bootstrap.min.css'>\n";
        out << "<script language='javascript' type='text/javascript' src='/static/js/jquery.min.js'></script>\n";
        out << "<script language='javascript' type='text/javascript' src='/static/js/bootstrap.min.js'></script>\n";

        if (OutputTableSorterJsCss) {
            out << "<link rel='stylesheet' href='/jquery.tablesorter.css'>\n";
            out << "<script language='javascript' type='text/javascript' src='/jquery.tablesorter.js'></script>\n";
        }

        out << "<style type=\"text/css\">\n";
        out << ".table-nonfluid { width: auto; }\n";
        out << ".narrow-line50 {line-height: 50%}\n";
        out << ".narrow-line60 {line-height: 60%}\n";
        out << ".narrow-line70 {line-height: 70%}\n";
        out << ".narrow-line80 {line-height: 80%}\n";
        out << ".narrow-line90 {line-height: 90%}\n";
        out << "</style>\n";
        out << "</head>";
        out << "<body>";

        OutputNavBar(out);

        out << "<div class='container'>";
        if (Title) {
            out << "<h2>" << Title << "</h2>";
        }
        OutputContent(request);
        out << "</div>";
        out << "</body>";
    }

    void OutputContent(NMonitoring::IMonHttpRequest &request) override {
        if (PreTag) {
            request.Output() << "<pre>\n";
        }
        Result.Output(request.Output());
        if (PreTag) {
            request.Output() << "</pre>\n";
        }
    }

private:
    TString Host;
    bool PreTag;
    const NMon::IEvHttpInfoRes &Result;
};


////////////////////////////////////////////////////////////////////////////////
// INDEX PAGE
// Redirects index page to fixed url
////////////////////////////////////////////////////////////////////////////////
class TIndexRedirectMonPage: public IMonPage {
public:
    TIndexRedirectMonPage(TIntrusivePtr<TIndexMonPage> indexMonPage, const TString& path = "internal")
        : IMonPage(path)
        , IndexMonPage(std::move(indexMonPage))
    {
    }

    void Output(IMonHttpRequest& request) override {
        auto& out = request.Output();
        out << HTTPOKHTML;
        out << "<html>\n";
        IndexMonPage->OutputHead(out);

        out << "<body>\n";

        // part of common navbar
        IndexMonPage->OutputNavBar(out);

        out << "<div class='container'>\n"
                << "<h2>" << IndexMonPage->Title << "</h2>\n";
        IndexMonPage->OutputIndex(out, true);
        out << "</div>\n"
            << "</body>\n";
        out << "</html>\n";
    }

    TIntrusivePtr<TIndexMonPage> IndexMonPage;
};


////////////////////////////////////////////////////////////////////////////////
// ACTOR MONITORING PAGE
// Encapsulates a request to an actor
////////////////////////////////////////////////////////////////////////////////
class TActorMonPage: public IMonPage {
public:
    TActorMonPage(const TString &path, const TString &title, const TString &host, bool preTag,
                    TActorSystem *actorSystem, const TActorId &actorId, const TVector<TString> &sids,
                    TMon::TRequestAuthorizer authorizer, TString monServiceName = "utils")
        : IMonPage(path, title)
        , Host(host)
        , PreTag(preTag)
        , ActorSystem(actorSystem)
        , TargetActorId(actorId)
        , AllowedSIDs(sids)
        , Authorizer(std::move(authorizer))
        , MonServiceName(monServiceName)
    {
    }

    void Output(IMonHttpRequest &request) override {
        auto promise = NThreading::NewPromise<THttpResponsePtr>();
        auto future = promise.GetFuture();

        ActorSystem->Register(new TMonRequest(TargetActorId, request, promise, AllowedSIDs, Authorizer));

        THttpResponsePtr result = future.ExtractValue(TDuration::Max());

        if (result) {
            Output(request, *result);
        } else {
            TStringStream out;
            out << "Error: timeout. We were not able to receive response from '"
                << Title << "'.\n";
            Output(request, NMon::TEvHttpInfoRes(out.Str()));
        }
    }

    void Output(IMonHttpRequest &request, const NMon::IEvHttpInfoRes &result) const {
        if (result.GetContentType() == NMon::IEvHttpInfoRes::Html) {
            THtmlResultMonPage resultPage(Path, Title, Host, PreTag, result);
            resultPage.Parent = this->Parent;
            resultPage.Output(request);
        } else {
            result.Output(request.Output());
        }
    }

    TString Host;
    bool PreTag;
    TActorSystem *ActorSystem;
    TActorId TargetActorId;
    const TVector<TString> AllowedSIDs;
    TMon::TRequestAuthorizer Authorizer;
    TString MonServiceName;
};

inline TString GetPageFullPath(const NMonitoring::IMonPage* page) {
    TStringBuilder path;
    if (page->Parent) {
        path << GetPageFullPath(page->Parent);
        path << '/';
    }
    path << page->Path;
    return path;
}

}

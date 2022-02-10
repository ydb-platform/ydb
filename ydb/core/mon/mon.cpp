#include "mon.h"

#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/mon.h>
#include <library/cpp/actors/core/probes.h>
#include <library/cpp/lwtrace/mon/mon_lwtrace.h>
#include <library/cpp/mime/types/mime.h>
#include <library/cpp/monlib/service/pages/version_mon_page.h>
#include <library/cpp/monlib/service/pages/mon_page.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/page.h>
#include <library/cpp/threading/future/future.h>
#include <library/cpp/string_utils/url/url.h>
#include <util/system/event.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/monitoring_provider.h>
#include <ydb/core/base/ticket_parser.h>

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
                LOG_WARN_S(ctx, NKikimrServices::HTTP,
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
            Result.SetValue(MakeHolder<NMon::TEvHttpInfoRes>(
                                "HTTP/1.1 204 No Content\r\n"
                                "Allow: OPTIONS, GET, POST\r\n"
                                "Content-Type: " + type + "\r\n"
                                "Connection: Keep-Alive\r\n\r\n", 0, NMon::IEvHttpInfoRes::EContentType::Custom));
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
            response << "HTTP/1.1 401 Unauthorized\r\n";
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

            out << HTTPOKHTML;

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

            out << "<link rel='stylesheet' href='https://yastatic.net/bootstrap/3.3.1/css/bootstrap.min.css'>\n";
            out << "<script language='javascript' type='text/javascript' src='https://yastatic.net/jquery/2.1.3/jquery.min.js'></script>\n";
            out << "<script language='javascript' type='text/javascript' src='https://yastatic.net/bootstrap/3.3.1/js/bootstrap.min.js'></script>\n";

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
            IndexMonPage->OutputIndexPage(request);
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
                      TMon::TRequestAuthorizer authorizer)
            : IMonPage(path, title)
            , Host(host)
            , PreTag(preTag)
            , ActorSystem(actorSystem)
            , TargetActorId(actorId)
            , AllowedSIDs(sids)
            , Authorizer(std::move(authorizer))
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

    private:
        void Output(IMonHttpRequest &request, const NMon::IEvHttpInfoRes &result) const {
            if (result.GetContentType() == NMon::IEvHttpInfoRes::Html) {
                THtmlResultMonPage resultPage(Path, Title, Host, PreTag, result);
                resultPage.Parent = this->Parent; 
                resultPage.Output(request);
            } else {
                result.Output(request.Output());
            }
        }

    private:
        TString Host;
        bool PreTag;
        TActorSystem *ActorSystem;
        TActorId TargetActorId;
        const TVector<TString> AllowedSIDs;
        TMon::TRequestAuthorizer Authorizer;
    };


    ////////////////////////////////////////////////////////////////////////////////
    // TMON CLASS
    ////////////////////////////////////////////////////////////////////////////////
    TMon::TMon(TMon::TConfig config)
        : TBase(config.Port, config.Address, config.Threads, config.Title)
        , Config(std::move(config))
    {
    }

    TMon::~TMon() {
        Stop();
    }

    void TMon::Start() {
        TBase::Register(new TIndexRedirectMonPage(IndexMonPage));
        TBase::Register(new NMonitoring::TVersionMonPage);
        TBase::Register(new NMonitoring::TTablesorterCssMonPage);
        TBase::Register(new NMonitoring::TTablesorterJsMonPage);

        NLwTraceMonPage::RegisterPages((TBase*)this);
        NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(ACTORLIB_PROVIDER));
        NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(MONITORING_PROVIDER));
        TBase::Start();
    }

    void TMon::Stop() {
        IndexMonPage->ClearPages(); // it's required to avoid loop-reference
        TBase::Stop();
    }

    void TMon::Register(NMonitoring::IMonPage *page) {
        TBase::Register(page);
        TBase::SortPages();
    }

    TIndexMonPage *TMon::RegisterIndexPage(const TString &path, const TString &title) {
        auto page = TBase::RegisterIndexPage(path, title);
        TBase::SortPages();
        return page;
    }

    IMonPage *TMon::RegisterActorPage(TIndexMonPage *index, const TString &relPath,
        const TString &title, bool preTag, TActorSystem *actorSystem, const TActorId &actorId, bool useAuth) {
        return RegisterActorPage({
            .Title = title,
            .RelPath = relPath,
            .ActorSystem = actorSystem,
            .Index = index,
            .PreTag = preTag,
            .ActorId = actorId,
            .UseAuth = useAuth,
        });
    }

    IMonPage* TMon::RegisterActorPage(TMon::TRegisterActorPageFields fields) {
        IMonPage *page = new TActorMonPage(
            fields.RelPath,
            fields.Title,
            Config.Host,
            fields.PreTag,
            fields.ActorSystem,
            fields.ActorId,
            fields.AllowedSIDs ? fields.AllowedSIDs : Config.AllowedSIDs,
            fields.UseAuth ? Config.Authorizer : TRequestAuthorizer());
        if (fields.Index) {
            fields.Index->Register(page);
            fields.Index->SortPages();
        } else {
            Register(page);
        }

        return page;
    }

    IMonPage *TMon::RegisterCountersPage(const TString &path, const TString &title, TIntrusivePtr<TDynamicCounters> counters) {
        TDynamicCountersPage* page = new TDynamicCountersPage(path, title, counters);
        page->SetUnknownGroupPolicy(EUnknownGroupPolicy::Ignore);
        Register(page);
        return page;
    }

    IMonPage *TMon::FindPage(const TString &relPath) {
        return TBase::FindPage(relPath);
    }

    TIndexMonPage *TMon::FindIndexPage(const TString &relPath) {
        return TBase::FindIndexPage(relPath);
    }

    void TMon::OutputIndexPage(IOutputStream& out) {
        if (Config.RedirectMainPageTo) {
            // XXX manual http response construction
            out << "HTTP/1.1 302 Found\r\n"
                << "Location: " << Config.RedirectMainPageTo << "\r\n"
                << "Connection: Close\r\n\r\n";
        } else {
            NMonitoring::TMonService2::OutputIndexPage(out);
        }
    }

    void TMon::SetAllowedSIDs(const TVector<TString>& sids) {
        Config.AllowedSIDs = sids;
    }

    ui16 TMon::GetListenPort() {
        return Options().Port;
    }

    NActors::IEventHandle* TMon::DefaultAuthorizer(const NActors::TActorId& owner, NMonitoring::IMonHttpRequest& request) {
        TStringBuf ydbSessionId = request.GetCookie("ydb_session_id");
        TStringBuf authorization = request.GetHeader("Authorization");
        if (!authorization.empty()) {
            return new NActors::IEventHandle(
                NKikimr::MakeTicketParserID(),
                owner,
                new NKikimr::TEvTicketParser::TEvAuthorizeTicket({
                    .Ticket = TString(authorization)
                }),
                IEventHandle::FlagTrackDelivery
            );
        } else if (!ydbSessionId.empty()) {
            return new NActors::IEventHandle(
                NKikimr::MakeTicketParserID(),
                owner,
                new NKikimr::TEvTicketParser::TEvAuthorizeTicket({
                    .Ticket = TString("Login ") + TString(ydbSessionId)
                }),
                IEventHandle::FlagTrackDelivery
            );
        } else if (NKikimr::AppData()->EnforceUserTokenRequirement && NKikimr::AppData()->DefaultUserSIDs.empty()) {
            return new NActors::IEventHandle(
                owner,
                owner,
                new NKikimr::TEvTicketParser::TEvAuthorizeTicketResult(TString(), {
                    .Message = "No security credentials were provided",
                    .Retryable = false
                })
            );
        } else if (!NKikimr::AppData()->DefaultUserSIDs.empty()) {
            TIntrusivePtr<NACLib::TUserToken> token = new NACLib::TUserToken(NKikimr::AppData()->DefaultUserSIDs);
            return new NActors::IEventHandle(
                owner,
                owner,
                new NKikimr::TEvTicketParser::TEvAuthorizeTicketResult(TString(), token, token->SerializeAsString())
            );
        } else {
            return nullptr;
        }
    }

} // NActors

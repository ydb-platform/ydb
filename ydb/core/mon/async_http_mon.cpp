#include "async_http_mon.h"
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/http/http_proxy.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/ticket_parser.h>

#include <library/cpp/lwtrace/all.h>
#include <library/cpp/lwtrace/mon/mon_lwtrace.h>
#include <library/cpp/actors/core/probes.h>
#include <ydb/core/base/monitoring_provider.h>

#include <library/cpp/monlib/service/pages/version_mon_page.h>
#include <library/cpp/monlib/service/pages/mon_page.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/page.h>

#include <util/system/hostname.h>

#include "mon_impl.h"

namespace NActors {

// compatibility layer
class THttpMonRequest : public NMonitoring::IMonHttpRequest {
public:
    NHttp::THttpIncomingRequestPtr Request;
    TStringStream& Response;
    NMonitoring::IMonPage* Page;
    TString PathInfo;
    mutable std::unique_ptr<THttpHeaders> Headers;
    mutable std::unique_ptr<TCgiParameters> Params;
    mutable std::unique_ptr<TCgiParameters> PostParams;

    THttpMonRequest(NHttp::THttpIncomingRequestPtr request, TStringStream& response, NMonitoring::IMonPage* page, const TString& pathInfo)
        : Request(request)
        , Response(response)
        , Page(page)
        , PathInfo(pathInfo)
    {
    }

    static TString GetPathFromUrl(TStringBuf url) {
        return TString(url.Before('?'));
    }

    static TString GetPathInfoFromUrl(NMonitoring::IMonPage* page, TStringBuf url) {
        TString path = GetPageFullPath(page);
        url.SkipPrefix(path);
        return GetPathFromUrl(url);
    }

    virtual IOutputStream& Output() override {
        return Response;
    }

    virtual HTTP_METHOD GetMethod() const override {
        if (Request->Method == "GET") {
            return HTTP_METHOD_GET;
        }
        if (Request->Method == "OPTIONS") {
            return HTTP_METHOD_OPTIONS;
        }
        if (Request->Method == "POST") {
            return HTTP_METHOD_POST;
        }
        if (Request->Method == "HEAD") {
            return HTTP_METHOD_HEAD;
        }
        if (Request->Method == "PUT") {
            return HTTP_METHOD_PUT;
        }
        return HTTP_METHOD_UNDEFINED;
    }

    virtual TStringBuf GetPath() const override {
        return GetPathFromUrl(Request->URL);
    }

    virtual TStringBuf GetPathInfo() const override {
        return PathInfo;
    }

    virtual TStringBuf GetUri() const override {
        return Request->URL;
    }

    virtual const TCgiParameters& GetParams() const override {
        if (!Params) {
            Params = std::make_unique<TCgiParameters>(Request->URL.After('?'));
        }
        return *Params;
    }

    virtual const TCgiParameters& GetPostParams() const override {
        if (!PostParams) {
            PostParams = std::make_unique<TCgiParameters>(Request->Body);
        }
        return *PostParams;
    }

    virtual TStringBuf GetPostContent() const override {
        return Request->Body;
    }

    virtual const THttpHeaders& GetHeaders() const override {
        if (!Headers) {
            TString strHeaders(Request->Headers);
            TStringInput headers(strHeaders);
            Headers = std::make_unique<THttpHeaders>(&headers);
        }
        return *Headers;
    }

    virtual TStringBuf GetHeader(TStringBuf name) const override {
        auto header = GetHeaders().FindHeader(name);
        if (header) {
            return header->Value();
        }
        return {};
    }

    virtual TStringBuf GetCookie(TStringBuf name) const override {
        NHttp::TCookies cookies(GetHeader("Cookie"));
        return cookies.Get(name);
    }

    virtual TString GetRemoteAddr() const override {
        return Request->Address.ToString();
    }

    virtual TString GetServiceTitle() const override {
        return {};
    }

    virtual NMonitoring::IMonPage* GetPage() const override {
        return Page;
    }

    virtual IMonHttpRequest* MakeChild(NMonitoring::IMonPage* page, const TString& pathInfo) const override {
        return new THttpMonRequest(Request, Response, page, pathInfo);
    }
};

// container for legacy requests
class THttpMonRequestContainer : public TStringStream, public THttpMonRequest {
public:
    THttpMonRequestContainer(NHttp::THttpIncomingRequestPtr request, NMonitoring::IMonPage* index)
        : THttpMonRequest(request, *this, index, GetPathInfoFromUrl(index, request->URL))
    {
    }
};

// handles actor communication
class THttpMonLegacyActorRequest : public TActorBootstrapped<THttpMonLegacyActorRequest> {
public:
    NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr Event;
    THttpMonRequestContainer Container;
    TActorMonPage* ActorMonPage;

    THttpMonLegacyActorRequest(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, TActorMonPage* actorMonPage)
        : Event(std::move(event))
        , Container(Event->Get()->Request, actorMonPage)
        , ActorMonPage(actorMonPage)
    {}

    void Bootstrap() {
        if (Event->Get()->Request->Method == "OPTIONS") {
            return ReplyOptionsAndPassAway();
        }
        Become(&THttpMonLegacyActorRequest::StateFunc);
        Schedule(TDuration::Seconds(600), new TEvents::TEvWakeup());
        if (ActorMonPage->Authorizer) {
            NActors::IEventHandle* handle = ActorMonPage->Authorizer(SelfId(), Container);
            if (handle) {
                TActivationContext::Send(handle);
                return;
            }
        }
        SendRequest();
    }

    void ReplyWith(NHttp::THttpOutgoingResponsePtr response) {
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
        TString origin = TString(headers["Origin"]);
        if (origin.empty()) {
            origin = "*";
        }
        TStringBuilder response;
        response << "HTTP/1.1 204 No Content\r\n"
                    "Access-Control-Allow-Origin: " << origin << "\r\n"
                    "Access-Control-Allow-Credentials: true\r\n"
                    "Access-Control-Allow-Headers: Content-Type,Authorization,Origin,Accept\r\n"
                    "Access-Control-Allow-Methods: OPTIONS, GET, POST\r\n"
                    "Content-Type: " + type + "\r\n"
                    "Connection: keep-alive\r\n\r\n";
        ReplyWith(request->CreateResponseString(response));
        PassAway();
    }

    void ReplyUnathorizedAndPassAway(const TString& error = {}) {
        NHttp::THttpIncomingRequestPtr request = Event->Get()->Request;
        NHttp::THeaders headers(request->Headers);
        TStringBuilder response;
        TStringBuilder body;
        body << "<html><body><h1>401 Unauthorized</h1>";
        if (!error.empty()) {
            body << "<p>" << error << "</p>";
        }
        body << "</body></html>";
        TString origin = TString(headers["Origin"]);
        if (origin.empty()) {
            origin = "*";
        }
        response << "HTTP/1.1 401 Unauthorized\r\n";
        response << "Access-Control-Allow-Origin: " << origin << "\r\n";
        response << "Access-Control-Allow-Credentials: true\r\n";
        response << "Access-Control-Allow-Headers: Content-Type,Authorization,Origin,Accept\r\n";
        response << "Access-Control-Allow-Methods: OPTIONS, GET, POST\r\n";
        response << "Content-Type: text/html\r\n";
        response << "Content-Length: " << body.Size() << "\r\n";
        response << "\r\n";
        response << body;
        ReplyWith(request->CreateResponseString(response));
        PassAway();
    }

    void ReplyForbiddenAndPassAway(const TString& error = {}) {
        NHttp::THttpIncomingRequestPtr request = Event->Get()->Request;
        TStringBuilder response;
        TStringBuilder body;
        body << "<html><body><h1>403 Forbidden</h1>";
        if (!error.empty()) {
            body << "<p>" << error << "</p>";
        }
        body << "</body></html>";
        response << "HTTP/1.1 403 Forbidden\r\n";
        response << "Content-Type: text/html\r\n";
        response << "Content-Length: " << body.Size() << "\r\n";
        response << "\r\n";
        response << body;
        ReplyWith(request->CreateResponseString(response));
        PassAway();
    }

    void SendRequest(const NKikimr::TEvTicketParser::TEvAuthorizeTicketResult* authorizeResult = {}) {
        NHttp::THttpIncomingRequestPtr request = Event->Get()->Request;
        if (ActorMonPage->Authorizer) {
            TString user = authorizeResult ? authorizeResult->Token->GetUserSID() : "anonymous";
            LOG_NOTICE_S(*TlsActivationContext, NActorsServices::HTTP,
                request->Address.ToString()
                << " " << user
                << " " << request->Method
                << " " << request->URL);
        }
        TString serializedToken = authorizeResult ? authorizeResult->SerializedToken : "";
        Send(ActorMonPage->TargetActorId, new NMon::TEvHttpInfo(
            Container, serializedToken), IEventHandle::FlagTrackDelivery);
    }

    void HandleWakeup(TEvents::TEvWakeup::TPtr&) {
        NHttp::THttpIncomingRequestPtr request = Event->Get()->Request;
        ReplyWith(request->CreateResponseGatewayTimeout(
            TStringBuilder() << "Timeout requesting actor " << ActorMonPage->TargetActorId));
        PassAway();
    }

    void HandleUndelivered(TEvents::TEvUndelivered::TPtr&) {
        NHttp::THttpIncomingRequestPtr request = Event->Get()->Request;
        ReplyWith(request->CreateResponseServiceUnavailable(
            TStringBuilder() << "Actor " << ActorMonPage->TargetActorId << " is not available"));
        PassAway();
    }

    void HandleResponse(NMon::IEvHttpInfoRes::TPtr& ev) {
        if (ev->Get()->GetContentType() == NMon::IEvHttpInfoRes::Html) {
            THtmlResultMonPage resultPage(ActorMonPage->Path, ActorMonPage->Title, ActorMonPage->Host, ActorMonPage->PreTag, *(ev->Get()));
            resultPage.Parent = ActorMonPage->Parent;
            resultPage.Output(Container);
        } else {
            ev->Get()->Output(Container);
        }
        ReplyWith(Event->Get()->Request->CreateResponseString(Container.Str()));
        PassAway();
    }

    void Handle(NKikimr::TEvTicketParser::TEvAuthorizeTicketResult::TPtr& ev) {
        const NKikimr::TEvTicketParser::TEvAuthorizeTicketResult& result(*ev->Get());
        if (result.Error) {
            return ReplyUnathorizedAndPassAway(result.Error.Message);
        }
        bool found = false;
        for (const TString& sid : ActorMonPage->AllowedSIDs) {
            if (result.Token->IsExist(sid)) {
                found = true;
                break;
            }
        }
        if (found || ActorMonPage->AllowedSIDs.empty()) {
            SendRequest(&result);
        } else {
            return ReplyForbiddenAndPassAway("SID is not allowed");
        }
    }

    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvWakeup, HandleWakeup);
            hFunc(TEvents::TEvUndelivered, HandleUndelivered);
            hFunc(NMon::IEvHttpInfoRes, HandleResponse);
            hFunc(NKikimr::TEvTicketParser::TEvAuthorizeTicketResult, Handle);
        }
    }
};

// handles all indexes and static data in synchronous way
class THttpMonLegacyIndexRequest : public TActorBootstrapped<THttpMonLegacyIndexRequest> {
public:
    NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr Event;
    THttpMonRequestContainer Container;

    THttpMonLegacyIndexRequest(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, NMonitoring::IMonPage* index)
        : Event(std::move(event))
        , Container(Event->Get()->Request, index)
    {}

    void Bootstrap() {
        ProcessRequest();
    }

    void ProcessRequest() {
        Container.Page->Output(Container);
        NHttp::THttpOutgoingResponsePtr response = Event->Get()->Request->CreateResponseString(Container.Str());
        Send(Event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        PassAway();
    }
};

// receives all requests for one actor page and converts them to request-actors
class THttpMonServiceLegacyActor : public TActorBootstrapped<THttpMonServiceLegacyActor> {
public:
    THttpMonServiceLegacyActor(TActorMonPage* actorMonPage)
        : ActorMonPage(actorMonPage)
    {
    }

    void Bootstrap() {
        Become(&THttpMonServiceLegacyActor::StateWork);
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev) {
        Register(new THttpMonLegacyActorRequest(std::move(ev), ActorMonPage));
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }

    TActorMonPage* ActorMonPage;
};

// receives everyhing not related to actor communcation, converts them to request-actors
class THttpMonServiceLegacyIndex : public TActorBootstrapped<THttpMonServiceLegacyIndex> {
public:
    THttpMonServiceLegacyIndex(TIntrusivePtr<NMonitoring::TIndexMonPage> indexMonPage, const TString& redirectRoot = {})
        : IndexMonPage(std::move(indexMonPage))
        , RedirectRoot(redirectRoot)
    {
    }

    void Bootstrap() {
        Become(&THttpMonServiceLegacyIndex::StateWork);
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev) {
        if (ev->Get()->Request->URL == "/" && RedirectRoot) {
            TStringBuilder response;
            response << "HTTP/1.1 302 Found\r\nLocation: " << RedirectRoot << "\r\n\r\n";
            Send(ev->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(ev->Get()->Request->CreateResponseString(response)));
        } else {
            Register(new THttpMonLegacyIndexRequest(std::move(ev), IndexMonPage.Get()));
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }

    TIntrusivePtr<NMonitoring::TIndexMonPage> IndexMonPage;
    TString RedirectRoot;
};

TAsyncHttpMon::TAsyncHttpMon(TConfig config)
    : Config(std::move(config))
    , IndexMonPage(new NMonitoring::TIndexMonPage("", Config.Title))
{
}

void TAsyncHttpMon::Start(TActorSystem* actorSystem) {
    if (actorSystem) {
        ActorSystem = actorSystem;
        Register(new TIndexRedirectMonPage(IndexMonPage));
        Register(new NMonitoring::TVersionMonPage);
        Register(new NMonitoring::TTablesorterCssMonPage);
        Register(new NMonitoring::TTablesorterJsMonPage);
        NLwTraceMonPage::RegisterPages(IndexMonPage.Get());
        NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(ACTORLIB_PROVIDER));
        NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(MONITORING_PROVIDER));
        HttpProxyActorId = ActorSystem->Register(
            NHttp::CreateHttpProxy(),
            TMailboxType::ReadAsFilled,
            ActorSystem->AppData<NKikimr::TAppData>()->UserPoolId);
        HttpMonServiceActorId = ActorSystem->Register(
            new THttpMonServiceLegacyIndex(IndexMonPage, Config.RedirectMainPageTo),
            TMailboxType::ReadAsFilled,
            ActorSystem->AppData<NKikimr::TAppData>()->UserPoolId);
        TStringBuilder workerName;
        workerName << FQDNHostName() << ":" << Config.Port;
        auto addPort = std::make_unique<NHttp::TEvHttpProxy::TEvAddListeningPort>();
        addPort->Port = Config.Port;
        addPort->WorkerName = workerName;
        addPort->Address = Config.Address;
        addPort->CompressContentTypes = {
            "text/plain",
            "text/html",
            "text/css",
            "application/javascript",
            "application/json",
        };
        ActorSystem->Send(HttpProxyActorId, addPort.release());
        ActorSystem->Send(HttpProxyActorId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/", HttpMonServiceActorId));
        for (NMonitoring::IMonPage* page : ActorMonPages) {
            RegisterActorMonPage(page);
        }
        ActorMonPages.clear();
    }
}

void TAsyncHttpMon::Stop() {
    IndexMonPage->ClearPages(); // it's required to avoid loop-reference
    if (ActorSystem) {
        for (const TActorId& actorId : ActorServices) {
            ActorSystem->Send(actorId, new TEvents::TEvPoisonPill);
        }
        ActorSystem->Send(HttpMonServiceActorId, new TEvents::TEvPoisonPill);
        ActorSystem->Send(HttpProxyActorId, new TEvents::TEvPoisonPill);
    }
}

void TAsyncHttpMon::Register(NMonitoring::IMonPage* page) {
    IndexMonPage->Register(page);
}

NMonitoring::TIndexMonPage* TAsyncHttpMon::RegisterIndexPage(const TString& path, const TString& title) {
    auto page = IndexMonPage->RegisterIndexPage(path, title);
    IndexMonPage->SortPages();
    return page;
}

void TAsyncHttpMon::RegisterActorMonPage(NMonitoring::IMonPage* page) {
    if (ActorSystem) {
        TActorMonPage* actorMonPage = reinterpret_cast<TActorMonPage*>(page);
        auto actorId = ActorSystem->Register(
            new THttpMonServiceLegacyActor(actorMonPage),
            TMailboxType::ReadAsFilled,
            ActorSystem->AppData<NKikimr::TAppData>()->UserPoolId);
        ActorSystem->Send(HttpProxyActorId, new NHttp::TEvHttpProxy::TEvRegisterHandler(GetPageFullPath(actorMonPage), actorId));
        ActorServices.push_back(actorId);
    }
}

NMonitoring::IMonPage* TAsyncHttpMon::RegisterActorPage(TRegisterActorPageFields fields) {
    TActorMonPage* page = new TActorMonPage(
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

    if (ActorSystem && HttpProxyActorId) {
        RegisterActorMonPage(page);
    } else {
        ActorMonPages.push_back(page);
    }

    return page;
}

NMonitoring::IMonPage* TAsyncHttpMon::RegisterCountersPage(const TString& path, const TString& title, TIntrusivePtr<NMonitoring::TDynamicCounters> counters) {
    TDynamicCountersPage* page = new TDynamicCountersPage(path, title, counters);
        page->SetUnknownGroupPolicy(EUnknownGroupPolicy::Ignore);
        Register(page);
        return page;
}

NMonitoring::IMonPage* TAsyncHttpMon::FindPage(const TString& relPath) {
    return IndexMonPage->FindPage(relPath);
}

}

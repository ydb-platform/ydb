#include "mon.h"
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/base/ticket_parser.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/lwtrace/all.h>
#include <library/cpp/lwtrace/mon/mon_lwtrace.h>
#include <ydb/library/actors/core/probes.h>
#include <ydb/core/base/monitoring_provider.h>

#include <library/cpp/monlib/service/pages/version_mon_page.h>
#include <library/cpp/monlib/service/pages/mon_page.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/page.h>

#include <util/system/hostname.h>

#include <ydb/core/base/counters.h>
#include <ydb/core/protos/mon.pb.h>

#include "mon_impl.h"

namespace NActors {

struct TEvMon {
    enum {
        EvMonitoringRequest = NActors::NMon::HttpInfo + 10,
        EvMonitoringResponse,
        End
    };

    static_assert(EvMonitoringRequest > NMon::End, "expect EvMonitoringRequest > NMon::End");
    static_assert(End < EventSpaceEnd(NActors::TEvents::ES_MON), "expect End < EventSpaceEnd(NActors::TEvents::ES_MON)");

    struct TEvMonitoringRequest : TEventPB<TEvMonitoringRequest, NKikimrMonProto::TEvMonitoringRequest, EvMonitoringRequest> {
        TEvMonitoringRequest() = default;
    };

    struct TEvMonitoringResponse : TEventPB<TEvMonitoringResponse, NKikimrMonProto::TEvMonitoringResponse, EvMonitoringResponse> {
        TEvMonitoringResponse() = default;
    };
};

namespace {

using namespace NKikimr;

bool HasJsonContent(NMonitoring::IMonHttpRequest& request) {
    const TStringBuf header = request.GetHeader("Content-Type");
    return header.empty() || AsciiEqualsIgnoreCase(header, "application/json"); // by default we will try to parse json, no error will be generated if parsing fails
}

TString GetDatabase(NMonitoring::IMonHttpRequest& request) {
    if (const auto dbIt = request.GetParams().Find("database"); dbIt != request.GetParams().end()) {
        return dbIt->second;
    }
    if (request.GetMethod() == HTTP_METHOD_POST && HasJsonContent(request)) {
        NJson::TJsonValue requestData;
        if (NJson::ReadJsonTree(request.GetPostContent(), &requestData)) {
            return requestData["database"].GetString(); // empty if not string or no such key
        }
    }
    return {};
}

IEventHandle* GetRequestAuthAndCheckHandle(const NActors::TActorId& owner, const TString& database, const TString& ticket) {
    return new NActors::IEventHandle(
        NGRpcService::CreateGRpcRequestProxyId(),
        owner,
        new NKikimr::NGRpcService::TEvRequestAuthAndCheck(
            database,
            ticket ? TMaybe<TString>(ticket) : Nothing(),
            owner),
        IEventHandle::FlagTrackDelivery
    );
}

} // namespace

NActors::IEventHandle* SelectAuthorizationScheme(const NActors::TActorId& owner, NMonitoring::IMonHttpRequest& request) {
    TStringBuf ydbSessionId = request.GetCookie("ydb_session_id");
    TStringBuf authorization = request.GetHeader("Authorization");
    if (!authorization.empty()) {
        return GetRequestAuthAndCheckHandle(owner, GetDatabase(request), TString(authorization));
    } else if (!ydbSessionId.empty()) {
        return GetRequestAuthAndCheckHandle(owner, GetDatabase(request), TString("Login ") + TString(ydbSessionId));
    } else {
        return nullptr;
    }
}

NActors::IEventHandle* GetAuthorizeTicketResult(const NActors::TActorId& owner) {
    if (NKikimr::AppData()->EnforceUserTokenRequirement && NKikimr::AppData()->DefaultUserSIDs.empty()) {
        return new NActors::IEventHandle(
            owner,
            owner,
            new NKikimr::NGRpcService::TEvRequestAuthAndCheckResult(
                Ydb::StatusIds::UNAUTHORIZED,
                "No security credentials were provided")
        );
    } else if (!NKikimr::AppData()->DefaultUserSIDs.empty()) {
        TIntrusivePtr<NACLib::TUserToken> token = new NACLib::TUserToken(NKikimr::AppData()->DefaultUserSIDs);
        return new NActors::IEventHandle(
            owner,
            owner,
            new NKikimr::NGRpcService::TEvRequestAuthAndCheckResult(
                {},
                {},
                token
            )
        );
    } else {
        return nullptr;
    }
}

void MakeJsonErrorReply(NJson::TJsonValue& jsonResponse, TString& message, const NYdb::TStatus& status) {
    MakeJsonErrorReply(jsonResponse, message, status.GetIssues(), status.GetStatus());
}

void MakeJsonErrorReply(NJson::TJsonValue& jsonResponse, TString& message, const NYql::TIssues& issues, NYdb::EStatus status) {
    google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> protoIssues;
    NYql::IssuesToMessage(issues, &protoIssues);

    message.clear();

    NJson::TJsonValue& jsonIssues = jsonResponse["issues"];
    for (const auto& queryIssue : protoIssues) {
        NJson::TJsonValue& issue = jsonIssues.AppendValue({});
        NProtobufJson::Proto2Json(queryIssue, issue);
    }

    TString textStatus = TStringBuilder() << status;
    jsonResponse["status"] = textStatus;

    // find first deepest error
    std::stable_sort(protoIssues.begin(), protoIssues.end(), [](const Ydb::Issue::IssueMessage& a, const Ydb::Issue::IssueMessage& b) -> bool {
        return a.severity() < b.severity();
    });

    const google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>* protoIssuesPtr = &protoIssues;
    while (protoIssuesPtr->size() > 0 && protoIssuesPtr->at(0).issuesSize() > 0) {
        protoIssuesPtr = &protoIssuesPtr->at(0).issues();
    }

    if (protoIssuesPtr->size() > 0) {
        const Ydb::Issue::IssueMessage& issue = protoIssuesPtr->at(0);
        NProtobufJson::Proto2Json(issue, jsonResponse["error"]);
        message = issue.message();
    } else {
        jsonResponse["error"]["message"] = textStatus;
    }

    if (message.empty()) {
        message = textStatus;
    }
}

IMonPage* TMon::RegisterActorPage(TIndexMonPage* index, const TString& relPath,
    const TString& title, bool preTag, TActorSystem* actorSystem, const TActorId& actorId, bool useAuth, bool sortPages) {
    return RegisterActorPage({
        .Title = title,
        .RelPath = relPath,
        .ActorSystem = actorSystem,
        .Index = index,
        .PreTag = preTag,
        .ActorId = actorId,
        .UseAuth = useAuth,
        .SortPages = sortPages,
    });
}

NActors::IEventHandle* TMon::DefaultAuthorizer(const NActors::TActorId& owner, NMonitoring::IMonHttpRequest& request) {
    NActors::IEventHandle* eventHandle = SelectAuthorizationScheme(owner, request);
    if (eventHandle != nullptr) {
        return eventHandle;
    }
    return GetAuthorizeTicketResult(owner);
}

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

    static TStringBuf GetPathFromUrl(TStringBuf url) {
        return url.Before('?');
    }

    static TStringBuf GetPathInfoFromUrl(NMonitoring::IMonPage* page, TStringBuf url) {
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
        if (Request->Method == "DELETE") {
            return HTTP_METHOD_DELETE;
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

    bool AcceptsJsonResponse() {
        TStringBuf acceptHeader = GetHeader("Accept");
        return acceptHeader.find(TStringBuf("application/json")) != TStringBuf::npos;
    }

    virtual TStringBuf GetCookie(TStringBuf name) const override {
        NHttp::TCookies cookies(GetHeader("Cookie"));
        return cookies.Get(name);
    }

    virtual TString GetRemoteAddr() const override {
        if (Request->Address) {
            return Request->Address->ToString();
        }
        return {};
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
        : THttpMonRequest(request, *this, index, TString(GetPathInfoFromUrl(index, request->URL)))
    {
    }
};

// handles actor communication
class THttpMonLegacyActorRequest : public TActorBootstrapped<THttpMonLegacyActorRequest> {
public:
    NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr Event;
    THttpMonRequestContainer Container;
    TIntrusivePtr<TActorMonPage> ActorMonPage;

    THttpMonLegacyActorRequest(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, TIntrusivePtr<TActorMonPage> actorMonPage)
        : Event(std::move(event))
        , Container(Event->Get()->Request, actorMonPage.Get())
        , ActorMonPage(actorMonPage)
    {}

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HTTP_MON_LEGACY_ACTOR_REQUEST;
    }

    void Bootstrap() {
        if (Event->Get()->Request->Method == "OPTIONS") {
            return ReplyOptionsAndPassAway();
        }
        Become(&THttpMonLegacyActorRequest::StateFunc);
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
        if (response->Status.StartsWith("2")) {
            TString url(Event->Get()->Request->URL.Before('?'));
            TString status(response->Status);
            NMonitoring::THistogramPtr ResponseTimeHgram = NKikimr::GetServiceCounters(NKikimr::AppData()->Counters,
                    ActorMonPage->MonServiceName)
                ->GetSubgroup("subsystem", "mon")
                ->GetSubgroup("url", url)
                ->GetSubgroup("status", status)
                ->GetHistogram("ResponseTimeMs", NMonitoring::ExponentialHistogram(20, 2, 1));
            ResponseTimeHgram->Collect(Event->Get()->Request->Timer.Passed() * 1000);
        }
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
                    "Access-Control-Allow-Headers: Content-Type,Authorization,Origin,Accept,X-Trace-Verbosity,X-Want-Trace,traceparent\r\n"
                    "Access-Control-Expose-Headers: traceresponse,X-Worker-Name\r\n"
                    "Access-Control-Allow-Methods: OPTIONS,GET,POST,PUT,DELETE\r\n"
                    "Content-Type: " << type << "\r\n"
                    "Connection: keep-alive\r\n\r\n";
        ReplyWith(request->CreateResponseString(response));
        PassAway();
    }

    bool CredentialsProvided() {
        return Container.GetCookie("ydb_session_id") || Container.GetHeader("Authorization");
    }

    TString YdbToHttpError(Ydb::StatusIds::StatusCode status) {
        switch (status) {
        case Ydb::StatusIds::UNAUTHORIZED:
            // YDB status UNAUTHORIZED is used for both access denied case and if no credentials were provided.
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

    void ReplyErrorAndPassAway(const NKikimr::NGRpcService::TEvRequestAuthAndCheckResult& result) {
        ReplyErrorAndPassAway(result.Status, result.Issues, true);
    }

    void ReplyErrorAndPassAway(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues, bool addAccessControlHeaders) {
        NHttp::THttpIncomingRequestPtr request = Event->Get()->Request;
        TStringBuilder response;
        TStringBuilder body;
        TStringBuf contentType;
        const TString httpError = YdbToHttpError(status);

        if (Container.AcceptsJsonResponse()) {
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
        ReplyErrorAndPassAway(Ydb::StatusIds::UNAUTHORIZED, issues, false);
    }

    void SendRequest(const NKikimr::NGRpcService::TEvRequestAuthAndCheckResult* result = nullptr) {
        NHttp::THttpIncomingRequestPtr request = Event->Get()->Request;
        if (ActorMonPage->Authorizer) {
            TString user = (result && result->UserToken) ? result->UserToken->GetUserSID() : "anonymous";
            LOG_NOTICE_S(*TlsActivationContext, NActorsServices::HTTP,
                (request->Address ? request->Address->ToString() : "")
                << " " << user
                << " " << request->Method
                << " " << request->URL);
        }
        TString serializedToken;
        if (result && result->UserToken) {
            serializedToken = result->UserToken->GetSerializedToken();
        }
        Send(ActorMonPage->TargetActorId, new NMon::TEvHttpInfo(
            Container, serializedToken), IEventHandle::FlagTrackDelivery);
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

    void Handle(NKikimr::NGRpcService::TEvRequestAuthAndCheckResult::TPtr& ev) {
        const NKikimr::NGRpcService::TEvRequestAuthAndCheckResult& result(*ev->Get());
        if (result.Status != Ydb::StatusIds::SUCCESS) {
            return ReplyErrorAndPassAway(result);
        }
        bool found = false;
        if (result.UserToken) {
            for (const TString& sid : ActorMonPage->AllowedSIDs) {
                if (result.UserToken->IsExist(sid)) {
                    found = true;
                    break;
                }
            }
        }
        if (found || ActorMonPage->AllowedSIDs.empty() || !result.UserToken) {
            SendRequest(&result);
        } else {
            return ReplyForbiddenAndPassAway("SID is not allowed");
        }
    }

    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvUndelivered, HandleUndelivered);
            hFunc(NMon::IEvHttpInfoRes, HandleResponse);
            hFunc(NKikimr::NGRpcService::TEvRequestAuthAndCheckResult, Handle);
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

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HTTP_MON_LEGACY_INDEX_REQUEST;
    }

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
    THttpMonServiceLegacyActor(TIntrusivePtr<TActorMonPage> actorMonPage)
        : ActorMonPage(std::move(actorMonPage))
    {
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HTTP_MON_LEGACY_ACTOR_SERVICE;
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
            cFunc(TEvents::TSystem::Poison, PassAway);
        }
    }

    TIntrusivePtr<TActorMonPage> ActorMonPage;
};

// receives everyhing not related to actor communcation, converts them to request-actors
class THttpMonServiceLegacyIndex : public TActor<THttpMonServiceLegacyIndex> {
public:
    THttpMonServiceLegacyIndex(TIntrusivePtr<NMonitoring::TIndexMonPage> indexMonPage, const TString& redirectRoot = {})
        : TActor(&THttpMonServiceLegacyIndex::StateWork)
        , IndexMonPage(std::move(indexMonPage))
        , RedirectRoot(redirectRoot)
    {
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HTTP_MON_LEGACY_INDEX_SERVICE;
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev) {
        bool redirect = false;
        if (RedirectRoot && ev->Get()->Request->URL == "/") {
            NHttp::THeaders headers(ev->Get()->Request->Headers);
            if (!headers.Has("Referer")) {
                redirect = true;
            }
        }
        if (redirect) {
            TStringBuilder response;
            response << "HTTP/1.1 302 Found\r\nLocation: " << RedirectRoot << "\r\n\r\n";
            Send(ev->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(ev->Get()->Request->CreateResponseString(response)));
            return;
        } else if (!ev->Get()->Request->URL.ends_with("/") && ev->Get()->Request->URL.find('?') == TStringBuf::npos) {
            TString url(ev->Get()->Request->URL);
            bool index = false;
            auto itPage = IndexPages.find(url);
            if (itPage == IndexPages.end()) {
                auto page = IndexMonPage->FindPageByAbsolutePath(url);
                if (page) {
                    index = page->IsIndex();
                    IndexPages[url] = index;
                }
            } else {
                index = itPage->second;
            }
            if (index) {
                TStringBuilder response;
                auto p = url.rfind('/');
                if (p != TString::npos) {
                    url = url.substr(p + 1);
                }
                url += '/';
                response << "HTTP/1.1 302 Found\r\nLocation: " << url << "\r\n\r\n";
                Send(ev->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(ev->Get()->Request->CreateResponseString(response)));
                return;
            }
        }
        Register(new THttpMonLegacyIndexRequest(std::move(ev), IndexMonPage.Get()));
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
            cFunc(TEvents::TSystem::Poison, PassAway);
        }
    }

    TIntrusivePtr<NMonitoring::TIndexMonPage> IndexMonPage;
    TString RedirectRoot;
    std::unordered_map<TString, bool> IndexPages;
};

inline TActorId MakeNodeProxyId(ui32 node) {
    char x[12] = "nodeproxy";
    return TActorId(node, TStringBuf(x, 12));
}

class THttpMonServiceNodeRequest : public TActorBootstrapped<THttpMonServiceNodeRequest> {
public:
    std::shared_ptr<NHttp::THttpEndpointInfo> Endpoint;
    TEvMon::TEvMonitoringRequest::TPtr Event;
    TActorId HttpProxyActorId;

    THttpMonServiceNodeRequest(std::shared_ptr<NHttp::THttpEndpointInfo> endpoint, TEvMon::TEvMonitoringRequest::TPtr event, TActorId httpProxyActorId)
        : Endpoint(std::move(endpoint))
        , Event(std::move(event))
        , HttpProxyActorId(httpProxyActorId)
    {}

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HTTP_MON_SERVICE_NODE_REQUEST;
    }

    static void FromProto(NHttp::THttpConfig::SocketAddressType& address, const NKikimrMonProto::TSockAddr& proto) {
        switch (proto.GetFamily()) {
            case AF_INET:
                address = std::make_shared<TSockAddrInet>(proto.GetAddress().data(), proto.GetPort());
                break;
            case AF_INET6:
                address = std::make_shared<TSockAddrInet6>(proto.GetAddress().data(), proto.GetPort());
                break;
        }
    }

    TString RewriteWithForwardedFromNode(const TString& response) {
        NHttp::THttpRequestParser parser(response);

        NHttp::THeadersBuilder headers(parser.Headers);
        headers.Set("X-Forwarded-From-Node", TStringBuilder() << Event->Sender.NodeId());

        NHttp::THttpRequestRenderer renderer;
        renderer.InitRequest(parser.Method, parser.URL, parser.Protocol, parser.Version);
        renderer.Set(headers);
        if (parser.HaveBody()) {
            renderer.SetBody(parser.Body); // it shouldn't be here, 30x with a body is a bad idea
        }
        renderer.Finish();
        return renderer.AsString();
    }

    void Bootstrap() {
        NHttp::THttpConfig::SocketAddressType address;
        FromProto(address, Event->Get()->Record.GetAddress());
        NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest(RewriteWithForwardedFromNode(Event->Get()->Record.GetHttpRequest()), Endpoint, address);
        TStringBuilder prefix;
        prefix << "/node/" << TActivationContext::ActorSystem()->NodeId;
        if (request->URL.SkipPrefix(prefix)) {
            Send(HttpProxyActorId, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(std::move(request)));
            Become(&THttpMonServiceNodeRequest::StateWork);
        } else {
            auto response = std::make_unique<TEvMon::TEvMonitoringResponse>();
            auto httpResponse = request->CreateResponseBadRequest();
            response->Record.SetHttpResponse(httpResponse->AsString());
            Send(Event->Sender, response.release(), 0, Event->Cookie);
            PassAway();
        }
    }

    TString RewriteLocationWithNode(const TString& response) {
        NHttp::THttpResponseParser parser(response);

        NHttp::THeadersBuilder headers(parser.Headers);
        headers.Set("Location", TStringBuilder() << "/node/" << TActivationContext::ActorSystem()->NodeId << headers["Location"]);

        NHttp::THttpResponseRenderer renderer;
        renderer.InitResponse(parser.Protocol, parser.Version, parser.Status, parser.Message);
        renderer.Set(headers);
        if (parser.HaveBody()) {
            renderer.SetBody(parser.Body); // it shouldn't be here, 30x with a body is a bad idea
        }
        renderer.Finish();
        return renderer.AsString();
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpOutgoingResponse::TPtr& ev) {
        TString httpResponse = ev->Get()->Response->AsString();
        switch (FromStringWithDefault<int>(ev->Get()->Response->Status)) {
            case 301:
            case 303:
            case 307:
            case 308:
                if (!NHttp::THeaders(ev->Get()->Response->Headers).Get("Location").starts_with("/node/")) {
                    httpResponse = RewriteLocationWithNode(httpResponse);
                }
                break;
        }
        auto response = std::make_unique<TEvMon::TEvMonitoringResponse>();
        response->Record.SetHttpResponse(httpResponse);
        Send(Event->Sender, response.release(), 0, Event->Cookie);
        PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpOutgoingResponse, Handle);
        }
    }
};

class THttpMonServiceMonRequest : public TActorBootstrapped<THttpMonServiceMonRequest> {
public:
    NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr Event;
    ui32 NodeId;

    THttpMonServiceMonRequest(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, ui32 nodeId)
        : Event(std::move(event))
        , NodeId(nodeId)
    {}

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HTTP_MON_SERVICE_MON_REQUEST;
    }

    static void ToProto(NKikimrMonProto::TSockAddr& proto, const NHttp::THttpConfig::SocketAddressType& address) {
        if (address) {
            switch (address->SockAddr()->sa_family) {
                case AF_INET: {
                        proto.SetFamily(AF_INET);
                        sockaddr_in* addr = (sockaddr_in*)address->SockAddr();
                        char ip[INET_ADDRSTRLEN];
                        inet_ntop(AF_INET, (void*)&addr->sin_addr, ip, INET_ADDRSTRLEN);
                        proto.SetAddress(ip);
                        proto.SetPort(htons(addr->sin_port));
                    }
                    break;
                case AF_INET6: {
                        proto.SetFamily(AF_INET6);
                        sockaddr_in6* addr = (sockaddr_in6*)address->SockAddr();
                        char ip6[INET6_ADDRSTRLEN];
                        inet_ntop(AF_INET6, (void*)&addr->sin6_addr, ip6, INET6_ADDRSTRLEN);
                        proto.SetAddress(ip6);
                        proto.SetPort(htons(addr->sin6_port));
                    }
                    break;
            }
        }
    }

    void Bootstrap() {
        TActorId monServiceNodeProxy = MakeNodeProxyId(NodeId);
        auto request = std::make_unique<TEvMon::TEvMonitoringRequest>();
        request->Record.SetHttpRequest(Event->Get()->Request->AsString());
        ToProto(*request->Record.MutableAddress(), Event->Get()->Request->Address);
        Send(monServiceNodeProxy, request.release(), IEventHandle::FlagTrackDelivery);
        Become(&THttpMonServiceMonRequest::StateWork);
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        TString reason;
        switch (ev->Get()->Reason) {
            case TEvents::TEvUndelivered::ReasonUnknown:
                reason = "ReasonUnknown";
                break;
            case TEvents::TEvUndelivered::ReasonActorUnknown:
                reason = "ReasonActorUnknown";
                break;
            case TEvents::TEvUndelivered::Disconnected:
                reason = "Disconnected";
                break;
        }
        Send(Event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Event->Get()->Request->CreateResponseServiceUnavailable(reason)), 0, Event->Cookie);
        PassAway();
    }

    void Handle(TEvMon::TEvMonitoringResponse::TPtr& ev) {
        TString responseTxt = ev->Get()->Record.GetHttpResponse();
        NHttp::THttpOutgoingResponsePtr responseObj = Event->Get()->Request->CreateResponseString(responseTxt);
        if (responseObj->Status == "301" || responseObj->Status == "302") {
            NHttp::THttpResponseParser parser(responseTxt);
            NHttp::THeadersBuilder headers(parser.Headers);
            if (headers["Location"].starts_with('/')) {
                NHttp::THttpOutgoingResponsePtr response = new NHttp::THttpOutgoingResponse(Event->Get()->Request);
                response->InitResponse(parser.Protocol, parser.Version, parser.Status, parser.Message);

                headers.Set("Location", TStringBuilder() << "/node/" << NodeId << headers["Location"]);

                response->Set(headers);
                if (parser.HaveBody()) {
                    response->SetBody(parser.Body);
                }
                responseObj = response;
            }
        }

        Send(Event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(responseObj.Release()), 0, Event->Cookie);
        PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvMon::TEvMonitoringResponse, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
        }
    }
};

// receives requests to another nodes
class THttpMonServiceNodeProxy : public TActor<THttpMonServiceNodeProxy> {
public:
    THttpMonServiceNodeProxy(TActorId httpProxyActorId)
        : TActor(&THttpMonServiceNodeProxy::StateWork)
        , HttpProxyActorId(httpProxyActorId)
        , Endpoint(std::make_shared<NHttp::THttpEndpointInfo>())
    {
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HTTP_MON_SERVICE_NODE_PROXY;
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev) {
        TStringBuf url = ev->Get()->Request->URL;
        TStringBuf node;
        ui32 nodeId;
        if (url.SkipPrefix("/node/") && url.NextTok('/', node) && TryFromStringWithDefault<ui32>(node, nodeId)) {
            Register(new THttpMonServiceMonRequest(std::move(ev), nodeId));
            return;
        }
        Send(ev->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(ev->Get()->Request->CreateResponseBadRequest("bad request")), 0, ev->Cookie);
    }

    void Handle(TEvMon::TEvMonitoringRequest::TPtr& ev) {
        Register(new THttpMonServiceNodeRequest(Endpoint, ev, HttpProxyActorId));
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
            hFunc(TEvMon::TEvMonitoringRequest, Handle);
            cFunc(TEvents::TSystem::Poison, PassAway);
        }
    }

protected:
    TActorId HttpProxyActorId;
    std::shared_ptr<NHttp::THttpEndpointInfo> Endpoint;
};

// initializes http and waits for the result
class THttpMonInitializator : public TActorBootstrapped<THttpMonInitializator> {
public:
    THttpMonInitializator(TActorId httpProxyActorId, std::unique_ptr<NHttp::TEvHttpProxy::TEvAddListeningPort> config, std::promise<void> promise)
        : HttpProxyActorId(httpProxyActorId)
        , Config(std::move(config))
        , Promise(std::move(promise))
    {
    }

    void Bootstrap() {
        Send(HttpProxyActorId, Config.release());
        Become(&THttpMonInitializator::StateWork);
    }

    void Handle(NHttp::TEvHttpProxy::TEvConfirmListen::TPtr& ev) {
        Promise.set_value();
        PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvConfirmListen, Handle);
        }
    }

protected:
    TActorId HttpProxyActorId;
    std::unique_ptr<NHttp::TEvHttpProxy::TEvAddListeningPort> Config;
    std::promise<void> Promise;
};

TMon::TMon(TConfig config)
    : Config(std::move(config))
    , IndexMonPage(new NMonitoring::TIndexMonPage("", Config.Title))
{
}

std::future<void> TMon::Start(TActorSystem* actorSystem) {
    Y_ABORT_UNLESS(actorSystem);
    TGuard<TMutex> g(Mutex);
    ActorSystem = actorSystem;
    Register(new TIndexRedirectMonPage(IndexMonPage));
    Register(new NMonitoring::TVersionMonPage);
    Register(new NMonitoring::TBootstrapCssMonPage);
    Register(new NMonitoring::TTablesorterCssMonPage);
    Register(new NMonitoring::TBootstrapJsMonPage);
    Register(new NMonitoring::TJQueryJsMonPage);
    Register(new NMonitoring::TTablesorterJsMonPage);
    Register(new NMonitoring::TBootstrapFontsEotMonPage);
    Register(new NMonitoring::TBootstrapFontsSvgMonPage);
    Register(new NMonitoring::TBootstrapFontsTtfMonPage);
    Register(new NMonitoring::TBootstrapFontsWoffMonPage);
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
    auto nodeProxyActorId = ActorSystem->Register(
        new THttpMonServiceNodeProxy(HttpProxyActorId),
        TMailboxType::ReadAsFilled,
        ActorSystem->AppData<NKikimr::TAppData>()->UserPoolId);
    NodeProxyServiceActorId = MakeNodeProxyId(ActorSystem->NodeId);
    ActorSystem->RegisterLocalService(NodeProxyServiceActorId, nodeProxyActorId);

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
        "text/javascript",
        "application/javascript",
        "application/json",
        "application/yaml",
    };
    addPort->SslCertificatePem = Config.Certificate;
    addPort->Secure = !Config.Certificate.empty();
    addPort->MaxRequestsPerSecond = Config.MaxRequestsPerSecond;

    std::promise<void> promise;
    std::future<void> future = promise.get_future();
    ActorSystem->Register(new THttpMonInitializator(HttpProxyActorId, std::move(addPort), std::move(promise)));
    ActorSystem->Send(HttpProxyActorId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/", HttpMonServiceActorId));
    ActorSystem->Send(HttpProxyActorId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/node", NodeProxyServiceActorId));
    for (auto& pageInfo : ActorMonPages) {
        if (pageInfo.Page) {
            RegisterActorMonPage(pageInfo);
        } else if (pageInfo.Handler) {
            ActorSystem->Send(HttpProxyActorId, new NHttp::TEvHttpProxy::TEvRegisterHandler(pageInfo.Path, pageInfo.Handler));
        }
    }
    ActorMonPages.clear();
    return future;
}

void TMon::Stop() {
    IndexMonPage->ClearPages(); // it's required to avoid loop-reference
    if (ActorSystem) {
        TGuard<TMutex> g(Mutex);
        for (const auto& [path, actorId] : ActorServices) {
            ActorSystem->Send(actorId, new TEvents::TEvPoisonPill);
        }
        ActorSystem->Send(NodeProxyServiceActorId, new TEvents::TEvPoisonPill);
        ActorSystem->Send(HttpMonServiceActorId, new TEvents::TEvPoisonPill);
        ActorSystem->Send(HttpProxyActorId, new TEvents::TEvPoisonPill);
        ActorSystem = nullptr;
    }
}

void TMon::Register(NMonitoring::IMonPage* page) {
    IndexMonPage->Register(page);
}

NMonitoring::TIndexMonPage* TMon::RegisterIndexPage(const TString& path, const TString& title) {
    auto page = IndexMonPage->RegisterIndexPage(path, title);
    IndexMonPage->SortPages();
    return page;
}

void TMon::RegisterActorMonPage(const TActorMonPageInfo& pageInfo) {
    if (ActorSystem) {
        TActorMonPage* actorMonPage = static_cast<TActorMonPage*>(pageInfo.Page.Get());
        auto& actorId = ActorServices[pageInfo.Path];
        if (actorId) {
            ActorSystem->Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, {}, nullptr, 0));
        }
        actorId = ActorSystem->Register(
            new THttpMonServiceLegacyActor(actorMonPage),
            TMailboxType::ReadAsFilled,
            ActorSystem->AppData<NKikimr::TAppData>()->UserPoolId);
        ActorSystem->Send(HttpProxyActorId, new NHttp::TEvHttpProxy::TEvRegisterHandler(pageInfo.Path, actorId));
    }
}

NMonitoring::IMonPage* TMon::RegisterActorPage(TRegisterActorPageFields fields) {
    TGuard<TMutex> g(Mutex);
    NMonitoring::TMonPagePtr page = new TActorMonPage(
        fields.RelPath,
        fields.Title,
        Config.Host,
        fields.PreTag,
        fields.ActorSystem,
        fields.ActorId,
        fields.AllowedSIDs ? fields.AllowedSIDs : Config.AllowedSIDs,
        fields.UseAuth ? Config.Authorizer : TRequestAuthorizer(),
        fields.MonServiceName);
    if (fields.Index) {
        fields.Index->Register(page);
        if (fields.SortPages) {
            fields.Index->SortPages();
        }
    } else {
        Register(page.Get());
    }

    TActorMonPageInfo pageInfo = {
        .Page = page,
        .Path = GetPageFullPath(page.Get()),
    };

    if (ActorSystem && HttpProxyActorId) {
        RegisterActorMonPage(pageInfo);
    } else {
        ActorMonPages.emplace_back(pageInfo);
    }

    return page.Get();
}

NMonitoring::IMonPage* TMon::RegisterCountersPage(const TString& path, const TString& title, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters) {
    TDynamicCountersPage* page = new TDynamicCountersPage(path, title, counters);
        page->SetUnknownGroupPolicy(EUnknownGroupPolicy::Ignore);
        Register(page);
        return page;
}

void TMon::RegisterHandler(const TString& path, const TActorId& handler) {
    if (ActorSystem) {
        ActorSystem->Send(HttpProxyActorId, new NHttp::TEvHttpProxy::TEvRegisterHandler(path, handler));
    } else {
        TGuard<TMutex> g(Mutex);
        ActorMonPages.emplace_back(TActorMonPageInfo{
            .Handler = handler,
            .Path = path,
        });
    }
}

NMonitoring::IMonPage* TMon::FindPage(const TString& relPath) {
    return IndexMonPage->FindPage(relPath);
}

}

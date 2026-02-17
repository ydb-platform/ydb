#include "mon.h"
#include "mon_impl.h"
#include "events_internal.h"
#include "counters_adapter_impl.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/auth.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/monitoring_provider.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/mon/audit/audit.h>
#include <ydb/core/protos/mon.pb.h>
#include <ydb/core/util/wildcard.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/probes.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/public/sdk/cpp/adapters/issue/issue.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/lwtrace/all.h>
#include <library/cpp/lwtrace/mon/mon_lwtrace.h>
#include <library/cpp/monlib/service/pages/version_mon_page.h>
#include <library/cpp/monlib/service/pages/mon_page.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/page.h>

#include <util/system/hostname.h>

namespace NActors {

namespace {

using namespace NKikimr;
using namespace NMonitoring::NPrivate;

bool HasJsonContent(NHttp::THttpIncomingRequest* request) {
    if (request->Method == "POST") {
        const TStringBuf header = request->ContentType.Before(';');
        return header.empty() || AsciiEqualsIgnoreCase(header, "application/json"); // by default we will try to parse json, no error will be generated if parsing fails
    }
    return false;
}

TString GetDatabase(NHttp::THttpIncomingRequest* request) {
    NHttp::TUrlParameters urlParams(request->URL);
    TString database = urlParams["database"];
    if (database) {
        return database;
    }
    if (HasJsonContent(request)) {
        NJson::TJsonValue requestData;
        if (NJson::ReadJsonTree(request->Body, &requestData)) {
            return requestData["database"].GetString(); // empty if not string or no such key
        }
    }
    return {};
}

IEventHandle* GetRequestAuthAndCheckHandle(const NActors::TActorId& owner, const TString& database, const TString& ticket, TString peerName) {
    return new NActors::IEventHandle(
        NGRpcService::CreateGRpcRequestProxyId(),
        owner,
        new NKikimr::NGRpcService::TEvRequestAuthAndCheck(
            database,
            ticket ? TMaybe<TString>(ticket) : Nothing(),
            owner,
            NGRpcService::TAuditMode::Modifying(NGRpcService::TAuditMode::TLogClassConfig::ClusterAdmin),
            std::move(peerName)),
        IEventHandle::FlagTrackDelivery
    );
}

} // namespace

NActors::IEventHandle* SelectAuthorizationScheme(const NActors::TActorId& owner, NHttp::THttpIncomingRequest* request) {
    NHttp::THeaders headers(request->Headers);
    NHttp::TCookies cookies(headers["Cookie"]);
    TStringBuf ydbSessionId = cookies["ydb_session_id"];
    TStringBuf authorization = headers["Authorization"];
    if (!authorization.empty()) {
        return GetRequestAuthAndCheckHandle(owner, GetDatabase(request), TString(authorization), NMonitoring::NAudit::ExtractRemoteAddress(request));
    } else if (!ydbSessionId.empty()) {
        return GetRequestAuthAndCheckHandle(owner, GetDatabase(request), TString("Login ") + TString(ydbSessionId), NMonitoring::NAudit::ExtractRemoteAddress(request));
    } else if (!request->MTlsClientCertificate.empty()) {
        return GetRequestAuthAndCheckHandle(owner, GetDatabase(request), request->MTlsClientCertificate, NMonitoring::NAudit::ExtractRemoteAddress(request));
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
                "No security credentials were provided",
                {})
        );
    } else if (!NKikimr::AppData()->DefaultUserSIDs.empty()) {
        TIntrusivePtr<NACLib::TUserToken> token = new NACLib::TUserToken(NKikimr::AppData()->DefaultUserSIDs);
        return new NActors::IEventHandle(
            owner,
            owner,
            new NKikimr::NGRpcService::TEvRequestAuthAndCheckResult(
                {},
                {},
                token,
                {}
            )
        );
    } else {
        return nullptr;
    }
}

void MakeJsonErrorReply(NJson::TJsonValue& jsonResponse, TString& message, const NYdb::TStatus& status) {
    MakeJsonErrorReply(jsonResponse, message, NYdb::NAdapters::ToYqlIssues(status.GetIssues()), status.GetStatus());
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

TVector<TString> TMon::GetCountersAllowedSIDs(const NKikimr::TAppData* appData) {
    TVector<TString> countersAllowedSIDs;
    if (!appData) {
        return countersAllowedSIDs;
    }
    const auto& securityConfig = appData->DomainsConfig.GetSecurityConfig();
    const auto addSIDs = [&countersAllowedSIDs](const auto& allowedSIDs) {
        for (const auto& sid : allowedSIDs) {
            countersAllowedSIDs.emplace_back(sid);
        }
    };
    addSIDs(securityConfig.GetViewerAllowedSIDs());
    addSIDs(securityConfig.GetMonitoringAllowedSIDs());
    addSIDs(securityConfig.GetAdministrationAllowedSIDs());
    return countersAllowedSIDs;
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
        .AuthMode = useAuth ? TMon::EAuthMode::Enforce : TMon::EAuthMode::Disabled,
        .SortPages = sortPages,
    });
}

NActors::IEventHandle* TMon::DefaultAuthorizer(const NActors::TActorId& owner, NHttp::THttpIncomingRequest* request) {
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
    NMonitoring::NAudit::TAuditCtx AuditCtx;

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
        AuditCtx.InitAudit(Event);
        Become(&THttpMonLegacyActorRequest::StateFunc);
        if (ActorMonPage->Authorizer) {
            NActors::IEventHandle* handle = ActorMonPage->Authorizer(SelfId(), Event->Get()->Request.Get());
            if (handle) {
                TActivationContext::Send(handle);
                return;
            }
        }
        AuditCtx.LogOnReceived();
        SendRequest();
    }

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
        return Container.GetCookie("ydb_session_id") ||
            Container.GetHeader("Authorization") ||
            !Event->Get()->Request->MTlsClientCertificate.empty();
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
        ReplyErrorAndPassAway(Ydb::StatusIds::UNAUTHORIZED, issues, true);
    }

    void SendRequest(const NKikimr::NGRpcService::TEvRequestAuthAndCheckResult* result = nullptr) {
        NHttp::THttpIncomingRequestPtr request = Event->Get()->Request;
        if (ActorMonPage->Authorizer) {
            TString user = (result && result->UserToken) ? result->UserToken->GetUserSID() : "anonymous";
            ALOG_NOTICE(NActorsServices::HTTP,
                (request->Address ? request->Address->ToString() : "")
                << " " << user
                << " " << request->Method
                << " " << request->URL);
        }
        TString serializedToken = result && result->UserToken ? result->UserToken->GetSerializedToken() : TString();
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
        AuditCtx.AddAuditLogParts(result.AuditLogParts);
        AuditCtx.LogOnReceived();
        if (result.UserToken) {
            AuditCtx.SetSubjectType(result.UserToken->GetSubjectType());
            Event->Get()->UserToken = result.UserToken->GetSerializedToken();
        }
        if (ActorMonPage->AuthMode == TMon::EAuthMode::ExtractOnly) {
            // Extract token but don't enforce authorization - let the handler decide
            SendRequest(&result);
            return;
        }
        if (result.Status != Ydb::StatusIds::SUCCESS) {
            return ReplyErrorAndPassAway(result);
        }
        if (IsTokenAllowed(result.UserToken.Get(), ActorMonPage->AllowedSIDs)) {
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

inline TActorId MakeNodeProxyId(ui32 node) {
    char x[12] = "nodeproxy";
    return TActorId(node, TStringBuf(x, 12));
}

class THttpMonServiceNodeRequest : public TActorBootstrapped<THttpMonServiceNodeRequest> {
public:
    using TBase = TActorBootstrapped<THttpMonServiceNodeRequest>;
    std::shared_ptr<NHttp::THttpEndpointInfo> Endpoint;
    TEvMon::TEvMonitoringRequest::TPtr Event;
    TActorId OwnerActorId;
    TString Address;
    TActorId HttpProxyActorId;
    NHttp::TEvHttpProxy::TEvSubscribeForCancel::TPtr CancelSubscriber;

    THttpMonServiceNodeRequest(std::shared_ptr<NHttp::THttpEndpointInfo> endpoint, TEvMon::TEvMonitoringRequest::TPtr event, TActorId ownerActorId, const TString& address, TActorId httpProxyActorId)
        : Endpoint(std::move(endpoint))
        , Event(std::move(event))
        , OwnerActorId(ownerActorId)
        , Address(address)
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
        if (parser.HasBody()) {
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
        if (parser.HasBody()) {
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
        if (ev->Get()->Response->IsDone()) {
            return PassAway();
        }
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk::TPtr& ev) {
        auto response = std::make_unique<TEvMon::TEvMonitoringResponse>();
        bool endOfData = false;
        if (ev->Get()->Error) {
            response->Record.SetError(ev->Get()->Error);
            endOfData = true;
        } else if (ev->Get()->DataChunk) {
            response->Record.SetDataChunk(ev->Get()->DataChunk->AsString());
            if (ev->Get()->DataChunk->IsEndOfData()) {
                response->Record.SetEndOfData(true);
                endOfData = true;
            }
        }
        Send(Event->Sender, response.release(), 0, Event->Cookie);
        if (endOfData) {
            return PassAway();
        }
    }

    void Handle(NHttp::TEvHttpProxy::TEvSubscribeForCancel::TPtr& ev) {
        CancelSubscriber = std::move(ev);
    }

    void Handle(NHttp::TEvHttpProxy::TEvRequestCancelled::TPtr& /* ev */) {
        if (CancelSubscriber) {
            Send(CancelSubscriber->Sender, new NHttp::TEvHttpProxy::TEvRequestCancelled(), 0, CancelSubscriber->Cookie);
        }
        PassAway();
    }

    void PassAway() {
        Send(OwnerActorId, new TEvMon::TEvCleanupProxy(Address));
        TBase::PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpOutgoingResponse, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvSubscribeForCancel, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvRequestCancelled, Handle);
        }
    }
};

class THttpMonServiceMonRequest : public TActorBootstrapped<THttpMonServiceMonRequest> {
public:
    using TBase = TActorBootstrapped<THttpMonServiceMonRequest>;

    NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr Event;
    ui32 NodeId;
    NHttp::THttpOutgoingResponsePtr CurrentResponse;

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
        Send(Event->Sender, new NHttp::TEvHttpProxy::TEvSubscribeForCancel(), IEventHandle::FlagTrackDelivery);
        TActorId monServiceNodeProxy = MakeNodeProxyId(NodeId);
        auto request = std::make_unique<TEvMon::TEvMonitoringRequest>();
        request->Record.SetHttpRequest(Event->Get()->Request->AsString());
        ToProto(*request->Record.MutableAddress(), Event->Get()->Request->Address);
        Send(monServiceNodeProxy, request.release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
        Become(&THttpMonServiceMonRequest::StateWork);
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        if (ev->Get()->SourceType == NHttp::TEvHttpProxy::EvSubscribeForCancel) {
            return Cancelled();
        }
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
        if (ev->Get()->Record.HasHttpResponse()) {
            TString responseTxt = ev->Get()->Record.GetHttpResponse();
            NHttp::THttpOutgoingResponsePtr responseObj = Event->Get()->Request->CreateResponseString(responseTxt);

            if (responseObj->Status == "301" || responseObj->Status == "302") {
                NHttp::THttpResponseParser parser(responseTxt);
                NHttp::THeadersBuilder headers(parser.Headers);
                auto location(headers["Location"]);
                if (location.starts_with('/') && !location.starts_with("/node/")) {
                    NHttp::THttpOutgoingResponsePtr response = new NHttp::THttpOutgoingResponse(Event->Get()->Request);
                    response->InitResponse(parser.Protocol, parser.Version, parser.Status, parser.Message);
                    headers.Set("Location", TStringBuilder() << "/node/" << NodeId << location);
                    response->Set(headers);
                    if (parser.HasBody()) {
                        response->SetBody(parser.Body);
                    }
                    responseObj = response;
                }
            }

            Send(Event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(responseObj), 0, Event->Cookie);

            if (responseObj->IsDone()) {
                return PassAway();
            } else {
                CurrentResponse = responseObj;
            }
        } else if (ev->Get()->Record.HasDataChunk() && CurrentResponse) {
            auto dataChunk = CurrentResponse->CreateIncompleteDataChunk();
            auto dataChunkContent = ev->Get()->Record.GetDataChunk();
            dataChunk->Assign(dataChunkContent.data(), dataChunkContent.size());
            bool endOfData = false;
            try {
                dataChunk->DataSize = std::stoul(TString(TStringBuf(dataChunkContent).Before('\r')).c_str(), nullptr, 16);
            } catch (const std::exception&) {
                dataChunk->DataSize = 0;
                endOfData = true;
                dataChunk->EndOfData = true;
            }
            if (ev->Get()->Record.GetEndOfData()) {
                endOfData = true;
                dataChunk->EndOfData = true;
            }
            Send(Event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(dataChunk), 0, Event->Cookie);
            if (endOfData) {
                return PassAway();
            }
        } else if (ev->Get()->Record.HasError() && CurrentResponse) {
            Send(Event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(ev->Get()->Record.GetError()), 0, Event->Cookie);
            return PassAway();
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& /* ev */) {
        if (CurrentResponse) {
            Send(Event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk("NodeDisconnected"), 0, Event->Cookie);
        } else {
            Send(Event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Event->Get()->Request->CreateResponseServiceUnavailable("NodeDisconnected")), 0, Event->Cookie);
        }
        PassAway();
    }

    void Cancelled() {
        auto request = std::make_unique<TEvMon::TEvMonitoringCancelRequest>();
        ToProto(*request->Record.MutableAddress(), Event->Get()->Request->Address);
        Send(MakeNodeProxyId(NodeId), request.release());
        PassAway();
    }

    void PassAway() override {
        Send(TActivationContext::InterconnectProxy(NodeId), new TEvents::TEvUnsubscribe());
        TBase::PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvMon::TEvMonitoringResponse, Handle);
            cFunc(NHttp::TEvHttpProxy::EvRequestCancelled, Cancelled);
            hFunc(TEvents::TEvUndelivered, Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
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

    static TString GetAddress(const NKikimrMonProto::TSockAddr& addr) {
        return TStringBuilder() << addr.GetFamily() << "://" << addr.GetAddress() << ":" << addr.GetPort();
    }

    void Handle(TEvMon::TEvMonitoringRequest::TPtr& ev) {
        auto address = GetAddress(ev->Get()->Record.GetAddress());
        auto actorId = Register(new THttpMonServiceNodeRequest(Endpoint, ev, SelfId(), address, HttpProxyActorId));
        RequestProxies[address] = actorId;
    }

    void Handle(TEvMon::TEvMonitoringCancelRequest::TPtr& ev) {
        auto address = GetAddress(ev->Get()->Record.GetAddress());
        auto itProxy = RequestProxies.find(address);
        if (itProxy != RequestProxies.end()) {
            Send(itProxy->second, new NHttp::TEvHttpProxy::TEvRequestCancelled());
        }
    }

    void Handle(TEvMon::TEvCleanupProxy::TPtr& ev) {
        RequestProxies.erase(ev->Get()->Address);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
            hFunc(TEvMon::TEvMonitoringRequest, Handle);
            hFunc(TEvMon::TEvMonitoringCancelRequest, Handle);
            hFunc(TEvMon::TEvCleanupProxy, Handle);
            cFunc(TEvents::TSystem::Poison, PassAway);
        }
    }

protected:
    TActorId HttpProxyActorId;
    std::shared_ptr<NHttp::THttpEndpointInfo> Endpoint;
    std::unordered_map<TString, TActorId> RequestProxies;
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

    void Handle(NHttp::TEvHttpProxy::TEvConfirmListen::TPtr& /* ev */) {
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

class THttpMonAuthorizedActorRequest : public TActorBootstrapped<THttpMonAuthorizedActorRequest> {
public:
    NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr Event;
    TMon::TRegisterHandlerFields Fields;
    TMon::TRequestAuthorizer Authorizer;
    NMonitoring::NAudit::TAuditCtx AuditCtx;
    NHttp::TEvHttpProxy::TEvSubscribeForCancel::TPtr CancelSubscriber;

    THttpMonAuthorizedActorRequest(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const TMon::TRegisterHandlerFields& fields, TMon::TRequestAuthorizer authorizer)
        : Event(std::move(event))
        , Fields(fields)
        , Authorizer(std::move(authorizer))
    {}

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HTTP_MON_AUTHORIZED_ACTOR_REQUEST;
    }

    void Bootstrap() {
        AuditCtx.InitAudit(Event);
        Send(Event->Sender, new NHttp::TEvHttpProxy::TEvSubscribeForCancel(), IEventHandle::FlagTrackDelivery);
        if (Fields.AuthMode != TMon::EAuthMode::Disabled && Authorizer) {
            NActors::IEventHandle* handle = Authorizer(SelfId(), Event->Get()->Request.Get());
            if (handle) {
                Send(handle);
                Become(&THttpMonAuthorizedActorRequest::StateWork);
                return;
            }
        }
        AuditCtx.LogOnReceived();
        SendRequest();
        Become(&THttpMonAuthorizedActorRequest::StateWork);
    }

    void ReplyWith(NHttp::THttpOutgoingResponsePtr response) {
        AuditCtx.LogOnCompleted(response);
        Send(Event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    bool CredentialsProvided() {
        NHttp::THttpIncomingRequestPtr request = Event->Get()->Request;
        NHttp::THeaders headers(request->Headers);
        if (headers.Has("Authorization")) {
            return true;
        }
        NHttp::TCookies cookies(headers["Cookie"]);
        if (cookies.Has("ydb_session_id")) {
            return true;
        }
        if (!request->MTlsClientCertificate.empty()) {
            return true;
        }
        return false;
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

    bool AcceptsJson() const {
        NHttp::THttpIncomingRequestPtr request = Event->Get()->Request;
        TStringBuf acceptHeader = NHttp::THeaders(request->Headers)["Accept"];
        return acceptHeader.find("application/json") != TStringBuf::npos;
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
        if (Authorizer) {
            TString user = (result && result->UserToken) ? result->UserToken->GetUserSID() : "anonymous";
            ALOG_NOTICE(NActorsServices::HTTP, (request->Address ? request->Address->ToString() : "")
                << " " << user
                << " " << request->Method
                << " " << request->URL);
        }
        Send(new IEventHandle(Fields.Handler, SelfId(), Event->ReleaseBase().Release(), IEventHandle::FlagTrackDelivery, Event->Cookie));
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
        ReplyWith(request->CreateResponseServiceUnavailable(
            TStringBuilder() << "Actor is not available"));
        PassAway();
    }

    void Handle(NKikimr::NGRpcService::TEvRequestAuthAndCheckResult::TPtr& ev) {
        const NKikimr::NGRpcService::TEvRequestAuthAndCheckResult& result(*ev->Get());
        AuditCtx.AddAuditLogParts(result.AuditLogParts);
        AuditCtx.LogOnReceived();
        if (result.UserToken) {
            AuditCtx.SetSubjectType(result.UserToken->GetSubjectType());
            Event->Get()->UserToken = result.UserToken->GetSerializedToken();
        }
        if (Fields.AuthMode == TMon::EAuthMode::ExtractOnly) {
            // Extract token but don't enforce authorization - let the handler decide
            SendRequest(&result);
            return;
        }
        if (result.Status != Ydb::StatusIds::SUCCESS) {
            return ReplyErrorAndPassAway(result);
        }
        if (IsTokenAllowed(result.UserToken.Get(), Fields.AllowedSIDs)) {
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
};

class THttpMonAuthorizedPageRequest : public TActorBootstrapped<THttpMonAuthorizedPageRequest> {
public:
    NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr Event;
    THttpMonRequestContainer Container;
    TVector<TString> AllowedSIDs;
    TMon::TRequestAuthorizer Authorizer;
    NMonitoring::NAudit::TAuditCtx AuditCtx;
    bool NeedAudit;
    TMon::EAuthMode AuthMode;

    THttpMonAuthorizedPageRequest(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, NMonitoring::IMonPage* page,
        TVector<TString> allowedSIDs, TMon::TRequestAuthorizer authorizer, bool needAudit = true,
        TMon::EAuthMode authMode = TMon::EAuthMode::Enforce
    )
        : Event(std::move(event))
        , Container(Event->Get()->Request, page)
        , AllowedSIDs(std::move(allowedSIDs))
        , Authorizer(std::move(authorizer))
        , NeedAudit(needAudit)
        , AuthMode(authMode)
    {}

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HTTP_MON_LEGACY_INDEX_REQUEST;
    }

    void Bootstrap() {
        AuditCtx.InitAudit(Event, NeedAudit);
        if (Authorizer) {
            NActors::IEventHandle* handle = Authorizer(SelfId(), Event->Get()->Request.Get());
            if (handle) {
                Send(handle);
                Become(&THttpMonAuthorizedPageRequest::StateWork);
                return;
            }
        }
        AuditCtx.LogOnReceived();
        ProcessRequest();
    }

    void ReplyWith(NHttp::THttpOutgoingResponsePtr response) {
        AuditCtx.LogOnCompleted(response);
        Send(Event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    bool CredentialsProvided() {
        NHttp::THttpIncomingRequestPtr request = Event->Get()->Request;
        NHttp::THeaders headers(request->Headers);
        if (headers.Has("Authorization")) {
            return true;
        }
        NHttp::TCookies cookies(headers["Cookie"]);
        if (cookies.Has("ydb_session_id")) {
            return true;
        }
        if (!request->MTlsClientCertificate.empty()) {
            return true;
        }
        return false;
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

        contentType = "text/html";
        body << "<html><body><h1>" << httpError << "</h1>";
        if (issues) {
            body << "<p>" << issues.ToString() << "</p>";
        }
        body << "</body></html>";

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
        ReplyErrorAndPassAway(Ydb::StatusIds::UNAUTHORIZED, issues, true);
    }

    void ProcessRequest() {
        Container.Page->Output(Container);
        NHttp::THttpOutgoingResponsePtr response = Event->Get()->Request->CreateResponseString(Container.Str());
        AuditCtx.LogOnCompleted(response);
        Send(Event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        PassAway();
    }

    void HandleUndelivered(TEvents::TEvUndelivered::TPtr&) {
        NHttp::THttpIncomingRequestPtr request = Event->Get()->Request;
        ReplyWith(request->CreateResponseServiceUnavailable("Actor is not available"));
        PassAway();
    }

    void Handle(NKikimr::NGRpcService::TEvRequestAuthAndCheckResult::TPtr& ev) {
        const NKikimr::NGRpcService::TEvRequestAuthAndCheckResult& result(*ev->Get());
        AuditCtx.AddAuditLogParts(result.AuditLogParts);
        AuditCtx.LogOnReceived();
        if (result.UserToken) {
            AuditCtx.SetSubjectType(result.UserToken->GetSubjectType());
            Event->Get()->UserToken = result.UserToken->GetSerializedToken();
        }
        if (AuthMode == TMon::EAuthMode::ExtractOnly) {
            // Extract token but don't enforce authorization - let the handler decide
            ProcessRequest();
            return;
        }
        if (result.Status != Ydb::StatusIds::SUCCESS) {
            return ReplyErrorAndPassAway(result);
        }
        if (IsTokenAllowed(result.UserToken.Get(), AllowedSIDs)) {
            ProcessRequest();
        } else {
            return ReplyForbiddenAndPassAway("SID is not allowed");
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvUndelivered, HandleUndelivered);
            hFunc(NKikimr::NGRpcService::TEvRequestAuthAndCheckResult, Handle);
        }
    }
};

// receives everyhing not related to actor communcation, converts them to request-actors
class THttpMonPageService : public TActor<THttpMonPageService> {
    TActorId HttpProxyActorId;
    TIntrusivePtr<NMonitoring::IMonPage> Page;
    TMon::TRequestAuthorizer Authorizer;
    TVector<TString> AllowedSIDs;
    TMon::EAuthMode AuthMode;

public:
THttpMonPageService(const TActorId& httpProxyActorId, TIntrusivePtr<NMonitoring::IMonPage> page,
    TMon::TRequestAuthorizer authorizer = {}, TVector<TString> allowedSIDs = {},
    TMon::EAuthMode authMode = TMon::EAuthMode::Enforce
)
        : TActor(&THttpMonPageService::StateWork)
        , HttpProxyActorId(httpProxyActorId)
        , Page(std::move(page))
        , Authorizer(std::move(authorizer))
        , AllowedSIDs(std::move(allowedSIDs))
        , AuthMode(authMode)
    {
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HTTP_MON_INDEX_SERVICE;
    }

    void ReplyWithOptions(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev) {
        NHttp::THttpIncomingRequestPtr request = ev->Get()->Request;
        TString url(request->URL.Before('?'));
        TString type = mimetypeByExt(url.data());
        if (type.empty()) {
            type = "application/json";
        }
        TString origin = TString(NHttp::THeaders(request->Headers)["Origin"]);
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
        Send(ev->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(request->CreateResponseString(response)));
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev) {
        if (ev->Get()->Request->Method == "OPTIONS") {
            return ReplyWithOptions(ev);
        }
        Register(new THttpMonAuthorizedPageRequest(
            std::move(ev), Page.Get(), AllowedSIDs, Authorizer, /* needAudit */ true, AuthMode)
        );
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
            cFunc(TEvents::TSystem::Poison, PassAway);
        }
    }
};

// receives everyhing not related to actor communcation, converts them to request-actors
class THttpMonIndexService : public TActor<THttpMonIndexService> {
public:
    THttpMonIndexService(const TActorId& httpProxyActorId, TIntrusivePtr<NMonitoring::TIndexMonPage> indexMonPage,
                         TVector<TString> allowedSIDs, TMon::TRequestAuthorizer authorizer, const TString& redirectRoot = {}, bool needMonLegacyAudit = true)
        : TActor(&THttpMonIndexService::StateWork)
        , HttpProxyActorId(httpProxyActorId)
        , IndexMonPage(std::move(indexMonPage))
        , AllowedSIDs(std::move(allowedSIDs))
        , Authorizer(std::move(authorizer))
        , RedirectRoot(redirectRoot)
        , NeedMonLegacyAudit(needMonLegacyAudit)
    {
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HTTP_MON_INDEX_SERVICE;
    }

    void ReplyWithOptions(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev) {
        NHttp::THttpIncomingRequestPtr request = ev->Get()->Request;
        TString url(request->URL.Before('?'));
        TString type = mimetypeByExt(url.data());
        if (type.empty()) {
            type = "application/json";
        }
        TString origin = TString(NHttp::THeaders(request->Headers)["Origin"]);
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
        Send(ev->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(request->CreateResponseString(response)));
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev) {
        if (ev->Get()->Request->Method == "OPTIONS") {
            return ReplyWithOptions(ev);
        }
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
        }

        if (!ev->Get()->Request->URL.ends_with("/") && ev->Get()->Request->URL.find('?') == TStringBuf::npos) {
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

        TStringBuf url = ev->Get()->Request->URL.Before('?');
        while (!url.empty()) {
            auto it = Handlers.find(TString(url));
            if (it != Handlers.end()) {
                Register(new THttpMonAuthorizedActorRequest(std::move(ev), it->second, Authorizer));
                return;
            } else {
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
        }

        Register(new THttpMonAuthorizedPageRequest(std::move(ev), IndexMonPage.Get(), AllowedSIDs, Authorizer, NeedMonLegacyAudit));
    }

    void Handle(TEvMon::TEvRegisterHandler::TPtr& ev) {
        Handlers[ev->Get()->Fields.Path] = ev->Get()->Fields;
        Send(HttpProxyActorId, new NHttp::TEvHttpProxy::TEvRegisterHandler(ev->Get()->Fields.Path, SelfId()));
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
            hFunc(TEvMon::TEvRegisterHandler, Handle);
            cFunc(TEvents::TSystem::Poison, PassAway);
        }
    }

    TActorId HttpProxyActorId;
    TIntrusivePtr<NMonitoring::TIndexMonPage> IndexMonPage;
    std::unordered_map<TString, bool> IndexPages;
    std::unordered_map<TString, TMon::TRegisterHandlerFields> Handlers;
    TVector<TString> AllowedSIDs;
    TMon::TRequestAuthorizer Authorizer;
    TString RedirectRoot;
    bool NeedMonLegacyAudit;
};

class THttpMonPingService : public TActor<THttpMonPingService> {
public:
    THttpMonPingService()
        : TActor(&THttpMonPingService::StateWork)
    {
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev) {
        Send(ev->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(ev->Get()->Request->CreateResponseOK("ok /ping", "text/plain")));
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
            cFunc(TEvents::TSystem::Poison, PassAway);
        }
    }
};

TMon::TMon(TConfig config)
    : Config(std::move(config))
    , IndexMonPage(new NMonitoring::TIndexMonPage("", Config.Title))
{
}

TMon::~TMon() {
    IndexMonPage->ClearPages(); // it's required to avoid loop-reference
}

void TMon::RegisterLwtrace() {
    NLwTraceMonPage::RegisterPages(IndexMonPage.Get());
    NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(ACTORLIB_PROVIDER));
    NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(MONITORING_PROVIDER));

    TVector<TString> monitoringAllowedSIDs;
    NKikimr::TAppData* appData = ActorSystem->AppData<NKikimr::TAppData>();
    if (appData) {
        {
            const auto& protoAllowedSIDs = appData->DomainsConfig.GetSecurityConfig().GetMonitoringAllowedSIDs();
            for (const auto& sid : protoAllowedSIDs) {
                monitoringAllowedSIDs.emplace_back(sid);
            }
        }
        {
            const auto& protoAllowedSIDs = appData->DomainsConfig.GetSecurityConfig().GetAdministrationAllowedSIDs();
            for (const auto& sid : protoAllowedSIDs) {
                monitoringAllowedSIDs.emplace_back(sid);
            }
        }
    }

    RegisterActorHandler({
        .Path = "/trace",
        .Handler = HttpAuthMonServiceActorId,
        .AuthMode = TMon::EAuthMode::Enforce,
        .AllowedSIDs = monitoringAllowedSIDs,
    });
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
    if (ActorSystem->AppData<NKikimr::TAppData>()) {
        auto metricsRoot = NKikimr::GetServiceCounters(ActorSystem->AppData<NKikimr::TAppData>()->Counters, "utils")->GetSubgroup("subsystem", "mon");
        Metrics = std::make_shared<TMetricFactoryForDynamicCounters>(std::move(metricsRoot));
    } else {
        Metrics = NMonitoring::TMetricRegistry::SharedInstance();
    }
    ui32 executorPool = ActorSystem->AppData<NKikimr::TAppData>() ? ActorSystem->AppData<NKikimr::TAppData>()->UserPoolId : 0;
    HttpProxyActorId = ActorSystem->Register(
        NHttp::CreateHttpProxy(Metrics),
        TMailboxType::ReadAsFilled,
        executorPool);
    HttpMonServiceActorId = ActorSystem->Register(
        new THttpMonIndexService(HttpProxyActorId, IndexMonPage, Config.AllowedSIDs, Config.Authorizer, Config.RedirectMainPageTo),
        TMailboxType::ReadAsFilled,
        executorPool);
    HttpAuthMonServiceActorId = ActorSystem->Register(
        new THttpMonIndexService(HttpMonServiceActorId, IndexMonPage, Config.AllowedSIDs, Config.Authorizer, Config.RedirectMainPageTo, false),
        TMailboxType::ReadAsFilled,
        executorPool);
    RegisterLwtrace();
    auto nodeProxyActorId = ActorSystem->Register(
        new THttpMonServiceNodeProxy(HttpProxyActorId),
        TMailboxType::ReadAsFilled,
        executorPool);
    NodeProxyServiceActorId = MakeNodeProxyId(ActorSystem->NodeId);
    ActorSystem->RegisterLocalService(NodeProxyServiceActorId, nodeProxyActorId);
    IActor* countersMonPageServiceActor;
    if (Config.RequireCountersAuthentication) {
        countersMonPageServiceActor = new THttpMonPageService(
            HttpProxyActorId,
            CountersMonPage,
            Config.Authorizer,
            GetCountersAllowedSIDs(ActorSystem->AppData<NKikimr::TAppData>()));
    } else {
        countersMonPageServiceActor = new THttpMonPageService(
            HttpProxyActorId,
            CountersMonPage);
    }
    CountersServiceActorId = ActorSystem->Register(countersMonPageServiceActor, TMailboxType::ReadAsFilled, executorPool);
    ActorSystem->RegisterLocalService(CountersServiceActorId, CountersServiceActorId);
    PingServiceActorId = ActorSystem->Register(new THttpMonPingService(), TMailboxType::ReadAsFilled, executorPool);

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
    addPort->CertificateFile = Config.CertificateFile;
    addPort->PrivateKeyFile = Config.PrivateKeyFile;
    addPort->CaFile = Config.CaFile;
    addPort->Secure = !Config.Certificate.empty() || !Config.CertificateFile.empty();
    addPort->MaxRequestsPerSecond = Config.MaxRequestsPerSecond;

    std::promise<void> promise;
    std::future<void> future = promise.get_future();
    ActorSystem->Register(new THttpMonInitializator(HttpProxyActorId, std::move(addPort), std::move(promise)));
    ActorSystem->Send(HttpProxyActorId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/", HttpMonServiceActorId));
    ActorSystem->Send(HttpProxyActorId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/node", NodeProxyServiceActorId));
    if (CountersMonPage) {
        ActorSystem->Send(HttpProxyActorId, new NHttp::TEvHttpProxy::TEvRegisterHandler(TStringBuilder() << "/" << CountersMonPage->GetPath(), CountersServiceActorId));
    }
    ActorSystem->Send(HttpProxyActorId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/ping", PingServiceActorId));
    for (auto& pageInfo : ActorMonPages) {
        if (pageInfo.Page) {
            RegisterActorMonPage(pageInfo);
        } else if (pageInfo.Handler) {
            ActorSystem->Send(HttpMonServiceActorId, new TEvMon::TEvRegisterHandler(pageInfo.Handler.value()));
        }
    }
    ActorMonPages.clear();
    return future;
}

void TMon::Stop() {
    TGuard<TMutex> g(Mutex);
    if (ActorSystem) {
        for (const auto& [path, actorId] : ActorServices) {
            ActorSystem->Send(actorId, new TEvents::TEvPoisonPill);
        }
        ActorSystem->Send(CountersServiceActorId, new TEvents::TEvPoisonPill);
        ActorSystem->Send(NodeProxyServiceActorId, new TEvents::TEvPoisonPill);
        ActorSystem->Send(HttpMonServiceActorId, new TEvents::TEvPoisonPill);
        ActorSystem->Send(HttpAuthMonServiceActorId, new TEvents::TEvPoisonPill);
        ActorSystem->Send(HttpProxyActorId, new TEvents::TEvPoisonPill);
        ActorSystem->Send(PingServiceActorId, new TEvents::TEvPoisonPill);
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
        fields.AuthMode != TMon::EAuthMode::Disabled ? Config.Authorizer : TRequestAuthorizer(),
        fields.AuthMode,
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
    CountersMonPage = new TDynamicCountersPage(path, title, counters);
    CountersMonPage->SetUnknownGroupPolicy(EUnknownGroupPolicy::Ignore);
    Register(CountersMonPage.Get());
    return CountersMonPage.Get();
}

void TMon::RegisterActorHandler(const TRegisterHandlerFields& fields) {
    TGuard<TMutex> g(Mutex);
    if (ActorSystem) {
        ActorSystem->Send(HttpMonServiceActorId, new TEvMon::TEvRegisterHandler(fields));
    } else {
        ActorMonPages.emplace_back(TActorMonPageInfo{
            .Handler = fields,
        });
    }
}

NMonitoring::IMonPage* TMon::FindPage(const TString& relPath) {
    return IndexMonPage->FindPage(relPath);
}

}

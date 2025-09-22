#include <ydb/core/mon/mon.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/testlib/test_client.h>

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <library/cpp/http/misc/httpcodes.h>
#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>
#include <util/string/builder.h>

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::Tests;

namespace {

constexpr TStringBuf TEST_MON_PATH = "test_mon";
constexpr TStringBuf TEST_RESPONSE = "Test actor";
constexpr TStringBuf AUTHORIZATION_HEADER = "Authorization";
constexpr TStringBuf VALID_TOKEN = "Bearer token";

class TTestActorPage : public NActors::TActorBootstrapped<TTestActorPage> {
public:
    void Bootstrap() {
        Become(&TTestActorPage::StateWork);
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev) {
        TStringBuilder body;
        body << "<html><body><p>" << TEST_RESPONSE << "</p></body></html>";
        Send(ev->Sender, new NMon::TEvHttpInfoRes(body));
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NMon::TEvHttpInfo, Handle);
        }
    }
};

class TTestActorHandler : public NActors::TActorBootstrapped<TTestActorHandler> {
public:
    void Bootstrap() {
        Become(&TTestActorHandler::StateWork);
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev) {
        TStringBuilder body;
        body << "<html><body><p>" << TEST_RESPONSE << "</p></body></html>";

        TStringBuilder response;
        response << "HTTP/1.1 200 OK\r\n"
                 << "Content-Type: text/html\r\n"
                 << "Content-Length: " << body.size() << "\r\n"
                 << "Connection: Close\r\n\r\n"
                 << body;

        Send(ev->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(
            ev->Get()->Request->CreateResponseString(response)));
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

class TTestMonPage : public NMonitoring::IMonPage {
public:
    TTestMonPage()
        : NMonitoring::IMonPage(TString(TEST_MON_PATH), TString("Test Page"))
    {
    }

    void Output(NMonitoring::IMonHttpRequest& request) override {
        const TStringBuf pathInfo = request.GetPathInfo();
        if (!pathInfo.empty() && pathInfo != TStringBuf("/")) {
            request.Output() << NMonitoring::HTTPNOTFOUND;
            return;
        }

        auto& out = request.Output();
        out << NMonitoring::HTTPOKHTML;
        out << "<html><body><p>" << TEST_RESPONSE << "</p></body></html>";
    }
};

struct TFakeTicketParserActor : public NActors::TActor<TFakeTicketParserActor> {
    static TVector<TString> GetDefaultGroups() {
        return {TString("group_name")};
    }

    TFakeTicketParserActor(TVector<TString> groupSIDs = GetDefaultGroups())
        : NActors::TActor<TFakeTicketParserActor>(&TFakeTicketParserActor::StateFunc)
        , GroupSIDs(std::move(groupSIDs))
    {}

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTicketParser::TEvAuthorizeTicket, Handle);
            default:
                break;
        }
    }

    void Handle(TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
        LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::TICKET_PARSER, "Ticket parser: got TEvAuthorizeTicket event: " << ev->Get()->Ticket << " " << ev->Get()->Database << " " << ev->Get()->Entries.size());
        ++AuthorizeTicketRequests;

        if (ev->Get()->Ticket != VALID_TOKEN) {
            Fail(ev, TStringBuilder() << "Incorrect token " << ev->Get()->Ticket);
            return;
        }

        Success(ev);
    }

    void Fail(TEvTicketParser::TEvAuthorizeTicket::TPtr& ev, const TString& message) {
        ++AuthorizeTicketFails;
        TEvTicketParser::TError err;
        err.Retryable = false;
        err.Message = message ? message : "Test error";
        LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::TICKET_PARSER,
            "Send TEvAuthorizeTicketResult: " << err.Message);
        Send(ev->Sender, new TEvTicketParser::TEvAuthorizeTicketResult(ev->Get()->Ticket, err));
    }

    void Success(TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
        ++AuthorizeTicketSuccesses;
        NACLib::TUserToken::TUserTokenInitFields args;
        args.UserSID = "username";
        args.GroupSIDs = GroupSIDs;
        TIntrusivePtr<NACLib::TUserToken> userToken = MakeIntrusive<NACLib::TUserToken>(args);
        LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::TICKET_PARSER,
            "Send TEvAuthorizeTicketResult success");
        Send(ev->Sender, new TEvTicketParser::TEvAuthorizeTicketResult(ev->Get()->Ticket, userToken));
    }

    size_t AuthorizeTicketRequests = 0;
    size_t AuthorizeTicketSuccesses = 0;
    size_t AuthorizeTicketFails = 0;
    TVector<TString> GroupSIDs;
};

void GrantConnect(Tests::TClient& client) {
    client.CreateUser("/Root", "username", "password");
    client.GrantConnect("username");

    const auto alterAttrsStatus = client.AlterUserAttributes("/", "Root", {
        { "folder_id", "test_folder_id" },
        { "database_id", "test_database_id" },
    });
    UNIT_ASSERT_EQUAL(alterAttrsStatus, NMsgBusProxy::MSTATUS_OK);
}

struct THttpMonTestEnvOptions {
    enum class ERegKind {
        None,
        ActorPage,
        ActorHandler,
        MonPage,
    };

    ERegKind RegKind = ERegKind::None;
    TVector<TString> ActorAllowedSIDs;
    TVector<TString> TicketParserGroupSIDs = TFakeTicketParserActor::GetDefaultGroups();
};

class THttpMonTestEnv {
public:
    THttpMonTestEnv(const THttpMonTestEnvOptions& options = {})
        : PortManager()
        , Port(PortManager.GetPort(2134))
        , GrpcPort(PortManager.GetPort(2135))
        , MonPort(PortManager.GetPort(8765))
        , Settings(Port)
        , Options(std::move(options))
    {
        Settings.InitKikimrRunConfig()
            .SetNodeCount(1)
            .SetUseRealThreads(true)
            .SetDomainName("Root")
            .SetUseSectorMap(true)
            .SetMonitoringPortOffset(MonPort, true);

        auto& securityConfig = *Settings.AppConfig->MutableDomainsConfig()->MutableSecurityConfig();
        securityConfig.SetEnforceUserTokenCheckRequirement(true);
        securityConfig.MutableMonitoringAllowedSIDs()->Add("ydb.clusters.monitor@as");

        Settings.CreateTicketParser = [&](const TTicketParserSettings&) -> NActors::IActor* {
            auto* actor = new TFakeTicketParserActor(Options.TicketParserGroupSIDs);
            TicketParser = actor;
            return actor;
        };

        Server = std::make_unique<TServer>(Settings);
        Server->EnableGRpc(GrpcPort);
        Client = std::make_unique<TClient>(Settings);
        Client->InitRootScheme();
        GrantConnect(*Client);

        Runtime = Server->GetRuntime();

        NActors::TMon* mon = Runtime->GetAppData().Mon;
        UNIT_ASSERT(mon != nullptr);

        switch (Options.RegKind) {
            case THttpMonTestEnvOptions::ERegKind::None:
                break;
            case THttpMonTestEnvOptions::ERegKind::ActorPage: {
                TestActorPage = new TTestActorPage();
                TestActorId = Runtime->Register(TestActorPage);
                mon->RegisterActorPage({
                    .RelPath = TString(TEST_MON_PATH),
                    .ActorSystem = Runtime->GetActorSystem(0),
                    .ActorId = TestActorId,
                    .UseAuth = true,
                    .AllowedSIDs = Options.ActorAllowedSIDs,
                });
                break;
            }
            case THttpMonTestEnvOptions::ERegKind::ActorHandler: {
                TestActorHandler = new TTestActorHandler();
                TestActorId = Runtime->Register(TestActorHandler);
                mon->RegisterActorHandler({
                    .Path = "/" + TString(TEST_MON_PATH),
                    .Handler = TestActorId,
                    .UseAuth = true,
                    .AllowedSIDs = Options.ActorAllowedSIDs,
                });
                break;
            }
            case THttpMonTestEnvOptions::ERegKind::MonPage: {
                mon->Register(new TTestMonPage());
                break;
            }
        }

        HttpClient = std::make_unique<TKeepAliveHttpClient>("localhost", MonPort);
    }

    TKeepAliveHttpClient::THeaders MakeAuthHeaders(const TString& token = TString(VALID_TOKEN)) const {
        TKeepAliveHttpClient::THeaders headers;
        headers[TString(AUTHORIZATION_HEADER)] = token;
        return headers;
    }

    TString MakeUrl(TStringBuf path = {}) const {
        TStringBuilder url;
        if (path.Empty()) {
            path = TEST_MON_PATH;
        }
        if (!path.StartsWith('/')) {
            url << '/';
        }
        url << path;
        return url;
    }

    TFakeTicketParserActor* GetTicketParser() const {
        return TicketParser;
    }

    TKeepAliveHttpClient& GetHttpClient() const {
        return *HttpClient;
    }

private:
    TPortManager PortManager;
    ui16 Port;
    ui16 GrpcPort;
    ui16 MonPort;
    TServerSettings Settings;
    std::unique_ptr<TServer> Server;
    std::unique_ptr<TClient> Client;
    TTestActorRuntime* Runtime = nullptr;
    TTestActorPage* TestActorPage = nullptr;
    TTestActorHandler* TestActorHandler = nullptr;
    NActors::TActorId TestActorId;
    TFakeTicketParserActor* TicketParser = nullptr;
    std::unique_ptr<TKeepAliveHttpClient> HttpClient;
    THttpMonTestEnvOptions Options;
};

} // namespace

Y_UNIT_TEST_SUITE(ActorPage) {
    Y_UNIT_TEST(HttpOk) {
        THttpMonTestEnv env({
            .RegKind = THttpMonTestEnvOptions::ERegKind::ActorPage,
            .ActorAllowedSIDs = {"valid_group"},
            .TicketParserGroupSIDs = {"valid_group"},
        });

        TStringStream responseStream;
        const auto status = env.GetHttpClient().DoGet(env.MakeUrl(), &responseStream, env.MakeAuthHeaders());
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_OK);

        const TString response = responseStream.ReadAll();
        UNIT_ASSERT_C(response.Contains(TEST_RESPONSE), response);

        TFakeTicketParserActor* ticketParser = env.GetTicketParser();
        UNIT_ASSERT(ticketParser);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketRequests, 1);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketSuccesses, 1);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketFails, 0);
    }

    Y_UNIT_TEST(NoValidGroupForbidden) {
        THttpMonTestEnv env({
            .RegKind = THttpMonTestEnvOptions::ERegKind::ActorPage,
            .ActorAllowedSIDs = {"valid_group"},
            .TicketParserGroupSIDs = {"wrong_group"},
        });

        TStringStream responseStream;
        const auto status = env.GetHttpClient().DoGet(env.MakeUrl(), &responseStream, env.MakeAuthHeaders());
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_FORBIDDEN);

        TFakeTicketParserActor* ticketParser = env.GetTicketParser();
        UNIT_ASSERT(ticketParser);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketRequests, 1);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketSuccesses, 1);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketFails, 0);
    }

    Y_UNIT_TEST(InvalidTokenForbidden) {
        THttpMonTestEnv env({
            .RegKind = THttpMonTestEnvOptions::ERegKind::ActorPage,
        });

        TStringStream responseStream;
        const TString invalidToken = TString("Bearer invalid");
        const auto status = env.GetHttpClient().DoGet(env.MakeUrl(), &responseStream, env.MakeAuthHeaders(invalidToken));
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_FORBIDDEN);

        TFakeTicketParserActor* ticketParser = env.GetTicketParser();
        UNIT_ASSERT(ticketParser);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketRequests, 1);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketSuccesses, 0);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketFails, 1);
    }

    Y_UNIT_TEST(OptionsNoContent) {
        THttpMonTestEnv env({
            .RegKind = THttpMonTestEnvOptions::ERegKind::ActorPage,
        });

        TStringStream responseStream;
        const auto status = env.GetHttpClient().DoRequest("OPTIONS", env.MakeUrl(), "", &responseStream);
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_NO_CONTENT);
    }
}

Y_UNIT_TEST_SUITE(ActorHandler) {
    Y_UNIT_TEST(HttpOk) {
        THttpMonTestEnv env({
            .RegKind = THttpMonTestEnvOptions::ERegKind::ActorHandler,
            .ActorAllowedSIDs = {"valid_group"},
            .TicketParserGroupSIDs = {"valid_group"},
        });

        TStringStream responseStream;
        const auto status = env.GetHttpClient().DoGet(env.MakeUrl(), &responseStream, env.MakeAuthHeaders());
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_OK);

        const TString response = responseStream.ReadAll();
        UNIT_ASSERT_C(response.Contains(TEST_RESPONSE), response);

        TFakeTicketParserActor* ticketParser = env.GetTicketParser();
        UNIT_ASSERT(ticketParser);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketRequests, 1);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketSuccesses, 1);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketFails, 0);
    }

    Y_UNIT_TEST(NoValidGroupForbidden) {
        THttpMonTestEnv env({
            .RegKind = THttpMonTestEnvOptions::ERegKind::ActorHandler,
            .ActorAllowedSIDs = {"valid_group"},
            .TicketParserGroupSIDs = {"wrong_group"},
        });

        TStringStream responseStream;
        const auto status = env.GetHttpClient().DoGet(env.MakeUrl(), &responseStream, env.MakeAuthHeaders());
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_FORBIDDEN);

        TFakeTicketParserActor* ticketParser = env.GetTicketParser();
        UNIT_ASSERT(ticketParser);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketRequests, 1);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketSuccesses, 1);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketFails, 0);
    }

    Y_UNIT_TEST(InvalidTokenForbidden) {
        THttpMonTestEnv env({
            .RegKind = THttpMonTestEnvOptions::ERegKind::ActorHandler,
        });

        TStringStream responseStream;
        const TString invalidToken = TString("Bearer invalid");
        const auto status = env.GetHttpClient().DoGet(env.MakeUrl(), &responseStream, env.MakeAuthHeaders(invalidToken));
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_FORBIDDEN);

        TFakeTicketParserActor* ticketParser = env.GetTicketParser();
        UNIT_ASSERT(ticketParser);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketRequests, 1);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketSuccesses, 0);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketFails, 1);
    }

    Y_UNIT_TEST(OptionsNoContent) {
        THttpMonTestEnv env({
            .RegKind = THttpMonTestEnvOptions::ERegKind::ActorHandler,
        });

        TStringStream responseStream;
        const auto status = env.GetHttpClient().DoRequest("OPTIONS", env.MakeUrl(), "", &responseStream);
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_NO_CONTENT);
    }
}

Y_UNIT_TEST_SUITE(MonPage) {
    Y_UNIT_TEST(HttpOk) {
        THttpMonTestEnv env({
            .RegKind = THttpMonTestEnvOptions::ERegKind::MonPage,
        });

        TStringStream responseStream;
        const auto status = env.GetHttpClient().DoGet(env.MakeUrl(), &responseStream, env.MakeAuthHeaders());
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_OK);

        const TString response = responseStream.ReadAll();
        UNIT_ASSERT_C(response.Contains(TEST_RESPONSE), response);

        TFakeTicketParserActor* ticketParser = env.GetTicketParser();
        UNIT_ASSERT(ticketParser);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketRequests, 0);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketSuccesses, 0);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketFails, 0);
    }

    Y_UNIT_TEST(OptionsNoContent) {
        THttpMonTestEnv env({
            .RegKind = THttpMonTestEnvOptions::ERegKind::MonPage,
        });

        TStringStream responseStream;
        const auto status = env.GetHttpClient().DoRequest("OPTIONS", env.MakeUrl(), "", &responseStream);
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_NO_CONTENT);
    }
}

Y_UNIT_TEST_SUITE(Other) {
    Y_UNIT_TEST(UnknownPathNotFound) {
        THttpMonTestEnv env;

        TStringStream responseStream;
        const auto url = env.MakeUrl("/wrong_path");
        const auto status = env.GetHttpClient().DoGet(url, &responseStream, env.MakeAuthHeaders());
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_NOT_FOUND);
    }
}

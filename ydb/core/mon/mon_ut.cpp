#include <ydb/core/mon/mon.h>
#include <ydb/core/mon/ut_utils/ut_utils.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/testlib/test_client.h>

#include <library/cpp/http/misc/httpcodes.h>
#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NMonitoring::NTests {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::Tests;

void GrantConnect(Tests::TClient& client) {
    client.CreateUser("/Root", "username", "password");
    client.GrantConnect("username");

    const auto alterAttrsStatus = client.AlterUserAttributes("/", "Root", {
        { "folder_id", "test_folder_id" },
        { "database_id", "test_database_id" },
    });
    UNIT_ASSERT_EQUAL(alterAttrsStatus, NMsgBusProxy::MSTATUS_OK);
}

void AssertCorsHeaders(const THttpHeaders& headers) {
    UNIT_ASSERT(headers.HasHeader("Access-Control-Allow-Origin"));
    UNIT_ASSERT(headers.HasHeader("Access-Control-Allow-Credentials"));
    UNIT_ASSERT(headers.HasHeader("Access-Control-Allow-Headers"));
    UNIT_ASSERT(headers.HasHeader("Access-Control-Allow-Methods"));
    UNIT_ASSERT_VALUES_EQUAL(headers.FindHeader("Access-Control-Allow-Credentials")->Value(), "true");
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
    TVector<TString> TicketParserGroupSIDs = DEFAULT_TICKET_PARSER_GROUPS;
    TMon::EAuthMode AuthMode = TMon::EAuthMode::Enforce;
};

class THttpMonTestEnv {
public:
    THttpMonTestEnv(const THttpMonTestEnvOptions& options = {})
        : Port(PortManager.GetPort(2134))
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
            return TicketParser = new TFakeTicketParserActor(Options.TicketParserGroupSIDs);
        };

        Server = std::make_unique<TServer>(Settings);
        Server->EnableGRpc(GrpcPort);
        Client = std::make_unique<TClient>(Settings);
        Client->InitRootScheme();
        GrantConnect(*Client);

        Runtime = Server->GetRuntime();

        TMon* mon = Runtime->GetAppData().Mon;
        UNIT_ASSERT(mon != nullptr);

        switch (Options.RegKind) {
            case THttpMonTestEnvOptions::ERegKind::None:
                break;
            case THttpMonTestEnvOptions::ERegKind::ActorPage: {
                TestActorPage = new TTestActorPage();
                TestActorId = Runtime->Register(TestActorPage);
                mon->RegisterActorPage({
                    .RelPath = TEST_MON_PATH,
                    .ActorSystem = Runtime->GetActorSystem(0),
                    .ActorId = TestActorId,
                    .AuthMode = Options.AuthMode,
                    .AllowedSIDs = Options.ActorAllowedSIDs,
                });
                break;
            }
            case THttpMonTestEnvOptions::ERegKind::ActorHandler: {
                TestActorHandler = new TTestActorHandler();
                TestActorId = Runtime->Register(TestActorHandler);
                mon->RegisterActorHandler({
                    .Path = MakeDefaultUrl(),
                    .Handler = TestActorId,
                    .AuthMode = Options.AuthMode,
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

    TKeepAliveHttpClient::THeaders MakeAuthHeaders(const TString& token = VALID_TOKEN) const {
        TKeepAliveHttpClient::THeaders headers;
        headers[AUTHORIZATION_HEADER] = token;
        return headers;
    }

    TString MakeDefaultUrl() const {
        TStringBuilder url;
        url << "/" << TEST_MON_PATH;
        return url;
    }

    TFakeTicketParserActor* GetTicketParser() const {
        UNIT_ASSERT(TicketParser);
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
    TActorId TestActorId;
    TFakeTicketParserActor* TicketParser = nullptr;
    std::unique_ptr<TKeepAliveHttpClient> HttpClient;
    THttpMonTestEnvOptions Options;
};

Y_UNIT_TEST_SUITE(ActorPage) {
    Y_UNIT_TEST(HttpOk) {
        THttpMonTestEnv env({
            .RegKind = THttpMonTestEnvOptions::ERegKind::ActorPage,
            .ActorAllowedSIDs = {"valid_group"},
            .TicketParserGroupSIDs = {"valid_group"},
        });

        TStringStream responseStream;
        const auto status = env.GetHttpClient().DoGet(env.MakeDefaultUrl(), &responseStream, env.MakeAuthHeaders());
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_OK);

        const TString response = responseStream.ReadAll();
        UNIT_ASSERT_STRING_CONTAINS(response, TEST_RESPONSE);

        TFakeTicketParserActor* ticketParser = env.GetTicketParser();
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
        THttpHeaders outHeaders;
        const auto status = env.GetHttpClient().DoGet(env.MakeDefaultUrl(), &responseStream, env.MakeAuthHeaders(), &outHeaders);
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_FORBIDDEN);

        AssertCorsHeaders(outHeaders);

        TFakeTicketParserActor* ticketParser = env.GetTicketParser();
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketRequests, 1);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketSuccesses, 1);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketFails, 0);
    }

    Y_UNIT_TEST(InvalidTokenForbidden) {
        THttpMonTestEnv env({
            .RegKind = THttpMonTestEnvOptions::ERegKind::ActorPage,
        });

        TStringStream responseStream;
        THttpHeaders outHeaders;
        const TString invalidToken = TString("Bearer invalid");
        const auto status = env.GetHttpClient().DoGet(env.MakeDefaultUrl(), &responseStream, env.MakeAuthHeaders(invalidToken), &outHeaders);
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_FORBIDDEN);

        AssertCorsHeaders(outHeaders);

        TFakeTicketParserActor* ticketParser = env.GetTicketParser();
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketRequests, 1);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketSuccesses, 0);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketFails, 1);
    }

    Y_UNIT_TEST(NoUseAuthOk) {
        THttpMonTestEnv env({
            .RegKind = THttpMonTestEnvOptions::ERegKind::ActorPage,
            .AuthMode = TMon::EAuthMode::Disabled,
        });

        TStringStream responseStream;
        const TString invalidToken = TString("Bearer invalid");
        const auto status = env.GetHttpClient().DoGet(env.MakeDefaultUrl(), &responseStream, env.MakeAuthHeaders(invalidToken));
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_OK);

        TFakeTicketParserActor* ticketParser = env.GetTicketParser();
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketRequests, 0);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketSuccesses, 0);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketFails, 0);
    }

    Y_UNIT_TEST(OptionsNoContent) {
        THttpMonTestEnv env({
            .RegKind = THttpMonTestEnvOptions::ERegKind::ActorPage,
        });

        TStringStream responseStream;
        THttpHeaders outHeaders;
        const auto status = env.GetHttpClient().DoRequest("OPTIONS", env.MakeDefaultUrl(), "", &responseStream, TKeepAliveHttpClient::THeaders(), &outHeaders);
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_NO_CONTENT);

        AssertCorsHeaders(outHeaders);
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
        const auto status = env.GetHttpClient().DoGet(env.MakeDefaultUrl(), &responseStream, env.MakeAuthHeaders());
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_OK);

        const TString response = responseStream.ReadAll();
        UNIT_ASSERT_STRING_CONTAINS(response, TEST_RESPONSE);

        TFakeTicketParserActor* ticketParser = env.GetTicketParser();
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
        THttpHeaders outHeaders;
        const auto status = env.GetHttpClient().DoGet(env.MakeDefaultUrl(), &responseStream, env.MakeAuthHeaders(), &outHeaders);
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_FORBIDDEN);

        AssertCorsHeaders(outHeaders);

        TFakeTicketParserActor* ticketParser = env.GetTicketParser();
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketRequests, 1);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketSuccesses, 1);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketFails, 0);
    }

    Y_UNIT_TEST(InvalidTokenForbidden) {
        THttpMonTestEnv env({
            .RegKind = THttpMonTestEnvOptions::ERegKind::ActorHandler,
        });

        TStringStream responseStream;
        THttpHeaders outHeaders;
        const TString invalidToken = TString("Bearer invalid");
        const auto status = env.GetHttpClient().DoGet(env.MakeDefaultUrl(), &responseStream, env.MakeAuthHeaders(invalidToken), &outHeaders);
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_FORBIDDEN);

        AssertCorsHeaders(outHeaders);

        TFakeTicketParserActor* ticketParser = env.GetTicketParser();
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketRequests, 1);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketSuccesses, 0);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketFails, 1);
    }

    Y_UNIT_TEST(NoUseAuthOk) {
        THttpMonTestEnv env({
            .RegKind = THttpMonTestEnvOptions::ERegKind::ActorHandler,
            .AuthMode = TMon::EAuthMode::Disabled,
        });

        TStringStream responseStream;
        const TString invalidToken = TString("Bearer invalid");
        const auto status = env.GetHttpClient().DoGet(env.MakeDefaultUrl(), &responseStream, env.MakeAuthHeaders(invalidToken));
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_OK);

        TFakeTicketParserActor* ticketParser = env.GetTicketParser();
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketRequests, 0);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketSuccesses, 0);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketFails, 0);
    }

    Y_UNIT_TEST(OptionsNoContent) {
        THttpMonTestEnv env({
            .RegKind = THttpMonTestEnvOptions::ERegKind::ActorHandler,
        });

        TStringStream responseStream;
        THttpHeaders outHeaders;
        const auto status = env.GetHttpClient().DoRequest("OPTIONS", env.MakeDefaultUrl(), "", &responseStream, TKeepAliveHttpClient::THeaders(), &outHeaders);
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_NO_CONTENT);

        AssertCorsHeaders(outHeaders);

        TFakeTicketParserActor* ticketParser = env.GetTicketParser();
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketRequests, 0);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketSuccesses, 0);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketFails, 0);
    }
}

Y_UNIT_TEST_SUITE(MonPage) {
    Y_UNIT_TEST(HttpOk) {
        THttpMonTestEnv env({
            .RegKind = THttpMonTestEnvOptions::ERegKind::MonPage,
        });

        TStringStream responseStream;
        const auto status = env.GetHttpClient().DoGet(env.MakeDefaultUrl(), &responseStream, env.MakeAuthHeaders());
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_OK);

        const TString response = responseStream.ReadAll();
        UNIT_ASSERT_STRING_CONTAINS(response, TEST_RESPONSE);

        TFakeTicketParserActor* ticketParser = env.GetTicketParser();
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketRequests, 1);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketSuccesses, 1);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketFails, 0);
    }

    Y_UNIT_TEST(OptionsNoContent) {
        THttpMonTestEnv env({
            .RegKind = THttpMonTestEnvOptions::ERegKind::MonPage,
        });

        TStringStream responseStream;
        THttpHeaders outHeaders;
        const auto status = env.GetHttpClient().DoRequest("OPTIONS", env.MakeDefaultUrl(), "", &responseStream, TKeepAliveHttpClient::THeaders(), &outHeaders);
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_NO_CONTENT);

        AssertCorsHeaders(outHeaders);

        TFakeTicketParserActor* ticketParser = env.GetTicketParser();
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketRequests, 0);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketSuccesses, 0);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketFails, 0);
    }
}

Y_UNIT_TEST_SUITE(Other) {
    Y_UNIT_TEST(UnknownPathNotFound) {
        THttpMonTestEnv env;

        TStringStream responseStream;
        const auto status = env.GetHttpClient().DoGet("/wrong_path", &responseStream, env.MakeAuthHeaders());
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_NOT_FOUND);
        // NOTE: no CORS check, because 404 response is generated by monlib
    }

    Y_UNIT_TEST(TraceHttpOk) {
        THttpMonTestEnv env({
            .TicketParserGroupSIDs = {"ydb.clusters.monitor@as"},
        });

        TStringStream responseStream;
        const auto status = env.GetHttpClient().DoGet("/trace", &responseStream, env.MakeAuthHeaders());
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_OK);

        TFakeTicketParserActor* ticketParser = env.GetTicketParser();
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketRequests, 2);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketSuccesses, 2);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketFails, 0);
    }

    Y_UNIT_TEST(TraceNoValidGroupForbidden) {
        THttpMonTestEnv env;

        TStringStream responseStream;
        THttpHeaders outHeaders;
        const auto status = env.GetHttpClient().DoGet("/trace", &responseStream, env.MakeAuthHeaders(), &outHeaders);
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_FORBIDDEN);

        AssertCorsHeaders(outHeaders);

        TFakeTicketParserActor* ticketParser = env.GetTicketParser();
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketRequests, 1);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketSuccesses, 1);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketFails, 0);
    }

    Y_UNIT_TEST(TraceInvalidTokenForbidden) {
        THttpMonTestEnv env;

        TStringStream responseStream;
        THttpHeaders outHeaders;
        const TString invalidToken = TString("Bearer invalid");
        const auto status = env.GetHttpClient().DoGet("/trace", &responseStream, env.MakeAuthHeaders(invalidToken), &outHeaders);
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_FORBIDDEN);

        AssertCorsHeaders(outHeaders);

        TFakeTicketParserActor* ticketParser = env.GetTicketParser();
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketRequests, 1);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketSuccesses, 0);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketFails, 1);
    }
}

} // namespace NMonitoring::NTests

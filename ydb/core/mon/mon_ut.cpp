#include <ydb/core/mon/counters_adapter_impl.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/mon/ut_utils/ut_utils.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/client/server/msgbus_http_server.h>
#include <ydb/core/testlib/test_client.h>

#include <library/cpp/http/misc/httpcodes.h>
#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/monlib/consumers/collecting_consumer.h>
#include <library/cpp/monlib/encode/spack/spack_v1.h>
#include <library/cpp/monlib/metrics/histogram_collector.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/algorithm.h>
#include <util/generic/is_in.h>
#include <util/system/datetime.h>

namespace NMonitoring::NTests {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::Tests;

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

Y_UNIT_TEST_SUITE(TMonUrlTest) {
    Y_UNIT_TEST(RelativeRoot) {
        UNIT_ASSERT_VALUES_EQUAL(GetRelativeRoot(""), "");
        UNIT_ASSERT_VALUES_EQUAL(GetRelativeRoot("/"), "");
        UNIT_ASSERT_VALUES_EQUAL(GetRelativeRoot("foo"), "");
        UNIT_ASSERT_VALUES_EQUAL(GetRelativeRoot("/foo"), "");
        UNIT_ASSERT_VALUES_EQUAL(GetRelativeRoot("foo/"), "../");
        UNIT_ASSERT_VALUES_EQUAL(GetRelativeRoot("/foo/"), "../");
        UNIT_ASSERT_VALUES_EQUAL(GetRelativeRoot("actors/sqsgc"), "../");
        UNIT_ASSERT_VALUES_EQUAL(GetRelativeRoot("/actors/sqsgc"), "../");
        UNIT_ASSERT_VALUES_EQUAL(GetRelativeRoot("/actors/sqsgc/"), "../../");
        UNIT_ASSERT_VALUES_EQUAL(GetRelativeRoot("/actors/blobstorageproxies/blobstorageproxy000000001"), "../../");
    }

    Y_UNIT_TEST(RelativeRootWithQueryAndFragment) {
        UNIT_ASSERT_VALUES_EQUAL(GetRelativeRoot("?"), "");
        UNIT_ASSERT_VALUES_EQUAL(GetRelativeRoot("#"), "");
        UNIT_ASSERT_VALUES_EQUAL(GetRelativeRoot("?next=/foo/bar"), "");
        UNIT_ASSERT_VALUES_EQUAL(GetRelativeRoot("#section/foo/bar"), "");
        UNIT_ASSERT_VALUES_EQUAL(GetRelativeRoot("/?next=/foo/bar#section/baz"), "");
        UNIT_ASSERT_VALUES_EQUAL(GetRelativeRoot("/actors/sqsgc?next=/foo/bar"), "../");
        UNIT_ASSERT_VALUES_EQUAL(GetRelativeRoot("/actors/sqsgc#section/foo/bar"), "../");
        UNIT_ASSERT_VALUES_EQUAL(GetRelativeRoot("/actors/sqsgc?next=/foo/bar#section/baz"), "../");
        UNIT_ASSERT_VALUES_EQUAL(GetRelativeRoot("/actors/sqsgc#section/foo?next=/bar/baz"), "../");
        UNIT_ASSERT_VALUES_EQUAL(GetRelativeRoot("/actors/sqsgc/?next=/foo/bar#section/baz"), "../../");
        UNIT_ASSERT_VALUES_EQUAL(GetRelativeRoot("/actors/sqsgc?#"), "../");
    }

    Y_UNIT_TEST(RelativeRootWithEncodedDelimitersAndEmptySegments) {
        UNIT_ASSERT_VALUES_EQUAL(GetRelativeRoot("/foo%2Fbar%3Fbaz%23qux"), "");
        UNIT_ASSERT_VALUES_EQUAL(GetRelativeRoot("/foo%2Fbar%3Fbaz%23qux/page"), "../");
        UNIT_ASSERT_VALUES_EQUAL(GetRelativeRoot("/foo%3Fbar/page?next=/x/y#section/z"), "../");
        UNIT_ASSERT_VALUES_EQUAL(GetRelativeRoot("/foo//bar"), "../../");
        UNIT_ASSERT_VALUES_EQUAL(GetRelativeRoot("/foo//"), "../../");
        UNIT_ASSERT_VALUES_EQUAL(GetRelativeRoot("//foo"), "../");
    }
}

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

    Y_UNIT_TEST(PreservesRequestIdAtTicketParser) {
        THttpMonTestEnv env({
            .RegKind = THttpMonTestEnvOptions::ERegKind::ActorPage,
            .ActorAllowedSIDs = {"valid_group"},
            .TicketParserGroupSIDs = {"valid_group"},
        });
        const TString requestId = "monitoring-inbound-request-id";
        auto headers = env.MakeAuthHeaders();
        headers["x-request-id"] = requestId;

        TStringStream responseStream;
        const auto status = env.GetHttpClient().DoGet(env.MakeDefaultUrl(), &responseStream, headers);
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_OK);

        TFakeTicketParserActor* ticketParser = env.GetTicketParser();
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketRequests, 1);
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->CapturedRequestId, requestId);
    }

    Y_UNIT_TEST(GeneratesRequestIdAtTicketParser) {
        THttpMonTestEnv env({
            .RegKind = THttpMonTestEnvOptions::ERegKind::ActorPage,
            .ActorAllowedSIDs = {"valid_group"},
            .TicketParserGroupSIDs = {"valid_group"},
        });

        TStringStream responseStream;
        const auto status = env.GetHttpClient().DoGet(env.MakeDefaultUrl(), &responseStream, env.MakeAuthHeaders());
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_OK);

        TFakeTicketParserActor* ticketParser = env.GetTicketParser();
        UNIT_ASSERT_VALUES_EQUAL(ticketParser->AuthorizeTicketRequests, 1);
        UNIT_ASSERT(!ticketParser->CapturedRequestId.empty());
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

Y_UNIT_TEST_SUITE(MessageBusHttp) {
    Y_UNIT_TEST(RejectsInvalidJsonAndKeepsServingRequests) {
        TPortManager ports;
        const ui16 monPort = ports.GetPort();
        TServerSettings settings(ports.GetPort());
        settings.InitKikimrRunConfig()
            .SetNodeCount(1)
            .SetUseRealThreads(true)
            .SetDomainName("Root")
            .SetUseSectorMap(true)
            .SetMonitoringPortOffset(monPort);
        TServer server(settings);
        server.EnableGRpc(ports.GetPort());
        TClient busClient(settings);
        busClient.InitRootScheme();
        NMsgBusProxy::IMessageBusHttpServer* httpServer = nullptr;
        const auto deadline = TInstant::Now() + TDuration::Seconds(30);
        while (!httpServer && TInstant::Now() < deadline) {
            httpServer = dynamic_cast<NMsgBusProxy::IMessageBusHttpServer*>(
                server.GetRuntime()->GetAppData().Mon->FindPage("CLI_MB"));
            if (!httpServer) {
                Sleep(TDuration::MilliSeconds(10));
            }
        }
        UNIT_ASSERT_C(httpServer, "MessageBus HTTP page was not registered within 30 seconds");
        TKeepAliveHttpClient client("localhost", monPort);
        const TKeepAliveHttpClient::THeaders headers{{"Content-Type", "application/json"}};

        TStringStream invalidJsonResponse;
        UNIT_ASSERT_VALUES_EQUAL(
            client.DoPost("/CLI_MB/TypesRequest", "TypesRequest", &invalidJsonResponse, headers), HTTP_BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS(invalidJsonResponse.Str(), "Invalid JSON request body");

        TStringStream invalidProtoResponse;
        UNIT_ASSERT_VALUES_EQUAL(
            client.DoPost("/CLI_MB/TypesRequest", R"({"ETag":{}})", &invalidProtoResponse, headers), HTTP_BAD_REQUEST);

        TStringStream validResponse;
        UNIT_ASSERT_VALUES_EQUAL(client.DoPost("/CLI_MB/TypesRequest", "{}", &validResponse, headers), HTTP_OK);
        UNIT_ASSERT_STRING_CONTAINS(validResponse.Str(), "TypeMetadata");

        UNIT_ASSERT_VALUES_EQUAL(httpServer->RequestsActive->Val(), 0);
        // LabelsMaintainer can reset derivative counters during startup; histograms retain their samples.
        const auto sampleCount = [](const THistogramPtr& histogram) {
            UNIT_ASSERT(histogram);
            const auto snapshot = histogram->Snapshot();
            ui64 count = 0;
            for (ui32 i = 0; i < snapshot->Count(); ++i) {
                count += snapshot->Value(i);
            }
            return count;
        };
        UNIT_ASSERT_VALUES_EQUAL(sampleCount(httpServer->RequestTotalTimeHistogram), 3);
        UNIT_ASSERT_VALUES_EQUAL(sampleCount(httpServer->RequestPrepareTimeHistogram), 1);
    }
}

Y_UNIT_TEST_SUITE(TMemOnlyCountersTest) {
    Y_UNIT_TEST(FactorySetsMemOnly) {
        auto counters = MakeIntrusive<TDynamicCounters>();
        TMetricFactoryForDynamicCounters factory(counters->GetSubgroup("subsystem", "mon"));
        auto labels = [](TStringBuf sensor) {
            return MakeLabels({{"sensor", sensor}, {"direction", "in"}, {"url", "/a"}});
        };
        factory.RateWithOpts(labels("count"), {.MemOnly = true})->Inc();
        factory.HistogramRateWithOpts(labels("time_ms"), ExplicitHistogram({1, 10}), {.MemOnly = true})->Record(5);
        factory.Rate(labels("plain"))->Inc();
        UNIT_ASSERT_EXCEPTION_CONTAINS(factory.Rate(labels("count")), yexception, "memOnly=");

        const auto group = counters->FindSubgroup("subsystem", "mon")->FindSubgroup("url", "/a");
        UNIT_ASSERT(group->FindCounter("count")->IsMemOnly());
        UNIT_ASSERT(group->FindHistogram("time_ms")->IsMemOnly());
        UNIT_ASSERT(!group->FindCounter("plain")->IsMemOnly());
    }

    // Fails if /counters does not export the HTTP proxy sensors memOnly.
    Y_UNIT_TEST(CountersPageExportsProxySensorsMemOnly) {
        THttpMonTestEnv env;
        auto fetch = [&](TStringBuf format) {
            TStringStream body;
            const auto status = env.GetHttpClient().DoGet(TStringBuilder() << "/counters/counters=utils/" << format,
                &body, env.MakeAuthHeaders());
            UNIT_ASSERT_VALUES_EQUAL(status, HTTP_OK);
            return body.ReadAll();
        };
        // (subsystem is mon, memOnly) of each metric.
        using TFlags = TVector<std::pair<bool, bool>>;
        auto fromJson = [](const TString& body) {
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(body, &json));
            TFlags flags;
            for (const auto& sensor : json["sensors"].GetArray()) {
                flags.emplace_back(sensor["labels"]["subsystem"].GetString() == "mon", sensor["memOnly"].GetBooleanSafe(false));
            }
            return flags;
        };
        auto fromSpack = [](const TString& body) {
            TStringInput in(body);
            TCollectingConsumer consumer;
            DecodeSpackV1(&in, &consumer);
            TFlags flags;
            for (const auto& metric : consumer.Metrics) {
                const auto subsystem = metric.Labels.Find("subsystem");
                flags.emplace_back(subsystem && subsystem->Value() == "mon", metric.Opts.MemOnly);
            }
            return flags;
        };
        auto check = [](const TFlags& flags, TStringBuf format) {
            UNIT_ASSERT_C(IsIn(flags, std::pair(true, true)), format << ": no memOnly mon metric");
            UNIT_ASSERT_C(!IsIn(flags, std::pair(true, false)), format << ": a mon metric without memOnly");
            UNIT_ASSERT_C(IsIn(flags, std::pair(false, false)), format << ": no plain metric");
        };

        // A request's sensors are recorded once its reply is built, so a scrape never shows its own request.
        TFlags json;
        const auto hasMon = [&] {
            json = fromJson(fetch("json"));
            return AnyOf(json, [](const auto& flag) { return flag.first; });
        };
        for (const TInstant deadline = TInstant::Now() + TDuration::Seconds(30); !hasMon() && TInstant::Now() < deadline;) {
            Sleep(TDuration::MilliSeconds(100));
        }
        check(json, "json");
        check(fromSpack(fetch("spack")), "spack");
    }
}

} // namespace NMonitoring::NTests

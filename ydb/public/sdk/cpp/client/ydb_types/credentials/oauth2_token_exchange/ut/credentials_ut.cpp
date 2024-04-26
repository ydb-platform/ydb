#include "credentials.h"

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/http/misc/parsed_request.h>
#include <library/cpp/http/server/http.h>
#include <library/cpp/http/server/response.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/string/builder.h>

using namespace NYdb;

class TTestTokenExchangeServer: public THttpServer::ICallBack {
public:
    struct TCheck {
        bool ExpectRequest = true;
        HttpCodes StatusCode = HTTP_OK;
        TCgiParameters ExpectedInputParams;
        TMaybe<TCgiParameters> InputParams;
        TString Response;
        TString ExpectedErrorPart;
        TString Error;

        void Check() {
            UNIT_ASSERT(InputParams || !ExpectRequest);
            if (InputParams) {
                UNIT_ASSERT_VALUES_EQUAL(ExpectedInputParams.Print(), InputParams->Print());
            }

            if (ExpectedErrorPart) {
                UNIT_ASSERT_STRING_CONTAINS(Error, ExpectedErrorPart);
            } else {
                UNIT_ASSERT(!Error);
            }
        }

        void Reset() {
            Error = {};
            InputParams = Nothing();
        }
    };

    class TRequest: public TRequestReplier {
    public:
        explicit TRequest(TTestTokenExchangeServer* server)
            : Server(server)
        {
        }

        bool DoReply(const TReplyParams& params) override {
            with_lock (Server->Lock) {
                const TParsedHttpFull parsed(params.Input.FirstLine());
                UNIT_ASSERT_VALUES_EQUAL(parsed.Path, "/exchange/token");
                const TString bodyStr = params.Input.ReadAll();

                Server->Check.InputParams.ConstructInPlace(bodyStr);
                THttpResponse resp(Server->Check.StatusCode);
                resp.SetContent(Server->Check.Response);
                resp.OutTo(params.Output);
                return true;
            }
        }

    public:
        TTestTokenExchangeServer* Server = nullptr;
    };

    TTestTokenExchangeServer()
        : HttpOptions(PortManager.GetPort())
        , HttpServer(this, HttpOptions)
    {
        HttpServer.Start();
    }

    TClientRequest* CreateClient() override {
        return new TRequest(this);
    }

    TString GetEndpoint() const {
        return TStringBuilder() << "http://localhost:" << HttpOptions.Port << "/exchange/token";
    }

    void Run(const std::function<void()>& f, bool checkExpectations = true) {
        Check.Reset();
        try {
            f();
        } catch (const std::exception& ex) {
            Check.Error = ex.what();
        }

        if (checkExpectations) {
            CheckExpectations();
        }
    }

    void Run(const TOauth2TokenExchangeParams& params, const TString& expectedToken = {}, bool checkExpectations = true) {
        TString token;
        Run([&]() {
            auto factory = CreateOauth2TokenExchangeCredentialsProviderFactory(params);
            if (expectedToken) {
                token = factory->CreateProvider()->GetAuthInfo();
            }
        },
        checkExpectations);

        if (expectedToken) {
            UNIT_ASSERT_VALUES_EQUAL(expectedToken, token);
        }
    }

    void CheckExpectations() {
        with_lock (Lock) {
            Check.Check();
        }
    }

    void WithLock(const std::function<void()>& f) {
        with_lock (Lock) {
            f();
        }
    }

public:
    TAdaptiveLock Lock;
    TPortManager PortManager;
    THttpServer::TOptions HttpOptions;
    THttpServer HttpServer;
    TCheck Check;
};

Y_UNIT_TEST_SUITE(TestTokenExchange) {
    Y_UNIT_TEST(Exchanges) {
        TTestTokenExchangeServer server;
        server.Check.ExpectedInputParams.emplace("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange");
        server.Check.ExpectedInputParams.emplace("requested_token_type", "urn:ietf:params:oauth:token-type:access_token");
        server.Check.ExpectedInputParams.emplace("subject_token", "test_token");
        server.Check.ExpectedInputParams.emplace("subject_token_type", "test_token_type");
        server.Check.ExpectedInputParams.emplace("audience", "test_aud");
        server.Check.ExpectedInputParams.emplace("scope", "s1 s2");
        server.Check.Response = R"({"access_token": "hello_token", "token_type": "bEareR", "expires_in": 42})";
        server.Run(
            TOauth2TokenExchangeParams()
                .TokenEndpoint(server.GetEndpoint())
                .Audience("test_aud")
                .AppendScope("s1")
                .AppendScope("s2")
                .SubjectTokenSource(CreateFixedTokenSource("test_token", "test_token_type")),
            "Bearer hello_token"
        );

        server.Check.ExpectedInputParams.emplace("audience", "test_aud_2");
        server.Run(
            TOauth2TokenExchangeParams()
                .TokenEndpoint(server.GetEndpoint())
                .AppendAudience("test_aud")
                .AppendAudience("test_aud_2")
                .AppendScope("s1")
                .AppendScope("s2")
                .SubjectTokenSource(CreateFixedTokenSource("test_token", "test_token_type")),
            "Bearer hello_token"
        );

        server.Check.ExpectedInputParams.erase("scope");
        server.Check.ExpectedInputParams.emplace("resource", "test_res");
        server.Check.ExpectedInputParams.emplace("actor_token", "act_token");
        server.Check.ExpectedInputParams.emplace("actor_token_type", "act_token_type");
        server.Run(
            TOauth2TokenExchangeParams()
                .TokenEndpoint(server.GetEndpoint())
                .AppendAudience("test_aud")
                .AppendAudience("test_aud_2")
                .Resource("test_res")
                .SubjectTokenSource(CreateFixedTokenSource("test_token", "test_token_type"))
                .ActorTokenSource(CreateFixedTokenSource("act_token", "act_token_type")),
            "Bearer hello_token"
        );
    }

    Y_UNIT_TEST(BadParams) {
        TTestTokenExchangeServer server;
        server.Check.ExpectRequest = false;

        server.Check.ExpectedErrorPart = "no token endpoint";
        server.Run(
            TOauth2TokenExchangeParams()
        );

        server.Check.ExpectedErrorPart = "empty audience";
        server.Run(
            TOauth2TokenExchangeParams()
                .TokenEndpoint(server.GetEndpoint())
                .AppendAudience("a")
                .AppendAudience("")
        );

        server.Check.ExpectedErrorPart = "empty scope";
        server.Run(
            TOauth2TokenExchangeParams()
                .TokenEndpoint(server.GetEndpoint())
                .AppendScope("s")
                .AppendScope("")
        );

        server.Check.ExpectedErrorPart = "failed to parse url";
        server.Run(
            TOauth2TokenExchangeParams()
                .TokenEndpoint("not an url")
        );
    }

    Y_UNIT_TEST(BadResponse) {
        TTestTokenExchangeServer server;
        server.Check.ExpectedInputParams.emplace("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange");
        server.Check.ExpectedInputParams.emplace("requested_token_type", "urn:ietf:params:oauth:token-type:access_token");
        server.Check.ExpectedInputParams.emplace("subject_token", "test_token");
        server.Check.ExpectedInputParams.emplace("subject_token_type", "test_token_type");

        TOauth2TokenExchangeParams params;
        params
            .TokenEndpoint(server.GetEndpoint())
            .SubjectTokenSource(CreateFixedTokenSource("test_token", "test_token_type"));

        server.Check.Response = R"(})";
        server.Check.ExpectedErrorPart = "json parsing error";
        server.Run(params);

        server.Check.Response = R"({"access_token": "hello_token", "token_type1": "bearer", "expires_in": 42})";
        server.Check.ExpectedErrorPart = "no field \"token_type\" in response";
        server.Run(params);

        server.Check.Response = R"({"access_token1": "hello_token", "token_type": "bearer", "expires_in": 42})";
        server.Check.ExpectedErrorPart = "no field \"access_token\" in response";
        server.Run(params);

        server.Check.Response = R"({"access_token": "hello_token", "token_type": "bearer", "expires_in1": 42})";
        server.Check.ExpectedErrorPart = "no field \"expires_in\" in response";
        server.Run(params);

        server.Check.Response = R"({"access_token": "hello_token", "token_type": "abc", "expires_in": 42})";
        server.Check.ExpectedErrorPart = "unsupported token type: \"abc\"";
        server.Run(params);

        server.Check.Response = R"({"access_token": "hello_token", "token_type": "bearer", "expires_in": 0})";
        server.Check.ExpectedErrorPart = "incorrect expiration time: 0";
        server.Run(params);

        server.Check.Response = R"({"access_token": "hello_token", "token_type": "bearer", "expires_in": "hello"})";
        server.Check.ExpectedErrorPart = "incorrect expiration time: 0";
        server.Run(params);

        server.Check.Response = R"({"access_token": "hello_token", "token_type": "bearer", "expires_in": -1})";
        server.Check.ExpectedErrorPart = "incorrect expiration time: -1";
        server.Run(params);

        server.Check.Response = R"({"access_token": "hello_token", "token_type": "bearer", "expires_in": 1, "scope": "s"})";
        server.Check.ExpectedErrorPart = "different scope. Expected \"\", but got \"s\"";
        server.Run(params);

        server.Check.Response = R"({"access_token": "", "token_type": "bearer", "expires_in": 1})";
        server.Check.ExpectedErrorPart = "got empty token";
        server.Run(params);

        server.Check.Response = R"({"access_token": "hello_token", "token_type": "bearer", "expires_in": 1, "scope": "s a"})";
        server.Check.ExpectedErrorPart = "different scope. Expected \"a\", but got \"s a\"";
        server.Check.ExpectedInputParams.emplace("scope", "a");
        server.Run(
            TOauth2TokenExchangeParams()
                .TokenEndpoint(server.GetEndpoint())
                .SubjectTokenSource(CreateFixedTokenSource("test_token", "test_token_type"))
                .Scope("a")
        );
        server.Check.ExpectedInputParams.erase("scope");


        server.Check.ExpectedErrorPart = "can not connect to";
        server.Check.ExpectRequest = false;
        server.Run(
            TOauth2TokenExchangeParams()
                .TokenEndpoint("https://localhost:42/aaa")
                .SubjectTokenSource(CreateFixedTokenSource("test_token", "test_token_type"))
        );
        server.Check.ExpectRequest = true;

        // parsing response
        server.Check.StatusCode = HTTP_FORBIDDEN;
        server.Check.Response = R"(not json)";
        server.Check.ExpectedErrorPart = "Exchange token error in Oauth 2 token exchange credentials provider: 403 Forbidden, could not parse response: ";
        server.Run(params);

        server.Check.Response = R"({"error": "smth"})";
        server.Check.ExpectedErrorPart = "Exchange token error in Oauth 2 token exchange credentials provider: 403 Forbidden, error: smth";
        server.Run(params);

        server.Check.Response = R"({"error": "smth", "error_description": "something terrible happened"})";
        server.Check.ExpectedErrorPart = "Exchange token error in Oauth 2 token exchange credentials provider: 403 Forbidden, error: smth, description: something terrible happened";
        server.Run(params);

        server.Check.StatusCode = HTTP_BAD_REQUEST;
        server.Check.Response = R"({"error_uri": "my_uri", "error_description": "something terrible happened"})";
        server.Check.ExpectedErrorPart = "Exchange token error in Oauth 2 token exchange credentials provider: 400 Bad request, description: something terrible happened, error_uri: my_uri";
        server.Run(params);
    }

    Y_UNIT_TEST(UpdatesToken) {
        TCredentialsProviderFactoryPtr factory;

        TTestTokenExchangeServer server;
        server.Check.ExpectedInputParams.emplace("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange");
        server.Check.ExpectedInputParams.emplace("requested_token_type", "urn:ietf:params:oauth:token-type:access_token");
        server.Check.ExpectedInputParams.emplace("subject_token", "test_token");
        server.Check.ExpectedInputParams.emplace("subject_token_type", "test_token_type");
        server.Check.Response = R"({"access_token": "token_1", "token_type": "bearer", "expires_in": 1})";
        server.Run(
            [&]() {
                factory = CreateOauth2TokenExchangeCredentialsProviderFactory(
                    TOauth2TokenExchangeParams()
                        .TokenEndpoint(server.GetEndpoint())
                        .SubjectTokenSource(CreateFixedTokenSource("test_token", "test_token_type")));
                UNIT_ASSERT_VALUES_EQUAL(factory->CreateProvider()->GetAuthInfo(), "Bearer token_1");
            }
        );

        server.WithLock(
            [&]() {
                server.Check.Response = R"({"access_token": "token_2", "token_type": "bearer", "expires_in": 1})";
            }
        );

        Sleep(TDuration::Seconds(1));
        server.Run(
            [&]() {
                UNIT_ASSERT_VALUES_EQUAL(factory->CreateProvider()->GetAuthInfo(), "Bearer token_2");
            }
        );
    }

    Y_UNIT_TEST(UsesCachedToken) {
        TCredentialsProviderFactoryPtr factory;
        TInstant startTime;

        TTestTokenExchangeServer server;
        server.Check.ExpectedInputParams.emplace("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange");
        server.Check.ExpectedInputParams.emplace("requested_token_type", "urn:ietf:params:oauth:token-type:access_token");
        server.Check.ExpectedInputParams.emplace("actor_token", "test_token");
        server.Check.ExpectedInputParams.emplace("actor_token_type", "test_token_type");
        server.Check.Response = R"({"access_token": "the_only_token", "token_type": "bearer", "expires_in": 20000})";
        server.Run(
            [&]() {
                factory = CreateOauth2TokenExchangeCredentialsProviderFactory(
                    TOauth2TokenExchangeParams()
                        .TokenEndpoint(server.GetEndpoint())
                        .ActorTokenSource(CreateFixedTokenSource("test_token", "test_token_type")));
                startTime = TInstant::Now();
                UNIT_ASSERT_VALUES_EQUAL(factory->CreateProvider()->GetAuthInfo(), "Bearer the_only_token");
            }
        );

        server.WithLock(
            [&]() {
                server.Check.StatusCode = HTTP_BAD_REQUEST;
                server.Check.Response = R"(invalid response)";
            }
        );

        UNIT_ASSERT_VALUES_EQUAL(factory->CreateProvider()->GetAuthInfo(), "Bearer the_only_token");
        UNIT_ASSERT_VALUES_EQUAL(factory->CreateProvider()->GetAuthInfo(), "Bearer the_only_token");
    }

    Y_UNIT_TEST(UpdatesTokenInBackgroud) {
        TCredentialsProviderFactoryPtr factory;
        TInstant startTime;

        TTestTokenExchangeServer server;
        server.Check.ExpectedInputParams.emplace("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange");
        server.Check.ExpectedInputParams.emplace("requested_token_type", "urn:ietf:params:oauth:token-type:access_token");
        server.Check.ExpectedInputParams.emplace("actor_token", "test_token");
        server.Check.ExpectedInputParams.emplace("actor_token_type", "test_token_type");

        for (int i = 0; i < 2; ++i) {
            server.WithLock(
                [&]() {
                    server.Check.Response = R"({"access_token": "token_1", "token_type": "bearer", "expires_in": 2})";
                }
            );
            if (!factory) {
                server.Run(
                    [&]() {
                        factory = CreateOauth2TokenExchangeCredentialsProviderFactory(
                            TOauth2TokenExchangeParams()
                                .TokenEndpoint(server.GetEndpoint())
                                .ActorTokenSource(CreateFixedTokenSource("test_token", "test_token_type")));
                        startTime = TInstant::Now();
                        UNIT_ASSERT_VALUES_EQUAL(factory->CreateProvider()->GetAuthInfo(), "Bearer token_1");
                    }
                );
            }

            server.WithLock(
                [&]() {
                    server.Check.Reset();
                    if (i == 0) {
                        server.Check.Response = R"({"access_token": "token_2", "token_type": "bearer", "expires_in": 2})";
                    } else {
                        server.Check.Response = R"({"access_token": "token_3", "token_type": "bearer", "expires_in": 2})";
                    }
                }
            );

            SleepUntil(startTime + TDuration::Seconds(1) + TDuration::MilliSeconds(5));
            const TString token = factory->CreateProvider()->GetAuthInfo();
            TInstant halfTimeTokenValid = TInstant::Now();
            if (halfTimeTokenValid < startTime + TDuration::Seconds(2)) { // valid => got cached token, but async update must be run after half time token is valid
                if (i == 0) {
                    UNIT_ASSERT_VALUES_EQUAL(token, "Bearer token_1");
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(token, "Bearer token_2");
                }
                do {
                    Sleep(TDuration::MilliSeconds(10));
                    bool gotRequest = false;
                    server.WithLock(
                        [&]() {
                            if (server.Check.InputParams) { // InputParams are created => got the request
                                gotRequest = true;
                            }
                        }
                    );
                    if (gotRequest) {
                        startTime = TInstant::Now(); // for second iteration
                        break;
                    }
                } while (TInstant::Now() <= startTime + TDuration::Seconds(30));
                server.CheckExpectations();
                server.WithLock(
                    [&]() {
                        server.Check.Reset();
                        server.Check.Response = R"(invalid response)"; // update must finish asyncronously
                    }
                );
                Sleep(TDuration::MilliSeconds(500)); // After the request is got, it takes some time to get updated token
                if (i == 0) { // Finally check that we got updated token
                    UNIT_ASSERT_VALUES_EQUAL(factory->CreateProvider()->GetAuthInfo(), "Bearer token_2");
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(factory->CreateProvider()->GetAuthInfo(), "Bearer token_3");
                }
                Cerr << "Checked backgroud update on " << i << " iteration" << Endl;
            }
        }
    }

    Y_UNIT_TEST(UpdatesTokenAndRetriesErrors) {
        TCredentialsProviderFactoryPtr factory;

        TTestTokenExchangeServer server;
        server.Check.ExpectedInputParams.emplace("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange");
        server.Check.ExpectedInputParams.emplace("requested_token_type", "urn:ietf:params:oauth:token-type:access_token");
        server.Check.ExpectedInputParams.emplace("subject_token", "test_token");
        server.Check.ExpectedInputParams.emplace("subject_token_type", "test_token_type");
        server.Check.Response = R"({"access_token": "token_1", "token_type": "bearer", "expires_in": 6})";
        server.Run(
            [&]() {
                factory = CreateOauth2TokenExchangeCredentialsProviderFactory(
                    TOauth2TokenExchangeParams()
                        .TokenEndpoint(server.GetEndpoint())
                        .SubjectTokenSource(CreateFixedTokenSource("test_token", "test_token_type")));
                UNIT_ASSERT_VALUES_EQUAL(factory->CreateProvider()->GetAuthInfo(), "Bearer token_1");
            }
        );

        server.WithLock(
            [&]() {
                server.Check.Reset();
                server.Check.StatusCode = HTTP_BAD_REQUEST; // all errors are temporary, because the first attempt is always successful (in constructor)
                server.Check.Response = R"({"error": "tmp", "error_description": "temporary error"})";
            }
        );

        Sleep(TDuration::Seconds(3) + TDuration::MilliSeconds(5));
        UNIT_ASSERT_VALUES_EQUAL(factory->CreateProvider()->GetAuthInfo(), "Bearer token_1");

        auto waitRequest = [&](TDuration howLong) {
            TInstant startTime = TInstant::Now();
            bool gotRequest = false;
            do {
                Sleep(TDuration::MilliSeconds(10));
                server.WithLock(
                    [&]() {
                        if (server.Check.InputParams) { // InputParams are created => got the request
                            gotRequest = true;
                        }
                    }
                );
                if (gotRequest) {
                    break;
                }
            } while (TInstant::Now() <= startTime + howLong);
            return gotRequest;
        };

        UNIT_ASSERT(waitRequest(TDuration::Seconds(30)));

        server.WithLock(
            [&]() {
                server.Check.Reset();
                server.Check.StatusCode = HTTP_OK;
                server.Check.Response = R"({"access_token": "token_2", "token_type": "bearer", "expires_in": 2})";
            }
        );

        UNIT_ASSERT(waitRequest(TDuration::Seconds(10)));
        Sleep(TDuration::MilliSeconds(500)); // After the request is got, it takes some time to get updated token
        UNIT_ASSERT_VALUES_EQUAL(factory->CreateProvider()->GetAuthInfo(), "Bearer token_2");

        server.WithLock(
            [&]() {
                server.Check.Reset();
                server.Check.StatusCode = HTTP_INTERNAL_SERVER_ERROR;
                server.Check.Response = R"({})";
            }
        );

        Sleep(TDuration::Seconds(2));
        UNIT_ASSERT_EXCEPTION(factory->CreateProvider()->GetAuthInfo(), std::runtime_error);
    }

    Y_UNIT_TEST(ShutdownWhileRefreshingToken) {
        TCredentialsProviderFactoryPtr factory;

        TTestTokenExchangeServer server;
        server.Check.ExpectedInputParams.emplace("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange");
        server.Check.ExpectedInputParams.emplace("requested_token_type", "urn:ietf:params:oauth:token-type:access_token");
        server.Check.ExpectedInputParams.emplace("subject_token", "test_token");
        server.Check.ExpectedInputParams.emplace("subject_token_type", "test_token_type");
        server.Check.Response = R"({"access_token": "token_1", "token_type": "bearer", "expires_in": 6})";

        server.Run(
            [&]() {
                factory = CreateOauth2TokenExchangeCredentialsProviderFactory(
                    TOauth2TokenExchangeParams()
                        .TokenEndpoint(server.GetEndpoint())
                        .SubjectTokenSource(CreateFixedTokenSource("test_token", "test_token_type")));
                UNIT_ASSERT_VALUES_EQUAL(factory->CreateProvider()->GetAuthInfo(), "Bearer token_1");
            }
        );

        server.WithLock(
            [&]() {
                server.Check.Reset();
                server.Check.StatusCode = HTTP_INTERNAL_SERVER_ERROR;
                server.Check.Response = R"({})";
            }
        );

        Sleep(TDuration::Seconds(3) + TDuration::MilliSeconds(5));

        UNIT_ASSERT_VALUES_EQUAL(factory->CreateProvider()->GetAuthInfo(), "Bearer token_1");

        const TInstant shutdownStart = TInstant::Now();
        factory = nullptr;
        const TInstant shutdownStop = TInstant::Now();
        Cerr << "Shutdown: " << (shutdownStop - shutdownStart) << Endl;
    }
}

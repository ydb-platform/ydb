#include "credentials.h"
#include "from_file.h"
#include <ydb/public/sdk/cpp/client/ydb_types/credentials/oauth2_token_exchange/ut/jwt_check_helper.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/http/misc/parsed_request.h>
#include <library/cpp/http/server/http.h>
#include <library/cpp/http/server/response.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/system/tempfile.h>

using namespace NYdb;

extern const TString TestRSAPrivateKeyContent;
extern const TString TestRSAPublicKeyContent;
extern const TString TestECPrivateKeyContent;
extern const TString TestECPublicKeyContent;
extern const TString TestHMACSecretKeyBase64Content;

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
        TMaybe<TJwtCheck> SubjectJwtCheck;
        TMaybe<TJwtCheck> ActorJwtCheck;

        void Check() {
            UNIT_ASSERT_C(InputParams || !ExpectRequest, "Request error: " << Error);
            if (InputParams) {
                if (SubjectJwtCheck || ActorJwtCheck) {
                    TCgiParameters inputParamsCopy = *InputParams;
                    if (SubjectJwtCheck) {
                        TString subjectJwt;
                        UNIT_ASSERT(inputParamsCopy.Has("subject_token"));
                        UNIT_ASSERT(inputParamsCopy.Has("subject_token_type", "urn:ietf:params:oauth:token-type:jwt"));
                        subjectJwt = inputParamsCopy.Get("subject_token");
                        inputParamsCopy.Erase("subject_token");
                        inputParamsCopy.Erase("subject_token_type");
                        SubjectJwtCheck->Check(subjectJwt);
                    }
                    if (ActorJwtCheck) {
                        TString actorJwt;
                        UNIT_ASSERT(inputParamsCopy.Has("actor_token"));
                        UNIT_ASSERT(inputParamsCopy.Has("actor_token_type", "urn:ietf:params:oauth:token-type:jwt"));
                        actorJwt = inputParamsCopy.Get("actor_token");
                        inputParamsCopy.Erase("actor_token");
                        inputParamsCopy.Erase("actor_token_type");
                        ActorJwtCheck->Check(actorJwt);
                    }
                    UNIT_ASSERT_VALUES_EQUAL(ExpectedInputParams.Print(), inputParamsCopy.Print());
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(ExpectedInputParams.Print(), InputParams->Print());
                }
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

    void RunFromConfig(const TString& fileName, const TString& expectedToken = {}, bool checkExpectations = true, const TString& explicitTokenEndpoint = {}) {
        TString token;
        Run([&]() {
            auto factory = CreateOauth2TokenExchangeFileCredentialsProviderFactory(fileName, explicitTokenEndpoint);
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

template <class TParent>
struct TJsonFillerArray {
    TJsonFillerArray(TParent* parent, NJson::TJsonWriter* writer)
        : Parent(parent)
        , Writer(writer)
    {
        Writer->OpenArray();
    }

    TJsonFillerArray& Value(const TStringBuf& value) {
        Writer->Write(value);
        return *this;
    }

    TParent& Build() {
        Writer->CloseArray();
        return *Parent;
    }

    TParent* Parent = nullptr;
    NJson::TJsonWriter* Writer = nullptr;
};

template <class TDerived>
struct TJsonFiller {
    TJsonFiller(NJson::TJsonWriter* writer)
        : Writer(writer)
    {}

    TDerived& Field(const TStringBuf& key, const TStringBuf& value) {
        Writer->WriteKey(key);
        Writer->Write(value);
        return *static_cast<TDerived*>(this);
    }

    TJsonFillerArray<TDerived> Array(const TStringBuf& key) {
        Writer->WriteKey(key);
        return TJsonFillerArray(static_cast<TDerived*>(this), Writer);
    }

    NJson::TJsonWriter* Writer = nullptr;
};

struct TTestConfigFile : public TJsonFiller<TTestConfigFile> {
    struct TSubMap : public TJsonFiller<TSubMap> {
        TSubMap(TTestConfigFile* parent, NJson::TJsonWriter* writer)
            : TJsonFiller<TSubMap>(writer)
            , Parent(parent)
        {
            Writer->OpenMap();
        }

        TTestConfigFile& Build() {
            Writer->CloseMap();
            return *Parent;
        }

        TTestConfigFile* Parent = nullptr;
    };

    TTestConfigFile()
        : TJsonFiller<TTestConfigFile>(&Writer)
        , Writer(&Result.Out, true)
        , TmpFile(MakeTempName(nullptr, "oauth2_cfg"))
    {
        Writer.OpenMap();
    }

    TSubMap SubMap(const TStringBuf& key) {
        Writer.WriteKey(key);
        return TSubMap(this, &Writer);
    }

    TString Build() {
        Writer.CloseMap();
        Writer.Flush();

        TUnbufferedFileOutput(TmpFile.Name()).Write(Result);

        return TmpFile.Name();
    }

    TStringBuilder Result;
    NJson::TJsonWriter Writer;
    TTempFile TmpFile;
};

Y_UNIT_TEST_SUITE(TestTokenExchange) {
    void Exchanges(bool fromConfig) {
        TTestTokenExchangeServer server;
        server.Check.ExpectedInputParams.emplace("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange");
        server.Check.ExpectedInputParams.emplace("requested_token_type", "urn:ietf:params:oauth:token-type:access_token");
        server.Check.ExpectedInputParams.emplace("subject_token", "test_token");
        server.Check.ExpectedInputParams.emplace("subject_token_type", "test_token_type");
        server.Check.ExpectedInputParams.emplace("audience", "test_aud");
        server.Check.ExpectedInputParams.emplace("scope", "s1 s2");
        server.Check.Response = R"({"access_token": "hello_token", "token_type": "bEareR", "expires_in": 42})";
        if (fromConfig) {
            server.RunFromConfig(
                TTestConfigFile()
                    .Field("token-endpoint", server.GetEndpoint())
                    .Field("aud", "test_aud")
                    .Array("scope")
                        .Value("s1")
                        .Value("s2")
                        .Build()
                    .SubMap("subject-credentials")
                        .Field("type", "Fixed")
                        .Field("token", "test_token")
                        .Field("token-type", "test_token_type")
                        .Build()
                    .Build(),
                "Bearer hello_token"
            );
        } else {
            server.Run(
                TOauth2TokenExchangeParams()
                    .TokenEndpoint(server.GetEndpoint())
                    .Audience("test_aud")
                    .AppendScope("s1")
                    .AppendScope("s2")
                    .SubjectTokenSource(CreateFixedTokenSource("test_token", "test_token_type")),
                "Bearer hello_token"
            );
        }

        server.Check.ExpectedInputParams.emplace("audience", "test_aud_2");
        if (fromConfig) {
            server.RunFromConfig(
                TTestConfigFile()
                    .Field("token-endpoint", server.GetEndpoint())
                    .Array("aud")
                        .Value("test_aud")
                        .Value("test_aud_2")
                        .Build()
                    .Array("scope")
                        .Value("s1")
                        .Value("s2")
                        .Build()
                    .SubMap("subject-credentials")
                        .Field("type", "Fixed")
                        .Field("token", "test_token")
                        .Field("token-type", "test_token_type")
                        .Build()
                    .Build(),
                "Bearer hello_token"
            );
        } else {
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
        }

        server.Check.ExpectedInputParams.erase("scope");
        server.Check.ExpectedInputParams.emplace("resource", "test_res");
        server.Check.ExpectedInputParams.emplace("actor_token", "act_token");
        server.Check.ExpectedInputParams.emplace("actor_token_type", "act_token_type");
        if (fromConfig) {
            server.RunFromConfig(
                TTestConfigFile()
                    .Field("token-endpoint", server.GetEndpoint())
                    .Array("aud")
                        .Value("test_aud")
                        .Value("test_aud_2")
                        .Build()
                    .Field("res", "test_res")
                    .SubMap("subject-credentials")
                        .Field("type", "Fixed")
                        .Field("token", "test_token")
                        .Field("token-type", "test_token_type")
                        .Build()
                    .SubMap("actor-credentials")
                        .Field("type", "Fixed")
                        .Field("token", "act_token")
                        .Field("token-type", "act_token_type")
                        .Build()
                    .Build(),
                "Bearer hello_token"
            );
        } else {
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
    }

    Y_UNIT_TEST(Exchanges) {
        Exchanges(false);
    }

    Y_UNIT_TEST(ExchangesFromConfig) {
        Exchanges(true);
    }

    void BadParams(bool fromConfig) {
        TTestTokenExchangeServer server;
        server.Check.ExpectRequest = false;

        server.Check.ExpectedErrorPart = "no token endpoint";
        if (fromConfig) {
            server.RunFromConfig(
                TTestConfigFile()
                    .Build()
            );
        } else {
            server.Run(
                TOauth2TokenExchangeParams()
            );
        }

        server.Check.ExpectedErrorPart = "empty audience";
        if (fromConfig) {
            server.RunFromConfig(
                TTestConfigFile()
                    .Field("token-endpoint", server.GetEndpoint())
                    .Array("aud")
                        .Value("a")
                        .Value("")
                        .Build()
                    .Build()
            );
        } else {
            server.Run(
                TOauth2TokenExchangeParams()
                    .TokenEndpoint(server.GetEndpoint())
                    .AppendAudience("a")
                    .AppendAudience("")
            );
        }

        server.Check.ExpectedErrorPart = "empty scope";
        if (fromConfig) {
            server.RunFromConfig(
                TTestConfigFile()
                    .Field("token-endpoint", server.GetEndpoint())
                    .Array("scope")
                        .Value("s")
                        .Value("")
                        .Build()
                    .Build()
            );
        } else {
            server.Run(
                TOauth2TokenExchangeParams()
                    .TokenEndpoint(server.GetEndpoint())
                    .AppendScope("s")
                    .AppendScope("")
            );
        }

        server.Check.ExpectedErrorPart = "failed to parse url";
        if (fromConfig) {
            server.RunFromConfig(
                TTestConfigFile()
                    .Field("token-endpoint", "not an url")
                    .Build()
            );
        } else {
            server.Run(
                TOauth2TokenExchangeParams()
                    .TokenEndpoint("not an url")
            );
        }
    }

    Y_UNIT_TEST(BadParams) {
        BadParams(false);
    }

    Y_UNIT_TEST(BadParamsFromConfig) {
        BadParams(true);
    }

    void BadResponse(bool fromConfig) {
        TTestTokenExchangeServer server;
        server.Check.ExpectedInputParams.emplace("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange");
        server.Check.ExpectedInputParams.emplace("requested_token_type", "urn:ietf:params:oauth:token-type:access_token");
        server.Check.ExpectedInputParams.emplace("subject_token", "test_token");
        server.Check.ExpectedInputParams.emplace("subject_token_type", "test_token_type");

        TOauth2TokenExchangeParams params;
        TTestConfigFile cfg;
        if (fromConfig) {
            cfg
                .Field("token-endpoint", server.GetEndpoint())
                .SubMap("subject-credentials")
                        .Field("type", "Fixed")
                        .Field("token", "test_token")
                        .Field("token-type", "test_token_type")
                        .Build()
                .Build();
        } else {
            params
                .TokenEndpoint(server.GetEndpoint())
                .SubjectTokenSource(CreateFixedTokenSource("test_token", "test_token_type"));
        }

        auto run = [&]() {
            if (fromConfig) {
                server.RunFromConfig(cfg.TmpFile.Name());
            } else {
                server.Run(params);
            }
        };

        server.Check.Response = R"(})";
        server.Check.ExpectedErrorPart = "json parsing error";
        run();

        server.Check.Response = R"({"access_token": "hello_token", "token_type1": "bearer", "expires_in": 42})";
        server.Check.ExpectedErrorPart = "no field \"token_type\" in response";
        run();

        server.Check.Response = R"({"access_token1": "hello_token", "token_type": "bearer", "expires_in": 42})";
        server.Check.ExpectedErrorPart = "no field \"access_token\" in response";
        run();

        server.Check.Response = R"({"access_token": "hello_token", "token_type": "bearer", "expires_in1": 42})";
        server.Check.ExpectedErrorPart = "no field \"expires_in\" in response";
        run();

        server.Check.Response = R"({"access_token": "hello_token", "token_type": "abc", "expires_in": 42})";
        server.Check.ExpectedErrorPart = "unsupported token type: \"abc\"";
        run();

        server.Check.Response = R"({"access_token": "hello_token", "token_type": "bearer", "expires_in": 0})";
        server.Check.ExpectedErrorPart = "incorrect expiration time: 0";
        run();

        server.Check.Response = R"({"access_token": "hello_token", "token_type": "bearer", "expires_in": "hello"})";
        server.Check.ExpectedErrorPart = "incorrect expiration time: 0";
        run();

        server.Check.Response = R"({"access_token": "hello_token", "token_type": "bearer", "expires_in": -1})";
        server.Check.ExpectedErrorPart = "incorrect expiration time: -1";
        run();

        server.Check.Response = R"({"access_token": "hello_token", "token_type": "bearer", "expires_in": 1, "scope": "s"})";
        server.Check.ExpectedErrorPart = "different scope. Expected \"\", but got \"s\"";
        run();

        server.Check.Response = R"({"access_token": "", "token_type": "bearer", "expires_in": 1})";
        server.Check.ExpectedErrorPart = "got empty token";
        run();

        server.Check.Response = R"({"access_token": "hello_token", "token_type": "bearer", "expires_in": 1, "scope": "s a"})";
        server.Check.ExpectedErrorPart = "different scope. Expected \"a\", but got \"s a\"";
        server.Check.ExpectedInputParams.emplace("scope", "a");
        if (fromConfig) {
            server.RunFromConfig(
                TTestConfigFile()
                    .Field("token-endpoint", server.GetEndpoint())
                    .SubMap("subject-credentials")
                        .Field("type", "Fixed")
                        .Field("token", "test_token")
                        .Field("token-type", "test_token_type")
                        .Build()
                    .Field("scope", "a")
                    .Build()
            );
        } else {
            server.Run(
                TOauth2TokenExchangeParams()
                    .TokenEndpoint(server.GetEndpoint())
                    .SubjectTokenSource(CreateFixedTokenSource("test_token", "test_token_type"))
                    .Scope("a")
            );
        }
        server.Check.ExpectedInputParams.erase("scope");


        server.Check.ExpectedErrorPart = "can not connect to";
        server.Check.ExpectRequest = false;
        if (fromConfig) {
            server.RunFromConfig(
                TTestConfigFile()
                    .Field("token-endpoint", "https://localhost:42/aaa")
                    .SubMap("subject-credentials")
                        .Field("type", "Fixed")
                        .Field("token", "test_token")
                        .Field("token-type", "test_token_type")
                        .Build()
                    .Build()
            );
        } else {
            server.Run(
                TOauth2TokenExchangeParams()
                    .TokenEndpoint("https://localhost:42/aaa")
                    .SubjectTokenSource(CreateFixedTokenSource("test_token", "test_token_type"))
            );
        }
        server.Check.ExpectRequest = true;

        // parsing response
        server.Check.StatusCode = HTTP_FORBIDDEN;
        server.Check.Response = R"(not json)";
        server.Check.ExpectedErrorPart = "Exchange token error in Oauth 2 token exchange credentials provider: 403 Forbidden, could not parse response: ";
        run();

        server.Check.Response = R"({"error": "smth"})";
        server.Check.ExpectedErrorPart = "Exchange token error in Oauth 2 token exchange credentials provider: 403 Forbidden, error: smth";
        run();

        server.Check.Response = R"({"error": "smth", "error_description": "something terrible happened"})";
        server.Check.ExpectedErrorPart = "Exchange token error in Oauth 2 token exchange credentials provider: 403 Forbidden, error: smth, description: something terrible happened";
        run();

        server.Check.StatusCode = HTTP_BAD_REQUEST;
        server.Check.Response = R"({"error_uri": "my_uri", "error_description": "something terrible happened"})";
        server.Check.ExpectedErrorPart = "Exchange token error in Oauth 2 token exchange credentials provider: 400 Bad request, description: something terrible happened, error_uri: my_uri";
        run();
    }

    Y_UNIT_TEST(BadResponse) {
        BadResponse(false);
    }

    Y_UNIT_TEST(BadResponseFromConfig) {
        BadResponse(true);
    }

    void UpdatesToken(bool fromConfig) {
        TCredentialsProviderFactoryPtr factory;

        TTestTokenExchangeServer server;
        server.Check.ExpectedInputParams.emplace("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange");
        server.Check.ExpectedInputParams.emplace("requested_token_type", "urn:ietf:params:oauth:token-type:access_token");
        server.Check.ExpectedInputParams.emplace("subject_token", "test_token");
        server.Check.ExpectedInputParams.emplace("subject_token_type", "test_token_type");
        server.Check.Response = R"({"access_token": "token_1", "token_type": "bearer", "expires_in": 1})";
        server.Run(
            [&]() {
                if (fromConfig) {
                    factory = CreateOauth2TokenExchangeFileCredentialsProviderFactory(
                        TTestConfigFile()
                            .Field("token-endpoint", server.GetEndpoint())
                            .SubMap("subject-credentials")
                                .Field("type", "Fixed")
                                .Field("token", "test_token")
                                .Field("token-type", "test_token_type")
                                .Build()
                            .Build());
                } else {
                    factory = CreateOauth2TokenExchangeCredentialsProviderFactory(
                        TOauth2TokenExchangeParams()
                            .TokenEndpoint(server.GetEndpoint())
                            .SubjectTokenSource(CreateFixedTokenSource("test_token", "test_token_type")));
                }
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

    Y_UNIT_TEST(UpdatesToken) {
        UpdatesToken(false);
    }

    Y_UNIT_TEST(UpdatesTokenFromConfig) {
        UpdatesToken(true);
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

    Y_UNIT_TEST(ExchangesFromFileConfig) {
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
        server.Check.ExpectedInputParams.emplace("resource", "test_res_2");
        server.Check.ExpectedInputParams.emplace("actor_token", "act_token");
        server.Check.ExpectedInputParams.emplace("actor_token_type", "act_token_type");
        server.Run(
            TOauth2TokenExchangeParams()
                .TokenEndpoint(server.GetEndpoint())
                .AppendAudience("test_aud")
                .AppendAudience("test_aud_2")
                .AppendResource("test_res")
                .AppendResource("test_res_2")
                .SubjectTokenSource(CreateFixedTokenSource("test_token", "test_token_type"))
                .ActorTokenSource(CreateFixedTokenSource("act_token", "act_token_type")),
            "Bearer hello_token"
        );
    }

    Y_UNIT_TEST(SkipsUnknownFieldsInConfig) {
        TTestTokenExchangeServer server;
        server.Check.ExpectedInputParams.emplace("grant_type", "test_grant_type");
        server.Check.ExpectedInputParams.emplace("requested_token_type", "test_requested_token_type");
        server.Check.ExpectedInputParams.emplace("subject_token", "test_token");
        server.Check.ExpectedInputParams.emplace("subject_token_type", "test_token_type");
        server.Check.ExpectedInputParams.emplace("resource", "r1");
        server.Check.ExpectedInputParams.emplace("resource", "r2");
        server.Check.Response = R"({"access_token": "received_token", "token_type": "bEareR", "expires_in": 42})";
        server.RunFromConfig(
            TTestConfigFile()
                .Field("token-endpoint", "bla-bla-bla") // use explicit endpoint via param
                .Field("unknown", "unknown value")
                .Field("grant-type", "test_grant_type")
                .Field("requested-token-type", "test_requested_token_type")
                .Array("res")
                    .Value("r1")
                    .Value("r2")
                    .Build()
                .SubMap("subject-credentials")
                    .Field("type", "Fixed")
                    .Field("token", "test_token")
                    .Field("token-type", "test_token_type")
                    .Field("unknown", "unknown value")
                    .Build()
                .Build(),
            "Bearer received_token",
            true,
            server.GetEndpoint()
        );
    }

    Y_UNIT_TEST(JwtTokenSourceInConfig) {
        TTestTokenExchangeServer server;
        server.Check.ExpectedInputParams.emplace("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange");
        server.Check.ExpectedInputParams.emplace("requested_token_type", "urn:ietf:params:oauth:token-type:access_token");
        server.Check.SubjectJwtCheck.ConstructInPlace()
            .TokenTtl(TDuration::Hours(24))
            .KeyId("test_key_id")
            .Issuer("test_iss")
            .Subject("test_sub")
            .Audience("test_aud")
            .Id("test_jti")
            .Alg<jwt::algorithm::rs384>(TestRSAPublicKeyContent);
        server.Check.ActorJwtCheck.ConstructInPlace()
            .AppendAudience("a1")
            .AppendAudience("a2");
        server.Check.Response = R"({"access_token": "received_token", "token_type": "bEareR", "expires_in": 42})";
        server.RunFromConfig(
            TTestConfigFile()
                .Field("token-endpoint", server.GetEndpoint())
                .SubMap("subject-credentials")
                    .Field("type", "jwt")
                    .Field("ttl", "24h")
                    .Field("kid", "test_key_id")
                    .Field("iss", "test_iss")
                    .Field("sub", "test_sub")
                    .Field("aud", "test_aud")
                    .Field("jti", "test_jti")
                    .Field("alg", "rs384")
                    .Field("private-key", TestRSAPrivateKeyContent)
                    .Field("unknown", "unknown value")
                    .Build()
                .SubMap("actor-credentials")
                    .Field("type", "JWT")
                    .Array("aud")
                        .Value("a1")
                        .Value("a2")
                        .Build()
                    .Field("alg", "RS256")
                    .Field("private-key", TestRSAPrivateKeyContent)
                    .Build()
                .Build(),
            "Bearer received_token"
        );

        // Other signing methods
        server.Check.SubjectJwtCheck.ConstructInPlace()
            .Id("jti")
            .Alg<jwt::algorithm::hs384>(Base64Decode(TestHMACSecretKeyBase64Content));
        server.Check.ActorJwtCheck.ConstructInPlace()
            .Alg<jwt::algorithm::es256>(TestECPublicKeyContent)
            .Issuer("iss");
        server.RunFromConfig(
            TTestConfigFile()
                .Field("token-endpoint", server.GetEndpoint())
                .SubMap("subject-credentials")
                    .Field("type", "jwt")
                    .Field("jti", "jti")
                    .Field("alg", "HS384")
                    .Field("private-key", TestHMACSecretKeyBase64Content)
                    .Build()
                .SubMap("actor-credentials")
                    .Field("type", "JWT")
                    .Field("alg", "ES256")
                    .Field("private-key", TestECPrivateKeyContent)
                    .Field("iss", "iss")
                    .Build()
                .Build(),
            "Bearer received_token"
        );
    }

    Y_UNIT_TEST(BadConfigParams) {
        TTestTokenExchangeServer server;
        server.Check.ExpectRequest = false;

        // wrong format
        TTempFile cfg(MakeTempName(nullptr, "oauth2_cfg"));
        TUnbufferedFileOutput(cfg.Name()).Write(""); // empty
        server.Check.ExpectedErrorPart = "Failed to parse config file";
        server.RunFromConfig(cfg.Name());

        TUnbufferedFileOutput(cfg.Name()).Write("not a json");
        server.Check.ExpectedErrorPart = "Failed to parse config file";
        server.RunFromConfig(cfg.Name());

        TUnbufferedFileOutput(cfg.Name()).Write("[\"not a map\"]");
        server.Check.ExpectedErrorPart = "Not a map";
        server.RunFromConfig(cfg.Name());

        server.Check.ExpectedErrorPart = "No \"type\" parameter";
        server.RunFromConfig(
            TTestConfigFile()
                .Field("token-endpoint", server.GetEndpoint())
                .SubMap("subject-credentials")
                    .Field("type1", "unknown")
                    .Build()
                .Build()
        );

        server.Check.ExpectedErrorPart = "Incorrect \"type\" parameter";
        server.RunFromConfig(
            TTestConfigFile()
                .Field("token-endpoint", server.GetEndpoint())
                .SubMap("subject-credentials")
                    .Field("type", "unknown")
                    .Build()
                .Build()
        );

        server.Check.ExpectedErrorPart = "Failed to parse \"iss\""; // must be string
        server.RunFromConfig(
            TTestConfigFile()
                .Field("token-endpoint", server.GetEndpoint())
                .SubMap("subject-credentials")
                    .Field("type", "jwt")
                    .Array("iss")
                        .Value("1")
                        .Build()
                    .Build()
                .Build()
        );

        server.Check.ExpectedErrorPart = "No \"token\" parameter";
        server.RunFromConfig(
            TTestConfigFile()
                .Field("token-endpoint", server.GetEndpoint())
                .SubMap("subject-credentials")
                    .Field("type", "fixed")
                    .Field("token-type", "test_token_type")
                    .Build()
                .Build()
        );

        server.Check.ExpectedErrorPart = "No \"token-type\" parameter";
        server.RunFromConfig(
            TTestConfigFile()
                .Field("token-endpoint", server.GetEndpoint())
                .SubMap("subject-credentials")
                    .Field("type", "fixed")
                    .Field("token", "test_token")
                    .Build()
                .Build()
        );

        server.Check.ExpectedErrorPart = "No \"alg\" parameter";
        server.RunFromConfig(
            TTestConfigFile()
                .Field("token-endpoint", server.GetEndpoint())
                .SubMap("subject-credentials")
                    .Field("type", "jwt")
                    .Field("private-key", TestRSAPrivateKeyContent)
                    .Build()
                .Build()
        );

        server.Check.ExpectedErrorPart = "No \"private-key\" parameter";
        server.RunFromConfig(
            TTestConfigFile()
                .Field("token-endpoint", server.GetEndpoint())
                .SubMap("subject-credentials")
                    .Field("type", "jwt")
                    .Field("alg", "rs256")
                    .Build()
                .Build()
        );

        server.Check.ExpectedErrorPart = "Failed to parse \"ttl\"";
        server.RunFromConfig(
            TTestConfigFile()
                .Field("token-endpoint", server.GetEndpoint())
                .SubMap("subject-credentials")
                    .Field("type", "jwt")
                    .Field("alg", "rs256")
                    .Field("private-key", TestRSAPrivateKeyContent)
                    .Field("ttl", "-1s")
                    .Build()
                .Build()
        );

        server.Check.ExpectedErrorPart = "Algorithm \"algorithm\" is not supported. Supported algorithms are: ";
        server.RunFromConfig(
            TTestConfigFile()
                .Field("token-endpoint", server.GetEndpoint())
                .SubMap("subject-credentials")
                    .Field("type", "jwt")
                    .Field("alg", "algorithm")
                    .Field("private-key", TestRSAPrivateKeyContent)
                    .Build()
                .Build()
        );

        server.Check.ExpectedErrorPart = "failed to load private key";
        server.RunFromConfig(
            TTestConfigFile()
                .Field("token-endpoint", server.GetEndpoint())
                .SubMap("subject-credentials")
                    .Field("type", "jwt")
                    .Field("alg", "rs256")
                    .Field("private-key", "not a key")
                    .Build()
                .Build()
        );

        server.Check.ExpectedErrorPart = "failed to decode HMAC secret from Base64";
        server.RunFromConfig(
            TTestConfigFile()
                .Field("token-endpoint", server.GetEndpoint())
                .SubMap("subject-credentials")
                    .Field("type", "jwt")
                    .Field("alg", "hs256")
                    .Field("private-key", "\n<not a base64>\n")
                    .Build()
                .Build()
        );

        server.Check.ExpectedErrorPart = "failed to load private key";
        server.RunFromConfig(
            TTestConfigFile()
                .Field("token-endpoint", server.GetEndpoint())
                .SubMap("subject-credentials")
                    .Field("type", "jwt")
                    .Field("alg", "es256")
                    .Field("private-key", TestRSAPrivateKeyContent) // Need EC key
                    .Build()
                .Build()
        );

        server.Check.ExpectedErrorPart = "failed to load private key";
        server.RunFromConfig(
            TTestConfigFile()
                .Field("token-endpoint", server.GetEndpoint())
                .SubMap("subject-credentials")
                    .Field("type", "jwt")
                    .Field("alg", "ps512")
                    .Field("private-key", TestHMACSecretKeyBase64Content) // Need RSA key
                    .Build()
                .Build()
        );

        server.Check.ExpectedErrorPart = "Not a map";
        server.RunFromConfig(
            TTestConfigFile()
                .Field("token-endpoint", server.GetEndpoint())
                .Array("subject-credentials")
                    .Value("42")
                    .Build()
                .Build()
        );
    }
}

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/iam.h>

#include <library/cpp/http/server/http.h>
#include <library/cpp/http/server/response.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/system/thread.h>

using namespace NYdb;

class TMetadataServer : public THttpServer::ICallBack {
public:
    class TRequest : public TRequestReplier {
    public:
        explicit TRequest(TMetadataServer* server)
            : Server(server)
        {}

        bool DoReply(const TReplyParams& params) override {
            with_lock (Server->Lock) {
                ++Server->RequestCount;
            }

            THttpResponse resp(Server->StatusCode);
            resp.SetContent(Server->Response);
            resp.OutTo(params.Output);
            return true;
        }

        TMetadataServer* Server = nullptr;
    };

    TMetadataServer()
        : Port(PortManager.GetPort())
        , HttpOptions(Port)
        , HttpServer(this, HttpOptions)
    {
        HttpServer.Start();
    }

    ~TMetadataServer() {
        HttpServer.Stop();
    }

    TClientRequest* CreateClient() override {
        return new TRequest(this);
    }

    void SetResponse(HttpCodes code, const TString& response) {
        StatusCode = code;
        Response = response;
    }

    int GetRequestCount() const {
        with_lock (Lock) {
            return RequestCount;
        }
    }

    void ResetRequestCount() {
        with_lock (Lock) {
            RequestCount = 0;
        }
    }

    TPortManager PortManager;
    uint16_t Port;
    THttpServer::TOptions HttpOptions;
    THttpServer HttpServer;
    HttpCodes StatusCode = HTTP_OK;
    TString Response;
    mutable TAdaptiveLock Lock;
    int RequestCount = 0;
};

static TString MakeTokenResponse(const TString& token, int expiresIn) {
    TStringStream ss;
    NJson::TJsonWriter w(&ss, false);
    w.OpenMap();
    w.WriteKey("access_token");
    w.Write(token);
    w.WriteKey("token_type");
    w.Write("Bearer");
    w.WriteKey("expires_in");
    w.Write(expiresIn);
    w.CloseMap();
    w.Flush();
    return ss.Str();
}

static TString MakeTokenResponseWithExpiry(const TString& token, const TInstant& expiry) {
    TStringStream ss;
    NJson::TJsonWriter w(&ss, false);
    w.OpenMap();
    w.WriteKey("access_token");
    w.Write(token);
    w.WriteKey("token_type");
    w.Write("Bearer");
    w.WriteKey("expiry");
    w.Write(expiry.FormatGmTime("%Y-%m-%dT%H:%M:%S.000000000Z"));
    w.CloseMap();
    w.Flush();
    return ss.Str();
}

static TString MakeTokenResponseNoExpiry(const TString& token) {
    TStringStream ss;
    NJson::TJsonWriter w(&ss, false);
    w.OpenMap();
    w.WriteKey("access_token");
    w.Write(token);
    w.WriteKey("token_type");
    w.Write("Bearer");
    w.CloseMap();
    w.Flush();
    return ss.Str();
}

Y_UNIT_TEST_SUITE(IamCredentialsProvider) {
    Y_UNIT_TEST(BasicTokenFetch) {
        TMetadataServer server;
        server.SetResponse(HTTP_OK, MakeTokenResponse("test-token-123", 3600));

        TIamHost params;
        params.Host = "localhost";
        params.Port = server.Port;
        params.RefreshPeriod = TDuration::Hours(1);

        auto factory = CreateIamCredentialsProviderFactory(params);
        auto provider = factory->CreateProvider();

        UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "test-token-123");
    }

    Y_UNIT_TEST(ExpiryFieldSupport) {
        TMetadataServer server;
        auto expiry = TInstant::Now() + TDuration::Hours(12);
        server.SetResponse(HTTP_OK, MakeTokenResponseWithExpiry("expiry-token", expiry));

        TIamHost params;
        params.Host = "localhost";
        params.Port = server.Port;
        params.RefreshPeriod = TDuration::Hours(1);

        auto factory = CreateIamCredentialsProviderFactory(params);
        auto provider = factory->CreateProvider();

        UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "expiry-token");

        // Second call should not trigger refresh (token is still valid)
        int countBefore = server.GetRequestCount();
        provider->GetAuthInfo();
        UNIT_ASSERT_VALUES_EQUAL(server.GetRequestCount(), countBefore);
    }

    Y_UNIT_TEST(NoExpiryFieldFallback) {
        TMetadataServer server;
        server.SetResponse(HTTP_OK, MakeTokenResponseNoExpiry("no-expiry-token"));

        TIamHost params;
        params.Host = "localhost";
        params.Port = server.Port;
        params.RefreshPeriod = TDuration::Hours(1);

        auto factory = CreateIamCredentialsProviderFactory(params);
        auto provider = factory->CreateProvider();

        // Token should be saved even without expires_in/expiry
        UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "no-expiry-token");

        // Should not immediately refresh (fallback interval should be > 0)
        int countBefore = server.GetRequestCount();
        provider->GetAuthInfo();
        UNIT_ASSERT_VALUES_EQUAL(server.GetRequestCount(), countBefore);
    }

    Y_UNIT_TEST(ServerError) {
        TMetadataServer server;
        server.SetResponse(HTTP_INTERNAL_SERVER_ERROR, "");

        TIamHost params;
        params.Host = "localhost";
        params.Port = server.Port;

        auto factory = CreateIamCredentialsProviderFactory(params);
        auto provider = factory->CreateProvider();

        // Constructor GetTicket() fails, token should be empty
        UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "");
    }

    Y_UNIT_TEST(ConcurrentAccess) {
        TMetadataServer server;
        server.SetResponse(HTTP_OK, MakeTokenResponse("concurrent-token", 3600));

        TIamHost params;
        params.Host = "localhost";
        params.Port = server.Port;
        params.RefreshPeriod = TDuration::MilliSeconds(1); // Force frequent refreshes

        auto factory = CreateIamCredentialsProviderFactory(params);
        auto provider = factory->CreateProvider();

        constexpr int NUM_THREADS = 8;
        constexpr int ITERATIONS = 100;

        std::vector<THolder<TThread>> threads;
        std::atomic<int> errors{0};

        for (int i = 0; i < NUM_THREADS; ++i) {
            threads.push_back(MakeHolder<TThread>([&]() {
                for (int j = 0; j < ITERATIONS; ++j) {
                    try {
                        auto token = provider->GetAuthInfo();
                        if (token != "concurrent-token") {
                            errors.fetch_add(1);
                        }
                    } catch (...) {
                        errors.fetch_add(1);
                    }
                }
            }));
            threads.back()->Start();
        }

        for (auto& t : threads) {
            t->Join();
        }

        UNIT_ASSERT_VALUES_EQUAL(errors.load(), 0);
    }
}

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/iam.h>

#include <library/cpp/http/server/http.h>
#include <library/cpp/http/server/response.h>
#include <library/cpp/json/json_writer.h>

#include <library/cpp/testing/common/network.h>

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

using namespace NYdb;

class TMetadataServer : public THttpServer::ICallBack {
public:
    class TRequest : public TRequestReplier {
    public:
        explicit TRequest(TMetadataServer* server)
            : Server(server)
        {}

        bool DoReply(const TReplyParams& params) override {
            {
                std::lock_guard lock(Server->Lock);
                ++Server->RequestCount;
            }

            THttpResponse resp(Server->StatusCode);
            resp.SetContent(TString{Server->Response});
            resp.OutTo(params.Output);
            return true;
        }

        TMetadataServer* Server = nullptr;
    };

    TMetadataServer()
        : PortHolder(NTesting::GetFreePort())
        , Port(static_cast<std::uint16_t>(PortHolder))
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

    void SetResponse(HttpCodes code, const std::string& response) {
        StatusCode = code;
        Response = response;
    }

    int GetRequestCount() const {
        std::lock_guard lock(Lock);
        return RequestCount;
    }

    void ResetRequestCount() {
        std::lock_guard lock(Lock);
        RequestCount = 0;
    }

    NTesting::TPortHolder PortHolder;
    uint16_t Port;
    THttpServer::TOptions HttpOptions;
    THttpServer HttpServer;
    HttpCodes StatusCode = HTTP_OK;
    std::string Response;
    mutable std::mutex Lock;
    int RequestCount = 0;
};

static std::string MakeTokenResponse(const std::string& token, int expiresIn) {
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

static std::string MakeTokenResponseWithExpiry(const std::string& token, const TInstant& expiry) {
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

static std::string MakeTokenResponseNoExpiry(const std::string& token) {
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

TEST(IamCredentialsProvider, BasicTokenFetch) {
    TMetadataServer server;
    server.SetResponse(HTTP_OK, MakeTokenResponse("test-token-123", 3600));

    TIamHost params;
    params.Host = "localhost";
    params.Port = server.Port;
    params.RefreshPeriod = TDuration::Hours(1);

    auto factory = CreateIamCredentialsProviderFactory(params);
    auto provider = factory->CreateProvider();

    EXPECT_EQ(provider->GetAuthInfo(), "test-token-123");
}

TEST(IamCredentialsProvider, ExpiryFieldSupport) {
    TMetadataServer server;
    auto expiry = TInstant::Now() + TDuration::Hours(12);
    server.SetResponse(HTTP_OK, MakeTokenResponseWithExpiry("expiry-token", expiry));

    TIamHost params;
    params.Host = "localhost";
    params.Port = server.Port;
    params.RefreshPeriod = TDuration::Hours(1);

    auto factory = CreateIamCredentialsProviderFactory(params);
    auto provider = factory->CreateProvider();

    EXPECT_EQ(provider->GetAuthInfo(), "expiry-token");

    // Second call should not trigger refresh (token is still valid)
    int countBefore = server.GetRequestCount();
    provider->GetAuthInfo();
    EXPECT_EQ(server.GetRequestCount(), countBefore);
}

TEST(IamCredentialsProvider, NoExpiryFieldFallback) {
    TMetadataServer server;
    server.SetResponse(HTTP_OK, MakeTokenResponseNoExpiry("no-expiry-token"));

    TIamHost params;
    params.Host = "localhost";
    params.Port = server.Port;
    params.RefreshPeriod = TDuration::Hours(1);

    auto factory = CreateIamCredentialsProviderFactory(params);
    auto provider = factory->CreateProvider();

    // Token should be saved even without expires_in/expiry
    EXPECT_EQ(provider->GetAuthInfo(), "no-expiry-token");

    // Should not immediately refresh (fallback interval should be > 0)
    int countBefore = server.GetRequestCount();
    provider->GetAuthInfo();
    EXPECT_EQ(server.GetRequestCount(), countBefore);
}

TEST(IamCredentialsProvider, ServerError) {
    TMetadataServer server;
    server.SetResponse(HTTP_INTERNAL_SERVER_ERROR, "");

    TIamHost params;
    params.Host = "localhost";
    params.Port = server.Port;

    auto factory = CreateIamCredentialsProviderFactory(params);
    auto provider = factory->CreateProvider();

    // Constructor GetTicket() fails, token should be empty
    EXPECT_EQ(provider->GetAuthInfo(), "");
}

TEST(IamCredentialsProvider, ConcurrentAccess) {
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

    std::vector<std::unique_ptr<std::thread>> threads;
    std::atomic<int> errors{0};

    for (int i = 0; i < NUM_THREADS; ++i) {
        threads.push_back(std::make_unique<std::thread>([&]() {
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
    }

    for (auto& t : threads) {
        t->join();
    }

    EXPECT_EQ(errors.load(), 0);
}

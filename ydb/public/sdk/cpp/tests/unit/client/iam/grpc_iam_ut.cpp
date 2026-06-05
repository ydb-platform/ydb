#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common/generic_provider.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/public/sdk/cpp/src/client/types/core_facility/simple_core_facility.h>

#include <ydb/public/api/client/yc_public/iam/iam_token_service.grpc.pb.h>
#include <ydb/public/api/client/yc_public/iam/iam_token_service.pb.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

using namespace NYdb;
using namespace yandex::cloud::iam::v1;

namespace {

class TBlockingIamTokenService final : public IamTokenService::Service {
public:
    grpc::Status Create(
        grpc::ServerContext* context,
        const CreateIamTokenRequest*,
        CreateIamTokenResponse* response) override
    {
        RpcEntered_.store(true);

        std::unique_lock lock(Mutex_);
        ReleasedCv_.wait(lock, [this] { return Released_; });

        if (context->IsCancelled()) {
            return grpc::Status::CANCELLED;
        }
        response->set_iam_token("released-token");
        response->mutable_expires_at()->set_seconds(4102444800);
        response->mutable_expires_at()->set_nanos(0);
        return grpc::Status::OK;
    }

    void Release() {
        {
            std::lock_guard lock(Mutex_);
            Released_ = true;
        }
        ReleasedCv_.notify_all();
    }

    bool WaitUntilRpcEntered(std::chrono::milliseconds timeout) const {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (RpcEntered_.load()) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        return RpcEntered_.load();
    }

private:
    std::atomic<bool> RpcEntered_{false};
    bool Released_ = false;
    mutable std::mutex Mutex_;
    std::condition_variable ReleasedCv_;
};

class TIamGrpcServer {
public:
    explicit TIamGrpcServer(TBlockingIamTokenService* service)
        : Service_(service)
    {}

    bool Start() {
        grpc::ServerBuilder builder;
        int boundPort = 0;
        builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &boundPort);
        builder.SetMaxSendMessageSize(4 * 1024 * 1024);
        builder.RegisterService(Service_);
        Server_ = builder.BuildAndStart();
        if (!Server_ || boundPort <= 0) {
            return false;
        }
        Port_ = boundPort;
        WaitThread_ = std::thread([this] { Server_->Wait(); });
        return true;
    }

    std::string Endpoint() const {
        return "127.0.0.1:" + std::to_string(Port_);
    }

    void Stop() {
        if (Server_) {
            Server_->Shutdown();
        }
        if (WaitThread_.joinable()) {
            WaitThread_.join();
        }
        Server_.reset();
    }

    ~TIamGrpcServer() {
        Stop();
    }

private:
    TBlockingIamTokenService* Service_ = nullptr;
    std::unique_ptr<grpc::Server> Server_;
    int Port_ = 0;
    std::thread WaitThread_;
};

} // namespace

TEST(GrpcIamCredentialsProvider, TeardownWhileIamCreatePendingCompletes) {
    TBlockingIamTokenService iamService;
    TIamGrpcServer server(&iamService);
    ASSERT_TRUE(server.Start());

    TIamOAuth params;
    params.Endpoint = server.Endpoint();
    params.OAuthToken = "unit-test-oauth-token";
    params.EnableSsl = false;
    params.RefreshPeriod = TDuration::Hours(1);
    // Short client deadline: completion arrives with DEADLINE_EXCEEDED while the server handler is still
    // blocked waiting for Release() (in-flight work on the server side vs client teardown).
    params.RequestTimeout = TDuration::MilliSeconds(400);

    auto work = [&params] {
        auto facility = std::make_shared<TSimpleCoreFacility>();
        TIamOAuthCredentialsProvider<CreateIamTokenRequest, CreateIamTokenResponse, IamTokenService> provider(
            params,
            facility);
        (void)provider;
    };

    std::future<void> done = std::async(std::launch::async, work);

    ASSERT_TRUE(iamService.WaitUntilRpcEntered(std::chrono::seconds(5)))
        << "server should have accepted the IAM Create call";

    ASSERT_EQ(done.wait_for(std::chrono::seconds(20)), std::future_status::ready)
        << "provider destructor must finish while an IAM Create is still blocked on the server "
           "(in-flight RPC vs channel teardown).";
    done.get();

    iamService.Release();
    server.Stop();
}

TEST(GrpcIamCredentialsProvider, TeardownWhileIamCreatePendingCompletesViaFactoryWrapper) {
    TBlockingIamTokenService iamService;
    TIamGrpcServer server(&iamService);
    ASSERT_TRUE(server.Start());

    TIamOAuth params;
    params.Endpoint = server.Endpoint();
    params.OAuthToken = "unit-test-oauth-token";
    params.EnableSsl = false;
    params.RefreshPeriod = TDuration::Hours(1);
    params.RequestTimeout = TDuration::MilliSeconds(400);

    auto factory = std::make_shared<TIamOAuthCredentialsProviderFactory<
        CreateIamTokenRequest, CreateIamTokenResponse, IamTokenService>>(params);

    auto work = [&factory] {
        auto provider = factory->CreateProvider();
        (void)provider;
    };

    std::future<void> done = std::async(std::launch::async, work);

    ASSERT_TRUE(iamService.WaitUntilRpcEntered(std::chrono::seconds(5)))
        << "server should have accepted the IAM Create call";

    ASSERT_EQ(done.wait_for(std::chrono::seconds(20)), std::future_status::ready)
        << "factory wrapper teardown must finish while an IAM Create is still blocked on the server";
    done.get();

    iamService.Release();
    server.Stop();
}

// Regression test for backward compatibility of the deprecated no-arg
// ICredentialsProviderFactory::CreateProvider() entry point on IAM factories. After commit
// 0140ad8 ("fix sdk: fixed self thread join in iam cred provider") this entry point was
// throwing "Not supported" and broke all out-of-tree callers that didn't yet plumb an
// ICoreFacility. The factory must spin up a private facility transparently.
TEST(GrpcIamCredentialsProvider, NoArgCreateProviderBackwardCompat) {
    TBlockingIamTokenService iamService;
    TIamGrpcServer server(&iamService);
    ASSERT_TRUE(server.Start());

    iamService.Release();

    TIamOAuth params;
    params.Endpoint = server.Endpoint();
    params.OAuthToken = "unit-test-oauth-token";
    params.EnableSsl = false;
    params.RefreshPeriod = TDuration::Hours(1);
    params.RequestTimeout = TDuration::Seconds(5);

    auto factory = std::make_shared<TIamOAuthCredentialsProviderFactory<
        CreateIamTokenRequest, CreateIamTokenResponse, IamTokenService>>(params);

    auto work = [&factory]() -> std::string {
        auto provider = factory->CreateProvider();
        return provider->GetAuthInfo();
    };

    std::future<std::string> done = std::async(std::launch::async, work);
    ASSERT_EQ(done.wait_for(std::chrono::seconds(20)), std::future_status::ready)
        << "no-arg CreateProvider() path must produce a token and tear down cleanly";
    EXPECT_EQ(done.get(), "released-token");

    server.Stop();
}

namespace {

// PS256-compatible RSA test keypair (also used in jwt_token_source_ut.cpp).
constexpr const char* TestRSAPrivateKey =
    "-----BEGIN PRIVATE KEY-----\n"
    "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC75/JS3rMcLJxv\n"
    "FgpOzF5+2gH+Yig3RE2MTl9uwC0BZKAv6foYr7xywQyWIK+W1cBhz8R4LfFmZo2j\n"
    "M0aCvdRmNBdW0EDSTnHLxCsFhoQWLVq+bI5f5jzkcoiioUtaEpADPqwgVULVtN/n\n"
    "nPJiZ6/dU30C3jmR6+LUgEntUtWt3eq3xQIn5lG3zC1klBY/HxtfH5Hu8xBvwRQT\n"
    "Jnh3UpPLj8XwSmriDgdrhR7o6umWyVuGrMKlLHmeivlfzjYtfzO1MOIMG8t2/zxG\n"
    "R+xb4Vwks73sH1KruH/0/JMXU97npwpe+Um+uXhpldPygGErEia7abyZB2gMpXqr\n"
    "WYKMo02NAgMBAAECggEAO0BpC5OYw/4XN/optu4/r91bupTGHKNHlsIR2rDzoBhU\n"
    "YLd1evpTQJY6O07EP5pYZx9mUwUdtU4KRJeDGO/1/WJYp7HUdtxwirHpZP0lQn77\n"
    "uccuX/QQaHLrPekBgz4ONk+5ZBqukAfQgM7fKYOLk41jgpeDbM2Ggb6QUSsJISEp\n"
    "zrwpI/nNT/wn+Hvx4DxrzWU6wF+P8kl77UwPYlTA7GsT+T7eKGVH8xsxmK8pt6lg\n"
    "svlBA5XosWBWUCGLgcBkAY5e4ZWbkdd183o+oMo78id6C+PQPE66PLDtHWfpRRmN\n"
    "m6XC03x6NVhnfvfozoWnmS4+e4qj4F/emCHvn0GMywKBgQDLXlj7YPFVXxZpUvg/\n"
    "rheVcCTGbNmQJ+4cZXx87huqwqKgkmtOyeWsRc7zYInYgraDrtCuDBCfP//ZzOh0\n"
    "LxepYLTPk5eNn/GT+VVrqsy35Ccr60g7Lp/bzb1WxyhcLbo0KX7/6jl0lP+VKtdv\n"
    "mto+4mbSBXSM1Y5BVVoVgJ3T/wKBgQDsiSvPRzVi5TTj13x67PFymTMx3HCe2WzH\n"
    "JUyepCmVhTm482zW95pv6raDr5CTO6OYpHtc5sTTRhVYEZoEYFTM9Vw8faBtluWG\n"
    "BjkRh4cIpoIARMn74YZKj0C/0vdX7SHdyBOU3bgRPHg08Hwu3xReqT1kEPSI/B2V\n"
    "4pe5fVrucwKBgQCNFgUxUA3dJjyMES18MDDYUZaRug4tfiYouRdmLGIxUxozv6CG\n"
    "ZnbZzwxFt+GpvPUV4f+P33rgoCvFU+yoPctyjE6j+0aW0DFucPmb2kBwCu5J/856\n"
    "kFwCx3blbwFHAco+SdN7g2kcwgmV2MTg/lMOcU7XwUUcN0Obe7UlWbckzQKBgQDQ\n"
    "nXaXHL24GGFaZe4y2JFmujmNy1dEsoye44W9ERpf9h1fwsoGmmCKPp90az5+rIXw\n"
    "FXl8CUgk8lXW08db/r4r+ma8Lyx0GzcZyplAnaB5/6j+pazjSxfO4KOBy4Y89Tb+\n"
    "TP0AOcCi6ws13bgY+sUTa/5qKA4UVw+c5zlb7nRpgwKBgGXAXhenFw1666482iiN\n"
    "cHSgwc4ZHa1oL6aNJR1XWH+aboBSwR+feKHUPeT4jHgzRGo/aCNHD2FE5I8eBv33\n"
    "of1kWYjAO0YdzeKrW0rTwfvt9gGg+CS397aWu4cy+mTI+MNfBgeDAIVBeJOJXLlX\n"
    "hL8bFAuNNVrCOp79TNnNIsh7\n"
    "-----END PRIVATE KEY-----\n";

constexpr const char* TestRSAPublicKey =
    "-----BEGIN PUBLIC KEY-----\n"
    "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAu+fyUt6zHCycbxYKTsxe\n"
    "ftoB/mIoN0RNjE5fbsAtAWSgL+n6GK+8csEMliCvltXAYc/EeC3xZmaNozNGgr3U\n"
    "ZjQXVtBA0k5xy8QrBYaEFi1avmyOX+Y85HKIoqFLWhKQAz6sIFVC1bTf55zyYmev\n"
    "3VN9At45kevi1IBJ7VLVrd3qt8UCJ+ZRt8wtZJQWPx8bXx+R7vMQb8EUEyZ4d1KT\n"
    "y4/F8Epq4g4Ha4Ue6OrplslbhqzCpSx5nor5X842LX8ztTDiDBvLdv88RkfsW+Fc\n"
    "JLO97B9Sq7h/9PyTF1Pe56cKXvlJvrl4aZXT8oBhKxImu2m8mQdoDKV6q1mCjKNN\n"
    "jQIDAQAB\n"
    "-----END PUBLIC KEY-----\n";

} // namespace

// Regression test for the deprecated no-arg CreateProvider() on the JWT factory. Mirrors the
// OAuth counterpart above. The standalone path spins up a private TSimpleCoreFacility behind
// TOwningFacilityCredentialsProvider; this verifies the JWT specialization wires it up
// correctly and tears down without aborting.
TEST(GrpcIamCredentialsProvider, NoArgCreateProviderBackwardCompatJwt) {
    TBlockingIamTokenService iamService;
    TIamGrpcServer server(&iamService);
    ASSERT_TRUE(server.Start());

    iamService.Release();

    TIamJwtParams params;
    params.Endpoint = server.Endpoint();
    params.EnableSsl = false;
    params.RefreshPeriod = TDuration::Hours(1);
    params.RequestTimeout = TDuration::Seconds(5);
    params.JwtParams.AccountId = "unit-test-account";
    params.JwtParams.KeyId = "unit-test-key";
    params.JwtParams.PrivKey = TestRSAPrivateKey;
    params.JwtParams.PubKey = TestRSAPublicKey;

    auto factory = std::make_shared<TIamJwtCredentialsProviderFactory<
        CreateIamTokenRequest, CreateIamTokenResponse, IamTokenService>>(params);

    auto work = [&factory]() -> std::string {
        auto provider = factory->CreateProvider();
        return provider->GetAuthInfo();
    };

    std::future<std::string> done = std::async(std::launch::async, work);
    ASSERT_EQ(done.wait_for(std::chrono::seconds(20)), std::future_status::ready)
        << "no-arg CreateProvider() path must produce a token and tear down cleanly";
    EXPECT_EQ(done.get(), "released-token");

    server.Stop();
}

namespace {

class TSlowBlockingAuthProvider final : public ICredentialsProvider {
public:
    std::string GetAuthInfo() const override {
        std::unique_lock lock(Mutex_);
        if (++CallCount_ > 1) {
            BlockedCv_.wait(lock, [this] { return Released_; });
        }
        return "slow-auth-token";
    }

    bool IsValid() const override {
        return true;
    }

    void Release() {
        {
            std::lock_guard lock(Mutex_);
            Released_ = true;
        }
        BlockedCv_.notify_all();
    }

    bool WaitUntilBlocked(std::chrono::milliseconds timeout) const {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            std::lock_guard lock(Mutex_);
            if (CallCount_ > 1 && !Released_) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        std::lock_guard lock(Mutex_);
        return CallCount_ > 1 && !Released_;
    }

private:
    mutable std::mutex Mutex_;
    mutable std::condition_variable BlockedCv_;
    mutable int CallCount_ = 0;
    bool Released_ = false;
};

} // namespace

// Regression: Stop() must not hang when FillContext is blocked inside AuthTokenProvider_->GetAuthInfo().
TEST(GrpcIamCredentialsProvider, StopDuringFillContextDoesNotHang) {
    TBlockingIamTokenService iamService;
    TIamGrpcServer server(&iamService);
    ASSERT_TRUE(server.Start());

    iamService.Release();

    auto authProvider = std::make_shared<TSlowBlockingAuthProvider>();

    TIamOAuth params;
    params.Endpoint = server.Endpoint();
    params.OAuthToken = "unit-test-oauth-token";
    params.EnableSsl = false;
    params.RefreshPeriod = TDuration::MilliSeconds(50);
    params.RequestTimeout = TDuration::Seconds(5);

    std::shared_ptr<TGrpcIamCredentialsProvider<CreateIamTokenRequest, CreateIamTokenResponse, IamTokenService>> provider;
    auto facility = std::make_shared<TSimpleCoreFacility>();

    provider = std::make_shared<TGrpcIamCredentialsProvider<CreateIamTokenRequest, CreateIamTokenResponse, IamTokenService>>(
        params,
        [token = params.OAuthToken](CreateIamTokenRequest& req) {
            req.set_yandex_passport_oauth_token(TStringType{token});
        },
        [](IamTokenService::Stub* stub, grpc::ClientContext* context, const CreateIamTokenRequest* request,
           CreateIamTokenResponse* response, std::function<void(grpc::Status)> cb) {
            stub->async()->Create(context, request, response, std::move(cb));
        },
        facility,
        authProvider);

    ASSERT_TRUE(authProvider->WaitUntilBlocked(std::chrono::seconds(10)))
        << "FillContext should block inside slow AuthTokenProvider during refresh";

    // Move-capture provider so provider.reset() is the last owner and triggers Stop().
    // facility stays here: its destructor joins the worker thread (still blocked in
    // GetAuthInfo()), so it must be destroyed after authProvider->Release().
    //
    // Mirrors production (TOwningFacilityCredentialsProvider): Inner_ dies first (Stop()),
    // then Facility_ dies (joins worker). No self-join: TImpl holds facility as weak_ptr,
    // callbacks capture only weak_ptr<TImpl>. If a callback locks TImpl temporarily, TImpl's
    // destructor is trivial (no joins), and it never strong-refs the facility.
    std::future<void> stopDone = std::async(std::launch::async,
        [provider = std::move(provider)]() mutable {
            // Last owner: triggers ~TGrpcIamCredentialsProvider -> Stop().
            // Stop() must return even though the worker thread is still blocked in GetAuthInfo().
            provider.reset();
        });

    ASSERT_EQ(stopDone.wait_for(std::chrono::seconds(20)), std::future_status::ready)
        << "provider destructor (Stop()) must complete while FillContext is blocked in GetAuthInfo()";
    stopDone.get();

    // Now unblock the worker thread so facility can be destroyed cleanly.
    authProvider->Release();
    facility.reset();  // joins the worker thread (now unblocked)

    server.Stop();
}

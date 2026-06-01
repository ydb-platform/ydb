#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common/generic_provider.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam_private/iam.h>

#include <ydb/public/api/client/yc_private/iam/iam_token_service.grpc.pb.h>
#include <ydb/public/api/client/yc_private/iam/iam_token_service.pb.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>

using namespace NYdb;
using namespace yandex::cloud::priv::iam::v1;

namespace {

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

// Implements both Create (used by the nested JWT provider) and CreateForService (used by the
// outer service provider). Returns distinct tokens so the outer one is observable via GetAuthInfo().
class TIamServiceStub final : public IamTokenService::Service {
public:
    grpc::Status Create(
        grpc::ServerContext*,
        const CreateIamTokenRequest*,
        CreateIamTokenResponse* response) override
    {
        response->set_iam_token("inner-jwt-token");
        response->mutable_expires_at()->set_seconds(4102444800);
        return grpc::Status::OK;
    }

    grpc::Status CreateForService(
        grpc::ServerContext*,
        const CreateIamTokenForServiceRequest*,
        CreateIamTokenResponse* response) override
    {
        response->set_iam_token("outer-service-token");
        response->mutable_expires_at()->set_seconds(4102444800);
        return grpc::Status::OK;
    }
};

class TIamGrpcServer {
public:
    explicit TIamGrpcServer(TIamServiceStub* service) : Service_(service) {}

    bool Start() {
        grpc::ServerBuilder builder;
        int boundPort = 0;
        builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &boundPort);
        builder.RegisterService(Service_);
        Server_ = builder.BuildAndStart();
        if (!Server_ || boundPort <= 0) {
            return false;
        }
        Port_ = boundPort;
        WaitThread_ = std::thread([this] { Server_->Wait(); });
        return true;
    }

    std::string Endpoint() const { return "127.0.0.1:" + std::to_string(Port_); }

    void Stop() {
        if (Server_) {
            Server_->Shutdown();
        }
        if (WaitThread_.joinable()) {
            WaitThread_.join();
        }
        Server_.reset();
    }

    ~TIamGrpcServer() { Stop(); }

private:
    TIamServiceStub* Service_ = nullptr;
    std::unique_ptr<grpc::Server> Server_;
    int Port_ = 0;
    std::thread WaitThread_;
};

} // namespace

// Regression test for the deprecated no-arg CreateProvider() on the IAM service-account
// factory with a nested gRPC JWT auth provider. Before the fix, both providers shared a single
// TSimpleCoreFacility, each registered a periodic refresh task, and TSimpleCoreFacility's
// single-task invariant tripped Y_ABORT_UNLESS and killed the process. The fix gives the
// nested auth provider its own facility (via a recursive no-arg CreateProvider()), so the two
// periodic tasks land on separate facilities.
TEST(IamServiceCredentialsProvider, NoArgCreateProviderWithGrpcInnerCreds) {
    TIamServiceStub stub;
    TIamGrpcServer server(&stub);
    ASSERT_TRUE(server.Start());

    TIamJwtParams jwtParams;
    jwtParams.Endpoint = server.Endpoint();
    jwtParams.EnableSsl = false;
    jwtParams.RefreshPeriod = TDuration::Hours(1);
    jwtParams.RequestTimeout = TDuration::Seconds(5);
    jwtParams.JwtParams.AccountId = "unit-test-account";
    jwtParams.JwtParams.KeyId = "unit-test-key";
    jwtParams.JwtParams.PrivKey = TestRSAPrivateKey;
    jwtParams.JwtParams.PubKey = TestRSAPublicKey;

    auto jwtFactory = std::make_shared<TIamJwtCredentialsProviderFactory<
        CreateIamTokenRequest, CreateIamTokenResponse, IamTokenService>>(jwtParams);

    TIamServiceParams serviceParams;
    serviceParams.Endpoint = server.Endpoint();
    serviceParams.EnableSsl = false;
    serviceParams.RefreshPeriod = TDuration::Hours(1);
    serviceParams.RequestTimeout = TDuration::Seconds(5);
    serviceParams.ServiceId = "unit-test-service";
    serviceParams.MicroserviceId = "unit-test-microservice";
    serviceParams.ResourceId = "unit-test-resource";
    serviceParams.ResourceType = "unit-test-resource-type";
    serviceParams.TargetServiceAccountId = "unit-test-target";
    serviceParams.SystemServiceAccountCredentials = jwtFactory;

    auto serviceFactory = CreateIamServiceCredentialsProviderFactory(serviceParams);

    auto work = [&serviceFactory]() -> std::string {
        auto provider = serviceFactory->CreateProvider();
        return provider->GetAuthInfo();
    };

    std::future<std::string> done = std::async(std::launch::async, work);
    ASSERT_EQ(done.wait_for(std::chrono::seconds(20)), std::future_status::ready)
        << "no-arg CreateProvider() with nested gRPC JWT auth must not abort and must produce a token";
    EXPECT_EQ(done.get(), "outer-service-token");

    server.Stop();
}

// Regression: MakeRequestFiller must capture TIamServiceParams by value so a temporary factory
// can be destroyed while the provider still handles periodic refresh ticks.
TEST(IamServiceCredentialsProvider, NoArgCreateProviderTemporaryFactory) {
    TIamServiceStub stub;
    TIamGrpcServer server(&stub);
    ASSERT_TRUE(server.Start());

    TIamJwtParams jwtParams;
    jwtParams.Endpoint = server.Endpoint();
    jwtParams.EnableSsl = false;
    jwtParams.RefreshPeriod = TDuration::Hours(1);
    jwtParams.RequestTimeout = TDuration::Seconds(5);
    jwtParams.JwtParams.AccountId = "unit-test-account";
    jwtParams.JwtParams.KeyId = "unit-test-key";
    jwtParams.JwtParams.PrivKey = TestRSAPrivateKey;
    jwtParams.JwtParams.PubKey = TestRSAPublicKey;

    auto jwtFactory = std::make_shared<TIamJwtCredentialsProviderFactory<
        CreateIamTokenRequest, CreateIamTokenResponse, IamTokenService>>(jwtParams);

    TIamServiceParams serviceParams;
    serviceParams.Endpoint = server.Endpoint();
    serviceParams.EnableSsl = false;
    serviceParams.RefreshPeriod = TDuration::MilliSeconds(100);
    serviceParams.RequestTimeout = TDuration::Seconds(5);
    serviceParams.ServiceId = "unit-test-service";
    serviceParams.MicroserviceId = "unit-test-microservice";
    serviceParams.ResourceId = "unit-test-resource";
    serviceParams.ResourceType = "unit-test-resource-type";
    serviceParams.TargetServiceAccountId = "unit-test-target";
    serviceParams.SystemServiceAccountCredentials = jwtFactory;

    auto provider = CreateIamServiceCredentialsProviderFactory(serviceParams)->CreateProvider();
    EXPECT_EQ(provider->GetAuthInfo(), "outer-service-token");

    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_EQ(provider->GetAuthInfo(), "outer-service-token");

    server.Stop();
}

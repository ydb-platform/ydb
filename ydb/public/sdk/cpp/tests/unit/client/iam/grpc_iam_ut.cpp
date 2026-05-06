#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common/generic_provider.h>
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

#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common/types.h>

#include <ydb/public/api/client/yc_public/iam/iam_token_service.grpc.pb.h>
#include <ydb/public/api/client/yc_public/iam/iam_token_service.pb.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

namespace NYdb::NTest {

class TIamTokenServiceStub final : public yandex::cloud::iam::v1::IamTokenService::Service {
public:
    void SetResponseToken(const std::string& token, int64_t expiresAtSeconds = 4102444800);

    grpc::Status Create(
        grpc::ServerContext*,
        const yandex::cloud::iam::v1::CreateIamTokenRequest* request,
        yandex::cloud::iam::v1::CreateIamTokenResponse* response) override;

    int GetRequestCount() const;
    yandex::cloud::iam::v1::CreateIamTokenRequest GetLastRequest() const;
    bool HasLastRequest() const;

private:
    mutable std::mutex Lock_;
    std::string IamToken_;
    int64_t ExpiresAtSeconds_ = 4102444800;
    int RequestCount_ = 0;
    yandex::cloud::iam::v1::CreateIamTokenRequest LastRequest_;
    bool HasLastRequest_ = false;
};

class TBlockingIamTokenService final : public yandex::cloud::iam::v1::IamTokenService::Service {
public:
    grpc::Status Create(
        grpc::ServerContext* context,
        const yandex::cloud::iam::v1::CreateIamTokenRequest*,
        yandex::cloud::iam::v1::CreateIamTokenResponse* response) override;

    void Release();
    void Shutdown();

    bool WaitUntilRpcEntered(std::chrono::milliseconds timeout) const;

private:
    std::atomic<bool> RpcEntered_{false};
    bool Released_ = false;
    bool ShuttingDown_ = false;
    mutable std::mutex Mutex_;
    std::condition_variable ReleasedCv_;
};

class TBlockingIamReleaseGuard {
public:
    explicit TBlockingIamReleaseGuard(TBlockingIamTokenService& service)
        : Service_(service)
    {}

    ~TBlockingIamReleaseGuard() {
        Service_.Release();
    }

private:
    TBlockingIamTokenService& Service_;
};

class TIamGrpcServer {
public:
    explicit TIamGrpcServer(grpc::Service* service);
    TIamGrpcServer(const TIamGrpcServer&) = delete;
    TIamGrpcServer& operator=(const TIamGrpcServer&) = delete;

    bool Start();
    std::string Endpoint() const;
    void Stop();

    ~TIamGrpcServer();

private:
    grpc::Service* Service_ = nullptr;
    std::unique_ptr<grpc::Server> Server_;
    int Port_ = 0;
    std::thread WaitThread_;
};

TIamOAuth MakeOAuthParams(const std::string& endpoint, const std::string& oauthToken = "unit-test-oauth-token");

TIamJwtParams MakeJwtParams(const std::string& endpoint);

} // namespace NYdb::NTest

#include "iam_grpc_mock_server.h"

#include "iam_test_keys.h"

namespace NYdb::NTest {

void TIamTokenServiceStub::SetResponseToken(const std::string& token, int64_t expiresAtSeconds) {
    std::lock_guard lock(Lock_);
    IamToken_ = token;
    ExpiresAtSeconds_ = expiresAtSeconds;
}

grpc::Status TIamTokenServiceStub::Create(
    grpc::ServerContext*,
    const yandex::cloud::iam::v1::CreateIamTokenRequest* request,
    yandex::cloud::iam::v1::CreateIamTokenResponse* response)
{
    std::lock_guard lock(Lock_);
    ++RequestCount_;
    LastRequest_ = *request;
    HasLastRequest_ = true;
    response->set_iam_token(IamToken_);
    response->mutable_expires_at()->set_seconds(ExpiresAtSeconds_);
    response->mutable_expires_at()->set_nanos(0);
    return grpc::Status::OK;
}

int TIamTokenServiceStub::GetRequestCount() const {
    std::lock_guard lock(Lock_);
    return RequestCount_;
}

yandex::cloud::iam::v1::CreateIamTokenRequest TIamTokenServiceStub::GetLastRequest() const {
    std::lock_guard lock(Lock_);
    return LastRequest_;
}

bool TIamTokenServiceStub::HasLastRequest() const {
    std::lock_guard lock(Lock_);
    return HasLastRequest_;
}

grpc::Status TBlockingIamTokenService::Create(
    grpc::ServerContext* context,
    const yandex::cloud::iam::v1::CreateIamTokenRequest*,
    yandex::cloud::iam::v1::CreateIamTokenResponse* response)
{
    RpcEntered_.store(true);

    std::unique_lock lock(Mutex_);
    ReleasedCv_.wait(lock, [this, context] {
        return Released_ || ShuttingDown_ || context->IsCancelled();
    });

    if (context->IsCancelled() || ShuttingDown_) {
        return grpc::Status::CANCELLED;
    }
    response->set_iam_token("released-token");
    response->mutable_expires_at()->set_seconds(4102444800);
    response->mutable_expires_at()->set_nanos(0);
    return grpc::Status::OK;
}

void TBlockingIamTokenService::Release() {
    {
        std::lock_guard lock(Mutex_);
        Released_ = true;
    }
    ReleasedCv_.notify_all();
}

void TBlockingIamTokenService::Shutdown() {
    {
        std::lock_guard lock(Mutex_);
        ShuttingDown_ = true;
        Released_ = true;
    }
    ReleasedCv_.notify_all();
}

bool TBlockingIamTokenService::WaitUntilRpcEntered(std::chrono::milliseconds timeout) const {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (RpcEntered_.load()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return RpcEntered_.load();
}

TIamGrpcServer::TIamGrpcServer(grpc::Service* service)
    : Service_(service)
{}

bool TIamGrpcServer::Start() {
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

std::string TIamGrpcServer::Endpoint() const {
    return "127.0.0.1:" + std::to_string(Port_);
}

void TIamGrpcServer::Stop() {
    if (auto* blocking = dynamic_cast<TBlockingIamTokenService*>(Service_)) {
        blocking->Shutdown();
    }
    if (Server_) {
        Server_->Shutdown();
    }
    if (WaitThread_.joinable()) {
        WaitThread_.join();
    }
    Server_.reset();
}

TIamGrpcServer::~TIamGrpcServer() {
    Stop();
}

TIamOAuth MakeOAuthParams(const std::string& endpoint, const std::string& oauthToken) {
    TIamOAuth params;
    params.Endpoint = endpoint;
    params.OAuthToken = oauthToken;
    params.EnableSsl = false;
    params.RefreshPeriod = TDuration::Hours(1);
    params.RequestTimeout = TDuration::Seconds(5);
    return params;
}

TIamJwtParams MakeJwtParams(const std::string& endpoint) {
    TIamJwtParams params;
    params.Endpoint = endpoint;
    params.EnableSsl = false;
    params.RefreshPeriod = TDuration::Hours(1);
    params.RequestTimeout = TDuration::Seconds(5);
    params.JwtParams.AccountId = "unit-test-account";
    params.JwtParams.KeyId = "unit-test-key";
    params.JwtParams.PrivKey = TestRSAPrivateKey;
    params.JwtParams.PubKey = TestRSAPublicKey;
    return params;
}

} // namespace NYdb::NTest

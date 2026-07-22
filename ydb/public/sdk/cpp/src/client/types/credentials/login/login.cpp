#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/internal/plain_status/status.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/grpc_connections/grpc_connections.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/core_facility/core_facility.h>
#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>

#include <util/string/cast.h>

#include <jwt-cpp/jwt.h>

#include <exception>

using namespace std::chrono_literals;

namespace NYdb::inline Dev {

namespace {

// copy-pasted from <robot/library/utils/time_convert.h>
template<typename Rep, typename Period>
constexpr ui64 ToMicroseconds(std::chrono::duration<Rep, Period> value) {
    return std::chrono::duration_cast<std::chrono::microseconds>(value).count();
}

template<typename Clock, typename Duration>
constexpr TInstant ToInstant(std::chrono::time_point<Clock, Duration> value) {
    return TInstant::MicroSeconds(ToMicroseconds(value.time_since_epoch()));
}

std::chrono::system_clock::time_point GetTokenExpiresAt(const std::string& token) {
    try {
        jwt::decoded_jwt decoded_token = jwt::decode(token);
        return decoded_token.get_expires_at();
    }
    catch (...) {
    }
    return {};
}

bool IsRetryable(EStatus status) {
    return status == EStatus::INTERNAL_ERROR || status == EStatus::ABORTED || status == EStatus::UNAVAILABLE ||
        status == EStatus::OVERLOADED || status == EStatus::GENERIC_ERROR || status == EStatus::TIMEOUT ||
        status == EStatus::CANCELLED || status == EStatus::UNDETERMINED || status == EStatus::SESSION_BUSY ||
        status == EStatus::TRANSPORT_UNAVAILABLE || status == EStatus::CLIENT_RESOURCE_EXHAUSTED ||
        status == EStatus::CLIENT_DEADLINE_EXCEEDED || status == EStatus::CLIENT_INTERNAL_ERROR ||
        status == EStatus::CLIENT_CANCELLED || status == EStatus::CLIENT_DISCOVERY_FAILED ||
        status == EStatus::CLIENT_LIMITS_REACHED;
}
}

class TLoginCredentialsProvider : public ICredentialsProvider, public std::enable_shared_from_this<TLoginCredentialsProvider> {
public:
    TLoginCredentialsProvider(std::weak_ptr<ICoreFacility> facility, TLoginCredentialsParams params);
    std::string GetAuthInfo() const override;
    NThreading::TFuture<std::string> GetAuthInfoAsync() const override;
    bool IsValid() const override;
    void Start();
    ~TLoginCredentialsProvider() { Fail("Login credentials provider stopped"); }

private:
    bool OnPeriodicTick(EStatus status);
    void RequestToken();
    void FinishRequest(Ydb::Auth::LoginResponse* response, TPlainStatus status, bool facilityAvailable);
    void Fail(std::string error);
    static std::string GetError(const TPlainStatus& status, const Ydb::Auth::LoginResponse& response);

    std::weak_ptr<ICoreFacility> Facility_;
    TLoginCredentialsParams Params_;
    mutable std::mutex Mutex_;
    TInstant TokenRequestAt_;
    bool Requesting_ = false;
    bool Stopped_ = false;
    mutable NThreading::TPromise<std::string> AuthInfo_;
};

TLoginCredentialsProvider::TLoginCredentialsProvider(std::weak_ptr<ICoreFacility> facility, TLoginCredentialsParams params)
    : Facility_(facility)
    , Params_(std::move(params))
    , AuthInfo_(NThreading::NewPromise<std::string>())
{}

void TLoginCredentialsProvider::Start() {
    auto facility = Facility_.lock();
    if (!facility) {
        Fail("Login credentials provider response facility is not available");
        return;
    }
    {
        std::lock_guard lock(Mutex_);
        Requesting_ = true;
    }
    try {
        facility->AddPeriodicTask([weak = weak_from_this()](NYdb::NIssue::TIssues&&, EStatus status) {
            if (auto self = weak.lock()) {
                return self->OnPeriodicTick(status);
            }
            return false;
        }, 1s);
    } catch (...) {
        Fail(CurrentExceptionMessage());
        return;
    }
    RequestToken();
}

bool TLoginCredentialsProvider::OnPeriodicTick(EStatus status) {
    if (status != EStatus::SUCCESS) {
        Fail("Login credentials provider periodic task failed");
        return false;
    }
    {
        std::lock_guard lock(Mutex_);
        if (Stopped_) {
            return false;
        }
        if (Requesting_ || TInstant::Now() < TokenRequestAt_) {
            return true;
        }
        if (AuthInfo_.GetFuture().IsReady()) {
            AuthInfo_ = NThreading::NewPromise<std::string>();
        }
        Requesting_ = true;
    }
    RequestToken();
    return true;
}

bool TLoginCredentialsProvider::IsValid() const {
    return true;
}

std::string TLoginCredentialsProvider::GetAuthInfo() const {
    return GetAuthInfoAsync().GetValueSync();
}

NThreading::TFuture<std::string> TLoginCredentialsProvider::GetAuthInfoAsync() const {
    std::lock_guard lock(Mutex_);
    return AuthInfo_.GetFuture();
}

void TLoginCredentialsProvider::RequestToken() {
    auto strongFacility = Facility_.lock();
    if (strongFacility) {
        auto responseCb = [facility = Facility_, weak = weak_from_this()](Ydb::Auth::LoginResponse* resp, TPlainStatus status) {
            if (auto self = weak.lock()) {
                self->FinishRequest(resp, std::move(status), !facility.expired());
            }
        };

        Ydb::Auth::LoginRequest request;
        request.set_user(TStringType{Params_.User});
        request.set_password(TStringType{Params_.Password});
        TRpcRequestSettings rpcSettings;
        rpcSettings.Deadline = TDeadline::AfterDuration(60s);

        try {
            TGRpcConnectionsImpl::RunOnDiscoveryEndpoint<Ydb::Auth::V1::AuthService, Ydb::Auth::LoginRequest, Ydb::Auth::LoginResponse>(
                strongFacility, std::move(request), std::move(responseCb), &Ydb::Auth::V1::AuthService::Stub::AsyncLogin,
                rpcSettings);
        } catch (...) {
            Fail(CurrentExceptionMessage());
        }
    } else {
        FinishRequest(nullptr, {}, false);
    }
}

void TLoginCredentialsProvider::FinishRequest(
    Ydb::Auth::LoginResponse* response,
    TPlainStatus status,
    bool facilityAvailable)
{
    if (!facilityAvailable) {
        Fail("Login credentials provider response facility is not available");
        return;
    }

    Ydb::Auth::LoginResponse emptyResponse;
    const auto& responseValue = response ? *response : emptyResponse;
    const auto operationStatus = static_cast<EStatus>(responseValue.operation().status());
    if (!status.Ok() || operationStatus != EStatus::SUCCESS) {
        if (IsRetryable(status.Ok() ? operationStatus : status.Status)) {
            std::lock_guard lock(Mutex_);
            Requesting_ = false;
            TokenRequestAt_ = TInstant::Now() + TDuration::Seconds(1);
            return;
        }
        Fail(GetError(status, responseValue));
        return;
    }

    Ydb::Auth::LoginResult result;
    if (!responseValue.operation().result().UnpackTo(&result) || result.token().empty()) {
        Fail("Login service returned an empty token");
        return;
    }
    auto token = result.token();
    const auto now = TInstant::Now();
    NThreading::TPromise<std::string> promise;
    {
        std::lock_guard lock(Mutex_);
        Requesting_ = false;
        TokenRequestAt_ = now + (ToInstant(GetTokenExpiresAt(token)) - now) / 2;
        promise = AuthInfo_;
    }
    promise.TrySetValue(std::move(token));
}

void TLoginCredentialsProvider::Fail(std::string error) {
    NThreading::TPromise<std::string> promise;
    {
        std::lock_guard lock(Mutex_);
        Stopped_ = true;
        Requesting_ = false;
        promise = AuthInfo_;
    }
    promise.TrySetException(std::make_exception_ptr(yexception() << error));
}

std::string TLoginCredentialsProvider::GetError(
    const TPlainStatus& status,
    const Ydb::Auth::LoginResponse& response)
{
    if (status.Ok()) {
        if (response.operation().issues_size() > 0) {
            return response.operation().issues(0).message();
        }
        return Ydb::StatusIds_StatusCode_Name(response.operation().status());
    }
    TStringBuilder str;
    str << "Couldn't get token for provided credentials from " << status.Endpoint
        << " with status " << status.Status << ".";
    for (const auto& issue : status.Issues) {
            str << Endl << "Issue: " << issue;
    }
    return str;
}

class TLoginCredentialsProviderFactory : public ICredentialsProviderFactory {
public:
    TLoginCredentialsProviderFactory(TLoginCredentialsParams params);
    virtual std::shared_ptr<ICredentialsProvider> CreateProvider() const override;
    virtual std::shared_ptr<ICredentialsProvider> CreateProvider(std::weak_ptr<ICoreFacility> facility) const override;

private:
    TLoginCredentialsParams Params_;
};

TLoginCredentialsProviderFactory::TLoginCredentialsProviderFactory(TLoginCredentialsParams params)
    : Params_(std::move(params))
{
}

std::shared_ptr<ICredentialsProvider> TLoginCredentialsProviderFactory::CreateProvider() const {
    ythrow yexception() << "Not supported";
}

std::shared_ptr<ICredentialsProvider> TLoginCredentialsProviderFactory::CreateProvider(std::weak_ptr<ICoreFacility> facility) const {
    auto provider = std::make_shared<TLoginCredentialsProvider>(std::move(facility), Params_);
    provider->Start();
    return provider;
}

std::shared_ptr<ICredentialsProviderFactory> CreateLoginCredentialsProviderFactory(TLoginCredentialsParams params) {
    return std::make_shared<TLoginCredentialsProviderFactory>(std::move(params));
}

} // namespace NYdb

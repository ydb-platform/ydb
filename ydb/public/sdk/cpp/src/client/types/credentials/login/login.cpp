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
}

class TLoginCredentialsProvider : public ICredentialsProvider {
public:
    TLoginCredentialsProvider(std::weak_ptr<ICoreFacility> facility, TLoginCredentialsParams params);
    virtual std::string GetAuthInfo() const override;
    virtual bool IsValid() const override;
    NThreading::TFuture<void> PrepareTokenAsync();

private:
    void PrepareToken();
    void RequestToken();
    void FinishRequest(Ydb::Auth::LoginResponse* response, TPlainStatus status, bool facilityAvailable);
    bool IsOk() const;
    void ParseToken();
    std::string GetToken() const;
    std::string GetError() const;
    std::string GetTokenOrError() const;

    enum class EState {
        Empty,
        Requesting,
        Done,
    };

    std::weak_ptr<ICoreFacility> Facility_;
    TLoginCredentialsParams Params_;
    EState State_ = EState::Empty;
    std::mutex Mutex_;
    std::atomic<ui64> TokenReceived_ = 1;
    std::atomic<ui64> TokenParsed_ = 0;
    std::optional<std::string> Token_;
    std::optional<std::string> Error_;
    TInstant TokenExpireAt_;
    TInstant TokenRequestAt_;
    TPlainStatus Status_;
    Ydb::Auth::LoginResponse Response_;
    NThreading::TPromise<void> TokenReadyPromise_;
};

TLoginCredentialsProvider::TLoginCredentialsProvider(std::weak_ptr<ICoreFacility> facility, TLoginCredentialsParams params)
    : Facility_(facility)
    , Params_(std::move(params))
    , TokenReadyPromise_(NThreading::NewPromise<void>())
{
    auto strongFacility = facility.lock();
    if (strongFacility) {
        auto periodicTask = [facility, this](NYdb::NIssue::TIssues&&, EStatus status) -> bool {
            if (status != EStatus::SUCCESS) {
                return false;
            }

            auto strongFacility = facility.lock();
            if (!strongFacility) {
                return false;
            }

            if (!TokenRequestAt_) {
                return true;
            }

            if (TInstant::Now() >= TokenRequestAt_) {
                RequestToken();
            }

            return true;
        };
        strongFacility->AddPeriodicTask(std::move(periodicTask), 1min);
    }
}

bool TLoginCredentialsProvider::IsValid() const {
    return true;
}

std::string TLoginCredentialsProvider::GetAuthInfo() const {
    if (TokenParsed_ == TokenReceived_) {
        return GetTokenOrError();
    } else {
        const_cast<TLoginCredentialsProvider*>(this)->PrepareToken(); // will block here
        return GetTokenOrError();
    }
}

void TLoginCredentialsProvider::RequestToken() {
    auto strongFacility = Facility_.lock();
    if (strongFacility) {
        TokenRequestAt_ = {};

        auto responseCb = [facility = Facility_, this](Ydb::Auth::LoginResponse* resp, TPlainStatus status) {
            auto strongFacility = facility.lock();
            FinishRequest(resp, std::move(status), static_cast<bool>(strongFacility));
        };

        Ydb::Auth::LoginRequest request;
        request.set_user(TStringType{Params_.User});
        request.set_password(TStringType{Params_.Password});
        TRpcRequestSettings rpcSettings;
        rpcSettings.Deadline = TDeadline::AfterDuration(60s);

        TGRpcConnectionsImpl::RunOnDiscoveryEndpoint<Ydb::Auth::V1::AuthService, Ydb::Auth::LoginRequest, Ydb::Auth::LoginResponse>(
            strongFacility, std::move(request), std::move(responseCb), &Ydb::Auth::V1::AuthService::Stub::AsyncLogin,
            rpcSettings);
    } else {
        FinishRequest(nullptr, {}, false);
    }
}

void TLoginCredentialsProvider::FinishRequest(
    Ydb::Auth::LoginResponse* response,
    TPlainStatus status,
    bool facilityAvailable)
{
    std::optional<std::string> error;
    {
        std::lock_guard lock(Mutex_);
        State_ = EState::Done;
        ++TokenReceived_;
        if (facilityAvailable) {
            Status_ = std::move(status);
            if (response) {
                Response_ = std::move(*response);
            }
            ParseToken();
        } else {
            Token_.reset();
            Error_ = "Login credentials provider response facility is not available";
            TokenParsed_ = TokenReceived_.load();
        }
        error = Error_;
    }
    if (error) {
        TokenReadyPromise_.TrySetException(std::make_exception_ptr(yexception() << *error));
    } else {
        TokenReadyPromise_.TrySetValue();
    }
}

void TLoginCredentialsProvider::PrepareToken() {
    PrepareTokenAsync().Wait();
    std::lock_guard lock(Mutex_);
    ParseToken();
}

NThreading::TFuture<void> TLoginCredentialsProvider::PrepareTokenAsync() {
    bool requestToken = false;
    auto future = TokenReadyPromise_.GetFuture();
    {
        std::unique_lock<std::mutex> lock(Mutex_);
        if (State_ == EState::Empty) {
            State_ = EState::Requesting;
            requestToken = true;
        }
    }
    if (requestToken) {
        RequestToken();
    }
    return future;
}

bool TLoginCredentialsProvider::IsOk() const {
    return State_ == EState::Done
        && Status_.Ok()
        && Response_.operation().status() == Ydb::StatusIds::SUCCESS;
}

void TLoginCredentialsProvider::ParseToken() { // works under mutex
    if (TokenParsed_ != TokenReceived_) {
        if (IsOk()) {
            Token_ = GetToken();
            Error_.reset();
            TInstant now = TInstant::Now();
            TokenExpireAt_ = ToInstant(GetTokenExpiresAt(Token_.value()));
            TokenRequestAt_ = now + TDuration::Minutes((TokenExpireAt_ - now).Minutes() / 2);
        } else {
            Token_.reset();
            Error_ = GetError();
        }
        TokenParsed_ = TokenReceived_.load();
    }
}

std::string TLoginCredentialsProvider::GetToken() const {
    Ydb::Auth::LoginResult result;
    Response_.operation().result().UnpackTo(&result);
    return result.token();
}

std::string TLoginCredentialsProvider::GetError() const {
    if (Status_.Ok()) {
        if (Response_.operation().issues_size() > 0) {
            return Response_.operation().issues(0).message();
        } else {
            return Ydb::StatusIds_StatusCode_Name(Response_.operation().status());
        }
    } else {
        TStringBuilder str;
        str << "Couldn't get token for provided credentials from " << Status_.Endpoint
            << " with status " << Status_.Status << ".";
        for (const auto& issue : Status_.Issues) {
            str << Endl << "Issue: " << issue;
        }
        return str;
    }
}

std::string TLoginCredentialsProvider::GetTokenOrError() const {
    if (Token_) {
        return Token_.value();
    }
    if (Error_) {
        ythrow yexception() << Error_.value();
    }
    ythrow yexception() << "Wrong state of credentials provider";
}

class TLoginCredentialsProviderFactory : public ICredentialsProviderFactory {
public:
    TLoginCredentialsProviderFactory(TLoginCredentialsParams params);
    virtual std::shared_ptr<ICredentialsProvider> CreateProvider() const override;
    virtual std::shared_ptr<ICredentialsProvider> CreateProvider(std::weak_ptr<ICoreFacility> facility) const override;
    virtual NThreading::TFuture<TCredentialsProviderPtr> CreateProviderAsync(std::weak_ptr<ICoreFacility> facility) const override;

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
    return std::make_shared<TLoginCredentialsProvider>(std::move(facility), Params_);
}

NThreading::TFuture<TCredentialsProviderPtr> TLoginCredentialsProviderFactory::CreateProviderAsync(std::weak_ptr<ICoreFacility> facility) const {
    auto provider = std::make_shared<TLoginCredentialsProvider>(std::move(facility), Params_);
    TCredentialsProviderPtr result = provider;
    return provider->PrepareTokenAsync().Return(std::move(result));
}

std::shared_ptr<ICredentialsProviderFactory> CreateLoginCredentialsProviderFactory(TLoginCredentialsParams params) {
    return std::make_shared<TLoginCredentialsProviderFactory>(std::move(params));
}

} // namespace NYdb

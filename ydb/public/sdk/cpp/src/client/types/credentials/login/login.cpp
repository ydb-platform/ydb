#include <ydb-cpp-sdk/client/types/credentials/credentials.h>
#define INCLUDE_YDB_INTERNAL_H
#include <src/client/impl/ydb_internal/plain_status/status.h>
#include <src/client/impl/ydb_internal/grpc_connections/grpc_connections.h>
#include <src/client/types/core_facility/core_facility.h>
#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>

#include <util/string/cast.h>

#include <jwt-cpp/jwt.h>

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

private:
    void PrepareToken();
    void RequestToken();
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
    std::condition_variable Notify_;
    std::atomic<ui64> TokenReceived_ = 1;
    std::atomic<ui64> TokenParsed_ = 0;
    std::optional<std::string> Token_;
    std::optional<std::string> Error_;
    TInstant TokenExpireAt_;
    TInstant TokenRequestAt_;
    TPlainStatus Status_;
    Ydb::Auth::LoginResponse Response_;
};

TLoginCredentialsProvider::TLoginCredentialsProvider(std::weak_ptr<ICoreFacility> facility, TLoginCredentialsParams params)
    : Facility_(facility)
    , Params_(std::move(params))
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
        strongFacility->AddPeriodicTask(std::move(periodicTask), TDuration::Minutes(1));
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
            if (strongFacility) {
                std::lock_guard<std::mutex> lock(Mutex_);
                Status_ = std::move(status);
                if (resp != nullptr) {
                    Response_ = std::move(*resp);
                }
                State_ = EState::Done;
                TokenReceived_++;
            }
            Notify_.notify_all();
        };

        Ydb::Auth::LoginRequest request;
        request.set_user(TStringType{Params_.User});
        request.set_password(TStringType{Params_.Password});
        TRpcRequestSettings rpcSettings;
        rpcSettings.ClientTimeout = TDuration::Seconds(60);

        TGRpcConnectionsImpl::RunOnDiscoveryEndpoint<Ydb::Auth::V1::AuthService, Ydb::Auth::LoginRequest, Ydb::Auth::LoginResponse>(
            strongFacility, std::move(request), std::move(responseCb), &Ydb::Auth::V1::AuthService::Stub::AsyncLogin,
            rpcSettings);
    }
}

void TLoginCredentialsProvider::PrepareToken() {
    std::unique_lock<std::mutex> lock(Mutex_);
    switch (State_) {
        case EState::Empty:
            State_ = EState::Requesting;
            RequestToken();
            [[fallthrough]];
        case EState::Requesting:
            Notify_.wait(lock, [&]{
                return State_ == EState::Done;
            });
            [[fallthrough]];
        case EState::Done:
            ParseToken();
            break;
    }
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

std::shared_ptr<ICredentialsProviderFactory> CreateLoginCredentialsProviderFactory(TLoginCredentialsParams params) {
    return std::make_shared<TLoginCredentialsProviderFactory>(std::move(params));
}

} // namespace NYdb

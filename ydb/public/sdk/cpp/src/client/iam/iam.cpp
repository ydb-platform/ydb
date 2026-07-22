#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/iam.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common/generic_provider.h>

#include <ydb/public/api/client/yc_public/iam/iam_token_service.pb.h>
#include <ydb/public/api/client/yc_public/iam/iam_token_service.grpc.pb.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/http/simple/http_client.h>

#include <exception>
#include <mutex>

using namespace yandex::cloud::iam::v1;

namespace NYdb::inline Dev {

class TIAMCredentialsProvider : public ICredentialsProvider, public std::enable_shared_from_this<TIAMCredentialsProvider> {
public:
    TIAMCredentialsProvider(const TIamHost& params, std::weak_ptr<ICoreFacility> facility)
        : HttpClient_(TSimpleHttpClient(TString(params.Host), params.Port))
        , Request_("/computeMetadata/v1/instance/service-accounts/default/token")
        , NextTicketUpdate_(TInstant::Zero())
        , RefreshPeriod_(params.RefreshPeriod)
        , Facility_(std::move(facility))
        , AuthInfo_(NThreading::NewPromise<std::string>())
    {}

    void Start() {
        auto facility = Facility_.lock();
        if (!facility) {
            Fail("IAM-token provider response facility is not available");
            return;
        }
        try {
            facility->AddPeriodicTask([weak = weak_from_this()](NYdb::NIssue::TIssues&&, EStatus status) {
                if (auto self = weak.lock()) {
                    return self->OnPeriodicTick(status);
                }
                return false;
            }, PERIODIC_TICK);
        } catch (...) {
            Fail(CurrentExceptionMessage());
        }
    }

    std::string GetAuthInfo() const override {
        return GetAuthInfoAsync().GetValueSync();
    }

    NThreading::TFuture<std::string> GetAuthInfoAsync() const override {
        std::lock_guard lock(Lock_);
        return AuthInfo_.GetFuture();
    }

    bool IsValid() const override {
        return true;
    }

    ~TIAMCredentialsProvider() { Fail("IAM-token provider stopped"); }

private:
    TSimpleHttpClient HttpClient_;
    std::string Request_;
    mutable std::mutex Lock_;
    mutable TInstant NextTicketUpdate_;
    TDuration RefreshPeriod_;
    std::weak_ptr<ICoreFacility> Facility_;
    mutable NThreading::TPromise<std::string> AuthInfo_;
    mutable bool Stopped_ = false;

    void Fail(std::string error) const {
        NThreading::TPromise<std::string> promise;
        {
            std::lock_guard lock(Lock_);
            Stopped_ = true;
            promise = AuthInfo_;
        }
        promise.TrySetException(std::make_exception_ptr(yexception() << error));
    }

    bool OnPeriodicTick(EStatus status) const {
        if (status != EStatus::SUCCESS) {
            Fail("IAM-token provider periodic task failed");
            return false;
        }

        NThreading::TPromise<std::string> promise;
        {
            std::lock_guard lock(Lock_);
            if (Stopped_) {
                return false;
            }
            if (TInstant::Now() < NextTicketUpdate_) {
                return true;
            }
            if (AuthInfo_.GetFuture().IsReady()) {
                AuthInfo_ = NThreading::NewPromise<std::string>();
            }
            promise = AuthInfo_;
        }

        try {
            auto [ticket, nextUpdate] = GetTicket();
            {
                std::lock_guard lock(Lock_);
                NextTicketUpdate_ = nextUpdate;
            }
            promise.TrySetValue(std::move(ticket));
        } catch (...) {
            const auto error = std::current_exception();
            if (!IsRetryable(error)) {
                Fail(CurrentExceptionMessage());
                return false;
            }
            std::lock_guard lock(Lock_);
            NextTicketUpdate_ = TInstant::Now() + std::min(RefreshPeriod_, TDuration::Seconds(10));
        }
        return true;
    }

    static bool IsRetryable(const std::exception_ptr& error) {
        try {
            std::rethrow_exception(error);
        } catch (const THttpRequestException& e) {
            const int code = e.GetStatusCode();
            return code == 0 || code == HTTP_REQUEST_TIME_OUT || code == HTTP_AUTHENTICATION_TIMEOUT ||
                code == HTTP_TOO_MANY_REQUESTS || (code >= 500 && code < 600);
        } catch (const TSystemError&) {
            return true;
        } catch (...) {
            return false;
        }
    }

    std::pair<std::string, TInstant> GetTicket() const {
        TStringStream out;
        TSimpleHttpClient::THeaders headers;
        headers["Metadata-Flavor"] = "Google";
        HttpClient_.DoGet(Request_, &out, headers);
        NJson::TJsonValue resp;
        NJson::ReadJsonTree(&out, &resp, true);

        auto respMap = resp.GetMap();
        std::string ticket;
        if (auto it = respMap.find("access_token"); it == respMap.end())
            ythrow yexception() << "Result doesn't contain access_token";
        else if (ticket = it->second.GetStringSafe(); ticket.empty())
            ythrow yexception() << "Got empty ticket";

        const auto now = TInstant::Now();
        TDuration expiresIn;
        if (auto it = respMap.find("expires_in"); it != respMap.end()) {
            const auto seconds = it->second.GetUInteger();
            if (seconds > 0) {
                expiresIn = TDuration::Seconds(seconds);
            }
        } else if (auto it = respMap.find("expiry"); it != respMap.end()) {
            TInstant expiry;
            if (TInstant::TryParseIso8601(it->second.GetStringSafe(), expiry) && expiry > now) {
                expiresIn = expiry - now;
            }
        }
        const auto interval = expiresIn > TDuration::Zero()
            ? std::max(std::min(expiresIn / 2, RefreshPeriod_), TDuration::MilliSeconds(100))
            : std::min(RefreshPeriod_, TDuration::Minutes(30));
        return {std::move(ticket), now + interval};
    }
};

class TIamCredentialsProviderFactory : public ICredentialsProviderFactory {
public:
    TIamCredentialsProviderFactory(const TIamHost& params): Params_(params) {}

    TCredentialsProviderPtr CreateProvider() const final {
        auto facility = CreateSimpleCoreFacility();
        auto provider = CreateProvider(facility);
        return std::make_shared<TOwningFacilityCredentialsProvider>(
            std::move(facility), std::move(provider));
    }

    TCredentialsProviderPtr CreateProvider(std::weak_ptr<ICoreFacility> facility) const final {
        auto provider = std::make_shared<TIAMCredentialsProvider>(Params_, std::move(facility));
        provider->Start();
        return provider;
    }
private:
    TIamHost Params_;
};

/// Acquire an IAM token using a local metadata service on a virtual machine.
TCredentialsProviderFactoryPtr CreateIamCredentialsProviderFactory(const TIamHost& params ) {
    return std::make_shared<TIamCredentialsProviderFactory>(params);
}

TCredentialsProviderFactoryPtr CreateIamJwtFileCredentialsProviderFactory(const TIamJwtFilename& params) {
    TIamJwtParams jwtParams = { params, ReadJwtKeyFile(params.JwtFilename) };
    return std::make_shared<TIamJwtCredentialsProviderFactory<CreateIamTokenRequest,
                                                              CreateIamTokenResponse,
                                                              IamTokenService>>(std::move(jwtParams));
}

TCredentialsProviderFactoryPtr CreateIamJwtParamsCredentialsProviderFactory(const TIamJwtContent& params) {
    TIamJwtParams jwtParams = { params, ParseJwtParams(params.JwtContent) };
    return std::make_shared<TIamJwtCredentialsProviderFactory<CreateIamTokenRequest,
                                                              CreateIamTokenResponse,
                                                              IamTokenService>>(std::move(jwtParams));
}

TCredentialsProviderFactoryPtr CreateIamOAuthCredentialsProviderFactory(const TIamOAuth& params) {
    return std::make_shared<TIamOAuthCredentialsProviderFactory<CreateIamTokenRequest,
                                                                CreateIamTokenResponse,
                                                                IamTokenService>>(params);
}

}

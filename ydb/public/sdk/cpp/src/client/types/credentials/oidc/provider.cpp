#include "provider.h"

#include <util/string/builder.h>

#include <algorithm>

namespace NYdb::inline Dev::NOidc {

    TOidcProvider::TOidcProvider(
        std::string issuer,
        std::shared_ptr<IOidcTokenCache> tokenCache,
        std::weak_ptr<ICoreFacility> facility)
        : Protocol_(std::move(issuer))
        , TokenCache_(std::move(tokenCache))
        , Facility_(std::move(facility))
        , AuthInfo_(NThreading::NewPromise<std::string>())
    {
    }

    TOidcProvider::~TOidcProvider() {
        Fail(std::make_exception_ptr(TOidcException("OIDC credentials provider stopped")));
    }

    std::string TOidcProvider::GetAuthInfo() const {
        return GetAuthInfoAsync().GetValueSync();
    }

    NThreading::TFuture<std::string> TOidcProvider::GetAuthInfoAsync() const {
        with_lock (Lock_) {
            if (Tokens_.AccessToken.IsUsable(TInstant::Now())) {
                return NThreading::MakeFuture(BuildAuthInfo(Tokens_.AccessToken.Token));
            }
            return AuthInfo_.GetFuture();
        }
    }

    bool TOidcProvider::IsValid() const {
        with_lock (Lock_) {
            return !Stopped_;
        }
    }

    void TOidcProvider::Start() {
        auto facility = Facility_.lock();
        if (!facility) {
            return Fail(std::make_exception_ptr(TOidcException("OIDC credentials provider response facility is not available")));
        }

        try {
            facility->AddPeriodicTask([weak = weak_from_this()](NYdb::NIssue::TIssues&&, EStatus status) {
                if (auto self = weak.lock()) {
                    return self->OnPeriodicTick(status);
                }
                return false;
            }, std::chrono::microseconds(PERIODIC_TICK.MicroSeconds()));
            Bootstrap();
        } catch (...) {
            HandleRefreshError(std::current_exception());
        }
    }

    TOidcProtocolClient& TOidcProvider::Protocol() {
        return Protocol_;
    }

    std::optional<TOidcTokenSet> TOidcProvider::ReadCache() const {
        return (TokenCache_ != nullptr) ? TokenCache_->Read() : std::nullopt;
    }

    TOidcTokenSet TOidcProvider::GetTokens() const {
        with_lock (Lock_) {
            return Tokens_;
        }
    }

    void TOidcProvider::Restore(TOidcTokenSet tokens) {
        with_lock (Lock_) {
            Tokens_ = std::move(tokens);
        }
    }

    void TOidcProvider::Publish(TOidcTokenSet tokens) {
        const TInstant now = TInstant::Now();
        if (!tokens.AccessToken.IsUsable(now)) {
            throw TOidcException("OIDC credentials provider cannot publish an expired access token");
        }

        if (TokenCache_) {
            TokenCache_->Write(tokens);
        }

        const std::string authInfo = BuildAuthInfo(tokens.AccessToken.Token);
        NThreading::TPromise<std::string> promise;
        with_lock (Lock_) {
            Tokens_ = std::move(tokens);
            NextRefresh_ = RefreshAt(Tokens_.AccessToken);
            Requesting_ = false;
            promise = AuthInfo_;
        }
        promise.TrySetValue(authInfo);
    }

    void TOidcProvider::Schedule(TInstant when) {
        with_lock (Lock_) {
            NextRefresh_ = when;
            Requesting_ = false;
        }
    }

    void TOidcProvider::ScheduleAfter(TDuration delay) {
        Schedule(TInstant::Now() + delay);
    }

    void TOidcProvider::StopRefreshing() {
        Schedule(TInstant::Max());
    }

    TInstant TOidcProvider::RefreshAt(const TOidcToken& accessToken) {
        if (!accessToken.ExpiresAt.has_value()) {
            return TInstant::Max();
        }
        const TInstant now = TInstant::Now();
        if (accessToken.ExpiresAt.value() <= now) {
            return now;
        }
        const TDuration remaining = accessToken.ExpiresAt.value() - now;
        const TDuration delay = remaining / 2 > REFRESH_SKEW
                                    ? remaining / 2
                                    : std::min(remaining, TDuration::Seconds(1));
        return now + delay;
    }

    bool TOidcProvider::OnPeriodicTick(EStatus status) {
        if (status != EStatus::SUCCESS) {
            Fail(std::make_exception_ptr(TOidcException("OIDC credentials provider periodic task failed")));
            return false;
        }

        with_lock (Lock_) {
            if (Stopped_) {
                return false;
            }
            if (Requesting_ || TInstant::Now() < NextRefresh_) {
                return true;
            }
            if (AuthInfo_.GetFuture().IsReady() && !Tokens_.AccessToken.IsUsable(TInstant::Now())) {
                AuthInfo_ = NThreading::NewPromise<std::string>();
            }
            Requesting_ = true;
        }

        try {
            Refresh();
        } catch (...) {
            HandleRefreshError(std::current_exception());
        }
        return IsValid();
    }

    void TOidcProvider::Fail(std::exception_ptr error) {
        NThreading::TPromise<std::string> promise;
        with_lock (Lock_) {
            if (Stopped_) {
                return;
            }
            Stopped_ = true;
            Requesting_ = false;
            promise = AuthInfo_;
        }
        promise.TrySetException(std::move(error));
    }

    void TOidcProvider::HandleRefreshError(std::exception_ptr error) {
        bool retryable = false;
        try {
            std::rethrow_exception(error);
        } catch (const std::exception& exception) {
            retryable = TOidcProtocolClient::IsRetryable(exception);
        } catch (...) {
            return Fail(std::move(error));
        }

        if (retryable) {
            return ScheduleAfter(RETRY_DELAY);
        }
        Fail(std::move(error));
    }

    std::string TOidcProvider::BuildAuthInfo(const std::string& accessToken) {
        return TStringBuilder() << "Bearer " << accessToken;
    }

} // namespace NYdb::inline Dev::NOidc

#pragma once

#include "types.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/time/time.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/core_facility/core_facility.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/grpc_common/constants.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/type_switcher.h>

#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <format>
#include <functional>
#include <string>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <utility>

namespace NYdb::inline Dev {

constexpr std::chrono::milliseconds BACKOFF_START{50};
constexpr std::chrono::milliseconds BACKOFF_MAX{10000};
constexpr std::chrono::milliseconds PERIODIC_TICK{100};
constexpr std::chrono::milliseconds MINIMUM_REFRESH_INTERVAL{100};

// This file contains internal generic implementation of IAM credentials providers.
// DO NOT USE THIS CLASS DIRECTLY. Use specialized factory methods for specific cases.
template<typename TRequest, typename TResponse, typename TService>
class TGrpcIamCredentialsProvider : public ICredentialsProvider {
protected:
    using TRequestFiller = std::function<void(TRequest&)>;
    using TAsyncInterface = typename TService::Stub::async_interface;
    using TAsyncRpc = std::function<void(typename TService::Stub*, grpc::ClientContext*, const TRequest*, TResponse*, std::function<void(grpc::Status)>)>;

    using SysClock = std::chrono::system_clock;
    using SysTimePoint = SysClock::time_point;

private:
    class TImpl : public std::enable_shared_from_this<TGrpcIamCredentialsProvider<TRequest, TResponse, TService>::TImpl> {
    public:
        TImpl(const TIamEndpoint& iamEndpoint,
              const TRequestFiller& requestFiller,
              TAsyncRpc rpc,
              std::weak_ptr<ICoreFacility> responseFacility,
              TCredentialsProviderPtr authTokenProvider)
            : Rpc_(rpc)
            , NextTicketUpdate_(SysTimePoint{})
            , IamEndpoint_(iamEndpoint)
            , RequestFiller_(requestFiller)
            , Context_(std::nullopt)
            , NeedStop_(false)
            , BackoffTimeout_(BACKOFF_START)
            , Lock_()
            , ResponseFacility_(std::move(responseFacility))
            , AuthTokenProvider_(authTokenProvider)
            , AuthInfo_(NThreading::NewPromise<std::string>())
        {
            std::shared_ptr<grpc::ChannelCredentials> creds = nullptr;
            if (IamEndpoint_.EnableSsl) {
                grpc::SslCredentialsOptions opts;
                opts.pem_root_certs = IamEndpoint_.CaCerts;
                creds = grpc::SslCredentials(opts);
            } else {
                creds = grpc::InsecureChannelCredentials();
            }

            grpc::ChannelArguments args;

            args.SetMaxSendMessageSize(NGrpc::DEFAULT_GRPC_MESSAGE_SIZE_LIMIT);
            args.SetMaxReceiveMessageSize(NGrpc::DEFAULT_GRPC_MESSAGE_SIZE_LIMIT);

            Channel_ = grpc::CreateCustomChannel(grpc::string{IamEndpoint_.Endpoint}, creds, args);
            Stub_ = TService::NewStub(Channel_);
        }

        void StartPeriodicTask() {
            auto facility = ResponseFacility_.lock();
            if (!facility) {
                Fail("IAM-token provider response facility is not available");
                return;
            }

            std::weak_ptr<TImpl> weakSelf = TGrpcIamCredentialsProvider<TRequest, TResponse, TService>::TImpl::weak_from_this();
            try {
                facility->AddPeriodicTask(
                    [weakSelf](NYdb::NIssue::TIssues&&, EStatus status) {
                        auto self = weakSelf.lock();
                        if (!self) {
                            return false;
                        }
                        if (status != EStatus::SUCCESS) {
                            self->Fail(TStringBuilder()
                                << "IAM-token provider periodic task failed with status "
                                << static_cast<int>(status));
                            return false;
                        }
                        return self->OnPeriodicTick();
                    },
                    PERIODIC_TICK
                );
            } catch (...) {
                Fail(TStringBuilder()
                    << "Failed to start IAM-token provider periodic task: "
                    << CurrentExceptionMessage());
            }
        }

        NThreading::TFuture<std::string> GetAuthInfoAsync() {
            std::lock_guard guard(Lock_);
            return AuthInfo_.GetFuture();
        }

        void Stop() {
            NThreading::TPromise<std::string> promise;
            {
                std::unique_lock guard(Lock_);
                NeedStop_ = true;
                promise = AuthInfo_;
                if (Context_.has_value()) {
                    Context_->TryCancel();
                }
                ContextReady_.wait(guard, [this]() { return !Context_.has_value(); });
            }
            promise.TrySetException(std::make_exception_ptr(
                yexception() << "IAM-token provider stopped before token was ready"));
            Stub_.reset();
            Channel_.reset();
        }

    private:
        using SysDuration = SysClock::duration;

        void Fail(std::string error) {
            NThreading::TPromise<std::string> promise;
            {
                std::lock_guard guard(Lock_);
                NeedStop_ = true;
                promise = AuthInfo_;
                if (Context_) {
                    Context_->TryCancel();
                }
            }
            promise.TrySetException(std::make_exception_ptr(yexception() << error));
        }

        static SysDuration ToBoundedSysDuration(const TDuration& d) {
            return std::chrono::duration_cast<SysDuration>(TDeadline::SafeDurationCast(d));
        }

        template <typename Rep, typename Period>
        static SysDuration ToBoundedSysDuration(const std::chrono::duration<Rep, Period>& d) {
            return std::chrono::duration_cast<SysDuration>(TDeadline::SafeDurationCast(d));
        }

        static SysTimePoint SafeAddSystemTime(SysTimePoint tp, SysDuration d) {
            if (d > SysDuration::zero()) {
                if (tp > SysClock::time_point::max() - d) {
                    return SysClock::time_point::max();
                }
            } else if (d < SysDuration::zero()) {
                if (SysClock::time_point::min() - d > tp) {
                    return SysClock::time_point::min();
                }
            }
            return tp + d;
        }

        void UpdateTicket() {
            auto response = std::make_shared<TResponse>();

            std::weak_ptr<TImpl> weakSelf = TGrpcIamCredentialsProvider<TRequest, TResponse, TService>::TImpl::weak_from_this();
            std::weak_ptr<ICoreFacility> weakFacility = ResponseFacility_;

            auto cb = [weakSelf, weakFacility, response] (grpc::Status status) mutable {
                auto work = [weakSelf, response, status = std::move(status)]() mutable {
                    if (auto self = weakSelf.lock()) {
                        self->ProcessIamResponse(std::move(status), std::move(*response));
                    }
                };
                auto facility = weakFacility.lock();

                try {
                    if (facility) {
                        facility->PostToResponseQueue(std::move(work));
                        return;
                    }
                } catch (...) {
                }

                if (auto self = weakSelf.lock()) {
                    {
                        std::lock_guard guard(self->Lock_);
                        self->ResetContextImpl();
                    }
                    self->Fail("IAM-token provider response facility is not available");
                }
            };

            TRequest req;

            try {
                RequestFiller_(req);
                Rpc_(Stub_.get(), &*Context_, &req, response.get(), std::move(cb));
            } catch (...) {
                std::lock_guard guard(Lock_);
                ResetContextImpl();
                RescheduleOnFailure();
            }
        }

        bool FillContext(std::unique_lock<std::mutex>& guard) {
            std::optional<std::string> authToken;
            if (AuthTokenProvider_) {
                guard.unlock();
                try {
                    if (!AuthTokenInfo_.Initialized()) {
                        AuthTokenInfo_ = AuthTokenProvider_->GetAuthInfoAsync();
                    }
                    if (!AuthTokenInfo_.IsReady()) {
                        guard.lock();
                        return false;
                    }
                    authToken = AuthTokenInfo_.GetValue();
                    AuthTokenInfo_ = {};
                } catch (...) {
                    AuthTokenInfo_ = {};
                    guard.lock();
                    throw;
                }
                guard.lock();
                if (NeedStop_) {
                    return false;
                }
            }

            auto& context = Context_.emplace();
            const auto deadline = gpr_time_add(
                gpr_now(GPR_CLOCK_MONOTONIC),
                gpr_time_from_micros(IamEndpoint_.RequestTimeout.MicroSeconds(), GPR_TIMESPAN));

            context.set_deadline(deadline);

            if (authToken) {
                context.AddMetadata("authorization", "Bearer " + *authToken);
            }
            return true;
        }

        void ResetContextImpl() {
            Context_.reset();
            ContextReady_.notify_all();
        }

        static std::string FormatSysTimeUtcIsoMicros(SysTimePoint tp) {
            const auto t = std::chrono::time_point_cast<std::chrono::microseconds>(tp);
            const auto secs = std::chrono::floor<std::chrono::seconds>(t);
            const auto frac = std::chrono::duration_cast<std::chrono::microseconds>(t - secs).count();
            return std::format("{:%Y-%m-%dT%H:%M:%S}.{:06}Z", secs, frac);
        }

        bool OnPeriodicTick() {
            std::optional<std::string> terminalError;
            bool updateTicket = false;
            bool authPending = false;
            {
                std::unique_lock guard(Lock_);
                if (NeedStop_) {
                    return false;
                }
                if (Context_.has_value() || SysClock::now() < NextTicketUpdate_) {
                    return true;
                }
                if (AuthInfo_.GetFuture().IsReady()) {
                    AuthInfo_ = NThreading::NewPromise<std::string>();
                }
                try {
                    authPending = !FillContext(guard);
                } catch (...) {
                    terminalError = TStringBuilder()
                        << "Last request error was at " << FormatSysTimeUtcIsoMicros(SysClock::now())
                        << ". Failed to prepare IAM request context: " << CurrentExceptionMessage();
                    ResetContextImpl();
                }
                if (NeedStop_) {
                    ResetContextImpl();
                    return false;
                }
                if (!Context_.has_value()) {
                    if (!authPending && !terminalError) {
                        RescheduleOnFailure();
                    }
                } else {
                    updateTicket = true;
                }
            }
            if (terminalError) {
                Fail(*terminalError);
                return false;
            }
            if (updateTicket) {
                UpdateTicket();
            }
            return true;
        }

        void ProcessIamResponse(grpc::Status&& status, TResponse&& result) {
            std::optional<std::string> token;
            std::optional<std::string> terminalError;
            NThreading::TPromise<std::string> promise;

            {
                std::lock_guard guard(Lock_);

                if (!status.ok()) {
                    const std::string error = TStringBuilder()
                        << "Last request error was at " << FormatSysTimeUtcIsoMicros(SysClock::now())
                        << ". GrpcStatusCode: " << static_cast<int>(status.error_code())
                        << " Message: \"" << status.error_message()
                        << "\" iam-endpoint: \"" << IamEndpoint_.Endpoint << "\"";

                    if (IsRetryable(status.error_code())) {
                        RescheduleOnFailure();
                    } else {
                        terminalError = error;
                    }
                } else if (result.iam_token().empty()) {
                    terminalError = "IAM-token service returned an empty token";
                } else {
                    token = result.iam_token();
                    promise = AuthInfo_;

                    const SysTimePoint expiresAt = SysClock::from_time_t(result.expires_at().seconds());
                    RescheduleOnSuccess(expiresAt);
                }

                ResetContextImpl();
            }

            if (token) {
                promise.TrySetValue(std::move(*token));
            } else if (terminalError) {
                Fail(*terminalError);
            }
        }

        static bool IsRetryable(grpc::StatusCode code) {
            return code == grpc::StatusCode::CANCELLED || code == grpc::StatusCode::UNKNOWN ||
                code == grpc::StatusCode::DEADLINE_EXCEEDED || code == grpc::StatusCode::RESOURCE_EXHAUSTED ||
                code == grpc::StatusCode::ABORTED || code == grpc::StatusCode::INTERNAL ||
                code == grpc::StatusCode::UNAVAILABLE;
        }

        void RescheduleOnFailure() { // call with Lock_
            const auto now = SysClock::now();
            const auto retryDelay = std::min(BackoffTimeout_, BACKOFF_MAX);
            NextTicketUpdate_ = SafeAddSystemTime(now, ToBoundedSysDuration(retryDelay));
            BackoffTimeout_ = std::min(BackoffTimeout_ * 2, BACKOFF_MAX);
        }

        void RescheduleOnSuccess(const SysTimePoint expiresAt) { // call with Lock_
            BackoffTimeout_ = BACKOFF_START;

            const auto now = SysClock::now();
            const SysTimePoint refreshAt = SafeAddSystemTime(now, ToBoundedSysDuration(IamEndpoint_.RefreshPeriod));
            const SysDuration requestMargin = ToBoundedSysDuration(IamEndpoint_.RequestTimeout);

            SysTimePoint nextUpdate = std::min(refreshAt, expiresAt);
            nextUpdate = SafeAddSystemTime(nextUpdate, -requestMargin);
            nextUpdate = std::max(nextUpdate, SafeAddSystemTime(now, ToBoundedSysDuration(MINIMUM_REFRESH_INTERVAL)));
            NextTicketUpdate_ = nextUpdate;
        }

    private:
        std::shared_ptr<grpc::Channel> Channel_;
        std::shared_ptr<typename TService::Stub> Stub_;
        TAsyncRpc Rpc_;

        SysTimePoint NextTicketUpdate_;
        const TIamEndpoint IamEndpoint_;
        const TRequestFiller RequestFiller_;
        std::optional<grpc::ClientContext> Context_;
        std::condition_variable ContextReady_;
        bool NeedStop_;
        std::chrono::milliseconds BackoffTimeout_;
        std::mutex Lock_;
        std::weak_ptr<ICoreFacility> ResponseFacility_;
        TCredentialsProviderPtr AuthTokenProvider_;
        NThreading::TFuture<std::string> AuthTokenInfo_;
        NThreading::TPromise<std::string> AuthInfo_;
    };

public:
    TGrpcIamCredentialsProvider(const TIamEndpoint& endpoint,
                                const TRequestFiller& requestFiller,
                                TAsyncRpc rpc,
                                std::weak_ptr<ICoreFacility> responseFacility,
                                TCredentialsProviderPtr authTokenProvider = nullptr)
        : Impl_(std::make_shared<TImpl>(endpoint, requestFiller, rpc, std::move(responseFacility), authTokenProvider))
    {
        Impl_->StartPeriodicTask();
    }

    ~TGrpcIamCredentialsProvider() {
        Impl_->Stop();
    }

    std::string GetAuthInfo() const override {
        return GetAuthInfoAsync().GetValueSync();
    }

    NThreading::TFuture<std::string> GetAuthInfoAsync() const override {
        return Impl_->GetAuthInfoAsync();
    }

    bool IsValid() const override {
        return true;
    }

private:
    std::shared_ptr<TImpl> Impl_;
};

// Adapter that keeps a self-owned ICoreFacility alive for the lifetime of an inner credentials
// provider. Used by deprecated no-arg ICredentialsProviderFactory::CreateProvider() paths where
// the caller hasn't supplied a facility.
class TOwningFacilityCredentialsProvider : public ICredentialsProvider {
public:
    TOwningFacilityCredentialsProvider(std::shared_ptr<ICoreFacility> facility,
                                       TCredentialsProviderPtr inner)
        : Facility_(std::move(facility))
        , Inner_(std::move(inner))
    {}

    std::string GetAuthInfo() const override {
        return Inner_->GetAuthInfo();
    }

    NThreading::TFuture<std::string> GetAuthInfoAsync() const override {
        return Inner_->GetAuthInfoAsync();
    }

    bool IsValid() const override {
        return Inner_->IsValid();
    }

    ~TOwningFacilityCredentialsProvider() {
        Inner_.reset();
        std::thread([facility = std::move(Facility_)] {}).detach();
    }

private:
    std::shared_ptr<ICoreFacility> Facility_;
    TCredentialsProviderPtr Inner_;
};

template<typename TRequest, typename TResponse, typename TService>
class TIamJwtCredentialsProvider : public TGrpcIamCredentialsProvider<TRequest, TResponse, TService> {
public:
    TIamJwtCredentialsProvider(const TIamJwtParams& params, std::weak_ptr<ICoreFacility> responseFacility)
        : TGrpcIamCredentialsProvider<TRequest, TResponse, TService>(params,
            [jwtParams = params.JwtParams](TRequest& req) {
                req.set_jwt(MakeSignedJwt(jwtParams));
            }, [](typename TService::Stub* stub, grpc::ClientContext* context, const TRequest* request, TResponse* response, std::function<void(grpc::Status)> cb) {
                stub->async()->Create(context, request, response, std::move(cb));
            }, std::move(responseFacility)) {}
};

template<typename TRequest, typename TResponse, typename TService>
class TIamOAuthCredentialsProvider : public TGrpcIamCredentialsProvider<TRequest, TResponse, TService> {
public:
    TIamOAuthCredentialsProvider(const TIamOAuth& params, std::weak_ptr<ICoreFacility> responseFacility)
        : TGrpcIamCredentialsProvider<TRequest, TResponse, TService>(params,
            [token = params.OAuthToken](TRequest& req) {
                req.set_yandex_passport_oauth_token(TStringType{token});
            }, [](typename TService::Stub* stub, grpc::ClientContext* context, const TRequest* request, TResponse* response, std::function<void(grpc::Status)> cb) {
                stub->async()->Create(context, request, response, std::move(cb));
            }, std::move(responseFacility)) {}
};

template<typename TRequest, typename TResponse, typename TService>
class TIamJwtCredentialsProviderFactory : public ICredentialsProviderFactory {
public:
    TIamJwtCredentialsProviderFactory(const TIamJwtParams& params): Params_(params) {}

    // Deprecated. Kept for backward compatibility with callers (including out-of-tree mirrors)
    // that don't have access to an ICoreFacility. Spins up a private TSimpleCoreFacility and ties
    // its lifetime to the returned provider via TOwningFacilityCredentialsProvider.
    TCredentialsProviderPtr CreateProvider() const final {
        auto facility = CreateSimpleCoreFacility();
        auto inner = std::make_shared<TIamJwtCredentialsProvider<TRequest, TResponse, TService>>(
            Params_, std::weak_ptr<ICoreFacility>(facility));
        return std::make_shared<TOwningFacilityCredentialsProvider>(std::move(facility), std::move(inner));
    }

    TCredentialsProviderPtr CreateProvider(std::weak_ptr<ICoreFacility> facility) const override {
        return std::make_shared<TIamJwtCredentialsProvider<TRequest, TResponse, TService>>(Params_, std::move(facility));
    }

private:
    TIamJwtParams Params_;
};

template<typename TRequest, typename TResponse, typename TService>
class TIamOAuthCredentialsProviderFactory : public ICredentialsProviderFactory {
public:
    TIamOAuthCredentialsProviderFactory(const TIamOAuth& params): Params_(params) {}

    // Deprecated. Kept for backward compatibility — see comment on TIamJwtCredentialsProviderFactory.
    TCredentialsProviderPtr CreateProvider() const final {
        auto facility = CreateSimpleCoreFacility();
        auto inner = std::make_shared<TIamOAuthCredentialsProvider<TRequest, TResponse, TService>>(
            Params_, std::weak_ptr<ICoreFacility>(facility));
        return std::make_shared<TOwningFacilityCredentialsProvider>(std::move(facility), std::move(inner));
    }

    TCredentialsProviderPtr CreateProvider(std::weak_ptr<ICoreFacility> facility) const override {
        return std::make_shared<TIamOAuthCredentialsProvider<TRequest, TResponse, TService>>(Params_, std::move(facility));
    }

private:
    TIamOAuth Params_;
};

} // namespace NYdb

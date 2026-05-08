#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common/types.h>

<<<<<<< HEAD:ydb/public/sdk/cpp/src/client/iam/common/iam.h
#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>
=======
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/time/time.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/core_facility/core_facility.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/grpc_common/constants.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/type_switcher.h>
>>>>>>> 0140ad83476 (fix sdk: fixed self thread join in iam cred provider (#39506)):ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common/generic_provider.h

#include <util/string/builder.h>

<<<<<<< HEAD:ydb/public/sdk/cpp/src/client/iam/common/iam.h
=======
#include <grpcpp/grpcpp.h>

#include <chrono>
#include <format>
#include <string>

>>>>>>> 0140ad83476 (fix sdk: fixed self thread join in iam cred provider (#39506)):ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common/generic_provider.h
namespace NYdb::inline Dev {

constexpr std::chrono::milliseconds BACKOFF_START{50};
constexpr std::chrono::milliseconds BACKOFF_MAX{10000};
constexpr std::chrono::milliseconds PERIODIC_TICK{100};
constexpr std::chrono::milliseconds MINIMUM_REFRESH_INTERVAL{100};

template<typename TRequest, typename TResponse, typename TService>
class TGrpcIamCredentialsProvider : public ICredentialsProvider {
protected:
    using TRequestFiller = std::function<void(TRequest&)>;

    using TSimpleRpc =
        typename NYdbGrpc::TSimpleRequestProcessor<
            typename TService::Stub,
            TRequest,
            TResponse>::TAsyncRequest;

    using SysClock = std::chrono::system_clock;
    using SysTimePoint = SysClock::time_point;

private:
    class TImpl : public std::enable_shared_from_this<TGrpcIamCredentialsProvider<TRequest, TResponse, TService>::TImpl> {
    public:
<<<<<<< HEAD:ydb/public/sdk/cpp/src/client/iam/common/iam.h
        TImpl(const TIamEndpoint& iamEndpoint, const TRequestFiller& requestFiller, TSimpleRpc rpc)
            : Client(std::make_unique<NYdbGrpc::TGRpcClientLow>())
            , Connection_(nullptr)
            , Rpc_(rpc)
=======
        TImpl(const TIamEndpoint& iamEndpoint,
              const TRequestFiller& requestFiller,
              TAsyncRpc rpc,
              std::weak_ptr<ICoreFacility> responseFacility,
              TCredentialsProviderPtr authTokenProvider)
            : Rpc_(rpc)
>>>>>>> 0140ad83476 (fix sdk: fixed self thread join in iam cred provider (#39506)):ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common/generic_provider.h
            , Ticket_("")
            , NextTicketUpdate_(SysTimePoint{})
            , IamEndpoint_(iamEndpoint)
            , RequestFiller_(requestFiller)
            , Context_(std::nullopt)
            , LastRequestError_("")
            , NeedStop_(false)
            , BackoffTimeout_(BACKOFF_START)
            , Lock_()
<<<<<<< HEAD:ydb/public/sdk/cpp/src/client/iam/common/iam.h
=======
            , ResponseFacility_(std::move(responseFacility))
            , AuthTokenProvider_(authTokenProvider)
>>>>>>> 0140ad83476 (fix sdk: fixed self thread join in iam cred provider (#39506)):ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common/generic_provider.h
        {
            NYdbGrpc::TGRpcClientConfig grpcConf;
            grpcConf.Locator = IamEndpoint_.Endpoint;
            grpcConf.EnableSsl = IamEndpoint_.EnableSsl;
            Connection_ = std::unique_ptr<NYdbGrpc::TServiceConnection<TService>>(Client->CreateGRpcServiceConnection<TService>(grpcConf).release());
        }

        void StartPeriodicTask() {
            auto facility = ResponseFacility_.lock();
            if (!facility) {
                return;
            }

<<<<<<< HEAD:ydb/public/sdk/cpp/src/client/iam/common/iam.h
            auto resultPromise = NThreading::NewPromise();

            std::shared_ptr<TImpl> self = TGrpcIamCredentialsProvider<TRequest, TResponse, TService>::TImpl::shared_from_this();

            auto cb = [self, resultPromise, sync](
                NYdbGrpc::TGrpcStatus&& status, TResponse&& result) mutable {
                self->ProcessIamResponse(std::move(status), std::move(result), sync);
                resultPromise.SetValue();
            };

            TRequest req;

            RequestFiller_(req);

            Connection_->template DoRequest<TRequest, TResponse>(
                std::move(req),
                std::move(cb),
                Rpc_,
                { {}, {}, IamEndpoint_.RequestTimeout }
            );

            if (sync) {
                resultPromise.GetFuture().Wait(2 * IamEndpoint_.RequestTimeout);
            }
=======
            std::weak_ptr<TImpl> weakSelf = TGrpcIamCredentialsProvider<TRequest, TResponse, TService>::TImpl::weak_from_this();
            facility->AddPeriodicTask(
                [weakSelf](NYdb::NIssue::TIssues&&, EStatus status) {
                    auto self = weakSelf.lock();
                    if (!self || status != EStatus::SUCCESS) {
                        return false;
                    }
                    return self->OnPeriodicTick();
                },
                PERIODIC_TICK
            );
>>>>>>> 0140ad83476 (fix sdk: fixed self thread join in iam cred provider (#39506)):ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common/generic_provider.h
        }

        std::string GetTicket() {
            std::lock_guard guard(Lock_);
            if (Ticket_.empty()) {
                ythrow yexception() << "IAM-token not ready yet. " << LastRequestError_;
            }
            return Ticket_;
        }

        void WaitForToken() {
            std::unique_lock guard(Lock_);
            TokenReady_.wait_for(guard,
                std::chrono::microseconds(2 * IamEndpoint_.RequestTimeout.MicroSeconds()),
                [this]() {
                    return NeedStop_ || !Ticket_.empty(); 
                }
            );
        }

        void Stop() {
            {
                std::unique_lock guard(Lock_);
                if (NeedStop_) {
                    return;
                }
                NeedStop_ = true;
                TokenReady_.notify_all();
                if (Context_.has_value()) {
                    Context_->TryCancel();
                }
                ContextReady_.wait(guard, [this]() { return !Context_.has_value(); });
            }

            Client.reset(); // Will trigger destroy
        }

    private:
<<<<<<< HEAD:ydb/public/sdk/cpp/src/client/iam/common/iam.h
        void ProcessIamResponse(NYdbGrpc::TGrpcStatus&& status, TResponse&& result, bool sync) {
            if (!status.Ok()) {
                TDuration sleepDuration;
                {
                    std::lock_guard guard(Lock_);
                    LastRequestError_ = TStringBuilder()
                        << "Last request error was at " << TInstant::Now()
                        << ". GrpcStatusCode: " << status.GRpcStatusCode
                        << " Message: \"" << status.Msg
                        << "\" internal: " << status.InternalError
                        << " iam-endpoint: \"" << IamEndpoint_.Endpoint << "\"";
=======
        using SysDuration = SysClock::duration;
>>>>>>> 0140ad83476 (fix sdk: fixed self thread join in iam cred provider (#39506)):ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common/generic_provider.h

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
                auto self = weakSelf.lock();

                try {
                    if (facility) {
                        facility->PostToResponseQueue(std::move(work));
                        return;
                    }
                } catch (...) {
                }

                if (self) {
                    std::lock_guard guard(self->Lock_);
                    self->ResetContextImpl();
                }
            };

            TRequest req;

            RequestFiller_(req);

            Rpc_(Stub_.get(), &*Context_, &req, response.get(), std::move(cb));
        }

        void FillContext(std::unique_lock<std::mutex>& guard) {
            auto& context = Context_.emplace();
            auto deadline = gpr_time_add(
                gpr_now(GPR_CLOCK_MONOTONIC),
                gpr_time_from_micros(IamEndpoint_.RequestTimeout.MicroSeconds(), GPR_TIMESPAN));

            context.set_deadline(deadline);

            if (AuthTokenProvider_) {
                guard.unlock();
                auto token = AuthTokenProvider_->GetAuthInfo();
                guard.lock();

                context.AddMetadata("authorization", "Bearer " + token);
            }
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
            {
                std::unique_lock guard(Lock_);
                if (NeedStop_) {
                    return false;
                }
                if (Context_.has_value() || SysClock::now() < NextTicketUpdate_) {
                    return true;
                }
                FillContext(guard);
                if (NeedStop_) {
                    return false;
                }
            }
            UpdateTicket();
            return true;
        }

        void ProcessIamResponse(grpc::Status&& status, TResponse&& result) {
            std::lock_guard guard(Lock_);

            if (!status.ok()) {
                LastRequestError_ = TStringBuilder()
                    << "Last request error was at " << FormatSysTimeUtcIsoMicros(SysClock::now())
                    << ". GrpcStatusCode: " << static_cast<int>(status.error_code())
                    << " Message: \"" << status.error_message()
                    << "\" iam-endpoint: \"" << IamEndpoint_.Endpoint << "\"";

                const auto now = SysClock::now();
                const auto retryDelay = std::min(BackoffTimeout_, BACKOFF_MAX);
                NextTicketUpdate_ = SafeAddSystemTime(now, ToBoundedSysDuration(retryDelay));
                BackoffTimeout_ = std::min(BackoffTimeout_ * 2, BACKOFF_MAX);
            } else {
                LastRequestError_ = "";
                Ticket_ = result.iam_token();
                BackoffTimeout_ = BACKOFF_START;

                const auto now = SysClock::now();
                const SysTimePoint refreshAt = SafeAddSystemTime(now, ToBoundedSysDuration(IamEndpoint_.RefreshPeriod));
                const SysTimePoint expiresAt = SysClock::from_time_t(result.expires_at().seconds());
                const SysDuration requestMargin = ToBoundedSysDuration(IamEndpoint_.RequestTimeout);

                SysTimePoint nextUpdate = std::min(refreshAt, expiresAt);
                nextUpdate = SafeAddSystemTime(nextUpdate, -requestMargin);
                nextUpdate = std::max(nextUpdate, SafeAddSystemTime(now, ToBoundedSysDuration(MINIMUM_REFRESH_INTERVAL)));
                NextTicketUpdate_ = nextUpdate;

                TokenReady_.notify_all();
            }

            ResetContextImpl();
        }

    private:
        std::unique_ptr<NYdbGrpc::TGRpcClientLow> Client;
        std::unique_ptr<NYdbGrpc::TServiceConnection<TService>> Connection_;
        TSimpleRpc Rpc_;
        std::string Ticket_;
        SysTimePoint NextTicketUpdate_;
        const TIamEndpoint IamEndpoint_;
        const TRequestFiller RequestFiller_;
        std::optional<grpc::ClientContext> Context_;
        std::condition_variable ContextReady_;
        std::condition_variable TokenReady_;
        std::string LastRequestError_;
        bool NeedStop_;
<<<<<<< HEAD:ydb/public/sdk/cpp/src/client/iam/common/iam.h
        TDuration BackoffTimeout_;
        TAdaptiveLock Lock_;
    };

public:
    TGrpcIamCredentialsProvider(const TIamEndpoint& endpoint, const TRequestFiller& requestFiller, TSimpleRpc rpc)
        : Impl_(std::make_shared<TImpl>(endpoint, requestFiller, rpc))
=======
        std::chrono::milliseconds BackoffTimeout_;
        std::mutex Lock_;
        std::weak_ptr<ICoreFacility> ResponseFacility_;
        TCredentialsProviderPtr AuthTokenProvider_;
    };

public:
    TGrpcIamCredentialsProvider(const TIamEndpoint& endpoint,
                                const TRequestFiller& requestFiller,
                                TAsyncRpc rpc,
                                std::weak_ptr<ICoreFacility> responseFacility,
                                TCredentialsProviderPtr authTokenProvider = nullptr)
        : Impl_(std::make_shared<TImpl>(endpoint, requestFiller, rpc, std::move(responseFacility), authTokenProvider))
>>>>>>> 0140ad83476 (fix sdk: fixed self thread join in iam cred provider (#39506)):ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common/generic_provider.h
    {
        Impl_->StartPeriodicTask();
        Impl_->WaitForToken();
    }

    ~TGrpcIamCredentialsProvider() {
        Impl_->Stop();
    }

    std::string GetAuthInfo() const override {
        return Impl_->GetTicket();
    }

    bool IsValid() const override {
        return true;
    }

private:
    std::shared_ptr<TImpl> Impl_;
};

template<typename TRequest, typename TResponse, typename TService>
class TIamJwtCredentialsProvider : public TGrpcIamCredentialsProvider<TRequest, TResponse, TService> {
public:
    TIamJwtCredentialsProvider(const TIamJwtParams& params, std::weak_ptr<ICoreFacility> responseFacility)
        : TGrpcIamCredentialsProvider<TRequest, TResponse, TService>(params,
            [jwtParams = params.JwtParams](TRequest& req) {
                req.set_jwt(MakeSignedJwt(jwtParams));
<<<<<<< HEAD:ydb/public/sdk/cpp/src/client/iam/common/iam.h
            }, &TService::Stub::AsyncCreate) {}
=======
            }, [](typename TService::Stub* stub, grpc::ClientContext* context, const TRequest* request, TResponse* response, std::function<void(grpc::Status)> cb) {
                stub->async()->Create(context, request, response, std::move(cb));
            }, std::move(responseFacility), nullptr) {}
>>>>>>> 0140ad83476 (fix sdk: fixed self thread join in iam cred provider (#39506)):ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common/generic_provider.h
};

template<typename TRequest, typename TResponse, typename TService>
class TIamOAuthCredentialsProvider : public TGrpcIamCredentialsProvider<TRequest, TResponse, TService> {
public:
    TIamOAuthCredentialsProvider(const TIamOAuth& params, std::weak_ptr<ICoreFacility> responseFacility)
        : TGrpcIamCredentialsProvider<TRequest, TResponse, TService>(params,
            [token = params.OAuthToken](TRequest& req) {
                req.set_yandex_passport_oauth_token(TStringType{token});
<<<<<<< HEAD:ydb/public/sdk/cpp/src/client/iam/common/iam.h
            }, &TService::Stub::AsyncCreate) {}
=======
            }, [](typename TService::Stub* stub, grpc::ClientContext* context, const TRequest* request, TResponse* response, std::function<void(grpc::Status)> cb) {
                stub->async()->Create(context, request, response, std::move(cb));
            }, std::move(responseFacility), nullptr) {}
>>>>>>> 0140ad83476 (fix sdk: fixed self thread join in iam cred provider (#39506)):ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common/generic_provider.h
};

template<typename TRequest, typename TResponse, typename TService>
class TIamJwtCredentialsProviderFactory : public ICredentialsProviderFactory {
public:
    TIamJwtCredentialsProviderFactory(const TIamJwtParams& params): Params_(params) {}

    TCredentialsProviderPtr CreateProvider() const final {
        ythrow yexception() << "Not supported";
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

    TCredentialsProviderPtr CreateProvider() const final {
        ythrow yexception() << "Not supported";
    }

    TCredentialsProviderPtr CreateProvider(std::weak_ptr<ICoreFacility> facility) const override {
        return std::make_shared<TIamOAuthCredentialsProvider<TRequest, TResponse, TService>>(Params_, std::move(facility));
    }

private:
    TIamOAuth Params_;
};

} // namespace NYdb

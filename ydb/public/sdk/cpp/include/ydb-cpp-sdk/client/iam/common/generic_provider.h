#pragma once

#include "types.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/grpc_common/constants.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/type_switcher.h>

#include <library/cpp/threading/future/future.h>

#include <util/string/builder.h>
#include <util/system/spinlock.h>

#include <grpcpp/grpcpp.h>

namespace NYdb::inline Dev {

constexpr TDuration BACKOFF_START = TDuration::MilliSeconds(50);
constexpr TDuration BACKOFF_MAX = TDuration::Seconds(10);

// This file contains internal generic implementation of IAM credentials providers.
// DO NOT USE THIS CLASS DIRECTLY. Use specialized factory methods for specific cases.
template<typename TRequest, typename TResponse, typename TService>
class TGrpcIamCredentialsProvider : public ICredentialsProvider {
protected:
    using TRequestFiller = std::function<void(TRequest&)>;
    using TAsyncInterface = typename TService::Stub::async_interface;
    using TAsyncRpc = std::function<void(typename TService::Stub*, grpc::ClientContext*, const TRequest*, TResponse*, std::function<void(grpc::Status)>)>;

private:
    class TImpl : public std::enable_shared_from_this<TGrpcIamCredentialsProvider<TRequest, TResponse, TService>::TImpl> {
    public:
        TImpl(const TIamEndpoint& iamEndpoint,
              const TRequestFiller& requestFiller,
              TAsyncRpc rpc,
              TCredentialsProviderPtr authTokenProvider = nullptr)
            : Rpc_(rpc)
            , Ticket_("")
            , NextTicketUpdate_(TInstant::Zero())
            , IamEndpoint_(iamEndpoint)
            , RequestFiller_(requestFiller)
            , RequestInflight_(false)
            , LastRequestError_("")
            , NeedStop_(false)
            , BackoffTimeout_(BACKOFF_START)
            , Lock_()
            , AuthTokenProvider_(authTokenProvider)
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

        void UpdateTicket(bool sync = false) {
            {
                std::lock_guard guard(Lock_);
                if (NeedStop_ || RequestInflight_) {
                    return;
                }
                RequestInflight_ = true;
            }

            auto resultPromise = NThreading::NewPromise();
            auto response = std::make_shared<TResponse>();
            auto context = std::make_shared<grpc::ClientContext>();

            std::shared_ptr<TImpl> self = TGrpcIamCredentialsProvider<TRequest, TResponse, TService>::TImpl::shared_from_this();

            auto cb = [self, sync, resultPromise, response, context] (grpc::Status status) mutable {
                self->ProcessIamResponse(std::move(status), std::move(*response), sync);
                resultPromise.SetValue();
            };

            TRequest req;

            RequestFiller_(req);

            auto deadline = gpr_time_add(
                gpr_now(GPR_CLOCK_MONOTONIC),
                gpr_time_from_micros(IamEndpoint_.RequestTimeout.MicroSeconds(), GPR_TIMESPAN));

            context->set_deadline(deadline);
            if (AuthTokenProvider_) {
                context->AddMetadata("authorization", "Bearer " + AuthTokenProvider_->GetAuthInfo());
            }

            Rpc_(Stub_.get(), context.get(), &req, response.get(), std::move(cb));

            if (sync) {
                resultPromise.GetFuture().Wait(2 * IamEndpoint_.RequestTimeout);
            }
        }

        std::string GetTicket() {
            TInstant nextTicketUpdate;
            std::string ticket;
            {
                std::lock_guard guard(Lock_);
                ticket = Ticket_;
                nextTicketUpdate = NextTicketUpdate_;
                if (ticket.empty())
                    ythrow yexception() << "IAM-token not ready yet. " << LastRequestError_;
            }
            if (TInstant::Now() >= nextTicketUpdate) {
                UpdateTicket();
            }
            return ticket;
        }

        void Stop() {
            {
                std::lock_guard guard(Lock_);
                if (NeedStop_) {
                    return;
                }
                NeedStop_ = true;
            }
            Stub_.reset();
            Channel_.reset();
        }

    private:
        void ProcessIamResponse(grpc::Status&& status, TResponse&& result, bool sync) {
            if (!status.ok()) {
                TDuration sleepDuration;
                {
                    std::lock_guard guard(Lock_);
                    LastRequestError_ = TStringBuilder()
                        << "Last request error was at " << TInstant::Now()
                        << ". GrpcStatusCode: " << static_cast<int>(status.error_code())
                        << " Message: \"" << status.error_message()
                        << "\" iam-endpoint: \"" << IamEndpoint_.Endpoint << "\"";

                    RequestInflight_ = false;
                    sleepDuration = std::min(BackoffTimeout_, BACKOFF_MAX);
                    BackoffTimeout_ = std::min(BackoffTimeout_ * 2, BACKOFF_MAX);
                }

                Sleep(sleepDuration);

                UpdateTicket(sync);
            } else {
                std::lock_guard guard(Lock_);
                LastRequestError_ = "";
                Ticket_ = result.iam_token();
                RequestInflight_ = false;
                BackoffTimeout_ = BACKOFF_START;

                const auto now = Now();
                NextTicketUpdate_ = std::min(
                    now + IamEndpoint_.RefreshPeriod,
                    TInstant::Seconds(result.expires_at().seconds())
                ) - IamEndpoint_.RequestTimeout;
                NextTicketUpdate_ = std::max(NextTicketUpdate_, now + TDuration::MilliSeconds(100));
            }
        }

    private:
        std::shared_ptr<grpc::Channel> Channel_;
        std::shared_ptr<typename TService::Stub> Stub_;
        TAsyncRpc Rpc_;

        std::string Ticket_;
        TInstant NextTicketUpdate_;
        const TIamEndpoint IamEndpoint_;
        const TRequestFiller RequestFiller_;
        bool RequestInflight_;
        std::string LastRequestError_;
        bool NeedStop_;
        TDuration BackoffTimeout_;
        TAdaptiveLock Lock_;
        TCredentialsProviderPtr AuthTokenProvider_;
    };

public:
    TGrpcIamCredentialsProvider(const TIamEndpoint& endpoint,
                                const TRequestFiller& requestFiller,
                                TAsyncRpc rpc,
                                TCredentialsProviderPtr authTokenProvider = nullptr)
        : Impl_(std::make_shared<TImpl>(endpoint, requestFiller, rpc, authTokenProvider))
    {
        Impl_->UpdateTicket(true);
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
    TIamJwtCredentialsProvider(const TIamJwtParams& params)
        : TGrpcIamCredentialsProvider<TRequest, TResponse, TService>(params,
            [jwtParams = params.JwtParams](TRequest& req) {
                req.set_jwt(MakeSignedJwt(jwtParams));
            }, [](typename TService::Stub* stub, grpc::ClientContext* context, const TRequest* request, TResponse* response, std::function<void(grpc::Status)> cb) {
                stub->async()->Create(context, request, response, std::move(cb));
            }) {}
};

template<typename TRequest, typename TResponse, typename TService>
class TIamOAuthCredentialsProvider : public TGrpcIamCredentialsProvider<TRequest, TResponse, TService> {
public:
    TIamOAuthCredentialsProvider(const TIamOAuth& params)
        : TGrpcIamCredentialsProvider<TRequest, TResponse, TService>(params,
            [token = params.OAuthToken](TRequest& req) {
                req.set_yandex_passport_oauth_token(TStringType{token});
            }, [](typename TService::Stub* stub, grpc::ClientContext* context, const TRequest* request, TResponse* response, std::function<void(grpc::Status)> cb) {
                stub->async()->Create(context, request, response, std::move(cb));
            }) {}
};

template<typename TRequest, typename TResponse, typename TService>
class TIamJwtCredentialsProviderFactory : public ICredentialsProviderFactory {
public:
    TIamJwtCredentialsProviderFactory(const TIamJwtParams& params): Params_(params) {}

    TCredentialsProviderPtr CreateProvider() const final {
        return std::make_shared<TIamJwtCredentialsProvider<TRequest, TResponse, TService>>(Params_);
    }

private:
    TIamJwtParams Params_;
};

template<typename TRequest, typename TResponse, typename TService>
class TIamOAuthCredentialsProviderFactory : public ICredentialsProviderFactory {
public:
    TIamOAuthCredentialsProviderFactory(const TIamOAuth& params): Params_(params) {}

    TCredentialsProviderPtr CreateProvider() const final {
        return std::make_shared<TIamOAuthCredentialsProvider<TRequest, TResponse, TService>>(Params_);
    }

private:
    TIamOAuth Params_;
};

} // namespace NYdb

#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common/types.h>

#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>

#include <library/cpp/threading/future/future.h>

#include <util/string/builder.h>
#include <util/system/spinlock.h>

namespace NYdb::inline Dev {

constexpr TDuration BACKOFF_START = TDuration::MilliSeconds(50);
constexpr TDuration BACKOFF_MAX = TDuration::Seconds(10);

template<typename TRequest, typename TResponse, typename TService>
class TGrpcIamCredentialsProvider : public ICredentialsProvider {
protected:
    using TRequestFiller = std::function<void(TRequest&)>;

    using TSimpleRpc =
        typename NYdbGrpc::TSimpleRequestProcessor<
            typename TService::Stub,
            TRequest,
            TResponse>::TAsyncRequest;

private:
    class TImpl : public std::enable_shared_from_this<TGrpcIamCredentialsProvider<TRequest, TResponse, TService>::TImpl> {
    public:
        TImpl(const TIamEndpoint& iamEndpoint, const TRequestFiller& requestFiller, TSimpleRpc rpc)
            : Client(std::make_unique<NYdbGrpc::TGRpcClientLow>())
            , Connection_(nullptr)
            , Rpc_(rpc)
            , Ticket_("")
            , NextTicketUpdate_(TInstant::Zero())
            , IamEndpoint_(iamEndpoint)
            , RequestFiller_(requestFiller)
            , RequestInflight_(false)
            , LastRequestError_("")
            , NeedStop_(false)
            , BackoffTimeout_(BACKOFF_START)
            , Lock_()
        {
            NYdbGrpc::TGRpcClientConfig grpcConf;
            grpcConf.Locator = IamEndpoint_.Endpoint;
            grpcConf.EnableSsl = IamEndpoint_.EnableSsl;
            Connection_ = std::unique_ptr<NYdbGrpc::TServiceConnection<TService>>(Client->CreateGRpcServiceConnection<TService>(grpcConf).release());
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

            Client.reset(); // Will trigger destroy
        }

    private:
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

                    RequestInflight_ = false;
                    sleepDuration = std::min(BackoffTimeout_, BACKOFF_MAX);
                    BackoffTimeout_ *= 2;
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
        std::unique_ptr<NYdbGrpc::TGRpcClientLow> Client;
        std::unique_ptr<NYdbGrpc::TServiceConnection<TService>> Connection_;
        TSimpleRpc Rpc_;
        std::string Ticket_;
        TInstant NextTicketUpdate_;
        const TIamEndpoint IamEndpoint_;
        const TRequestFiller RequestFiller_;
        bool RequestInflight_;
        std::string LastRequestError_;
        bool NeedStop_;
        TDuration BackoffTimeout_;
        TAdaptiveLock Lock_;
    };

public:
    TGrpcIamCredentialsProvider(const TIamEndpoint& endpoint, const TRequestFiller& requestFiller, TSimpleRpc rpc)
        : Impl_(std::make_shared<TImpl>(endpoint, requestFiller, rpc))
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
            }, &TService::Stub::AsyncCreate) {}
};

template<typename TRequest, typename TResponse, typename TService>
class TIamOAuthCredentialsProvider : public TGrpcIamCredentialsProvider<TRequest, TResponse, TService> {
public:
    TIamOAuthCredentialsProvider(const TIamOAuth& params)
        : TGrpcIamCredentialsProvider<TRequest, TResponse, TService>(params,
            [token = params.OAuthToken](TRequest& req) {
                req.set_yandex_passport_oauth_token(TStringType{token});
            }, &TService::Stub::AsyncCreate) {}
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

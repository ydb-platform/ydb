#pragma once

#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>

#include <ydb/library/grpc/client/grpc_client_low.h>
#include <library/cpp/threading/atomic/bool.h>
#include <library/cpp/threading/future/core/future.h>
#include <library/cpp/json/json_reader.h>

#include <ydb/public/lib/jwt/jwt.h>
#include <util/datetime/base.h>

#include <util/system/spinlock.h>
#include <util/stream/file.h>
#include <util/string/builder.h>

namespace NYdb {

namespace NIam {
constexpr TStringBuf DEFAULT_ENDPOINT = "iam.api.cloud.yandex.net";
constexpr bool DEFAULT_ENABLE_SSL = true;

constexpr TStringBuf DEFAULT_HOST = "169.254.169.254";
constexpr ui32 DEFAULT_PORT = 80;

constexpr TDuration DEFAULT_REFRESH_PERIOD = TDuration::Hours(1);
constexpr TDuration DEFAULT_REQUEST_TIMEOUT = TDuration::Seconds(10);
}

struct TIamHost {
    TString Host = TString(NIam::DEFAULT_HOST);
    ui32 Port = NIam::DEFAULT_PORT;
    TDuration RefreshPeriod = NIam::DEFAULT_REFRESH_PERIOD;
};

struct TIamEndpoint {
    TString Endpoint = TString(NIam::DEFAULT_ENDPOINT);
    TDuration RefreshPeriod = NIam::DEFAULT_REFRESH_PERIOD;
    TDuration RequestTimeout = NIam::DEFAULT_REQUEST_TIMEOUT;
    bool EnableSsl = NIam::DEFAULT_ENABLE_SSL;
};

struct TIamJwtFilename : TIamEndpoint { TString JwtFilename; };

struct TIamJwtContent : TIamEndpoint { TString JwtContent; };

struct TIamJwtParams : TIamEndpoint { TJwtParams JwtParams; };

inline TJwtParams ReadJwtKeyFile(const TString& filename) {
    return ParseJwtParams(TFileInput(filename).ReadAll());
}

struct TIamOAuth : TIamEndpoint { TString OAuthToken; };

/// Acquire an IAM token using a local metadata service on a virtual machine.
TCredentialsProviderFactoryPtr CreateIamCredentialsProviderFactory(const TIamHost& params = {});

/// Acquire an IAM token using a JSON Web Token (JWT) file name.
TCredentialsProviderFactoryPtr CreateIamJwtFileCredentialsProviderFactory(const TIamJwtFilename& params);

/// Acquire an IAM token using JSON Web Token (JWT) contents.
TCredentialsProviderFactoryPtr CreateIamJwtParamsCredentialsProviderFactory(const TIamJwtContent& param);

// Acquire an IAM token using a user OAuth token.
TCredentialsProviderFactoryPtr CreateIamOAuthCredentialsProviderFactory(const TIamOAuth& params);

constexpr TDuration BACKOFF_START = TDuration::MilliSeconds(50);
constexpr TDuration BACKOFF_MAX = TDuration::Seconds(10);

template<typename TRequest, typename TResponse, typename TService>
class TGrpcIamCredentialsProvider : public ICredentialsProvider {
protected:
    using TRequestFiller = std::function<void(TRequest&)>;

private:
    class TImpl : public std::enable_shared_from_this<TGrpcIamCredentialsProvider<TRequest, TResponse, TService>::TImpl> {
    public:
        TImpl(const TIamEndpoint& iamEndpoint, const TRequestFiller& requestFiller)
            : Client(MakeHolder<NYdbGrpc::TGRpcClientLow>())
            , Connection_(nullptr)
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
            Connection_ = THolder<NYdbGrpc::TServiceConnection<TService>>(Client->CreateGRpcServiceConnection<TService>(grpcConf).release());
        }

        void UpdateTicket(bool sync = false) {
            with_lock(Lock_) {
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
                &TService::Stub::AsyncCreate,
                { {}, {}, IamEndpoint_.RequestTimeout }
            );

            if (sync) {
                resultPromise.GetFuture().Wait(2 * IamEndpoint_.RequestTimeout);
            }
        }

        TStringType GetTicket() {
            TInstant nextTicketUpdate;
            TString ticket;
            with_lock(Lock_) {
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
            with_lock(Lock_) {
                if (NeedStop_) {
                    return;
                }
                NeedStop_ = true;
            }

            Client.Reset(); // Will trigger destroy
        }

    private:
        void ProcessIamResponse(NYdbGrpc::TGrpcStatus&& status, TResponse&& result, bool sync) {
            if (!status.Ok()) {
                TDuration sleepDuration;
                with_lock(Lock_) {
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
                with_lock(Lock_) {
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
        }

    private:

        THolder<NYdbGrpc::TGRpcClientLow> Client;
        THolder<NYdbGrpc::TServiceConnection<TService>> Connection_;
        TStringType Ticket_;
        TInstant NextTicketUpdate_;
        const TIamEndpoint IamEndpoint_;
        const TRequestFiller RequestFiller_;
        bool RequestInflight_;
        TStringType LastRequestError_;
        bool NeedStop_;
        TDuration BackoffTimeout_;
        TAdaptiveLock Lock_;
    };

public:
    TGrpcIamCredentialsProvider(const TIamEndpoint& endpoint, const TRequestFiller& requestFiller)
        : Impl_(std::make_shared<TImpl>(endpoint, requestFiller))
    {
        Impl_->UpdateTicket(true);
    }

    ~TGrpcIamCredentialsProvider() {
        Impl_->Stop();
    }

    TStringType GetAuthInfo() const override {
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
            }) {}
};

template<typename TRequest, typename TResponse, typename TService>
class TIamOAuthCredentialsProvider : public TGrpcIamCredentialsProvider<TRequest, TResponse, TService> {
public:
    TIamOAuthCredentialsProvider(const TIamOAuth& params)
        : TGrpcIamCredentialsProvider<TRequest, TResponse, TService>(params,
            [token = params.OAuthToken](TRequest& req) {
                req.set_yandex_passport_oauth_token(token);
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

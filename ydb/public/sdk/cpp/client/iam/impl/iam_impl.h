#pragma once
#include <ydb/public/sdk/cpp/client/iam/common/iam.h>

#include <library/cpp/grpc/client/grpc_client_low.h>
#include <library/cpp/threading/atomic/bool.h>
#include <library/cpp/threading/future/core/future.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/http/simple/http_client.h>

#include <util/system/spinlock.h>
#include <util/stream/file.h>
#include <util/string/builder.h>

#include <chrono>

using namespace NGrpc;

namespace NYdb {

constexpr TDuration BACKOFF_START = TDuration::MilliSeconds(50);
constexpr TDuration BACKOFF_MAX = TDuration::Seconds(10);

class TIAMCredentialsProvider : public ICredentialsProvider {
public:
    TIAMCredentialsProvider(const TIamHost& params)
        : HttpClient_(TSimpleHttpClient(params.Host, params.Port))
        , Request_("/computeMetadata/v1/instance/service-accounts/default/token")
        , NextTicketUpdate_(TInstant::Zero())
        , RefreshPeriod_(params.RefreshPeriod)
    {
        GetTicket();
    }

    TStringType GetAuthInfo() const override {
        if (TInstant::Now() >= NextTicketUpdate_) {
            GetTicket();
        }
        return Ticket_;
    }

    bool IsValid() const override {
        return true;
    }

private:
    TSimpleHttpClient HttpClient_;
    TStringType Request_;
    mutable TStringType Ticket_;
    mutable TInstant NextTicketUpdate_;
    TDuration RefreshPeriod_;

    void GetTicket() const {
        try {
            TStringStream out;
            TSimpleHttpClient::THeaders headers;
            headers["Metadata-Flavor"] = "Google";
            HttpClient_.DoGet(Request_, &out, headers);
            NJson::TJsonValue resp;
            NJson::ReadJsonTree(&out, &resp, true);

            auto respMap = resp.GetMap();

            if (auto it = respMap.find("access_token"); it == respMap.end())
                ythrow yexception() << "Result doesn't contain access_token";
            else if (TString ticket = it->second.GetStringSafe(); ticket.empty())
                ythrow yexception() << "Got empty ticket";
            else
                Ticket_ = std::move(ticket);

            if (auto it = respMap.find("expires_in"); it == respMap.end())
                ythrow yexception() << "Result doesn't contain expires_in";
            else {
                const TDuration expiresIn = TDuration::Seconds(it->second.GetUInteger());

                NextTicketUpdate_ = TInstant::Now() + std::max(expiresIn, RefreshPeriod_);
            }
        } catch (...) {
        }
    }
};


template<typename TRequest, typename TResponse, typename TService>
class TGrpcIamCredentialsProvider : public ICredentialsProvider {
protected:
    using TRequestFiller = std::function<void(TRequest&)>;

private:
    class TImpl : public std::enable_shared_from_this<TGrpcIamCredentialsProvider<TRequest, TResponse, TService>::TImpl> {
    public:
        TImpl(const TIamEndpoint& iamEndpoint, const TRequestFiller& requestFiller)
            : Client(MakeHolder<NGrpc::TGRpcClientLow>())
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
            TGRpcClientConfig grpcConf;
            grpcConf.Locator = IamEndpoint_.Endpoint;
            grpcConf.EnableSsl = true;
            Connection_ = THolder<TServiceConnection<TService>>(Client->CreateGRpcServiceConnection<TService>(grpcConf).release());
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
                NGrpc::TGrpcStatus&& status, TResponse&& result) mutable {
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
        void ProcessIamResponse(NGrpc::TGrpcStatus&& status, TResponse&& result, bool sync) {
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

        THolder<TGRpcClientLow> Client;
        THolder<TServiceConnection<TService>> Connection_;
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

struct TIamJwtParams : TIamEndpoint {
    TJwtParams JwtParams;
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

inline TJwtParams ReadJwtKeyFile(const TString& filename) {
    return ParseJwtParams(TFileInput(filename).ReadAll());
}

class TIamCredentialsProviderFactory : public ICredentialsProviderFactory {
public:
    TIamCredentialsProviderFactory(const TIamHost& params): Params_(params) {}

    TCredentialsProviderPtr CreateProvider() const final {
        return std::make_shared<TIAMCredentialsProvider>(Params_);
    }

private:
    TIamHost Params_;
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

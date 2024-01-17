#include "token_accessor_client.h"

#include <ydb/library/yql/providers/common/token_accessor/grpc/token_accessor_pb.pb.h>
#include <ydb/library/yql/providers/common/token_accessor/grpc/token_accessor_pb.grpc.pb.h>

#include <ydb/library/grpc/client/grpc_client_low.h>
#include <library/cpp/threading/atomic/bool.h>
#include <library/cpp/threading/future/core/future.h>

#include <util/string/builder.h>

namespace NYql {

namespace {

const TDuration BACKOFF_START = TDuration::MilliSeconds(50);
const TDuration BACKOFF_MAX = TDuration::Seconds(10);

class TTokenAccessorCredentialsProvider : public NYdb::ICredentialsProvider {
private:
    class TImpl : public std::enable_shared_from_this<TImpl> {
    public:
        TImpl(
            std::shared_ptr<NYdbGrpc::TGRpcClientLow> client,
            std::shared_ptr<NYdbGrpc::TServiceConnection<TokenAccessorService>> connection,
            const TString& serviceAccountId,
            const TString& serviceAccountIdSignature,
            const TDuration& refreshPeriod,
            const TDuration& requestTimeout)
            : Client(std::move(client))
            , Connection(std::move(connection))
            , NextTicketUpdate(TInstant::Zero())
            , ServiceAccountId(serviceAccountId)
            , ServiceAccountIdSignature(serviceAccountIdSignature)
            , RefreshPeriod(refreshPeriod)
            , RequestTimeout(requestTimeout)
            , Infly(0)
        {
        }

        void UpdateTicket(bool sync = false) const {
            if (NeedStop || RequestInflight) {
                return;
            }

            RequestInflight = true;
            auto resultPromise = NThreading::NewPromise();

            auto context = Client->CreateContext();
            if (!context) {
                throw yexception() << "Client is being shutted down";
            }
            std::weak_ptr<const TImpl> weakSelf = shared_from_this();
            // hold context until reply
            auto cb = [weakSelf, resultPromise, sync, context](NYdbGrpc::TGrpcStatus&& status, GetTokenResponse&& result) mutable {
                if (auto self = weakSelf.lock()) {
                    self->ProcessResponse(std::move(status), std::move(result), sync);
                }
                resultPromise.SetValue();
            };

            GetTokenRequest req;
            req.set_type(GetTokenRequest::TYPE_SERVICE_ACCOUNT);
            req.set_token_id(ServiceAccountId);
            req.set_signature(ServiceAccountIdSignature);
            with_lock(Lock) {
                Infly++;
                Connection->DoRequest<GetTokenRequest, GetTokenResponse>(
                    std::move(req),
                    std::move(cb),
                    &TokenAccessorService::Stub::AsyncGetToken,
                    {
                        {}, {}, RequestTimeout
                    },
                    context.get()
                );
            }
            if (sync) {
                resultPromise.GetFuture().Wait(RequestTimeout + TDuration::Seconds(10));
            }
        }

        TString GetTicket() const {
            TInstant nextTicketUpdate;
            TString ticket;
            with_lock(Lock) {
                ticket = Ticket;
                nextTicketUpdate = NextTicketUpdate;
                if (ticket.empty()) {
                    throw yexception() << "IAM-token not ready yet. " << LastRequestError;
                }
            }
            if (TInstant::Now() >= nextTicketUpdate) {
                UpdateTicket();
            }
            return ticket;
        }

        void Stop() {
            NeedStop = true;
        }

    private:
        void ProcessResponse(NYdbGrpc::TGrpcStatus&& status, GetTokenResponse&& result, bool sync) const {
            if (!status.Ok()) {
                with_lock(Lock) {
                    --Infly;
                    LastRequestError = TStringBuilder() << "Last request error was at " << TInstant::Now()
                        << ". GrpcStatusCode: " << status.GRpcStatusCode << " Message: \"" << status.Msg
                        << "\" internal: " << status.InternalError;
                }
                RequestInflight = false;
                Sleep(std::min(BackoffTimeout, BACKOFF_MAX));
                BackoffTimeout *= 2;
                UpdateTicket(sync);
            } else {
                with_lock(Lock) {
                    --Infly;
                    LastRequestError = "";
                    Ticket = result.token();
                    NextTicketUpdate = TInstant::Now() + RefreshPeriod - RequestTimeout;
                }
                RequestInflight = false;
                BackoffTimeout = BACKOFF_START;
            }
        }

    private:
        const std::shared_ptr<NYdbGrpc::TGRpcClientLow> Client;
        const std::shared_ptr<NYdbGrpc::TServiceConnection<TokenAccessorService>> Connection;
        mutable TString Ticket;
        mutable TInstant NextTicketUpdate;
        const TString ServiceAccountId;
        const TString ServiceAccountIdSignature;
        const TDuration RefreshPeriod;
        const TDuration RequestTimeout;
        TAdaptiveLock Lock;
        mutable NAtomic::TBool RequestInflight;
        mutable TString LastRequestError;
        NAtomic::TBool NeedStop = false;
        mutable TDuration BackoffTimeout = BACKOFF_START;
        mutable ui32 Infly;
    };

public:
    TTokenAccessorCredentialsProvider(
        std::shared_ptr<NYdbGrpc::TGRpcClientLow> client,
        std::shared_ptr<NYdbGrpc::TServiceConnection<TokenAccessorService>> connection,
        const TString& serviceAccountId,
        const TString& serviceAccountIdSignature,
        const TDuration& refreshPeriod,
        const TDuration& requestTimeout
    )
        : Impl(std::make_shared<TImpl>(std::move(client), std::move(connection), serviceAccountId, serviceAccountIdSignature, refreshPeriod, requestTimeout))
    {
        Impl->UpdateTicket(true);
    }

    ~TTokenAccessorCredentialsProvider() {
        Impl->Stop();
    }

    TString GetAuthInfo() const override {
        return Impl->GetTicket();
    }

    bool IsValid() const override {
        return true;
    }

private:
    std::shared_ptr<TImpl> Impl;
};

}

std::shared_ptr<NYdb::ICredentialsProvider> CreateTokenAccessorCredentialsProvider(
    const TString& tokenAccessorEndpoint,
    bool useSsl,
    const TString& sslCaCert,
    const TString& serviceAccountId,
    const TString& serviceAccountIdSignature,
    const TDuration& refreshPeriod,
    const TDuration& requestTimeout
)
{
    auto client = std::make_unique<NYdbGrpc::TGRpcClientLow>();
    NYdbGrpc::TGRpcClientConfig grpcConf;
    grpcConf.Locator = tokenAccessorEndpoint;
    grpcConf.EnableSsl = useSsl;
    grpcConf.SslCredentials.pem_root_certs = sslCaCert;
    std::shared_ptr<NYdbGrpc::TServiceConnection<TokenAccessorService>> connection = client->CreateGRpcServiceConnection<TokenAccessorService>(grpcConf);

    return CreateTokenAccessorCredentialsProvider(std::move(client), std::move(connection), serviceAccountId, serviceAccountIdSignature, refreshPeriod, requestTimeout);
}

std::shared_ptr<NYdb::ICredentialsProvider> CreateTokenAccessorCredentialsProvider(
    std::shared_ptr<NYdbGrpc::TGRpcClientLow> client,
    std::shared_ptr<NYdbGrpc::TServiceConnection<TokenAccessorService>> connection,
    const TString& serviceAccountId,
    const TString& serviceAccountIdSignature,
    const TDuration& refreshPeriod,
    const TDuration& requestTimeout
)
{
    return std::make_shared<TTokenAccessorCredentialsProvider>(std::move(client), std::move(connection), serviceAccountId, serviceAccountIdSignature, refreshPeriod, requestTimeout);
}

}

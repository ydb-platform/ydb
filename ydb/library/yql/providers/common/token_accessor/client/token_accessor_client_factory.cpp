#include "token_accessor_client_factory.h"
#include "token_accessor_client.h"

#include <util/string/cast.h>

namespace NYql {

namespace {

class TTokenAccessorCredentialsProviderFactory : public NYdb::ICredentialsProviderFactory {
public:
    TTokenAccessorCredentialsProviderFactory(
        std::shared_ptr<NYdbGrpc::TGRpcClientLow> client,
        std::shared_ptr<NYdbGrpc::TServiceConnection<TokenAccessorService>> connection,
        const TString& serviceAccountId,
        const TString& serviceAccountIdSignature,
        const TDuration& refreshPeriod,
        const TDuration& requestTimeout
    )
        : Client(std::move(client))
        , Connection(std::move(connection))
        , ServiceAccountId(serviceAccountId)
        , ServiceAccountIdSignature(serviceAccountIdSignature)
        , RefreshPeriod(refreshPeriod)
        , RequestTimeout(requestTimeout)
    {
    }

    std::shared_ptr<NYdb::ICredentialsProvider> CreateProvider() const override {
        return CreateTokenAccessorCredentialsProvider(Client, Connection, ServiceAccountId, ServiceAccountIdSignature, RefreshPeriod, RequestTimeout);
    }

private:
    const std::shared_ptr<NYdbGrpc::TGRpcClientLow> Client;
    const std::shared_ptr<NYdbGrpc::TServiceConnection<TokenAccessorService>> Connection;
    const TString ServiceAccountId;
    const TString ServiceAccountIdSignature;
    const TDuration RefreshPeriod;
    const TDuration RequestTimeout;
};

}

std::shared_ptr<NYdb::ICredentialsProviderFactory> CreateTokenAccessorCredentialsProviderFactory(
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

    return CreateTokenAccessorCredentialsProviderFactory(std::move(client), std::move(connection), serviceAccountId, serviceAccountIdSignature, refreshPeriod, requestTimeout);
}

std::shared_ptr<NYdb::ICredentialsProviderFactory> CreateTokenAccessorCredentialsProviderFactory(
    std::shared_ptr<NYdbGrpc::TGRpcClientLow> client,
    std::shared_ptr<NYdbGrpc::TServiceConnection<TokenAccessorService>> connection,
    const TString& serviceAccountId,
    const TString& serviceAccountIdSignature,
    const TDuration& refreshPeriod,
    const TDuration& requestTimeout
)
{
    return std::make_shared<TTokenAccessorCredentialsProviderFactory>(std::move(client), std::move(connection), serviceAccountId, serviceAccountIdSignature, refreshPeriod, requestTimeout);
}

}

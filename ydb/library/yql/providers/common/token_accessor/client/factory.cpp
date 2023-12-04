#include "factory.h"
#include "bearer_credentials_provider.h"
#include "token_accessor_client_factory.h"
#include <ydb/library/yql/providers/common/structured_token/yql_structured_token.h>
#include <ydb/library/yql/providers/common/structured_token/yql_token_builder.h>
#include <util/string/cast.h>

namespace NYql {

namespace {

using TTokenAccessorConnectionPool = std::vector<std::shared_ptr<NYdbGrpc::TServiceConnection<TokenAccessorService>>>;

class TSecuredServiceAccountCredentialsFactoryImpl : public ISecuredServiceAccountCredentialsFactory {
public:
    TSecuredServiceAccountCredentialsFactoryImpl(
        const TString& tokenAccessorEndpoint,
        bool useSsl,
        const TString& sslCaCert,
        ui32 connectionPoolSize,
        const TDuration& refreshPeriod,
        const TDuration& requestTimeout
    )
        : RefreshPeriod(refreshPeriod)
        , RequestTimeout(requestTimeout)
        , Client(std::make_shared<NYdbGrpc::TGRpcClientLow>())
    {
        GrpcClientConfig.Locator = tokenAccessorEndpoint;
        GrpcClientConfig.EnableSsl = useSsl;
        GrpcClientConfig.SslCredentials.pem_root_certs = sslCaCert;
        Connections.reserve(connectionPoolSize);
        for (ui32 i = 0; i < connectionPoolSize; ++i) {
            Connections.push_back(Client->CreateGRpcServiceConnection<TokenAccessorService>(GrpcClientConfig));
        }
    }

    std::shared_ptr<NYdb::ICredentialsProviderFactory> Create(const TString& serviceAccountId, const TString& serviceAccountIdSignature) override {
        Y_ENSURE(serviceAccountId);
        Y_ENSURE(serviceAccountIdSignature);

        std::shared_ptr<NYdbGrpc::TServiceConnection<TokenAccessorService>> connection;
        if (Connections.empty()) {
            connection = Client->CreateGRpcServiceConnection<TokenAccessorService>(GrpcClientConfig);
        } else {
            connection = Connections[NextConnectionIndex++ % Connections.size()];
        }

        return CreateTokenAccessorCredentialsProviderFactory(Client, std::move(connection), serviceAccountId, serviceAccountIdSignature, RefreshPeriod, RequestTimeout);
    }

private:
    NYdbGrpc::TGRpcClientConfig GrpcClientConfig;
    const TDuration RefreshPeriod;
    const TDuration RequestTimeout;
    const std::shared_ptr<NYdbGrpc::TGRpcClientLow> Client;
    TTokenAccessorConnectionPool Connections;
    mutable std::atomic<ui32> NextConnectionIndex = 0;
};

std::shared_ptr<NYdb::ICredentialsProviderFactory> WrapWithBearerIfNeeded(std::shared_ptr<NYdb::ICredentialsProviderFactory> delegatee, bool addBearerToToken) {
    if (!addBearerToToken) {
        return delegatee;
    }
    return WrapCredentialsProviderFactoryWithBearer(delegatee);
}
}

ISecuredServiceAccountCredentialsFactory::TPtr CreateSecuredServiceAccountCredentialsOverTokenAccessorFactory(
    const TString& tokenAccessorEndpoint,
    bool useSsl,
    const TString& sslCaCert,
    ui32 connectionPoolSize,
    const TDuration& refreshPeriod,
    const TDuration& requestTimeout) {
    return std::make_shared<TSecuredServiceAccountCredentialsFactoryImpl>(tokenAccessorEndpoint, useSsl, sslCaCert, connectionPoolSize, refreshPeriod, requestTimeout);
}

std::shared_ptr<NYdb::ICredentialsProviderFactory> CreateCredentialsProviderFactoryForStructuredToken(ISecuredServiceAccountCredentialsFactory::TPtr factory, const TString& structuredTokenJson, bool addBearerToToken) {
    if (!NYql::IsStructuredTokenJson(structuredTokenJson)) {
        return WrapWithBearerIfNeeded(NYdb::CreateOAuthCredentialsProviderFactory(structuredTokenJson), addBearerToToken);
    }

    NYql::TStructuredTokenParser parser = NYql::CreateStructuredTokenParser(structuredTokenJson);
    if (parser.HasIAMToken()) {
        return WrapWithBearerIfNeeded(NYdb::CreateOAuthCredentialsProviderFactory(parser.GetIAMToken()), addBearerToToken); // OK for any static token (OAuth, IAM).
    }

    if (parser.HasServiceAccountIdAuth()) {
        TString id;
        TString signature;
        parser.GetServiceAccountIdAuth(id, signature);

        if (!factory) {
            ythrow yexception() << "You must provide credentials factory instance to transform service account credentials into IAM-token.";
        }
        return WrapWithBearerIfNeeded(factory->Create(id, signature), addBearerToToken);
    }

    if (parser.IsNoAuth()) {
        return NYdb::CreateInsecureCredentialsProviderFactory();
    }

    ythrow yexception() << "Unrecognized token of length " << structuredTokenJson.size();
}

}

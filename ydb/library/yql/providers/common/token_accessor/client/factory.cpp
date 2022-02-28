#include "factory.h"
#include "bearer_credentials_provider.h"
#include "token_accessor_client_factory.h"
#include <ydb/library/yql/providers/common/structured_token/yql_structured_token.h>
#include <ydb/library/yql/providers/common/structured_token/yql_token_builder.h>
#include <util/string/cast.h>

namespace NYql {

namespace {

class TSecuredServiceAccountCredentialsFactoryImpl : public ISecuredServiceAccountCredentialsFactory {
public:
    TSecuredServiceAccountCredentialsFactoryImpl(
        const TString& tokenAccessorEndpoint,
        bool useSsl,
        const TString& sslCaCert,
        const TDuration& refreshPeriod,
        const TDuration& requestTimeout
    )
        : TokenAccessorEndpoint(tokenAccessorEndpoint)
        , UseSsl(useSsl)
        , SslCaCert(sslCaCert)
        , RefreshPeriod(refreshPeriod)
        , RequestTimeout(requestTimeout) {
    }

    std::shared_ptr<NYdb::ICredentialsProviderFactory> Create(const TString& serviceAccountId, const TString& serviceAccountIdSignature) override {
        Y_ENSURE(serviceAccountId);
        Y_ENSURE(serviceAccountIdSignature);

        return CreateTokenAccessorCredentialsProviderFactory(TokenAccessorEndpoint, UseSsl, SslCaCert, serviceAccountId, serviceAccountIdSignature, RefreshPeriod, RequestTimeout);
    }

private:
    const TString TokenAccessorEndpoint;
    const bool UseSsl;
    const TString SslCaCert;
    const TDuration RefreshPeriod;
    const TDuration RequestTimeout;
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
    const TDuration& refreshPeriod,
    const TDuration& requestTimeout) {
    return std::make_shared<TSecuredServiceAccountCredentialsFactoryImpl>(tokenAccessorEndpoint, useSsl, sslCaCert, refreshPeriod, requestTimeout);
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
            ythrow yexception() << "Service account id credentials are not supported, service account id: " << id;
        }
        return WrapWithBearerIfNeeded(factory->Create(id, signature), addBearerToToken);
    }

    if (parser.IsNoAuth()) {
        return NYdb::CreateInsecureCredentialsProviderFactory();
    }

    ythrow yexception() << "Unrecognized token of length " << structuredTokenJson.size();
}

}

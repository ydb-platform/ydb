#pragma once

#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>
#include <util/datetime/base.h>

namespace NYql {

class ISecuredServiceAccountCredentialsFactory {
public:
    typedef std::shared_ptr<ISecuredServiceAccountCredentialsFactory> TPtr;

public:
    virtual ~ISecuredServiceAccountCredentialsFactory() {}
    virtual std::shared_ptr<NYdb::ICredentialsProviderFactory> Create(const TString& serviceAccountId, const TString& serviceAccountIdSignature) = 0;
};

ISecuredServiceAccountCredentialsFactory::TPtr CreateSecuredServiceAccountCredentialsOverTokenAccessorFactory(
    const TString& tokenAccessorEndpoint,
    bool useSsl,
    const TString& sslCaCert,
    ui32 connectionPoolSize = 0,
    const TDuration& refreshPeriod = TDuration::Hours(1),
    const TDuration& requestTimeout = TDuration::Seconds(10)
);

std::shared_ptr<NYdb::ICredentialsProviderFactory> CreateCredentialsProviderFactoryForStructuredToken(
    ISecuredServiceAccountCredentialsFactory::TPtr factory,
    const TString& structuredTokenJson,
    bool addBearerToToken = false
);
}

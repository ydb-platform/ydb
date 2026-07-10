#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <yql/essentials/providers/common/structured_token/yql_structured_token.h>
#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>
#include <util/datetime/base.h>

namespace NYql {

class ISecuredServiceAccountCredentialsFactory {
public:
    typedef std::shared_ptr<ISecuredServiceAccountCredentialsFactory> TPtr;

public:
    virtual ~ISecuredServiceAccountCredentialsFactory() {}
    virtual std::shared_ptr<NYdb::ICredentialsProviderFactory> Create(const NYql::TStructuredTokenParser& parser) = 0;
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

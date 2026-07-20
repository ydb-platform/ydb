#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>
#include <util/datetime/base.h>

#include <functional>
#include <memory>

namespace NYql {

using TIamAuthCredentialsFactory = std::function<
    std::shared_ptr<NYdb::ICredentialsProviderFactory>(const TStructuredTokenParser& parser)>;

class ISecuredServiceAccountCredentialsFactory {
public:
    typedef std::shared_ptr<ISecuredServiceAccountCredentialsFactory> TPtr;

public:
    virtual ~ISecuredServiceAccountCredentialsFactory() {}
    virtual std::shared_ptr<NYdb::ICredentialsProviderFactory> Create(const TString& serviceAccountId, const TString& serviceAccountIdSignature) = 0;
};

class IStructuredTokenCredentialsFactory {
public:
    typedef std::shared_ptr<IStructuredTokenCredentialsFactory> TPtr;

public:
    virtual ~IStructuredTokenCredentialsFactory() {}
    virtual std::shared_ptr<NYdb::ICredentialsProviderFactory> Create(
        const TString& structuredTokenJson,
        bool addBearerToToken = false) = 0;
};

IStructuredTokenCredentialsFactory::TPtr CreateStructuredTokenCredentialsFactory(
    ISecuredServiceAccountCredentialsFactory::TPtr saFactory = {},
    TIamAuthCredentialsFactory iamAuthFactory = {});

IStructuredTokenCredentialsFactory::TPtr CreateStructuredTokenCredentialsOverTokenAccessorFactory(
    const TString& tokenAccessorEndpoint,
    bool useSsl,
    const TString& sslCaCert,
    ui32 connectionPoolSize = 0,
    const TDuration& refreshPeriod = TDuration::Hours(1),
    const TDuration& requestTimeout = TDuration::Seconds(10)
);

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

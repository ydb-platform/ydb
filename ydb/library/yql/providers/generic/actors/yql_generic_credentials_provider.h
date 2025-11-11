#pragma once

#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>
#include <yql/essentials/public/issue/yql_issue.h>

namespace NYql {
    class TGenericDataSourceInstance;
}

namespace NYql::NDq {
    // When accessing the external data sources, there are several options for authentication:
    //
    // 1. Use static IAM-token provided by user (especially useful during debugging).
    // 2. Use service account credentials in order to get (and refresh) IAM-token by demand.
    // 3. Use login + password for basic auth.
    class TGenericCredentialsProvider {
    public:
        using TPtr = std::unique_ptr<TGenericCredentialsProvider>;
        TGenericCredentialsProvider() = default; // No auth required
        TGenericCredentialsProvider(const TString& staticIamToken);
        // Remove this constructor as soon as the first part of YQ-4730 is merged
        TGenericCredentialsProvider(
            const TString& serviceAccountId,
            const TString& ServiceAccountIdSignature,
            const ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory);
        TGenericCredentialsProvider(
            const TString& structuredToken,
            const ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory);

        // FillCredentials sets the credentials to access the remote datasource into the DataSourceInstance object.
        // It can be either IAM-token or login + password for basic auth.
        // Returns string containing error, if it happened.
        TString FillCredentials(NYql::TGenericDataSourceInstance& dsi) const;

    private:
        std::optional<TString> StaticIAMToken_;

        struct BasicAuthCredentials {
            TString Username;
            TString Password;
        };
        std::optional<BasicAuthCredentials> BasicAuthCredentials_;

        NYdb::TCredentialsProviderPtr CredentialsProvider_;
    };

    TGenericCredentialsProvider::TPtr
    CreateGenericCredentialsProvider(
        const TString& structuredTokenJSON,
        /*[[deprecated]]*/ const TString& staticIamToken,
        /*[[deprecated]]*/ const TString& serviceAccountId,
        /*[[deprecated]]*/ const TString& serviceAccountIdSignature,
        const ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory);
} // namespace NYql::NDq

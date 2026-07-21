#pragma once

#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>
#include <yql/essentials/public/issue/yql_issue.h>

namespace NYql {
    class TGenericDataSourceInstance;
}

namespace NYql::NDq {
    class TGenericCredentialsProvider {
    public:
        using TPtr = std::unique_ptr<TGenericCredentialsProvider>;

        TGenericCredentialsProvider(
            const TString& structuredToken,
            const IStructuredTokenCredentialsFactory::TPtr& credentialsFactory);

        // FillCredentials sets the credentials to access the remote datasource into the DataSourceInstance object.
        // It can be either IAM-token or login + password for basic auth.
        // Returns empty string on success or (non-empty) an error message.
        // Requires IsReady().
        TString FillCredentials(NYql::TGenericDataSourceInstance& dsi) const;

        NThreading::TFuture<TGenericCredentials> AsyncCredentials() const;

        // Return true when provider is ready to serve requests
        bool IsReady() const;

        // Invoke callback asynchronously when provider is ready to serve requests
        void Subscribe(std::function<void(void)>&& callback);

    private:
        struct BasicAuthCredentials {
            TString Username;
            TString Password;
        };
        std::optional<BasicAuthCredentials> BasicAuthCredentials_;

        NThreading::TFuture<NYdb::TCredentialsProviderPtr> AsyncCredentialsProvider_;
    };

    TGenericCredentialsProvider::TPtr
    CreateGenericCredentialsProvider(
        const TString& structuredTokenJSON,
        const IStructuredTokenCredentialsFactory::TPtr& credentialsFactory);
} // namespace NYql::NDq

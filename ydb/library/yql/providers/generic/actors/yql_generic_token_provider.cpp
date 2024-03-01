#include "yql_generic_token_provider.h"

#include <ydb/library/yql/providers/common/structured_token/yql_token_builder.h>

namespace NYql::NDq {
    TGenericTokenProvider::TGenericTokenProvider(
        const NYql::NGeneric::TSource& source, const ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory)
        : Source_(source)
        , StaticIAMToken_(source.GetToken())
        , CredentialsProvider_(nullptr) {
        // 1. User has provided IAM-token itself.
        // This token will be used during the whole lifetime of a read actor.
        if (!StaticIAMToken_.empty()) {
            return;
        }

        // 2. User has provided service account creds.
        // We create token accessor client that will renew token accessor by demand.
        if (source.GetServiceAccountId() && source.GetServiceAccountIdSignature()) {
            Y_ENSURE(credentialsFactory, "CredentialsFactory is not initialized");

            auto structuredTokenJSON =
                TStructuredTokenBuilder()
                    .SetServiceAccountIdAuth(source.GetServiceAccountId(), source.GetServiceAccountIdSignature())
                    .ToJson();

            // If service account is provided, obtain IAM-token
            Y_ENSURE(structuredTokenJSON, "empty structured token");

            auto credentialsProviderFactory =
                CreateCredentialsProviderFactoryForStructuredToken(credentialsFactory, structuredTokenJSON, false);
            CredentialsProvider_ = credentialsProviderFactory->CreateProvider();
        }

        // 3. If we reached this point, it means that user doesn't need token auth.
    }

    void TGenericTokenProvider::MaybeFillToken(NConnector::NApi::TDataSourceInstance* dsi) const {
        // 1. Don't need tokens if basic auth is set
        if (dsi->credentials().has_basic()) {
            return;
        }

        *dsi->mutable_credentials()->mutable_token()->mutable_type() = "IAM";

        // 2. If static IAM-token has been provided, use it
        if (!StaticIAMToken_.empty()) {
            *dsi->mutable_credentials()->mutable_token()->mutable_value() = StaticIAMToken_;
            return;
        }

        // 3. Otherwise use credentials provider to get token 
        Y_ENSURE(CredentialsProvider_, "CredentialsProvider is not initialized");

        auto iamToken = CredentialsProvider_->GetAuthInfo();
        Y_ENSURE(iamToken, "CredentialsProvider returned empty IAM token");

        *dsi->mutable_credentials()->mutable_token()->mutable_value() = std::move(iamToken);
    }

    TGenericTokenProvider::TPtr
    CreateGenericTokenProvider(const NYql::NGeneric::TSource& source,
                               const ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory) {
        return std::make_unique<TGenericTokenProvider>(source, credentialsFactory);
    }
} //namespace NYql::NDq

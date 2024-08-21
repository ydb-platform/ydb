#include "yql_generic_token_provider.h"

#include <ydb/library/yql/providers/common/structured_token/yql_token_builder.h>

namespace NYql::NDq {
    TGenericTokenProvider::TGenericTokenProvider(const TString& staticIamToken)
        : StaticIAMToken_(staticIamToken)
    {
    }

    TGenericTokenProvider::TGenericTokenProvider(
        const TString& serviceAccountId,
        const TString& ServiceAccountIdSignature,
        const ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory)
    {
        Y_ENSURE(!serviceAccountId.Empty(), "No service account provided");
        Y_ENSURE(!ServiceAccountIdSignature.Empty(), "No service account signature provided");
        Y_ENSURE(credentialsFactory, "CredentialsFactory is not initialized");

        auto structuredTokenJSON =
            TStructuredTokenBuilder()
                .SetServiceAccountIdAuth(serviceAccountId, ServiceAccountIdSignature)
                .ToJson();

        Y_ENSURE(structuredTokenJSON, "empty structured token");

        auto credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(credentialsFactory, structuredTokenJSON, false);
        CredentialsProvider_ = credentialsProviderFactory->CreateProvider();
    }

    void TGenericTokenProvider::MaybeFillToken(NConnector::NApi::TDataSourceInstance& dsi) const {
        // 1. Don't need tokens if basic auth is set
        if (dsi.credentials().has_basic()) {
            return;
        }

        *dsi.mutable_credentials()->mutable_token()->mutable_type() = "IAM";

        // 2. If static IAM-token has been provided, use it
        if (!StaticIAMToken_.empty()) {
            *dsi.mutable_credentials()->mutable_token()->mutable_value() = StaticIAMToken_;
            return;
        }

        // 3. Otherwise use credentials provider to get token
        Y_ENSURE(CredentialsProvider_, "CredentialsProvider is not initialized");

        auto iamToken = CredentialsProvider_->GetAuthInfo();
        Y_ENSURE(iamToken, "CredentialsProvider returned empty IAM token");

        *dsi.mutable_credentials()->mutable_token()->mutable_value() = std::move(iamToken);
    }

    TGenericTokenProvider::TPtr
    CreateGenericTokenProvider(
        const TString& staticIamToken,
        const TString& serviceAccountId, const TString& serviceAccountIdSignature,
        const ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory)
    {
        if (!staticIamToken.Empty()) {
            return std::make_unique<TGenericTokenProvider>(staticIamToken);
        }
        if (!serviceAccountId.Empty()) {
            return std::make_unique<TGenericTokenProvider>(serviceAccountId, serviceAccountIdSignature, credentialsFactory);
        }
        return std::make_unique<TGenericTokenProvider>();
    }
} // namespace NYql::NDq

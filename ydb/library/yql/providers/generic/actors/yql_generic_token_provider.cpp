#include "yql_generic_token_provider.h"

#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql::NDq {
    TGenericTokenProvider::TGenericTokenProvider(const TString& staticIamToken)
        : StaticIAMToken_(staticIamToken)
    {
    }

    TGenericTokenProvider::TGenericTokenProvider(
        const TString& serviceAccountId, 
        const TString& serviceAccountIdSignature,
        const ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory) {
        Y_ENSURE(!serviceAccountId.empty(), "No service account provided");
        Y_ENSURE(!serviceAccountIdSignature.empty(), "No service account signature provided");
        Y_ENSURE(credentialsFactory, "CredentialsFactory is not initialized");

        auto structuredTokenJSON =
            TStructuredTokenBuilder().SetServiceAccountIdAuth(serviceAccountId, serviceAccountIdSignature).ToJson();

        Y_ENSURE(structuredTokenJSON, "empty structured token");

        auto credentialsProviderFactory =
            CreateCredentialsProviderFactoryForStructuredToken(credentialsFactory, structuredTokenJSON, false);
        CredentialsProvider_ = credentialsProviderFactory->CreateProvider();
    }

    TString TGenericTokenProvider::MaybeFillToken(NYql::TGenericDataSourceInstance& dsi) const {
        // 1. Don't need tokens if basic auth is set
        if (dsi.credentials().has_basic()) {
            return {};
        }

        *dsi.mutable_credentials()->mutable_token()->mutable_type() = "IAM";

        // 2. If static IAM-token has been provided, use it
        if (!StaticIAMToken_.empty()) {
            *dsi.mutable_credentials()->mutable_token()->mutable_value() = StaticIAMToken_;
            return {};
        }

        // 3. Otherwise use credentials provider to get token
        Y_ENSURE(CredentialsProvider_, "CredentialsProvider is not initialized");

        std::string iamToken;
        try {
            iamToken = CredentialsProvider_->GetAuthInfo();
        } catch (const std::exception& e) {
            YQL_CLOG(ERROR, ProviderGeneric) << "MaybeFillToken: " << e.what();
            return TString(e.what());
        }

        Y_ENSURE(!iamToken.empty(), "CredentialsProvider returned empty IAM token");

        *dsi.mutable_credentials()->mutable_token()->mutable_value() = std::move(iamToken);
        return {};
    }

    TGenericTokenProvider::TPtr
    CreateGenericTokenProvider(const TString& staticIamToken,
                               const TString& serviceAccountId,
                               const TString& serviceAccountIdSignature,
                               const ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory) {
        if (!staticIamToken.empty()) {
            return std::make_unique<TGenericTokenProvider>(staticIamToken);
        }
        if (!serviceAccountId.empty()) {
            return std::make_unique<TGenericTokenProvider>(serviceAccountId, serviceAccountIdSignature,
                                                           credentialsFactory);
        }
        return std::make_unique<TGenericTokenProvider>();
    }
} // namespace NYql::NDq

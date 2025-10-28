#include "yql_generic_credentials_provider.h"

#include <ydb/library/yql/providers/generic/proto/source.pb.h>
#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql::NDq {
    TGenericCredentialsProvider::TGenericCredentialsProvider(
        const TString& structuredTokenJSON,
        const ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory) {
        Y_ENSURE(!structuredTokenJSON.empty(), "empty structured token");
        Y_ENSURE(credentialsFactory, "CredentialsFactory is not initialized");

        NYql::TStructuredTokenParser parser = NYql::CreateStructuredTokenParser(structuredTokenJSON);

        if (parser.HasBasicAuth()) {
            TString login;
            TString password;
            parser.GetBasicAuth(login, password);
            BasicAuthCredentials_ = {login, password};
            return;
        }

        auto credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(
            credentialsFactory,
            structuredTokenJSON,
            false);

        CredentialsProvider_ = credentialsProviderFactory->CreateProvider();
    }

    TString TGenericCredentialsProvider::FillCredentials(NYql::TGenericDataSourceInstance& dsi) const {
        // 1. If basic auth creds have been provided, use it
        if (BasicAuthCredentials_) {
            auto basic = dsi.mutable_credentials()->mutable_basic();
            *basic->mutable_username() = BasicAuthCredentials_->Username;
            *basic->mutable_password() = BasicAuthCredentials_->Password;
            return {};
        }

        *dsi.mutable_credentials()->mutable_token()->mutable_type() = "IAM";

        // 2. If static IAM-token has been provided, use it
        if (StaticIAMToken_) {
            *dsi.mutable_credentials()->mutable_token()->mutable_value() = *StaticIAMToken_;
            return {};
        }

        // 3. Otherwise use credentials provider to get token from Token Accessor
        Y_ENSURE(CredentialsProvider_, "CredentialsProvider is not initialized");

        std::string iamToken;
        try {
            iamToken = CredentialsProvider_->GetAuthInfo();
        } catch (const std::exception& e) {
            YQL_CLOG(ERROR, ProviderGeneric) << "FillCredentials: " << e.what();
            return TString(e.what());
        }

        Y_ENSURE(!iamToken.empty(), "CredentialsProvider returned empty IAM token");

        *dsi.mutable_credentials()->mutable_token()->mutable_value() = std::move(iamToken);
        return {};
    }

    TGenericCredentialsProvider::TPtr
    CreateGenericCredentialsProvider(const TString& structuredTokenJSON,
                                     const ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory) {
        if (!structuredTokenJSON.empty()) {
            return std::make_unique<TGenericCredentialsProvider>(structuredTokenJSON, credentialsFactory);
        }
        return std::make_unique<TGenericCredentialsProvider>();
    }
} // namespace NYql::NDq

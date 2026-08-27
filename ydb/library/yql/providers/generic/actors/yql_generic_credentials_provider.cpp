#include "yql_generic_credentials_provider.h"

#include <ydb/library/yql/providers/generic/actors/yql_generic_helpers.h>
#include <ydb/library/yql/providers/generic/proto/source.pb.h>
#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql::NDq {
    TGenericCredentialsProvider::TGenericCredentialsProvider(
        const TString& structuredTokenJSON,
        const ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory) {
        if (IsStructuredTokenJson(structuredTokenJSON)) {
            TStructuredTokenParser parser = CreateStructuredTokenParser(structuredTokenJSON);
            if (parser.HasBasicAuth()) {
                TString login;
                TString password;
                parser.GetBasicAuth(login, password);
                BasicAuthCredentials_ = {login, password};
                return;
            }
        }

        auto credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(
            credentialsFactory,
            structuredTokenJSON,
            false);

        CredentialsProvider_ = credentialsProviderFactory->CreateProvider();
    }

    NThreading::TFuture<TGenericCredentials> TGenericCredentialsProvider::AsyncCredentials() const {
        // 1. If basic auth creds have been provided, use it
        if (BasicAuthCredentials_) {
            TGenericCredentials credentials;
            auto basic = credentials.mutable_basic();
            *basic->mutable_username() = BasicAuthCredentials_->Username;
            *basic->mutable_password() = BasicAuthCredentials_->Password;
            return NThreading::MakeFuture(credentials);
        }
        // 2. Otherwise use credentials provider to get token from Token Accessor
        Y_ENSURE(CredentialsProvider_);
        return CredentialsProvider_->GetAuthInfoAsync()
            .Apply([](const NThreading::TFuture<std::string>& future) {
                auto iamToken = ExtractFromConstFuture(future);
                TGenericCredentials credentials;
                auto& token = *credentials.mutable_token();
                *token.mutable_type() = "IAM";
                *token.mutable_value() = std::move(iamToken);
                return credentials;
            });
    }

    TGenericCredentialsProvider::TPtr
    CreateGenericCredentialsProvider(const TString& structuredTokenJSON,
                                     const ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory) {
        return std::make_unique<TGenericCredentialsProvider>(structuredTokenJSON, credentialsFactory);
    }
} // namespace NYql::NDq

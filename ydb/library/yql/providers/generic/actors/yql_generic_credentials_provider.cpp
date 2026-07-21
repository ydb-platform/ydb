#include "yql_generic_credentials_provider.h"

#include <ydb/library/yql/providers/generic/proto/source.pb.h>
#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql::NDq {
    TGenericCredentialsProvider::TGenericCredentialsProvider(
        const TString& structuredTokenJSON,
        const IStructuredTokenCredentialsFactory::TPtr& credentialsFactory) {
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

        auto credentialsProviderFactory = credentialsFactory->Create(structuredTokenJSON, false);

        AsyncCredentialsProvider_ = credentialsProviderFactory->CreateProviderAsync();
    }

    TString TGenericCredentialsProvider::FillCredentials(TGenericDataSourceInstance& dsi) const {
        // 1. If basic auth creds have been provided, use it
        if (BasicAuthCredentials_) {
            auto basic = dsi.mutable_credentials()->mutable_basic();
            *basic->mutable_username() = BasicAuthCredentials_->Username;
            *basic->mutable_password() = BasicAuthCredentials_->Password;
            return {};
        }

        try {
            // 3. Otherwise use credentials provider to get token from Token Accessor
            Y_ENSURE(IsReady());
            auto credentialsProvider = AsyncCredentialsProvider_.GetValue();
            Y_ENSURE(credentialsProvider, "CredentialsProvider is not initialized");

            std::string iamToken = credentialsProvider->GetAuthInfo();

            *dsi.mutable_credentials()->mutable_token()->mutable_type() = "IAM";
            *dsi.mutable_credentials()->mutable_token()->mutable_value() = std::move(iamToken);
            return {};
        } catch (const std::exception& e) {
            YQL_CLOG(ERROR, ProviderGeneric) << "FillCredentials: " << e.what();
            return TString(e.what());
        }
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
        return AsyncCredentialsProvider_.Apply([](const NThreading::TFuture<NYdb::TCredentialsProviderPtr>& future) {
            auto provider = future.GetValue();
            Y_ENSURE(provider);
            return provider->GetAuthInfoAsync();
        }).Apply([](const NThreading::TFuture<std::string>& f1) {
            NThreading::TFuture<std::string> f2 = f1;
            auto iamToken = f2.ExtractValueSync();
            TGenericCredentials credentials;
            auto& token = *credentials.mutable_token();
            *token.mutable_type() = "IAM";
            *token.mutable_value() = std::move(iamToken);
            return credentials;
        });
    }

    bool TGenericCredentialsProvider::IsReady() const {
        if (BasicAuthCredentials_) {
            return true;
        }
        return AsyncCredentialsProvider_.IsReady();
    }

    void TGenericCredentialsProvider::Subscribe(std::function<void(void)>&& callback) {
        if (BasicAuthCredentials_) {
            callback();
            return;
        }
        AsyncCredentialsProvider_.Subscribe([callback=std::move(callback)](const auto&) {
            callback();
        });
    }

    TGenericCredentialsProvider::TPtr
    CreateGenericCredentialsProvider(const TString& structuredTokenJSON,
                                     const IStructuredTokenCredentialsFactory::TPtr& credentialsFactory) {
        return std::make_unique<TGenericCredentialsProvider>(structuredTokenJSON, credentialsFactory);
    }
} // namespace NYql::NDq

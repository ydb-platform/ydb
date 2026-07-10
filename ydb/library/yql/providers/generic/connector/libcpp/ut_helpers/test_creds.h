#pragma once
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>

namespace NYql::NTestCreds {

    class TCredentialsProvider: public NYdb::ICredentialsProvider {
    public:
        std::string GetAuthInfo() const override {
            return "TEST_TOKEN";
        }
        bool IsValid() const override {
            return true;
        }
    };

    class TCredentialsProviderFactory: public NYdb::ICredentialsProviderFactory {
    public:
        virtual NYdb::TCredentialsProviderPtr CreateProvider() const {
            return std::make_shared<TCredentialsProvider>();
        }
    };

    class TSecuredServiceAccountCredentialsFactory: public ISecuredServiceAccountCredentialsFactory {
    public:
        typedef std::shared_ptr<ISecuredServiceAccountCredentialsFactory> TPtr;

    public:
        std::shared_ptr<NYdb::ICredentialsProviderFactory> Create(const NYql::TStructuredTokenParser& parser) override {
            if (parser.HasServiceAccountIdAuth()) {
                TString serviceAccountId;
                TString serviceAccountIdSignature;
                parser.GetServiceAccountIdAuth(serviceAccountId, serviceAccountIdSignature);
                Y_ABORT_UNLESS(serviceAccountId == "testsaid");
                Y_ABORT_UNLESS(serviceAccountIdSignature == "fake_signature");
                return std::make_shared<TCredentialsProviderFactory>();
            }
            return nullptr;
        }
    };

} // namespace  NYql::NTestCreds

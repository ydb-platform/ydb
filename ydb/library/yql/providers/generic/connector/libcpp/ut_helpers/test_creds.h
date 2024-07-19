#pragma once
#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>

namespace NYql::NTestCreds {

    class TCredentialsProvider: public NYdb::ICredentialsProvider {
    public:
        NYdb::TStringType GetAuthInfo() const override {
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
        std::shared_ptr<NYdb::ICredentialsProviderFactory> Create(const TString& serviceAccountId, const TString& serviceAccountIdSignature) {
            Y_ABORT_UNLESS(serviceAccountId == "testsaid");
            Y_ABORT_UNLESS(serviceAccountIdSignature == "fake_signature");
            return std::make_shared<TCredentialsProviderFactory>();
        }
    };

} //namespace  NYql::NTestCreds

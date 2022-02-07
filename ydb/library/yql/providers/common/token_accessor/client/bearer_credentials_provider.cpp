#include "bearer_credentials_provider.h"
#include <util/string/cast.h>

namespace NYql {

namespace {
class TBearerCredentialsProvider : public NYdb::ICredentialsProvider {
public:
    explicit TBearerCredentialsProvider(std::shared_ptr<NYdb::ICredentialsProvider> delegatee)
        : Delegatee(delegatee) {
    }

    TString GetAuthInfo() const override {
        TString result = Delegatee->GetAuthInfo();
        if (!result || result.StartsWith("Bearer ")) {
            return result;
        }

        return "Bearer " + result;
    }

    bool IsValid() const override {
        return Delegatee->IsValid();
    }

private:
    const std::shared_ptr<NYdb::ICredentialsProvider> Delegatee;
};

class TBearerCredentialsProviderFactory : public NYdb::ICredentialsProviderFactory {
public:
    explicit TBearerCredentialsProviderFactory(std::shared_ptr<NYdb::ICredentialsProviderFactory> delegatee)
        : Delegatee(delegatee) {
    }

    TString GetClientIdentity() const override {
        return "BEARER_CRED_PROV_FACTORY" + ToString((ui64)this);
    }

    std::shared_ptr<NYdb::ICredentialsProvider> CreateProvider() const override {
        return std::make_shared<TBearerCredentialsProvider>(Delegatee->CreateProvider());
    }

private:
    const std::shared_ptr<NYdb::ICredentialsProviderFactory> Delegatee;
};

}

std::shared_ptr<NYdb::ICredentialsProviderFactory> WrapCredentialsProviderFactoryWithBearer(std::shared_ptr<NYdb::ICredentialsProviderFactory> delegatee) {
    return std::make_shared<TBearerCredentialsProviderFactory>(delegatee);
}

}

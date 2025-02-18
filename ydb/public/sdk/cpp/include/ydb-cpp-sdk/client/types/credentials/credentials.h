#pragma once

#include <ydb-cpp-sdk/client/types/fwd.h>

#include <memory>
#include <string>

namespace NYdb::inline V3 {

class ICredentialsProvider {
public:
    virtual ~ICredentialsProvider() = default;
    virtual std::string GetAuthInfo() const = 0;
    virtual bool IsValid() const = 0;
};

using TCredentialsProviderPtr = std::shared_ptr<ICredentialsProvider>;

class ICoreFacility;
class ICredentialsProviderFactory {
public:
    virtual ~ICredentialsProviderFactory() = default;
    virtual TCredentialsProviderPtr CreateProvider() const = 0;
    // !!!Experimental!!!
    virtual TCredentialsProviderPtr CreateProvider([[maybe_unused]] std::weak_ptr<ICoreFacility> facility) const {
        return CreateProvider();
    }
    virtual std::string GetClientIdentity() const;
};

using TCredentialsProviderFactoryPtr = std::shared_ptr<ICredentialsProviderFactory>;

std::shared_ptr<ICredentialsProviderFactory> CreateInsecureCredentialsProviderFactory();
std::shared_ptr<ICredentialsProviderFactory> CreateOAuthCredentialsProviderFactory(const std::string& token);

struct TLoginCredentialsParams {
    std::string User;
    std::string Password;
};

std::shared_ptr<ICredentialsProviderFactory> CreateLoginCredentialsProviderFactory(TLoginCredentialsParams params);

} // namespace NYdb

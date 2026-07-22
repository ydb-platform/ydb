#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/fwd.h>

#include <library/cpp/threading/future/future.h>

#include <exception>
#include <memory>
#include <string>

namespace NYdb::inline Dev {

class ICredentialsProvider {
public:
    virtual ~ICredentialsProvider() = default;
    virtual std::string GetAuthInfo() const = 0;
    virtual bool IsValid() const = 0;
    virtual NThreading::TFuture<std::string> GetAuthInfoAsync() const {
        try {
            return NThreading::MakeFuture(GetAuthInfo());
        } catch (...) {
            return NThreading::MakeErrorFuture<std::string>(std::current_exception());
        }
    }
};

using TCredentialsProviderPtr = std::shared_ptr<ICredentialsProvider>;

class ICoreFacility;
class ICredentialsProviderFactory {
public:
    virtual ~ICredentialsProviderFactory() = default;
    // deprecated, use CreateProvider(std::weak_ptr<ICoreFacility> facility) instead
    virtual TCredentialsProviderPtr CreateProvider() const = 0;
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

#include <ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <util/string/cast.h>

namespace NYdb::inline V3 {

class TInsecureCredentialsProvider : public ICredentialsProvider {
public:
    TInsecureCredentialsProvider()
    {}

    std::string GetAuthInfo() const override {
        return std::string();
    }

    bool IsValid() const override {
        return false;
    }
};

std::string ICredentialsProviderFactory::GetClientIdentity() const {
    return ToString((ui64)this);
}

class TInsecureCredentialsProviderFactory : public ICredentialsProviderFactory {
public:
    TInsecureCredentialsProviderFactory()
    {}

    std::shared_ptr<ICredentialsProvider> CreateProvider() const override {
        return std::make_shared<TInsecureCredentialsProvider>();
    }

    std::string GetClientIdentity() const override {
        return std::string();
    }
};

class TOAuthCredentialsProvider : public ICredentialsProvider {
public:
    TOAuthCredentialsProvider(const std::string& token)
        : Token(token)
    {}

    std::string GetAuthInfo() const override {
        return Token;
    }

    bool IsValid() const override {
        return !Token.empty();
    }

private:
    std::string Token;
};

class TOAuthCredentialsProviderFactory : public ICredentialsProviderFactory {
public:
    TOAuthCredentialsProviderFactory(const std::string& token)
        : Token(token)
    {}

    std::shared_ptr<ICredentialsProvider> CreateProvider() const override {
        return std::make_shared<TOAuthCredentialsProvider>(Token);
    }

    std::string GetClientIdentity() const override {
        return Token;
    }

private:
    std::string Token;
};

std::shared_ptr<ICredentialsProviderFactory> CreateInsecureCredentialsProviderFactory() {
    return std::make_shared<TInsecureCredentialsProviderFactory>();
}
std::shared_ptr<ICredentialsProviderFactory> CreateOAuthCredentialsProviderFactory(const std::string& token) {
    return std::make_shared<TOAuthCredentialsProviderFactory>(token);
}

} // namespace NYdb


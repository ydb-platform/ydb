#include "credentials.h"
#include <util/string/cast.h>

namespace NYdb {

class TInsecureCredentialsProvider : public ICredentialsProvider {
public:
    TInsecureCredentialsProvider()
    {}

    TStringType GetAuthInfo() const override {
        return TStringType();
    }

    bool IsValid() const override {
        return false;
    }
};

TStringType ICredentialsProviderFactory::GetClientIdentity() const {
    return ToString((ui64)this);
}

class TInsecureCredentialsProviderFactory : public ICredentialsProviderFactory {
public:
    TInsecureCredentialsProviderFactory()
    {}

    std::shared_ptr<ICredentialsProvider> CreateProvider() const override {
        return std::make_shared<TInsecureCredentialsProvider>();
    }

    TStringType GetClientIdentity() const override {
        return TStringType();
    }
};

class TOAuthCredentialsProvider : public ICredentialsProvider {
public:
    TOAuthCredentialsProvider(const TStringType& token)
        : Token(token)
    {}

    TStringType GetAuthInfo() const override {
        return Token;
    }

    bool IsValid() const override {
        return !Token.empty();
    }

private:
    TStringType Token;
};

class TOAuthCredentialsProviderFactory : public ICredentialsProviderFactory {
public:
    TOAuthCredentialsProviderFactory(const TStringType& token)
        : Token(token)
    {}

    std::shared_ptr<ICredentialsProvider> CreateProvider() const override {
        return std::make_shared<TOAuthCredentialsProvider>(Token);
    }

    TStringType GetClientIdentity() const override {
        return Token;
    }

private:
    TStringType Token;
};

std::shared_ptr<ICredentialsProviderFactory> CreateInsecureCredentialsProviderFactory() {
    return std::make_shared<TInsecureCredentialsProviderFactory>();
}
std::shared_ptr<ICredentialsProviderFactory> CreateOAuthCredentialsProviderFactory(const TStringType& token) {
    return std::make_shared<TOAuthCredentialsProviderFactory>(token);
}

} // namespace NYdb


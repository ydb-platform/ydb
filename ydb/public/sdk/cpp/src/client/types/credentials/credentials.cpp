#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <util/string/cast.h>

#include <mutex>
#include <unordered_map>

namespace NYdb::inline Dev {

namespace NCredentials::NDetail {

namespace {

struct TCredentialsProviderCacheEntry {
    std::mutex Mutex;
    std::weak_ptr<ICredentialsProvider> Provider;
};

class TCredentialsProviderCache {
public:
    TCredentialsProviderPtr Get(
        const std::string& identity,
        TCredentialsProviderCreator createProvider)
    {
        std::shared_ptr<TCredentialsProviderCacheEntry> entry;
        {
            std::lock_guard guard(Mutex_);
            RemoveExpiredEntries();
            auto [it, inserted] = Entries_.try_emplace(identity);
            if (inserted) {
                it->second = std::make_shared<TCredentialsProviderCacheEntry>();
            }
            entry = it->second;
        }

        std::lock_guard guard(entry->Mutex);
        if (auto provider = entry->Provider.lock()) {
            return provider;
        }

        auto provider = createProvider();
        entry->Provider = provider;
        return provider;
    }

private:
    void RemoveExpiredEntries() {
        for (auto it = Entries_.begin(); it != Entries_.end();) {
            auto entry = it->second;
            if (entry.use_count() == 2) { // The map and this local variable.
                std::unique_lock entryLock(entry->Mutex, std::try_to_lock);
                if (entryLock.owns_lock() && entry.use_count() == 2 && entry->Provider.expired()) {
                    entryLock.unlock();
                    entry.reset();
                    it = Entries_.erase(it);
                    continue;
                }
            }
            ++it;
        }
    }

private:
    std::mutex Mutex_;
    std::unordered_map<std::string, std::shared_ptr<TCredentialsProviderCacheEntry>> Entries_;
};

} // namespace

TCredentialsProviderPtr GetOrCreateCachedProvider(
    const std::string& identity,
    TCredentialsProviderCreator createProvider)
{
    static TCredentialsProviderCache cache;
    return cache.Get(identity, std::move(createProvider));
}

} // namespace NCredentials::NDetail

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

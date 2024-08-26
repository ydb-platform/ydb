#pragma once

#include <library/cpp/cache/cache.h>
#include <library/cpp/threading/future/future.h>
#include <util/thread/pool.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/library/yql/providers/s3/credentials/credentials.h>

#include <contrib/libs/re2/re2/re2.h>

#include <memory>
#include <variant>
#include <vector>

namespace NYql {

template<typename T>
class TIterator {
public:
    virtual T Next() = 0;

    virtual bool HasNext() = 0;

    virtual ~TIterator() = default;
};

namespace NS3Lister {

enum class ES3PatternVariant { FilePattern, PathPattern };

enum class ES3PatternType {
    /**
     * Pattern may include following wildcard expressions:
     * * - any (possibly empty) sequence of characters
     * ? - single character
     * {variant1, variant2} - list of alternatives
     */
    Wildcard,
    /**
     * Pattern should be valid RE2 regex
     */
    Regexp
};

class TSharedListingContext {
public:
    TSharedListingContext(
        size_t callbackThreadCount, size_t callbackPerThreadQueueSize, size_t regexpCacheSize)
        : ThreadPoolEnabled(callbackThreadCount != 0)
        , RegexpCacheEnabled(regexpCacheSize != 0)
        , RegexpCache(regexpCacheSize) {
        if (ThreadPoolEnabled) {
            CallbackProcessingPool.Start(callbackThreadCount, callbackPerThreadQueueSize);
        }
    }

    template<typename F>
    void SubmitCallbackProcessing(F&& f) {
        if (ThreadPoolEnabled && CallbackProcessingPool.AddFunc(std::forward<F>(f))) {
            return;
        }
        f();
    }

    std::shared_ptr<RE2> GetOrCreateRegexp(const TString& regexp) {
        if (!RegexpCacheEnabled) {
            return std::make_shared<RE2>(re2::StringPiece(regexp), RE2::Options());
        }

        if (auto it = GetRegexp(regexp); it != nullptr) {
            return it;
        } else {
            auto re = std::make_shared<RE2>(re2::StringPiece(regexp), RE2::Options());
            Y_ENSURE(re->ok());
            auto wLock = TWriteGuard{RWLock};
            RegexpCache.Insert(regexp, re);
            return re;
        }
    }

    ~TSharedListingContext() {
        if (ThreadPoolEnabled) {
            CallbackProcessingPool.Stop();
        }
    }

private:
    std::shared_ptr<RE2> GetRegexp(const TString& regexp) {
        auto lock = TReadGuard{RWLock};
        if (auto it = RegexpCache.Find(regexp); it != RegexpCache.End()) {
            return *it;
        } else {
            return nullptr;
        }
    }

private:
    bool ThreadPoolEnabled = true;
    bool RegexpCacheEnabled = true;
    TThreadPool CallbackProcessingPool;
    TLRUCache<TString, std::shared_ptr<RE2>> RegexpCache;
    TRWMutex RWLock;
};

using TSharedListingContextPtr = std::shared_ptr<TSharedListingContext>;

struct TObjectListEntry {
    TString Path;
    ui64 Size = 0;
    std::vector<TString> MatchedGlobs;
};

struct TDirectoryListEntry {
    TString Path;
    bool MatchedRegexp = false;
    std::vector<TString> MatchedGlobs;
};

class TListEntries {
public:
    [[nodiscard]] size_t Size() const { return Objects.size() + Directories.size(); }
    [[nodiscard]] bool Empty() const { return Objects.empty() && Directories.empty(); }

    TListEntries& operator+=(TListEntries&& other) {
        Objects.insert(
            Objects.end(),
            std::make_move_iterator(other.Objects.begin()),
            std::make_move_iterator(other.Objects.end()));
        Directories.insert(
            Directories.end(),
            std::make_move_iterator(other.Directories.begin()),
            std::make_move_iterator(other.Directories.end()));
        return *this;
    }

public:
    std::vector<TObjectListEntry> Objects;
    std::vector<TDirectoryListEntry> Directories;
};

enum class EListError {
    GENERAL,
    LIMIT_EXCEEDED
};
struct TListError {
    EListError Type;
    TIssues Issues;
};
using TListResult = std::variant<TListEntries, TListError>;

struct TListingRequest {
    TString Url;
    TS3Credentials Credentials;
    TString Pattern;
    ES3PatternType PatternType = ES3PatternType::Wildcard;
    TString Prefix;
};

IOutputStream& operator<<(IOutputStream& stream, const TListingRequest& request);

class IS3Lister : public TIterator<NThreading::TFuture<TListResult>> {
public:
    using TPtr = std::shared_ptr<IS3Lister>;
};

IS3Lister::TPtr MakeS3Lister(
    const IHTTPGateway::TPtr& httpGateway,
    const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy,
    const TListingRequest& listingRequest,
    const TMaybe<TString>& delimiter,
    bool allowLocalFiles,
    NActors::TActorSystem* actorSystem,
    TSharedListingContextPtr sharedCtx = nullptr);

class IS3ListerFactory {
public:
    using TPtr = std::shared_ptr<IS3ListerFactory>;

    virtual NThreading::TFuture<NS3Lister::IS3Lister::TPtr> Make(
        const IHTTPGateway::TPtr& httpGateway,
        const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy,
        const NS3Lister::TListingRequest& listingRequest,
        const TMaybe<TString>& delimiter,
        bool allowLocalFiles) = 0;

    virtual ~IS3ListerFactory() = default;
};

IS3ListerFactory::TPtr MakeS3ListerFactory(
    size_t maxParallelOps,
    size_t callbackThreadCount,
    size_t callbackPerThreadQueueSize,
    size_t regexpCacheSize,
    NActors::TActorSystem* actorSystem);

} // namespace NS3Lister
} // namespace NYql

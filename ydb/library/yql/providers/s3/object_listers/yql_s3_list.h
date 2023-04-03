#pragma once

#include <library/cpp/cache/cache.h>
#include <library/cpp/threading/future/future.h>
#include <util/thread/pool.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>

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
        : RegexpCache(regexpCacheSize) {
        CallbackProcessingPool.Start(callbackThreadCount, callbackPerThreadQueueSize);
    }

    template<typename F>
    void SubmitCallbackProcessing(F&& f) {
        if (!CallbackProcessingPool.AddFunc(std::forward<F>(f))) {
            f();
        }
    }

    std::shared_ptr<RE2> GetOrCreate(const TString& regexp) {
        if (auto it = Get(regexp); it != nullptr) {
            return it;
        } else {
            auto re = std::make_shared<RE2>(re2::StringPiece(regexp), RE2::Options());
            Y_ENSURE(re->ok());
            auto wLock = TWriteGuard{RWLock};
            RegexpCache.Insert(regexp, re);
            return re;
        }
    }
    std::shared_ptr<RE2> Get(const TString& regexp) {
        auto lock = TReadGuard{RWLock};
        if (auto it = RegexpCache.Find(regexp); it != RegexpCache.End()) {
            return *it;
        } else {
            return nullptr;
        }
    }

    ~TSharedListingContext() { CallbackProcessingPool.Stop(); }

private:
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

using TListResult = std::variant<TListEntries, TIssues>;

struct TListingRequest {
    TString Url;
    TString Token;
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
    const TListingRequest& listingRequest,
    const TMaybe<TString>& delimiter,
    bool allowLocalFiles,
    TSharedListingContextPtr sharedCtx = nullptr);

class IS3ListerFactory {
public:
    using TPtr = std::shared_ptr<IS3ListerFactory>;

    virtual NThreading::TFuture<NS3Lister::IS3Lister::TPtr> Make(
        const IHTTPGateway::TPtr& httpGateway,
        const NS3Lister::TListingRequest& listingRequest,
        const TMaybe<TString>& delimiter,
        bool allowLocalFiles) = 0;

    virtual ~IS3ListerFactory() = default;
};

IS3ListerFactory::TPtr MakeS3ListerFactory(
    size_t maxParallelOps,
    size_t callbackThreadCount,
    size_t callbackPerThreadQueueSize,
    size_t regexpCacheSize);

} // namespace NS3Lister
} // namespace NYql

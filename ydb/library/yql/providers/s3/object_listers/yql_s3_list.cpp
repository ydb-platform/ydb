#include "yql_s3_list.h"
#include "yql_s3_path.h"

#include <ydb/library/yql/providers/common/http_gateway/yql_http_default_retry_policy.h>
#include <ydb/library/yql/providers/s3/common/util.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/url_builder.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <contrib/libs/re2/re2/re2.h>

#ifdef THROW
#undef THROW
#endif
#include <library/cpp/string_utils/quote/quote.h>
#include <library/cpp/threading/future/async_semaphore.h>
#include <library/cpp/xml/document/xml-document.h>
#include <util/folder/iterator.h>
#include <util/generic/guid.h>
#include <util/string/builder.h>

#include <deque>
#include <utility>

namespace NYql::NS3Lister {

IOutputStream& operator<<(IOutputStream& stream, const TListingRequest& request) {
    return stream << "TListingRequest{.url=" << request.Url
                  << ",.Prefix=" << request.Prefix
                  << ",.Pattern=" << request.Pattern
                  << ",.PatternType=" << request.PatternType
                  << ",.Credentials=" << request.Credentials << "}";
}

namespace {

using namespace NThreading;

using TPathFilter =
    std::function<bool(const TString& path, std::vector<TString>& matchedGlobs)>;
using TEarlyStopChecker = std::function<bool(const TString& path)>;

std::pair<TPathFilter, TEarlyStopChecker> MakeFilterRegexp(const TString& regex, const TSharedListingContextPtr& sharedCtx) {
    std::shared_ptr<RE2> re;
    if (sharedCtx) {
        re = sharedCtx->GetOrCreateRegexp(regex);
    } else {
        re = std::make_shared<RE2>(re2::StringPiece(regex), RE2::Options());
    }

    const size_t numGroups = re->NumberOfCapturingGroups();
    YQL_CLOG(DEBUG, ProviderS3)
        << "Got regex: '" << regex << "' with " << numGroups << " capture groups ";

    auto groups = std::make_shared<std::vector<std::string>>(numGroups);
    auto reArgs = std::make_shared<std::vector<re2::RE2::Arg>>(numGroups);
    auto reArgsPtr = std::make_shared<std::vector<re2::RE2::Arg*>>(numGroups);

    for (size_t i = 0; i < numGroups; ++i) {
        (*reArgs)[i] = &(*groups)[i];
        (*reArgsPtr)[i] = &(*reArgs)[i];
    }

    auto filter = [groups,
                   reArgs,
                   reArgsPtr,
                   re](const TString& path, std::vector<TString>& matchedGlobs) {
        matchedGlobs.clear();
        bool matched =
            re2::RE2::FullMatchN(path, *re, reArgsPtr->data(), reArgsPtr->size());
        if (matched) {
            matchedGlobs.reserve(groups->size());
            for (auto& group : *groups) {
                matchedGlobs.push_back(ToString(group));
            }
        }
        return matched;
    };

    auto checker = [](const TString& path) {
        Y_UNUSED(path);
        return false;
    };

    return std::make_pair(std::move(filter), std::move(checker));
}

std::pair<TPathFilter, TEarlyStopChecker> MakeFilterWildcard(const TString& pattern, const TSharedListingContextPtr& sharedCtx) {
    auto augmentedPattern = pattern;
    // we treat directories as 'directory/*'
    if (augmentedPattern.Empty()) {
        augmentedPattern += '*';
    }

    auto regexPatternPrefix = augmentedPattern.substr(0, NS3::GetFirstWildcardPos(augmentedPattern));
    if (regexPatternPrefix == augmentedPattern) {
        // just match for equality
        auto filter = [augmentedPattern](const TString& path, std::vector<TString>& matchedGlobs) {
            matchedGlobs.clear();
            return path == augmentedPattern;
        };

        auto checker = [augmentedPattern](const TString& path) { return path > augmentedPattern; };

        return std::make_pair(std::move(filter), std::move(checker));
    }

    const auto regex = NS3::RegexFromWildcards(augmentedPattern);
    YQL_CLOG(DEBUG, ProviderS3) << "Got prefix: '" << regexPatternPrefix << "', regex: '"
                                << regex << "' from original pattern '" << pattern << "'";

    return MakeFilterRegexp(regex, sharedCtx);
}

std::pair<TPathFilter, TEarlyStopChecker> MakeFilter(const TString& pattern, ES3PatternType patternType, const TSharedListingContextPtr& sharedCtx) {
    switch (patternType) {
        case ES3PatternType::Wildcard:
            return MakeFilterWildcard(pattern, sharedCtx);
        case ES3PatternType::Regexp:
            return MakeFilterRegexp(pattern, sharedCtx);
        default:
            ythrow yexception() << "Unknown 'patternType': " << int(patternType);
    }
}

struct TS3ListObjectV2Response {
    bool IsTruncated = false;
    ui64 MaxKeys = 0U;
    ui64 KeyCount = 0U;
    TMaybe<TString> ContinuationToken;
    std::vector<TObjectListEntry> Contents;
    std::vector<TString> CommonPrefixes;
};

TS3ListObjectV2Response ParseListObjectV2Response(
    const NXml::TDocument& xml, const TString& requestId = "UnspecifiedRequestId") {
    if (const auto& root = xml.Root(); root.Name() == "Error") {
        const auto& code = root.Node("Code", true).Value<TString>();
        const auto& message = root.Node("Message", true).Value<TString>();
        ythrow yexception() << message << ", error: code: " << code << ", request id: ["
                            << requestId << "]";
    } else if (root.Name() != "ListBucketResult") {
        ythrow yexception() << "Unexpected response '" << root.Name()
                            << "' on discovery, request id: [" << requestId << "]";
    } else {
        const NXml::TNamespacesForXPath nss(
            1U, {"s3", "http://s3.amazonaws.com/doc/2006-03-01/"});

        TS3ListObjectV2Response result;
        auto continuationTokenNode = root.Node("s3:NextContinuationToken", true, nss);
        result.ContinuationToken =
            (!continuationTokenNode.IsNull())
                ? TMaybe<TString>(continuationTokenNode.Value<TString>())
                : Nothing();
        result.IsTruncated = root.Node("s3:IsTruncated", false, nss).Value<bool>();
        result.MaxKeys = root.Node("s3:MaxKeys", false, nss).Value<size_t>();
        result.KeyCount = root.Node("s3:KeyCount", false, nss).Value<size_t>();

        const auto& prefixes = root.XPath("s3:CommonPrefixes/s3:Prefix", true, nss);
        for (const auto& prefix : prefixes) {
            auto prefixString = prefix.Value<TString>();
            result.CommonPrefixes.push_back(prefixString);
        }
        const auto& contents = root.XPath("s3:Contents", true, nss);
        for (const auto& content : contents) {
            auto& newContent = result.Contents.emplace_back();
            newContent.Path = content.Node("s3:Key", false, nss).Value<TString>();
            newContent.Size = content.Node("s3:Size", false, nss).Value<ui64>();
        }
        return result;
    }
}

class TLocalS3Lister : public IS3Lister {
public:
    TLocalS3Lister(const TListingRequest& listingRequest, const TMaybe<TString>& delimiter)
        : ListingRequest(listingRequest) {
        Y_ENSURE(!delimiter.Defined(), "delimiter is not supported for local files");
        Filter =
            MakeFilter(listingRequest.Pattern, listingRequest.PatternType, nullptr).first;
    }

    TFuture<TListResult> Next() override {
        Y_ENSURE(IsFirst, "Should not be called more than once");
        Y_ENSURE(ListingRequest.Url.substr(0, 7) == "file://");
        IsFirst = false;
        auto promise = NewPromise<TListResult>();
        try {
            auto fullPath = ListingRequest.Url.substr(7);
            for (const auto& e : TPathSplit(fullPath)) {
                if (e == "..") {
                    promise.SetException(
                        "Security violation: trying access parent directory in path");
                }
            }

            auto output = TListEntries{};
            for (const auto& entry : TDirIterator(fullPath)) {
                if (entry.fts_type != FTS_F) {
                    continue;
                }

                auto filename = TString(entry.fts_path + ListingRequest.Url.size() - 7);
                TVector<TString> matches;
                if (Filter(filename, matches)) {
                    auto& object = output.Objects.emplace_back();
                    object.Path = filename;
                    object.Size = entry.fts_statp->st_size;
                    object.MatchedGlobs.swap(matches);
                }
            }
            promise.SetValue(std::move(output));
        } catch (const std::exception& ex) {
            promise.SetException(std::current_exception());
        }
        return promise.GetFuture();
    }

    bool HasNext() override { return IsFirst; }

private:
    const TListingRequest ListingRequest;
    TPathFilter Filter;
    bool IsFirst = true;
};

class TS3Lister : public IS3Lister {
public:
    struct TListingContext {
        const TSharedListingContextPtr SharedCtx;
        // Filter
        const TPathFilter Filter;
        const TEarlyStopChecker EarlyStopChecker;
        // Result processing
        NThreading::TPromise<TListResult> Promise;
        NThreading::TPromise<TMaybe<TListingContext>> NextRequestPromise;
        const std::shared_ptr<TListEntries> Output;
        // HTTP control
        const IHTTPGateway::TWeakPtr GatewayWeak;
        const IHTTPGateway::TRetryPolicy::TPtr RetryPolicy;
        const TString RequestId;
        const TListingRequest ListingRequest;
        const TMaybe<TString> Delimiter;
        const TMaybe<TString> ContinuationToken;
        const ui64 MaxKeys;
    };

    TS3Lister(
        const IHTTPGateway::TPtr& httpGateway,
        const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy,
        const TListingRequest& listingRequest,
        const TMaybe<TString>& delimiter,
        size_t maxFilesPerQuery,
        TSharedListingContextPtr sharedCtx)
        : MaxFilesPerQuery(maxFilesPerQuery) {
        Y_ENSURE(
            listingRequest.Url.substr(0, 7) != "file://",
            "This lister does not support reading local files");

        auto [filter, checker] =
            MakeFilter(listingRequest.Pattern, listingRequest.PatternType, sharedCtx);

        auto request = listingRequest;
        request.Url = UrlEscapeRet(request.Url, true);
        auto ctx = TListingContext{
            std::move(sharedCtx),
            std::move(filter),
            std::move(checker),
            NewPromise<TListResult>(),
            NewPromise<TMaybe<TListingContext>>(),
            std::make_shared<TListEntries>(),
            IHTTPGateway::TWeakPtr(httpGateway),
            retryPolicy,
            CreateGuidAsString(),
            std::move(request),
            delimiter,
            Nothing(),
            MaxFilesPerQuery};

        YQL_CLOG(TRACE, ProviderS3)
            << "[TS3Lister] Got URL: '" << ctx.ListingRequest.Url
            << "' with path prefix '" << ctx.ListingRequest.Prefix
            << "' capture pattern '" << ctx.ListingRequest.Pattern << "' and delimiter '"
            << ctx.Delimiter.GetOrElse("NO_DELIMITER") << "'";

        auto promise = NewPromise<TMaybe<TListingContext>>();
        promise.SetValue(TMaybe<TListingContext>(ctx));
        NextRequestCtx = promise;
    }

    ~TS3Lister() override = default;
private:
    static void SubmitRequestIntoGateway(TListingContext& ctx) {
        const auto& authInfo = ctx.ListingRequest.Credentials.GetAuthInfo();
        IHTTPGateway::THeaders headers = IHTTPGateway::MakeYcHeaders(ctx.RequestId, authInfo.GetToken(), {}, authInfo.GetAwsUserPwd(), authInfo.GetAwsSigV4());

        // We have to sort the cgi parameters for the correct aws signature
        // This requirement will be fixed in the curl library
        // https://github.com/curl/curl/commit/fc76a24c53b08cdf6eec8ba787d8eac64651d56e
        // https://github.com/curl/curl/commit/c87920353883ef9d5aa952e724a8e2589d76add5
        TUrlBuilder urlBuilder(ctx.ListingRequest.Url);
        if (ctx.ContinuationToken.Defined()) {
            urlBuilder.AddUrlParam("continuation-token", *ctx.ContinuationToken);
        }
        if (ctx.Delimiter.Defined()) {
            urlBuilder.AddUrlParam("delimiter", *ctx.Delimiter);
        }

        urlBuilder.AddUrlParam("list-type", "2")
            .AddUrlParam("max-keys", TStringBuilder() << ctx.MaxKeys)
            .AddUrlParam("prefix", ctx.ListingRequest.Prefix);

        auto gateway = ctx.GatewayWeak.lock();
        if (!gateway) {
            ythrow yexception() << "Gateway disappeared";
        }

        auto sharedCtx = ctx.SharedCtx;
        auto retryPolicy = ctx.RetryPolicy;
        auto callback = CallbackFactoryMethod(std::move(ctx));
        auto httpCallback = [sharedCtx = std::move(sharedCtx),
                             callback = std::move(callback)](
                                IHTTPGateway::TResult&& result) mutable {
            if (sharedCtx) {
                sharedCtx->SubmitCallbackProcessing(
                    [callback = std::move(callback), result = std::move(result)]() mutable {
                        callback(std::move(result));
                    });
            } else {
                callback(std::move(result));
            }
        };

        gateway->Download(
            urlBuilder.Build(),
            headers,
            0U,
            0U,
            std::move(httpCallback),
            /*data=*/"",
            retryPolicy);
    }
    static IHTTPGateway::TOnResult CallbackFactoryMethod(TListingContext&& listingContext) {
        return [c = std::move(listingContext)](IHTTPGateway::TResult&& result) {
            OnDiscovery(c, std::move(result));
        };
    }

    static void OnDiscovery(TListingContext ctx, IHTTPGateway::TResult&& result) try {
        auto gateway = ctx.GatewayWeak.lock();
        if (!gateway) {
            ythrow yexception() << "Gateway disappeared";
        }
        if (!result.Issues) {
            auto xmlString = result.Content.Extract();
            const NXml::TDocument xml(xmlString, NXml::TDocument::String);
            auto parsedResponse = ParseListObjectV2Response(xml, ctx.RequestId);
            YQL_CLOG(DEBUG, ProviderS3)
                << "Listing of " << ctx.ListingRequest.Url
                << ctx.ListingRequest.Prefix << ": have " << ctx.Output->Size()
                << " entries, got another " << parsedResponse.KeyCount
                << " entries, request id: [" << ctx.RequestId << "]";

            auto earlyStop = false;
            for (const auto& content : parsedResponse.Contents) {
                if (content.Path.EndsWith('/')) {
                    // skip 'directories'
                    continue;
                }
                TVector<TString> matchedGlobs;
                if (ctx.Filter(content.Path, matchedGlobs)) {
                    auto& object = ctx.Output->Objects.emplace_back();
                    object.Path = content.Path;
                    object.Size = content.Size;
                    object.MatchedGlobs.swap(matchedGlobs);
                }
                if (ctx.EarlyStopChecker(content.Path)) {
                    earlyStop = true;
                }
            }
            for (const auto& prefix : parsedResponse.CommonPrefixes) {
                auto& directory = ctx.Output->Directories.emplace_back();
                directory.Path = prefix;
                directory.MatchedRegexp = ctx.Filter(prefix, directory.MatchedGlobs);
            }

            if (parsedResponse.IsTruncated && !earlyStop) {
                YQL_CLOG(DEBUG, ProviderS3) << "Listing of " << ctx.ListingRequest.Url
                                            << ctx.ListingRequest.Prefix
                                            << ": got truncated flag, will continue";

                auto newCtx = TListingContext{
                    ctx.SharedCtx,
                    ctx.Filter,
                    ctx.EarlyStopChecker,
                    NewPromise<TListResult>(),
                    NewPromise<TMaybe<TListingContext>>(),
                    std::make_shared<TListEntries>(),
                    ctx.GatewayWeak,
                    ctx.RetryPolicy,
                    CreateGuidAsString(),
                    ctx.ListingRequest,
                    ctx.Delimiter,
                    parsedResponse.ContinuationToken,
                    parsedResponse.MaxKeys};

                ctx.NextRequestPromise.SetValue(TMaybe<TListingContext>(newCtx));
            } else {
                ctx.NextRequestPromise.SetValue(Nothing());
            }
            ctx.Promise.SetValue(std::move(*ctx.Output));
        } else {
            auto issues = NS3Util::AddParentIssue(
                TStringBuilder{} << "request id: [" << ctx.RequestId << "]",
                std::move(result.Issues));
            YQL_CLOG(INFO, ProviderS3)
                << "Listing of " << ctx.ListingRequest.Url << ctx.ListingRequest.Prefix
                << ": got error from http gateway: " << issues.ToString(true);
            ctx.Promise.SetValue(TListError{EListError::GENERAL, std::move(issues)});
            ctx.NextRequestPromise.SetValue(Nothing());
        }
    } catch (const std::exception& ex) {
        YQL_CLOG(INFO, ProviderS3)
            << "Listing of " << ctx.ListingRequest.Url << ctx.ListingRequest.Prefix
            << " : got exception: " << ex.what();
        ctx.Promise.SetException(std::current_exception());
        ctx.NextRequestPromise.SetValue(Nothing());
    }

public:
    TFuture<TListResult> Next() override {
        auto maybeRequestCtx = NextRequestCtx.GetValueSync();

        Y_ENSURE(maybeRequestCtx.Defined());
        auto result = maybeRequestCtx->Promise;

        NextRequestCtx = maybeRequestCtx->NextRequestPromise;

        SubmitRequestIntoGateway(*maybeRequestCtx);
        return result.GetFuture();
    }

    bool HasNext() override {
        // User should process future returned from `Next()` method call before asking if more data is available
        // If returned DATA from `Next()` was NOT PROCESSED than this method might BLOCK.
        NextRequestCtx.Wait();
        return NextRequestCtx.HasValue() && NextRequestCtx.GetValue().Defined();
    }

private:
    const size_t MaxFilesPerQuery;
    TFuture<TMaybe<TListingContext>> NextRequestCtx;
};

class TS3ParallelLimitedListerFactory : public IS3ListerFactory {
public:
    using TPtr = std::shared_ptr<TS3ParallelLimitedListerFactory>;

    explicit TS3ParallelLimitedListerFactory(
        size_t maxParallelOps, TSharedListingContextPtr sharedCtx)
        : SharedCtx(std::move(sharedCtx))
        , Semaphore(TAsyncSemaphore::Make(std::max<size_t>(1, maxParallelOps))) { }

    TFuture<NS3Lister::IS3Lister::TPtr> Make(
        const IHTTPGateway::TPtr& httpGateway,
        const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy,
        const NS3Lister::TListingRequest& listingRequest,
        const TMaybe<TString>& delimiter,
        bool allowLocalFiles) override {
        auto acquired = Semaphore->AcquireAsync();
        return acquired.Apply(
            [ctx = SharedCtx, httpGateway, retryPolicy, listingRequest, delimiter, allowLocalFiles](const auto& f) {
                return std::shared_ptr<NS3Lister::IS3Lister>(new TListerLockReleaseWrapper{
                    NS3Lister::MakeS3Lister(
                        httpGateway, retryPolicy, listingRequest, delimiter, allowLocalFiles, ctx),
                    std::make_unique<TAsyncSemaphore::TAutoRelease>(
                        f.GetValue()->MakeAutoRelease())});
            });
    }

private:
    class TListerLockReleaseWrapper : public NS3Lister::IS3Lister {
    public:
        using TLockPtr = std::unique_ptr<TAsyncSemaphore::TAutoRelease>;

        TListerLockReleaseWrapper(NS3Lister::IS3Lister::TPtr listerPtr, TLockPtr lock)
            : ListerPtr(std::move(listerPtr))
            , Lock(std::move(lock)) {
            if (ListerPtr == nullptr) {
                Lock.reset();
            }
        }

        TFuture<NS3Lister::TListResult> Next() override { return ListerPtr->Next(); }
        bool HasNext() override {
            auto hasNext = ListerPtr->HasNext();
            if (!hasNext) {
                Lock.reset();
            }
            return ListerPtr->HasNext();
        }

    private:
        NS3Lister::IS3Lister::TPtr ListerPtr;
        TLockPtr Lock;
    };

private:
    TSharedListingContextPtr SharedCtx;
    const TAsyncSemaphore::TPtr Semaphore;
};

} // namespace

IS3Lister::TPtr MakeS3Lister(
    const IHTTPGateway::TPtr& httpGateway,
    const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy,
    const TListingRequest& listingRequest,
    const TMaybe<TString>& delimiter,
    bool allowLocalFiles,
    TSharedListingContextPtr sharedCtx) {
    if (listingRequest.Url.substr(0, 7) != "file://") {
        return std::make_shared<TS3Lister>(
            httpGateway, retryPolicy, listingRequest, delimiter, 1000, std::move(sharedCtx));
    }

    if (!allowLocalFiles) {
        ythrow yexception() << "Using local files as DataSource isn't allowed, but trying access "
                            << listingRequest.Url;
    }
    return std::make_shared<TLocalS3Lister>(listingRequest, delimiter);
}

IS3ListerFactory::TPtr MakeS3ListerFactory(
    size_t maxParallelOps,
    size_t callbackThreadCount,
    size_t callbackPerThreadQueueSize,
    size_t regexpCacheSize) {
    std::shared_ptr<TSharedListingContext> sharedCtx = nullptr;
    if (callbackThreadCount != 0 || regexpCacheSize != 0) {
        sharedCtx = std::make_shared<TSharedListingContext>(
            callbackThreadCount, callbackPerThreadQueueSize, regexpCacheSize);
    }
    return std::make_shared<TS3ParallelLimitedListerFactory>(maxParallelOps, sharedCtx);
}

} // namespace NYql::NS3Lister

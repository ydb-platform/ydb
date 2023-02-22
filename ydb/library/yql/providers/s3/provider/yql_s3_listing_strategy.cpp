#include "yql_s3_listing_strategy.h"

#include <ydb/library/yql/providers/s3/common/util.h>
#include <ydb/library/yql/providers/s3/object_listers/yql_s3_future_algorithms.h>
#include <ydb/library/yql/providers/s3/object_listers/yql_s3_path.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/url_builder.h>

#ifdef THROW
#undef THROW
#endif
#include <library/cpp/threading/future/async_semaphore.h>
#include <library/cpp/xml/document/xml-document.h>

#include <util/generic/overloaded.h>
#include <deque>
#include <utility>

namespace NYql {

IOutputStream& operator<<(IOutputStream& stream, ES3ListingOptions option) {
    switch (option) {
        case ES3ListingOptions::NoOptions:
            stream << "[NoOptions]";
            break;
        case ES3ListingOptions::PartitionedDataset:
            stream << "[PartitionedDataset]";
            break;
        case ES3ListingOptions::UnPartitionedDataset:
            stream << "[UnPartitionedDataset]";
            break;
        default:
            ythrow yexception() << "Undefined option: " << int(option);
    }
    return stream;
}

namespace {

using namespace NThreading;

TIssue MakeLimitExceededIssue() {
    auto issue = TIssue("Limit exceeded");
    issue.SetCode(0U, ESeverity::TSeverityIds_ESeverityId_S_WARNING);
    return issue;
}

bool IsRecoverableIssue(const TIssues& issues) {
    if (issues.Size() != 1) {
        return false;
    }

    return issues.begin()->GetSeverity() >= ESeverity::TSeverityIds_ESeverityId_S_WARNING;
}

class TCollectingS3ListingStrategy : public IS3ListingStrategy {
public:
    using TListerFactoryMethod = std::function<TFuture<NS3Lister::IS3Lister::TPtr>(
        const NS3Lister::TListingRequest& listingRequest, ES3ListingOptions options)>;

    TCollectingS3ListingStrategy(size_t limit, TListerFactoryMethod&& listerFactoryMethod)
        : Limit(limit)
        , ListerFactoryMethod(std::move(listerFactoryMethod)) { }

    TFuture<NS3Lister::TListResult> List(
        const NS3Lister::TListingRequest& listingRequest,
        ES3ListingOptions options) override {
        Y_UNUSED(options);
        auto futureLister = ListerFactoryMethod(listingRequest, options);
        return futureLister.Apply([this, listingRequest, options](
                                      const TFuture<NS3Lister::IS3Lister::TPtr>& lister) {
            try {
                return DoListCallback(lister.GetValue(), options);
            } catch (...) {
                return MakeErrorFuture<NS3Lister::TListResult>(std::current_exception());
            }
        });
    }

private:
    static inline auto MakeNewListingChunkHandler(
        NS3Lister::TListResult& state, size_t limit) {
        return [&state, limit](NS3Lister::TListEntries&& chunkEntries) {
            auto& stateEntries = std::get<NS3Lister::TListEntries>(state);
            if (stateEntries.Size() + chunkEntries.Size() > limit) {
                YQL_CLOG(INFO, ProviderS3)
                    << "[TCollectingS3ListingStrategy] Collected "
                    << stateEntries.Size() + chunkEntries.Size()
                    << " object paths which is more than limit " << limit;
                state = TIssues{MakeLimitExceededIssue()};
                return EAggregationAction::Stop;
            }
            stateEntries += std::move(chunkEntries);
            return EAggregationAction::Proceed;
        };
    }
    static inline auto MakeIssuesHandler(NS3Lister::TListResult& state) {
        return [&state](const TIssues& issues) {
            state = issues;
            return EAggregationAction::Stop;
        };
    }
    static inline EAggregationAction ExceptionHandler(
        NS3Lister::TListResult& state, const std::exception& exception) {
        state = TIssues{TIssue{exception.what()}};
        return EAggregationAction::Stop;
    }
    TFuture<NS3Lister::TListResult> DoListCallback(
        NS3Lister::IS3Lister::TPtr lister, ES3ListingOptions options) const {
        Y_UNUSED(options);
        return NYql::AccumulateWithEarlyStop<NS3Lister::TListResult>(
            std::move(lister),
            NS3Lister::TListResult{},
            [limit = Limit](NS3Lister::TListResult& state, NS3Lister::TListResult&& chunk) {
                return std::visit(
                    TOverloaded{
                        std::move(MakeNewListingChunkHandler(state, limit)),
                        std::move(MakeIssuesHandler(state))},
                    std::move(chunk));
            },
            ExceptionHandler);
    }

private:
    const size_t Limit;
    const TListerFactoryMethod ListerFactoryMethod;
};

class TFlatFileS3ListingStrategy : public TCollectingS3ListingStrategy {
public:
    TFlatFileS3ListingStrategy(
        const IS3ListerFactory::TPtr& listerFactory,
        const IHTTPGateway::TPtr& httpGateway,
        size_t limit,
        bool allowLocalFiles)
        : TCollectingS3ListingStrategy(
              limit,
              [allowLocalFiles, httpGateway, listerFactory](
                  const NS3Lister::TListingRequest& listingRequest,
                  ES3ListingOptions options) {
                  Y_UNUSED(options);
                  return listerFactory->Make(
                      httpGateway, listingRequest, Nothing(), allowLocalFiles);
              }) { }
};

class TDirectoryS3ListingStrategy : public TCollectingS3ListingStrategy {
public:
    TDirectoryS3ListingStrategy(
        const IS3ListerFactory::TPtr& listerFactory,
        const IHTTPGateway::TPtr& httpGateway,
        size_t limit,
        bool allowLocalFiles)
        : TCollectingS3ListingStrategy(
              limit,
              [allowLocalFiles, httpGateway, listerFactory](
                  const NS3Lister::TListingRequest& listingRequest,
                  ES3ListingOptions options) {
                  Y_UNUSED(options);
                  return listerFactory->Make(
                      httpGateway, listingRequest, "/", allowLocalFiles);
              }) { }
};

class TCompositeS3ListingStrategy : public IS3ListingStrategy {
public:
    using TStrategyContainer = std::vector<std::shared_ptr<IS3ListingStrategy>>;
    using TStrategyContainerPtr = std::shared_ptr<TStrategyContainer>;

    class TStrategyListIterator : public NS3Lister::IS3Lister {
    public:
        TStrategyListIterator(
            NS3Lister::TListingRequest listingRequest,
            ES3ListingOptions options,
            TStrategyContainerPtr strategies)
            : ListingRequest(std::move(listingRequest))
            , Options(options)
            , Strategies(std::move(strategies))
            , Iterator(Strategies->cbegin())
            , End(Strategies->end()) { }

        TFuture<NS3Lister::TListResult> Next() override {
            return (*Iterator)->List(ListingRequest, Options);
        }

        bool HasNext() override {
            if (!IsFirst) {
                Iterator++;
            } else {
                IsFirst = false;
            }
            return Iterator != End;
        }

    private:
        const NS3Lister::TListingRequest ListingRequest;
        const ES3ListingOptions Options;
        const TStrategyContainerPtr Strategies;
        TStrategyContainer::const_iterator Iterator;
        const TStrategyContainer::const_iterator End;
        bool IsFirst = true;
    };

    class AggregationState {
    public:
        NS3Lister::TListResult Result;
        bool Set = false;
    };

    explicit TCompositeS3ListingStrategy(TStrategyContainer&& strategies)
        : Strategies(std::make_shared<TStrategyContainer>(std::move(strategies))) { }

    TFuture<NS3Lister::TListResult> List(
        const NS3Lister::TListingRequest& listingRequest,
        ES3ListingOptions options) override {
        auto strategyListIterator =
            std::make_unique<TStrategyListIterator>(listingRequest, options, Strategies);

        return NYql::AccumulateWithEarlyStop<AggregationState>(
                   std::move(strategyListIterator),
                   AggregationState{NS3Lister::TListEntries{}},
                   [](AggregationState& state, NS3Lister::TListResult&& chunk) {
                       auto newChunkHandler =
                           [&state](NS3Lister::TListEntries&& chunkEntries) mutable {
                               YQL_CLOG(INFO, ProviderS3)
                                   << "[TCompositeS3ListingStrategy] Strategy successfully listed paths. Returning result: "
                                   << chunkEntries.Objects.size() << " objects, "
                                   << chunkEntries.Directories.size() << " path prefixes";
                               std::get<NS3Lister::TListEntries>(state.Result) =
                                   chunkEntries;
                               state.Set = true;
                               return EAggregationAction::Stop;
                           };
                       auto errorHandler = [&state](const TIssues& issues) mutable {
                           if (IsRecoverableIssue(issues)) {
                               YQL_CLOG(INFO, ProviderS3)
                                   << "[TCompositeS3ListingStrategy] Strategy failed "
                                   << " to list paths. Trying next one... ";
                               return EAggregationAction::Proceed;
                           }

                           state.Result = issues;
                           state.Set = true;
                           return EAggregationAction::Stop;
                       };
                       return std::visit(
                           TOverloaded{
                               std::move(newChunkHandler),
                               std::move(errorHandler),
                           },
                           std::move(chunk));
                   },
                   [](AggregationState& state, const std::exception& exception) {
                       state.Result = TIssues{TIssue{exception.what()}};
                       state.Set = true;
                       return EAggregationAction::Stop;
                   })
            .Apply([](const TFuture<AggregationState>& state) {
                auto& result = state.GetValue();
                return result.Set ? result.Result
                                  : TIssues{TIssue("No more strategy to test")};
            });
    }

private:
    const TStrategyContainerPtr Strategies;
};

class TConditionalS3ListingStrategy : public IS3ListingStrategy {
public:
    using TStrategyCondition = std::function<bool(ES3ListingOptions options)>;
    using TPair = std::pair<TStrategyCondition, std::shared_ptr<IS3ListingStrategy>>;

    TConditionalS3ListingStrategy(std::initializer_list<TPair> list)
        : Strategies(list.begin(), list.end()) { }

    TFuture<NS3Lister::TListResult> List(
        const NS3Lister::TListingRequest& listingRequest,
        ES3ListingOptions options) override {
        auto strategyIt = std::find_if(
            Strategies.begin(), Strategies.end(), [options](const TPair& record) {
                auto [condition, strategy] = record;
                return condition(options);
            });

        if (strategyIt == Strategies.end()) {
            auto issue = TIssues{TIssue("No strategy matched listing request")};
            return MakeFuture(NS3Lister::TListResult{issue});
        }

        return strategyIt->second->List(listingRequest, options);
    }

private:
    const std::vector<TPair> Strategies;
};

class TPartitionedDatasetS3ListingStrategy : public TCollectingS3ListingStrategy {
public:
    class PartitionedDirectoryResolverIterator : public NS3Lister::IS3Lister {
    public:
        PartitionedDirectoryResolverIterator(
            NS3Lister::TListingRequest defaultParams,
            const TString& basePrefix,
            ES3ListingOptions options,
            TDirectoryS3ListingStrategy directoryListingStrategy)
            : DefaultParams(std::move(defaultParams))
            , Options(options)
            , DirectoryPrefixQueue(std::make_shared<std::deque<TString>>())
            , DirectoryListingStrategy(std::move(directoryListingStrategy)) {
            DirectoryPrefixQueue->push_front(basePrefix);
        }

        /**
         * For each directory in listing queue:
         *  1) List path
         *  2) If files are matched against regexp - remember them (extract extra columns)
         *  3) If file is not matched against regexp - filter it out
         *  3) If directory is matched against regexp - remember them (extract extra columns)
         *  4) If directory is not matched against regexp - list this directory recursively
         *  5) if there is no directory to list - algorithm ends
         */
        TFuture<NS3Lister::TListResult> Next() override {
            First = false;
            NextDirectoryListeningChunk = GetNextPrefixLister().Apply(
                [queue = DirectoryPrefixQueue](
                    const TFuture<NS3Lister::TListResult>& future) -> NS3Lister::TListResult {
                    try {
                        auto& nextBatch = future.GetValue();
                        if (std::holds_alternative<TIssues>(nextBatch)) {
                            return std::get<TIssues>(nextBatch);
                        }

                        auto& listingResult = std::get<NS3Lister::TListEntries>(nextBatch);
                        auto result = NS3Lister::TListEntries{};
                        result.Objects.insert(
                            result.Objects.begin(),
                            listingResult.Objects.cbegin(),
                            listingResult.Objects.cend());
                        for (auto& directoryPrefix : listingResult.Directories) {
                            if (directoryPrefix.MatchedGlobs.empty()) {
                                // We need to list until extra columns are extracted
                                queue->push_back(directoryPrefix.Path);
                            } else {
                                result.Directories.push_back(directoryPrefix);
                            }
                        }

                        return NS3Lister::TListResult{result};
                    } catch (std::exception& e) {
                        return TIssues{TIssue{e.what()}};
                    }
                });
            return NextDirectoryListeningChunk;
        }

        bool HasNext() override {
            if (!First) {
                NextDirectoryListeningChunk.Wait();
            }
            return !DirectoryPrefixQueue->empty();
        }

    private:
        TFuture<NS3Lister::TListResult> GetNextPrefixLister() {
            if (DirectoryPrefixQueue->empty()) {
                return MakeFuture(
                    NS3Lister::TListResult{TIssues{TIssue{"No path to list"}}});
            }
            auto prefix = DirectoryPrefixQueue->front();
            DirectoryPrefixQueue->pop_front();

            auto request = NS3Lister::TListingRequest(DefaultParams);
            request.Prefix = prefix;

            return DirectoryListingStrategy.List(request, Options);
        }

    private:
        const NS3Lister::TListingRequest DefaultParams;
        const ES3ListingOptions Options;
        std::shared_ptr<std::deque<TString>> DirectoryPrefixQueue;
        std::vector<NS3Lister::TObjectListEntry> Objects;
        TDirectoryS3ListingStrategy DirectoryListingStrategy;
        TFuture<NS3Lister::TListResult> NextDirectoryListeningChunk;
        bool First = true;
    };

    TPartitionedDatasetS3ListingStrategy(
        const IS3ListerFactory::TPtr& listerFactory,
        const IHTTPGateway::TPtr& httpGateway,
        size_t limit,
        bool allowLocalFiles)
        : TCollectingS3ListingStrategy(
              limit,
              [listerFactory, httpGateway, limit, allowLocalFiles](
                  const NS3Lister::TListingRequest& listingRequest,
                  ES3ListingOptions options) {
                  auto ptr = std::shared_ptr<NS3Lister::IS3Lister>(
                      new PartitionedDirectoryResolverIterator{
                          listingRequest,
                          listingRequest.Prefix,
                          options,
                          TDirectoryS3ListingStrategy{
                              listerFactory, httpGateway, limit, allowLocalFiles}});
                  return MakeFuture(std::move(ptr));
              }) { }
};

class TBFSDirectoryResolverIterator : public NS3Lister::IS3Lister {
public:
    using TListingRequestFactory = std::function<NS3Lister::TListingRequest(
        const NS3Lister::TListingRequest& defaultParams, const TString& pathPrefix)>;

    TBFSDirectoryResolverIterator(
        NS3Lister::TListingRequest defaultParams,
        TListingRequestFactory listingRequestFactory,
        ES3ListingOptions options,
        std::deque<TString> initialPathPrefixes,
        TDirectoryS3ListingStrategy directoryListingStrategy,
        size_t minParallelism,
        size_t limit)
        : DefaultParams(std::move(defaultParams))
        , Options(options)
        , ListingRequestFactory(std::move(listingRequestFactory))
        , DirectoryPrefixQueue(std::move(initialPathPrefixes))
        , DirectoryListingStrategy(std::move(directoryListingStrategy))
        , MinParallelism(minParallelism)
        , Limit(limit) { }

    TFuture<NS3Lister::TListResult> Next() override {
        First = false;
        if (DirectoryPrefixQueue.empty()) {
            return MakeFuture(NS3Lister::TListResult{TIssues{TIssue{"No path to list"}}});
        }
        auto sourcePrefix = DirectoryPrefixQueue.front();
        DirectoryPrefixQueue.pop_front();
        NextDirectoryListeningChunk =
            GetPrefixLister(sourcePrefix)
                .Apply(
                    [this, sourcePrefix](const TFuture<NS3Lister::TListResult>& future)
                        -> NS3Lister::TListResult {
                        try {
                            auto& nextBatch = future.GetValue();
                            if (std::holds_alternative<TIssues>(nextBatch)) {
                                return std::get<TIssues>(nextBatch);
                            }

                            auto& listingResult =
                                std::get<NS3Lister::TListEntries>(nextBatch);
                            auto result = NS3Lister::TListEntries{};
                            auto currentListingTotalSize = ReturnedSize +
                                                           DirectoryPrefixQueue.size() +
                                                           listingResult.Size();
                            if (currentListingTotalSize > Limit) {
                                // Stop listing
                                result.Directories.push_back({.Path = sourcePrefix});
                                for (auto& directoryPrefix : DirectoryPrefixQueue) {
                                    result.Directories.push_back({.Path = directoryPrefix});
                                }
                                DirectoryPrefixQueue.clear();
                            } else {
                                result.Objects.insert(
                                    result.Objects.end(),
                                    std::make_move_iterator(listingResult.Objects.begin()),
                                    std::make_move_iterator(listingResult.Objects.end()));
                                if (currentListingTotalSize < MinParallelism) {
                                    for (auto& directoryPrefix : listingResult.Directories) {
                                        DirectoryPrefixQueue.push_back(directoryPrefix.Path);
                                    }
                                } else {
                                    for (auto& directoryPrefix : listingResult.Directories) {
                                        result.Directories.push_back(
                                            {.Path = directoryPrefix.Path});
                                    }
                                    for (auto& directoryPrefix : DirectoryPrefixQueue) {
                                        result.Directories.push_back(
                                            {.Path = directoryPrefix});
                                    }
                                    DirectoryPrefixQueue.clear();
                                }
                            }
                            ReturnedSize += result.Size();
                            return NS3Lister::TListResult{result};
                        } catch (std::exception& e) {
                            return TIssues{TIssue{e.what()}};
                        }
                    });
        return NextDirectoryListeningChunk;
    }

    bool HasNext() override {
        if (!First) {
            NextDirectoryListeningChunk.Wait();
            if (NextDirectoryListeningChunk.HasException()) {
                return false;
            }
            NextDirectoryListeningChunk.GetValue();
        }
        return !DirectoryPrefixQueue.empty();
    }

private:
    TFuture<NS3Lister::TListResult> GetPrefixLister(const TString& prefix) {
        const auto& listingRequest = ListingRequestFactory(DefaultParams, prefix);
        auto listResult = DirectoryListingStrategy.List(listingRequest, Options);
        return listResult;
    }

private:
    const NS3Lister::TListingRequest DefaultParams;
    const ES3ListingOptions Options;
    TListingRequestFactory ListingRequestFactory;
    std::deque<TString> DirectoryPrefixQueue;
    std::vector<NS3Lister::TObjectListEntry> Objects;
    TDirectoryS3ListingStrategy DirectoryListingStrategy;
    TFuture<NS3Lister::TListResult> NextDirectoryListeningChunk;
    bool First = true;
    size_t ReturnedSize = 0;
    const size_t MinParallelism;
    const size_t Limit;
};

class TUnPartitionedDatasetS3ListingStrategy : public TCollectingS3ListingStrategy {
public:
    TUnPartitionedDatasetS3ListingStrategy(
        const IS3ListerFactory::TPtr& listerFactory,
        const IHTTPGateway::TPtr& httpGateway,
        size_t limit,
        size_t minParallelism,
        bool allowLocalFiles)
        : TCollectingS3ListingStrategy(
              limit,
              [listerFactory, httpGateway, limit, minParallelism, allowLocalFiles](
                  const NS3Lister::TListingRequest& listingRequest,
                  ES3ListingOptions options) {
                  auto ptr = std::shared_ptr<NS3Lister::IS3Lister>(
                      new TBFSDirectoryResolverIterator{
                          listingRequest,
                          [](const NS3Lister::TListingRequest& defaultParams,
                             const TString& pathPrefix) {
                              NS3Lister::TListingRequest request(defaultParams);
                              request.Prefix = pathPrefix;
                              return request;
                          },
                          options,
                          std::deque<TString>{
                              (!listingRequest.Prefix.empty())
                                  ? listingRequest.Prefix
                                  : listingRequest.Pattern.substr(
                                        0, NS3::GetFirstWildcardPos(listingRequest.Pattern))},
                          TDirectoryS3ListingStrategy{
                              listerFactory, httpGateway, limit, allowLocalFiles},
                          minParallelism,
                          limit});
                  return MakeFuture(std::move(ptr));
              }) { }
};

class TSimpleS3BatchListingStrategy : public IS3BatchListingStrategy {
public:
    TSimpleS3BatchListingStrategy(
        IS3ListerFactory::TPtr listerFactory,
        IHTTPGateway::TPtr httpGateway,
        size_t limit,
        size_t minParallelism,
        bool allowLocalFiles)
        : ListerFactory(std::move(listerFactory))
        , HttpGateway(std::move(httpGateway))
        , Limit(limit)
        , MinParallelism(minParallelism)
        , AllowLocalFiles(allowLocalFiles) { }

    NThreading::TFuture<NS3Lister::TListResult> List(
        const NS3Lister::TListingRequest& baseRequest,
        const std::vector<TString>& paths,
        TPatternFactory patternFactory,
        ES3ListingOptions options) override {
        Y_ENSURE(
            options == ES3ListingOptions::UnPartitionedDataset,
            "This strategy only works for un partitioned datasets");

        auto strategy = TCollectingS3ListingStrategy(
            Limit,
            [this, paths, patternFactory](
                const NS3Lister::TListingRequest& listingRequest,
                ES3ListingOptions options) {
                auto listerPtr = std::make_shared<TBFSDirectoryResolverIterator>(
                    listingRequest,
                    [patternFactory](
                        const NS3Lister::TListingRequest& defaultParams,
                        const TString& pathPrefix) {
                        NS3Lister::TListingRequest request(defaultParams);
                        request.Prefix = pathPrefix;
                        request.Pattern = patternFactory(defaultParams, pathPrefix);
                        return request;
                    },
                    options,
                    std::deque<TString>(paths.begin(), paths.end()),
                    TDirectoryS3ListingStrategy{
                        ListerFactory, HttpGateway, Limit, AllowLocalFiles},
                    MinParallelism,
                    Limit);
                return MakeFuture(std::static_pointer_cast<NS3Lister::IS3Lister>(listerPtr));
            });

        return strategy.List(baseRequest, options);
    };

private:
    IS3ListerFactory::TPtr ListerFactory;
    const IHTTPGateway::TPtr HttpGateway;
    const size_t Limit;
    const size_t MinParallelism;
    const bool AllowLocalFiles;
};

class TS3ParallelLimitedListerFactory : public IS3ListerFactory {
public:
    using TPtr = std::shared_ptr<TS3ParallelLimitedListerFactory>;

    explicit TS3ParallelLimitedListerFactory(size_t maxParallelOps = 1)
        : Semaphore(TAsyncSemaphore::Make(std::max<size_t>(1, maxParallelOps))) { }

    TFuture<NS3Lister::IS3Lister::TPtr> Make(
        const IHTTPGateway::TPtr& httpGateway,
        const NS3Lister::TListingRequest& listingRequest,
        const TMaybe<TString>& delimiter,
        bool allowLocalFiles) override {
        auto acquired = Semaphore->AcquireAsync();
        return acquired.Apply(
            [httpGateway, listingRequest, delimiter, allowLocalFiles](const auto& f) {
                return std::shared_ptr<NS3Lister::IS3Lister>(new TListerLockReleaseWrapper{
                    NS3Lister::MakeS3Lister(
                        httpGateway, listingRequest, delimiter, allowLocalFiles),
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
    const TAsyncSemaphore::TPtr Semaphore;
};

} // namespace

IS3ListerFactory::TPtr MakeS3ListerFactory(size_t maxParallelOps) {
    return std::make_shared<TS3ParallelLimitedListerFactory>(maxParallelOps);
}

IS3ListingStrategy::TPtr MakeS3ListingStrategy(
    const IHTTPGateway::TPtr& httpGateway,
    const IS3ListerFactory::TPtr& listerFactory,
    ui64 maxFilesPerQueryFiles,
    ui64 maxFilesPerQueryDirectory,
    ui64 minDesiredDirectoriesOfFilesPerQuery,
    bool allowLocalFiles) {
    return std::make_shared<TCompositeS3ListingStrategy>(
        std::vector<std::shared_ptr<IS3ListingStrategy>>{
            std::make_shared<TFlatFileS3ListingStrategy>(
                listerFactory, httpGateway, maxFilesPerQueryFiles, allowLocalFiles),
            std::make_shared<TConditionalS3ListingStrategy>(
                std::initializer_list<TConditionalS3ListingStrategy::TPair>{
                    {[](ES3ListingOptions options) {
                         return options == ES3ListingOptions::PartitionedDataset;
                     },
                     std::make_shared<TPartitionedDatasetS3ListingStrategy>(
                         listerFactory, httpGateway, maxFilesPerQueryDirectory, allowLocalFiles)},
                    {[](ES3ListingOptions options) {
                         return options == ES3ListingOptions::UnPartitionedDataset;
                     },
                     std::make_shared<TUnPartitionedDatasetS3ListingStrategy>(
                         listerFactory,
                         httpGateway,
                         maxFilesPerQueryDirectory,
                         minDesiredDirectoriesOfFilesPerQuery,
                         allowLocalFiles)},
                })});
}

IS3BatchListingStrategy::TPtr MakeS3BatchListingStrategy(
    const IHTTPGateway::TPtr& httpGateway,
    const IS3ListerFactory::TPtr& listerFactory,
    ui64 maxFilesPerQueryFiles,
    ui64 maxFilesPerQueryDirectory,
    ui64 minDesiredDirectoriesOfFilesPerQuery,
    bool allowLocalFiles) {
    auto maxSize = std::max(maxFilesPerQueryFiles, maxFilesPerQueryDirectory);
    return std::make_shared<TSimpleS3BatchListingStrategy>(
        listerFactory,
        httpGateway,
        maxSize,
        minDesiredDirectoriesOfFilesPerQuery,
        allowLocalFiles);
}

} // namespace NYql

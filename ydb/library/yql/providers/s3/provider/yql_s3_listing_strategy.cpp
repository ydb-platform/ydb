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
IOutputStream& operator<<(IOutputStream& stream, const TS3ListingOptions& options) {
    return stream << "TS3ListingOptions{.IsPartitionedDataset="
                  << options.IsPartitionedDataset
                  << ",.IsConcurrentListing=" << options.IsConcurrentListing << "}";
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
        const NS3Lister::TListingRequest& listingRequest, TS3ListingOptions options)>;

    TCollectingS3ListingStrategy(TListerFactoryMethod&& listerFactoryMethod, TString collectingName)
        : ListerFactoryMethod(std::move(listerFactoryMethod))
        , CollectingName(std::move(collectingName)) { }

    TFuture<NS3Lister::TListResult> List(
        const NS3Lister::TListingRequest& listingRequest,
        const TS3ListingOptions& options) override {
        Y_UNUSED(options);
        YQL_CLOG(TRACE, ProviderS3) << "[TCollectingS3ListingStrategy:" << CollectingName
                                   << "] Going to list " << listingRequest;
        auto futureLister = ListerFactoryMethod(listingRequest, options);

        return futureLister.Apply([listingRequest, options, name = CollectingName, limit = options.MaxResultSet](
                                      const TFuture<NS3Lister::IS3Lister::TPtr>& lister) {
            try {
                return DoListCallback(lister.GetValue(), options, name, limit);
            } catch (...) {
                return MakeErrorFuture<NS3Lister::TListResult>(std::current_exception());
            }
        });
    }

private:
    static inline auto MakeNewListingChunkHandler(
        NS3Lister::TListResult& state, size_t limit, TString collectingName) {
        return [&state, limit, name = std::move(collectingName)](NS3Lister::TListEntries&& chunkEntries) {
            auto& stateEntries = std::get<NS3Lister::TListEntries>(state);
            if (stateEntries.Size() + chunkEntries.Size() > limit) {
                YQL_CLOG(TRACE, ProviderS3)
                    << "[TCollectingS3ListingStrategy:" << name
                    << "] Collected "
                    << stateEntries.Size() + chunkEntries.Size()
                    << " object paths which is more than limit " << limit;
                state = TIssues{MakeLimitExceededIssue()};
                return EAggregationAction::Stop;
            }
            YQL_CLOG(TRACE, ProviderS3)
                << "[TCollectingS3ListingStrategy:" << name << "] Collected "
                << stateEntries.Size() + chunkEntries.Size() << " entries. Listing limit "
                << limit << " Listing continues. ";
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
    static TFuture<NS3Lister::TListResult> DoListCallback(
        NS3Lister::IS3Lister::TPtr lister, TS3ListingOptions options, TString name, ui64 limit) {
        Y_UNUSED(options);
        return NYql::AccumulateWithEarlyStop<NS3Lister::TListResult>(
            std::move(lister),
            NS3Lister::TListResult{},
            [limit, name](NS3Lister::TListResult& state, NS3Lister::TListResult&& chunk) {
                return std::visit(
                    TOverloaded{
                        std::move(MakeNewListingChunkHandler(state, limit, std::move(name))),
                        std::move(MakeIssuesHandler(state))},
                    std::move(chunk));
            },
            ExceptionHandler);
    }

private:
    const TListerFactoryMethod ListerFactoryMethod;
    const TString CollectingName;
};

class TFlatFileS3ListingStrategy : public TCollectingS3ListingStrategy {
public:
    TFlatFileS3ListingStrategy(
        const NS3Lister::IS3ListerFactory::TPtr& listerFactory,
        const IHTTPGateway::TPtr& httpGateway,
        bool allowLocalFiles)
        : TCollectingS3ListingStrategy(
              [allowLocalFiles, httpGateway, listerFactory](
                  const NS3Lister::TListingRequest& listingRequest,
                  TS3ListingOptions options) {
                  Y_UNUSED(options);
                  return listerFactory->Make(
                      httpGateway, listingRequest, Nothing(), allowLocalFiles);
              },
              "TFlatFileS3ListingStrategy") { }
};

class TDirectoryS3ListingStrategy : public TCollectingS3ListingStrategy {
public:
    TDirectoryS3ListingStrategy(
        const NS3Lister::IS3ListerFactory::TPtr& listerFactory,
        const IHTTPGateway::TPtr& httpGateway,
        bool allowLocalFiles)
        : TCollectingS3ListingStrategy(
              [allowLocalFiles, httpGateway, listerFactory](
                  const NS3Lister::TListingRequest& listingRequest,
                  TS3ListingOptions options) {
                  Y_UNUSED(options);
                  return listerFactory->Make(
                      httpGateway, listingRequest, "/", allowLocalFiles);
              },
              "TDirectoryS3ListingStrategy") { }
};

class TCompositeS3ListingStrategy : public IS3ListingStrategy {
public:
    using TStrategyContainer = std::vector<std::shared_ptr<IS3ListingStrategy>>;
    using TStrategyContainerPtr = std::shared_ptr<TStrategyContainer>;

    class TStrategyListIterator : public NS3Lister::IS3Lister {
    public:
        TStrategyListIterator(
            NS3Lister::TListingRequest listingRequest,
            TS3ListingOptions options,
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
        const TS3ListingOptions Options;
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
        const TS3ListingOptions& options) override {
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
    using TStrategyCondition = std::function<bool(const TS3ListingOptions& options)>;
    using TPair = std::pair<TStrategyCondition, std::shared_ptr<IS3ListingStrategy>>;

    TConditionalS3ListingStrategy(std::initializer_list<TPair> list)
        : Strategies(list.begin(), list.end()) { }

    TFuture<NS3Lister::TListResult> List(
        const NS3Lister::TListingRequest& listingRequest,
        const TS3ListingOptions& options) override {
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
            TS3ListingOptions options,
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
        const TS3ListingOptions Options;
        std::shared_ptr<std::deque<TString>> DirectoryPrefixQueue;
        std::vector<NS3Lister::TObjectListEntry> Objects;
        TDirectoryS3ListingStrategy DirectoryListingStrategy;
        TFuture<NS3Lister::TListResult> NextDirectoryListeningChunk;
        bool First = true;
    };

    TPartitionedDatasetS3ListingStrategy(
        const NS3Lister::IS3ListerFactory::TPtr& listerFactory,
        const IHTTPGateway::TPtr& httpGateway,
        bool allowLocalFiles)
        : TCollectingS3ListingStrategy(
              [listerFactory, httpGateway, allowLocalFiles](
                  const NS3Lister::TListingRequest& listingRequest,
                  TS3ListingOptions options) {
                  auto ptr = std::shared_ptr<NS3Lister::IS3Lister>(
                      new PartitionedDirectoryResolverIterator{
                          listingRequest,
                          listingRequest.Prefix,
                          options,
                          TDirectoryS3ListingStrategy{
                              listerFactory, httpGateway, allowLocalFiles}});
                  return MakeFuture(std::move(ptr));
              },
              "TPartitionedDatasetS3ListingStrategy") { }
};

class TBFSDirectoryResolverIterator : public NS3Lister::IS3Lister {
public:
    using TListingRequestFactory = std::function<NS3Lister::TListingRequest(
        const NS3Lister::TListingRequest& defaultParams, const TString& pathPrefix)>;

    TBFSDirectoryResolverIterator(
        NS3Lister::TListingRequest defaultParams,
        TListingRequestFactory listingRequestFactory,
        TS3ListingOptions options,
        std::deque<TString> initialPathPrefixes,
        TDirectoryS3ListingStrategy directoryListingStrategy,
        size_t minParallelism,
        size_t limit)
        : DefaultParams(std::move(defaultParams))
        , Options(options)
        , DirectoryListingStrategy(std::move(directoryListingStrategy))
        , ListingRequestFactory(std::move(listingRequestFactory))
        , DirectoryPrefixQueue(std::move(initialPathPrefixes))
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
                        YQL_CLOG(TRACE, ProviderS3)
                            << "[TBFSDirectoryResolverIterator] Got new listing result. Collected entries: "
                            << ReturnedSize + DirectoryPrefixQueue.size();

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
    const TS3ListingOptions Options;
    TDirectoryS3ListingStrategy DirectoryListingStrategy;
    TListingRequestFactory ListingRequestFactory;

    std::deque<TString> DirectoryPrefixQueue;
    std::vector<NS3Lister::TObjectListEntry> Objects;
    TFuture<NS3Lister::TListResult> NextDirectoryListeningChunk;

    bool First = true;
    size_t ReturnedSize = 0;
    const size_t MinParallelism;
    const size_t Limit;
};

class TUnPartitionedDatasetS3ListingStrategy : public TCollectingS3ListingStrategy {
public:
    TUnPartitionedDatasetS3ListingStrategy(
        const NS3Lister::IS3ListerFactory::TPtr& listerFactory,
        const IHTTPGateway::TPtr& httpGateway,
        size_t minParallelism,
        bool allowLocalFiles)
        : TCollectingS3ListingStrategy(
              [listerFactory, httpGateway, minParallelism, allowLocalFiles](
                  const NS3Lister::TListingRequest& listingRequest,
                  TS3ListingOptions options) {
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
                              listerFactory, httpGateway, allowLocalFiles},
                          minParallelism,
                          options.MaxResultSet});
                  return MakeFuture(std::move(ptr));
              },
              "TUnPartitionedDatasetS3ListingStrategy") {}
};

// When data is collected result will be emitted (occurs once per iterator);
class TConcurrentBFSDirectoryResolverIterator : public NS3Lister::IS3Lister {
public:
    using TListingRequestFactory = std::function<NS3Lister::TListingRequest(
        const NS3Lister::TListingRequest& defaultParams, const TString& pathPrefix)>;

    struct TSharedState {
        using TLimitExceededStateModificator =
            std::function<void(TSharedState& state, const TString& pathPrefix)>;
        using TDirectoryToListMatcher =
            std::function<bool(const NS3Lister::TDirectoryListEntry& entry)>;
        using TEarlyStopMatcher = std::function<bool(const TSharedState& state)>;

        // Initial params
        const NS3Lister::TListingRequest DefaultParams;
        const TS3ListingOptions Options;
        TDirectoryS3ListingStrategy DirectoryListingStrategy;
        const TListingRequestFactory ListingRequestFactory;
        const TLimitExceededStateModificator LimitExceededStateModificator;
        const TDirectoryToListMatcher DirectoryToListMatcher;
        const TEarlyStopMatcher EarlyStopMatcher;
        // Mutable state
        std::mutex StateLock;
        std::deque<TString> DirectoryPrefixQueue;
        std::list<TString> InProgressPaths;
        std::vector<TIssues> Issues;
        std::vector<NS3Lister::TObjectListEntry> Objects;
        std::vector<NS3Lister::TDirectoryListEntry> Directories;
        std::vector<TFuture<NS3Lister::TListResult>> NextDirectoryListeningChunk;
        // CurrentListing
        TPromise<NS3Lister::TListResult> CurrentPromise;
        bool IsListingFinished = false;
        // Configuration
        const size_t Limit = 1;
        const size_t MaxParallelOps = 1;
        //
        std::weak_ptr<TSharedState> This;
    public:
        static void ListingCallback(
            const std::weak_ptr<TSharedState>& stateWeakPtr,
            const TFuture<NS3Lister::TListResult>& future,
            const TString& sourcePath
        ) {
            auto state = stateWeakPtr.lock();
            if (!state) {
                YQL_CLOG(TRACE, ProviderS3)
                    << "[TConcurrentBFSDirectoryResolverIterator] No state" << sourcePath;
                return;
            }
            YQL_CLOG(TRACE, ProviderS3) << "ListingCallback before lock";
            auto lock = std::lock_guard(state->StateLock);

            YQL_CLOG(TRACE, ProviderS3)
                << "[TConcurrentBFSDirectoryResolverIterator] Got new listing result. Collected entries: "
                << state->InProgressPaths.size() + state->DirectoryPrefixQueue.size() +
                       state->Objects.size() + state->Directories.size();

            try {
                state->RemovePathFromInProgress(sourcePath);

                if (state->IsListingFinished) {
                    YQL_CLOG(TRACE, ProviderS3)
                        << "[TConcurrentBFSDirectoryResolverIterator] Listing finished - discarding results of listing for "
                        << sourcePath;
                    return;
                }

                auto& nextChunk = future.GetValue();
                state->AddChunkToState(nextChunk, sourcePath);
                YQL_CLOG(TRACE, ProviderS3)
                    << "[TConcurrentBFSDirectoryResolverIterator] Added listing to state ";
                while (state->TryScheduleNextListing()) {
                    YQL_CLOG(TRACE, ProviderS3)
                        << "[TConcurrentBFSDirectoryResolverIterator] Scheduled new listing";
                }
            } catch (std::exception& e) {
                YQL_CLOG(TRACE, ProviderS3)
                    << "[TConcurrentBFSDirectoryResolverIterator] An exception has happened - saving an issue";
                state->Issues.emplace_back(TIssues{TIssue{e.what()}});
                state->IsListingFinished = true;
            }

            state->TrySetPromise();
            YQL_CLOG(TRACE, ProviderS3)
                << "[TConcurrentBFSDirectoryResolverIterator] Callback end";
        }
        void RemovePathFromInProgress(const TString& path) {
            Y_VERIFY(!InProgressPaths.empty());
            auto sizeBeforeRemoval = InProgressPaths.size();
            auto pos = std::find(InProgressPaths.begin(), InProgressPaths.end(), path);
            Y_VERIFY(pos != InProgressPaths.end());
            InProgressPaths.erase(pos);
            Y_VERIFY(sizeBeforeRemoval == InProgressPaths.size() + 1);
        }
        void AddChunkToState(
            const NS3Lister::TListResult& nextBatch,
            const TString& sourcePath) {

            if (std::holds_alternative<TIssues>(nextBatch)) {
                IsListingFinished = true;
                Issues.push_back(std::get<TIssues>(nextBatch));
                YQL_CLOG(TRACE, ProviderS3) << "[TConcurrentBFSDirectoryResolverIterator] AddChunkToState listing is finished due to Issue";
                return;
            }

            auto& listingResult = std::get<NS3Lister::TListEntries>(nextBatch);

            auto currentListingTotalSize = InProgressPaths.size() +
                                           DirectoryPrefixQueue.size() + Objects.size() +
                                           Directories.size() + listingResult.Size();

            // Process new listing result
            if (currentListingTotalSize > Limit) {
                IsListingFinished = true;
                YQL_CLOG(TRACE, ProviderS3) << "[TConcurrentBFSDirectoryResolverIterator] AddChunkToState listing is finished due to Limit";
                LimitExceededStateModificator(*this, sourcePath);
                return;
            }

            Objects.insert(
                Objects.end(),
                std::make_move_iterator(listingResult.Objects.begin()),
                std::make_move_iterator(listingResult.Objects.end()));

            for (auto& directoryEntry : listingResult.Directories) {
                if (DirectoryToListMatcher(directoryEntry)) {
                    DirectoryPrefixQueue.push_back(directoryEntry.Path);
                } else {
                    Directories.push_back(directoryEntry);
                }
            }

            if ((DirectoryPrefixQueue.empty() && InProgressPaths.empty()) ||
                EarlyStopMatcher(*this)) {
                YQL_CLOG(TRACE, ProviderS3)
                    << "[TConcurrentBFSDirectoryResolverIterator] AddToState listing is finished due to MinParallelism";
                IsListingFinished = true;
            }
        }

        void TrySetPromise() {
            if (!IsListingFinished) {
                YQL_CLOG(TRACE, ProviderS3) << "[TConcurrentBFSDirectoryResolverIterator] TrySetPromise listing not finished";
                return;
            }
            SetPromise();
        }
        void SetPromise() {
            Y_ENSURE(IsListingFinished);
            YQL_CLOG(TRACE, ProviderS3) << "[TConcurrentBFSDirectoryResolverIterator] SetPromise going to set promise";
            NS3Lister::TListResult res;
            if (!Issues.empty()) {
                auto result = TIssues{};
                for (auto& issues : Issues) {
                    result.AddIssues(issues);
                }
                YQL_CLOG(TRACE, ProviderS3) << "[TConcurrentBFSDirectoryResolverIterator] SetPromise before set 1";
                res = std::move(result);
            } else {
                // TODO: add verification
                auto result = NS3Lister::TListEntries{.Objects = Objects};
                for (auto& directoryPrefix : DirectoryPrefixQueue) {
                    result.Directories.push_back({.Path = directoryPrefix});
                }
                for (auto& directoryPrefix: InProgressPaths) {
                    result.Directories.push_back({.Path = directoryPrefix});
                }
                for (auto& directoryEntry : Directories) {
                    result.Directories.push_back(directoryEntry);
                }
                YQL_CLOG(TRACE, ProviderS3) << "[TConcurrentBFSDirectoryResolverIterator] SetPromise before set 2";
                res = std::move(result);
            }

            CurrentPromise.SetValue(res);
            YQL_CLOG(DEBUG, ProviderS3) << "[TConcurrentBFSDirectoryResolverIterator] SetPromise promise was set";
        }

        bool TryScheduleNextListing() {
            YQL_CLOG(TRACE, ProviderS3) << "[TConcurrentBFSDirectoryResolverIterator] TryScheduleNextListing next listing";
            if (IsListingFinished) {
                return false;
            }
            if (InProgressPaths.size() >= MaxParallelOps) {
                return false;
            }
            if (DirectoryPrefixQueue.empty()) {
                return false;
            }
            ScheduleNextListing();
            return true;
        }
        void ScheduleNextListing() {
            Y_VERIFY(!DirectoryPrefixQueue.empty());
            auto prefix = DirectoryPrefixQueue.front();
            DirectoryPrefixQueue.pop_front();
            InProgressPaths.push_back(prefix);
            YQL_CLOG(TRACE, ProviderS3) << "[TConcurrentBFSDirectoryResolverIterator] ScheduleNextListing next listing " << prefix;
            const auto& listingRequest = ListingRequestFactory(DefaultParams, prefix);

            DirectoryListingStrategy.List(listingRequest, Options)
                .Subscribe(
                    [prefix, self = This](const TFuture<NS3Lister::TListResult>& future) {
                        ListingCallback(self, future, prefix);
                    });
        }
    };
    using TSharedStatePtr = std::shared_ptr<TSharedState>;
    static TSharedStatePtr MakeState(
        NS3Lister::TListingRequest defaultParams,
        TListingRequestFactory listingRequestFactory,
        TSharedState::TLimitExceededStateModificator limitExceededStateModificator,
        TSharedState::TDirectoryToListMatcher directoryToListMatcher,
        TSharedState::TEarlyStopMatcher earlyStopMatcher,
        TS3ListingOptions options,
        std::deque<TString> initialPathPrefixes,
        TDirectoryS3ListingStrategy directoryListingStrategy,
        size_t limit,
        size_t maxParallelOps) {
        auto res = TSharedStatePtr(new TSharedState{
            .DefaultParams = (std::move(defaultParams)),
            .Options = (options),
            .DirectoryListingStrategy = (std::move(directoryListingStrategy)),
            .ListingRequestFactory = (std::move(listingRequestFactory)),
            .LimitExceededStateModificator = (std::move(limitExceededStateModificator)),
            .DirectoryToListMatcher = (std::move(directoryToListMatcher)),
            .EarlyStopMatcher = (std::move(earlyStopMatcher)),
            .DirectoryPrefixQueue = (std::move(initialPathPrefixes)),
            .CurrentPromise = NewPromise<NS3Lister::TListResult>(),
            .Limit = (limit),
            .MaxParallelOps = maxParallelOps});
        res->This = res;
        return res;
    }

    TConcurrentBFSDirectoryResolverIterator(
        NS3Lister::TListingRequest defaultParams,
        TListingRequestFactory listingRequestFactory,
        TSharedState::TLimitExceededStateModificator limitExceededStateModificator,
        TSharedState::TDirectoryToListMatcher directoryToListMatcher,
        TSharedState::TEarlyStopMatcher earlyStopMatcher,
        TS3ListingOptions options,
        std::deque<TString> initialPathPrefixes,
        TDirectoryS3ListingStrategy directoryListingStrategy,
        size_t limit,
        size_t maxParallelOps)
        : State(MakeState(
              std::move(defaultParams),
              std::move(listingRequestFactory),
              std::move(limitExceededStateModificator),
              std::move(directoryToListMatcher),
              std::move(earlyStopMatcher),
              options,
              std::move(initialPathPrefixes),
              std::move(directoryListingStrategy),
              limit,
              maxParallelOps)) { }

    TFuture<NS3Lister::TListResult> Next() override {
        if (!First) {
            return MakeFuture(NS3Lister::TListResult{TIssues{
                TIssue{"This iterator should be finished after first iteration"}}});
        }

        YQL_CLOG(TRACE, ProviderS3) << "[TConcurrentBFSDirectoryResolverIterator] Next before lock";
        auto lock = std::lock_guard{State->StateLock};

        First = false;
        if (State->DirectoryPrefixQueue.empty()) {
            return MakeFuture(NS3Lister::TListResult{TIssues{TIssue{"No path to list"}}});
        }

        if (!State->IsListingFinished) {
            YQL_CLOG(TRACE, ProviderS3) << "[TConcurrentBFSDirectoryResolverIterator] Next listing is not finished ";
            while (State->TryScheduleNextListing());
        } else {
            YQL_CLOG(TRACE, ProviderS3) << "[TConcurrentBFSDirectoryResolverIterator] Next listing is finished - reading result ";
            State->SetPromise();
        }

        return State->CurrentPromise;
    }

    bool HasNext() override {
        YQL_CLOG(TRACE, ProviderS3) << "[TConcurrentBFSDirectoryResolverIterator] HasNext";
        return (First & !State->DirectoryPrefixQueue.empty());
    }

private:
    std::shared_ptr<TSharedState> State;
    bool First = true;
};

class TConcurrentUnPartitionedDatasetS3ListingStrategy :
    public TCollectingS3ListingStrategy {
public:
    TConcurrentUnPartitionedDatasetS3ListingStrategy(
        const NS3Lister::IS3ListerFactory::TPtr& listerFactory,
        const IHTTPGateway::TPtr& httpGateway,
        size_t minParallelism,
        size_t maxParallelOps,
        bool allowLocalFiles)
        : TCollectingS3ListingStrategy(
              [listerFactory, httpGateway, minParallelism, allowLocalFiles, maxParallelOps](
                  const NS3Lister::TListingRequest& listingRequest,
                  TS3ListingOptions options) {
                  auto ptr = std::shared_ptr<NS3Lister::IS3Lister>(
                      new TConcurrentBFSDirectoryResolverIterator{
                          listingRequest,
                          [](const NS3Lister::TListingRequest& defaultParams,
                             const TString& pathPrefix) {
                              NS3Lister::TListingRequest request(defaultParams);
                              request.Prefix = pathPrefix;
                              return request;
                          },
                          [](TConcurrentBFSDirectoryResolverIterator::TSharedState& state,
                             const TString& pathPrefix) {
                              state.DirectoryPrefixQueue.push_back(pathPrefix);
                          },
                          [](const NS3Lister::TDirectoryListEntry& entry) -> bool {
                              Y_UNUSED(entry);
                              return true;
                          },
                          [minParallelism](const TConcurrentBFSDirectoryResolverIterator::TSharedState&
                                 state) -> bool {
                              auto currentListedSize = state.InProgressPaths.size() +
                                                       state.DirectoryPrefixQueue.size() +
                                                       state.Objects.size() +
                                                       state.Directories.size();
                              return currentListedSize > minParallelism;
                          },
                          options,
                          std::deque<TString>{
                              (!listingRequest.Prefix.empty())
                                  ? listingRequest.Prefix
                                  : listingRequest.Pattern.substr(
                                        0, NS3::GetFirstWildcardPos(listingRequest.Pattern))},
                          TDirectoryS3ListingStrategy{
                              listerFactory, httpGateway, allowLocalFiles},
                          options.MaxResultSet,
                          maxParallelOps});
                  return MakeFuture(std::move(ptr));
              },
              "TConcurrentUnPartitionedDatasetS3ListingStrategy") { }
};

class TConcurrentPartitionedDatasetS3ListingStrategy :
    public TCollectingS3ListingStrategy {
public:
    TConcurrentPartitionedDatasetS3ListingStrategy(
        const NS3Lister::IS3ListerFactory::TPtr& listerFactory,
        const IHTTPGateway::TPtr& httpGateway,
        size_t maxParallelOps,
        bool allowLocalFiles)
        : TCollectingS3ListingStrategy(
              [listerFactory, httpGateway, allowLocalFiles, maxParallelOps](
                  const NS3Lister::TListingRequest& listingRequest,
                  TS3ListingOptions options) {
                  auto ptr = std::shared_ptr<NS3Lister::IS3Lister>(
                      new TConcurrentBFSDirectoryResolverIterator{
                          listingRequest,
                          [](const NS3Lister::TListingRequest& defaultParams,
                             const TString& pathPrefix) {
                              NS3Lister::TListingRequest request(defaultParams);
                              request.Prefix = pathPrefix;
                              return request;
                          },
                          [](TConcurrentBFSDirectoryResolverIterator::TSharedState& state,
                             const TString& pathPrefix) {
                              Y_UNUSED(pathPrefix);
                              state.Issues.push_back(TIssues{MakeLimitExceededIssue()});
                          },
                          [](const NS3Lister::TDirectoryListEntry& entry) -> bool {
                              return !entry.MatchedRegexp;
                          },
                          [](const TConcurrentBFSDirectoryResolverIterator::TSharedState&
                                               state) -> bool {
                              Y_UNUSED(state);
                              return false;
                          },
                          options,
                          std::deque<TString>{
                              (!listingRequest.Prefix.empty())
                                  ? listingRequest.Prefix
                                  : listingRequest.Pattern.substr(
                                        0, NS3::GetFirstWildcardPos(listingRequest.Pattern))},
                          TDirectoryS3ListingStrategy{
                              listerFactory, httpGateway, allowLocalFiles},
                          options.MaxResultSet,
                          maxParallelOps});
                  return MakeFuture(std::move(ptr));
              },
              "TConcurrentUnPartitionedDatasetS3ListingStrategy") { }
};



} // namespace

class TLoggingS3ListingStrategy : public IS3ListingStrategy {
public:
    explicit TLoggingS3ListingStrategy(IS3ListingStrategy::TPtr lister)
        : Lister(std::move(lister)) { }

    TFuture<NS3Lister::TListResult> List(
        const NS3Lister::TListingRequest& listingRequest,
        const TS3ListingOptions& options) override {
        YQL_CLOG(INFO, ProviderS3) << "[TLoggingS3ListingStrategy] Going to list request "
                                   << listingRequest << " with options " << options;
        return Lister->List(listingRequest, options)
            .Apply([start = TInstant::Now()](const TFuture<NS3Lister::TListResult>& future) {
                auto duration = TInstant::Now() - start;
                std::visit(
                    TOverloaded{
                        [duration](const NS3Lister::TListEntries& entries) {
                            YQL_CLOG(INFO, ProviderS3)
                                << "[TLoggingS3ListingStrategy] Listing took " << duration
                                << " and ended with " << entries.Size() << " entries";
                        },
                        [duration](const TIssues& issues) {
                            YQL_CLOG(INFO, ProviderS3)
                                << "[TLoggingS3ListingStrategy] Listing took " << duration
                                << " and ended with " << issues.Size() << " issues";
                        }},
                    future.GetValueSync());


                return future;
            });
    }

private:
    IS3ListingStrategy::TPtr Lister;
};

IS3ListingStrategy::TPtr MakeS3ListingStrategy(
    const IHTTPGateway::TPtr& httpGateway,
    const NS3Lister::IS3ListerFactory::TPtr& listerFactory,
    ui64 minDesiredDirectoriesOfFilesPerQuery,
    size_t maxParallelOps,
    bool allowLocalFiles) {
    return std::make_shared<TLoggingS3ListingStrategy>(
        std::make_shared<TCompositeS3ListingStrategy>(
            std::vector<std::shared_ptr<IS3ListingStrategy>>{
                std::make_shared<TFlatFileS3ListingStrategy>(
                    listerFactory, httpGateway, allowLocalFiles),
                std::make_shared<TConditionalS3ListingStrategy>(
                    std::initializer_list<TConditionalS3ListingStrategy::TPair>{
                        {[](const TS3ListingOptions& options) {
                             return options.IsPartitionedDataset &&
                                    !options.IsConcurrentListing;
                         },
                         std::make_shared<TPartitionedDatasetS3ListingStrategy>(
                             listerFactory,
                             httpGateway,
                             allowLocalFiles)},
                        {[](const TS3ListingOptions& options) {
                             return options.IsPartitionedDataset &&
                                    options.IsConcurrentListing;
                         },
                         std::make_shared<TConcurrentPartitionedDatasetS3ListingStrategy>(
                             listerFactory,
                             httpGateway,
                             maxParallelOps,
                             allowLocalFiles)},
                        {[](const TS3ListingOptions& options) {
                             return !options.IsPartitionedDataset &&
                                    !options.IsConcurrentListing;
                         },
                         std::make_shared<TUnPartitionedDatasetS3ListingStrategy>(
                             listerFactory,
                             httpGateway,
                             minDesiredDirectoriesOfFilesPerQuery,
                             allowLocalFiles)},
                        {[](const TS3ListingOptions& options) {
                             return !options.IsPartitionedDataset &&
                                    options.IsConcurrentListing;
                         },
                         std::make_shared<TConcurrentUnPartitionedDatasetS3ListingStrategy>(
                             listerFactory,
                             httpGateway,
                             minDesiredDirectoriesOfFilesPerQuery,
                             maxParallelOps,
                             allowLocalFiles)}})}));
}

} // namespace NYql

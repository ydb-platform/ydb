#include "yql_s3_listing_strategy.h"

#include <ydb/library/yql/core/issue/protos/issue_id.pb.h>
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
using namespace NS3Lister;

TListError MakeLimitExceededError(
    const TString& componentName, ui64 limit, ui64 actual) {
    auto issue = TIssue(
        TStringBuilder{} << '[' << componentName << "] Limit exceeded. Limit: " << limit
                         << " Actual: " << actual);
    return TListError{EListError::LIMIT_EXCEEDED, TIssues{std::move(issue)}};
}

TListError MakeGenericError(const TString& description) {
    auto issue = TIssue(description);
    return TListError{EListError::GENERAL, TIssues{std::move(issue)}};
}


bool IsRecoverableError(const TListError& error) {
    return error.Type == EListError::LIMIT_EXCEEDED;
}

class TCollectingS3ListingStrategy : public IS3ListingStrategy {
public:
    using TListerFactoryMethod = std::function<TFuture<IS3Lister::TPtr>(
        const TListingRequest& listingRequest, TS3ListingOptions options)>;

    TCollectingS3ListingStrategy(TListerFactoryMethod&& listerFactoryMethod, TString collectingName)
        : ListerFactoryMethod(std::move(listerFactoryMethod))
        , CollectingName(std::move(collectingName)) { }

    TFuture<TListResult> List(
        const TListingRequest& listingRequest,
        const TS3ListingOptions& options) override {
        Y_UNUSED(options);
        YQL_CLOG(TRACE, ProviderS3) << "[TCollectingS3ListingStrategy:" << CollectingName
                                   << "] Going to list " << listingRequest;
        auto futureLister = ListerFactoryMethod(listingRequest, options);

        return futureLister.Apply([listingRequest, options, name = CollectingName, limit = options.MaxResultSet](
                                      const TFuture<IS3Lister::TPtr>& lister) {
            try {
                return DoListCallback(lister.GetValue(), options, name, limit);
            } catch (...) {
                return MakeErrorFuture<TListResult>(std::current_exception());
            }
        });
    }

private:
    static inline auto MakeNewListingChunkHandler(
        TListResult& state, size_t limit, TString collectingName) {
        return [&state, limit, name = std::move(collectingName)](TListEntries&& chunkEntries) {
            auto& stateEntries = std::get<TListEntries>(state);
            if (stateEntries.Size() + chunkEntries.Size() > limit) {
                YQL_CLOG(TRACE, ProviderS3)
                    << "[TCollectingS3ListingStrategy:" << name
                    << "] Collected "
                    << stateEntries.Size() + chunkEntries.Size()
                    << " object paths which is more than limit " << limit;
                state = MakeLimitExceededError(
                    name, limit, stateEntries.Size() + chunkEntries.Size());
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
    static inline auto MakeIssuesHandler(TListResult& state) {
        return [&state](const TListError& error) {
            state = error;
            return EAggregationAction::Stop;
        };
    }
    static inline EAggregationAction ExceptionHandler(
        TListResult& state, const std::exception& exception) {
        state = MakeGenericError(exception.what());
        return EAggregationAction::Stop;
    }
    static TFuture<TListResult> DoListCallback(
        IS3Lister::TPtr lister, TS3ListingOptions options, TString name, ui64 limit) {
        Y_UNUSED(options);
        return NYql::AccumulateWithEarlyStop<TListResult>(
            std::move(lister),
            TListResult{},
            [limit, name](TListResult& state, TListResult&& chunk) {
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
        const IS3ListerFactory::TPtr& listerFactory,
        const IHTTPGateway::TPtr& httpGateway,
        const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy,
        bool allowLocalFiles)
        : TCollectingS3ListingStrategy(
              [allowLocalFiles, httpGateway, retryPolicy, listerFactory](
                  const TListingRequest& listingRequest,
                  TS3ListingOptions options) {
                  Y_UNUSED(options);
                  return listerFactory->Make(
                      httpGateway, retryPolicy, listingRequest, Nothing(), allowLocalFiles);
              },
              "TFlatFileS3ListingStrategy") { }
};

class TDirectoryS3ListingStrategy : public TCollectingS3ListingStrategy {
public:
    TDirectoryS3ListingStrategy(
        const IS3ListerFactory::TPtr& listerFactory,
        const IHTTPGateway::TPtr& httpGateway,
        const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy,
        bool allowLocalFiles)
        : TCollectingS3ListingStrategy(
              [allowLocalFiles, httpGateway, retryPolicy, listerFactory](
                  const TListingRequest& listingRequest,
                  TS3ListingOptions options) {
                  Y_UNUSED(options);
                  return listerFactory->Make(
                      httpGateway, retryPolicy, listingRequest, "/", allowLocalFiles);
              },
              "TDirectoryS3ListingStrategy") { }
};

class TCompositeS3ListingStrategy : public IS3ListingStrategy {
public:
    using TStrategyContainer = std::vector<std::shared_ptr<IS3ListingStrategy>>;
    using TStrategyContainerPtr = std::shared_ptr<TStrategyContainer>;

    class TStrategyListIterator : public IS3Lister {
    public:
        TStrategyListIterator(
            TListingRequest listingRequest,
            TS3ListingOptions options,
            TStrategyContainerPtr strategies)
            : ListingRequest(std::move(listingRequest))
            , Options(options)
            , Strategies(std::move(strategies))
            , Iterator(Strategies->cbegin())
            , End(Strategies->end()) { }

        TFuture<TListResult> Next() override {
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
        const TListingRequest ListingRequest;
        const TS3ListingOptions Options;
        const TStrategyContainerPtr Strategies;
        TStrategyContainer::const_iterator Iterator;
        const TStrategyContainer::const_iterator End;
        bool IsFirst = true;
    };

    struct AggregationState {
        TListResult Result;
        std::vector<TIntrusivePtr<TIssue>> PreviousIssues;
        bool Set = false;
    };

    explicit TCompositeS3ListingStrategy(TStrategyContainer&& strategies)
        : Strategies(std::make_shared<TStrategyContainer>(std::move(strategies))) { }

    TFuture<TListResult> List(
        const TListingRequest& listingRequest,
        const TS3ListingOptions& options) override {
        auto strategyListIterator =
            std::make_unique<TStrategyListIterator>(listingRequest, options, Strategies);

        return NYql::AccumulateWithEarlyStop<AggregationState>(
                   std::move(strategyListIterator),
                   AggregationState{
                       TListEntries{}, std::vector<TIntrusivePtr<TIssue>>{}},
                   [](AggregationState& state, TListResult&& chunk) {
                       auto newChunkHandler =
                           [&state](TListEntries&& chunkEntries) mutable {
                               YQL_CLOG(INFO, ProviderS3)
                                   << "[TCompositeS3ListingStrategy] Strategy successfully listed paths. Returning result: "
                                   << chunkEntries.Objects.size() << " objects, "
                                   << chunkEntries.Directories.size() << " path prefixes";
                               std::get<TListEntries>(state.Result) =
                                   chunkEntries;
                               state.Set = true;
                               return EAggregationAction::Stop;
                           };
                       auto errorHandler = [&state](const TListError& error) mutable {
                           auto issue = MakeIntrusive<TIssue>("Strategy failed with issues");
                           for (auto& subIssue: error.Issues) {
                            issue->AddSubIssue(MakeIntrusive<TIssue>(subIssue));
                           }
                           state.PreviousIssues.emplace_back(std::move(issue));

                           if (IsRecoverableError(error)) {
                               YQL_CLOG(INFO, ProviderS3)
                                   << "[TCompositeS3ListingStrategy] Strategy failed "
                                   << " to list paths. Trying next one... ";
                               return EAggregationAction::Proceed;
                           }

                           state.Result = error;
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
                       state.Result = MakeGenericError(exception.what());
                       state.Set = true;
                       return EAggregationAction::Stop;
                   })
            .Apply([](const TFuture<AggregationState>& state) -> TListResult {
                auto& result = state.GetValue();
                if (!result.Set) {
                    auto issue = TIssue{"Couldn’t list paths in S3 source"};
                    for (auto& subIssue : result.PreviousIssues) {
                        issue.AddSubIssue(subIssue);
                    }
                    return TListError{EListError::GENERAL, TIssues{std::move(issue)}};
                }
                return result.Result;
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

    TFuture<TListResult> List(
        const TListingRequest& listingRequest,
        const TS3ListingOptions& options) override {
        auto strategyIt = std::find_if(
            Strategies.begin(), Strategies.end(), [options](const TPair& record) {
                auto [condition, strategy] = record;
                return condition(options);
            });

        if (strategyIt == Strategies.end()) {
            auto error = MakeGenericError("No strategy matched listing request");
            return MakeFuture(TListResult{std::move(error)});
        }

        return strategyIt->second->List(listingRequest, options);
    }

private:
    const std::vector<TPair> Strategies;
};

class TPartitionedDatasetS3ListingStrategy : public TCollectingS3ListingStrategy {
public:
    class PartitionedDirectoryResolverIterator : public IS3Lister {
    public:
        PartitionedDirectoryResolverIterator(
            TListingRequest defaultParams,
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
        TFuture<TListResult> Next() override {
            First = false;
            NextDirectoryListeningChunk = GetNextPrefixLister().Apply(
                [queue = DirectoryPrefixQueue](
                    const TFuture<TListResult>& future) -> TListResult {
                    try {
                        auto& nextBatch = future.GetValue();
                        if (std::holds_alternative<TListError>(nextBatch)) {
                            return std::get<TListError>(nextBatch);
                        }

                        auto& listingResult = std::get<TListEntries>(nextBatch);
                        auto result = TListEntries{};
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

                        return TListResult{result};
                    } catch (std::exception& e) {
                        return MakeGenericError(e.what());
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
        TFuture<TListResult> GetNextPrefixLister() {
            if (DirectoryPrefixQueue->empty()) {
                return MakeFuture(TListResult{MakeGenericError("No path to list")});
            }
            auto prefix = DirectoryPrefixQueue->front();
            DirectoryPrefixQueue->pop_front();

            auto request = TListingRequest(DefaultParams);
            request.Prefix = prefix;

            return DirectoryListingStrategy.List(request, Options);
        }

    private:
        const TListingRequest DefaultParams;
        const TS3ListingOptions Options;
        std::shared_ptr<std::deque<TString>> DirectoryPrefixQueue;
        std::vector<TObjectListEntry> Objects;
        TDirectoryS3ListingStrategy DirectoryListingStrategy;
        TFuture<TListResult> NextDirectoryListeningChunk;
        bool First = true;
    };

    TPartitionedDatasetS3ListingStrategy(
        const IS3ListerFactory::TPtr& listerFactory,
        const IHTTPGateway::TPtr& httpGateway,
        const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy,
        bool allowLocalFiles)
        : TCollectingS3ListingStrategy(
              [listerFactory, httpGateway, retryPolicy, allowLocalFiles](
                  const TListingRequest& listingRequest,
                  TS3ListingOptions options) {
                  auto ptr = std::shared_ptr<IS3Lister>(
                      new PartitionedDirectoryResolverIterator{
                          listingRequest,
                          listingRequest.Prefix,
                          options,
                          TDirectoryS3ListingStrategy{
                              listerFactory, httpGateway, retryPolicy, allowLocalFiles}});
                  return MakeFuture(std::move(ptr));
              },
              "TPartitionedDatasetS3ListingStrategy") { }
};

class TBFSDirectoryResolverIterator : public IS3Lister {
public:
    using TListingRequestFactory = std::function<TListingRequest(
        const TListingRequest& defaultParams, const TString& pathPrefix)>;

    TBFSDirectoryResolverIterator(
        TListingRequest defaultParams,
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

    TFuture<TListResult> Next() override {
        First = false;
        if (DirectoryPrefixQueue.empty()) {
            return MakeFuture(TListResult{MakeGenericError("No path to list")});
        }
        auto sourcePrefix = DirectoryPrefixQueue.front();
        DirectoryPrefixQueue.pop_front();
        NextDirectoryListeningChunk =
            GetPrefixLister(sourcePrefix)
                .Apply([this, sourcePrefix](const TFuture<TListResult>& future) -> TListResult {
                    YQL_CLOG(TRACE, ProviderS3)
                        << "[TBFSDirectoryResolverIterator] Got new listing result. Collected entries: "
                        << ReturnedSize + DirectoryPrefixQueue.size();

                    try {
                        auto& nextBatch = future.GetValue();
                        if (std::holds_alternative<TListError>(nextBatch)) {
                            auto& error = std::get<TListError>(nextBatch);
                            if (error.Type == EListError::LIMIT_EXCEEDED) {
                                auto result = TListEntries{};
                                PerformEarlyStop(result, sourcePrefix);
                                return result;
                            }

                            return error;
                        }

                        auto& listingResult = std::get<TListEntries>(nextBatch);
                        auto currentListingTotalSize = ReturnedSize +
                                                       DirectoryPrefixQueue.size() +
                                                       listingResult.Size();

                        auto result = TListEntries{};
                        if (currentListingTotalSize > Limit) {
                            // Stop listing
                            PerformEarlyStop(result, sourcePrefix);
                        } else {
                            ProcessDataChunk(result, listingResult, currentListingTotalSize);
                        }

                        ReturnedSize += result.Size();
                        return result;
                    } catch (std::exception& e) {
                        return MakeGenericError(e.what());
                    }
                });
        return NextDirectoryListeningChunk;
    }

    static TString ParseBasePath(const TString& path) {
        TString basePath = TString{TStringBuf{path}.RBefore('/')};
        return basePath == path && !basePath.EndsWith('/') ? TString{} : basePath;
    }

    void PerformEarlyStop(TListEntries& result, const TString& sourcePrefix) {
        result.Directories.push_back({.Path = ParseBasePath(sourcePrefix)});
        for (auto& directoryPrefix : DirectoryPrefixQueue) {
            result.Directories.push_back({.Path = directoryPrefix});
        }
        DirectoryPrefixQueue.clear();
    }
    void ProcessDataChunk(
        TListEntries& result,
        const TListEntries& listingResult,
        ui64 currentListingTotalSize) {
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
                result.Directories.push_back({.Path = directoryPrefix.Path});
            }
            for (auto& directoryPrefix : DirectoryPrefixQueue) {
                result.Directories.push_back({.Path = directoryPrefix});
            }
            DirectoryPrefixQueue.clear();
        }
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
    TFuture<TListResult> GetPrefixLister(const TString& prefix) {
        const auto& listingRequest = ListingRequestFactory(DefaultParams, prefix);
        auto listResult = DirectoryListingStrategy.List(listingRequest, Options);
        return listResult;
    }

private:
    const TListingRequest DefaultParams;
    const TS3ListingOptions Options;
    TDirectoryS3ListingStrategy DirectoryListingStrategy;
    TListingRequestFactory ListingRequestFactory;

    std::deque<TString> DirectoryPrefixQueue;
    std::vector<TObjectListEntry> Objects;
    TFuture<TListResult> NextDirectoryListeningChunk;

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
        const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy,
        size_t minParallelism,
        bool allowLocalFiles)
        : TCollectingS3ListingStrategy(
              [listerFactory, httpGateway, retryPolicy, minParallelism, allowLocalFiles](
                  const TListingRequest& listingRequest,
                  TS3ListingOptions options) {
                  auto ptr = std::shared_ptr<IS3Lister>(
                      new TBFSDirectoryResolverIterator{
                          listingRequest,
                          [](const TListingRequest& defaultParams,
                             const TString& pathPrefix) {
                              TListingRequest request(defaultParams);
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
                              listerFactory, httpGateway, retryPolicy, allowLocalFiles},
                          minParallelism,
                          options.MaxResultSet});
                  return MakeFuture(std::move(ptr));
              },
              "TUnPartitionedDatasetS3ListingStrategy") {}
};

enum class ELimitExceededAction : ui8 { Proceed = 0, RaiseError = 1 };

// When data is collected result will be emitted (occurs once per iterator);
class TConcurrentBFSDirectoryResolverIterator : public IS3Lister {
public:
    using TListingRequestFactory = std::function<TListingRequest(
        const TListingRequest& defaultParams, const TString& pathPrefix)>;

    struct TSharedState {
        using TDirectoryToListMatcher =
            std::function<bool(const TDirectoryListEntry& entry)>;
        using TEarlyStopMatcher = std::function<bool(const TSharedState& state)>;

        // Initial params
        const TListingRequest DefaultParams;
        const TS3ListingOptions Options;
        TDirectoryS3ListingStrategy DirectoryListingStrategy;
        const TListingRequestFactory ListingRequestFactory;
        const ELimitExceededAction LimitExceededAction;
        const TDirectoryToListMatcher DirectoryToListMatcher;
        const TEarlyStopMatcher EarlyStopMatcher;
        // Mutable state
        std::mutex StateLock;
        std::deque<TString> DirectoryPrefixQueue;
        std::list<TString> InProgressPaths;
        TMaybe<TListError> MaybeError;
        std::vector<TObjectListEntry> Objects;
        std::vector<TDirectoryListEntry> Directories;
        std::vector<TFuture<TListResult>> NextDirectoryListeningChunk;
        // CurrentListing
        TPromise<TListResult> CurrentPromise;
        bool IsListingFinished = false;
        // Configuration
        const size_t Limit = 1;
        const size_t MaxParallelOps = 1;
        //
        std::weak_ptr<TSharedState> This;
    public:
        static void ListingCallback(
            const std::weak_ptr<TSharedState>& stateWeakPtr,
            const TFuture<TListResult>& future,
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
                state->MaybeError = MakeGenericError(e.what());
                state->IsListingFinished = true;
            }

            state->TrySetPromise();
            YQL_CLOG(TRACE, ProviderS3)
                << "[TConcurrentBFSDirectoryResolverIterator] Callback end";
        }
        void RemovePathFromInProgress(const TString& path) {
            Y_ABORT_UNLESS(!InProgressPaths.empty());
            auto sizeBeforeRemoval = InProgressPaths.size();
            auto pos = std::find(InProgressPaths.begin(), InProgressPaths.end(), path);
            Y_ABORT_UNLESS(pos != InProgressPaths.end());
            InProgressPaths.erase(pos);
            Y_ABORT_UNLESS(sizeBeforeRemoval == InProgressPaths.size() + 1);
        }
        void HandleLimitExceeded(const TString& sourcePath, const TListError& error) {
            IsListingFinished = true;
            if (LimitExceededAction == ELimitExceededAction::RaiseError) {
                YQL_CLOG(TRACE, ProviderS3)
                    << "[TConcurrentBFSDirectoryResolverIterator] AddChunkToState listing is finished due to Limit";
                MaybeError = error;
            } else {
                DirectoryPrefixQueue.push_back(sourcePath);
            };
        }
        void AddChunkToState(
            const TListResult& nextBatch,
            const TString& sourcePath) {

            if (std::holds_alternative<TListError>(nextBatch)) {
                auto& error = std::get<TListError>(nextBatch);
                HandleLimitExceeded(sourcePath, error);
                return;
            }

            auto& listingResult = std::get<TListEntries>(nextBatch);

            auto currentListingTotalSize = InProgressPaths.size() +
                                           DirectoryPrefixQueue.size() + Objects.size() +
                                           Directories.size() + listingResult.Size();

            // Process new listing result
            if (currentListingTotalSize > Limit) {
                auto error = MakeLimitExceededError(
                    "TConcurrentBFSDirectoryResolverIterator",
                    Limit,
                    currentListingTotalSize);
                HandleLimitExceeded(sourcePath, std::move(error));
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
            TListResult res;
            if (MaybeError) {
                YQL_CLOG(TRACE, ProviderS3) << "[TConcurrentBFSDirectoryResolverIterator] SetPromise before set 1";
                res = *MaybeError;
            } else {
                // TODO: add verification
                auto result = TListEntries{.Objects = Objects};
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
            Y_ABORT_UNLESS(!DirectoryPrefixQueue.empty());
            auto prefix = DirectoryPrefixQueue.front();
            DirectoryPrefixQueue.pop_front();
            InProgressPaths.push_back(prefix);
            YQL_CLOG(TRACE, ProviderS3) << "[TConcurrentBFSDirectoryResolverIterator] ScheduleNextListing next listing " << prefix;
            const auto& listingRequest = ListingRequestFactory(DefaultParams, prefix);

            DirectoryListingStrategy.List(listingRequest, Options)
                .Subscribe(
                    [prefix, self = This](const TFuture<TListResult>& future) {
                        ListingCallback(self, future, prefix);
                    });
        }
    };
    using TSharedStatePtr = std::shared_ptr<TSharedState>;
    static TSharedStatePtr MakeState(
        TListingRequest defaultParams,
        TListingRequestFactory listingRequestFactory,
        ELimitExceededAction limitExceededAction,
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
            .LimitExceededAction = (std::move(limitExceededAction)),
            .DirectoryToListMatcher = (std::move(directoryToListMatcher)),
            .EarlyStopMatcher = (std::move(earlyStopMatcher)),
            .DirectoryPrefixQueue = (std::move(initialPathPrefixes)),
            .CurrentPromise = NewPromise<TListResult>(),
            .Limit = (limit),
            .MaxParallelOps = maxParallelOps});
        res->This = res;
        return res;
    }

    TConcurrentBFSDirectoryResolverIterator(
        TListingRequest defaultParams,
        TListingRequestFactory listingRequestFactory,
        ELimitExceededAction limitExceededAction,
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
              std::move(limitExceededAction),
              std::move(directoryToListMatcher),
              std::move(earlyStopMatcher),
              options,
              std::move(initialPathPrefixes),
              std::move(directoryListingStrategy),
              limit,
              maxParallelOps)) { }

    TFuture<TListResult> Next() override {
        if (!First) {
            return MakeFuture(TListResult{MakeGenericError(
                "This iterator should be finished after first iteration")});
        }

        YQL_CLOG(TRACE, ProviderS3) << "[TConcurrentBFSDirectoryResolverIterator] Next before lock";
        auto lock = std::lock_guard{State->StateLock};

        First = false;
        if (State->DirectoryPrefixQueue.empty()) {
            return MakeFuture(TListResult{MakeGenericError("No path to list")});
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
        const IS3ListerFactory::TPtr& listerFactory,
        const IHTTPGateway::TPtr& httpGateway,
        const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy,
        size_t minParallelism,
        size_t maxParallelOps,
        bool allowLocalFiles)
        : TCollectingS3ListingStrategy(
              [listerFactory, httpGateway, retryPolicy, minParallelism, allowLocalFiles, maxParallelOps](
                  const TListingRequest& listingRequest,
                  TS3ListingOptions options) {
                  auto ptr = std::shared_ptr<IS3Lister>(
                      new TConcurrentBFSDirectoryResolverIterator{
                          listingRequest,
                          [](const TListingRequest& defaultParams,
                             const TString& pathPrefix) {
                              TListingRequest request(defaultParams);
                              request.Prefix = pathPrefix;
                              return request;
                          },
                          ELimitExceededAction::Proceed,
                          [](const TDirectoryListEntry& entry) -> bool {
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
                              listerFactory, httpGateway, retryPolicy, allowLocalFiles},
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
        const IS3ListerFactory::TPtr& listerFactory,
        const IHTTPGateway::TPtr& httpGateway,
        const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy,
        size_t maxParallelOps,
        bool allowLocalFiles)
        : TCollectingS3ListingStrategy(
              [listerFactory, httpGateway, retryPolicy, allowLocalFiles, maxParallelOps](
                  const TListingRequest& listingRequest,
                  TS3ListingOptions options) {
                  auto ptr = std::shared_ptr<IS3Lister>(
                      new TConcurrentBFSDirectoryResolverIterator{
                          listingRequest,
                          [](const TListingRequest& defaultParams,
                             const TString& pathPrefix) {
                              TListingRequest request(defaultParams);
                              request.Prefix = pathPrefix;
                              return request;
                          },
                          ELimitExceededAction::RaiseError,
                          [](const TDirectoryListEntry& entry) -> bool {
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
                              listerFactory, httpGateway, retryPolicy, allowLocalFiles},
                          options.MaxResultSet,
                          maxParallelOps});
                  return MakeFuture(std::move(ptr));
              },
              "TConcurrentPartitionedDatasetS3ListingStrategy") { }
};



} // namespace

class TLoggingS3ListingStrategy : public IS3ListingStrategy {
public:
    explicit TLoggingS3ListingStrategy(IS3ListingStrategy::TPtr lister)
        : Lister(std::move(lister)) { }

    TFuture<TListResult> List(
        const TListingRequest& listingRequest,
        const TS3ListingOptions& options) override {
        YQL_CLOG(INFO, ProviderS3) << "[TLoggingS3ListingStrategy] Going to list request "
                                   << listingRequest << " with options " << options;
        return Lister->List(listingRequest, options)
            .Apply([start = TInstant::Now()](const TFuture<TListResult>& future) {
                auto duration = TInstant::Now() - start;
                std::visit(
                    TOverloaded{
                        [duration](const TListEntries& entries) {
                            YQL_CLOG(INFO, ProviderS3)
                                << "[TLoggingS3ListingStrategy] Listing took " << duration
                                << " and ended with " << entries.Size() << " entries";
                        },
                        [duration](const TListError& error) {
                            YQL_CLOG(INFO, ProviderS3)
                                << "[TLoggingS3ListingStrategy] Listing took " << duration
                                << " and ended with " << error.Issues.Size() << " issues";
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
    const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy,
    const IS3ListerFactory::TPtr& listerFactory,
    ui64 minDesiredDirectoriesOfFilesPerQuery,
    size_t maxParallelOps,
    bool allowLocalFiles) {
    return std::make_shared<TLoggingS3ListingStrategy>(
        std::make_shared<TCompositeS3ListingStrategy>(
            std::vector<std::shared_ptr<IS3ListingStrategy>>{
                std::make_shared<TFlatFileS3ListingStrategy>(
                    listerFactory, httpGateway, retryPolicy, allowLocalFiles),
                std::make_shared<TConditionalS3ListingStrategy>(
                    std::initializer_list<TConditionalS3ListingStrategy::TPair>{
                        {[](const TS3ListingOptions& options) {
                             return options.IsPartitionedDataset &&
                                    !options.IsConcurrentListing;
                         },
                         std::make_shared<TPartitionedDatasetS3ListingStrategy>(
                             listerFactory,
                             httpGateway,
                             retryPolicy,
                             allowLocalFiles)},
                        {[](const TS3ListingOptions& options) {
                             return options.IsPartitionedDataset &&
                                    options.IsConcurrentListing;
                         },
                         std::make_shared<TConcurrentPartitionedDatasetS3ListingStrategy>(
                             listerFactory,
                             httpGateway,
                             retryPolicy,
                             maxParallelOps,
                             allowLocalFiles)},
                        {[](const TS3ListingOptions& options) {
                             return !options.IsPartitionedDataset &&
                                    !options.IsConcurrentListing;
                         },
                         std::make_shared<TUnPartitionedDatasetS3ListingStrategy>(
                             listerFactory,
                             httpGateway,
                             retryPolicy,
                             minDesiredDirectoriesOfFilesPerQuery,
                             allowLocalFiles)},
                        {[](const TS3ListingOptions& options) {
                             return !options.IsPartitionedDataset &&
                                    options.IsConcurrentListing;
                         },
                         std::make_shared<TConcurrentUnPartitionedDatasetS3ListingStrategy>(
                             listerFactory,
                             httpGateway,
                             retryPolicy,
                             minDesiredDirectoriesOfFilesPerQuery,
                             maxParallelOps,
                             allowLocalFiles)}})}));
}

} // namespace NYql

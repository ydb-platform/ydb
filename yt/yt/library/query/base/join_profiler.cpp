#include "join_profiler.h"

#include <yt/yt/library/query/base/coordination_helpers.h>
#include <yt/yt/library/query/base/helpers.h>
#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_helpers.h>

#include <yt/yt/library/query/misc/rowset_subrange_reader.h>
#include <yt/yt/library/query/misc/rowset_writer.h>

#include <yt/yt/library/numeric/algorithm_helpers.h>

#include <yt/yt/client/query_client/query_statistics.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/pipe.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/unversioned_writer.h>

#include <library/cpp/yt/misc/range_formatters.h>

#include <library/cpp/cache/cache.h>

#include <absl/container/flat_hash_map.h>

namespace NYT::NQueryClient {

using namespace NLogging;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TJoinedRowsVectorSizeProvider
{
    size_t operator()(const std::vector<TOwningRow>& rows) const
    {
        return std::max<size_t>(rows.size(), 1);
    }
};

using TJoinRowsCache = TLRUCache<TOwningRow, std::vector<TOwningRow>, TNoopDelete, TJoinedRowsVectorSizeProvider>;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TBatchReader)

class TBatchReader
    : public ISchemafulUnversionedReader
{
public:
    explicit TBatchReader(IUnversionedRowBatchPtr batch)
        : Batch_(std::move(batch))
    { }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& /*options*/) override
    {
        return std::exchange(Batch_, {});
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        return {};
    }
    NChunkClient::TCodecStatistics GetDecompressionStatistics() const override
    {
        return {};
    }
    bool IsFetchingCompleted() const override
    {
        return true;
    }
    std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override
    {
        return {};
    }

    TFuture<void> GetReadyEvent() const override
    {
        return OKFuture;
    }

private:
    IUnversionedRowBatchPtr Batch_;
};

DEFINE_REFCOUNTED_TYPE(TBatchReader)

ISchemafulUnversionedReaderPtr ExecutePlanAndGetReader(
    const TExecutePlan& executePlan,
    const TConsumeSubqueryStatistics& consumeSubqueryStatistics,
    const IMemoryChunkProviderPtr& memoryChunkProvider,
    TPlanFragment fragment)
{
    auto pipe = NTableClient::CreateSchemafulPipe(memoryChunkProvider);

    executePlan(std::move(fragment), pipe->GetWriter())
        .AsUnique().Subscribe(BIND([consumeSubqueryStatistics, pipe] (TErrorOr<TQueryStatistics>&& error) {
            if (!error.IsOK()) {
                pipe->Fail(error);
            } else {
                consumeSubqueryStatistics(std::move(error.Value()));
            }
        }));

    return pipe->GetReader();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TForeignJoinRowsPrefetcher)

class TForeignJoinRowsPrefetcher
    : public IJoinRowsProducer
{
public:
    TForeignJoinRowsPrefetcher(
        TFuture<TSharedRange<TRow>> asyncRows,
        size_t foreignKeyPrefix)
        : AsyncRows_(std::move(asyncRows))
        , ForeignKeyPrefix_(foreignKeyPrefix)
    { }

    ISchemafulUnversionedReaderPtr FetchJoinedRows(std::vector<TRow> keys, TRowBufferPtr /*permanentBuffer*/) override {
        if (keys.empty()) {
            return New<TBatchReader>(/*batch*/ nullptr);
        } else {
            return CreateRowsetSubrangeReader(
                AsyncRows_,
                {
                    TKeyBoundRef(keys.front().FirstNElements(ForeignKeyPrefix_), /*inclusive*/ true, /*upper*/ false),
                    TKeyBoundRef(keys.back().FirstNElements(ForeignKeyPrefix_), /*inclusive*/ true, /*upper*/ true),
                });
        }
    }

private:
    const TFuture<TSharedRange<TRow>> AsyncRows_;
    const size_t ForeignKeyPrefix_;
};

DEFINE_REFCOUNTED_TYPE(TForeignJoinRowsPrefetcher)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TJoinSubqueryProfiler)

class TJoinSubqueryProfiler
    : public virtual IJoinProfiler
    , public virtual IJoinRowsProducer
{
public:
    TJoinSubqueryProfiler(
        TConstJoinClausePtr joinClause,
        TExecutePlan executeForeign,
        TConsumeSubqueryStatistics consumeSubqueryStatistics,
        TGetPrefetchJoinDataSource getPrefetchJoinDataSource,
        IMemoryChunkProviderPtr memoryChunkProvider,
        bool useOrderByInJoinSubqueries,
        bool allowHeavyRangeInferenceInJoins,
        std::optional<i64> cacheSize,
        TLogger logger)
        : JoinClause_(std::move(joinClause))
        , ExecutePlan_(std::move(executeForeign))
        , ConsumeSubqueryStatistics_(std::move(consumeSubqueryStatistics))
        , GetPrefetchJoinDataSource_(std::move(getPrefetchJoinDataSource))
        , MemoryChunkProvider_(std::move(memoryChunkProvider))
        , UseOrderByInJoinSubqueries_(useOrderByInJoinSubqueries)
        , AllowHeavyRangeInferenceInJoins_(allowHeavyRangeInferenceInJoins)
        , Cache_(cacheSize
            ? std::make_unique<TJoinRowsCache>(*cacheSize, /*multiValue*/ false, TJoinedRowsVectorSizeProvider{})
            : nullptr)
        , Logger(std::move(logger))
    { }

    IJoinRowsProducerPtr Profile() override
    {
        if (auto dataSource = GetPrefetchJoinDataSource_()) {
            dataSource->ObjectId = JoinClause_->ForeignObjectId;
            dataSource->CellId = JoinClause_->ForeignCellId;

            auto joinSubquery = JoinClause_->GetJoinSubquery();
            joinSubquery->InferRanges = false;
            if (UseOrderByInJoinSubqueries_) {
                joinSubquery->OrderClause = MakeOrderByPrefixClause(*JoinClause_);
            }
            joinSubquery->ScanOrder = EScanOrder::Ordered;
            joinSubquery->Prefetching = true;

            YT_TLOG_DEBUG("Evaluating remote subquery")
                .With("SubqueryId", joinSubquery->Id);

            auto writer = New<TSimpleRowsetWriter>(MemoryChunkProvider_);

            ExecutePlan_(
                TPlanFragment{
                    .Query = std::move(joinSubquery),
                    .DataSource = std::move(*dataSource),
                },
                writer)
                .AsUnique().Subscribe(BIND([this, this_ = MakeStrong(this), writer] (TErrorOr<TQueryStatistics>&& error) {
                    if (!error.IsOK()) {
                        writer->Fail(error);
                    } else {
                        ConsumeSubqueryStatistics_(std::move(error.Value()));
                    }
                }));

            return New<TForeignJoinRowsPrefetcher>(writer->GetResult(), JoinClause_->ForeignKeyPrefix);
        } else {
            return this;
        }
    }

    ISchemafulUnversionedReaderPtr FetchJoinedRows(
        std::vector<TRow> keys,
        TRowBufferPtr permanentBuffer) override
    {
        if (keys.empty()) {
            return ISchemafulUnversionedReaderPtr{};
        }

        if (Cache_) {
            return FetchJoinedRowsUsingCache(std::move(keys), std::move(permanentBuffer));
        }

        auto joinFragment = GetForeignQuery(std::move(keys), std::move(permanentBuffer));

        YT_TLOG_DEBUG("Evaluating remote subquery")
            .With("SubqueryId", joinFragment.Query->Id);

        return ExecutePlanAndGetReader(ExecutePlan_, ConsumeSubqueryStatistics_, MemoryChunkProvider_, std::move(joinFragment));
    }

private:
    const TConstJoinClausePtr JoinClause_;
    const TExecutePlan ExecutePlan_;
    const TConsumeSubqueryStatistics ConsumeSubqueryStatistics_;
    const TGetPrefetchJoinDataSource GetPrefetchJoinDataSource_;
    const IMemoryChunkProviderPtr MemoryChunkProvider_;
    const bool UseOrderByInJoinSubqueries_;
    const bool AllowHeavyRangeInferenceInJoins_;

    const std::unique_ptr<TJoinRowsCache> Cache_;

    const TLogger Logger;

    ISchemafulUnversionedReaderPtr FetchJoinedRowsUsingCache(
        std::vector<TRow> keys,
        TRowBufferPtr permanentBuffer)
    {
        int foreignKeyPrefix = JoinClause_->ForeignKeyPrefix;
        int joinKeySize = JoinClause_->SelfEquations.size();

        std::vector<TOwningRow> cachedRows;
        std::vector<TOwningRow> missingOwningKeys;
        i64 keyCount = std::ssize(keys);
        i64 missingKeyCount = 0;

        for (auto key : keys) {
            TOwningRow keyOwning(key);
            auto it = Cache_->Find(keyOwning);
            if (it != Cache_->End()) {
                for (const auto& row : it.Value()) {
                    cachedRows.push_back(row);
                }
            } else {
                missingOwningKeys.push_back(std::move(keyOwning));
                keys[missingKeyCount++] = key;
            }
        }
        keys.resize(missingKeyCount);

        YT_TLOG_DEBUG("Collected join rows from cache")
            .With("MissCount", missingKeyCount)
            .With("HitCount", keyCount - std::ssize(missingOwningKeys))
            .With("FoundCachedRowCount", cachedRows.size());

        if (missingOwningKeys.empty()) {
            std::vector<TRow> rows;
            rows.reserve(cachedRows.size());
            for (const auto& owningRow : cachedRows) {
                rows.push_back(TRow(owningRow));
            }
            return New<TBatchReader>(CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows), std::move(cachedRows))));
        }

        auto joinFragment = GetForeignQuery(std::move(keys), permanentBuffer);

        YT_TLOG_DEBUG("Evaluating remote subquery with cache")
            .With("SubqueryId", joinFragment.Query->Id);

        auto writer = New<TSimpleRowsetWriter>(MemoryChunkProvider_);

        ExecutePlan_(joinFragment, writer)
            .AsUnique().Subscribe(BIND([this, this_ = MakeStrong(this), writer] (TErrorOr<TQueryStatistics>&& error) {
                if (!error.IsOK()) {
                    writer->Fail(error);
                } else {
                    ConsumeSubqueryStatistics_(std::move(error.Value()));
                }
            }));

        auto asyncMerged = writer->GetResult().Apply(
            BIND([this, this_ = MakeStrong(this),
                cachedRows = std::move(cachedRows),
                sortedMissingKeys = std::move(missingOwningKeys),
                foreignKeyPrefix,
                joinKeySize
            ] (const TSharedRange<TRow>& subqueryRows) mutable {
                auto joinKeyLess = [&] (const TOwningRow& lhs, TRow rhs) {
                    return CompareValueRanges(lhs.Elements(), rhs.FirstNElements(joinKeySize)) < 0;
                };
                auto joinKeyEqual = [&] (TRow lhs, const TOwningRow& rhs) {
                    return CompareValueRanges(lhs.FirstNElements(joinKeySize), rhs.Elements()) == 0;
                };

                std::vector<TOwningRow> fetched;
                fetched.reserve(subqueryRows.size());
                std::vector<bool> seen(sortedMissingKeys.size(), false);

                for (auto row : subqueryRows) {
                    auto lb = std::lower_bound(
                        sortedMissingKeys.begin(),
                        sortedMissingKeys.end(),
                        row,
                        joinKeyLess);
                    if (lb != sortedMissingKeys.end() && joinKeyEqual(row, *lb)) {
                        seen[lb - sortedMissingKeys.begin()] = true;
                        fetched.emplace_back(row);
                    }
                }

                std::sort(fetched.begin(), fetched.end(), [&] (const TOwningRow& lhs, const TOwningRow& rhs) {
                    return CompareValueRanges(lhs.FirstNElements(joinKeySize), rhs.FirstNElements(joinKeySize)) < 0;
                });

                for (auto groupIt = fetched.begin(); groupIt != fetched.end(); ) {
                    auto groupEnd = groupIt;
                    while (groupEnd != fetched.end() &&
                        CompareValueRanges(groupEnd->FirstNElements(joinKeySize), groupIt->FirstNElements(joinKeySize)) == 0)
                    {
                        ++groupEnd;
                    }
                    auto lb = std::lower_bound(sortedMissingKeys.begin(), sortedMissingKeys.end(), TRow(*groupIt), joinKeyLess);
                    Cache_->Insert(*lb, std::vector<TOwningRow>(groupIt, groupEnd));
                    groupIt = groupEnd;
                }

                for (size_t i = 0; i < sortedMissingKeys.size(); ++i) {
                    if (!seen[i]) {
                        Cache_->Insert(sortedMissingKeys[i], {});
                    }
                }

                std::vector<TRow> merged;
                merged.reserve(cachedRows.size() + fetched.size());
                std::merge(
                    cachedRows.begin(), cachedRows.end(),
                    fetched.begin(), fetched.end(),
                    std::back_inserter(merged),
                    [foreignKeyPrefix] (const TOwningRow& lhs, const TOwningRow& rhs) {
                        return CompareValueRanges(
                            lhs.FirstNElements(foreignKeyPrefix),
                            rhs.FirstNElements(foreignKeyPrefix)) < 0;
                    });

                return MakeSharedRange(std::move(merged), std::move(cachedRows), std::move(fetched));
            }));

        return CreateRowsetSubrangeReader(std::move(asyncMerged));
    }

    TPlanFragment GetForeignQuery(
        std::vector<TRow> keys,
        TRowBufferPtr buffer)
    {
        TDataSource dataSource;
        dataSource.ObjectId = JoinClause_->ForeignObjectId;
        dataSource.CellId = JoinClause_->ForeignCellId;

        const auto& foreignEquations = JoinClause_->ForeignEquations;
        auto foreignKeyPrefix = JoinClause_->ForeignKeyPrefix;
        auto newQuery = JoinClause_->GetJoinSubquery();

        auto mode = GetJoinSubqueryMode(*JoinClause_, Logger);

        if (mode == EJoinSubqueryMode::FullScan || mode == EJoinSubqueryMode::PredicateGuidedScan) {
            YT_TLOG_DEBUG("Using join via IN clause");

            TRowRanges universalRange{{
                buffer->CaptureRow(NTableClient::MinKey().Get()),
                buffer->CaptureRow(NTableClient::MaxKey().Get()),
            }};

            dataSource.Ranges = MakeSharedRange(std::move(universalRange), buffer);

            auto inClause = New<TInExpression>(
                foreignEquations,
                MakeSharedRange(std::move(keys), std::move(buffer)));

            newQuery->WhereClause = MakeAndExpression(inClause, newQuery->WhereClause);

            if (JoinClause_->Schema.Original->HasComputedColumns() &&
                AllComputedColumnsEvaluated(*JoinClause_))
            {
                if (AllowHeavyRangeInferenceInJoins_) {
                    YT_TLOG_DEBUG("Using heavy range inference in join subquery");
                } else {
                    newQuery->ForceLightRangeInference = true;
                    YT_TLOG_DEBUG("Using light range inference in join subquery");
                }
            }

            if (foreignKeyPrefix > 0) {
                if (UseOrderByInJoinSubqueries_) {
                    newQuery->OrderClause = MakeOrderByPrefixClause(*JoinClause_);
                }
                newQuery->Prefetching = true;
                newQuery->ScanOrder = EScanOrder::Ordered;
            } else {
                newQuery->ScanOrder = EScanOrder::Unordered;
            }
        } else {
            if (mode == EJoinSubqueryMode::Lookup) {
                YT_TLOG_DEBUG("Using join via source ranges");
                dataSource.Keys = MakeSharedRange(std::move(keys), std::move(buffer));
            } else {
                YT_TLOG_DEBUG("Using join via prefix ranges");
                std::vector<TRow> prefixKeys;
                prefixKeys.reserve(keys.size());
                for (auto key : keys) {
                    prefixKeys.push_back(buffer->CaptureRow(
                        TRange(key.Begin(), foreignKeyPrefix),
                        /*captureValues*/ false));
                }
                prefixKeys.erase(std::unique(prefixKeys.begin(), prefixKeys.end()), prefixKeys.end());
                dataSource.Keys = MakeSharedRange(std::move(prefixKeys), std::move(buffer));
            }

            newQuery->InferRanges = false;
            newQuery->Prefetching = true;
            newQuery->ScanOrder = EScanOrder::Ordered;
            if (UseOrderByInJoinSubqueries_) {
                newQuery->OrderClause = MakeOrderByPrefixClause(*JoinClause_);
            }
        }

        newQuery->GroupClause = JoinClause_->GroupClause;

        return {
            .Query = std::move(newQuery),
            .DataSource = std::move(dataSource),
            .SubqueryFragment = nullptr,
        };
    }

    static bool AllComputedColumnsEvaluated(const TJoinClause& joinClause)
    {
        auto isColumnInEquations = [&] (TStringBuf column) {
            for (const auto& equation : joinClause.ForeignEquations) {
                if (const auto* reference = equation->As<TReferenceExpression>();
                    reference && column == reference->ColumnName)
                {
                    return true;
                }
            }

            return false;
        };

        auto renamedSchema = joinClause.Schema.GetRenamedSchema();

        for (const auto& column : renamedSchema->Columns()) {
            if (!column.SortOrder()) {
                break;
            }
            if (!column.Expression()) {
                continue;
            }
            if (!isColumnInEquations(column.Name())) {
                return false;
            }
        }

        return true;
    }

    static TConstOrderClausePtr MakeOrderByPrefixClause(const TJoinClause& joinClause)
    {
        YT_VERIFY(joinClause.ForeignKeyPrefix > 0);

        auto order = New<TOrderClause>();

        if (joinClause.GroupClause) {
            YT_VERIFY(joinClause.GroupClause->GroupItems.size() >= joinClause.ForeignKeyPrefix);

            for (size_t index = 0; index < joinClause.ForeignKeyPrefix; ++index) {
                const auto& item = joinClause.GroupClause->GroupItems[index];

                YT_VERIFY(Compare(item.Expression, joinClause.ForeignEquations[index]));

                order->OrderItems.push_back(TOrderItem{
                    .Expression = New<TReferenceExpression>(
                        item.Expression->LogicalType,
                        item.Name),
                    .Descending = false,
                });
            }
        } else {
            auto renamedSchema = joinClause.Schema.GetRenamedSchema();

            for (size_t index = 0; index < joinClause.ForeignKeyPrefix; ++index) {
                const auto& column = renamedSchema->Columns()[index];
                order->OrderItems.push_back(TOrderItem{
                    .Expression = New<TReferenceExpression>(column.LogicalType(), column.Name()),
                    .Descending = false,
                });
            }
        }

        return order;
    }
};

DEFINE_REFCOUNTED_TYPE(TJoinSubqueryProfiler)

////////////////////////////////////////////////////////////////////////////////

EJoinSubqueryMode GetJoinSubqueryMode(const TJoinClause& joinClause, const NLogging::TLogger& Logger)
{
    const auto& foreignEquations = joinClause.ForeignEquations;
    auto foreignKeyPrefix = joinClause.ForeignKeyPrefix;

    bool predicateRefines = false;

    if (joinClause.Predicate) {
        auto keyColumns = joinClause.Schema.GetKeyColumns();

        auto dummyInClause = New<TInExpression>(foreignEquations, /*values*/ nullptr);
        auto dummyWhereClause = MakeAndExpression(std::move(dummyInClause), joinClause.Predicate);
        auto signature = GetExpressionConstraintSignature(std::move(dummyWhereClause), keyColumns);
        auto score = GetConstraintSignatureScore(signature);

        YT_TLOG_DEBUG("Calculated score for join via IN with predicate")
            .With("Signature", signature)
            .With("Score", score);

        predicateRefines = score > static_cast<int>(2 * foreignKeyPrefix);
    }

    if (foreignKeyPrefix == 0) {
        return EJoinSubqueryMode::FullScan;
    } else if (predicateRefines) {
        return EJoinSubqueryMode::PredicateGuidedScan;
    } else if (foreignKeyPrefix == foreignEquations.size()) {
        return EJoinSubqueryMode::Lookup;
    } else {
        return EJoinSubqueryMode::PrefixScan;
    }
}

////////////////////////////////////////////////////////////////////////////////

IJoinProfilerPtr CreateJoinSubqueryProfiler(
    TConstJoinClausePtr joinClause,
    TExecutePlan executeForeign,
    TConsumeSubqueryStatistics consumeSubqueryStatistics,
    TGetPrefetchJoinDataSource getPrefetchJoinDataSource,
    IMemoryChunkProviderPtr memoryChunkProvider,
    bool useOrderByInJoinSubqueries,
    bool allowHeavyRangeInferenceInJoins,
    std::optional<i64> cacheSize,
    TLogger logger)
{
    return New<TJoinSubqueryProfiler>(
        std::move(joinClause),
        std::move(executeForeign),
        std::move(consumeSubqueryStatistics),
        std::move(getPrefetchJoinDataSource),
        std::move(memoryChunkProvider),
        useOrderByInJoinSubqueries,
        allowHeavyRangeInferenceInJoins,
        std::move(cacheSize),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

class TMergeJoinRowsetProfiler
    : public IJoinProfiler
    , public IJoinRowsProducer
{
public:
    TMergeJoinRowsetProfiler(TSharedRange<TRow> rowset, int joinKeySize, TLogger logger)
        : ForeignRowset_(std::move(rowset))
        , JoinKeySize_(joinKeySize)
        , Logger(std::move(logger))
    { }

    IJoinRowsProducerPtr Profile() override
    {
        return this;
    }

    ISchemafulUnversionedReaderPtr FetchJoinedRows(
        std::vector<TRow> joinKeys,
        TRowBufferPtr /*buffer*/) override
    {
        YT_TLOG_DEBUG("Merge join profiler got keys")
            .With("Keys", joinKeys);

        YT_ASSERT(std::is_sorted(joinKeys.begin(), joinKeys.end(), [&] (TRow lhs, TRow rhs) {
            return CompareValueRanges(lhs.FirstNElements(JoinKeySize_), rhs.FirstNElements(JoinKeySize_)) < 0;
        }));

        std::vector<TRow> matchedRows;

        auto beginSearch = ForeignRowset_.Begin();

        for (const auto& key : joinKeys) {
            auto it = ExponentialSearch(
                beginSearch,
                ForeignRowset_.End(),
                [&] (auto foreignRowIt) {
                    return CompareValueRanges(
                        foreignRowIt->FirstNElements(JoinKeySize_),
                        key.Elements()) < 0;
                });

            while (it != ForeignRowset_.End() &&
                CompareValueRanges(it->FirstNElements(JoinKeySize_), key.Elements()) == 0)
            {
                matchedRows.push_back(*it);
                it++;
            }

            beginSearch = it;
        }

        YT_TLOG_DEBUG("Merge join profiler matched rows")
            .With("Rows", matchedRows);

        auto batch = CreateBatchFromRows(MakeSharedRange(std::move(matchedRows), ForeignRowset_.GetHolder()));
        return New<TBatchReader>(std::move(batch));
    }

private:
    const TSharedRange<TRow> ForeignRowset_;
    const int JoinKeySize_;

    const TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

using TJoinHashTable = absl::flat_hash_map<
    TUnversionedValueRange,
    std::vector<TRow>,
    TDefaultUnversionedValueRangeHash,
    TDefaultUnversionedValueRangeEqual>;

class THashJoinRowsetProfiler
    : public IJoinProfiler
    , public IJoinRowsProducer
{
public:
    THashJoinRowsetProfiler(TSharedRange<TRow> rowset, int joinKeySize, TLogger logger)
        : ForeignRowset_(std::move(rowset))
        , HashTable_(MakeHashTable(ForeignRowset_, joinKeySize))
        , Logger(std::move(logger))
    { }

    IJoinRowsProducerPtr Profile() override
    {
        return this;
    }

    ISchemafulUnversionedReaderPtr FetchJoinedRows(
        std::vector<TRow> joinKeys,
        TRowBufferPtr /*buffer*/) override
    {
        YT_TLOG_DEBUG("Hash table join profiler got keys")
            .With("Keys", joinKeys);

        std::vector<TRow> matchedRows;
        for (auto joinKey : joinKeys) {
            auto it = HashTable_.find(joinKey.Elements());
            if (it != HashTable_.end()) {
                matchedRows.insert(matchedRows.end(), it->second.begin(), it->second.end());
            }
        }

        YT_TLOG_DEBUG("Hash table join profiler matched rows")
            .With("Rows", matchedRows);

        auto batch = CreateBatchFromRows(MakeSharedRange(std::move(matchedRows), ForeignRowset_.GetHolder()));
        return New<TBatchReader>(std::move(batch));
    }

private:
    const TSharedRange<TRow> ForeignRowset_;
    const TJoinHashTable HashTable_;

    const TLogger Logger;

    static TJoinHashTable MakeHashTable(TRange<TRow> rowset, int joinKeySize)
    {
        TJoinHashTable hashTable;
        for (auto row : rowset) {
            auto [it, inserted] = hashTable.insert({row.FirstNElements(joinKeySize), {}});
            it->second.push_back(row);
        }
        return hashTable;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMergeHashJoinRowsetProfiler
    : public IJoinProfiler
    , public IJoinRowsProducer
{
public:
    TMergeHashJoinRowsetProfiler(TSharedRange<TRow> rowset, int foreignKeyPrefix, int joinKeySize, TLogger logger)
        : ForeignRowset_(std::move(rowset))
        , ForeignKeyPrefix_(foreignKeyPrefix)
        , JoinKeySize_(joinKeySize)
        , PrefixGroups_(BuildPrefixGroups(ForeignRowset_, foreignKeyPrefix, joinKeySize))
        , Logger(std::move(logger))
    { }

    IJoinRowsProducerPtr Profile() override
    {
        return this;
    }

    ISchemafulUnversionedReaderPtr FetchJoinedRows(
        std::vector<TRow> joinKeys,
        TRowBufferPtr /*buffer*/) override
    {
        YT_TLOG_DEBUG("Hybrid join profiler got keys")
            .With("Keys", joinKeys);

        YT_ASSERT(std::is_sorted(joinKeys.begin(), joinKeys.end(), [&] (TRow lhs, TRow rhs) {
            return CompareValueRanges(lhs.FirstNElements(JoinKeySize_), rhs.FirstNElements(JoinKeySize_)) < 0;
        }));

        std::vector<TRow> matchedRows;

        auto beginGroupIt = PrefixGroups_.begin();
        for (const auto& joinKey : joinKeys) {
            auto joinKeyPrefix = joinKey.FirstNElements(ForeignKeyPrefix_);

            auto groupIt = ExponentialSearch(
                beginGroupIt,
                PrefixGroups_.end(),
                [&] (auto currentGroupIt) {
                    return CompareValueRanges(currentGroupIt->Prefix, joinKeyPrefix) < 0;
                });
            beginGroupIt = groupIt;

            if (groupIt == PrefixGroups_.end()) {
                break;
            }
            if (CompareValueRanges(groupIt->Prefix, joinKeyPrefix) > 0) {
                continue;
            }

            auto joinKeySuffix = TUnversionedValueRange(
                joinKey.Begin() + ForeignKeyPrefix_,
                joinKey.Begin() + JoinKeySize_);

            auto suffixIt = groupIt->SuffixHashTable.find(joinKeySuffix);
            if (suffixIt != groupIt->SuffixHashTable.end()) {
                matchedRows.insert(matchedRows.end(), suffixIt->second.begin(), suffixIt->second.end());
            }
        }

        YT_TLOG_DEBUG("Hybrid join profiler matched rows")
            .With("Rows", matchedRows);

        auto batch = CreateBatchFromRows(MakeSharedRange(std::move(matchedRows), ForeignRowset_.GetHolder()));
        return New<TBatchReader>(std::move(batch));
    }

private:
    struct TPrefixGroup
    {
        TUnversionedValueRange Prefix;
        TJoinHashTable SuffixHashTable;
    };

    const TSharedRange<TRow> ForeignRowset_;
    const int ForeignKeyPrefix_;
    const int JoinKeySize_;
    const std::vector<TPrefixGroup> PrefixGroups_;

    const TLogger Logger;

    static std::vector<TPrefixGroup> BuildPrefixGroups(TRange<TRow> rowset, int foreignKeyPrefix, int joinKeySize)
    {
        std::vector<TPrefixGroup> result;

        if (rowset.Empty()) {
            return result;
        }

        auto currentPrefix = rowset.Front().FirstNElements(foreignKeyPrefix);
        TJoinHashTable currentHashTable;

        for (auto row : rowset) {
            auto prefix = row.FirstNElements(foreignKeyPrefix);

            if (CompareValueRanges(prefix, currentPrefix) != 0) {
                result.emplace_back(currentPrefix, std::exchange(currentHashTable, {}));
                currentPrefix = prefix;
            }

            auto suffix = TUnversionedValueRange(row.Begin() + foreignKeyPrefix, row.Begin() + joinKeySize);
            currentHashTable[suffix].push_back(row);
        }

        result.emplace_back(currentPrefix, std::move(currentHashTable));

        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

IJoinProfilerPtr CreateJoinRowsetProfiler(
    TSharedRange<TRow> rowset,
    int foreignKeyPrefix,
    int joinKeySize,
    TLogger Logger)
{
    YT_TLOG_DEBUG("Creating join rowset profiler")
        .With("ForeignKeyPrefix", foreignKeyPrefix)
        .With("JoinKeySize", joinKeySize)
        .With("Rowset", rowset);

    YT_ASSERT(foreignKeyPrefix == 0 || std::is_sorted(rowset.begin(), rowset.end(), [&] (TRow lhs, TRow rhs) {
        return CompareValueRanges(lhs.FirstNElements(foreignKeyPrefix), rhs.FirstNElements(foreignKeyPrefix)) < 0;
    }));

    if (foreignKeyPrefix == 0) {
        return New<THashJoinRowsetProfiler>(std::move(rowset), joinKeySize, std::move(Logger));
    }
    if (foreignKeyPrefix == joinKeySize) {
        return New<TMergeJoinRowsetProfiler>(std::move(rowset), joinKeySize, std::move(Logger));
    }
    return New<TMergeHashJoinRowsetProfiler>(std::move(rowset), foreignKeyPrefix, joinKeySize, std::move(Logger));
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(THierarchicalJoinRowsProducer)

class THierarchicalJoinRowsProducer
    : public IJoinRowsProducer
{
public:
    THierarchicalJoinRowsProducer(
        TConstHierarchicalJoinClausePtr hierarchicalJoinClause,
        TExecutePlan executePlan,
        TConsumeSubqueryStatistics consumeSubqueryStatistics,
        IMemoryChunkProviderPtr memoryChunkProvider,
        TLogger logger)
        : ForeignEquations_(hierarchicalJoinClause->ForeignEquations)
        , JoinSubqueryTemplate_(hierarchicalJoinClause->GetJoinSubquery())
        , DataSource_(TDataSource{
            .ObjectId = hierarchicalJoinClause->ForeignObjectId,
            .CellId = hierarchicalJoinClause->ForeignCellId,
          })
        , ExecutePlan_(std::move(executePlan))
        , ConsumeSubqueryStatistics_(std::move(consumeSubqueryStatistics))
        , MemoryChunkProvider_(std::move(memoryChunkProvider))
        , Logger(std::move(logger))
    { }

    ISchemafulUnversionedReaderPtr FetchJoinedRows(std::vector<TRow> keys, TRowBufferPtr buffer) override
    {
        if (keys.empty()) {
            return ISchemafulUnversionedReaderPtr{};
        }

        auto dataSource = DataSource_;

        auto joinSubquery = New<TQuery>(*JoinSubqueryTemplate_);

        TRowRanges universalRange{{
            buffer->CaptureRow(NTableClient::MinKey().Get()),
            buffer->CaptureRow(NTableClient::MaxKey().Get()),
        }};
        dataSource.Ranges = MakeSharedRange(std::move(universalRange), buffer);

        auto inClause = New<TInExpression>(
            ForeignEquations_,
            MakeSharedRange(std::move(keys), std::move(buffer)));

        joinSubquery->WhereClause = MakeAndExpression(inClause, joinSubquery->WhereClause);

        YT_TLOG_DEBUG("Evaluating hierarchical join subquery")
            .With("SubqueryId", joinSubquery->Id);

        return ExecutePlanAndGetReader(
            ExecutePlan_,
            ConsumeSubqueryStatistics_,
            MemoryChunkProvider_,
            TPlanFragment{
                .Query = std::move(joinSubquery),
                .DataSource = std::move(dataSource),
            });
    }

private:
    const std::vector<TConstExpressionPtr> ForeignEquations_;
    const TQueryPtr JoinSubqueryTemplate_;
    const TDataSource DataSource_;
    const TExecutePlan ExecutePlan_;
    const TConsumeSubqueryStatistics ConsumeSubqueryStatistics_;
    const IMemoryChunkProviderPtr MemoryChunkProvider_;
    const TLogger Logger;
};

DEFINE_REFCOUNTED_TYPE(THierarchicalJoinRowsProducer)

////////////////////////////////////////////////////////////////////////////////

IJoinProfilerPtr TJoinProfilerRegistry::GetJoinProfilerOrThrow(size_t index) const
{
    auto it = Profilers_.find(index);
    THROW_ERROR_EXCEPTION_IF(it == Profilers_.end(), "Join profiler not found for index %v", index);
    return it->second;
}

void TJoinProfilerRegistry::InsertJoinProfilerOrThrow(size_t index, IJoinProfilerPtr profiler)
{
    auto [it, inserted] = Profilers_.emplace(index, std::move(profiler));
    THROW_ERROR_EXCEPTION_IF(!inserted, "Join profiler already exists for index %v", index);
}

TJoinProfilerRegistry::TJoinProfilerRegistry(
    TExecutePlan executePlan,
    TConsumeSubqueryStatistics consumeSubqueryStatistics,
    IMemoryChunkProviderPtr memoryChunkProvider,
    NLogging::TLogger logger)
    : ExecutePlan_(std::move(executePlan))
    , ConsumeSubqueryStatistics_(std::move(consumeSubqueryStatistics))
    , MemoryChunkProvider_(std::move(memoryChunkProvider))
    , Logger_(std::move(logger))
{ }

IJoinRowsProducerPtr TJoinProfilerRegistry::CreateHierarchicalJoinRowsProducer(
    TConstHierarchicalJoinClausePtr hierarchicalJoinClause) const
{
    return New<THierarchicalJoinRowsProducer>(
        std::move(hierarchicalJoinClause),
        ExecutePlan_,
        ConsumeSubqueryStatistics_,
        MemoryChunkProvider_,
        Logger_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

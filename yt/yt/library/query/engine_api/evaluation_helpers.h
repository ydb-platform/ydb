#pragma once

#include "position_independent_value.h"

#include "public.h"

#include <yt/yt/library/query/base/callbacks.h>

#include <yt/yt/library/query/misc/objects_holder.h>
#include <yt/yt/library/query/misc/function_context.h>

#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>

#include <deque>
#include <unordered_map>
#include <unordered_set>

#include <sparsehash/dense_hash_set>
#include <sparsehash/dense_hash_map>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

constexpr i64 RowsetProcessingSize = 1024;
constexpr i64 WriteRowsetSize = 64 * RowsetProcessingSize;
constexpr i64 MaxJoinBatchSize = 1024 * RowsetProcessingSize;

class TInterruptedIncompleteException
{ };

struct TOutputBufferTag
{ };

struct TIntermediateBufferTag
{ };

struct TPermanentBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

constexpr const size_t InitialGroupOpHashtableCapacity = 1024;

using THasherFunction = ui64(const TPIValue*);
using TComparerFunction = char(const TPIValue*, const TPIValue*);
using TTernaryComparerFunction = i64(const TPIValue*, const TPIValue*);

namespace NDetail {

class TGroupHasher
{
public:
    // Intentionally implicit.
    TGroupHasher(THasherFunction* ptr)
        : Ptr_(ptr)
    { }

    ui64 operator () (const TPIValue* row) const
    {
        return Ptr_(row);
    }

private:
    THasherFunction* Ptr_;
};

class TRowComparer
{
public:
    // Intentionally implicit.
    TRowComparer(TComparerFunction* ptr)
        : Ptr_(ptr)
    { }

    bool operator () (const TPIValue* a, const TPIValue* b) const
    {
        return a == b || a && b && Ptr_(a, b);
    }

private:
    TComparerFunction* Ptr_;
};

} // namespace NDetail

using TLookupRows = google::dense_hash_set<
    const TPIValue*,
    NDetail::TGroupHasher,
    NDetail::TRowComparer>;

using TJoinLookup = google::dense_hash_map<
    const TPIValue*,
    std::pair<int, bool>,
    NDetail::TGroupHasher,
    NDetail::TRowComparer>;

using TJoinLookupRows = std::unordered_multiset<
    const TPIValue*,
    NDetail::TGroupHasher,
    NDetail::TRowComparer>;

struct TExecutionContext;

struct TSingleJoinParameters
{
    size_t KeySize;
    bool IsLeft;
    bool IsPartiallySorted;
    std::vector<size_t> ForeignColumns;
    TJoinSubqueryEvaluator ExecuteForeign;
};

struct TMultiJoinParameters
{
    TCompactVector<TSingleJoinParameters, 10> Items;
    size_t PrimaryRowSize;
    size_t BatchSize;
};

struct TMultiJoinClosure
{
    TRowBufferPtr Buffer;

    using THashJoinLookup = google::dense_hash_set<
        TPIValue*,
        NDetail::TGroupHasher,
        NDetail::TRowComparer>;  // + slot after row

    std::vector<TPIValue*> PrimaryRows;

    struct TItem
    {
        TRowBufferPtr Buffer;
        size_t KeySize;
        TComparerFunction* PrefixEqComparer;

        THashJoinLookup Lookup;
        std::vector<TPIValue*> OrderedKeys;  // + slot after row
        const TPIValue* LastKey = nullptr;

        TItem(
            IMemoryChunkProviderPtr chunkProvider,
            size_t keySize,
            TComparerFunction* prefixEqComparer,
            THasherFunction* lookupHasher,
            TComparerFunction* lookupEqComparer);
    };

    TCompactVector<TItem, 32> Items;

    size_t PrimaryRowSize;
    size_t BatchSize;
    std::function<void(size_t)> ProcessSegment;
    std::function<bool()> ProcessJoinBatch;
};

struct TGroupByClosure
{
    TRowBufferPtr Buffer;
    TComparerFunction* PrefixEqComparer;
    TLookupRows Lookup;
    const TPIValue* LastKey = nullptr;
    std::vector<const TPIValue*> GroupedRows;
    int KeySize;
    int ValuesCount;
    bool CheckNulls;

    // GroupedRows can be flushed and cleared during aggregation.
    // So we have to count grouped rows separately.
    size_t GroupedRowCount = 0;

    TGroupByClosure(
        IMemoryChunkProviderPtr chunkProvider,
        TComparerFunction* prefixEqComparer,
        THasherFunction* groupHasher,
        TComparerFunction* groupComparer,
        int keySize,
        int valuesCount,
        bool checkNulls);

    std::function<void()> ProcessSegment;
};

struct TWriteOpClosure
{
    TRowBufferPtr OutputBuffer;

    // Rows stored in OutputBuffer
    std::vector<TRow> OutputRowsBatch;
    size_t RowSize;

    explicit TWriteOpClosure(IMemoryChunkProviderPtr chunkProvider);
};

using TExpressionContext = TRowBuffer;

#define CHECK_STACK() (void) 0;

struct TExecutionContext
{
    ISchemafulUnversionedReaderPtr Reader;
    IUnversionedRowsetWriterPtr Writer;

    TQueryStatistics* Statistics = nullptr;

    // These limits prevent full scan.
    i64 InputRowLimit = std::numeric_limits<i64>::max();
    i64 OutputRowLimit = std::numeric_limits<i64>::max();
    i64 GroupRowLimit = std::numeric_limits<i64>::max();
    i64 JoinRowLimit = std::numeric_limits<i64>::max();

    // Offset from OFFSET clause.
    i64 Offset = 0;
    // Limit from LIMIT clause.
    i64 Limit = std::numeric_limits<i64>::max();

    bool Ordered = false;
    bool IsMerge = false;

    IMemoryChunkProviderPtr MemoryChunkProvider;

    TExecutionContext()
    {
        auto context = this;
        Y_UNUSED(context);
        CHECK_STACK();
    }
};

class TTopCollector
{
public:
    TTopCollector(
        i64 limit,
        TComparerFunction* comparer,
        size_t rowSize,
        IMemoryChunkProviderPtr memoryChunkProvider);

    std::vector<const TPIValue*> GetRows() const;

    void AddRow(const TPIValue* row);

private:
    // GarbageMemorySize <= AllocatedMemorySize <= TotalMemorySize
    size_t TotalMemorySize_ = 0;
    size_t AllocatedMemorySize_ = 0;
    size_t GarbageMemorySize_ = 0;

    class TComparer
    {
    public:
        explicit TComparer(TComparerFunction* ptr)
            : Ptr_(ptr)
        { }

        bool operator() (const std::pair<const TPIValue*, int>& lhs, const std::pair<const TPIValue*, int>& rhs) const
        {
            return (*this)(lhs.first, rhs.first);
        }

        bool operator () (const TPIValue* a, const TPIValue* b) const
        {
            return Ptr_(a, b);
        }

    private:
        TComparerFunction* const Ptr_;
    };

    TComparer Comparer_;
    size_t RowSize_;
    IMemoryChunkProviderPtr MemoryChunkProvider_;

    std::vector<TRowBufferPtr> Buffers_;
    std::vector<int> EmptyBufferIds_;
    std::vector<std::pair<const TPIValue*, int>> Rows_;

    std::pair<const TPIValue*, int> Capture(const TPIValue* row);

    void AccountGarbage(const TPIValue* row);
};

class TCGVariables
{
public:
    template <class T, class... TArgs>
    int AddOpaque(TArgs&&... args);

    TRange<void*> GetOpaqueData() const;

    void Clear();

    int AddLiteralValue(TOwningValue value);

    TRange<TPIValue> GetLiteralValues() const;

private:
    TObjectsHolder Holder_;
    std::vector<void*> OpaquePointers_;
    std::vector<TOwningValue> OwningLiteralValues_;
    mutable std::unique_ptr<TPIValue[]> LiteralValues_;

    static void InitLiteralValuesIfNeeded(const TCGVariables* variables);
};

using TCGPIQuerySignature = void(const TPIValue*, void* const*, TExecutionContext*);
using TCGPIExpressionSignature = void(const TPIValue*, void* const*, TPIValue*, const TPIValue*, TExpressionContext*);
using TCGPIAggregateInitSignature = void(TExpressionContext*, TPIValue*);
using TCGPIAggregateUpdateSignature = void(TExpressionContext*, TPIValue*, const TPIValue*);
using TCGPIAggregateMergeSignature = void(TExpressionContext*, TPIValue*, const TPIValue*);
using TCGPIAggregateFinalizeSignature = void(TExpressionContext*, TPIValue*, const TPIValue*);

using TCGQuerySignature = void(TRange<TPIValue>, TRange<void*>, TExecutionContext*);
using TCGExpressionSignature = void(TRange<TPIValue>, TRange<void*>, TValue*, TRange<TValue>, TRowBuffer*);
using TCGAggregateInitSignature = void(TExpressionContext*, TValue*);
using TCGAggregateUpdateSignature = void(TExpressionContext*, TValue*, TRange<TValue>);
using TCGAggregateMergeSignature = void(TExpressionContext*, TValue*, const TValue*);
using TCGAggregateFinalizeSignature = void(TExpressionContext*, TValue*, const TValue*);

using TCGQueryCallback = TCallback<TCGQuerySignature>;
using TCGExpressionCallback = TCallback<TCGExpressionSignature>;
using TCGAggregateInitCallback = TCallback<TCGAggregateInitSignature>;
using TCGAggregateUpdateCallback = TCallback<TCGAggregateUpdateSignature>;
using TCGAggregateMergeCallback = TCallback<TCGAggregateMergeSignature>;
using TCGAggregateFinalizeCallback = TCallback<TCGAggregateFinalizeSignature>;

struct TCGAggregateCallbacks
{
    TCGAggregateInitCallback Init;
    TCGAggregateUpdateCallback Update;
    TCGAggregateMergeCallback Merge;
    TCGAggregateFinalizeCallback Finalize;
};

////////////////////////////////////////////////////////////////////////////////

std::pair<TQueryPtr, TDataSource> GetForeignQuery(
    TQueryPtr subquery,
    TConstJoinClausePtr joinClause,
    std::vector<TRow> keys,
    TRowBufferPtr permanentBuffer);

////////////////////////////////////////////////////////////////////////////////

struct TExpressionClosure;

struct TJoinComparers
{
    TComparerFunction* PrefixEqComparer;
    THasherFunction* SuffixHasher;
    TComparerFunction* SuffixEqComparer;
    TComparerFunction* SuffixLessComparer;
    TComparerFunction* ForeignPrefixEqComparer;
    TComparerFunction* ForeignSuffixLessComparer;
    TTernaryComparerFunction* FullTernaryComparer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

#define EVALUATION_HELPERS_INL_H_
#include "evaluation_helpers-inl.h"
#undef EVALUATION_HELPERS_INL_H_

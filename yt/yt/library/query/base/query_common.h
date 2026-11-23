#pragma once

#include "public.h"

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/transaction_client/helpers.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/library/codegen_api/execution_backend.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NQueryClient {

using NTransactionClient::TReadTimestampRange;

////////////////////////////////////////////////////////////////////////////////

struct TDataSplit
{
    TGuid ObjectId;

    TTableSchemaPtr TableSchema;

    NHydra::TRevision MountRevision;
};

////////////////////////////////////////////////////////////////////////////////

using TStructMemberAccessor = std::string;
using TTupleItemIndexAccessor = int;

using TSourceLocation = std::pair<int, int>;
static const TSourceLocation NullSourceLocation(0, 0);

DEFINE_ENUM(EUnaryOp,
    // Arithmetical operations.
    (Plus)
    (Minus)
    // Integral operations.
    (BitNot)
    // Logical operations.
    (Not)
);

DEFINE_ENUM(EBinaryOp,
    // Arithmetical operations.
    (Plus)
    (Minus)
    (Multiply)
    (Divide)
    // Integral operations.
    (Modulo)
    (LeftShift)
    (RightShift)
    (BitOr)
    (BitAnd)
    // Logical operations.
    (And)
    (Or)
    // Relational operations.
    (Equal)
    (NotEqual)
    (Less)
    (LessOrEqual)
    (Greater)
    (GreaterOrEqual)
    // String operations.
    (Concatenate)
);

DEFINE_ENUM(EStringMatchOp,
    (Like)
    (CaseInsensitiveLike)
    (Regex)
);

DEFINE_ENUM(ETotalsMode,
    (None)
    (BeforeHaving)
    (AfterHaving)
);

DEFINE_ENUM(EAggregateFunction,
    (Sum)
    (Min)
    (Max)
);

DEFINE_ENUM(EStreamTag,
    (Aggregated)
    (Intermediate)
    (Totals)
);

const char* GetUnaryOpcodeLexeme(EUnaryOp opcode);
const char* GetBinaryOpcodeLexeme(EBinaryOp opcode);
const char* GetStringMatchOpcodeLexeme(EStringMatchOp opcode);

//! Reverse binary opcode for comparison operations (for swapping arguments).
EBinaryOp GetReversedBinaryOpcode(EBinaryOp opcode);

//! Inverse binary opcode for comparison operations (for inverting the operation).
EBinaryOp GetInversedBinaryOpcode(EBinaryOp opcode);

//! Classifies binary opcode according to classification above.
bool IsArithmeticalBinaryOp(EBinaryOp opcode);

//! Classifies binary opcode according to classification above.
bool IsIntegralBinaryOp(EBinaryOp opcode);

//! Classifies binary opcode according to classification above.
bool IsLogicalBinaryOp(EBinaryOp opcode);

//! Classifies binary opcode according to classification above.
bool IsRelationalBinaryOp(EBinaryOp opcode);

//! Classifies binary opcode according to classification above.
bool IsStringBinaryOp(EBinaryOp opcode);

//! Cast values.
TOwningValue CastValueWithCheck(TValue value, EValueType targetType);

//! Throws if non-null operands have different or non-integral types.
//! Returns a value with default Id and Flags, independent of operand metadata.
TValue EvaluateModulo(TValue lhs, TValue rhs);

////////////////////////////////////////////////////////////////////////////////

// TODO(lukyan): Use opaque data descriptor instead of ObjectId, CellId and MountRevision.
struct TDataSource
{
    // Could be:
    // * a table id;
    // * a tablet id.
    NObjectClient::TObjectId ObjectId;
    // If #ObjectId is a tablet id then this is the id of the cell hosting this tablet.
    // COMPAT(babenko): legacy clients may omit this field.
    NObjectClient::TCellId CellId;

    NHydra::TRevision MountRevision;

    TSharedRange<TRowRange> Ranges;
    TSharedRange<TRow> Keys;
};

void VerifyIdsInRange(const TRowRange& range);
void VerifyIdsInRanges(TRange<TRowRange> ranges);
void VerifyIdsInKeys(TRange<TRow> keys);

void ToProto(
    NProto::TDataSource* serialized,
    const TDataSource& original,
    TRange<TLogicalTypePtr> schema);
void FromProto(
    TDataSource* original,
    const NProto::TDataSource& serialized,
    const IMemoryChunkProviderPtr& memoryChunkProvider = GetDefaultMemoryChunkProvider());

////////////////////////////////////////////////////////////////////////////////

struct TQueryBaseOptions
{
    TReadSessionId ReadSessionId;

    i64 InputRowLimit = std::numeric_limits<i64>::max();
    i64 OutputRowLimit = std::numeric_limits<i64>::max();
    size_t MemoryLimitPerNode = std::numeric_limits<size_t>::max();

    NCodegen::EExecutionBackend ExecutionBackend = NCodegen::EExecutionBackend::Native;
    NCodegen::EOptimizationLevel OptimizationLevel = NCodegen::EOptimizationLevel::Default;

    bool EnableCodeCache = true;
    bool UseCanonicalNullRelations = false;
    bool MergeVersionedRows = true;
    // COMPAT(sabdenovch)
    bool AllowUnorderedGroupByWithLimit = true;
    std::optional<int> TruncatedQueryLengthForTracing;
    bool PrefetchJoinTables = false;
    bool EnableParallelizeUnorderedGroupBy = false;
};

struct TQueryOptions
    : public TQueryBaseOptions
{
    TReadTimestampRange TimestampRange{
        .Timestamp = NTransactionClient::SyncLastCommittedTimestamp,
        .RetentionTimestamp = NTransactionClient::NullTimestamp,
    };

    std::optional<std::string> ExecutionPool;
    TWorkloadDescriptor WorkloadDescriptor;

    std::optional<bool> UseLookupCache;

    ui64 RangeExpansionLimit = 0;

    TInstant Deadline = TInstant::Max();

    i64 MinRowCountPerSubquery = 100'000;
    int MaxSubqueries = std::numeric_limits<int>::max();

    i64 RowsetProcessingBatchSize = DefaultRowsetProcessingBatchSize;
    i64 WriteRowsetSize = DefaultWriteRowsetSize;
    i64 MaxJoinBatchSize = DefaultMaxJoinBatchSize;
    std::optional<int> MaxJitStackFrameSize;

    EStatisticsAggregation StatisticsAggregation = EStatisticsAggregation::DepthOmitNode;

    bool VerboseLogging = false;
    bool AllowFullScan = true;
    bool SuppressAccessTracking = false;
    // COMPAT(lukyan)
    bool NewRangeInference = true;
    // COMPAT(dtorilov)
    bool AdaptiveOrderedSchemafulReader = true;
    // COMPAT(sabdenovch)
    bool UseOrderByInJoinSubqueries = false;
    bool AllowUdfObjectCodeCache = false;

    bool AllowReverseScanForOrderBy = false;

    std::optional<i64> JoinCacheSize;

    NHydra::EPeerKind ReadFrom = NHydra::EPeerKind::Leader;
};

void ToProto(NProto::TQueryOptions* serialized, const TQueryOptions& original);
void FromProto(TQueryOptions* original, const NProto::TQueryOptions& serialized);

TQueryOptions GetJoinSubqueryOptions(const TQueryOptions& queryOptions);

struct TJoinSubqueryOptionsPatch
{
    std::optional<NTransactionClient::TTimestamp> Timestamp;
    std::optional<int> MaxSubqueries;
};

TQueryOptions ApplyJoinSubqueryOptionsPatch(
    const TQueryOptions& base,
    const TJoinSubqueryOptionsPatch& patch);

////////////////////////////////////////////////////////////////////////////////

struct TFeatureFlags
{
    bool WithTotalsFinalizesAggregatedOnCoordinator = false;
    bool GroupByWithLimitIsUnordered = false;
};

TFeatureFlags MostFreshFeatureFlags();
TFeatureFlags MostArchaicFeatureFlags();

std::string ToString(const TFeatureFlags& featureFlags);

void ToProto(NProto::TFeatureFlags* serialized, const TFeatureFlags& original);
void FromProto(TFeatureFlags* original, const NProto::TFeatureFlags& serialized);

////////////////////////////////////////////////////////////////////////////////

struct TShuffleNavigator
{
    THashMap<std::string, TSharedRange<TKeyRange>> DestinationMap;
    int PrefixHint;
};

using TJoinLayerDataSourceSet = std::vector<NQueryClient::TDataSource>;

////////////////////////////////////////////////////////////////////////////////

using TPlanFragmentPtr = TIntrusivePtr<TPlanFragment>;

struct TPlanFragment final
{
    TQueryPtr Query;
    TDataSource DataSource;
    TPlanFragmentPtr SubqueryFragment;
    TUnionAllQueryPtr UnionAllQuery;
};

////////////////////////////////////////////////////////////////////////////////

struct TPreparePlanFragmentOptions
{
    int SyntaxVersion = 1;
    int BuilderVersion = 1;
    NCodegen::EExecutionBackend ExecutionBackend = NCodegen::EExecutionBackend::Native;
    bool ShouldRewriteCardinalityIntoHyperLogLog = false; // COMPAT(dtorilov): Remove after 25.4.
    int HyperLogLogPrecision = 14;
    bool AllowJoinWithAsyncLastCommittedTimestampIfRequireSyncReplicaIsFalse = false; // COMPAT(dtorilov): Remove after 26.1.
    bool AllowReverseScanForOrderBy = false;
    std::optional<int> MaxProjectionCount;
    bool EnableScalarSubqueryOrderByAndLimit = false; // COMPAT(dtorilov): Remove after 26.2.
    // COMPAT(sabdenovch)
    bool AllowUnorderedGroupByWithLimit = true;
    bool MergeVersionedRows = true;
};

struct TPreparePlanFragmentContext
{
    const NLogging::TLogger& Logger;
    const THashMap<NYPath::TYPath, TDataSplit>& DataSplits;
    const TPreparePlanFragmentOptions& Options;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

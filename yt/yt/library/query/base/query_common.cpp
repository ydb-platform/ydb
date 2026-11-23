#include "query_common.h"

#include <yt/yt/library/query/proto/query.pb.h>

#include <yt/yt/client/table_client/wire_protocol.h>
#include <yt/yt/client/table_client/helpers.h>

namespace NYT::NQueryClient {

using namespace NTableClient;

using NYT::FromProto;
using NYT::ToProto;

using namespace NHydra;

using NCodegen::EExecutionBackend;
using NCodegen::EOptimizationLevel;

////////////////////////////////////////////////////////////////////////////////

const char* GetUnaryOpcodeLexeme(EUnaryOp opcode)
{
    switch (opcode) {
        case EUnaryOp::Plus:   return "+";
        case EUnaryOp::Minus:  return "-";
        case EUnaryOp::Not:    return "NOT";
        case EUnaryOp::BitNot: return "~";
        default:               YT_ABORT();
    }
}

const char* GetBinaryOpcodeLexeme(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::Plus:           return "+";
        case EBinaryOp::Minus:          return "-";
        case EBinaryOp::Multiply:       return "*";
        case EBinaryOp::Divide:         return "/";
        case EBinaryOp::Modulo:         return "%";
        case EBinaryOp::LeftShift:      return "<<";
        case EBinaryOp::RightShift:     return ">>";
        case EBinaryOp::BitAnd:         return "&";
        case EBinaryOp::BitOr:          return "|";
        case EBinaryOp::And:            return "AND";
        case EBinaryOp::Or:             return "OR";
        case EBinaryOp::Equal:          return "=";
        case EBinaryOp::NotEqual:       return "!=";
        case EBinaryOp::Less:           return "<";
        case EBinaryOp::LessOrEqual:    return "<=";
        case EBinaryOp::Greater:        return ">";
        case EBinaryOp::GreaterOrEqual: return ">=";
        case EBinaryOp::Concatenate:    return "||";
        default:                        YT_ABORT();
    }
}

const char* GetStringMatchOpcodeLexeme(EStringMatchOp opcode)
{
    switch (opcode) {
        case EStringMatchOp::Like:                return "LIKE";
        case EStringMatchOp::CaseInsensitiveLike: return "ILIKE";
        case EStringMatchOp::Regex:               return "REGEX";
        default:                                  YT_ABORT();
    }
}

EBinaryOp GetReversedBinaryOpcode(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::Less:           return EBinaryOp::Greater;
        case EBinaryOp::LessOrEqual:    return EBinaryOp::GreaterOrEqual;
        case EBinaryOp::Greater:        return EBinaryOp::Less;
        case EBinaryOp::GreaterOrEqual: return EBinaryOp::LessOrEqual;
        default:                        return opcode;
    }
}

EBinaryOp GetInversedBinaryOpcode(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::Equal:          return EBinaryOp::NotEqual;
        case EBinaryOp::NotEqual:       return EBinaryOp::Equal;
        case EBinaryOp::Less:           return EBinaryOp::GreaterOrEqual;
        case EBinaryOp::LessOrEqual:    return EBinaryOp::Greater;
        case EBinaryOp::Greater:        return EBinaryOp::LessOrEqual;
        case EBinaryOp::GreaterOrEqual: return EBinaryOp::Less;
        default:                        YT_ABORT();
    }
}

bool IsArithmeticalBinaryOp(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::Plus:
        case EBinaryOp::Minus:
        case EBinaryOp::Multiply:
        case EBinaryOp::Divide:
            return true;
        default:
            return false;
    }
}

bool IsIntegralBinaryOp(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::Modulo:
        case EBinaryOp::LeftShift:
        case EBinaryOp::RightShift:
        case EBinaryOp::BitOr:
        case EBinaryOp::BitAnd:
            return true;
        default:
            return false;
    }
}

bool IsLogicalBinaryOp(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::And:
        case EBinaryOp::Or:
            return true;
        default:
            return false;
    }
}

bool IsRelationalBinaryOp(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::Equal:
        case EBinaryOp::NotEqual:
        case EBinaryOp::Less:
        case EBinaryOp::LessOrEqual:
        case EBinaryOp::Greater:
        case EBinaryOp::GreaterOrEqual:
            return true;
        default:
            return false;
    }
}

bool IsStringBinaryOp(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::Concatenate:
            return true;
        default:
            return false;
    }
}

TOwningValue CastValueWithCheck(TValue value, EValueType targetType)
{
    if (value.Type == targetType || value.Type == EValueType::Null) {
        return value;
    }

    if (targetType == EValueType::Any) {
        TChunkedMemoryPool pool;
        return EncodeUnversionedAnyValue(value, &pool);
    }

    if (value.Type == EValueType::Int64) {
        if (targetType == EValueType::Double) {
            auto int64Value = value.Data.Int64;
            auto doubleValue = static_cast<double>(int64Value);
            // NB: Casting the rounded double value back to i64 is UB when it does not fit into i64
            // (i.e. for double(i64 max)): garbage on x86_64, which throws by luck, and saturation
            // on aarch64, which silently accepts the inaccurate conversion.
            if (doubleValue >= static_cast<double>(std::numeric_limits<i64>::max()) ||
                static_cast<i64>(doubleValue) != int64Value)
            {
                THROW_ERROR_EXCEPTION("Failed to cast %v to double: inaccurate conversion", int64Value);
            }
            value.Data.Double = doubleValue;
        } else {
            YT_VERIFY(targetType == EValueType::Uint64);
        }
    } else if (value.Type == EValueType::Uint64) {
        if (targetType == EValueType::Int64) {
            if (value.Data.Uint64 > std::numeric_limits<i64>::max()) {
                THROW_ERROR_EXCEPTION(
                    "Failed to cast %vu to int64: value is greater than maximum", value.Data.Uint64);
            }
        } else if (targetType == EValueType::Double) {
            auto uint64Value = value.Data.Uint64;
            auto doubleValue = static_cast<double>(uint64Value);
            // NB: Same as above: double(ui64 max) does not fit into ui64.
            if (doubleValue >= static_cast<double>(std::numeric_limits<ui64>::max()) ||
                static_cast<ui64>(doubleValue) != uint64Value)
            {
                THROW_ERROR_EXCEPTION("Failed to cast %vu to double: inaccurate conversion", uint64Value);
            }
            value.Data.Double = doubleValue;
        } else {
            YT_ABORT();
        }
    } else if (value.Type == EValueType::Double) {
        auto doubleValue = value.Data.Double;
        if (targetType == EValueType::Uint64) {
            // NB: Guard before casting since casting an out-of-range (or NaN) double to ui64 is UB
            // (see above). The guard formulation also rejects NaN.
            if (!(doubleValue >= 0. && doubleValue < static_cast<double>(std::numeric_limits<ui64>::max())) ||
                static_cast<double>(static_cast<ui64>(doubleValue)) != doubleValue)
            {
                THROW_ERROR_EXCEPTION("Failed to cast %v to uint64: inaccurate conversion", doubleValue);
            }
            value.Data.Uint64 = doubleValue;
        } else if (targetType == EValueType::Int64) {
            // NB: Same as above.
            if (!(doubleValue >= static_cast<double>(std::numeric_limits<i64>::min()) &&
                doubleValue < static_cast<double>(std::numeric_limits<i64>::max())) ||
                static_cast<double>(static_cast<i64>(doubleValue)) != doubleValue)
            {
                THROW_ERROR_EXCEPTION("Failed to cast %v to int64: inaccurate conversion", doubleValue);
            }
            value.Data.Int64 = doubleValue;
        } else {
            YT_ABORT();
        }
    } else {
        YT_ABORT();
    }

    value.Type = targetType;
    return value;
}

////////////////////////////////////////////////////////////////////////////////

TValue EvaluateModulo(TValue lhs, TValue rhs)
{
    if (lhs.Type == EValueType::Null || rhs.Type == EValueType::Null) {
        return MakeUnversionedNullValue();
    }

    if (lhs.Type != rhs.Type) {
        THROW_ERROR_EXCEPTION(
            "Modulo operands have different types %Qlv and %Qlv",
            lhs.Type,
            rhs.Type);
    }

    switch (lhs.Type) {
        case EValueType::Int64:
            if (rhs.Data.Int64 == 0) {
                THROW_ERROR_EXCEPTION("Division by zero");
            }

            if (lhs.Data.Int64 == std::numeric_limits<i64>::min() && rhs.Data.Int64 == -1) {
                THROW_ERROR_EXCEPTION("Division of INT_MIN by -1");
            }

            return MakeUnversionedInt64Value(lhs.Data.Int64 % rhs.Data.Int64);

        case EValueType::Uint64:
            if (rhs.Data.Uint64 == 0) {
                THROW_ERROR_EXCEPTION("Division by zero");
            }

            return MakeUnversionedUint64Value(lhs.Data.Uint64 % rhs.Data.Uint64);

        default:
            THROW_ERROR_EXCEPTION(
                "Cannot compute modulo for values of types %Qlv and %Qlv",
                lhs.Type,
                rhs.Type);
    }
}

////////////////////////////////////////////////////////////////////////////////

TFeatureFlags MostFreshFeatureFlags()
{
    return {
        .WithTotalsFinalizesAggregatedOnCoordinator = true,
        .GroupByWithLimitIsUnordered = true,
    };
}

TFeatureFlags MostArchaicFeatureFlags()
{
    return {
        .WithTotalsFinalizesAggregatedOnCoordinator = false,
        .GroupByWithLimitIsUnordered = false,
    };
}

std::string ToString(const TFeatureFlags& featureFlags)
{
    return Format(
        "{WithTotalsFinalizesAggregatedOnCoordinator: %v, GroupByWithLimitIsUnordered: %v}",
        featureFlags.WithTotalsFinalizesAggregatedOnCoordinator,
        featureFlags.GroupByWithLimitIsUnordered);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TDataSource* serialized,
    const TDataSource& original,
    TRange<TLogicalTypePtr> schema)
{
    ToProto(serialized->mutable_object_id(), original.ObjectId);
    ToProto(serialized->mutable_cell_id(), original.CellId);
    serialized->set_mount_revision(ToProto(original.MountRevision));

    auto rangesWriter = CreateWireProtocolWriter();
    for (const auto& range : original.Ranges) {
        rangesWriter->WriteUnversionedRow(range.first);
        rangesWriter->WriteUnversionedRow(range.second);
    }
    ToProto(serialized->mutable_ranges(), MergeRefsToString(rangesWriter->Finish()));

    if (original.Keys) {
        std::vector<TColumnSchema> columns;
        for (auto type : schema) {
            columns.emplace_back("", type);
        }

        TTableSchema schema(columns);
        auto keysWriter = CreateWireProtocolWriter();
        keysWriter->WriteTableSchema(schema);
        keysWriter->WriteSchemafulRowset(original.Keys);
        ToProto(serialized->mutable_keys(), MergeRefsToString(keysWriter->Finish()));
    }
}

void FromProto(
    TDataSource* original,
    const NProto::TDataSource& serialized,
    const IMemoryChunkProviderPtr& memoryChunkProvider)
{
    FromProto(&original->ObjectId, serialized.object_id());
    FromProto(&original->CellId, serialized.cell_id());
    FromProto(&original->MountRevision, serialized.mount_revision());

    struct TDataSourceBufferTag
    { };

    TRowRanges ranges;
    auto rowBuffer = New<TRowBuffer>(TDataSourceBufferTag(), memoryChunkProvider);
    auto rangesReader = CreateWireProtocolReader(
        TSharedRef::FromString<TDataSourceBufferTag>(serialized.ranges()),
        rowBuffer);
    while (!rangesReader->IsFinished()) {
        auto lowerBound = rangesReader->ReadUnversionedRow(true);
        auto upperBound = rangesReader->ReadUnversionedRow(true);
        ranges.emplace_back(lowerBound, upperBound);
    }
    original->Ranges = MakeSharedRange(std::move(ranges), rowBuffer);

    if (serialized.has_keys()) {
        auto keysReader = CreateWireProtocolReader(
            TSharedRef::FromString<TDataSourceBufferTag>(serialized.keys()),
            rowBuffer);

        auto schema = keysReader->ReadTableSchema();
        auto schemaData = keysReader->GetSchemaData(schema, NTableClient::TColumnFilter());
        original->Keys = keysReader->ReadSchemafulRowset(schemaData, true);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TFeatureFlags* serialized, const TFeatureFlags& original)
{
    serialized->set_with_totals_finalizes_aggregated_on_coordinator(original.WithTotalsFinalizesAggregatedOnCoordinator);
    serialized->set_group_by_with_limit_is_unordered(original.GroupByWithLimitIsUnordered);
}

void FromProto(TFeatureFlags* original, const NProto::TFeatureFlags& serialized)
{
    original->WithTotalsFinalizesAggregatedOnCoordinator = serialized.with_totals_finalizes_aggregated_on_coordinator();
    if (serialized.has_group_by_with_limit_is_unordered()) {
        original->GroupByWithLimitIsUnordered = serialized.group_by_with_limit_is_unordered();
    }
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TQueryOptions* serialized, const TQueryOptions& original)
{
    serialized->set_timestamp(ToProto(original.TimestampRange.Timestamp));
    serialized->set_retention_timestamp(ToProto(original.TimestampRange.RetentionTimestamp));
    serialized->set_verbose_logging(original.VerboseLogging);
    serialized->set_new_range_inference(original.NewRangeInference);
    serialized->set_adaptive_ordered_schemaful_reader(original.AdaptiveOrderedSchemafulReader);
    serialized->set_use_canonical_null_relations(original.UseCanonicalNullRelations);
    serialized->set_execution_backend(ToProto(original.ExecutionBackend));
    serialized->set_optimization_level(ToProto(original.OptimizationLevel));
    serialized->set_max_subqueries(original.MaxSubqueries);
    serialized->set_enable_code_cache(original.EnableCodeCache);
    ToProto(serialized->mutable_workload_descriptor(), original.WorkloadDescriptor);
    serialized->set_allow_full_scan(original.AllowFullScan);
    ToProto(serialized->mutable_read_session_id(), original.ReadSessionId);
    serialized->set_deadline(ToProto(original.Deadline));
    serialized->set_memory_limit_per_node(original.MemoryLimitPerNode);
    if (original.ExecutionPool) {
        serialized->set_execution_pool(*original.ExecutionPool);
    }
    serialized->set_suppress_access_tracking(original.SuppressAccessTracking);
    serialized->set_range_expansion_limit(original.RangeExpansionLimit);
    serialized->set_merge_versioned_rows(original.MergeVersionedRows);
    if (original.UseLookupCache) {
        serialized->set_use_lookup_cache(*original.UseLookupCache);
    }
    serialized->set_min_row_count_per_subquery(original.MinRowCountPerSubquery);
    serialized->set_allow_unordered_group_by_with_limit(original.AllowUnorderedGroupByWithLimit);
    serialized->set_rowset_processing_batch_size(original.RowsetProcessingBatchSize);
    serialized->set_write_rowset_size(original.WriteRowsetSize);
    serialized->set_max_join_batch_size(original.MaxJoinBatchSize);
    serialized->set_use_order_by_in_join_subqueries(original.UseOrderByInJoinSubqueries);
    serialized->set_allow_udf_object_code_cache(original.AllowUdfObjectCodeCache);
    serialized->set_statistics_aggregation(ToProto(original.StatisticsAggregation));
    serialized->set_read_from(ToProto(original.ReadFrom));
    if (original.JoinCacheSize) {
        serialized->set_join_cache_size(*original.JoinCacheSize);
    }
    serialized->set_enable_parallelize_unordered_group_by(original.EnableParallelizeUnorderedGroupBy);
}

void FromProto(TQueryOptions* original, const NProto::TQueryOptions& serialized)
{
    original->TimestampRange.Timestamp = FromProto<TTimestamp>(serialized.timestamp());
    original->TimestampRange.RetentionTimestamp = FromProto<TTimestamp>(serialized.retention_timestamp());
    original->VerboseLogging = serialized.verbose_logging();
    original->MaxSubqueries = serialized.max_subqueries();
    original->EnableCodeCache = serialized.enable_code_cache();
    original->WorkloadDescriptor = serialized.has_workload_descriptor()
        ? FromProto<TWorkloadDescriptor>(serialized.workload_descriptor())
        : TWorkloadDescriptor();
    original->AllowFullScan = serialized.allow_full_scan();
    original->ReadSessionId  = serialized.has_read_session_id()
        ? FromProto<TReadSessionId>(serialized.read_session_id())
        : TReadSessionId::Create();

    if (serialized.has_memory_limit_per_node()) {
        original->MemoryLimitPerNode = serialized.memory_limit_per_node();
    }

    if (serialized.has_execution_pool()) {
        original->ExecutionPool = serialized.execution_pool();
    }

    original->Deadline = serialized.has_deadline()
        ? FromProto<TInstant>(serialized.deadline())
        : TInstant::Max();

    if (serialized.has_suppress_access_tracking()) {
        original->SuppressAccessTracking = serialized.suppress_access_tracking();
    }

    if (serialized.has_range_expansion_limit()) {
        original->RangeExpansionLimit = serialized.range_expansion_limit();
    }
    if (serialized.has_new_range_inference()) {
        original->NewRangeInference = serialized.new_range_inference();
    }
    if (serialized.has_adaptive_ordered_schemaful_reader()) {
        original->AdaptiveOrderedSchemafulReader = serialized.adaptive_ordered_schemaful_reader();
    }
    if (serialized.has_use_canonical_null_relations()) {
        original->UseCanonicalNullRelations = serialized.use_canonical_null_relations();
    }
    if (serialized.has_merge_versioned_rows()) {
        original->MergeVersionedRows = serialized.merge_versioned_rows();
    }
    if (serialized.has_execution_backend()) {
        original->ExecutionBackend = static_cast<EExecutionBackend>(serialized.execution_backend());
    }
    if (serialized.has_optimization_level()) {
        original->OptimizationLevel = static_cast<EOptimizationLevel>(serialized.optimization_level());
    }
    if (serialized.has_use_lookup_cache()) {
        original->UseLookupCache = serialized.use_lookup_cache();
    }
    if (serialized.has_min_row_count_per_subquery()) {
        original->MinRowCountPerSubquery = serialized.min_row_count_per_subquery();
    }
    if (serialized.has_allow_unordered_group_by_with_limit()) {
        original->AllowUnorderedGroupByWithLimit = serialized.allow_unordered_group_by_with_limit();
    }
    if (serialized.has_rowset_processing_batch_size()) {
        original->RowsetProcessingBatchSize = serialized.rowset_processing_batch_size();
    }
    if (serialized.has_write_rowset_size()) {
        original->WriteRowsetSize = serialized.write_rowset_size();
    }
    if (serialized.has_max_join_batch_size()) {
        original->MaxJoinBatchSize = serialized.max_join_batch_size();
    }
    if (serialized.has_allow_udf_object_code_cache()) {
        original->AllowUdfObjectCodeCache = serialized.allow_udf_object_code_cache();
    }
    if (serialized.has_use_order_by_in_join_subqueries()) {
        original->UseOrderByInJoinSubqueries = serialized.use_order_by_in_join_subqueries();
    }
    if (serialized.has_statistics_aggregation()) {
        original->StatisticsAggregation = FromProto<EStatisticsAggregation>(serialized.statistics_aggregation());
    }
    if (serialized.has_read_from()) {
        original->ReadFrom = FromProto<EPeerKind>(serialized.read_from());
    }
    if (serialized.has_join_cache_size()) {
        original->JoinCacheSize = serialized.join_cache_size();
    }
    if (serialized.has_enable_parallelize_unordered_group_by()) {
        original->EnableParallelizeUnorderedGroupBy = serialized.enable_parallelize_unordered_group_by();
    }
}

TQueryOptions GetJoinSubqueryOptions(const TQueryOptions& queryOptions)
{
    auto result = queryOptions;
    result.MergeVersionedRows = true;

    return result;
}

TQueryOptions ApplyJoinSubqueryOptionsPatch(
    const TQueryOptions& base,
    const TJoinSubqueryOptionsPatch& patch)
{
    auto result = base;

    if (patch.Timestamp) {
        result.TimestampRange.Timestamp = *patch.Timestamp;
    }

    if (patch.MaxSubqueries) {
        result.MaxSubqueries = *patch.MaxSubqueries;
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

void VerifyIdsInRange(const TRowRange& range)
{
    auto validateBorder = [&] (TUnversionedRow row) {
        int id = 0;
        for (; id < static_cast<int>(row.GetCount()) - 1; ++id) {
            YT_VERIFY(row[id].Id == id);
        }

        if (row.GetCount() != 0) {
            YT_VERIFY(row[id].Id == id || IsSentinelType(row[id].Type));
        }
    };

    validateBorder(range.first);
    validateBorder(range.second);
}

void VerifyIdsInRanges(TRange<TRowRange> ranges)
{
    for (const auto& range : ranges) {
        VerifyIdsInRange(range);
    }
}

void VerifyIdsInKeys(TRange<TRow> keys)
{
    for (const auto& key : keys) {
        for (ui32 id = 0; id < key.GetCount(); ++id) {
            YT_VERIFY(key[id].Id == id);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

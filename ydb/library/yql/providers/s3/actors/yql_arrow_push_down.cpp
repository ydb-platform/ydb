#include "yql_arrow_push_down.h"

#include <util/generic/vector.h>
#include <ydb/library/yql/providers/generic/pushdown/yql_generic_match_predicate.h>
#include <parquet/statistics.h>

namespace {

TMaybe<NYql::NGenericPushDown::TTimestampColumnStatsData> GetDateStatistics(parquet::Type::type physicalType, std::shared_ptr<parquet::Statistics> statistics) {
    switch (physicalType) {
        case parquet::Type::type::INT32: {
            const parquet::TypedStatistics<arrow::Int32Type>* typedStatistics = static_cast<const parquet::TypedStatistics<arrow::Int32Type>*>(statistics.get());
            int64_t minValue = typedStatistics->min();
            int64_t maxValue = typedStatistics->max();
            NYql::NGenericPushDown::TTimestampColumnStatsData stats;
            stats.lowValue = TInstant::Days(minValue);
            stats.highValue = TInstant::Days(maxValue);
            return stats;
        }
        case parquet::Type::type::INT64: {
            const parquet::TypedStatistics<arrow::Int64Type>* typedStatistics = static_cast<const parquet::TypedStatistics<arrow::Int64Type>*>(statistics.get());
            int64_t minValue = typedStatistics->min();
            int64_t maxValue = typedStatistics->max();
            NYql::NGenericPushDown::TTimestampColumnStatsData stats;
            stats.lowValue = TInstant::Days(minValue);
            stats.highValue = TInstant::Days(maxValue);
            return stats;
        }
        case parquet::Type::type::BOOLEAN:
        case parquet::Type::type::INT96:
        case parquet::Type::type::FLOAT:
        case parquet::Type::type::DOUBLE:
        case parquet::Type::type::BYTE_ARRAY:
        case parquet::Type::type::FIXED_LEN_BYTE_ARRAY:
        case parquet::Type::type::UNDEFINED:
        return {};
    }
}

TMaybe<NYql::NGenericPushDown::TTimestampColumnStatsData> GetTimestampStatistics(const parquet::TimestampLogicalType* typestampLogicalType, parquet::Type::type physicalType, std::shared_ptr<parquet::Statistics> statistics) {
    int64_t multiplier = 1;
    switch (typestampLogicalType->time_unit()) {
        case parquet::LogicalType::TimeUnit::unit::UNKNOWN:
        case parquet::LogicalType::TimeUnit::unit::NANOS:
            return {};
        case parquet::LogicalType::TimeUnit::unit::MILLIS:
            multiplier *= 1000;
        break;
        case parquet::LogicalType::TimeUnit::unit::MICROS:
        break;
    }
    switch (physicalType) {
        case parquet::Type::type::INT32: {
            const parquet::TypedStatistics<arrow::Int32Type>* typedStatistics = static_cast<const parquet::TypedStatistics<arrow::Int32Type>*>(statistics.get());
            int64_t minValue = typedStatistics->min();
            int64_t maxValue = typedStatistics->max();
            NYql::NGenericPushDown::TTimestampColumnStatsData stats;
            stats.lowValue = TInstant::FromValue(minValue * multiplier);
            stats.highValue = TInstant::FromValue(maxValue * multiplier);
            return stats;
        }
        case parquet::Type::type::INT64: {
            const parquet::TypedStatistics<arrow::Int64Type>* typedStatistics = static_cast<const parquet::TypedStatistics<arrow::Int64Type>*>(statistics.get());
            int64_t minValue = typedStatistics->min();
            int64_t maxValue = typedStatistics->max();
            NYql::NGenericPushDown::TTimestampColumnStatsData stats;
            stats.lowValue = TInstant::FromValue(minValue * multiplier);
            stats.highValue = TInstant::FromValue(maxValue * multiplier);
            return stats;
        }
        case parquet::Type::type::BOOLEAN:
        case parquet::Type::type::INT96:
        case parquet::Type::type::FLOAT:
        case parquet::Type::type::DOUBLE:
        case parquet::Type::type::BYTE_ARRAY:
        case parquet::Type::type::FIXED_LEN_BYTE_ARRAY:
        case parquet::Type::type::UNDEFINED:
        return {};
    }
}

NYql::NGenericPushDown::TColumnStatistics MakeTimestampStatistics(const TString& name, ::Ydb::Type::PrimitiveTypeId type, const TMaybe<NYql::NGenericPushDown::TTimestampColumnStatsData>& statistics) {
    NYql::NGenericPushDown::TColumnStatistics columnStatistics;
    columnStatistics.ColumnName = name;
    columnStatistics.ColumnType.set_type_id(type);
    columnStatistics.Timestamp = statistics;
    return columnStatistics;
}

bool MatchRowGroup(std::unique_ptr<parquet::RowGroupMetaData> rowGroupMetadata, const NYql::NConnector::NApi::TPredicate& predicate) {
    TMap<TString, NYql::NGenericPushDown::TColumnStatistics> columns;
    for (int i = 0; i < rowGroupMetadata->schema()->num_columns(); i++) {
        auto columnChunkMetadata = rowGroupMetadata->ColumnChunk(i);
        if (!columnChunkMetadata->is_stats_set()) {
            continue;
        }
        auto column = rowGroupMetadata->schema()->Column(i);
        std::shared_ptr<const parquet::LogicalType> logicalType = column->logical_type();
        parquet::Type::type physicalType = column->physical_type();
        switch (logicalType->type()) {
            case parquet::LogicalType::Type::type::DATE: {
                auto statistics = GetDateStatistics(physicalType, columnChunkMetadata->statistics());
                if (statistics) {
                    const TString columnName{column->name()};
                    columns[columnName] = MakeTimestampStatistics(columnName, ::Ydb::Type::DATE, statistics);
                }
            }
            break;
            case parquet::LogicalType::Type::type::TIMESTAMP: {
                const parquet::TimestampLogicalType* typestampLogicalType = static_cast<const parquet::TimestampLogicalType*>(logicalType.get());
                auto statistics = GetTimestampStatistics(typestampLogicalType, physicalType, columnChunkMetadata->statistics());
                if (statistics) {
                    const TString columnName{column->name()};
                    columns[columnName] = MakeTimestampStatistics(columnName, ::Ydb::Type::TIMESTAMP, statistics);
                }
            }
            break;
            case parquet::LogicalType::Type::type::UNDEFINED:
            case parquet::LogicalType::Type::type::STRING:
            case parquet::LogicalType::Type::type::MAP:
            case parquet::LogicalType::Type::type::LIST:
            case parquet::LogicalType::Type::type::ENUM:
            case parquet::LogicalType::Type::type::DECIMAL:
            case parquet::LogicalType::Type::type::TIME:
            case parquet::LogicalType::Type::type::INTERVAL:
            case parquet::LogicalType::Type::type::INT:
            case parquet::LogicalType::Type::type::NIL:
            case parquet::LogicalType::Type::type::JSON:
            case parquet::LogicalType::Type::type::BSON:
            case parquet::LogicalType::Type::type::UUID:
            case parquet::LogicalType::Type::type::NONE:
            break;
        }
    }
    return NYql::NGenericPushDown::MatchPredicate(columns, predicate);
}

TVector<ui64> MatchedRowGroupsImpl(parquet::FileMetaData* fileMetadata, const NYql::NConnector::NApi::TPredicate& predicate) {
    TVector<ui64> matchedRowGroups;
    matchedRowGroups.reserve(fileMetadata->num_row_groups());
    for (int i = 0; i < fileMetadata->num_row_groups(); i++) {
        if (MatchRowGroup(fileMetadata->RowGroup(i), predicate)) {
            matchedRowGroups.push_back(i);
        }
    }
    return matchedRowGroups;
}

}

namespace NYql::NDq {

TVector<ui64> MatchedRowGroups(std::shared_ptr<parquet::FileMetaData> fileMetadata, const NYql::NConnector::NApi::TPredicate& predicate) {
    return MatchedRowGroupsImpl(fileMetadata.get(), predicate);
}

TVector<ui64> MatchedRowGroups(const std::unique_ptr<parquet::FileMetaData>& fileMetadata, const NYql::NConnector::NApi::TPredicate& predicate) {
    return MatchedRowGroupsImpl(fileMetadata.get(), predicate);
}



} // namespace NYql::NDq

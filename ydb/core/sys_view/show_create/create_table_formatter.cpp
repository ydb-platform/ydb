#include "create_table_formatter.h"

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <yql/essentials/ast/yql_ast_escaping.h>
#include <yql/essentials/minikql/mkql_type_ops.h>

#include <util/generic/yexception.h>

namespace NKikimr {
namespace NSysView {

using namespace NKikimrSchemeOp;
using namespace Ydb::Table;
using namespace NYdb;

void TCreateTableFormatter::EscapeName(const TString& str) {
    NYql::EscapeArbitraryAtom(str, '`', &Stream);
}

void TCreateTableFormatter::EscapeString(const TString& str) {
    NYql::EscapeArbitraryAtom(str, '\'', &Stream);
}

void TCreateTableFormatter::EscapeBinary(const TString& str) {
    NYql::EscapeBinaryAtom(str, '\'', &Stream);
}

void TCreateTableFormatter::FormatValue(NYdb::TValueParser& parser, bool isPartition, TString del) {
    TGuard<NMiniKQL::TScopedAlloc> guard(Alloc);
    switch (parser.GetKind()) {
        case NYdb::TTypeParser::ETypeKind::Primitive: {
            Stream << del;
            FormatPrimitive(parser);
            return;
        }
        case NYdb::TTypeParser::ETypeKind::Optional: {
            parser.OpenOptional();
            if (parser.IsNull()) {
                if (!isPartition) {
                    Stream << del << "NULL";
                }
            } else {
                FormatValue(parser, isPartition, del);
            }
            parser.CloseOptional();
            return;
        }
        case NYdb::TTypeParser::ETypeKind::Tuple: {
            parser.OpenTuple();
            bool first = true;
            Stream << "(";
            while (parser.TryNextElement()) {
                if (!first) {
                    FormatValue(parser, isPartition, ", ");
                } else {
                    FormatValue(parser, isPartition);
                }
                first = false;
            }
            Stream << ")";
            parser.CloseTuple();
            return;
        }
        case TTypeParser::ETypeKind::Decimal: {
            auto decimal = parser.GetDecimal();
            auto precision = decimal.DecimalType_.Precision;
            auto scale = decimal.DecimalType_.Scale;
            Stream << "CAST(";
            EscapeString(decimal.ToString());
            Stream << " AS Decimal(" << ui32(precision) << "," << ui32(scale) << ")";
            Stream << ")";
            return;
        }
        default:
            ythrow TFormatFail(Ydb::StatusIds::UNSUPPORTED, "Unsupported value type for SHOW CREATE TABLE");
    }
}
void TCreateTableFormatter::FormatPrimitive(NYdb::TValueParser& parser) {
    switch (parser.GetPrimitiveType()) {
        case NYdb::EPrimitiveType::Bool: {
            if (parser.GetBool()) {
                Stream << "true";
            } else {
                Stream << "false";
            }
            break;
        }
        case NYdb::EPrimitiveType::Int8: {
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Int8, NUdf::TUnboxedValuePod(parser.GetInt8()));
            Y_ENSURE(str.HasValue());
            Stream << TString(str.AsStringRef());
            break;
        }
        case NYdb::EPrimitiveType::Uint8: {
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Uint8, NUdf::TUnboxedValuePod(parser.GetUint8()));
            Y_ENSURE(str.HasValue());
            Stream << TString(str.AsStringRef());
            break;
        }
        case NYdb::EPrimitiveType::Int16: {
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Int16, NUdf::TUnboxedValuePod(parser.GetInt16()));
            Y_ENSURE(str.HasValue());
            Stream << TString(str.AsStringRef());
            break;
        }
        case NYdb::EPrimitiveType::Uint16: {
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Uint16, NUdf::TUnboxedValuePod(parser.GetUint16()));
            Y_ENSURE(str.HasValue());
            Stream << TString(str.AsStringRef());
            break;
        }
        case NYdb::EPrimitiveType::Int32: {
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Int32, NUdf::TUnboxedValuePod(parser.GetInt32()));
            Y_ENSURE(str.HasValue());
            Stream << TString(str.AsStringRef());
            break;
        }
        case NYdb::EPrimitiveType::Uint32: {
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Uint32, NUdf::TUnboxedValuePod(parser.GetUint32()));
            Y_ENSURE(str.HasValue());
            Stream << TString(str.AsStringRef());
            break;
        }
        case NYdb::EPrimitiveType::Int64: {
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Int64, NUdf::TUnboxedValuePod(parser.GetInt64()));
            Y_ENSURE(str.HasValue());
            Stream << TString(str.AsStringRef());
            break;
        }
        case NYdb::EPrimitiveType::Uint64: {
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Uint64, NUdf::TUnboxedValuePod(parser.GetUint64()));
            Y_ENSURE(str.HasValue());
            Stream << TString(str.AsStringRef());
            break;
        }
        case NYdb::EPrimitiveType::Float: {
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Float, NUdf::TUnboxedValuePod(parser.GetFloat()));
            Y_ENSURE(str.HasValue());
            Stream << TString(str.AsStringRef());
            break;
        }
        case NYdb::EPrimitiveType::Double: {
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Double, NUdf::TUnboxedValuePod(parser.GetDouble()));
            Y_ENSURE(str.HasValue());
            Stream << TString(str.AsStringRef());
            break;
        }
        case NYdb::EPrimitiveType::Utf8: {
            EscapeString(TString(parser.GetUtf8()));
            break;
        }
        case NYdb::EPrimitiveType::Date: {
            Stream << "DATE(";
            EscapeString(parser.GetDate().FormatGmTime("%Y-%m-%d"));
            Stream << ")";
            break;
        }
        case NYdb::EPrimitiveType::Datetime: {
            Stream << "DATETIME(";
            EscapeString(parser.GetDatetime().ToStringUpToSeconds());
            Stream << ")";
            break;
        }
        case NYdb::EPrimitiveType::Timestamp: {
            Stream << "TIMESTAMP(";
            EscapeString(parser.GetTimestamp().ToString());
            Stream << ")";
            break;
        }
        case NYdb::EPrimitiveType::Interval: {
            Stream << "INTERVAL(";
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Interval, NUdf::TUnboxedValuePod(parser.GetInterval()));
            Y_ENSURE(str.HasValue());
            EscapeString(TString(str.AsStringRef()));
            Stream << ")";
            break;
        }
        case NYdb::EPrimitiveType::Date32: {
            Stream << "DATE32(";
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Date32, NUdf::TUnboxedValuePod(parser.GetDate32()));
            Y_ENSURE(str.HasValue());
            EscapeString(TString(str.AsStringRef()));
            Stream << ")";
            break;
        }
        case NYdb::EPrimitiveType::Datetime64: {
            Stream << "DATETIME64(";
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Datetime64, NUdf::TUnboxedValuePod(parser.GetDatetime64()));
            Y_ENSURE(str.HasValue());
            EscapeString(TString(str.AsStringRef()));
            Stream << ")";
            break;
        }
        case NYdb::EPrimitiveType::Timestamp64: {
            Stream << "TIMESTAMP64(";
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Timestamp64, NUdf::TUnboxedValuePod(parser.GetTimestamp64()));
            Y_ENSURE(str.HasValue());
            EscapeString(TString(str.AsStringRef()));
            Stream << ")";
            break;
        }
        case NYdb::EPrimitiveType::Interval64: {
            Stream << "INTERVAL64(";
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Interval64, NUdf::TUnboxedValuePod(parser.GetInterval64()));
            Y_ENSURE(str.HasValue());
            EscapeString(TString(str.AsStringRef()));
            Stream << ")";
            break;
        }
        case NYdb::EPrimitiveType::String:
            EscapeString(TString(parser.GetString()));
            break;
        case NYdb::EPrimitiveType::Yson:
            EscapeString(TString(parser.GetYson()));
            break;
        case NYdb::EPrimitiveType::Json:
            EscapeString(TString(parser.GetJson()));
            break;
        case NYdb::EPrimitiveType::DyNumber: {
            Stream << "DyNumber(";
            EscapeString(TString(parser.GetDyNumber()));
            Stream << ")";
            break;
        }
        case NYdb::EPrimitiveType::Uuid: {
            Stream << "UUID(";
            EscapeString(TString(parser.GetUuid().ToString()));
            Stream << ")";
            break;
        }
        default:
            ythrow TFormatFail(Ydb::StatusIds::UNSUPPORTED, "Unsupported primitive type for SHOW CREATE TABLE");
    }
}

void TCreateTableFormatter::Format(const Ydb::TypedValue& value, bool isPartition) {
    NYdb::TValueParser parser(NYdb::TValue(value.type(), value.value()));
    FormatValue(parser, isPartition);
}

class TStringStreamWrapper {
public:
    TStringStreamWrapper(TStringStream& stream)
        : Stream(stream)
    {}

    ~TStringStreamWrapper() {
        Stream.Clear();
    }

private:
    TStringStream& Stream;
};

TCreateTableFormatter::TResult TCreateTableFormatter::Format(const TString& tablePath, const TTableDescription& tableDesc, bool temporary) {
    Stream.Clear();

    TStringStreamWrapper wrapper(Stream);

    Ydb::Table::CreateTableRequest createRequest;
    if (temporary) {
        Stream << "CREATE TEMPORARY TABLE ";
    } else {
        Stream << "CREATE TABLE ";
    }
    EscapeName(tablePath);
    Stream << " (\n";

    NKikimrMiniKQL::TType mkqlKeyType;
    try {
        FillColumnDescription(createRequest, mkqlKeyType, tableDesc);
    } catch (const yexception& e) {
        return TResult(Ydb::StatusIds::UNSUPPORTED, e.what());
    }

    Y_ENSURE(!tableDesc.GetColumns().empty());
    Y_ENSURE(tableDesc.GetColumns().size() == createRequest.columns().size());

    std::map<ui32, const TColumnDescription*> columns;

    for (const auto& column : tableDesc.GetColumns()) {
        columns[column.GetId()] = &column;
    }

    try {
        auto it = columns.cbegin();
        Format(*it->second);
        std::advance(it, 1);
        for (; it != columns.end(); ++it) {
            Stream << ",\n";
            Format(*it->second);
        }
    } catch (const TFormatFail& ex) {
        return TResult(ex.Status, ex.Error);
    } catch (const yexception& e) {
        return TResult(Ydb::StatusIds::UNSUPPORTED, e.what());
    }

    try {
        FillTableBoundary(createRequest, tableDesc, mkqlKeyType);
        FillIndexDescription(createRequest, tableDesc);
    } catch (const yexception& e) {
        return TResult(Ydb::StatusIds::UNSUPPORTED, e.what());;
    }

    if (!createRequest.indexes().empty()) {
        Stream << ",\n";
        try {
            Format(createRequest.indexes(0));
            for (int i = 1; i < createRequest.indexes().size(); i++) {
                Stream << ",\n";
                Format(createRequest.indexes(i));
            }
        } catch (const TFormatFail& ex) {
            return TResult(ex.Status, ex.Error);
        } catch (const yexception& e) {
            return TResult(Ydb::StatusIds::UNSUPPORTED, e.what());
        }
    }

    if (tableDesc.HasPartitionConfig()) {
        const auto partitionConfig = tableDesc.GetPartitionConfig();

        if (!partitionConfig.GetColumnFamilies().empty()) {
            try {
                Stream << ",\n";
                Format(partitionConfig.GetColumnFamilies(0));
                for (int i = 1; i < partitionConfig.GetColumnFamilies().size(); i++) {
                    Stream << ",\n";
                    Format(partitionConfig.GetColumnFamilies(i));
                }
            } catch (const TFormatFail& ex) {
                return TResult(ex.Status, ex.Error);
            } catch (const yexception& e) {
                return TResult(Ydb::StatusIds::UNSUPPORTED, e.what());
            }
        }
    }

    Y_ENSURE(!tableDesc.GetKeyColumnIds().empty());
    Stream << ",\n\tPRIMARY KEY (";
    EscapeName(columns[tableDesc.GetKeyColumnIds(0)]->GetName());
    for (int i = 1; i < tableDesc.GetKeyColumnIds().size(); i++) {
        Stream << ", ";
        EscapeName(columns[tableDesc.GetKeyColumnIds(i)]->GetName());
    }
    Stream << ")\n";

    Stream << ")";

    FillPartitioningSettings(createRequest, tableDesc);

    Stream << " WITH (\n";
    Format(createRequest.partitioning_settings());

    if (createRequest.partitions_case() == Ydb::Table::CreateTableRequest::kPartitionAtKeys) {
        try {
            Format(createRequest.partition_at_keys());
        } catch (const TFormatFail& ex) {
            return TResult(ex.Status, ex.Error);
        } catch (const yexception& e) {
            return TResult(Ydb::StatusIds::UNSUPPORTED, e.what());
        }
    }

    FillReadReplicasSettings(createRequest, tableDesc);

    if (createRequest.has_read_replicas_settings()) {
        Format(createRequest.read_replicas_settings());
    }

    FillKeyBloomFilter(createRequest, tableDesc);

    if (createRequest.key_bloom_filter() == Ydb::FeatureFlag::ENABLED) {
        Stream << ",\n";
        Stream << "\tKEY_BLOOM_FILTER = ENABLED";
    } else if (createRequest.key_bloom_filter() == Ydb::FeatureFlag::DISABLED) {
        Stream << ",\n";
        Stream << "\tKEY_BLOOM_FILTER = DISABLED";
    }

    if (createRequest.has_ttl_settings()) {
        try {
            Format(createRequest.ttl_settings());
        } catch (const TFormatFail& ex) {
            return TResult(ex.Status, ex.Error);
        } catch (const yexception& e) {
            return TResult(Ydb::StatusIds::UNSUPPORTED, e.what());
        }
    }

    Stream << "\n);";
    auto result = TResult(Stream.Str());

    return result;
}

void TCreateTableFormatter::Format(const NKikimrSchemeOp::TColumnDescription& columnDesc) {
    Stream << "\t";
    EscapeName(columnDesc.GetName());
    Stream << " ";

    auto type = columnDesc.GetType();
    std::optional<Ydb::TypedValue> defaultFromLiteral;
    switch (columnDesc.GetDefaultValueCase()) {
        case NKikimrSchemeOp::TColumnDescription::kDefaultFromLiteral: {
            defaultFromLiteral = columnDesc.GetDefaultFromLiteral();
            break;
        }
        case NKikimrSchemeOp::TColumnDescription::kDefaultFromSequence: {
            auto lowerType = to_lower(type);
            if (lowerType == "int64") {
                type = "Serial8";
            } else if (lowerType == "int32") {
                type = "Serial4";
            } else if (lowerType == "int16") {
                type = "Serial2";
            }
            break;
        }
        default: break;
    }

    Stream << type;

    if (columnDesc.HasFamilyName()) {
        Stream << " FAMILY ";
        EscapeName(columnDesc.GetFamilyName());
    }
    if (columnDesc.GetNotNull()) {
        Stream << " NOT NULL";
    }
    if (defaultFromLiteral) {
        Stream << " DEFAULT ";
        Format(defaultFromLiteral.value());
    }
}

void TCreateTableFormatter::Format(const TableIndex& index) {
    Stream << "\tINDEX ";
    EscapeName(index.name());
    std::optional<KMeansTreeSettings> kMeansTreeSettings;
    switch (index.type_case()) {
        case TableIndex::kGlobalIndex: {
            Stream << " GLOBAL SYNC ON ";
            break;
        }
        case TableIndex::kGlobalAsyncIndex: {
            Stream << " GLOBAL ASYNC ON ";
            break;
        }
        case TableIndex::kGlobalUniqueIndex: {
            Stream << " GLOBAL UNIQUE SYNC ON ";
            break;
        }
        case TableIndex::kGlobalVectorKmeansTreeIndex: {
            Stream << " GLOBAL USING vector_kmeans_tree ON ";
            kMeansTreeSettings = index.global_vector_kmeans_tree_index().vector_settings();
            break;
        }
        case Ydb::Table::TableIndex::TYPE_NOT_SET:
            ythrow TFormatFail(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected Ydb::Table::TableIndex::TYPE_NOT_SET");
    }

    Y_ENSURE(!index.index_columns().empty());
    Stream << "(";
    EscapeName(index.index_columns(0));
    for (int i = 1; i < index.index_columns().size(); i++) {
        Stream << ", ";
        EscapeName(index.index_columns(i));
    }
    Stream << ")";

    if (!index.data_columns().empty()) {
        Stream << " COVER ";
        Stream << "(";
        EscapeName(index.data_columns(0));
        for (int i = 1; i < index.data_columns().size(); i++) {
            Stream << ", ";
            EscapeName(index.data_columns(i));
        }
        Stream << ")";
    }

    if (kMeansTreeSettings) {
        Stream << " WITH (";

        switch (kMeansTreeSettings->settings().metric()) {
            case Ydb::Table::VectorIndexSettings::SIMILARITY_INNER_PRODUCT:
                Stream << "similarity=product";
                break;
            case Ydb::Table::VectorIndexSettings::SIMILARITY_COSINE:
                Stream << "similarity=cosine";
                break;
            case Ydb::Table::VectorIndexSettings::DISTANCE_COSINE:
                Stream << "distance=cosine";
                break;
            case Ydb::Table::VectorIndexSettings::DISTANCE_MANHATTAN:
                Stream << "distance=manhattan";
                break;
            case Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN:
                Stream << "distance=euclidean";
                break;
            default:
                ythrow TFormatFail(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected Ydb::Table::VectorIndexSettings");
        }

        TString del = "";
        if (kMeansTreeSettings->settings().metric() != Ydb::Table::VectorIndexSettings::METRIC_UNSPECIFIED) {
            del = ", ";
        }

        switch (kMeansTreeSettings->settings().vector_type()) {
            case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_BIT:
                Stream << del << "vector_type=\"bit\"";
                break;
            case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_INT8:
                Stream << del << "vector_type=\"int8\"";
                break;
            case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8:
                Stream << del << "vector_type=\"uint8\"";
                break;
            case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT:
                Stream << del << "vector_type=\"float\"";
                break;
            default:
                ythrow TFormatFail(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected Ydb::Table::VectorIndexSettings");
        }

        if (kMeansTreeSettings->settings().vector_type() != Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UNSPECIFIED) {
            del = ", ";
        }

        if (kMeansTreeSettings->settings().vector_dimension() != 0) {
            Stream << del << "vector_dimension=" << kMeansTreeSettings->settings().vector_dimension();
            del = ", ";
        }

        if (kMeansTreeSettings->clusters() != 0) {
            Stream << del << "clusters=" << kMeansTreeSettings->clusters();
            del = ", ";
        }

        if (kMeansTreeSettings->levels() != 0) {
            Stream << del << "levels=" << kMeansTreeSettings->levels();
            del = ", ";
        }

        Stream << ")";
    }
}

void TCreateTableFormatter::Format(const TFamilyDescription& familyDesc) {
    Stream << "\tFAMILY ";
    if (familyDesc.HasName() && !familyDesc.GetName().empty()) {
        EscapeName(familyDesc.GetName());
    } else if (familyDesc.HasId() && familyDesc.GetId() != 0) {
        ythrow TFormatFail(Ydb::StatusIds::UNSUPPORTED, "Family by id is not supported");
    } else {
        Stream << "default";
    }
    Stream << " (";

    TString del;
    if (familyDesc.HasStorageConfig() && familyDesc.GetStorageConfig().HasData()) {
        const auto& data = familyDesc.GetStorageConfig().GetData();
        if (!data.GetAllowOtherKinds()) {
            if (!data.GetPreferredPoolKind().empty()) {
                Stream << "DATA = " << "\"" << data.GetPreferredPoolKind() << "\"";
                del = ", ";
            }
        }
    }

    if (familyDesc.HasColumnCodec()) {
        switch (familyDesc.GetColumnCodec()) {
            case NKikimrSchemeOp::ColumnCodecPlain:
                Stream << del << "COMPRESSION = " << "\"off\"";
                break;
            case NKikimrSchemeOp::ColumnCodecLZ4:
                Stream << del << "COMPRESSION = " << "\"lz4\"";
                break;
            case NKikimrSchemeOp::ColumnCodecZSTD:
                ythrow TFormatFail(Ydb::StatusIds::UNSUPPORTED, "ZSTD COMPRESSION codec is not supported");
        }
    } else if (familyDesc.GetCodec() == 1) {
        Stream << del << "COMPRESSION = " << "\"lz4\"";
    } else {
        Stream << del << "COMPRESSION = " << "\"off\"";
    }

    Stream << ")";
}

void TCreateTableFormatter::Format(const Ydb::Table::PartitioningSettings& partitionSettings) {
    if (partitionSettings.partitioning_by_size() == Ydb::FeatureFlag::ENABLED) {
        Stream << "\tAUTO_PARTITIONING_BY_SIZE = ENABLED,\n";
        auto partitionBySize = partitionSettings.partition_size_mb();
        Stream << "\tAUTO_PARTITIONING_PARTITION_SIZE_MB = " << partitionBySize;
    } else {
        Stream << "\tAUTO_PARTITIONING_BY_SIZE = DISABLED";
    }

    Stream << ",\n";
    if (partitionSettings.partitioning_by_load() == Ydb::FeatureFlag::ENABLED) {
        Stream << "\tAUTO_PARTITIONING_BY_LOAD = ENABLED";
    } else {
        Stream << "\tAUTO_PARTITIONING_BY_LOAD = DISABLED";
    }

    if (partitionSettings.min_partitions_count()) {
        Stream << ",\n";
        Stream << "\tAUTO_PARTITIONING_MIN_PARTITIONS_COUNT = " << partitionSettings.min_partitions_count();
    }

    if (partitionSettings.max_partitions_count()) {
        Stream << ",\n";
        Stream << "\tAUTO_PARTITIONING_MAX_PARTITIONS_COUNT = " << partitionSettings.max_partitions_count();
    }
}

void TCreateTableFormatter::Format(const Ydb::Table::ExplicitPartitions& explicitPartitions) {
    if (explicitPartitions.split_points().empty()) {
        return;
    }
    Stream << ",\n";
    Stream << "\tPARTITION_AT_KEYS = (";
    Format(explicitPartitions.split_points(0), true);
    for (int i = 1; i < explicitPartitions.split_points().size(); i++) {
        Stream << ", ";
        Format(explicitPartitions.split_points(i), true);
    }
    Stream << ")";
}

void TCreateTableFormatter::Format(const Ydb::Table::ReadReplicasSettings& readReplicasSettings) {
    Stream << ",\n";
    Stream << "\tREAD_REPLICAS_SETTINGS = ";
    switch (readReplicasSettings.settings_case()) {
        case Ydb::Table::ReadReplicasSettings::kPerAzReadReplicasCount:
        {
            Stream << "\"PER_AZ:" << readReplicasSettings.per_az_read_replicas_count() << "\"";
            break;
        }
        case Ydb::Table::ReadReplicasSettings::kAnyAzReadReplicasCount:
        {
            Stream << "\"ANY_AZ:" << readReplicasSettings.any_az_read_replicas_count() << "\"";
            break;
        }
        default:
            break;
    }
}

void TCreateTableFormatter::Format(ui64 expireAfterSeconds, std::optional<TString> storage) {
    TGuard<NMiniKQL::TScopedAlloc> guard(Alloc);
    Stream << "INTERVAL(";
    const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Interval, NUdf::TUnboxedValuePod(expireAfterSeconds * 1000000));
    Y_ENSURE(str.HasValue());
    EscapeString(TString(str.AsStringRef()));
    Stream << ") ";
    if (storage) {
        Stream << "TO EXTERNAL DATA SOURCE ";
        EscapeName(*storage);
    } else {
        Stream << "DELETE";
    }
}

void TCreateTableFormatter::Format(const Ydb::Table::TtlSettings& ttlSettings) {
    Stream << ",\n";
    Stream << "\tTTL =\n\t  ";
    bool first = true;
    std::optional<TString> columnName;
    std::optional<Ydb::Table::ValueSinceUnixEpochModeSettings::Unit> columnUnit;
    std::optional<Ydb::Table::TtlTier::ExpressionCase> expressionType;
    switch (ttlSettings.mode_case()) {
        case Ydb::Table::TtlSettings::kDateTypeColumn: {
            const auto& mode = ttlSettings.date_type_column();
            columnName = mode.column_name();
            Format(mode.expire_after_seconds());
            break;
        }
        case Ydb::Table::TtlSettings::kValueSinceUnixEpoch: {
            const auto& mode = ttlSettings.value_since_unix_epoch();
            columnName = mode.column_name();
            columnUnit = mode.column_unit();
            Format(mode.expire_after_seconds());
            break;
        }
        case Ydb::Table::TtlSettings::kTieredTtl: {
            if (!ttlSettings.tiered_ttl().tiers_size()) {
                ythrow TFormatFail(Ydb::StatusIds::INTERNAL_ERROR, "No tiers in TTL settings");
            }

            for (const auto& tier : ttlSettings.tiered_ttl().tiers()) {
                TString tierColumnName;
                ui32 expireAfterSeconds;
                switch (tier.expression_case()) {
                    case Ydb::Table::TtlTier::kDateTypeColumn: {
                        const auto& mode = tier.date_type_column();
                        expireAfterSeconds = mode.expire_after_seconds();
                        tierColumnName = mode.column_name();
                        break;
                    }
                    case Ydb::Table::TtlTier::kValueSinceUnixEpoch: {
                        const auto& mode = tier.value_since_unix_epoch();
                        tierColumnName = mode.column_name();
                        expireAfterSeconds = mode.expire_after_seconds();
                        if (columnUnit) {
                            if (*columnUnit != mode.column_unit()) {
                                ythrow TFormatFail(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder()
                                    << "Unit of the TTL columns must be the same for all tiers: "
                                    << Ydb::Table::ValueSinceUnixEpochModeSettings::Unit_Name(*columnUnit)
                                    << " != " << Ydb::Table::ValueSinceUnixEpochModeSettings::Unit_Name(mode.column_unit()));
                            }
                        } else {
                            columnUnit = mode.column_unit();
                        }
                        break;
                    }
                    case Ydb::Table::TtlTier::EXPRESSION_NOT_SET:
                        ythrow TFormatFail(Ydb::StatusIds::INTERNAL_ERROR, "Tier expression is undefined");
                }

                if (columnName) {
                    if (*columnName != tierColumnName) {
                        ythrow TFormatFail(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "TTL columns must be the same for all tiers: " << *columnName << " != " << tierColumnName);
                    }
                } else {
                    columnName = tierColumnName;
                }

                if (expressionType) {
                    if (*expressionType != tier.expression_case()) {
                        ythrow TFormatFail(Ydb::StatusIds::INTERNAL_ERROR, "Expression type must be the same for all tiers");
                    }
                } else {
                    expressionType = tier.expression_case();
                }

                if (!first) {
                    Stream << ", ";
                }

                switch (tier.action_case()) {
                    case Ydb::Table::TtlTier::kDelete:
                        Format(expireAfterSeconds);
                        break;
                    case Ydb::Table::TtlTier::kEvictToExternalStorage:
                        Format(expireAfterSeconds, tier.evict_to_external_storage().storage());
                        break;
                    case Ydb::Table::TtlTier::ACTION_NOT_SET:
                        ythrow TFormatFail(Ydb::StatusIds::INTERNAL_ERROR, "Tier action is undefined");
                }
                first = false;
            }
        } break;

        case Ydb::Table::TtlSettings::MODE_NOT_SET:
            ythrow TFormatFail(Ydb::StatusIds::INTERNAL_ERROR, "TTL mode is undefined");
    }

    Stream << "\n\t  ON " << columnName;
    if (columnUnit) {
        Stream << " AS ";
        switch (*columnUnit) {
            case Ydb::Table::ValueSinceUnixEpochModeSettings::UNIT_SECONDS:
                Stream << "SECONDS";
                break;
            case Ydb::Table::ValueSinceUnixEpochModeSettings::UNIT_MILLISECONDS:
                Stream << "MILLISECONDS";
                break;
            case Ydb::Table::ValueSinceUnixEpochModeSettings::UNIT_MICROSECONDS:
                Stream << "MICROSECONDS";
                break;
            case Ydb::Table::ValueSinceUnixEpochModeSettings::UNIT_NANOSECONDS:
                Stream << "NANOSECONDS";
                break;
            default:
                ythrow TFormatFail(Ydb::StatusIds::INTERNAL_ERROR, "Unsupported unit");
        }
    }
}

} // NSysView
} // NKikimr

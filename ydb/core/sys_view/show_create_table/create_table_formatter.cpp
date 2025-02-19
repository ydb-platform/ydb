#include "create_table_formatter.h"

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <yql/essentials/ast/yql_ast_escaping.h>

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
            Stream << " AS Decimal(" << precision << "," << scale << ")";
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
        case NYdb::EPrimitiveType::Int8:
            Stream << parser.GetInt8();
            break;
        case NYdb::EPrimitiveType::Uint8:
            Stream << parser.GetUint8();
            break;
        case NYdb::EPrimitiveType::Int16:
            Stream << parser.GetInt16();
            break;
        case NYdb::EPrimitiveType::Uint16:
            Stream << parser.GetUint16();
            break;
        case NYdb::EPrimitiveType::Int32:
            Stream << parser.GetInt32();
            break;
        case NYdb::EPrimitiveType::Uint32:
            Stream << parser.GetUint32();
            break;
        case NYdb::EPrimitiveType::Int64:
            Stream << parser.GetInt64();
            break;
        case NYdb::EPrimitiveType::Uint64:
            Stream << parser.GetUint64();
            break;
        case NYdb::EPrimitiveType::Float: {
            Stream << "CAST(";
            Stream << parser.GetFloat();
            Stream << " AS FLOAT)";
            break;
        }
        case NYdb::EPrimitiveType::Double:
            Stream << parser.GetDouble();
            break;
        case NYdb::EPrimitiveType::Utf8:
            EscapeString(TString(parser.GetUtf8()));
            break;
        case NYdb::EPrimitiveType::Date: {
            Stream << "CAST(";
            EscapeString(parser.GetDate().FormatGmTime("%Y-%m-%d"));
            Stream << " AS DATE)";
            break;
        }
        case NYdb::EPrimitiveType::Datetime: {
            Stream << "CAST(";
            EscapeString(parser.GetDatetime().ToStringUpToSeconds());
            Stream << " AS DATETIME)";
            break;
        }
        case NYdb::EPrimitiveType::Timestamp: {
            Stream << "CAST(";
            EscapeString(parser.GetTimestamp().ToString());
            Stream << " AS TIMESTAMP)";
            break;
        }
        case NYdb::EPrimitiveType::Interval: {
            Stream << "CAST(";
            Stream << parser.GetInterval();
            Stream << " AS INTERVAL)";
            break;
        }
        case NYdb::EPrimitiveType::Date32: {
            Stream << "CAST(";
            Stream << parser.GetDate32();
            Stream << " AS DATE32)";
            break;
        }
        case NYdb::EPrimitiveType::Datetime64: {
            Stream << "CAST(";
            Stream << parser.GetDatetime64();
            Stream << " AS DATETIME64)";
            break;
        }
        case NYdb::EPrimitiveType::Timestamp64: {
            Stream << "CAST(";
            Stream << parser.GetTimestamp64();
            Stream << " AS TIMESTAMP64)";
            break;
        }
        case NYdb::EPrimitiveType::Interval64: {
            Stream << "CAST(";
            Stream << parser.GetInterval64();
            Stream << " AS INTERVAL64)";
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
            Stream << "CAST(";
            EscapeString(TString(parser.GetDyNumber()));
            Stream << " AS DyNumber)";
            break;
        }
        case NYdb::EPrimitiveType::Uuid: {
            Stream << "CAST(";
            EscapeString(TString(parser.GetUuid().ToString()));
            Stream << " AS UUID)";
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

TCreateTableFormatter::TResult TCreateTableFormatter::Format(const TTableDescription& tableDesc) {
    Stream.Clear();

    TStringStreamWrapper wrapper(Stream);

    Ydb::Table::CreateTableRequest createRequest;
    if (tableDesc.GetTemporary()) {
        Stream << "CREATE TEMPORARY TABLE ";
    } else {
        Stream << "CREATE TABLE ";
    }
    EscapeName(tableDesc.GetName());
    Stream << " (\n";

    NKikimrMiniKQL::TType mkqlKeyType;
    try {
        FillColumnDescription(createRequest, mkqlKeyType, tableDesc);
    } catch (const yexception& e) {
        return TResult(Ydb::StatusIds::UNSUPPORTED, e.what());
    }

    Y_ENSURE(!tableDesc.GetColumns().empty());
    Y_ENSURE(tableDesc.GetColumns().size() == createRequest.columns().size());

    try {
        Format(tableDesc.GetColumns(0));
        for (int i = 1; i < tableDesc.GetColumns().size(); i++) {
            Stream << ",\n";
            Format(tableDesc.GetColumns(i));
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

    Y_ENSURE(!tableDesc.GetKeyColumnNames().empty());
    Stream << ",\n\tPRIMARY KEY (";
    EscapeName(tableDesc.GetKeyColumnNames(0));
    for (int i = 1; i < tableDesc.GetKeyColumnNames().size(); i++) {
        Stream << ", ";
        EscapeName(tableDesc.GetKeyColumnNames(i));
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
                Stream << "similarity=product";
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
                Stream << del << "vector_type=\"float\"";
                break;
            case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_INT8:
                Stream << del << "vector_type=\"int8\"";
                break;
            case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8:
                Stream << del << "vector_type=\"uint8\"";
                break;
            case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT:
                Stream << del << "vector_type=\"bit\"";
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
    Stream << "\n";
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

} // NSysView
} // NKikimr

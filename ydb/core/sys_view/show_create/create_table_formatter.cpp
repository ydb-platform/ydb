#include "create_table_formatter.h"

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <ydb/public/lib/ydb_cli/dump/util/query_utils.h>

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
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Int64, NUdf::TUnboxedValuePod(static_cast<i64>(parser.GetInt64())));
            Y_ENSURE(str.HasValue());
            Stream << TString(str.AsStringRef());
            break;
        }
        case NYdb::EPrimitiveType::Uint64: {
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Uint64, NUdf::TUnboxedValuePod(static_cast<ui64>(parser.GetUint64())));
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
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Interval, NUdf::TUnboxedValuePod(static_cast<i64>(parser.GetInterval())));
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
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Datetime64, NUdf::TUnboxedValuePod(static_cast<i64>(parser.GetDatetime64())));
            Y_ENSURE(str.HasValue());
            EscapeString(TString(str.AsStringRef()));
            Stream << ")";
            break;
        }
        case NYdb::EPrimitiveType::Timestamp64: {
            Stream << "TIMESTAMP64(";
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Timestamp64, NUdf::TUnboxedValuePod(static_cast<i64>(parser.GetTimestamp64())));
            Y_ENSURE(str.HasValue());
            EscapeString(TString(str.AsStringRef()));
            Stream << ")";
            break;
        }
        case NYdb::EPrimitiveType::Interval64: {
            Stream << "INTERVAL64(";
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Interval64, NUdf::TUnboxedValuePod(static_cast<i64>(parser.GetInterval64())));
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

TCreateTableFormatter::TResult TCreateTableFormatter::Format(const TString& tablePath,
        const TTableDescription& tableDesc, bool temporary) {
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
    Stream << ",\n";

    bool isFamilyPrinted = false;
    if (tableDesc.HasPartitionConfig()) {
        const auto partitionConfig = tableDesc.GetPartitionConfig();

        if (!partitionConfig.GetColumnFamilies().empty()) {
            try {
                isFamilyPrinted = Format(partitionConfig.GetColumnFamilies(0));
                for (int i = 1; i < partitionConfig.GetColumnFamilies().size(); i++) {
                    if (isFamilyPrinted) {
                        Stream << ",\n";
                    }
                    isFamilyPrinted = Format(partitionConfig.GetColumnFamilies(i));
                }
            } catch (const TFormatFail& ex) {
                return TResult(ex.Status, ex.Error);
            } catch (const yexception& e) {
                return TResult(Ydb::StatusIds::UNSUPPORTED, e.what());
            }
        }
    }

    Y_ENSURE(!tableDesc.GetKeyColumnIds().empty());
    if (isFamilyPrinted) {
        Stream << ",\n";
    }
    Stream << "\tPRIMARY KEY (";
    EscapeName(columns[tableDesc.GetKeyColumnIds(0)]->GetName());
    for (int i = 1; i < tableDesc.GetKeyColumnIds().size(); i++) {
        Stream << ", ";
        EscapeName(columns[tableDesc.GetKeyColumnIds(i)]->GetName());
    }
    Stream << ")\n";
    Stream << ")";

    TString del = "";
    bool printed = false;

    if (tableDesc.HasPartitionConfig()) {
        if (tableDesc.GetPartitionConfig().HasPartitioningPolicy()) {
            ui32 shardsToCreate = NSchemeShard::TTableInfo::ShardsToCreate(tableDesc);
            printed |= Format(tableDesc.GetPartitionConfig().GetPartitioningPolicy(), shardsToCreate, del, !printed);
        }
    }

    if (createRequest.partitions_case() == Ydb::Table::CreateTableRequest::kPartitionAtKeys) {
        try {
            printed |= Format(createRequest.partition_at_keys(), del, !printed);
        } catch (const TFormatFail& ex) {
            return TResult(ex.Status, ex.Error);
        } catch (const yexception& e) {
            return TResult(Ydb::StatusIds::UNSUPPORTED, e.what());
        }
    }

    FillReadReplicasSettings(createRequest, tableDesc);

    if (createRequest.has_read_replicas_settings()) {
        printed |= Format(createRequest.read_replicas_settings(), del, !printed);
    }

    FillKeyBloomFilter(createRequest, tableDesc);

    if (createRequest.key_bloom_filter() == Ydb::FeatureFlag::ENABLED) {
        if (!printed) {
            Stream << " WITH (\n";
        }
        Stream << del << "\tKEY_BLOOM_FILTER = ENABLED";
        printed = true;
        del = ",\n";
    } else if (createRequest.key_bloom_filter() == Ydb::FeatureFlag::DISABLED) {
        if (!printed) {
            Stream << " WITH (\n";
        }
        Stream << del << "\tKEY_BLOOM_FILTER = DISABLED";
        printed = true;
        del = ",\n";
    }

    if (createRequest.has_ttl_settings()) {
        try {
            printed |= Format(createRequest.ttl_settings(), del, !printed);
        } catch (const TFormatFail& ex) {
            return TResult(ex.Status, ex.Error);
        } catch (const yexception& e) {
            return TResult(Ydb::StatusIds::UNSUPPORTED, e.what());
        }
    }

    if (printed) {
        Stream << "\n);";
    }

    TString statement = Stream.Str();
    TString formattedStatement;
    NYql::TIssues issues;
    if (!NYdb::NDump::Format(statement, formattedStatement, issues)) {
        return TResult(Ydb::StatusIds::INTERNAL_ERROR, issues.ToString());
    }

    auto result = TResult(std::move(formattedStatement));

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

bool TCreateTableFormatter::Format(const TFamilyDescription& familyDesc) {
    TString familyName;
    if (familyDesc.HasName() && !familyDesc.GetName().empty()) {
        familyName = familyDesc.GetName();
    } else if (familyDesc.HasId() && familyDesc.GetId() != 0) {
        ythrow TFormatFail(Ydb::StatusIds::UNSUPPORTED, "Family by id is not supported");
    } else {
        familyName = "default";
    }

    TString dataName;
    if (familyDesc.HasStorageConfig() && familyDesc.GetStorageConfig().HasData()) {
        const auto& data = familyDesc.GetStorageConfig().GetData();
        if (!data.GetAllowOtherKinds()) {
            if (!data.GetPreferredPoolKind().empty()) {
                dataName = data.GetPreferredPoolKind();
            }
        }
    }

    TString compression;
    if (familyDesc.HasColumnCodec()) {
        switch (familyDesc.GetColumnCodec()) {
            case NKikimrSchemeOp::ColumnCodecPlain:
                compression = "off";
                break;
            case NKikimrSchemeOp::ColumnCodecLZ4:
                compression  = "lz4";
                break;
            case NKikimrSchemeOp::ColumnCodecZSTD:
                ythrow TFormatFail(Ydb::StatusIds::UNSUPPORTED, "ZSTD COMPRESSION codec is not supported");
        }
    } else if (familyDesc.HasCodec()) {
        if (familyDesc.GetCodec() == 1) {
            compression  = "lz4";
        } else {
            compression = "off";
        }
    }

    if (familyName == "default") {
        if (!dataName && !compression) {
            return false;
        }
    }

    Y_ENSURE(familyName);

    Stream << "\tFAMILY ";
    EscapeName(familyName);
    Stream << " (";

    TString del = "";
    if (dataName) {
        Stream << "DATA = " << "\"" << dataName << "\"";
        del = ", ";
    }

    if (dataName) {
        Stream << del << "COMPRESSION = " << "\"" << compression << "\"";
    }

    Stream << ")";
    return true;
}

bool TCreateTableFormatter::Format(const NKikimrSchemeOp::TPartitioningPolicy& policy, ui32 shardsToCreate, TString& del, bool needWith) {
    bool printed = false;
    if (policy.HasSizeToSplit()) {
        if (needWith) {
            Stream << " WITH (\n";
            needWith = false;
        }
        if (policy.GetSizeToSplit()) {
            Stream << del << "\tAUTO_PARTITIONING_BY_SIZE = ENABLED,\n";
            auto partitionBySize = policy.GetSizeToSplit() / (1 << 20);
            Stream << "\tAUTO_PARTITIONING_PARTITION_SIZE_MB = " << partitionBySize;
        } else {
            Stream << del << "\tAUTO_PARTITIONING_BY_SIZE = DISABLED";
        }
        del = ",\n";
        printed = true;
    }

    if (policy.HasSplitByLoadSettings()) {
        if (needWith) {
            Stream << " WITH (\n";
            needWith = false;
        }
        if (policy.GetSplitByLoadSettings().GetEnabled()) {
            Stream << del << "\tAUTO_PARTITIONING_BY_LOAD = ENABLED";
        } else {
            Stream << del << "\tAUTO_PARTITIONING_BY_LOAD = DISABLED";
        }
        del = ",\n";
        printed = true;
    }

    if (policy.HasMinPartitionsCount() && policy.GetMinPartitionsCount() && policy.GetMinPartitionsCount() != shardsToCreate) {
        if (needWith) {
            Stream << " WITH (\n";
            needWith = false;
        }
        Stream << del << "\tAUTO_PARTITIONING_MIN_PARTITIONS_COUNT = " << policy.GetMinPartitionsCount();
        del = ",\n";
        printed = true;
    }

    if (policy.HasMaxPartitionsCount() && policy.GetMaxPartitionsCount()) {
        if (needWith) {
            Stream << " WITH (\n";
        }
        Stream << del << "\tAUTO_PARTITIONING_MAX_PARTITIONS_COUNT = " << policy.GetMaxPartitionsCount();
        del = ",\n";
        printed = true;
    }
    return printed;
}

bool TCreateTableFormatter::Format(const Ydb::Table::ExplicitPartitions& explicitPartitions, TString& del, bool needWith) {
    if (explicitPartitions.split_points().empty()) {
        return false;
    }
    if (needWith) {
        Stream << " WITH (\n";
    }
    Stream << del << "\tPARTITION_AT_KEYS = (";
    del = ",\n";
    Format(explicitPartitions.split_points(0), true);
    for (int i = 1; i < explicitPartitions.split_points().size(); i++) {
        Stream << ", ";
        Format(explicitPartitions.split_points(i), true);
    }
    Stream << ")";
    return true;
}

bool TCreateTableFormatter::Format(const Ydb::Table::ReadReplicasSettings& readReplicasSettings, TString& del, bool needWith) {
    switch (readReplicasSettings.settings_case()) {
        case Ydb::Table::ReadReplicasSettings::kPerAzReadReplicasCount:
        {
            if (needWith) {
                Stream << " WITH (\n";
            }
            Stream << del << "\tREAD_REPLICAS_SETTINGS = \"PER_AZ:" << readReplicasSettings.per_az_read_replicas_count() << "\"";
            del = ",\n";
            return true;
        }
        case Ydb::Table::ReadReplicasSettings::kAnyAzReadReplicasCount:
        {
            if (needWith) {
                Stream << " WITH (\n";
            }
            Stream << del << "\tREAD_REPLICAS_SETTINGS = \"ANY_AZ:" << readReplicasSettings.any_az_read_replicas_count() << "\"";
            del = ",\n";
            return true;
        }
        default:
            break;
    }
    return false;
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

bool TCreateTableFormatter::Format(const Ydb::Table::TtlSettings& ttlSettings, TString& del, bool needWith) {
    if (needWith) {
        Stream << " WITH (\n";
    }
    Stream << del;
    Stream << "\tTTL =\n\t  ";
    del = ",\n";
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
    return true;
}

} // NSysView
} // NKikimr
